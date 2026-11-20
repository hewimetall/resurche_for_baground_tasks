"""Проверки механизмов simple-q 1.1.0–1.3.0.

Redis только на порту 6430, каталог под results/long/simpleq-1.3.0/.
Сценарии стенда и их JSON не меняются.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path("/workspace")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from bench.longrun.stand import ensure_redis, flush_owned  # noqa: E402
from bench.simpleq_130_app import alloc, fail, nap, once  # noqa: E402
from redis_fault_proxy import FaultProxy  # noqa: E402
from simple_q import Client, Config, QueueAdmin  # noqa: E402
from simple_q.admin import DeadPage, Snapshot, TaskPage  # noqa: E402

PORT = 6430
PROXY_PORT = 6431
REDIS_DIR = "/workspace/results/long/simpleq-1.3.0/redis-6430"
OUT = ROOT / "results" / "simpleq-1.3.0" / "mechanisms.json"
WORKER = "/workspace/.venv/bin/agent-worker"
EVENTS = Path("/tmp/sq130-events.jsonl")
ONCE = Path("/tmp/sq130-once.txt")
CGROUP = Path("/sys/fs/cgroup/sq130mem")


def redis_cli(*args: str, port: int = PORT) -> str:
    proc = subprocess.run(
        ["redis-cli", "-p", str(port), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return (proc.stdout or proc.stderr or "").strip()


def flush() -> None:
    flush_owned(PORT, REDIS_DIR)


def read_events() -> list[dict]:
    if not EVENTS.exists():
        return []
    rows = []
    for line in EVENTS.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def reset_events() -> None:
    EVENTS.write_text("", encoding="utf-8")
    ONCE.write_text("", encoding="utf-8")


def base_env(extra: dict[str, str] | None = None, url: str | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env["REDIS_URL"] = url or f"redis://127.0.0.1:{PORT}/0"
    env["PYTHONPATH"] = str(ROOT)
    env["SQ130_EVENTS"] = str(EVENTS)
    env["SQ130_ONCE"] = str(ONCE)
    env["PYTHONUNBUFFERED"] = "1"
    env["DRAMATIQ_PROM_PORT"] = "9193"
    if extra:
        env.update(extra)
    return env


def start_worker(
    env: dict[str, str],
    *,
    processes: int = 1,
    soft: str | None = None,
    hard: str | None = None,
    log_path: Path,
    cgroup: bool = False,
) -> subprocess.Popen:
    cmd = [
        WORKER,
        "worker",
        "--app",
        "bench.simpleq_130_app:create_app",
        "--factory",
        "--queues",
        "bench",
        "--processes",
        str(processes),
        "--concurrency",
        "1",
        "--max-tasks-per-process",
        "50",
    ]
    if soft:
        cmd += ["--child-memory-soft", soft]
    if hard:
        cmd += ["--child-memory-hard", hard]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(log_path, "w", encoding="utf-8")
    if not cgroup:
        return subprocess.Popen(
            ["stdbuf", "-oL", "-eL", *cmd],
            cwd=ROOT,
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    allow = {
        "REDIS_URL",
        "PYTHONPATH",
        "PYTHONUNBUFFERED",
        "SQ130_EVENTS",
        "SQ130_ONCE",
        "DRAMATIQ_PROM_PORT",
        "LD_PRELOAD",
        "SIMPLE_Q_HEARTBEAT_SECS",
        "SIMPLE_Q_MISSED_HEARTBEATS",
        "SIMPLE_Q_DEAD_TTL_SECS",
        "SIMPLE_Q_MAX_DELIVERIES",
    }
    pairs = [f"{key}={env[key]}" for key in allow if key in env]
    inner = f'echo $$ > {CGROUP}/cgroup.procs && exec "$@"'
    return subprocess.Popen(
        ["sudo", "env", *pairs, "bash", "-c", inner, "bash", *cmd],
        cwd=ROOT,
        stdout=handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def stop_worker(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        _kill_app()
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        proc.wait(timeout=6)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        proc.wait(timeout=5)
    _kill_app()


def _kill_app() -> None:
    subprocess.run(["pkill", "-f", "bench.simpleq_130_app:create_app"], check=False)
    time.sleep(0.2)


def wait_events(kind: str, token: str, timeout: float) -> list[dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = [row for row in read_events() if row.get("kind") == kind and row.get("token") == token]
        if rows:
            return rows
        time.sleep(0.05)
    return []


def child_pids() -> list[int]:
    proc = subprocess.run(["pgrep", "-f", "agent-worker.bin worker --child"], check=False, capture_output=True, text=True)
    pids = []
    for line in proc.stdout.splitlines():
        try:
            pids.append(int(line.strip()))
        except ValueError:
            continue
    return pids


def rss_of(pid: int) -> int | None:
    try:
        for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def log_excerpt(path: Path, limit: int = 4000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= limit:
        return text
    return text[:limit] + "\n…\n" + text[-limit:]


def interesting_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    needles = (
        "draining child",
        "killing child",
        "failed to signal",
        "supervisor exited",
        "drain requested",
        "restart",
        "worker",
        "broken",
        "Backend",
    )
    rows = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if any(needle in line for needle in needles):
            rows.append(line[:400])
    return rows[-80:]


async def send(env: dict[str, str], actor, kwargs: dict, **options: Any) -> str | None:
    client = Client(env["REDIS_URL"], Config.from_env())
    try:
        return await client.send_with_options(actor, kwargs=kwargs, **options)
    finally:
        close = getattr(client, "close", None)
        if close:
            result = close()
            if asyncio.iscoroutine(result):
                await result


def config_defaults() -> dict:
    os.environ.pop("SIMPLE_Q_PING_BUDGET_SECS", None)
    cfg = Config()
    fields = (
        "batch_size",
        "poll_interval_ms",
        "ping_interval_ms",
        "ping_timeout_ms",
        "lifecycle_interval_ms",
        "sweep_interval_secs",
        "sweep_batch",
        "idempotency_wait_ms",
        "idempotency_wait_max_ms",
        "ping_budget_secs",
        "dead_ttl_secs",
        "unique_ttl_secs",
        "heartbeat_secs",
        "missed_heartbeats",
        "max_deliveries",
        "memory_soft_ratio",
        "memory_hard_ratio",
    )
    return {name: getattr(cfg, name) for name in fields}


def check_defaults() -> dict:
    got = config_defaults()
    expect = {
        "poll_interval_ms": 500,
        "ping_interval_ms": 2000,
        "ping_timeout_ms": 3000,
        "lifecycle_interval_ms": 200,
        "sweep_interval_secs": 60,
        "sweep_batch": 100,
        "batch_size": 1,
        "idempotency_wait_ms": 20,
        "idempotency_wait_max_ms": 5000,
        "ping_budget_secs": 1860,
    }
    return {"config": got, "matches_changelog": all(got[key] == value for key, value in expect.items())}


async def _unique_release() -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path("/tmp/sq130-unique.log")
    proc = start_worker(env, log_path=log)
    try:
        key = "item:unique-release"
        redis_key = f"bench:idempotency:{key}"
        first = await send(env, nap, {"token": "unique-release", "seconds": 0.2}, unique_key=key)
        ends = wait_events("end", "unique-release", 15)
        gone_at = None
        deadline = time.time() + 8
        while time.time() < deadline:
            if redis_cli("EXISTS", redis_key) == "0":
                gone_at = time.time()
                break
            time.sleep(0.05)
        second = await send(env, nap, {"token": "unique-release-2", "seconds": 0.1}, unique_key=key)
        second_end = wait_events("end", "unique-release-2", 15)
        return {
            "first_id": first,
            "key": redis_key,
            "released": gone_at is not None,
            "release_after_end_s": None if gone_at is None or not ends else round(gone_at - ends[0]["t"], 3),
            "second_id": second,
            "second_ran": bool(second_end),
            "ttl_left": redis_cli("TTL", redis_key),
        }
    finally:
        stop_worker(proc)


async def _unique_stop() -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path("/tmp/sq130-unique-stop.log")
    proc = start_worker(env, log_path=log)
    key = "item:unique-stop"
    redis_key = f"bench:idempotency:{key}"
    try:
        await send(env, nap, {"token": "unique-stop", "seconds": 30}, unique_key=key)
        wait_events("start", "unique-stop", 15)
        time.sleep(0.3)
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=5)
    finally:
        _kill_app()
    time.sleep(0.5)
    return {
        "key": redis_key,
        "exists_after_sigkill": redis_cli("EXISTS", redis_key),
        "ttl": redis_cli("TTL", redis_key),
    }


async def _unique_redis_error() -> dict:
    flush()
    reset_events()
    proxy = FaultProxy(PROXY_PORT, PORT)
    proxy.start()
    env = base_env(url=f"redis://127.0.0.1:{PROXY_PORT}/0")
    log = Path("/tmp/sq130-unique-err.log")
    proc = start_worker(env, log_path=log)
    key = "item:unique-err"
    redis_key = f"bench:idempotency:{key}"
    try:
        await send(env, nap, {"token": "unique-err", "seconds": 0.3}, unique_key=key)
        wait_events("start", "unique-err", 15)
        proxy.blackhole(2.5)
        ends = wait_events("end", "unique-err", 10)
        deadline = time.time() + 12
        released = False
        while time.time() < deadline:
            if redis_cli("EXISTS", redis_key) == "0":
                released = True
                break
            time.sleep(0.1)
        return {
            "handler_ended": bool(ends),
            "released_after_blackhole": released,
            "ttl": redis_cli("TTL", redis_key),
            "exists": redis_cli("EXISTS", redis_key),
        }
    finally:
        stop_worker(proc)
        proxy.stop()


async def _slow_ack() -> dict:
    flush()
    reset_events()
    proxy = FaultProxy(PROXY_PORT, PORT)
    proxy.start()
    proxy.arm("finish", "delay_response", delay_s=3, once=False)
    env = base_env(url=f"redis://127.0.0.1:{PROXY_PORT}/0")
    log = Path("/tmp/sq130-slow-ack.log")
    proc = start_worker(env, log_path=log)
    key = "item:slow-ack"
    redis_key = f"bench:idempotency:{key}"
    samples = []
    try:
        await send(env, nap, {"token": "slow-ack", "seconds": 0.4}, unique_key=key)
        started = time.time()
        deadline = time.time() + 15
        while time.time() < deadline:
            samples.append({"t": round(time.time() - started, 3), "exists": redis_cli("EXISTS", redis_key)})
            if samples[-1]["exists"] == "0" and any(row["exists"] == "1" for row in samples):
                break
            time.sleep(0.2)
        ends = wait_events("end", "slow-ack", 1)
        return {
            "handler_ended": bool(ends),
            "saw_key": any(row["exists"] == "1" for row in samples),
            "released": any(row["exists"] == "0" and row["t"] > 0.5 for row in samples),
            "samples": samples[:40],
            "delayed_replies": [row for row in proxy.snapshot().get("events", []) if row.get("action") == "delay_response"][:6],
        }
    finally:
        stop_worker(proc)
        proxy.stop()


async def _delivery_ttl() -> dict:
    flush()
    reset_events()
    env = base_env({"SIMPLE_Q_DEAD_TTL_SECS": "120", "SIMPLE_Q_MAX_DELIVERIES": "1"})
    log = Path("/tmp/sq130-ttl.log")
    proc = start_worker(env, log_path=log)
    try:
        message_id = await send(env, fail, {"token": "ttl"})
        wait_events("start", "ttl", 15)
        time.sleep(1.5)
        keys = redis_cli("KEYS", "delivery:*").splitlines()
        ttls = {key: redis_cli("TTL", key) for key in keys if key}
        return {"message_id": message_id, "keys": ttls, "dead_ttl_setting": 120}
    finally:
        stop_worker(proc)


async def _max_attempts() -> dict:
    flush()
    env = base_env()
    message_id = await send(env, nap, {"token": "attempts", "seconds": 30})
    time.sleep(0.3)
    blob = redis_cli("--scan", "--pattern", "*")
    found = []
    for key in blob.splitlines():
        if not key or "idempotency" in key:
            continue
        raw = redis_cli("HGETALL", key)
        if "max_attempts" in raw or (message_id and message_id in raw):
            found.append({"key": key, "hgetall": raw[:500]})
        if len(found) >= 4:
            break
    return {"message_id": message_id, "hits": found}


async def _retry_new_task() -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path("/tmp/sq130-retry.log")
    proc = start_worker(env, log_path=log)
    try:
        message_id = await send(env, once, {"token": "retry-new"})
        deadline = time.time() + 15
        while time.time() < deadline and len(wait_events("end", "retry-new", 0.1)) == 0:
            time.sleep(0.1)
        rows = [row for row in read_events() if row.get("token") == "retry-new" and row.get("kind") == "start"]
        return {
            "message_id": message_id,
            "starts": [
                {"retries": row.get("retries"), "deliveries": row.get("deliveries"), "worker": row.get("worker"), "pid": row.get("pid")}
                for row in rows
            ],
            "ended": bool([row for row in read_events() if row.get("token") == "retry-new" and row.get("kind") == "end"]),
        }
    finally:
        stop_worker(proc)


def _signal_times(path: Path, needle: str) -> list[dict]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if needle not in line:
            continue
        pid = None
        for part in line.replace(",", " ").split():
            if part.startswith("pid="):
                try:
                    pid = int(part.split("=", 1)[1])
                except ValueError:
                    pid = None
        rows.append({"line": line[:240], "pid": pid})
    return rows


async def _per_child_signals(kind: str) -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path(f"/tmp/sq130-{kind}.log")
    soft = "1048576" if kind == "soft" else None
    hard = "1048576" if kind == "hard" else None
    proc = start_worker(env, processes=3, soft=soft, hard=hard, log_path=log)
    try:
        time.sleep(3.5)
        needle = "draining child" if kind == "soft" else "killing child"
        rows = _signal_times(log, needle)
        pids = [row["pid"] for row in rows if row["pid"]]
        return {
            "kind": kind,
            "lines": len(rows),
            "distinct_pids": len(set(pids)),
            "pids": pids[:12],
            "sample": [row["line"] for row in rows[:8]],
        }
    finally:
        stop_worker(proc)


async def _soft_during_task() -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path("/tmp/sq130-soft-busy.log")
    proc = start_worker(env, soft="1048576", log_path=log)
    try:
        await send(env, nap, {"token": "soft-busy", "seconds": 6})
        ends = wait_events("end", "soft-busy", 20)
        drains = _signal_times(log, "draining child")
        started = [row for row in read_events() if row.get("token") == "soft-busy" and row.get("kind") == "start"]
        child_alive_at_end = None
        if ends:
            child_alive_at_end = Path(f"/proc/{ends[0]['pid']}").exists()
        exited_after = None
        if ends:
            deadline = time.time() + 8
            while time.time() < deadline and Path(f"/proc/{ends[0]['pid']}").exists():
                time.sleep(0.1)
            exited_after = not Path(f"/proc/{ends[0]['pid']}").exists()
        return {
            "handler_ended": bool(ends),
            "drain_lines": len(drains),
            "drain_before_end": bool(started and ends and drains),
            "child_alive_when_handler_ended": child_alive_at_end,
            "child_exited_after_handler": exited_after,
            "sample": [row["line"] for row in drains[:4]],
        }
    finally:
        stop_worker(proc)


async def _hard_ignores_busy() -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path("/tmp/sq130-hard-busy.log")
    proc = start_worker(env, hard="1048576", log_path=log)
    try:
        await send(env, nap, {"token": "hard-busy", "seconds": 8})
        wait_events("start", "hard-busy", 15)
        time.sleep(2.5)
        kills = _signal_times(log, "killing child")
        ends = [row for row in read_events() if row.get("token") == "hard-busy" and row.get("kind") == "end"]
        return {
            "kill_lines": len(kills),
            "handler_reached_end": bool(ends),
            "sample": [row["line"] for row in kills[:4]],
        }
    finally:
        stop_worker(proc)


async def _slot_after_kill() -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path("/tmp/sq130-slot.log")
    proc = start_worker(env, soft="1048576", hard="8388608", log_path=log)
    try:
        await send(env, alloc, {"token": "slot", "mb": 32, "seconds": 8})
        wait_events("start", "slot", 15)
        time.sleep(4)
        kills = _signal_times(log, "killing child")
        drains = _signal_times(log, "draining child")
        kill_pids = {row["pid"] for row in kills if row["pid"]}
        later_drains = [row for row in drains if row["pid"] and row["pid"] not in kill_pids]
        return {
            "kills": [row["pid"] for row in kills[:6]],
            "drains": [row["pid"] for row in drains[:8]],
            "new_child_drained": bool(later_drains),
            "sample": [row["line"] for row in (kills[:3] + drains[:4])],
        }
    finally:
        stop_worker(proc)


def _cgroup_ready() -> str | None:
    try:
        subprocess.run(["sudo", "mkdir", "-p", str(CGROUP)], check=True)
        subprocess.run(["sudo", "sh", "-c", f"echo {256 * 1024 * 1024} > {CGROUP}/memory.max"], check=True)
    except subprocess.CalledProcessError as exc:
        return f"cgroup memory.max не записывается: {exc}"
    return None


async def _cgroup_case(name: str, mb: int, processes: int, memory_max: int, hold_s: float) -> dict:
    reason = _cgroup_ready()
    if reason:
        return {"measured": False, "reason": reason}
    subprocess.run(["sudo", "sh", "-c", f"echo {memory_max} > {CGROUP}/memory.max"], check=False)
    swap = CGROUP / "memory.swap.max"
    if swap.exists():
        subprocess.run(["sudo", "sh", "-c", f"echo 0 > {swap}"], check=False)
    flush()
    reset_events()
    env = base_env()
    log = Path(f"/tmp/sq130-cgroup-{name}.log")
    proc = start_worker(env, processes=processes, log_path=log, cgroup=True)
    try:
        time.sleep(1.5)
        if not child_pids():
            return {
                "measured": False,
                "reason": "процесс не оказался в cgroup или worker не стартовал",
                "log": log_excerpt(log, 1500),
            }
        sends = []
        for index in range(processes):
            sends.append(await send(env, alloc, {"token": f"{name}-{index}", "mb": mb, "seconds": hold_s}))
        time.sleep(min(hold_s, 6))
        current = (CGROUP / "memory.current").read_text().strip()
        limit = (CGROUP / "memory.max").read_text().strip()
        rss = {pid: rss_of(pid) for pid in child_pids()}
        return {
            "measured": True,
            "memory_current": int(current),
            "memory_max": int(limit) if limit.isdigit() else limit,
            "child_rss": rss,
            "soft_lines": [row["line"] for row in _signal_times(log, "soft threshold")[:6]],
            "hard_lines": [row["line"] for row in _signal_times(log, "hard threshold")[:6]],
            "drains": [row["pid"] for row in _signal_times(log, "draining child")[:8]],
            "kills": [row["pid"] for row in _signal_times(log, "killing child")[:8]],
            "ids": sends,
        }
    finally:
        stop_worker(proc)


def _undelivered() -> dict:
    source = Path("/tmp/sq130_kill.c")
    source.write_text(
        """#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
static int left = 1;
int kill(pid_t pid, int sig) {
    static int (*real_kill)(pid_t, int);
    if (!real_kill) real_kill = (int (*)(pid_t, int))dlsym(RTLD_NEXT, "kill");
    if (sig == SIGUSR1 && left > 0) { left--; errno = EPERM; return -1; }
    return real_kill(pid, sig);
}
""",
        encoding="utf-8",
    )
    lib = Path("/tmp/sq130_kill.so")
    built = subprocess.run(["gcc", "-shared", "-fPIC", "-o", str(lib), str(source)], check=False, capture_output=True, text=True)
    if built.returncode != 0:
        return {"measured": False, "reason": "gcc не собрал перехват kill", "stderr": built.stderr[-400:]}
    flush()
    reset_events()
    env = base_env({"LD_PRELOAD": str(lib)})
    log = Path("/tmp/sq130-undelivered.log")
    proc = start_worker(env, soft="1048576", log_path=log)
    try:
        time.sleep(3.5)
    finally:
        stop_worker(proc)
    failed = _signal_times(log, "failed to signal")
    drains = _signal_times(log, "draining child")
    return {
        "measured": True,
        "failed_lines": len(failed),
        "drain_lines": len(drains),
        "failed_pid": failed[0]["pid"] if failed else None,
        "drain_pids": [row["pid"] for row in drains[:4]],
        "same_pid_retried": bool(failed and drains and failed[0]["pid"] in [row["pid"] for row in drains]),
        "sample": [row["line"] for row in (failed[:2] + drains[:2])],
    }


async def _reply_loss(kind: str, mode: str) -> dict:
    flush()
    reset_events()
    proxy = FaultProxy(PROXY_PORT, PORT)
    proxy.start()
    env = base_env(
        {"SIMPLE_Q_HEARTBEAT_SECS": "2", "SIMPLE_Q_MISSED_HEARTBEATS": "2"},
        url=f"redis://127.0.0.1:{PROXY_PORT}/0",
    )
    log = Path(f"/tmp/sq130-loss-{kind}-{mode}.log")
    proc = start_worker(env, log_path=log)
    try:
        await send(env, nap, {"token": f"loss-{kind}", "seconds": 4})
        wait_events("start", f"loss-{kind}", 15)
        before = child_pids()
        proxy.arm(kind, mode, once=True)
        time.sleep(6)
        ends = wait_events("end", f"loss-{kind}", 20)
        after = child_pids()
        return {
            "kind": kind,
            "mode": mode,
            "handler_ended": bool(ends),
            "starts": len([row for row in read_events() if row.get("token") == f"loss-{kind}" and row.get("kind") == "start"]),
            "pids_before": before,
            "pids_after": after,
            "same_child_pid": bool(set(before) & set(after)),
            "proxy": [row for row in proxy.snapshot().get("events", []) if row.get("action") in {"drop_response", "reset", "drop_request"}][:8],
        }
    finally:
        stop_worker(proc)
        proxy.stop()


async def _fetch_lost() -> dict:
    flush()
    reset_events()
    proxy = FaultProxy(PROXY_PORT, PORT)
    proxy.start()
    env = base_env(
        {"SIMPLE_Q_HEARTBEAT_SECS": "2", "SIMPLE_Q_MISSED_HEARTBEATS": "2"},
        url=f"redis://127.0.0.1:{PROXY_PORT}/0",
    )
    log = Path("/tmp/sq130-fetch-lost.log")
    proc = start_worker(env, processes=1, log_path=log)
    try:
        time.sleep(1)
        proxy.arm("fetch", "drop_response", once=True, require_payload=True)
        message_id = await send(env, nap, {"token": "fetch-lost", "seconds": 0.2})
        starts = []
        deadline = time.time() + 20
        while time.time() < deadline:
            starts = [row for row in read_events() if row.get("token") == "fetch-lost" and row.get("kind") == "start"]
            if starts:
                break
            time.sleep(0.2)
        names = []
        for line in interesting_lines(log):
            if "agent-worker" in line and line not in names:
                names.append(line[:240])
        return {
            "message_id": message_id,
            "starts": len(starts),
            "workers": [row.get("worker") for row in starts],
            "pids": [row.get("pid") for row in starts],
            "child_pids": child_pids(),
            "proxy": [row for row in proxy.snapshot().get("events", []) if row.get("action") == "drop_response"][:4],
            "log_sample": names[:8],
        }
    finally:
        stop_worker(proc)
        proxy.stop()


async def _gap_double() -> dict:
    flush()
    reset_events()
    proxy = FaultProxy(PROXY_PORT, PORT)
    proxy.start()
    env = base_env(
        {"SIMPLE_Q_HEARTBEAT_SECS": "2", "SIMPLE_Q_MISSED_HEARTBEATS": "2"},
        url=f"redis://127.0.0.1:{PROXY_PORT}/0",
    )
    log = Path("/tmp/sq130-gap.log")
    proc = start_worker(env, processes=2, log_path=log)
    try:
        await send(env, nap, {"token": "gap", "seconds": 12})
        wait_events("start", "gap", 15)
        proxy.blackhole(8)
        time.sleep(9)
        deadline = time.time() + 20
        while time.time() < deadline:
            if [row for row in read_events() if row.get("token") == "gap" and row.get("kind") == "end"]:
                break
            time.sleep(0.2)
        starts = [row for row in read_events() if row.get("token") == "gap" and row.get("kind") == "start"]
        ends = [row for row in read_events() if row.get("token") == "gap" and row.get("kind") == "end"]
        return {
            "orphan_window_s": 4,
            "blackhole_s": 8,
            "starts": len(starts),
            "ends": len(ends),
            "workers": [row.get("worker") for row in starts],
            "pids": [row.get("pid") for row in starts],
            "start_t": [round(row["t"], 3) for row in starts],
        }
    finally:
        stop_worker(proc)
        proxy.stop()


async def _ack_blackhole() -> dict:
    flush()
    reset_events()
    proxy = FaultProxy(PROXY_PORT, PORT)
    proxy.start()
    env = base_env(url=f"redis://127.0.0.1:{PROXY_PORT}/0")
    log = Path("/tmp/sq130-ack-hole.log")
    proc = start_worker(env, log_path=log)
    try:
        await send(env, nap, {"token": "ack-hole", "seconds": 1})
        wait_events("start", "ack-hole", 15)
        proxy.blackhole(8)
        ends = wait_events("end", "ack-hole", 5)
        time.sleep(9)
        done = redis_cli("ZCARD", "bench:done")
        return {
            "handler_ended_during_blackhole": bool(ends),
            "starts": len([row for row in read_events() if row.get("token") == "ack-hole" and row.get("kind") == "start"]),
            "done_card_after": done,
            "child_still_up": bool(child_pids()),
        }
    finally:
        stop_worker(proc)
        proxy.stop()


async def _supervisor_kill() -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path("/tmp/sq130-parent.log")
    proc = start_worker(env, log_path=log)
    try:
        await send(env, nap, {"token": "parent", "seconds": 5})
        started = wait_events("start", "parent", 15)
        children = child_pids()
        t_kill = time.time()
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=5)
        ends = wait_events("end", "parent", 15)
        gone = None
        deadline = time.time() + 15
        while time.time() < deadline:
            if children and all(not Path(f"/proc/{pid}").exists() for pid in children):
                gone = time.time()
                break
            time.sleep(0.05)
        second = await send(env, nap, {"token": "parent-2", "seconds": 0.2})
        second_start = wait_events("start", "parent-2", 3)
        return {
            "children": children,
            "handler_ended": bool(ends),
            "child_exit_s": None if gone is None else round(gone - t_kill, 3),
            "second_id": second,
            "second_fetched": bool(second_start),
            "log": [row["line"] for row in _signal_times(log, "supervisor exited")[:4]],
            "started": bool(started),
        }
    finally:
        _kill_app()


async def _prom_delayed() -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path("/tmp/sq130-prom.log")
    proc = start_worker(env, log_path=log)
    try:
        await send(env, nap, {"token": "delayed", "seconds": 0.2}, delay=3000)
        time.sleep(1)
        body = ""
        try:
            import urllib.request

            with urllib.request.urlopen("http://127.0.0.1:9193/metrics", timeout=3) as response:
                body = response.read().decode(errors="replace")
        except Exception as exc:  # noqa: BLE001
            return {"measured": False, "reason": str(exc)}
        lines = [line for line in body.splitlines() if line.startswith("dramatiq_delayed_messages_inprogress")]
        return {"lines": lines, "all_zero": all(line.split()[-1] in {"0", "0.0"} for line in lines if not line.startswith("#"))}
    finally:
        stop_worker(proc)


async def _admin_shapes() -> dict:
    flush()
    reset_events()
    env = base_env()
    log = Path("/tmp/sq130-admin.log")
    proc = start_worker(env, log_path=log)
    try:
        await send(env, nap, {"token": "admin", "seconds": 0.2})
        wait_events("end", "admin", 15)
        client = Client(env["REDIS_URL"], Config.from_env())
        admin = QueueAdmin(client)
        snapshot = await admin.snapshot(["bench"])
        dead = await admin.dead("bench", limit=5)
        page = await admin.peek("bench", "active", limit=5)
        dispatch = [row for row in read_events() if row.get("kind") == "dispatch"]
        return {
            "snapshot_type": type(snapshot).__name__,
            "dead_type": type(dead).__name__,
            "page_type": type(page).__name__,
            "types_match": isinstance(snapshot, Snapshot) and isinstance(dead, DeadPage) and isinstance(page, TaskPage),
            "dispatch_is_dict": [row.get("is_dict") for row in dispatch],
            "dispatch_type": [row.get("message_type") for row in dispatch],
            "queues": [item.name for item in snapshot.queues],
        }
    finally:
        stop_worker(proc)


def _kind() -> dict:
    docker = subprocess.run(["bash", "-lc", "command -v docker"], capture_output=True, text=True)
    kind = subprocess.run(["bash", "-lc", "command -v kind"], capture_output=True, text=True)
    return {
        "measured": False,
        "reason": "docker и kind в машине агента не установлены",
        "docker": bool(docker.stdout.strip()),
        "kind": bool(kind.stdout.strip()),
    }


def redact(payload: dict) -> dict:
    secrets = [os.path.expanduser("~"), socket.gethostname(), socket.getfqdn(), os.environ.get("USER", ""), os.environ.get("LOGNAME", "")]
    secrets = [item for item in secrets if item and item not in {".", "/"}]
    secrets.sort(key=len, reverse=True)
    text = json.dumps(payload, ensure_ascii=False)
    for secret in secrets:
        text = text.replace(secret, "<redacted>")
    return json.loads(text)


def run_async(func) -> dict:
    return asyncio.run(func())


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    ensure_redis(PORT, REDIS_DIR)
    checks: list[dict] = []

    def add(name: str, func) -> None:
        started = time.time()
        print(f"check {name}", flush=True)
        try:
            result = func()
            checks.append({"id": name, "measured": result.get("measured", True) if isinstance(result, dict) else True, "duration_s": round(time.time() - started, 3), "result": result})
        except Exception as exc:  # noqa: BLE001
            checks.append({"id": name, "measured": False, "duration_s": round(time.time() - started, 3), "error": f"{type(exc).__name__}: {exc}"})
        print(f"  done {name}", flush=True)

    add("defaults", check_defaults)
    add("unique_release", lambda: run_async(_unique_release))
    add("unique_stop", lambda: run_async(_unique_stop))
    add("unique_redis_error", lambda: run_async(_unique_redis_error))
    add("slow_ack", lambda: run_async(_slow_ack))
    add("delivery_ttl", lambda: run_async(_delivery_ttl))
    add("max_attempts_stored", lambda: run_async(_max_attempts))
    add("retry_new_enqueue", lambda: run_async(_retry_new_task))
    add("per_child_soft", lambda: run_async(lambda: _per_child_signals("soft")))
    add("per_child_hard", lambda: run_async(lambda: _per_child_signals("hard")))
    add("soft_during_delivery", lambda: run_async(_soft_during_task))
    add("hard_ignores_busy", lambda: run_async(_hard_ignores_busy))
    add("slot_after_sigkill", lambda: run_async(_slot_after_kill))
    add("undelivered_sigusr1", _undelivered)
    add("cgroup_soft_80", lambda: run_async(lambda: _cgroup_case("soft80", 160, 1, 220 * 1024 * 1024, 8)))
    add("cgroup_hard_95_half", lambda: run_async(lambda: _cgroup_case("hard95", 180, 1, 220 * 1024 * 1024, 8)))
    add("cgroup_95_below_half", lambda: run_async(lambda: _cgroup_case("below", 40, 3, 180 * 1024 * 1024, 8)))
    add("reply_heartbeat", lambda: run_async(lambda: _reply_loss("renew", "drop_response")))
    add("reply_ack", lambda: run_async(lambda: _reply_loss("finish", "drop_response")))
    add("broken_pipe", lambda: run_async(lambda: _reply_loss("keepalive", "reset")))
    add("fetch_lost", lambda: run_async(_fetch_lost))
    add("gap_over_orphan", lambda: run_async(_gap_double))
    add("ack_during_blackhole", lambda: run_async(_ack_blackhole))
    add("supervisor_sigkill", lambda: run_async(_supervisor_kill))
    add("prom_delayed", lambda: run_async(_prom_delayed))
    add("admin_shapes", lambda: run_async(_admin_shapes))
    add("kind_rollout", _kind)
    report = {
        "library": "simpleq",
        "version": "1.3.0",
        "scenario": "mechanisms-1.3.0",
        "redis_port": PORT,
        "checks": checks,
    }
    OUT.write_text(json.dumps(redact(report), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
