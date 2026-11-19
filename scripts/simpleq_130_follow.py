"""Допроверки механизмов, которые первый прогон смешал с чужими pid или не довёл.

Redis 6430. Каталог результатов — results/simpleq-1.3.0/mechanisms-follow.json.
"""

from __future__ import annotations

import asyncio
import json
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path("/workspace")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import simpleq_130_checks as base  # noqa: E402
from bench.simpleq_130_app import alloc, nap  # noqa: E402
from redis_fault_proxy import FaultProxy  # noqa: E402

OUT = Path("/workspace/results/simpleq-1.3.0/mechanisms-follow.json")
CGROUP = Path("/sys/fs/cgroup/sq130mem")


def redact(payload: dict) -> dict:
    return base.redact(payload)


def own_children(proc: subprocess.Popen) -> list[int]:
    """Дети именно этого supervisor, не чужие agent-worker."""
    try:
        text = Path(f"/proc/{proc.pid}/task/{proc.pid}/children").read_text()
    except OSError:
        text = ""
    pids = []
    for part in text.split():
        try:
            pid = int(part)
        except ValueError:
            continue
        cmd = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").decode(errors="replace") if Path(f"/proc/{pid}/cmdline").exists() else ""
        if "worker --child" in cmd or "agent-worker" in cmd:
            pids.append(pid)
    # sudo: proc.pid is sudo, the worker is a descendant
    if not pids:
        proc_ps = subprocess.run(["ps", "-eo", "pid,ppid,args"], capture_output=True, text=True, check=False)
        by_ppid: dict[int, list[int]] = {}
        args: dict[int, str] = {}
        for line in proc_ps.stdout.splitlines()[1:]:
            bits = line.split(None, 2)
            if len(bits) < 3:
                continue
            pid, ppid = int(bits[0]), int(bits[1])
            by_ppid.setdefault(ppid, []).append(pid)
            args[pid] = bits[2]
        stack = [proc.pid]
        seen = set()
        while stack:
            current = stack.pop()
            for child in by_ppid.get(current, []):
                if child in seen:
                    continue
                seen.add(child)
                stack.append(child)
                if "worker --child" in args.get(child, ""):
                    pids.append(child)
    return pids


def cgroup_of(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8").strip()
    except OSError as exc:
        return f"unreadable:{exc}"


async def _ratios() -> dict:
    base.flush()
    base.reset_events()
    subprocess.run(["sudo", "mkdir", "-p", str(CGROUP)], check=False)
    subprocess.run(["sudo", "sh", "-c", f"echo {1024 * 1024 * 1024} > {CGROUP}/memory.max"], check=False)
    swap = CGROUP / "memory.swap.max"
    if swap.exists():
        subprocess.run(["sudo", "sh", "-c", f"echo 0 > {swap}"], check=False)
    launcher = Path("/tmp/sq130-cgroup.sh")
    launcher.write_text(
        "#!/bin/bash\nset -eu\necho $$ > /sys/fs/cgroup/sq130mem/cgroup.procs\nexec \"$@\"\n",
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    env = base.base_env()
    log = Path("/tmp/sq130-follow-cgroup.log")
    handle = open(log, "w", encoding="utf-8")
    pairs = [f"{key}={env[key]}" for key in ("REDIS_URL", "PYTHONPATH", "PYTHONUNBUFFERED", "SQ130_EVENTS", "DRAMATIQ_PROM_PORT") if key in env]
    cmd = [
        "sudo",
        "env",
        *pairs,
        "bash",
        str(launcher),
        "stdbuf",
        "-oL",
        "-eL",
        base.WORKER,
        "worker",
        "--app",
        "bench.simpleq_130_app:create_app",
        "--factory",
        "--queues",
        "bench",
        "--processes",
        "3",
        "--concurrency",
        "1",
        "--max-tasks-per-process",
        "20",
    ]
    proc = subprocess.Popen(cmd, cwd="/workspace", stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        time.sleep(1.5)
        kids = own_children(proc)
        if not kids:
            return {"measured": False, "reason": "дети не стартовали", "log": base.log_excerpt(log, 800)}
        joined = all("sq130mem" in cgroup_of(pid) for pid in kids)
        if not joined:
            return {
                "measured": False,
                "reason": "процесс не в cgroup sq130mem",
                "cgroups": {pid: cgroup_of(pid) for pid in kids},
            }
        await base.send(env, alloc, {"token": "big", "mb": 80, "seconds": 12})
        await base.send(env, alloc, {"token": "small-a", "mb": 20, "seconds": 12})
        await base.send(env, alloc, {"token": "small-b", "mb": 20, "seconds": 12})
        deadline = time.time() + 8
        while time.time() < deadline and len([row for row in base.read_events() if row.get("kind") == "allocated"]) < 1:
            time.sleep(0.1)
        time.sleep(0.8)
        current = int((CGROUP / "memory.current").read_text().strip())
        rss = {pid: base.rss_of(pid) for pid in own_children(proc)}
        # 95% сразу под текущий working set, чтобы порог был уже пройден.
        hard_max = max(current + 1_048_576, int(current / 0.96))
        subprocess.run(["sudo", "sh", "-c", f"echo {hard_max} > {CGROUP}/memory.max"], check=False)
        time.sleep(2.5)
        hard_lines = base._signal_times(log, "hard threshold")
        soft_lines = base._signal_times(log, "soft threshold")
        kills = base._signal_times(log, "killing child")
        # затем мягкий порог на новом лимите выше текущего, если процесс ещё жив
        return {
            "measured": True,
            "joined_cgroup": joined,
            "rss_before_limit": rss,
            "memory_current_before": current,
            "memory_max_for_95": hard_max,
            "hard_lines": [row["line"] for row in hard_lines[:6]],
            "soft_lines": [row["line"] for row in soft_lines[:6]],
            "kills": [row["pid"] for row in kills[:6]],
            "allocated": [row.get("token") for row in base.read_events() if row.get("kind") == "allocated"],
        }
    finally:
        base.stop_worker(proc)


async def _soft_busy() -> dict:
    base.flush()
    base.reset_events()
    env = base.base_env()
    log = Path("/tmp/sq130-follow-soft.log")
    proc = base.start_worker(env, soft="1048576", log_path=log)
    try:
        t0 = time.time()
        await base.send(env, nap, {"token": "soft2", "seconds": 6})
        ends = base.wait_events("end", "soft2", 20)
        time.sleep(2.5)
        log.flush() if False else None
        text = log.read_text(encoding="utf-8", errors="replace") if log.exists() else ""
        return {
            "handler_ended": bool(ends),
            "end_after_send_s": None if not ends else round(ends[0]["t"] - t0, 3),
            "drain_lines": [line[:240] for line in text.splitlines() if "draining child" in line][:4],
            "paused_lines": [line[:240] for line in text.splitlines() if "paused" in line][:4],
        }
    finally:
        base.stop_worker(proc)


async def _slot() -> dict:
    base.flush()
    base.reset_events()
    env = base.base_env()
    log = Path("/tmp/sq130-follow-slot.log")
    # hard выше простоя (~30 МБ) и ниже alloc 64 МБ; soft ниже простоя.
    proc = base.start_worker(env, soft="20971520", hard="52428800", log_path=log)
    try:
        await base.send(env, alloc, {"token": "slot2", "mb": 48, "seconds": 8})
        base.wait_events("start", "slot2", 15)
        time.sleep(4)
        text = log.read_text(encoding="utf-8", errors="replace") if log.exists() else ""
        return {
            "lines": [line[:240] for line in text.splitlines() if "killing child" in line or "draining child" in line][:12],
        }
    finally:
        base.stop_worker(proc)


async def _slow_ack() -> dict:
    base.flush()
    base.reset_events()
    proxy = FaultProxy(base.PROXY_PORT, base.PORT)
    proxy.start()
    proxy.arm("finish", "delay_response", delay_s=3, once=True)
    env = base.base_env(url=f"redis://127.0.0.1:{base.PROXY_PORT}/0")
    log = Path("/tmp/sq130-follow-ack.log")
    proc = base.start_worker(env, log_path=log)
    key = "bench:idempotency:item:slow2"
    try:
        await base.send(env, nap, {"token": "slow2", "seconds": 0.3}, unique_key="item:slow2")
        started = time.time()
        samples = []
        deadline = time.time() + 12
        while time.time() < deadline:
            samples.append({"t": round(time.time() - started, 3), "exists": base.redis_cli("EXISTS", key)})
            if len(samples) > 3 and samples[-1]["exists"] == "0":
                break
            time.sleep(0.25)
        return {
            "saw_key": any(row["exists"] == "1" for row in samples),
            "released": samples[-1]["exists"] == "0" if samples else False,
            "release_t": next((row["t"] for row in samples if row["exists"] == "0"), None),
            "delayed": len([row for row in proxy.snapshot().get("events", []) if row.get("action") == "delay_response"]),
            "samples_head": samples[:6],
            "samples_tail": samples[-4:],
        }
    finally:
        base.stop_worker(proc)
        proxy.stop()


async def _parent() -> dict:
    base.flush()
    base.reset_events()
    env = base.base_env()
    log = Path("/tmp/sq130-follow-parent.log")
    proc = base.start_worker(env, log_path=log)
    try:
        await base.send(env, nap, {"token": "parent2", "seconds": 4})
        base.wait_events("start", "parent2", 15)
        kids = own_children(proc)
        # supervisor может быть ребёнком sudo; берём pid из лога, если свой список пуст
        t_kill = time.time()
        os.kill(proc.pid, signal.SIGKILL)
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass
        ends = base.wait_events("end", "parent2", 12)
        gone = None
        deadline = time.time() + 12
        while time.time() < deadline:
            if kids and all(not Path(f"/proc/{pid}").exists() for pid in kids):
                gone = time.time()
                break
            time.sleep(0.05)
        second = await base.send(env, nap, {"token": "parent2b", "seconds": 0.2})
        fetched = base.wait_events("start", "parent2b", 2)
        text = log.read_text(encoding="utf-8", errors="replace") if log.exists() else ""
        return {
            "children": kids,
            "handler_ended": bool(ends),
            "child_exit_s": None if gone is None else round(gone - t_kill, 3),
            "second_id": second,
            "second_fetched": bool(fetched),
            "supervisor_line": [line[:240] for line in text.splitlines() if "supervisor exited" in line][:2],
        }
    finally:
        base._kill_app()


async def _prom() -> dict:
    base.flush()
    base.reset_events()
    env = base.base_env()
    log = Path("/tmp/sq130-follow-prom.log")
    proc = base.start_worker(env, log_path=log)
    try:
        await base.send(env, nap, {"token": "prom2", "seconds": 0.2}, delay=4000)
        time.sleep(1.2)
        import urllib.request

        with urllib.request.urlopen("http://127.0.0.1:9193/metrics", timeout=3) as response:
            body = response.read().decode(errors="replace")
        delayed = [line for line in body.splitlines() if "delayed_messages" in line]
        inprog = [line for line in body.splitlines() if "messages_inprogress" in line and not line.startswith("#")]
        return {"delayed_lines": delayed, "inprogress_lines": inprog[:8], "body_len": len(body)}
    finally:
        base.stop_worker(proc)


def _from_logs() -> dict:
    fetch = Path("/tmp/sq130-fetch-lost.log")
    soft = Path("/tmp/sq130-soft-busy.log")
    gap = Path("/tmp/sq130-gap.log")

    def lines(path: Path, needle: str) -> list[str]:
        if not path.exists():
            return []
        return [line[:240] for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if needle in line][:8]

    return {
        "fetch_lost_log": lines(fetch, "Worker ") + lines(fetch, "running actor") + lines(fetch, "BackendError"),
        "soft_busy_log": lines(soft, "running actor") + lines(soft, "draining child") + lines(soft, "recycled"),
        "gap_running": lines(gap, "running actor"),
    }


def main() -> None:
    base.ensure_redis(base.PORT, base.REDIS_DIR)
    checks = []

    def add(name, func):
        print(f"follow {name}", flush=True)
        started = time.time()
        try:
            result = func()
            checks.append({"id": name, "measured": True if not isinstance(result, dict) else result.get("measured", True), "duration_s": round(time.time() - started, 3), "result": result})
        except Exception as exc:  # noqa: BLE001
            checks.append({"id": name, "measured": False, "duration_s": round(time.time() - started, 3), "error": f"{type(exc).__name__}: {exc}"})
        print(f"  done {name}", flush=True)

    add("log_reread", _from_logs)
    add("soft_during_delivery", lambda: asyncio.run(_soft_busy()))
    add("slot_after_sigkill", lambda: asyncio.run(_slot()))
    add("slow_ack_once", lambda: asyncio.run(_slow_ack()))
    add("cgroup_ratios", lambda: asyncio.run(_ratios()))
    add("supervisor_sigkill", lambda: asyncio.run(_parent()))
    add("prom_delayed", lambda: asyncio.run(_prom()))
    report = {"library": "simpleq", "version": "1.3.0", "scenario": "mechanisms-follow", "checks": checks}
    OUT.write_text(json.dumps(redact(report), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
