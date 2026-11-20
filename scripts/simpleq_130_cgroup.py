"""Пороги cgroup, смерть supervisor и метрика delayed. Redis 6432."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path("/workspace")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import simpleq_130_checks as base  # noqa: E402
from bench.simpleq_130_app import alloc, nap  # noqa: E402

PORT = 6432
REDIS_DIR = "/workspace/results/long/simpleq-1.3.0/redis-6432"
OUT = Path("/workspace/results/simpleq-1.3.0/mechanisms-cgroup.json")
CGROUP = Path("/sys/fs/cgroup/sq130b")


def prepare(limit: int) -> None:
    subprocess.run(["sudo", "mkdir", "-p", str(CGROUP)], check=True)
    subprocess.run(["sudo", "sh", "-c", f"echo {limit} > {CGROUP}/memory.max"], check=True)
    if (CGROUP / "memory.swap.max").exists():
        subprocess.run(["sudo", "sh", "-c", f"echo 0 > {CGROUP}/memory.swap.max"], check=False)


def start(processes: int, log: Path, soft: str | None = None, hard: str | None = None) -> subprocess.Popen:
    launcher = Path("/tmp/sq130b.sh")
    launcher.write_text("#!/bin/bash\nset -eu\necho $$ > /sys/fs/cgroup/sq130b/cgroup.procs\nexec \"$@\"\n", encoding="utf-8")
    launcher.chmod(0o755)
    env = base.base_env()
    env["REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
    env["DRAMATIQ_PROM_PORT"] = "9194"
    pairs = [f"{key}={env[key]}" for key in ("REDIS_URL", "PYTHONPATH", "PYTHONUNBUFFERED", "SQ130_EVENTS", "SQ130_ONCE", "DRAMATIQ_PROM_PORT")]
    cmd = ["sudo", "env", *pairs, "bash", str(launcher), "stdbuf", "-oL", "-eL", base.WORKER, "worker", "--app", "bench.simpleq_130_app:create_app", "--factory", "--queues", "bench", "--processes", str(processes), "--concurrency", "1", "--max-tasks-per-process", "10"]
    if soft:
        cmd += ["--child-memory-soft", soft]
    if hard:
        cmd += ["--child-memory-hard", hard]
    handle = open(log, "w", encoding="utf-8")
    return subprocess.Popen(cmd, cwd=str(ROOT), stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)


def lines(path: Path, needle: str) -> list[str]:
    if not path.exists():
        return []
    return [line[:300] for line in path.read_text(encoding="utf-8", errors="replace").splitlines() if needle in line]


async def wait_alloc(n: int, timeout: float) -> list[dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = [row for row in base.read_events() if row.get("kind") == "allocated"]
        if len(rows) >= n:
            return rows
        time.sleep(0.1)
    return [row for row in base.read_events() if row.get("kind") in {"start", "allocated", "end"}]


def snapshot() -> dict:
    current = int((CGROUP / "memory.current").read_text().strip())
    limit = (CGROUP / "memory.max").read_text().strip()
    return {"current": current, "max": int(limit) if limit.isdigit() else limit}


async def case_half() -> dict:
    """Один ребёнок держит большую часть working set, порог 95%."""
    prepare(1024 * 1024 * 1024)
    base.flush_owned(PORT, REDIS_DIR)
    base.reset_events()
    log = Path("/tmp/sq130b-half.log")
    proc = start(1, log)
    try:
        env = base.base_env()
        env["REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
        await base.send(env, alloc, {"token": "half", "mb": 70, "seconds": 20})
        rows = await wait_alloc(1, 15)
        time.sleep(0.4)
        before = snapshot()
        # current / max >= 0.95
        new_max = max(before["current"] + 1024, int(before["current"] / 0.96))
        subprocess.run(["sudo", "sh", "-c", f"echo {new_max} > {CGROUP}/memory.max"], check=True)
        time.sleep(2.4)
        return {
            "before": before,
            "max_set": new_max,
            "events": rows,
            "hard": lines(log, "hard threshold"),
            "kills": lines(log, "killing child"),
            "drains": lines(log, "draining child"),
        }
    finally:
        base.stop_worker(proc)


async def case_soft() -> dict:
    """Два ребёнка разного размера на 80–95%."""
    prepare(1024 * 1024 * 1024)
    base.flush_owned(PORT, REDIS_DIR)
    base.reset_events()
    log = Path("/tmp/sq130b-soft.log")
    proc = start(2, log)
    try:
        env = base.base_env()
        env["REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
        await base.send(env, alloc, {"token": "big", "mb": 60, "seconds": 20})
        await base.send(env, alloc, {"token": "small", "mb": 16, "seconds": 20})
        rows = await wait_alloc(2, 15)
        time.sleep(0.4)
        before = snapshot()
        # 80% <= current/max < 95%
        new_max = int(before["current"] / 0.85)
        subprocess.run(["sudo", "sh", "-c", f"echo {new_max} > {CGROUP}/memory.max"], check=True)
        time.sleep(3.2)
        return {
            "before": before,
            "max_set": new_max,
            "events": rows,
            "soft": lines(log, "soft threshold"),
            "hard": lines(log, "hard threshold"),
            "drains": lines(log, "draining child"),
            "kills": lines(log, "killing child"),
        }
    finally:
        base.stop_worker(proc)


async def case_no_second() -> dict:
    """Свой hard уже убивает самого большого. Второго за 95% быть не должно."""
    prepare(1024 * 1024 * 1024)
    base.flush_owned(PORT, REDIS_DIR)
    base.reset_events()
    log = Path("/tmp/sq130b-second.log")
    proc = start(2, log, hard="46000000")
    try:
        env = base.base_env()
        env["REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
        await base.send(env, alloc, {"token": "over", "mb": 40, "seconds": 20})
        await base.send(env, alloc, {"token": "under", "mb": 8, "seconds": 20})
        await wait_alloc(1, 12)
        time.sleep(0.3)
        before = snapshot()
        new_max = max(before["current"] + 1024, int(before["current"] / 0.96))
        subprocess.run(["sudo", "sh", "-c", f"echo {new_max} > {CGROUP}/memory.max"], check=True)
        time.sleep(2.5)
        return {
            "before": before,
            "max_set": new_max,
            "kills": lines(log, "killing child"),
            "hard": lines(log, "hard threshold"),
            "largest": lines(log, "largest child"),
        }
    finally:
        base.stop_worker(proc)


async def case_parent() -> dict:
    base.flush_owned(PORT, REDIS_DIR)
    base.reset_events()
    log = Path("/tmp/sq130b-parent.log")
    # без cgroup: обычный старт
    env = base.base_env()
    env["REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
    env["DRAMATIQ_PROM_PORT"] = "9195"
    proc = base.start_worker(env, log_path=log)
    try:
        await base.send(env, nap, {"token": "sup", "seconds": 4})
        base.wait_events("start", "sup", 15)
        kids = []
        for pid in base.child_pids():
            try:
                cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
            except OSError:
                continue
            if b"simpleq_130_app" in cmd:
                kids.append(pid)
        t0 = time.time()
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=3)
        ends = base.wait_events("end", "sup", 12)
        gone = None
        deadline = time.time() + 10
        while time.time() < deadline:
            if kids and all(not Path(f"/proc/{pid}").exists() for pid in kids):
                gone = time.time()
                break
            time.sleep(0.05)
        second = await base.send(env, nap, {"token": "sup2", "seconds": 0.2})
        fetched = base.wait_events("start", "sup2", 2)
        return {
            "children": kids,
            "handler_ended": bool(ends),
            "child_exit_s": None if gone is None else round(gone - t0, 3),
            "second_fetched": bool(fetched),
            "second_id": second,
            "line": lines(log, "supervisor exited"),
        }
    finally:
        base._kill_app()


async def case_prom() -> dict:
    base.flush_owned(PORT, REDIS_DIR)
    base.reset_events()
    log = Path("/tmp/sq130b-prom.log")
    env = base.base_env()
    env["REDIS_URL"] = f"redis://127.0.0.1:{PORT}/0"
    env["DRAMATIQ_PROM_PORT"] = "9194"
    proc = base.start_worker(env, log_path=log)
    try:
        await base.send(env, nap, {"token": "now", "seconds": 0.1})
        base.wait_events("end", "now", 15)
        await base.send(env, nap, {"token": "later", "seconds": 0.1}, delay=5000)
        time.sleep(1)
        with urllib.request.urlopen("http://127.0.0.1:9194/metrics", timeout=3) as response:
            body = response.read().decode(errors="replace")
        delayed = [line for line in body.splitlines() if "delayed" in line]
        return {"body": body[:2000], "delayed_lines": delayed, "has_name": "dramatiq_delayed_messages_inprogress" in body}
    finally:
        base.stop_worker(proc)


def main() -> None:
    from bench.longrun.stand import ensure_redis

    ensure_redis(PORT, REDIS_DIR)
    checks = []

    def add(name, coro):
        print("cgroup", name, flush=True)
        started = time.time()
        try:
            result = asyncio.run(coro())
            checks.append({"id": name, "measured": True, "duration_s": round(time.time() - started, 3), "result": result})
        except Exception as exc:  # noqa: BLE001
            checks.append({"id": name, "measured": False, "duration_s": round(time.time() - started, 3), "error": f"{type(exc).__name__}: {exc}"})
        print("  done", name, flush=True)

    add("cgroup_95_half", case_half)
    add("cgroup_80", case_soft)
    add("cgroup_95_no_second_kill", case_no_second)
    add("supervisor_sigkill", case_parent)
    add("prom_delayed", case_prom)
    OUT.write_text(json.dumps(base.redact({"library": "simpleq", "version": "1.3.0", "scenario": "mechanisms-cgroup", "checks": checks}), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
