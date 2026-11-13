#!/usr/bin/env python3
"""Burst stress of the fast task. Not a 2400-second scored run.

Redis is 127.0.0.1:6412 under results/long/stress-redis-6412.
Ports 6379 and 6380 are not opened or flushed.
simple-q runs from .venv-sq120 (1.2.0). The other libraries use the
existing bench virtualenvs.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("/workspace")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from bench.longrun.stand import ensure_redis, flush_owned, shutdown_save  # noqa: E402
from bench.workers import command, python_for  # noqa: E402
from long_run import _stop_group  # noqa: E402

REDIS_PORT = 6412
REDIS_DIR = ROOT / "results" / "long" / "stress-redis-6412"
STRESS_ROOT = ROOT / "results" / "long" / "stress"
SQ_PY = ROOT / ".venv-sq120" / "bin"
COUNT_DEFAULT = 5000
SLOTS = 8


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _python(lib: str) -> str:
    if lib == "simpleq":
        return str(SQ_PY / "python")
    return python_for(lib)


def _command(lib: str) -> list[str]:
    if lib == "simpleq":
        return [
            str(SQ_PY / "agent-worker"),
            "worker",
            "--app",
            "bench.simpleq_app:create_app",
            "--factory",
            "--queues",
            "bench",
            "--processes",
            "2",
            "--concurrency",
            "4",
            "--max-tasks-per-process",
            "20000",
        ]
    if lib == "celery":
        return command(lib, {"processes": 1, "concurrency": SLOTS})
    if lib == "taskiq":
        return command(lib, {"processes": 2, "concurrency": 4, "prefetch": SLOTS})
    return command(lib, {"processes": 2, "concurrency": 4, "max_tasks": 20000})


def _versions(lib: str) -> dict:
    script = r"""
import importlib.metadata as m
names = ["simple-q-runtime","simple-q-worker","dramatiq","celery","kombu","arq","taskiq","taskiq-redis","redis"]
out = {}
for name in names:
    try:
        out[name] = m.version(name)
    except m.PackageNotFoundError:
        pass
print(__import__("json").dumps(out))
"""
    result = subprocess.run([_python(lib), "-c", script], capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return {"error": (result.stderr or result.stdout)[-400:]}
    return json.loads(result.stdout)


def _read_latency(path: Path) -> list[tuple[int, int, int]]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split(",")
        if len(parts) != 3:
            continue
        try:
            rows.append((int(parts[0]), int(parts[1]), int(parts[2])))
        except ValueError:
            continue
    return rows


def _percentile(values: list[int], q: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[index]


def _rss_tree(pid: int) -> int:
    result = subprocess.run(["ps", "-eo", "pid,ppid,rss"], capture_output=True, text=True, timeout=5)
    wanted = {pid}
    parsed = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            parsed.append((int(parts[0]), int(parts[1]), int(parts[2])))
        except ValueError:
            continue
    changed = True
    while changed:
        changed = False
        for child, parent, _rss in parsed:
            if parent in wanted and child not in wanted:
                wanted.add(child)
                changed = True
    return sum(rss for pid_, _parent, rss in parsed if pid_ in wanted)


def _broker(lib: str) -> str:
    commands = {
        "simpleq": ["ZCARD", "bench:scheduled"],
        "dramatiq": ["LLEN", "dramatiq:bench"],
        "celery": ["LLEN", "bench"],
        "arq": ["ZCARD", "arq:queue"],
        "taskiq": ["XLEN", "bench"],
    }[lib]
    result = subprocess.run(
        ["redis-cli", "-p", str(REDIS_PORT), *commands],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return result.stdout.strip()


def run(lib: str, count: int, deadline_s: float) -> dict:
    run_id = f"st-{_utc()}-{lib}-burst-{count}"
    run_dir = STRESS_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    lat_path = run_dir / "latency.txt"
    log_path = run_dir / "worker.log"
    ensure_redis(REDIS_PORT, str(REDIS_DIR))
    flush_owned(REDIS_PORT, str(REDIS_DIR))
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(ROOT),
            "PYTHONUNBUFFERED": "1",
            "REDIS_URL": f"redis://127.0.0.1:{REDIS_PORT}/0",
            "PG_DSN": "postgresql://ubuntu@127.0.0.1:5432/longrun",
            "MOCK_URL": "http://127.0.0.1:6411",
            "LAT_PATH": str(lat_path),
            "ARQ_MAX_JOBS": str(SLOTS),
        }
    )
    for key in list(env):
        if key.startswith("CHAOS_") or key == "DRAIN_FLAG":
            env.pop(key, None)
    cmd = _command(lib)
    log = log_path.open("w", encoding="utf-8")
    worker = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    samples = []
    enqueue = {}
    status = "incomplete"
    try:
        time.sleep(3)
        if worker.poll() is not None:
            status = "failed"
            enqueue = {"error": "worker exited before enqueue", "code": worker.returncode}
        else:
            payload = json.dumps({"op": "fast", "count": count, "job_id": f"{run_id}-job"})
            started = time.time()
            sent = subprocess.run(
                [_python(lib), "-m", "bench.cli", "many", "--lib", lib, "--payload", payload],
                cwd=str(ROOT),
                env=env,
                capture_output=True,
                text=True,
                timeout=300,
            )
            enqueue = {
                "code": sent.returncode,
                "seconds": round(time.time() - started, 3),
                "stdout": sent.stdout[-1500:],
                "stderr": sent.stderr[-800:],
            }
            if sent.returncode != 0:
                status = "failed"
            else:
                deadline = time.time() + deadline_s
                while time.time() < deadline:
                    rows = _read_latency(lat_path)
                    samples.append(
                        {
                            "utc": datetime.now(timezone.utc).isoformat(),
                            "done": len(rows),
                            "broker": _broker(lib),
                            "rss_kb": _rss_tree(worker.pid),
                        }
                    )
                    if len({row[0] for row in rows}) >= count:
                        break
                    if worker.poll() is not None:
                        break
                    time.sleep(1)
        rows = _read_latency(lat_path)
        unique = {row[0] for row in rows}
        dup = len(rows) - len(unique)
        latency = [row[2] - row[1] for row in rows if row[2] >= row[1]]
        span_s = None
        if rows:
            span_s = round((max(row[2] for row in rows) - min(row[1] for row in rows)) / 1000, 3)
        if status != "failed":
            if len(unique) >= count and dup == 0 and worker.poll() is None:
                status = "passed"
            elif len(unique) >= count and dup:
                status = "failed"
            else:
                status = "incomplete"
        manifest = {
            "run_id": run_id,
            "class": "stress",
            "counts_as_2400": False,
            "scenario_id": "S_burst_fast",
            "library": lib,
            "count": count,
            "slots": SLOTS,
            "command": cmd,
            "versions": _versions(lib),
            "status": status,
            "done_lines": len(rows),
            "unique_n": len(unique),
            "duplicate_lines": dup,
            "enqueue": enqueue,
            "span_s": span_s,
            "throughput_per_s": round(len(unique) / span_s, 2) if span_s else None,
            "latency_ms": {
                "p50": _percentile(latency, 0.50),
                "p95": _percentile(latency, 0.95),
                "max": max(latency) if latency else None,
            },
            "worker_exit_during_run": worker.poll(),
            "redis_port": REDIS_PORT,
            "redis_dir": str(REDIS_DIR),
            "resource_limits": "no cgroup; host is shared",
        }
        (run_dir / "samples.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in samples),
            encoding="utf-8",
        )
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({k: manifest[k] for k in ("run_id", "status", "unique_n", "duplicate_lines", "span_s", "throughput_per_s", "latency_ms")}))
        return manifest
    finally:
        if worker.poll() is None:
            _stop_group(worker, grace_s=20)
        log.flush()
        log.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--library", required=True, choices=["simpleq", "dramatiq", "celery", "arq", "taskiq"])
    parser.add_argument("--count", type=int, default=COUNT_DEFAULT)
    parser.add_argument("--deadline-s", type=float, default=180)
    parser.add_argument("--keep-redis", action="store_true")
    args = parser.parse_args()
    try:
        manifest = run(args.library, args.count, args.deadline_s)
    finally:
        if not args.keep_redis:
            try:
                shutdown_save(REDIS_PORT, str(REDIS_DIR))
            except Exception as exc:  # noqa: BLE001
                print(f"redis shutdown: {exc}", file=sys.stderr)
    raise SystemExit(0 if manifest["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
