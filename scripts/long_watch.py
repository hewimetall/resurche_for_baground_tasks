#!/usr/bin/env python3
"""Read-only observer for a long run.

Writes only under results/long/watcher/. Does not signal workers, does not
write Redis, and does not add files to the run directory: the runner hashes
that directory when it closes the manifest.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("/workspace/results/long")
WATCH_DIR = ROOT / "watcher"
OWNED_PREFIX = os.path.realpath("/workspace/results/long")
FORBIDDEN_PORTS = {6379, 6380, 5432, 8090}


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append(path: Path, row: dict) -> None:
    data = (json.dumps(row, ensure_ascii=False, default=str) + "\n").encode()
    fd = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)


def _load_manifest(run_dir: Path) -> dict | None:
    path = run_dir / "manifest.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _running_dirs() -> list[Path]:
    found = []
    for run_dir in sorted(ROOT.glob("lr-*")):
        manifest = _load_manifest(run_dir)
        if manifest and manifest.get("status") == "running":
            found.append(run_dir)
    return found


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            rows.append({"event": "partial_line", "bytes": len(line)})
    return rows


def _assert_owned(port: int) -> str:
    if port in FORBIDDEN_PORTS or port < 6400 or port > 6499:
        raise RuntimeError(f"watcher refuses port {port}")
    result = subprocess.run(
        ["redis-cli", "-p", str(port), "CONFIG", "GET", "dir"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if result.returncode != 0 or len(lines) < 2:
        raise RuntimeError(f"CONFIG GET dir failed on {port}: {result.stderr.strip() or result.stdout.strip()}")
    directory = os.path.realpath(lines[1])
    if not directory.startswith(OWNED_PREFIX + os.sep) and directory != OWNED_PREFIX:
        raise RuntimeError(f"port {port} serves {directory}, not the long-run redis")
    return directory


def _redis(port: int, *args: str) -> dict:
    result = subprocess.run(
        ["redis-cli", "-p", str(port), "--raw", *args],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return {
        "cmd": list(args),
        "code": result.returncode,
        "stdout": result.stdout.strip()[:2000],
        "stderr": result.stderr.strip()[:300],
    }


def _broker(library: str, port: int) -> dict:
    directory = _assert_owned(port)
    common = {"redis_dir": directory, "time": _redis(port, "TIME")}
    if library == "celery":
        common["unacked_n"] = _redis(port, "ZCARD", "unacked_index")
        common["unacked_scores"] = _redis(port, "ZRANGE", "unacked_index", "0", "-1", "WITHSCORES")
        common["unacked_hash"] = _redis(port, "HLEN", "unacked")
        common["queue_len"] = _redis(port, "LLEN", "bench")
    elif library == "dramatiq":
        common["queue_len"] = _redis(port, "LLEN", "dramatiq:bench")
        common["heartbeats"] = _redis(port, "ZRANGE", "dramatiq:__heartbeats__", "0", "-1", "WITHSCORES")
    elif library == "simpleq":
        common["scheduled"] = _redis(port, "ZCARD", "bench:scheduled")
        common["workers"] = _redis(port, "ZRANGE", "bench:workers", "0", "-1", "WITHSCORES")
    elif library == "arq":
        common["queue"] = _redis(port, "ZCARD", "arq:queue")
        common["in_progress"] = _redis(port, "--scan", "--pattern", "arq:in-progress:*")
    elif library == "taskiq":
        common["xlen"] = _redis(port, "XLEN", "bench")
        common["xpending"] = _redis(port, "XPENDING", "bench", "taskiq")
    else:
        common["error"] = f"no read set for {library}"
    return common


def _processes() -> list[dict]:
    result = subprocess.run(
        ["ps", "-eo", "pid,ppid,rss,pcpu,etime,cmd"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    rows = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        pid, ppid, rss, pcpu, etime, cmd = parts
        if cmd.startswith("tmux "):
            continue
        if "long_run.py" in cmd or "long_watch.py" in cmd or any(
            token in cmd
            for token in (
                "celery -A bench.celery_app",
                "bench.dramatiq_app",
                "bench.arq_app",
                "taskiq worker",
                "agent-worker",
            )
        ):
            rows.append(
                {
                    "pid": int(pid),
                    "ppid": int(ppid),
                    "rss_kb": int(rss),
                    "pcpu": pcpu,
                    "etime": etime,
                    "cmd": cmd[:180],
                }
            )
    return rows


def _event_view(rows: list[dict]) -> dict:
    counts: dict[str, dict[str, int]] = {}
    last = rows[-1] if rows else {}
    for row in rows:
        execution = str(row.get("execution_id") or "")
        bucket = counts.setdefault(execution, {})
        event = str(row.get("event") or "")
        bucket[event] = bucket.get(event, 0) + 1
    progress = [row for row in rows if row.get("event") == "progress"]
    last_progress = progress[-1] if progress else {}
    return {
        "events": len(rows),
        "last_event": last.get("event"),
        "last_event_utc": last.get("utc"),
        "last_step": last_progress.get("step_id"),
        "last_progress_utc": last_progress.get("utc"),
        "last_execution_id": last_progress.get("execution_id"),
        "by_execution": counts,
    }


def _sample(run_dir: Path, manifest: dict) -> dict:
    events = _read_jsonl(run_dir / "events.jsonl")
    effects_path = run_dir / "effects.jsonl"
    effect_rows = _read_jsonl(effects_path) if effects_path.exists() else []
    real = time.time()
    mono = time.monotonic()
    view = _event_view(events)
    quiet_s = None
    if view.get("last_progress_utc"):
        last = datetime.fromisoformat(view["last_progress_utc"])
        quiet_s = round((datetime.now(timezone.utc) - last).total_seconds(), 3)
    port = int(manifest["redis_port"])
    try:
        broker = _broker(str(manifest.get("library")), port)
        broker_error = None
    except Exception as exc:  # noqa: BLE001
        broker = None
        broker_error = str(exc)
    return {
        "event": "watch",
        "utc": _utc(),
        "mono_ns": time.monotonic_ns(),
        "watcher_pid": os.getpid(),
        "run_id": manifest.get("run_id"),
        "scenario_id": manifest.get("scenario_id"),
        "library": manifest.get("library"),
        "manifest_status": manifest.get("status"),
        "clock": {"realtime": real, "monotonic": mono, "offset_realtime_minus_mono": real - mono},
        "events_bytes": (run_dir / "events.jsonl").stat().st_size if (run_dir / "events.jsonl").exists() else 0,
        "effects": len(effect_rows),
        "effects_duplicate": sum(1 for row in effect_rows if row.get("duplicate")),
        "progress_quiet_s": quiet_s,
        "broker_error": broker_error,
        "broker": broker,
        "ps": _processes(),
        **view,
    }


def _watch_one(run_dir: Path, interval: float) -> str:
    manifest = _load_manifest(run_dir)
    if not manifest:
        raise SystemExit(f"no manifest in {run_dir}")
    run_id = str(manifest.get("run_id") or run_dir.name)
    out = WATCH_DIR / f"{run_id}.jsonl"
    _append(
        out,
        {
            "event": "watcher_attached",
            "utc": _utc(),
            "mono_ns": time.monotonic_ns(),
            "watcher_pid": os.getpid(),
            "run_id": run_id,
            "run_dir": str(run_dir),
            "journal": str(out),
            "reason": "journal is outside the run directory so the runner checksum does not include it",
        },
    )
    last_bytes = -1
    while True:
        current = _load_manifest(run_dir) or manifest
        row = _sample(run_dir, current)
        row["events_grew"] = row["events_bytes"] != last_bytes
        last_bytes = row["events_bytes"]
        _append(out, row)
        if current.get("status") != "running":
            _append(
                out,
                {
                    "event": "watcher_detached",
                    "utc": _utc(),
                    "mono_ns": time.monotonic_ns(),
                    "watcher_pid": os.getpid(),
                    "run_id": run_id,
                    "manifest_status": current.get("status"),
                    "reason": "manifest left status running",
                },
            )
            return str(current.get("status"))
        time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--interval", type=float, default=10)
    parser.add_argument("--follow", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()
    WATCH_DIR.mkdir(parents=True, exist_ok=True)
    while True:
        if args.run_dir:
            run_dir = Path(args.run_dir)
            args.run_dir = ""
        else:
            found = _running_dirs()
            if not found:
                if not args.follow:
                    raise SystemExit("no running long run")
                time.sleep(args.interval)
                continue
            run_dir = found[-1]
        _watch_one(run_dir, args.interval)
        if not args.follow:
            return


if __name__ == "__main__":
    main()
