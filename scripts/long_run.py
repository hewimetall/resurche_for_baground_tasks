"""Один длительный прогон одной библиотеки на Redis порта 6410.

Класс short — проверка проводки, не зачёт 2400 с.
Класс scored сценария A1_control_40 требует steps*step_s == 2400.
Порты 6379 и 6380 не используются.
"""

from __future__ import annotations

import argparse
import hashlib
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

from bench.longrun.events import emit
from bench.longrun.stand import ensure_redis, flush_owned, shutdown_save, start_effect, stop_effect
from bench.workers import command, python_for

REDIS_PORT = 6410
EFFECT_PORT = 6411
REDIS_DIR = ROOT / "results" / "long" / "redis-6410"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_pg() -> None:
    subprocess.run(
        ["psql", "-d", "postgres", "-c", "CREATE DATABASE longrun"],
        capture_output=True,
        text=True,
        timeout=20,
    )
    subprocess.run(
        ["psql", "-d", "longrun", "-v", "ON_ERROR_STOP=1", "-f", str(ROOT / "bench/schema.sql")],
        check=True,
        timeout=20,
    )
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _versions(python: str) -> dict:
    script = (
        "import importlib.metadata as m, json\n"
        "names=['simple-q','dramatiq','celery','kombu','arq','taskiq','taskiq-redis','redis']\n"
        "out={}\n"
        "for n in names:\n"
        "    try: out[n]=m.version(n)\n"
        "    except m.PackageNotFoundError: pass\n"
        "print(json.dumps(out))\n"
    )
    result = subprocess.run([python, "-c", script], capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return {"error": result.stderr[-500:]}
    return json.loads(result.stdout)


def _effective(python: str, lib: str, env: dict) -> dict:
    script = r"""
import json, os
lib = os.environ["LONGRUN_LIB"]
out = {"env": {k: os.environ[k] for k in sorted(os.environ) if k.startswith("LONGRUN_") or k.startswith("SIMPLE_Q") or k.startswith("DRAMATIQ_") or k.startswith("CELERY_") or k.startswith("ARQ_") or k.startswith("TASKIQ_")}}
if lib == "simpleq":
    from bench.simpleq_app import hold
    from simple_q import Config
    cfg = Config.from_env()
    out["actor"] = {name: getattr(hold, name, None) for name in ("name", "time_limit", "max_retries", "max_deliveries", "queue")}
    out["config"] = {name: getattr(cfg, name, None) for name in dir(cfg) if not name.startswith("_")}
elif lib == "dramatiq":
    from bench.dramatiq_app import hold, HEARTBEAT_MS, broker
    out["heartbeat_ms"] = HEARTBEAT_MS
    out["maintenance_chance"] = getattr(broker, "maintenance_chance", None)
    out["actor_options"] = dict(getattr(hold, "options", {}) or {})
elif lib == "celery":
    from bench.celery_app import hold, app
    out["time_limit"] = hold.time_limit
    out["soft_time_limit"] = hold.soft_time_limit
    out["max_retries"] = hold.max_retries
    out["acks_late"] = app.conf.task_acks_late
    out["broker_transport_options"] = dict(app.conf.broker_transport_options or {})
    out["result_backend_transport_options"] = dict(app.conf.result_backend_transport_options or {})
elif lib == "arq":
    from bench.arq_app import WorkerSettings
    out["functions"] = [{"name": fn.name, "timeout_s": getattr(fn, "timeout_s", None), "max_tries": getattr(fn, "max_tries", None)} for fn in WorkerSettings.functions if fn.name == "hold"]
elif lib == "taskiq":
    from bench.taskiq_app import hold, IDLE_MS
    out["idle_timeout_ms"] = IDLE_MS
    out["labels"] = dict(getattr(hold, "labels", {}) or {})
print(json.dumps(out, default=str))
"""
    env = dict(env)
    env["LONGRUN_LIB"] = lib
    result = subprocess.run([python, "-c", script], capture_output=True, text=True, env=env, cwd=str(ROOT), timeout=60)
    if result.returncode != 0:
        return {"error": (result.stderr or result.stdout)[-800:]}
    return json.loads(result.stdout)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line:
            rows.append(json.loads(line))
    return rows


def _sample(lib: str, port: int) -> dict:
    commands = {
        "simpleq": ["ZCARD", "bench:scheduled"],
        "dramatiq": ["LLEN", "dramatiq:bench"],
        "celery": ["ZCARD", "unacked_index"],
        "arq": ["ZCARD", "arq:queue"],
        "taskiq": ["XLEN", "bench"],
    }[lib]
    result = subprocess.run(
        ["redis-cli", "-p", str(port), *commands],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return {"cmd": commands, "stdout": result.stdout.strip(), "code": result.returncode}


def _ps(leader: int) -> list[dict]:
    result = subprocess.run(
        ["ps", "-eo", "pid,ppid,rss,pcpu,etime,cmd"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    rows = []
    wanted = {leader}
    lines = result.stdout.splitlines()[1:]
    changed = True
    parsed = []
    for line in lines:
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        pid, ppid, rss, pcpu, etime, cmd = parts
        parsed.append((int(pid), int(ppid), rss, pcpu, etime, cmd))
    while changed:
        changed = False
        for pid, ppid, rss, pcpu, etime, cmd in parsed:
            if ppid in wanted and pid not in wanted:
                wanted.add(pid)
                changed = True
    for pid, ppid, rss, pcpu, etime, cmd in parsed:
        if pid in wanted:
            rows.append({"pid": pid, "ppid": ppid, "rss_kb": int(rss), "pcpu": pcpu, "etime": etime, "cmd": cmd[:180]})
    return rows


def _stop_group(pid: int) -> dict:
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return {"signal": "SIGTERM", "already_dead": True}
    for _ in range(75):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return {"signal": "SIGTERM", "exited": True}
        time.sleep(0.2)
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        return {"signal": "SIGKILL", "already_dead": True}
    return {"signal": "SIGKILL", "reason": "sigterm_grace_exceeded"}


def run(args: argparse.Namespace) -> int:
    steps = args.steps
    step_s = args.step_s
    planned = steps * step_s
    if args.class_name == "scored":
        if args.scenario != "A1_control_40" or steps != 240 or step_s != 10 or planned != 2400:
            raise SystemExit("scored run is only A1_control_40 with 240 steps of 10s")
    elif planned > 30:
        raise SystemExit("short class refuses a workload longer than 30s")

    run_id = f"lr-{_utc()}-{args.library}-{args.scenario}-{args.class_name}"
    run_dir = ROOT / "results" / "long" / run_id
    run_dir.mkdir(parents=True)
    events_path = run_dir / "events.jsonl"
    effects_path = run_dir / "effects.jsonl"
    samples_path = run_dir / "samples.jsonl"
    worker_log = run_dir / "worker.log"
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    python = python_for(args.library)
    real = time.clock_gettime(time.CLOCK_REALTIME)
    mono = time.clock_gettime(time.CLOCK_MONOTONIC)

    ensure_pg()
    ensure_redis(REDIS_PORT, str(REDIS_DIR))
    flush_owned(REDIS_PORT, str(REDIS_DIR))
    server = start_effect(EFFECT_PORT, str(effects_path))
    env = os.environ.copy()
    env.update(
        {
            "REDIS_URL": f"redis://127.0.0.1:{REDIS_PORT}/0",
            "PG_DSN": "postgresql://ubuntu@127.0.0.1:5432/longrun",
            "MOCK_URL": f"http://127.0.0.1:{EFFECT_PORT}",
            "LONGRUN_EVENTS": str(events_path),
            "LONGRUN_RUN_ID": run_id,
            "LONGRUN_SCENARIO": args.scenario,
            "LONGRUN_LIBRARY": args.library,
            "LONGRUN_EFFECT_URL": f"http://127.0.0.1:{EFFECT_PORT}",
            "LONGRUN_TIME_LIMIT_S": str(args.time_limit_s),
            "LONGRUN_MAX_RETRIES": str(args.max_retries),
            "LONGRUN_LIB": args.library,
            "PYTHONPATH": str(ROOT),
            "PYTHONUNBUFFERED": "1",
        }
    )
    for key in list(env):
        if key.startswith("CHAOS_") or key == "DRAIN_FLAG":
            env.pop(key, None)
    versions = _versions(python)
    effective = _effective(python, args.library, env)
    manifest = {
        "run_id": run_id,
        "scenario_id": args.scenario,
        "class": args.class_name,
        "library": args.library,
        "versions": versions,
        "source_commit": sha,
        "planned_s": planned,
        "steps": steps,
        "step_s": step_s,
        "kind": "sleep",
        "time_limit_s": args.time_limit_s,
        "max_retries": args.max_retries,
        "workers": 1,
        "fault": None,
        "counts_as_2400": args.class_name == "scored",
        "redis_port": REDIS_PORT,
        "redis_dir": str(REDIS_DIR),
        "effect_port": EFFECT_PORT,
        "clock": {"realtime": real, "monotonic": mono, "offset_realtime_minus_mono": real - mono},
        "resource_limits": "no cgroup and no RLIMIT_AS on the control worker; host is shared with kind",
        "effective": effective,
        "status": "running",
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8")
    emit(
        str(events_path),
        "run_started",
        run_id=run_id,
        scenario_id=args.scenario,
        library=args.library,
        job_id=None,
        message_id=None,
        execution_id=None,
        reason=args.class_name,
    )

    cmd = command(args.library, {"processes": 1, "concurrency": 1})
    log = worker_log.open("w", encoding="utf-8")
    worker = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    job_id = f"{run_id}-job"
    message_id = None
    status = "incomplete"
    stop_info: dict = {}
    try:
        time.sleep(2)
        if worker.poll() is not None:
            status = "failed"
            manifest["worker_exit"] = worker.returncode
            return 1
        payload = {"kwargs": {"job_id": job_id, "steps": steps, "step_s": step_s, "kind": "sleep"}}
        env["LONGRUN_PAYLOAD"] = json.dumps(payload)
        sent = subprocess.run(
            [python, "-m", "bench.longrun.send"],
            cwd=str(ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        (run_dir / "enqueue.json").write_text(
            json.dumps({"code": sent.returncode, "stdout": sent.stdout, "stderr": sent.stderr[-1000:]}, indent=2)
            + "\n",
            encoding="utf-8",
        )
        message_id = None
        if sent.returncode == 0 and sent.stdout.strip():
            message_id = json.loads(sent.stdout).get("id")
        emit(
            str(events_path),
            "accepted",
            run_id=run_id,
            scenario_id=args.scenario,
            library=args.library,
            job_id=job_id,
            message_id=message_id,
            execution_id=None,
            reason=f"enqueue_exit {sent.returncode}",
        )
        if sent.returncode != 0:
            status = "failed"
            return 1
        deadline = time.monotonic() + planned + (90 if args.class_name == "scored" else 45)
        extensions = 0
        seen_finish = 0
        while time.monotonic() < deadline:
            rows = _read_events(events_path)
            starts = [row for row in rows if row.get("event") == "handler_started"]
            finishes = [row for row in rows if row.get("event") == "handler_finished"]
            sample = {
                "utc": datetime.now(timezone.utc).isoformat(),
                "broker": _sample(args.library, REDIS_PORT),
                "ps": _ps(worker.pid),
                "starts": len(starts),
                "finishes": len(finishes),
            }
            with samples_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(sample, default=str) + "\n")
            if len(starts) > 1 and extensions < 2:
                extensions += 1
                deadline = time.monotonic() + planned + 90
            if finishes and len(finishes) == len(starts) and len(finishes) > seen_finish:
                seen_finish = len(finishes)
                if len(starts) == len(finishes):
                    status = "finished"
                    break
            if worker.poll() is not None and not finishes:
                status = "failed"
                manifest["worker_exit_during_run"] = worker.returncode
                break
            time.sleep(5 if args.class_name == "scored" else 0.5)
        if status == "finished":
            pass
        elif worker.poll() is None and status != "finished":
            pass
    finally:
        if worker.poll() is None:
            stop_info = _stop_group(worker.pid)
            emit(
                str(events_path),
                "process_exited",
                run_id=run_id,
                scenario_id=args.scenario,
                library=args.library,
                job_id=job_id,
                message_id=message_id,
                execution_id=None,
                reason=json.dumps(stop_info),
            )
        log.flush()
        log.close()
        stop_effect(server)
        if args.keep_redis:
            pass
        else:
            try:
                shutdown_save(REDIS_PORT, str(REDIS_DIR))
            except Exception as exc:  # noqa: BLE001
                manifest["redis_shutdown_error"] = str(exc)
        rows = _read_events(events_path)
        starts = [row for row in rows if row.get("event") == "handler_started"]
        finishes = [row for row in rows if row.get("event") == "handler_finished"]
        span_s = None
        if starts and finishes:
            span_s = (finishes[-1]["mono_ns"] - starts[0]["mono_ns"]) / 1e9
        progress = [row for row in rows if row.get("event") == "progress"]
        if args.class_name == "short" and status == "finished" and len(progress) >= steps:
            final = "short_control"
        elif args.class_name == "scored" and status == "finished" and span_s is not None and span_s >= 2400 and len(progress) >= 240:
            final = "passed"
        elif status == "finished":
            final = "failed"
        else:
            final = "incomplete" if status == "incomplete" else "failed"
        manifest["status"] = final
        manifest["handler_starts"] = len(starts)
        manifest["handler_finishes"] = len(finishes)
        manifest["progress_events"] = len(progress)
        manifest["span_s"] = span_s
        manifest["stop"] = stop_info
        files = {}
        for path in sorted(run_dir.iterdir()):
            if path.is_file():
                files[path.name] = {"bytes": path.stat().st_size, "sha256": _sha256(path)}
        manifest["files_before_manifest"] = files
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8")
        print(json.dumps({"run_id": run_id, "status": final, "span_s": span_s, "dir": str(run_dir)}))
    return 0 if manifest["status"] in {"passed", "short_control"} else 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--library", required=True, choices=["simpleq", "dramatiq", "celery", "arq", "taskiq"])
    parser.add_argument("--scenario", default="A0_wire")
    parser.add_argument("--class", dest="class_name", choices=["short", "scored"], default="short")
    parser.add_argument("--steps", type=int, default=3)
    parser.add_argument("--step-s", type=float, default=1)
    parser.add_argument("--time-limit-s", type=int, default=3000)
    parser.add_argument("--max-retries", type=int, default=0)
    parser.add_argument("--keep-redis", action="store_true")
    args = parser.parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
