"""
E09: отказоустойчивость Redis — отдельно outage и data loss.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

from research.taskiq.lib import config, harness, observe, process


def _start_temp_redis(port: int, dir_path: Path, *, appendonly: bool = False) -> subprocess.Popen:
    dir_path.mkdir(parents=True, exist_ok=True)
    cmd = [
        "redis-server",
        "--port",
        str(port),
        "--bind",
        "127.0.0.1",
        "--dir",
        str(dir_path),
        "--save",
        "",
        "--appendonly",
        "yes" if appendonly else "no",
        "--daemonize",
        "no",
    ]
    log = dir_path / "redis.log"
    return process.start(cmd, log_path=log)


def _wait_ping(port: int, timeout_s: float = 10.0) -> None:
    import redis

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            r = redis.Redis(host="127.0.0.1", port=port)
            if r.ping():
                r.close()
                return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"redis :{port} not up")


def run_brief_outage() -> dict:
    port = 6414
    data = Path("/tmp/taskiq-research-redis-6414")
    # kill any previous
    subprocess.run(["redis-cli", "-p", str(port), "SHUTDOWN", "NOSAVE"], check=False, capture_output=True)
    time.sleep(0.2)
    redis_proc = _start_temp_redis(port, data)
    _wait_ping(port)
    redis_url = f"redis://127.0.0.1:{port}/0"
    run_dir = harness.new_run_dir("redis_fault", "e09-brief-outage")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns("e09b")
    harness.reset_stream(redis_url, stream, group)
    env = harness.base_env(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        xread_block=500,
        lock_s=5,
    )
    proc, _ = harness.readiness_handshake(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        env=env,
    )
    # brief outage during idle listen
    os.kill(redis_proc.pid, signal.SIGSTOP)
    time.sleep(2.0)
    os.kill(redis_proc.pid, signal.SIGCONT)
    time.sleep(1.0)
    enqueue_err = None
    try:
        harness.enqueue("fast", "after-outage", 0, time.monotonic(), redis_url=redis_url, env=env)
    except Exception as exc:
        enqueue_err = repr(exc)
    finished = harness.wait_event(
        events,
        lambda r: r.get("event") == "handler_finish" and r.get("job_id") == "after-outage",
        timeout_s=20,
    )
    stop = process.stop_group(proc, grace_s=3)
    process.stop_group(redis_proc, grace_s=1)
    result = {
        "case": "brief_outage_sigstop",
        "enqueue_error": enqueue_err,
        "finished_after_resume": finished is not None,
        "worker_stop": process.as_dict(stop),
        "artifacts": str(run_dir),
    }
    observe.write_json(run_dir / "result.json", result)
    return result


def run_restart_persist_vs_loss() -> dict:
    port = 6415
    data = Path("/tmp/taskiq-research-redis-6415")
    subprocess.run(["redis-cli", "-p", str(port), "SHUTDOWN", "NOSAVE"], check=False, capture_output=True)
    time.sleep(0.2)
    redis_proc = _start_temp_redis(port, data, appendonly=False)
    _wait_ping(port)
    redis_url = f"redis://127.0.0.1:{port}/0"
    run_dir = harness.new_run_dir("redis_fault", "e09-restart")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns("e09r")
    harness.reset_stream(redis_url, stream, group)
    env = harness.base_env(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        lock_s=5,
    )
    proc, _ = harness.readiness_handshake(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        env=env,
    )
    harness.enqueue("sleep_async", "persist-victim", 30.0, redis_url=redis_url, env=env)
    assert harness.wait_event(
        events,
        lambda r: r.get("event") == "handler_start" and r.get("job_id") == "persist-victim",
        timeout_s=15,
    )
    client = observe.connect(redis_url)
    before = observe.snapshot(client, stream, group)
    client.close()
    # data loss restart
    subprocess.run(["redis-cli", "-p", str(port), "SHUTDOWN", "NOSAVE"], check=True)
    process.stop_group(redis_proc, grace_s=1)
    time.sleep(0.3)
    redis_proc2 = _start_temp_redis(port, data, appendonly=False)
    _wait_ping(port)
    client2 = observe.connect(redis_url)
    after_loss = observe.snapshot(client2, stream, group)
    # worker likely errored; stop it
    stop = process.stop_group(proc, grace_s=3)
    process.stop_group(redis_proc2, grace_s=1)
    client2.close()
    result = {
        "case": "restart_with_data_loss",
        "before": observe.dump_snapshot(before),
        "after_loss": observe.dump_snapshot(after_loss),
        "stream_empty_after_loss": after_loss.xlen == 0 and after_loss.pending_count == 0,
        "worker_stop": process.as_dict(stop),
        "note": "Separated from network outage: SHUTDOWN NOSAVE drops stream/PEL.",
        "artifacts": str(run_dir),
    }
    observe.write_json(run_dir / "result.json", result)
    return result


def run_maxmemory_noeviction() -> dict:
    """Fill via direct XADD (not per-task subprocess) until OOM on noeviction."""
    port = 6416
    data = Path("/tmp/taskiq-research-redis-6416")
    subprocess.run(["redis-cli", "-p", str(port), "SHUTDOWN", "NOSAVE"], check=False, capture_output=True)
    time.sleep(0.2)
    redis_proc = _start_temp_redis(port, data)
    _wait_ping(port)
    subprocess.run(
        ["redis-cli", "-p", str(port), "CONFIG", "SET", "maxmemory", "1mb"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["redis-cli", "-p", str(port), "CONFIG", "SET", "maxmemory-policy", "noeviction"],
        check=True,
        capture_output=True,
    )
    run_dir = harness.new_run_dir("redis_fault", "e09-maxmemory")
    stream = f"e09m:{int(time.time())}"
    client = observe.connect(f"redis://127.0.0.1:{port}/0")
    errors: list[str] = []
    accepted = 0
    payload = b"x" * 2048
    try:
        for i in range(20000):
            try:
                client.xadd(stream, {"data": payload})
                accepted += 1
            except Exception as exc:
                errors.append(repr(exc))
                break
    finally:
        client.close()
        process.stop_group(redis_proc, grace_s=1)
    result = {
        "case": "maxmemory_noeviction",
        "accepted_before_error": accepted,
        "errors": errors[:3],
        "note": "Direct XADD against stream; producer-side failure under noeviction.",
        "artifacts": str(run_dir),
    }
    observe.write_json(run_dir / "result.json", result)
    return result


def run() -> dict:
    summary = {
        "experiment": "e09_redis_fault",
        "brief_outage": run_brief_outage(),
        "data_loss_restart": run_restart_persist_vs_loss(),
        "maxmemory": run_maxmemory_noeviction(),
        "untested_here": [
            "long unavailability with worker reconnect semantics under load",
            "outage during result save specifically",
            "outage during retry publication specifically",
        ],
        "evidence_class": "measured",
    }
    observe.write_json(config.ARTIFACTS / "redis_fault" / "e09-summary.json", summary)
    return summary


if __name__ == "__main__":
    print(run())
