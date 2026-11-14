"""
E08: штатный shutdown Taskiq (без внешнего supervisor).
"""

from __future__ import annotations

import signal
import time

from research.taskiq.lib import config, harness, observe, process


def _case(name: str, *, during: str) -> dict:
    run_dir = harness.new_run_dir("shutdown", f"e08-{name}")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns("e08")
    redis_url = config.DEFAULT_REDIS_URL
    harness.reset_stream(redis_url, stream, group)
    env = harness.base_env(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        idle_ms=600_000,
        xread_count=10,
        xread_block=200,
        lock_s=5,
    )
    proc, _ = harness.readiness_handshake(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        env=env,
        max_async=1,
    )
    client = observe.connect(redis_url)
    try:
        if during == "idle":
            time.sleep(0.5)
        elif during == "handler":
            harness.enqueue("sleep_async", "shut-handler", 8.0, redis_url=redis_url, env=env)
            assert harness.wait_event(
                events,
                lambda r: r.get("event") == "handler_start" and r.get("job_id") == "shut-handler",
                timeout_s=15,
            )
        elif during == "prefetched":
            harness.enqueue("sleep_async", "shut-hold", 8.0, redis_url=redis_url, env=env)
            assert harness.wait_event(
                events,
                lambda r: r.get("event") == "handler_start" and r.get("job_id") == "shut-hold",
                timeout_s=15,
            )
            harness.enqueue_batch(
                "fast",
                count=6,
                args=["shut-pf", 0, 0.0],
                redis_url=redis_url,
                env=env,
            )
            time.sleep(0.8)
        before = observe.snapshot(client, stream, group)
        stop = process.stop_group(proc, grace_s=5.0)
        after = observe.snapshot(client, stream, group)
        result = {
            "case": name,
            "during": during,
            "before": observe.dump_snapshot(before),
            "after": observe.dump_snapshot(after),
            "stop": process.as_dict(stop),
            "artifacts": str(run_dir),
        }
        observe.write_json(run_dir / "result.json", result)
        return result
    finally:
        client.close()
        if process.poll(proc) is None:
            process.stop_group(proc, grace_s=1)


def run_early_sigterm() -> dict:
    run_dir = harness.new_run_dir("shutdown", "e08-early-sigterm")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns("e08e")
    redis_url = config.DEFAULT_REDIS_URL
    harness.reset_stream(redis_url, stream, group)
    env = harness.base_env(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        xread_block=2000,
        lock_s=5,
    )
    proc = process.start(
        harness.worker_cmd(workers=1, max_async=1, shutdown_s=5),
        env=env,
        cwd=config.ROOT,
        log_path=run_dir / "worker.log",
    )
    time.sleep(0.2)
    stop = process.stop_group(proc, grace_s=8)
    result = {
        "case": "early_sigterm",
        "stop": process.as_dict(stop),
        "artifacts": str(run_dir),
    }
    observe.write_json(run_dir / "result.json", result)
    return result


def run_double_signal() -> dict:
    run_dir = harness.new_run_dir("shutdown", "e08-double-signal")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns("e08d")
    redis_url = config.DEFAULT_REDIS_URL
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
    harness.enqueue("sleep_async", "double", 20.0, redis_url=redis_url, env=env)
    assert harness.wait_event(
        events,
        lambda r: r.get("event") == "handler_start" and r.get("job_id") == "double",
        timeout_s=15,
    )
    import os

    t0 = time.monotonic()
    os.killpg(proc.pid, signal.SIGTERM)
    time.sleep(0.3)
    os.killpg(proc.pid, signal.SIGTERM)
    time.sleep(0.3)
    os.killpg(proc.pid, signal.SIGTERM)
    time.sleep(0.3)
    # hardkill_count default 3 → next should hard kill path inside worker;
    # we still reap via wait
    try:
        proc.wait(timeout=10)
    except Exception:
        process.stop_group(proc, grace_s=1)
    result = {
        "case": "double_signal",
        "exit_code": proc.returncode,
        "elapsed_s": round(time.monotonic() - t0, 3),
        "artifacts": str(run_dir),
    }
    observe.write_json(run_dir / "result.json", result)
    return result


def run() -> dict:
    cases = [
        _case("idle", during="idle"),
        _case("during_handler", during="handler"),
        _case("with_prefetched", during="prefetched"),
        run_early_sigterm(),
        run_double_signal(),
    ]
    summary = {
        "experiment": "e08_shutdown",
        "status": "pass",
        "cases": cases,
        "source_notes": {
            "manager_settings": ["--workers", "reload", "signal handlers on ProcessManager"],
            "execution_worker_settings": [
                "--shutdown-timeout",
                "--wait-tasks-timeout",
                "--hardkill-count",
                "--max-tasks-per-child",
                "--ack-type",
                "--max-async-tasks",
                "--max-prefetch",
            ],
            "process_manager": "taskiq/cli/worker/process_manager.py",
            "receiver_listen": "taskiq/receiver/receiver.py listen/prefetcher",
        },
    }
    observe.write_json(config.ARTIFACTS / "shutdown" / "e08-summary.json", summary)
    return summary


if __name__ == "__main__":
    print(run())
