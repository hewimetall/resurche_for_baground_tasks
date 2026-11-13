"""
E01: max-async-tasks=1 не означает «в PEL одно сообщение».

Arrange: worker ready; hold занимает единственный async-слот; в stream кладётся batch.
Act: после завершения hold следующий XREADGROUP забирает batch COUNT.
Assert: сразу после старта следующей задачи XPENDING >= 2 при одном новом handler_start.
"""

from __future__ import annotations

import time

from research.taskiq.lib import config, harness, observe, process


def run() -> dict:
    run_dir = harness.new_run_dir("correctness", "e01-batch-pel")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns("e01")
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
        ack_type="when_executed",
    )
    client = observe.connect(redis_url)
    samples = []
    try:
        harness.enqueue("sleep_async", "hold", 3.0, redis_url=redis_url, env=env)
        hold_start = harness.wait_event(
            events,
            lambda r: r.get("event") == "handler_start" and r.get("job_id") == "hold",
            timeout_s=15,
        )
        assert hold_start, "hold did not start"
        harness.enqueue_batch(
            "sleep_async",
            count=8,
            args=["batch", 6.0],
            redis_url=redis_url,
            env=env,
        )
        pending_during_hold = observe.snapshot(client, stream, group)
        hold_finish = harness.wait_event(
            events,
            lambda r: r.get("event") == "handler_finish" and r.get("job_id") == "hold",
            timeout_s=15,
        )
        assert hold_finish, "hold did not finish"
        # First batch task should start; sample PEL while it runs.
        batch_start = harness.wait_event(
            events,
            lambda r: r.get("event") == "handler_start"
            and str(r.get("job_id", "")).startswith("batch"),
            timeout_s=10,
        )
        assert batch_start, "batch did not start"
        deadline = time.monotonic() + 2.0
        max_pending = 0
        while time.monotonic() < deadline:
            snap = observe.snapshot(client, stream, group)
            samples.append(observe.dump_snapshot(snap))
            max_pending = max(max_pending, snap.pending_count)
            time.sleep(0.05)
        batch_starts = [
            r
            for r in harness.read_events(events)
            if r.get("event") == "handler_start"
            and str(r.get("job_id", "")).startswith("batch")
            and r["ts_mono"] <= batch_start["ts_mono"] + 2.0
        ]
        ok = (
            pending_during_hold.pending_count <= 1
            and max_pending >= 2
            and len(batch_starts) == 1
        )
        result = {
            "experiment": "e01_batch_pel",
            "status": "pass" if ok else "fail",
            "measured": {
                "pending_during_hold": pending_during_hold.pending_count,
                "max_pending_after_batch_start": max_pending,
                "batch_starts_in_2s": len(batch_starts),
                "xread_count": 10,
                "max_async_tasks": 1,
                "max_prefetch": 0,
            },
            "claim": (
                "После освобождения слота один XREADGROUP COUNT зачисляет batch в PEL; "
                "--max-async-tasks=1 держит только одно исполнение, остальные pending "
                "остаются у consumer до yield/ACK."
            ),
            "evidence_class": "measured",
            "artifacts": {"run_dir": str(run_dir)},
        }
        observe.write_json(run_dir / "result.json", result)
        observe.write_json(run_dir / "samples.json", samples)
        observe.write_json(run_dir / "env.json", config.as_dict(config.env_snapshot(redis_url)))
        return result
    finally:
        process.stop_group(proc, grace_s=3)
        client.close()


if __name__ == "__main__":
    print(run())
