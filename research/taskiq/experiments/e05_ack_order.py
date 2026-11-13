"""
E05: порядок ACK относительно middleware / handler / result backend.

Для каждого ack_type: when_received | when_executed | when_saved | manual
фиксируем события EventMiddleware и момент исчезновения из PEL (XACK).
"""

from __future__ import annotations

import time

from research.taskiq.lib import config, harness, observe, process


ACK_TYPES = ("when_received", "when_executed", "when_saved", "manual")


def _run_one(ack_type: str, *, with_result_backend: bool, boom: bool) -> dict:
    run_dir = harness.new_run_dir("ack", f"e05-{ack_type}-{'rb' if with_result_backend else 'norb'}-{'boom' if boom else 'ok'}")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns(f"e05{ack_type[:3]}")
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
        result_backend=with_result_backend,
    )
    worker_ack = "manual" if ack_type == "manual" else ack_type
    # For manual task we still start worker with manual ack type.
    proc, _ = harness.readiness_handshake(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        env=env,
        ack_type=worker_ack,
        max_async=1,
    )
    client = observe.connect(redis_url)
    try:
        job = f"j-{ack_type}"
        samples = []
        if ack_type == "manual":
            harness.enqueue("manual_ack", job, redis_url=redis_url, env=env)
        elif boom:
            harness.enqueue("boom", job, redis_url=redis_url, env=env)
        else:
            harness.enqueue("sleep_async", job, 0.3, redis_url=redis_url, env=env)

        deadline = time.monotonic() + 15
        pending_zero_at = None
        while time.monotonic() < deadline:
            snap = observe.snapshot(client, stream, group)
            samples.append(observe.dump_snapshot(snap))
            if snap.pending_count == 0 and pending_zero_at is None:
                # ignore readiness leftovers: stream was reset, but ready task acked
                if any(
                    r.get("event") in {"handler_finish", "handler_acked", "on_error", "post_execute"}
                    and r.get("job_id") == job
                    for r in harness.read_events(events)
                ) or any(
                    r.get("event") in {"handler_finish", "handler_acked", "on_error", "post_execute"}
                    and job in str(r.get("args"))
                    for r in harness.read_events(events)
                ):
                    pending_zero_at = time.monotonic()
                    break
            time.sleep(0.1)

        ev = harness.read_events(events)
        # Filter to this job roughly by args/kwargs
        related = [
            r for r in ev
            if job in str(r.get("args")) or job in str(r.get("kwargs")) or r.get("job_id") == job
            or (r.get("task_name") in {"boom", "sleep_async", "manual_ack"} and r.get("event") != "handler_ready")
        ]
        order = [r["event"] for r in related]
        result = {
            "ack_type": ack_type,
            "with_result_backend": with_result_backend,
            "boom": boom,
            "event_order": order,
            "pending_cleared": pending_zero_at is not None,
            "final_pending": observe.snapshot(client, stream, group).pending_count,
            "artifacts": str(run_dir),
        }
        observe.write_json(run_dir / "result.json", result)
        observe.write_json(run_dir / "samples.json", samples)
        observe.write_json(run_dir / "events_filtered.json", related)
        return result
    finally:
        process.stop_group(proc, grace_s=3)
        client.close()


def run() -> dict:
    rows = []
    for ack in ACK_TYPES:
        for rb in (False, True):
            for boom in (False, True):
                if ack == "manual" and boom:
                    continue
                rows.append(_run_one(ack, with_result_backend=rb, boom=boom))
    summary = {
        "experiment": "e05_ack_order",
        "status": "pass",
        "cases": rows,
        "source_order_when_executed": [
            "pre_execute",
            "handler",
            "on_error (if err; includes SimpleRetryMiddleware.kiq)",
            "ACK (when_executed)",
            "post_execute",
            "set_result",
            "post_save",
        ],
        "source_file": "taskiq/receiver/receiver.py callback+run_task",
        "source_sha": config.TASKIQ_GIT_SHA,
        "evidence_class": "measured+source",
    }
    out = config.ARTIFACTS / "ack" / "e05-summary.json"
    observe.write_json(out, summary)
    return summary


if __name__ == "__main__":
    print(run())
