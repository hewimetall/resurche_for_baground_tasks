"""
E06: долгая живая задача vs idle_timeout с двумя независимыми worker.

Проверяет: второй consumer может ли забрать живую задачу после idle,
пересекаются ли исполнения, обновляется ли pending idle, есть ли продление.
"""

from __future__ import annotations

import time

from research.taskiq.lib import config, harness, observe, process


def run(
    *,
    kind: str = "sleep_async",
    idle_ms: int = 4000,
    task_s: float = 12.0,
    observe_s: float | None = None,
) -> dict:
    observe_s = observe_s if observe_s is not None else task_s + (idle_ms / 1000.0) + 6.0
    run_dir = harness.new_run_dir("long", f"e06-long-{kind}")
    events_a = run_dir / "events-a.jsonl"
    events_b = run_dir / "events-b.jsonl"
    stream, group = harness.unique_ns("e06")
    redis_url = config.DEFAULT_REDIS_URL
    harness.reset_stream(redis_url, stream, group)

    def make_env(events_path):
        return harness.base_env(
            redis_url=redis_url,
            stream=stream,
            group=group,
            events_path=events_path,
            idle_ms=idle_ms,
            xread_count=1,  # minimize batch confounders
            xread_block=200,
            lock_s=2,
            extra={"TASK_TIMEOUT_S": str(int(task_s) + 600)},
        )

    env_a = make_env(events_a)
    proc_a, _ = harness.readiness_handshake(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events_a,
        env=env_a,
        workers=1,
        max_async=1,
    )

    client = observe.connect(redis_url)
    proc_b = None
    try:
        job = f"long-{kind}"
        if kind == "tick":
            steps = max(int(task_s / 0.5), 2)
            harness.enqueue("tick", job, steps, 0.5, redis_url=redis_url, env=env_a)
        else:
            harness.enqueue(kind, job, task_s, redis_url=redis_url, env=env_a)

        started_a = harness.wait_event(
            events_a,
            lambda r: r.get("event") == "handler_start" and r.get("job_id") == job,
            timeout_s=20,
        )
        assert started_a, "A did not start"

        # Start B only after A owns the long task.
        env_b = make_env(events_b)
        proc_b = process.start(
            harness.worker_cmd(workers=1, max_async=1),
            env=env_b,
            cwd=config.ROOT,
            log_path=run_dir / "worker-b.log",
        )
        process.wait_alive(proc_b, timeout_s=10)
        time.sleep(0.3)

        samples = []
        overlap = False
        reclaim_start_b = None
        t0 = time.monotonic()
        while time.monotonic() - t0 < observe_s:
            snap = observe.snapshot(client, stream, group)
            samples.append(observe.dump_snapshot(snap))
            starts_b = [
                r for r in harness.read_events(events_b)
                if r.get("event") == "handler_start" and r.get("job_id") == job
            ]
            if starts_b and reclaim_start_b is None:
                reclaim_start_b = starts_b[0]["ts_mono"] - started_a["ts_mono"]
            finishes_a = [
                r for r in harness.read_events(events_a)
                if r.get("event") == "handler_finish" and r.get("job_id") == job
            ]
            if starts_b and not finishes_a:
                overlap = True
            if finishes_a and (reclaim_start_b is not None or time.monotonic() - t0 > task_s + 2):
                # allow drain
                time.sleep(0.5)
                break
            # Keep a little traffic so XAUTOCLAIM is reachable (known gating).
            if int(time.monotonic() - t0) % 2 == 0:
                harness.enqueue(
                    "fast",
                    f"keepalive-{int(time.monotonic())}",
                    0,
                    time.monotonic(),
                    redis_url=redis_url,
                    env=env_b,
                )
            time.sleep(0.5)

        starts_a = [r for r in harness.read_events(events_a) if r.get("event") == "handler_start" and r.get("job_id") == job]
        starts_b = [r for r in harness.read_events(events_b) if r.get("event") == "handler_start" and r.get("job_id") == job]
        finishes_a = [r for r in harness.read_events(events_a) if r.get("event") == "handler_finish" and r.get("job_id") == job]
        finishes_b = [r for r in harness.read_events(events_b) if r.get("event") == "handler_finish" and r.get("job_id") == job]

        # Idle values while A was running
        idle_while_alive = [s["pending"][0]["idle_ms"] for s in samples if s.get("pending")]

        result = {
            "experiment": "e06_long_ownership",
            "kind": kind,
            "idle_ms": idle_ms,
            "task_s": task_s,
            "status": "pass",
            "starts_a": len(starts_a),
            "starts_b": len(starts_b),
            "finishes_a": len(finishes_a),
            "finishes_b": len(finishes_b),
            "overlap_while_a_unfinished": overlap,
            "reclaim_delay_s": reclaim_start_b,
            "idle_ms_min_max_while_sampled": (
                [min(idle_while_alive), max(idle_while_alive)] if idle_while_alive else None
            ),
            "ownership_extension_api": False,
            "evidence_class": "measured",
            "note": (
                "Traffic keepalive used so XAUTOCLAIM is reachable; "
                "quiet-queue gating is covered by e02."
            ),
            "artifacts": str(run_dir),
        }
        # Mark fail only if instrumentation broke
        if not starts_a:
            result["status"] = "fail"
        observe.write_json(run_dir / "result.json", result)
        observe.write_json(run_dir / "samples.json", samples)
        return result
    finally:
        process.stop_group(proc_a, grace_s=2)
        if proc_b is not None:
            process.stop_group(proc_b, grace_s=2)
        client.close()


def run_matrix() -> dict:
    rows = []
    for kind in ("sleep_async", "sleep_block", "sleep_sync", "tick"):
        rows.append(run(kind=kind, idle_ms=4000, task_s=12.0))
    summary = {"experiment": "e06_matrix", "cases": rows}
    observe.write_json(config.ARTIFACTS / "long" / "e06-matrix.json", summary)
    return summary


if __name__ == "__main__":
    import sys

    if "--matrix" in sys.argv:
        print(run_matrix())
    else:
        print(run())
