"""
E03 / Scenario B: смерть worker при поступлении новых сообщений.

Новые сообщения дают непустой XREADGROUP → цикл доходит до XAUTOCLAIM.
После idle_timeout pending жертвы должен быть claimed.
"""

from __future__ import annotations

import time

from research.taskiq.lib import config, harness, observe, process


def run(*, idle_ms: int = 3000, observe_s: float | None = None) -> dict:
    observe_s = observe_s if observe_s is not None else (idle_ms / 1000.0) + 12.0
    run_dir = harness.new_run_dir("recovery", "e03-traffic-reclaim")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns("e03")
    redis_url = config.DEFAULT_REDIS_URL
    harness.reset_stream(redis_url, stream, group)
    env = harness.base_env(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        idle_ms=idle_ms,
        xread_count=5,
        xread_block=200,
        lock_s=2,
    )
    proc_a, _ = harness.readiness_handshake(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        env=env,
    )
    client = observe.connect(redis_url)
    try:
        harness.enqueue("sleep_async", "victim", 120.0, redis_url=redis_url, env=env)
        assert harness.wait_event(
            events,
            lambda r: r.get("event") == "handler_start" and r.get("job_id") == "victim",
            timeout_s=20,
        )
        before = observe.snapshot(client, stream, group)
        process.sigkill_execution_child(proc_a)
        process.stop_group(proc_a, grace_s=0.1, final_sig=9)
        time.sleep(0.3)

        events_b = run_dir / "events-b.jsonl"
        env_b = dict(env)
        env_b["TASKIQ_EVENTS_PATH"] = str(events_b)
        proc_b = process.start(
            harness.worker_cmd(workers=1, max_async=1),
            env=env_b,
            cwd=config.ROOT,
            log_path=run_dir / "worker-b.log",
        )
        process.wait_alive(proc_b, timeout_s=10)
        time.sleep(0.4)

        samples = []
        reclaimed_at = None
        traffic_ids = []
        t0 = time.monotonic()
        next_traffic = t0
        while time.monotonic() - t0 < observe_s:
            now = time.monotonic()
            if now >= next_traffic:
                tid = harness.enqueue(
                    "fast",
                    f"traffic-{len(traffic_ids)}",
                    len(traffic_ids),
                    now,
                    redis_url=redis_url,
                    env=env_b,
                )
                traffic_ids.append(tid)
                next_traffic = now + 1.0
            snap = observe.snapshot(client, stream, group)
            samples.append(observe.dump_snapshot(snap))
            if reclaimed_at is None and any(
                r.get("event") == "handler_start" and r.get("job_id") == "victim"
                for r in harness.read_events(events_b)
            ):
                reclaimed_at = now - t0
                # continue a bit to capture pending drop
            if reclaimed_at is not None and snap.pending_count == 0:
                break
            time.sleep(0.4)

        final = observe.snapshot(client, stream, group)
        victim_starts_b = [
            r for r in harness.read_events(events_b)
            if r.get("event") == "handler_start" and r.get("job_id") == "victim"
        ]
        status = "pass" if victim_starts_b else "fail"
        result = {
            "experiment": "e03_traffic_reclaim",
            "scenario": "B_death_with_new_messages",
            "status": status,
            "idle_ms": idle_ms,
            "observe_s": observe_s,
            "reclaimed_after_s": reclaimed_at,
            "before": observe.dump_snapshot(before),
            "final": observe.dump_snapshot(final),
            "victim_starts_on_b": len(victim_starts_b),
            "traffic_enqueued": len(traffic_ids),
            "evidence_class": "measured",
            "artifacts": str(run_dir),
        }
        observe.write_json(run_dir / "result.json", result)
        observe.write_json(run_dir / "samples.json", samples)
        process.stop_group(proc_b, grace_s=2)
        return result
    finally:
        client.close()


if __name__ == "__main__":
    print(run())
