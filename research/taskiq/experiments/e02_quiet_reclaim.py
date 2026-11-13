"""
E02 / Scenario A: смерть worker, новых сообщений нет.

Ожидание по исходникам 1.2.4: пустой XREADGROUP → continue до XAUTOCLAIM,
поэтому на тихом потоке reclaim не происходит даже после idle_timeout.
"""

from __future__ import annotations

import time

from research.taskiq.lib import config, harness, observe, process


def run(*, idle_ms: int = 3000, observe_s: float | None = None) -> dict:
    observe_s = observe_s if observe_s is not None else (idle_ms / 1000.0) + 8.0
    run_dir = harness.new_run_dir("recovery", "e02-quiet-reclaim")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns("e02")
    redis_url = config.DEFAULT_REDIS_URL
    harness.reset_stream(redis_url, stream, group)
    env = harness.base_env(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        idle_ms=idle_ms,
        xread_count=10,
        xread_block=200,
        lock_s=2,
    )
    # Worker A — будет убит
    proc_a, _ = harness.readiness_handshake(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        env=env,
        workers=1,
        max_async=1,
    )
    client = observe.connect(redis_url)
    try:
        harness.enqueue("sleep_async", "victim", 120.0, redis_url=redis_url, env=env)
        started = harness.wait_event(
            events,
            lambda r: r.get("event") == "handler_start" and r.get("job_id") == "victim",
            timeout_s=20,
        )
        assert started, "victim did not start"
        before_kill = observe.snapshot(client, stream, group)
        killed = process.sigkill_execution_child(proc_a)
        # Также убьём весь process group A, чтобы listen точно остановился
        process.stop_group(proc_a, grace_s=0.1, final_sig=9)
        time.sleep(0.3)
        after_kill = observe.snapshot(client, stream, group)

        # Worker B — живой consumer на том же stream/group
        events_b = run_dir / "events-b.jsonl"
        env_b = dict(env)
        env_b["TASKIQ_EVENTS_PATH"] = str(events_b)
        proc_b = process.start(
            harness.worker_cmd(workers=1, max_async=1, ack_type="when_executed"),
            env=env_b,
            cwd=config.ROOT,
            log_path=run_dir / "worker-b.log",
        )
        process.wait_alive(proc_b, timeout_s=10)
        time.sleep(0.5)

        samples = []
        t0 = time.monotonic()
        reclaimed = False
        while time.monotonic() - t0 < observe_s:
            snap = observe.snapshot(client, stream, group)
            samples.append(observe.dump_snapshot(snap))
            if any(
                r.get("event") == "handler_start" and r.get("job_id") == "victim"
                for r in harness.read_events(events_b)
            ):
                reclaimed = True
                break
            time.sleep(0.5)

        final = observe.snapshot(client, stream, group)
        # Negative invariant: no second start on quiet stream within observe window
        status = "pass" if (not reclaimed and final.pending_count >= 1) else "fail"
        # If somehow reclaimed, that's also an important finding — mark unexpected_pass
        if reclaimed:
            status = "unexpected_reclaim"

        result = {
            "experiment": "e02_quiet_reclaim",
            "scenario": "A_death_no_new_messages",
            "status": status,
            "idle_ms": idle_ms,
            "observe_s": observe_s,
            "killed_pids": killed,
            "before_kill": observe.dump_snapshot(before_kill),
            "after_kill": observe.dump_snapshot(after_kill),
            "final": observe.dump_snapshot(final),
            "reclaimed_by_b": reclaimed,
            "evidence_class": "measured",
            "source": {
                "file": "taskiq_redis/redis_broker.py",
                "lines": "287-288 continue before XAUTOCLAIM",
                "sha": config.TASKIQ_REDIS_GIT_SHA,
                "version": "1.2.4",
            },
            "artifacts": str(run_dir),
        }
        observe.write_json(run_dir / "result.json", result)
        observe.write_json(run_dir / "samples.json", samples)
        process.stop_group(proc_b, grace_s=2)
        return result
    finally:
        client.close()
        try:
            process.stop_group(proc_a, grace_s=0.5)
        except Exception:
            pass


if __name__ == "__main__":
    print(run())
