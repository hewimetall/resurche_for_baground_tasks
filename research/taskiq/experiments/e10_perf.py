"""
E10: performance profiles (отдельно от correctness).
События: enqueue(sent_mono) → handler_start → handler_finish.
"""

from __future__ import annotations

import statistics
import time

from research.taskiq.lib import config, harness, observe, process


def _pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    seq = sorted(values)
    idx = min(len(seq) - 1, max(0, int(round((p / 100.0) * (len(seq) - 1)))))
    return seq[idx]


def run_profile(name: str, *, n: int, max_async: int = 4, workers: int = 1) -> dict:
    run_dir = harness.new_run_dir("performance", f"e10-{name}")
    events = run_dir / "events.jsonl"
    stream, group = harness.unique_ns("e10")
    redis_url = config.DEFAULT_REDIS_URL
    harness.reset_stream(redis_url, stream, group)
    env = harness.base_env(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        idle_ms=600_000,
        xread_count=50,
        xread_block=200,
        lock_s=5,
    )
    proc, _ = harness.readiness_handshake(
        redis_url=redis_url,
        stream=stream,
        group=group,
        events_path=events,
        env=env,
        workers=workers,
        max_async=max_async,
    )
    try:
        t0 = time.monotonic()
        if name == "idle_single":
            time.sleep(2.0)
            harness.enqueue_batch("fast", count=1, args=["p", 0, 0.0], redis_url=redis_url, env=env)
            expect = 1
        elif name == "short_burst":
            harness.enqueue_batch("fast", count=n, args=["p", 0, 0.0], redis_url=redis_url, env=env)
            expect = n
        elif name == "sustained":
            # enqueue in waves
            left = n
            while left > 0:
                batch = min(20, left)
                harness.enqueue_batch("fast", count=batch, args=["p", 0, 0.0], redis_url=redis_url, env=env)
                left -= batch
                time.sleep(0.05)
            expect = n
        elif name == "mixed":
            for i in range(n):
                if i % 5 == 0:
                    harness.enqueue("sleep_async", f"slow-{i}", 0.2, redis_url=redis_url, env=env)
                else:
                    harness.enqueue("fast", f"p-{i}", i, time.monotonic(), redis_url=redis_url, env=env)
            expect = n
        else:
            raise ValueError(name)

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            finishes = [
                r for r in harness.read_events(events)
                if r.get("event") == "handler_finish" and r.get("task_kind") in {"fast", "sleep_async"}
            ]
            # exclude readiness
            if len(finishes) >= expect:
                break
            time.sleep(0.05)
        t1 = time.monotonic()
        starts = [r for r in harness.read_events(events) if r.get("event") == "handler_start" and r.get("task_kind") == "fast"]
        lags = [float(r["lag_s"]) for r in starts if "lag_s" in r]
        result = {
            "profile": name,
            "n": n,
            "completed_fast_starts": len(starts),
            "wall_s": round(t1 - t0, 3),
            "throughput_per_s": round(len(starts) / max(t1 - t0, 1e-6), 3),
            "enqueue_to_handler_start_s": {
                "p50": _pct(lags, 50),
                "p95": _pct(lags, 95),
                "p99": _pct(lags, 99),
                "event_pair": "enqueue.sent_mono → handler_start.start_mono (lag_s)",
            },
            "workers": workers,
            "max_async": max_async,
            "artifacts": str(run_dir),
            "evidence_class": "measured",
        }
        observe.write_json(run_dir / "result.json", result)
        return result
    finally:
        process.stop_group(proc, grace_s=3)


def run() -> dict:
    rows = [
        run_profile("idle_single", n=1, max_async=1),
        run_profile("short_burst", n=50, max_async=8),
        run_profile("sustained", n=100, max_async=8),
        run_profile("mixed", n=40, max_async=4),
    ]
    summary = {"experiment": "e10_perf", "profiles": rows}
    observe.write_json(config.ARTIFACTS / "performance" / "e10-summary.json", summary)
    return summary


if __name__ == "__main__":
    print(run())
