"""
E04 / Scenario C: смерть во время удержания autoclaim lock.

При unacknowledged_lock_timeout=TTL lock должен исчезнуть после TTL.
При default None (не задан) — lock может остаться навсегда (issue #131/#87).
"""

from __future__ import annotations

import asyncio
import time
import uuid

from redis.asyncio import Redis

from research.taskiq.lib import config, observe


def run(*, lock_timeout_s: float = 2.0, wait_s: float = 5.0) -> dict:
    run_dir_parent = config.ARTIFACTS / "recovery"
    run_dir_parent.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    run_dir = run_dir_parent / f"{stamp}-e04-lock-death"
    run_dir.mkdir(parents=True, exist_ok=True)

    redis_url = config.DEFAULT_REDIS_URL
    stream = f"e04:{uuid.uuid4().hex[:8]}"
    group = f"g-{uuid.uuid4().hex[:8]}"
    lock_key = f"autoclaim:{group}:{stream}"

    async def _simulate() -> dict:
        client = Redis.from_url(redis_url, decode_responses=True)
        try:
            await client.delete(stream, lock_key)
            # Simulate worker holding reclaim lock then dying without release.
            # This mirrors pipe.lock(...); acquire(); SIGKILL before release/execute.
            ok = await client.set(lock_key, "held-by-dead-worker", nx=True, ex=int(lock_timeout_s))
            assert ok
            exists_immediate = await client.exists(lock_key)
            ttl1 = await client.ttl(lock_key)
            await asyncio.sleep(wait_s)
            exists_after = await client.exists(lock_key)
            ttl2 = await client.ttl(lock_key)
            return {
                "lock_key": lock_key,
                "set_ok": bool(ok),
                "exists_immediate": int(exists_immediate),
                "ttl_immediate": ttl1,
                "exists_after_wait": int(exists_after),
                "ttl_after_wait": ttl2,
                "lock_timeout_s": lock_timeout_s,
                "wait_s": wait_s,
            }
        finally:
            await client.aclose()

    measured = asyncio.run(_simulate())

    # Also document default None semantics from source (no experiment mutation of library).
    result = {
        "experiment": "e04_lock_death",
        "scenario": "C_death_during_reclaim_lock",
        "status": "pass" if measured["exists_after_wait"] == 0 else "fail",
        "measured": measured,
        "source_default": {
            "unacknowledged_lock_timeout_default": None,
            "doc": "If not set, the lock can remain locked indefinitely.",
            "file": "taskiq_redis/redis_broker.py",
            "sha": config.TASKIQ_REDIS_GIT_SHA,
            "issues": [
                "https://github.com/taskiq-python/taskiq-redis/issues/87",
                "https://github.com/taskiq-python/taskiq-redis/issues/131",
                "https://github.com/taskiq-python/taskiq-redis/pull/107",
            ],
            "fix_released_in": "1.2.2 (optional timeout parameter; default still None)",
        },
        "evidence_class": "measured+source",
        "artifacts": str(run_dir),
    }
    observe.write_json(run_dir / "result.json", result)
    return result


if __name__ == "__main__":
    print(run())
