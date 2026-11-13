"""Адаптер Dramatiq 2.2.1. Async-актор идёт через AsyncIO: цикл в отдельном потоке."""

from __future__ import annotations

import asyncio
import os

import dramatiq
from dramatiq import group
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AsyncIO, CurrentMessage, GroupCallbacks, Middleware
from dramatiq.middleware.prometheus import Prometheus
from dramatiq.rate_limits.backends.redis import RedisBackend as RateLimitRedis

from bench import core

REDIS_URL = os.environ["REDIS_URL"]
HEARTBEAT_MS = int(os.environ.get("DRAMATIQ_HEARTBEAT_MS", "60000"))

broker = RedisBroker(url=REDIS_URL, heartbeat_timeout=HEARTBEAT_MS)
broker.add_middleware(AsyncIO())
broker.add_middleware(CurrentMessage())
broker.add_middleware(Prometheus())
broker.add_middleware(GroupCallbacks(RateLimitRedis(url=REDIS_URL)))
dramatiq.set_broker(broker)


def _who() -> str:
    return f"{os.uname().nodename}:{os.getpid()}"


def _meta() -> str:
    message = CurrentMessage.get_current_message()
    if message is None:
        return ""
    return f"retries={message.options.get('retries', 0)} id={message.message_id}"


class Deps(Middleware):
    def before_worker_shutdown(self, broker, worker) -> None:
        try:
            asyncio.run(core.close_deps())
        except RuntimeError:
            return


broker.add_middleware(Deps())


@dramatiq.actor(queue_name="bench", max_retries=0, time_limit=30_000)
async def probe(token: str) -> None:
    path = os.environ.get("PROBE_PATH", "/tmp/bench-probe")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(token + "\n")


@dramatiq.actor(queue_name="bench", max_retries=3, min_backoff=200, max_backoff=2_000, time_limit=120_000)
async def item(job_id: str, n: int) -> None:
    await core.do_item(job_id, int(n), worker=_who(), note=_meta())


@dramatiq.actor(queue_name="bench", max_retries=2, time_limit=120_000)
async def fanout(job_id: str, n: int) -> None:
    await core.note_attempt(job_id, -2, _who(), "fanout")
    for i in range(int(n)):
        item.send(job_id, i)


@dramatiq.actor(queue_name="bench", max_retries=3, min_backoff=200, max_backoff=2_000, time_limit=120_000)
async def chain(job_id: str, step: int, total: int) -> None:
    await core.note_attempt(job_id, int(step), _who(), "chain " + _meta())
    await core.mark_progress(job_id, int(step))
    nxt = int(step) + 1
    if nxt < int(total):
        if int(step) == 1:
            chain.send_with_options(args=(job_id, nxt, int(total)), delay=2000)
        else:
            chain.send(job_id, nxt, int(total))


@dramatiq.actor(
    queue_name="bench",
    max_retries=30,
    min_backoff=500,
    max_backoff=2_000,
    time_limit=600_000,
    notify_shutdown=True,
)
async def longjob(job_id: str, steps: int, step_s: float) -> None:
    await core.note_attempt(job_id, -1, _who(), "long " + _meta())
    await core.long_job(job_id, int(steps), float(step_s))


@dramatiq.actor(queue_name="bench", max_retries=2, min_backoff=100, max_backoff=300, time_limit=30_000,
                on_failure="failed_attempt")
async def boom(job_id: str) -> None:
    await core.note_attempt(job_id, 0, _who(), "boom " + _meta())
    raise RuntimeError("boom")


@dramatiq.actor(queue_name="bench", max_retries=1, min_backoff=100, max_backoff=200, time_limit=1_500,
                on_failure="failed_attempt")
async def limitjob(job_id: str) -> None:
    await core.note_attempt(job_id, 0, _who(), "limit " + _meta())
    await asyncio.sleep(30)


@dramatiq.actor(queue_name="bench", max_retries=0, time_limit=600_000)
async def snooze(job_id: str, n: int, seconds: float) -> None:
    await core.note_attempt(job_id, int(n), _who(), "snooze-start")
    await asyncio.sleep(float(seconds))
    await core.note_attempt(job_id, int(n), _who(), "snooze-end")


@dramatiq.actor(queue_name="bench", max_retries=0, time_limit=600_000)
async def blocker(job_id: str, seconds: float) -> None:
    import time

    await core.note_attempt(job_id, 0, _who(), "block-start")
    time.sleep(float(seconds))
    await core.note_attempt(job_id, 1, _who(), "block-end")


@dramatiq.actor(queue_name="bench", max_retries=0, time_limit=60_000)
async def fast(job_id: str, n: int, sent_ms: int) -> None:
    core.fast_record(job_id, int(n), int(sent_ms))


@dramatiq.actor(queue_name="bench", max_retries=0)
async def failed_attempt(message: dict, info: dict) -> None:
    kwargs = message.get("kwargs") or {}
    job_id = kwargs.get("job_id")
    if not job_id and message.get("args"):
        job_id = message["args"][0]
    if not job_id:
        return
    options = message.get("options") or {}
    retries = int(options.get("retries") or 0)
    actor = broker.get_actor(message.get("actor_name"))
    max_retries = int(options.get("max_retries", actor.options.get("max_retries", 20)))
    kind = str(info.get("type") or "")
    await core.note(str(job_id), "failure_attempt", f"type={kind} retries={retries} max={max_retries}")
    # Retries middleware increments options['retries'] before Callbacks runs.
    # The attempt that exhausts the budget is the one where the stored count
    # has already moved past max_retries.
    if retries > max_retries:
        reason = "timeout" if "TimeLimit" in kind else "retries"
        await core.mark_failed(str(job_id), f"{reason} {kind} retries={retries}")


@dramatiq.actor(queue_name="bench", max_retries=0)
async def grouped(job_id: str) -> None:
    await core.note(job_id, "group_done", "dramatiq")


OPS = {
    "probe": probe,
    "item": item,
    "fanout": fanout,
    "chain": chain,
    "longjob": longjob,
    "poisonjob": longjob,
    "boom": boom,
    "snooze": snooze,
    "blocker": blocker,
    "fast": fast,
    "limitjob": limitjob,
}


def enqueue(op: str, payload: dict) -> dict:
    if op == "group":
        job_id = payload["job_id"]
        n = int(payload["n"])
        messages = [item.message(job_id, i) for i in range(n)]
        bundle = group(messages)
        bundle.add_completion_callback(grouped.message(job_id))
        bundle.run()
        return {"accepted": True, "id": "group", "native_dedup": False, "native_group": True}
    actor = OPS[op]
    kwargs = payload.get("kwargs") or {}
    delay = payload.get("delay_ms")
    if kwargs:
        message = actor.send_with_options(kwargs=kwargs, delay=delay)
    else:
        args = payload.get("args") or []
        message = actor.send_with_options(args=tuple(args), delay=delay)
    return {
        "accepted": True,
        "id": message.message_id,
        "native_dedup": False,
        "duplicate_rejected": False,
    }


def _text(value: bytes | str) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return value


def redis_snapshot(client) -> dict:
    """Снимок ключей Dramatiq 2.2.

    Типы из ``brokers/redis/dispatch.lua``: очередь и ``*.DQ`` — list,
    ``*.msgs`` — hash, ``*.XQ`` и ``__heartbeats__`` — sorted set.
    ``ZCARD`` по delay-очереди и ``HGETALL`` по heartbeats дают WRONGTYPE,
    как только воркер жив или в очереди есть отложенное сообщение.
    """
    keys = [_text(key) for key in client.keys("dramatiq*")[:80]]
    queues = {}
    for name in ("bench", "default"):
        queues[name] = {
            "msgs": int(client.hlen(f"dramatiq:{name}.msgs")),
            "queue": int(client.llen(f"dramatiq:{name}")),
            "dq": int(client.llen(f"dramatiq:{name}.DQ")),
            "xq": int(client.zcard(f"dramatiq:{name}.XQ")),
        }
    heartbeats = {
        _text(member): int(score)
        for member, score in client.zrange("dramatiq:__heartbeats__", 0, -1, withscores=True)
    }
    return {"queues": queues, "heartbeats": heartbeats, "keys": keys}


def admin_snapshot() -> dict:
    import redis

    return redis_snapshot(redis.Redis.from_url(REDIS_URL))
