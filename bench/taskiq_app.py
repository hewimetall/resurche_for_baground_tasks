"""Адаптер taskiq 0.13 + taskiq-redis 1.2.4, RedisStreamBroker."""

from __future__ import annotations

import asyncio
import os
import socket

from taskiq import SimpleRetryMiddleware, TaskiqMiddleware, TaskiqResult
from taskiq.message import TaskiqMessage
from taskiq.middlewares import PrometheusMiddleware
from taskiq_redis import RedisStreamBroker

from bench import core

REDIS_URL = os.environ["REDIS_URL"]
IDLE_MS = int(os.environ.get("TASKIQ_IDLE_MS", "600000"))


class FailHook(TaskiqMiddleware):
    """Финальный провал: свой код, у taskiq нет on_dead."""

    async def on_error(self, message: TaskiqMessage, result: TaskiqResult, exception: BaseException) -> None:
        retries = int(message.labels.get("_retries", 0)) + 1
        max_retries = int(message.labels.get("max_retries", 3))
        if retries < max_retries:
            return
        kwargs = message.kwargs or {}
        job_id = kwargs.get("job_id") or (message.args[0] if message.args else None)
        if not job_id:
            return
        name = type(exception).__name__
        reason = "timeout" if name in {"TimeoutError", "ExceptionGroup"} or "timeout" in name.lower() else "retries"
        await core.mark_failed(str(job_id), f"{reason} {name} retries={retries}")


_middlewares = [
    SimpleRetryMiddleware(default_retry_count=3),
    FailHook(),
]
if os.environ.get("TASKIQ_METRICS") == "1":
    _middlewares.append(
        PrometheusMiddleware(
            server_port=int(os.environ.get("TASKIQ_PROM_PORT", "9000")),
            server_addr="127.0.0.1",
        )
    )

broker = RedisStreamBroker(
    url=REDIS_URL,
    queue_name="bench",
    idle_timeout=IDLE_MS,
    unacknowledged_lock_timeout=5,
).with_middlewares(*_middlewares)


def _who() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


@broker.task(task_name="probe", retry_on_error=False, max_retries=1, timeout=30)
async def probe(token: str) -> None:
    path = os.environ.get("PROBE_PATH", "/tmp/bench-probe")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(token + "\n")


@broker.task(task_name="item", retry_on_error=True, max_retries=3, timeout=120)
async def item(job_id: str, n: int) -> None:
    await core.do_item(job_id, int(n), worker=_who())


@broker.task(task_name="fanout", retry_on_error=True, max_retries=2, timeout=120)
async def fanout(job_id: str, n: int) -> None:
    await core.note_attempt(job_id, -2, _who(), "fanout")
    for i in range(int(n)):
        await item.kiq(job_id, i)


@broker.task(task_name="chain", retry_on_error=True, max_retries=3, timeout=120)
async def chain(job_id: str, step: int, total: int) -> None:
    await core.note_attempt(job_id, int(step), _who(), "chain")
    await core.mark_progress(job_id, int(step))
    nxt = int(step) + 1
    if nxt < int(total):
        if int(step) == 1:
            await asyncio.sleep(2)
        await chain.kiq(job_id, nxt, int(total))


@broker.task(task_name="longjob", retry_on_error=True, max_retries=20, timeout=600)
async def longjob(job_id: str, steps: int, step_s: float) -> None:
    await core.note_attempt(job_id, -1, _who(), "long")
    await core.long_job(job_id, int(steps), float(step_s))


@broker.task(task_name="boom", retry_on_error=True, max_retries=3, timeout=30)
async def boom(job_id: str) -> None:
    await core.note_attempt(job_id, 0, _who(), "boom")
    raise RuntimeError("boom")


@broker.task(task_name="limitjob", retry_on_error=True, max_retries=2, timeout=1.5)
async def limitjob(job_id: str) -> None:
    await core.note_attempt(job_id, 0, _who(), "limit")
    await asyncio.sleep(30)


@broker.task(task_name="snooze", retry_on_error=False, max_retries=1, timeout=600)
async def snooze(job_id: str, n: int, seconds: float) -> None:
    await core.note_attempt(job_id, int(n), _who(), "snooze-start")
    await asyncio.sleep(float(seconds))
    await core.note_attempt(job_id, int(n), _who(), "snooze-end")


@broker.task(task_name="blocker", retry_on_error=False, max_retries=1, timeout=600)
async def blocker(job_id: str, seconds: float) -> None:
    import time

    await core.note_attempt(job_id, 0, _who(), "block-start")
    time.sleep(float(seconds))
    await core.note_attempt(job_id, 1, _who(), "block-end")


@broker.task(task_name="fast", retry_on_error=False, max_retries=1, timeout=60)
async def fast(job_id: str, n: int, sent_ms: int) -> None:
    core.fast_record(job_id, int(n), int(sent_ms))


TASKS = {
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


async def enqueue_many(payload: dict) -> dict:
    await broker.startup()
    accepted = 0
    started = asyncio.get_event_loop().time()
    try:
        op = payload["op"]
        task = TASKS[op]
        count = int(payload["count"])
        job_id = payload["job_id"]
        for n in range(count):
            if op == "fast":
                import time

                await task.kiq(job_id=job_id, n=n, sent_ms=int(time.time() * 1000))
            elif op == "snooze":
                await task.kiq(job_id=job_id, n=n, seconds=payload.get("seconds", 1))
            else:
                await task.kiq(job_id=job_id, n=n)
            accepted += 1
        return {"accepted": accepted, "rejected": 0, "seconds": round(asyncio.get_event_loop().time() - started, 3)}
    finally:
        await broker.shutdown()


async def enqueue(op: str, payload: dict) -> dict:
    await broker.startup()
    try:
        task = TASKS[op]
        kwargs = payload.get("kwargs") or {}
        kicker = task.kicker()
        unique = payload.get("unique_key")
        if unique:
            kicker = kicker.with_task_id(str(unique))
        sent = await kicker.kiq(**kwargs)
        return {"accepted": True, "id": sent.task_id, "native_dedup": False}
    finally:
        await broker.shutdown()


async def admin_snapshot() -> dict:
    from redis.asyncio import Redis

    client = Redis.from_url(REDIS_URL)
    try:
        pending = await client.xpending("bench", "taskiq")
        info = await client.xinfo_groups("bench")
        consumers = await client.xinfo_consumers("bench", "taskiq")
        return {
            "xlen": await client.xlen("bench"),
            "pending": _plain(pending),
            "groups": _plain(info),
            "consumers": _plain(consumers),
        }
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        await client.aclose()


def _plain(value):
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, dict):
        return {_plain(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value
