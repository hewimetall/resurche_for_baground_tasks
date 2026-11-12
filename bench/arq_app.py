"""Адаптер arq 0.28. Нативный asyncio, один процесс на команду arq."""

from __future__ import annotations

import asyncio
import os
import socket

from arq import Retry, create_pool, func
from arq.connections import RedisSettings
from arq.jobs import Job

from bench import core

REDIS_URL = os.environ["REDIS_URL"]


def _settings() -> RedisSettings:
    return RedisSettings.from_dsn(REDIS_URL)


def _who(ctx) -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _meta(ctx) -> str:
    return f"try={ctx.get('job_try')} id={ctx.get('job_id')}"


async def probe(ctx, token: str) -> None:
    path = os.environ.get("PROBE_PATH", "/tmp/bench-probe")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(token + "\n")


async def item(ctx, job_id: str, n: int) -> None:
    await core.do_item(job_id, int(n), worker=_who(ctx), note=_meta(ctx))


async def fanout(ctx, job_id: str, n: int) -> None:
    await core.note_attempt(job_id, -2, _who(ctx), "fanout")
    pool = ctx["redis"]
    for i in range(int(n)):
        await pool.enqueue_job("item", job_id, i, _job_id=f"item:{job_id}:{i}")


async def chain(ctx, job_id: str, step: int, total: int) -> None:
    await core.note_attempt(job_id, int(step), _who(ctx), "chain " + _meta(ctx))
    await core.mark_progress(job_id, int(step))
    nxt = int(step) + 1
    if nxt < int(total):
        defer = 2 if int(step) == 1 else None
        await ctx["redis"].enqueue_job(
            "chain",
            job_id,
            nxt,
            int(total),
            _job_id=f"chain:{job_id}:{nxt}",
            _defer_by=defer,
        )


async def longjob(ctx, job_id: str, steps: int, step_s: float) -> None:
    await core.note_attempt(job_id, -1, _who(ctx), "long " + _meta(ctx))
    try:
        await core.long_job(job_id, int(steps), float(step_s))
    except core.Draining:
        raise Retry(defer=1)


async def boom(ctx, job_id: str) -> None:
    await core.note_attempt(job_id, 0, _who(ctx), "boom " + _meta(ctx))
    tries = int(ctx.get("job_try") or 1)
    if tries >= 3:
        await core.mark_failed(job_id, f"retries try={tries}")
    raise RuntimeError("boom")


async def limitjob(ctx, job_id: str) -> None:
    await core.note_attempt(job_id, 0, _who(ctx), "limit " + _meta(ctx))
    await asyncio.sleep(30)


async def snooze(ctx, job_id: str, n: int, seconds: float) -> None:
    await core.note_attempt(job_id, int(n), _who(ctx), "snooze-start")
    await asyncio.sleep(float(seconds))
    await core.note_attempt(job_id, int(n), _who(ctx), "snooze-end")


async def blocker(ctx, job_id: str, seconds: float) -> None:
    import time

    await core.note_attempt(job_id, 0, _who(ctx), "block-start " + _meta(ctx))
    time.sleep(float(seconds))
    await core.note_attempt(job_id, 1, _who(ctx), "block-end " + _meta(ctx))


async def fast(ctx, job_id: str, n: int, sent_ms: int) -> None:
    core.fast_record(job_id, int(n), int(sent_ms))


def _seconds(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


_JOB_TIMEOUT = _seconds("ARQ_JOB_TIMEOUT", 300)
_LONG_TIMEOUT = _seconds("ARQ_LONG_TIMEOUT", _JOB_TIMEOUT)
_BLOCK_TIMEOUT = _seconds("ARQ_BLOCK_TIMEOUT", _JOB_TIMEOUT)


async def startup(ctx) -> None:
    await core.open_deps()


async def shutdown(ctx) -> None:
    await core.close_deps()


async def job_end(ctx) -> None:
    return


class WorkerSettings:
    functions = [
        func(probe, name="probe", timeout=30, max_tries=1),
        func(item, name="item", timeout=120, max_tries=4),
        func(fanout, name="fanout", timeout=120, max_tries=3),
        func(chain, name="chain", timeout=120, max_tries=4),
        func(longjob, name="longjob", timeout=_LONG_TIMEOUT, max_tries=20),
        func(boom, name="boom", timeout=30, max_tries=3),
        func(limitjob, name="limitjob", timeout=2, max_tries=2),
        func(snooze, name="snooze", timeout=_JOB_TIMEOUT, max_tries=1),
        func(blocker, name="blocker", timeout=_BLOCK_TIMEOUT, max_tries=2),
        func(fast, name="fast", timeout=60, max_tries=1),
    ]
    redis_settings = _settings()
    on_startup = startup
    on_shutdown = shutdown
    on_job_end = job_end
    max_jobs = int(os.environ.get("ARQ_MAX_JOBS", "4"))
    job_timeout = _JOB_TIMEOUT
    max_tries = int(os.environ.get("ARQ_MAX_TRIES", "5"))
    job_completion_wait = int(os.environ.get("ARQ_JOB_COMPLETION_WAIT", "60"))
    health_check_interval = 5
    allow_abort_jobs = True
    retry_jobs = True
    queue_name = os.environ.get("ARQ_QUEUE", "arq:queue")


async def enqueue_many(payload: dict) -> dict:
    pool = await create_pool(_settings())
    accepted = 0
    rejected = 0
    started = asyncio.get_event_loop().time()
    try:
        op = payload["op"]
        count = int(payload["count"])
        job_id = payload["job_id"]
        for n in range(count):
            kwargs = _many_kwargs(op, job_id, n, payload)
            job = await pool.enqueue_job(op, *_args(op, kwargs))
            if job is None:
                rejected += 1
            else:
                accepted += 1
        return {"accepted": accepted, "rejected": rejected, "seconds": round(asyncio.get_event_loop().time() - started, 3)}
    finally:
        await pool.close()


def _many_kwargs(op: str, job_id: str, n: int, payload: dict) -> dict:
    if op == "fast":
        import time

        return {"job_id": job_id, "n": n, "sent_ms": int(time.time() * 1000)}
    if op == "snooze":
        return {"job_id": job_id, "n": n, "seconds": payload.get("seconds", 1)}
    return {"job_id": job_id, "n": n}


async def enqueue(op: str, payload: dict) -> dict:
    pool = await create_pool(_settings())
    try:
        kwargs = payload.get("kwargs") or {}
        job_id = payload.get("unique_key")
        defer = payload.get("defer_s")
        if payload.get("delay_ms"):
            defer = payload["delay_ms"] / 1000
        args = _args(op, kwargs)
        job = await pool.enqueue_job(op if op != "poisonjob" else "longjob", *args, _job_id=job_id, _defer_by=defer)
        if job is None:
            return {"accepted": False, "id": job_id, "native_dedup": True}
        return {"accepted": True, "id": job.job_id, "native_dedup": True}
    finally:
        await pool.close()


def _args(op: str, kwargs: dict) -> tuple:
    order = {
        "probe": ("token",),
        "item": ("job_id", "n"),
        "fanout": ("job_id", "n"),
        "chain": ("job_id", "step", "total"),
        "longjob": ("job_id", "steps", "step_s"),
        "poisonjob": ("job_id", "steps", "step_s"),
        "boom": ("job_id",),
        "snooze": ("job_id", "n", "seconds"),
        "blocker": ("job_id", "seconds"),
        "fast": ("job_id", "n", "sent_ms"),
        "limitjob": ("job_id",),
    }[op]
    return tuple(kwargs[name] for name in order)


async def admin_snapshot() -> dict:
    import redis.asyncio as redis

    client = redis.from_url(REDIS_URL)
    queue = os.environ.get("ARQ_QUEUE", "arq:queue")
    try:
        queued = await client.zcard(queue)
        health = await client.get(queue + ":health-check")
        in_progress = []
        async for key in client.scan_iter(match="arq:in-progress:*", count=50):
            in_progress.append(key.decode() if isinstance(key, bytes) else key)
            if len(in_progress) >= 20:
                break
        return {
            "queue": queue,
            "queued": queued,
            "health": None if health is None else health.decode(),
            "in_progress_sample": in_progress,
        }
    finally:
        await client.aclose()
