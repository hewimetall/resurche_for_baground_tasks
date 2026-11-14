"""Адаптер simple-q 1.0.0rc7."""

from __future__ import annotations

import os
from typing import Any

import simple_q
from simple_q import App, Client, Config, DeadLetter, current_message, worker_name

from bench import core

_client: Client | None = None


def _cfg() -> Config:
    return Config.from_env()


def client() -> Client:
    global _client
    if _client is None:
        _client = Client(os.environ["REDIS_URL"], _cfg())
        _client.on_send(_trace_header)
    return _client


def _meta() -> str:
    message = current_message()
    if message is None:
        return ""
    return f"retries={message.retries} deliveries={message.deliveries} id={message.message_id}"


@simple_q.actor(
    name="probe",
    queue="bench",
    max_retries=0,
    time_limit=30_000,
    max_deliveries=3,
)
async def probe(token: str) -> None:
    path = os.environ.get("PROBE_PATH", "/tmp/bench-probe")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(token + "\n")


@simple_q.actor(
    name="pause",
    queue="bench",
    max_retries=0,
    time_limit=180_000,
    max_deliveries=3,
)
async def pause(token: str, seconds: float) -> None:
    import asyncio

    path = os.environ.get("EFFECT_PATH", "/tmp/bench-effect")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"start {token}\n")
        handle.flush()
    await asyncio.sleep(float(seconds))
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"end {token}\n")
        handle.flush()


@simple_q.actor(
    name="failonce",
    queue="bench",
    max_retries=1,
    min_backoff=100,
    max_backoff=300,
    time_limit=30_000,
    max_deliveries=3,
)
async def failonce(token: str) -> None:
    path = os.environ.get("EFFECT_PATH", "/tmp/bench-effect")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(token + "\n")
        handle.flush()
    seen = [line.strip() for line in open(path, encoding="utf-8") if line.strip()]
    if seen.count(token) == 1:
        raise RuntimeError("fail once")


@simple_q.actor(
    name="item",
    queue="bench",
    max_retries=3,
    min_backoff=200,
    max_backoff=2_000,
    time_limit=120_000,
    max_deliveries=5,
)
async def item(job_id: str, n: int) -> None:
    await core.do_item(job_id, int(n), worker=worker_name(), note=_meta())


@simple_q.actor(
    name="fanout",
    queue="bench",
    max_retries=2,
    min_backoff=200,
    max_backoff=2_000,
    time_limit=120_000,
    max_deliveries=5,
)
async def fanout(job_id: str, n: int) -> None:
    await core.note_attempt(job_id, -2, worker_name(), "fanout " + _meta())
    for i in range(int(n)):
        await client().send_with_options(
            item,
            kwargs={"job_id": job_id, "n": i},
            unique_key=f"item:{job_id}:{i}",
        )


@simple_q.actor(
    name="chain",
    queue="bench",
    max_retries=3,
    min_backoff=200,
    max_backoff=2_000,
    time_limit=120_000,
    max_deliveries=5,
)
async def chain(job_id: str, step: int, total: int) -> None:
    await core.note_attempt(job_id, int(step), worker_name(), "chain " + _meta())
    await core.mark_progress(job_id, int(step))
    nxt = int(step) + 1
    if nxt < int(total):
        delay = 2000 if int(step) == 1 else None
        await client().send_with_options(
            chain,
            kwargs={"job_id": job_id, "step": nxt, "total": int(total)},
            delay=delay,
            unique_key=f"chain:{job_id}:{nxt}",
        )


@simple_q.actor(
    name="longjob",
    queue="bench",
    max_retries=30,
    min_backoff=500,
    max_backoff=2_000,
    time_limit=600_000,
    max_deliveries=8,
)
async def longjob(job_id: str, steps: int, step_s: float) -> None:
    await core.note_attempt(job_id, -1, worker_name(), "long " + _meta())
    await core.long_job(job_id, int(steps), float(step_s))


@simple_q.actor(
    name="poisonjob",
    queue="bench",
    max_retries=30,
    min_backoff=200,
    max_backoff=1_000,
    time_limit=600_000,
    max_deliveries=5,
)
async def poisonjob(job_id: str, steps: int, step_s: float) -> None:
    await core.note_attempt(job_id, -1, worker_name(), "poison " + _meta())
    await core.long_job(job_id, int(steps), float(step_s))


@simple_q.actor(
    name="boom",
    queue="bench",
    max_retries=2,
    min_backoff=100,
    max_backoff=300,
    time_limit=30_000,
    max_deliveries=8,
)
async def boom(job_id: str) -> None:
    await core.note_attempt(job_id, 0, worker_name(), "boom " + _meta())
    raise RuntimeError("boom")


@simple_q.actor(
    name="snooze",
    queue="bench",
    max_retries=0,
    time_limit=600_000,
    max_deliveries=3,
)
async def snooze(job_id: str, n: int, seconds: float) -> None:
    import asyncio

    await core.note_attempt(job_id, int(n), worker_name(), "snooze-start")
    await asyncio.sleep(float(seconds))
    await core.note_attempt(job_id, int(n), worker_name(), "snooze-end")


@simple_q.actor(
    name="blocker",
    queue="bench",
    max_retries=0,
    time_limit=600_000,
    max_deliveries=3,
)
async def blocker(job_id: str, seconds: float) -> None:
    import time

    await core.note_attempt(job_id, 0, worker_name(), "block-start " + _meta())
    time.sleep(float(seconds))
    await core.note_attempt(job_id, 1, worker_name(), "block-end " + _meta())


@simple_q.actor(
    name="fast",
    queue="bench",
    max_retries=0,
    time_limit=60_000,
    max_deliveries=3,
)
async def fast(job_id: str, n: int, sent_ms: int) -> None:
    core.fast_record(job_id, int(n), int(sent_ms))


@simple_q.actor(
    name="limitjob",
    queue="bench",
    max_retries=1,
    min_backoff=100,
    max_backoff=200,
    time_limit=1_500,
    max_deliveries=5,
)
async def limitjob(job_id: str) -> None:
    import asyncio

    await core.note_attempt(job_id, 0, worker_name(), "limit " + _meta())
    await asyncio.sleep(30)


@simple_q.actor(
    name="hold",
    queue="bench",
    max_retries=int(os.environ.get("LONGRUN_MAX_RETRIES", "0")),
    min_backoff=500,
    max_backoff=2_000,
    time_limit=int(os.environ.get("LONGRUN_TIME_LIMIT_S", "3000")) * 1000,
    max_deliveries=int(os.environ.get("LONGRUN_MAX_DELIVERIES", "8")),
)
async def hold(job_id: str, steps: int, step_s: float, kind: str = "sleep") -> None:
    from bench.longrun.workload import hold as run_hold

    await run_hold(job_id, int(steps), float(step_s), kind)


async def on_dead(letter: DeadLetter) -> None:
    job_id = letter.kwargs.get("job_id")
    if not job_id:
        return
    await core.mark_failed(str(job_id), f"{letter.reason} deliveries={letter.deliveries} retries={letter.retries}")


async def around(message: Any, call_next):
    current = current_message()
    header = ""
    job_id = None
    if current is not None:
        header = (current.headers or {}).get("traceparent", "")
        job_id = (current.kwargs or {}).get("job_id")
    elif isinstance(message, dict):
        header = (message.get("headers") or {}).get("traceparent", "")
        job_id = (message.get("kwargs") or {}).get("job_id")
    if header:
        await core.note(str(job_id) if job_id else None, "traceparent", header)
    return await call_next()


def _trace_header(actor_name: str, kwargs: dict, headers: dict) -> dict:
    updated = dict(headers)
    updated.setdefault("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    return updated


def create_app() -> App:
    return App(
        client=client(),
        actors=[
            probe,
            pause,
            failonce,
            item,
            fanout,
            chain,
            longjob,
            poisonjob,
            boom,
            snooze,
            blocker,
            fast,
            limitjob,
            hold,
        ],
        on_start=[core.open_deps],
        on_stop=[core.close_deps],
        on_dead=[on_dead],
        around_dispatch=[around],
    )


ACTORS = {
    "probe": probe,
    "pause": pause,
    "failonce": failonce,
    "item": item,
    "fanout": fanout,
    "chain": chain,
    "longjob": longjob,
    "poisonjob": poisonjob,
    "boom": boom,
    "snooze": snooze,
    "blocker": blocker,
    "fast": fast,
    "limitjob": limitjob,
    "hold": hold,
}


async def enqueue(op: str, payload: dict) -> dict:
    actor = ACTORS[op]
    kwargs = dict(payload.get("kwargs") or {})
    unique = payload.get("unique_key")
    delay = payload.get("delay_ms")
    headers = payload.get("headers")
    options = {"kwargs": kwargs}
    if delay:
        options["delay"] = int(delay)
    if unique:
        options["unique_key"] = unique
    if headers:
        options["headers"] = headers
    message_id = await client().send_with_options(actor, **options)
    return {"accepted": message_id is not None, "id": message_id}


async def admin_snapshot() -> dict:
    from simple_q import QueueAdmin

    admin = QueueAdmin(client())
    snap = await admin.snapshot(["bench"])
    dead = await admin.dead("bench", limit=20)
    waiting = await admin.peek("bench", "active", limit=20)
    return {
        "snapshot": _freeze(snap),
        "dead": [_freeze(item) for item in dead.messages],
        "waiting": [_freeze(item) for item in waiting.tasks],
    }


def _freeze(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__") or hasattr(value, "_asdict"):
        raw = getattr(value, "__dict__", None)
        if raw:
            return {k: _freeze(v) for k, v in raw.items() if not k.startswith("_")}
    if isinstance(value, dict):
        return {str(k): _freeze(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_freeze(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
