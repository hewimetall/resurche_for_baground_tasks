"""Приложение для проверок механизмов 1.1.0–1.3.0.

Сценарии стенда его не вызывают. События пишутся в ``SQ130_EVENTS``.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import simple_q
from simple_q import App, Client, Config, current_message, worker_name


def _note(kind: str, token: str, **extra: Any) -> None:
    path = os.environ.get("SQ130_EVENTS")
    if not path:
        return
    message = current_message()
    row = {
        "t": time.time(),
        "kind": kind,
        "token": token,
        "pid": os.getpid(),
        "worker": worker_name(),
        "message_id": None if message is None else message.message_id,
        "retries": None if message is None else message.retries,
        "deliveries": None if message is None else message.deliveries,
    }
    row.update(extra)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")


@simple_q.actor(name="nap", queue="bench", max_retries=0, time_limit=180_000, max_deliveries=3)
async def nap(token: str, seconds: float) -> None:
    import asyncio

    _note("start", token)
    await asyncio.sleep(float(seconds))
    _note("end", token)


@simple_q.actor(name="alloc", queue="bench", max_retries=0, time_limit=180_000, max_deliveries=3)
async def alloc(token: str, mb: int, seconds: float) -> None:
    import asyncio

    _note("start", token)
    size = int(mb) * 1024 * 1024
    blob = bytearray(size)
    for offset in range(0, size, 4096):
        blob[offset] = 1
    _note("allocated", token, rss_hint=size)
    await asyncio.sleep(float(seconds))
    _note("end", token)
    del blob


@simple_q.actor(name="fail", queue="bench", max_retries=0, time_limit=30_000, max_deliveries=1)
async def fail(token: str) -> None:
    _note("start", token)
    raise RuntimeError("fail")


@simple_q.actor(name="once", queue="bench", max_retries=1, min_backoff=50, max_backoff=80, time_limit=30_000, max_deliveries=4)
async def once(token: str) -> None:
    _note("start", token)
    path = os.environ.get("SQ130_ONCE", "")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(token + "\n")
        if open(path, encoding="utf-8").read().count(token + "\n") == 1:
            raise RuntimeError("once")
    _note("end", token)


async def around(message: Any, call_next):
    _note(
        "dispatch",
        "",
        message_type=type(message).__name__,
        is_dict=isinstance(message, dict),
    )
    return await call_next()


def create_app() -> App:
    return App(
        client=Client(os.environ["REDIS_URL"], Config.from_env()),
        actors=[nap, alloc, fail, once],
        around_dispatch=[around],
    )
