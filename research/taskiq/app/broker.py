"""Минимальный брокер стенда. Не дублирует production-алгоритм Taskiq."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import time
from pathlib import Path

from taskiq import Context, SimpleRetryMiddleware, TaskiqDepends, TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend, RedisStreamBroker

REDIS_URL = os.environ.get("TASKIQ_RESEARCH_REDIS_URL", "redis://127.0.0.1:6413/0")
STREAM = os.environ.get("TASKIQ_RESEARCH_STREAM", "tqresearch")
GROUP = os.environ.get("TASKIQ_RESEARCH_GROUP", "tqresearch")
IDLE_MS = int(os.environ.get("TASKIQ_IDLE_MS", "600000"))
XREAD_BLOCK = int(os.environ.get("TASKIQ_XREAD_BLOCK", "2000"))
XREAD_COUNT = int(os.environ.get("TASKIQ_XREAD_COUNT", "100"))
LOCK_TIMEOUT = os.environ.get("TASKIQ_UNACK_LOCK_S")
EVENTS_PATH = Path(os.environ.get("TASKIQ_EVENTS_PATH", "/tmp/taskiq-research-events.jsonl"))
USE_RESULT_BACKEND = os.environ.get("TASKIQ_RESULT_BACKEND", "0") == "1"
BROKER_KIND = os.environ.get("TASKIQ_BROKER_KIND", "stream")  # stream|list


class EventMiddleware(TaskiqMiddleware):
    """Пишет границы lifecycle в jsonl для AAA-ассертов."""

    def _write(self, event: str, message: TaskiqMessage, **extra) -> None:
        EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "ts_mono": time.monotonic(),
            "event": event,
            "task_id": message.task_id,
            "task_name": message.task_name,
            "labels": dict(message.labels),
            "args": list(message.args),
            "kwargs": dict(message.kwargs),
            "pid": os.getpid(),
            "host": socket.gethostname(),
            **extra,
        }
        with EVENTS_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        self._write("pre_execute", message)
        return message

    async def post_execute(self, message: TaskiqMessage, result: TaskiqResult) -> None:
        self._write(
            "post_execute",
            message,
            is_err=result.is_err,
            error=repr(result.error) if result.error else None,
            execution_time=result.execution_time,
        )

    async def post_save(self, message: TaskiqMessage, result: TaskiqResult) -> None:
        self._write("post_save", message, is_err=result.is_err)

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult,
        exception: BaseException,
    ) -> None:
        self._write("on_error", message, error=repr(exception))


_lock_timeout: float | None
if LOCK_TIMEOUT is None or LOCK_TIMEOUT == "":
    _lock_timeout = None
else:
    _lock_timeout = float(LOCK_TIMEOUT)

_middlewares = [
    SimpleRetryMiddleware(default_retry_count=3),
    EventMiddleware(),
]

if BROKER_KIND == "list":
    broker = ListQueueBroker(url=REDIS_URL, queue_name=STREAM).with_middlewares(*_middlewares)
else:
    broker = RedisStreamBroker(
        url=REDIS_URL,
        queue_name=STREAM,
        consumer_group_name=GROUP,
        idle_timeout=IDLE_MS,
        xread_block=XREAD_BLOCK,
        xread_count=XREAD_COUNT,
        unacknowledged_lock_timeout=_lock_timeout,
    ).with_middlewares(*_middlewares)

if USE_RESULT_BACKEND:
    broker = broker.with_result_backend(
        RedisAsyncResultBackend(redis_url=REDIS_URL, result_ex_time=3600)
    )


def _note(event: str, **payload) -> None:
    EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "ts_mono": time.monotonic(),
        "event": event,
        "pid": os.getpid(),
        "host": socket.gethostname(),
        **payload,
    }
    with EVENTS_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


@broker.task(task_name="ready", retry_on_error=False)
async def ready(token: str, context: Context = TaskiqDepends()) -> str:
    _note("handler_ready", token=token)
    # Under --ack-type manual the handshake must ack itself.
    if context.is_ackable and not context.is_acked:
        await context.ack()
    return token


@broker.task(task_name="sleep_async", retry_on_error=False, timeout=int(os.environ.get("TASK_TIMEOUT_S", "3600")))
async def sleep_async(job_id: str, seconds: float) -> str:
    _note("handler_start", job_id=job_id, task_kind="sleep_async", seconds=seconds)
    await asyncio.sleep(float(seconds))
    _note("handler_finish", job_id=job_id, task_kind="sleep_async")
    return job_id


@broker.task(task_name="sleep_block", retry_on_error=False, timeout=int(os.environ.get("TASK_TIMEOUT_S", "3600")))
async def sleep_block(job_id: str, seconds: float) -> str:
    """Блокирующий sleep внутри async handler — event loop не отдаёт управление."""
    _note("handler_start", job_id=job_id, task_kind="sleep_block", seconds=seconds)
    time.sleep(float(seconds))
    _note("handler_finish", job_id=job_id, task_kind="sleep_block")
    return job_id


@broker.task(task_name="sleep_sync", retry_on_error=False, timeout=int(os.environ.get("TASK_TIMEOUT_S", "3600")))
def sleep_sync(job_id: str, seconds: float) -> str:
    _note("handler_start", job_id=job_id, task_kind="sleep_sync", seconds=seconds)
    time.sleep(float(seconds))
    _note("handler_finish", job_id=job_id, task_kind="sleep_sync")
    return job_id


@broker.task(task_name="tick", retry_on_error=False, timeout=int(os.environ.get("TASK_TIMEOUT_S", "3600")))
async def tick(job_id: str, steps: int, step_s: float) -> str:
    _note("handler_start", job_id=job_id, task_kind="tick", steps=steps, step_s=step_s)
    for i in range(int(steps)):
        await asyncio.sleep(float(step_s))
        _note("handler_tick", job_id=job_id, step=i)
    _note("handler_finish", job_id=job_id, task_kind="tick")
    return job_id


@broker.task(task_name="fast", retry_on_error=False)
async def fast(job_id: str, n: int, sent_mono: float) -> dict:
    started = time.monotonic()
    _note(
        "handler_start",
        job_id=job_id,
        task_kind="fast",
        n=n,
        sent_mono=sent_mono,
        start_mono=started,
        lag_s=started - float(sent_mono),
    )
    _note("handler_finish", job_id=job_id, task_kind="fast", n=n, finish_mono=time.monotonic())
    return {"n": n, "lag_s": started - float(sent_mono)}


@broker.task(task_name="boom", retry_on_error=True, max_retries=3)
async def boom(job_id: str) -> None:
    _note("handler_start", job_id=job_id, task_kind="boom")
    raise RuntimeError(f"boom:{job_id}")


@broker.task(task_name="manual_ack", retry_on_error=False)
async def manual_ack(job_id: str, context: Context = TaskiqDepends()) -> str:
    _note("handler_start", job_id=job_id, task_kind="manual_ack")
    await asyncio.sleep(0.2)
    await context.ack()
    _note("handler_acked", job_id=job_id)
    return job_id


@broker.task(task_name="effect_then_fail", retry_on_error=False)
async def effect_then_fail(job_id: str) -> None:
    _note("external_effect", job_id=job_id)
    raise RuntimeError("after_effect")
