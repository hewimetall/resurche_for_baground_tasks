"""Реальная работа нужной длительности. Часы процесса не подменяются."""

from __future__ import annotations

import asyncio
import os
import time

import httpx

from bench.longrun.events import emit


def _message_id(explicit: str | None) -> str | None:
    if explicit:
        return explicit
    try:
        from simple_q import current_message

        message = current_message()
        if message is not None and getattr(message, "message_id", None):
            return str(message.message_id)
    except Exception:
        pass
    try:
        from dramatiq.middleware import CurrentMessage

        message = CurrentMessage.get_current_message()
        if message is not None:
            return str(message.message_id)
    except Exception:
        pass
    try:
        from celery import current_task

        request = getattr(current_task, "request", None)
        if request is not None and getattr(request, "id", None):
            return str(request.id)
    except Exception:
        pass
    return None


def _base(job_id: str, execution_id: str, message_id: str | None) -> dict:
    return {
        "run_id": os.environ["LONGRUN_RUN_ID"],
        "scenario_id": os.environ["LONGRUN_SCENARIO"],
        "library": os.environ["LONGRUN_LIBRARY"],
        "job_id": job_id,
        "execution_id": execution_id,
        "message_id": message_id,
        "worker_pid": os.getpid(),
    }


async def _step_body(kind: str, step_s: float, job_id: str, step: int) -> None:
    if kind == "sleep":
        await asyncio.sleep(step_s)
        return
    if kind == "block":
        time.sleep(step_s)
        return
    if kind == "cpu":
        end = time.monotonic() + step_s
        acc = 0
        while time.monotonic() < end:
            acc = (acc + 1) % 1_000_003
        return
    if kind == "http_hang":
        timeout = float(os.environ.get("LONGRUN_HTTP_CLIENT_TIMEOUT", "5"))
        async with httpx.AsyncClient() as client:
            await client.post(
                os.environ["LONGRUN_EFFECT_URL"] + f"/hang?s={step_s}",
                json={"job_id": job_id, "step_id": step, "kind": kind},
                timeout=timeout,
            )
        return
    raise RuntimeError(f"unknown kind {kind}")


async def _effect(job_id: str, step: int, execution_id: str) -> int:
    headers = {}
    if os.environ.get("LONGRUN_IDEMPOTENCY") == "1":
        headers["X-Idempotency-Key"] = f"{job_id}:{step}"
    body = {"job_id": job_id, "step_id": step, "execution_id": execution_id}
    async with httpx.AsyncClient() as client:
        response = await client.post(
            os.environ["LONGRUN_EFFECT_URL"] + "/effect",
            json=body,
            headers=headers,
            timeout=10,
        )
    return response.status_code


async def hold(
    job_id: str,
    steps: int,
    step_s: float,
    kind: str = "sleep",
    message_id: str | None = None,
) -> None:
    path = os.environ["LONGRUN_EVENTS"]
    execution_id = f"{os.getpid()}-{time.monotonic_ns()}"
    seen = _message_id(message_id)
    base = _base(job_id, execution_id, seen)
    emit(path, "handler_started", **base, step_id=0, reason=kind)
    try:
        for step in range(int(steps)):
            await _step_body(kind, float(step_s), job_id, step)
            emit(
                path,
                "external_effect_requested",
                **base,
                step_id=step,
                reason="post /effect" if kind != "http_hang" else "post /hang",
            )
            if kind != "http_hang":
                status = await _effect(job_id, step, execution_id)
                emit(
                    path,
                    "external_effect_recorded",
                    **base,
                    step_id=step,
                    reason=f"http {status}",
                )
            emit(path, "progress", **base, step_id=step, reason="step_done")
    except asyncio.CancelledError:
        emit(path, "handler_cancelled", **base, reason="CancelledError")
        raise
    except Exception as exc:
        emit(path, "handler_exception", **base, reason=type(exc).__name__)
        raise
    else:
        emit(path, "handler_finished", **base, reason="completed")
