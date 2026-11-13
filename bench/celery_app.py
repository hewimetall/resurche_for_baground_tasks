"""Адаптер Celery 5.7. Хендлер синхронный: свой event loop на процесс."""

from __future__ import annotations

import asyncio
import os

from celery import Celery, chord, current_task, group
from celery.signals import task_failure, worker_process_init, worker_process_shutdown

from bench import core

REDIS_URL = os.environ["REDIS_URL"]
VISIBILITY = int(os.environ.get("CELERY_VISIBILITY_TIMEOUT", "2100"))
MAX_TASKS = int(os.environ.get("CELERY_MAX_TASKS", "0"))
MAX_MEMORY_KB = int(os.environ.get("CELERY_MAX_MEMORY_KB", "0"))
SOFT_SHUTDOWN = float(os.environ.get("CELERY_SOFT_SHUTDOWN", "30"))

app = Celery("bench", broker=REDIS_URL, backend=REDIS_URL)
app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_default_queue="bench",
    broker_transport_options={"visibility_timeout": VISIBILITY},
    worker_soft_shutdown_timeout=SOFT_SHUTDOWN,
    result_expires=3600,
    task_ignore_result=False,
)
if MAX_TASKS > 0:
    app.conf.worker_max_tasks_per_child = MAX_TASKS
if MAX_MEMORY_KB > 0:
    app.conf.worker_max_memory_per_child = MAX_MEMORY_KB

_loop: asyncio.AbstractEventLoop | None = None


def _run(coro):
    if _loop is None:
        raise RuntimeError("celery child loop is not ready")
    return _loop.run_until_complete(coro)


def _who() -> tuple[str, str]:
    request = current_task.request if current_task else None
    host = getattr(request, "hostname", "") if request else ""
    retries = getattr(request, "retries", None) if request else None
    return f"{host}:{os.getpid()}", f"retries={retries}"


@worker_process_init.connect
def _init_loop(**_kwargs) -> None:
    global _loop
    _loop = asyncio.new_event_loop()


@worker_process_shutdown.connect
def _close_loop(**_kwargs) -> None:
    global _loop
    if _loop is not None:
        try:
            _loop.run_until_complete(core.close_deps())
        except Exception:
            pass
        _loop.close()
        _loop = None


@task_failure.connect
def _on_failure(sender=None, task_id=None, exception=None, args=None, kwargs=None, einfo=None, **_extra) -> None:
    job_id = None
    if kwargs and kwargs.get("job_id"):
        job_id = kwargs["job_id"]
    elif args:
        job_id = args[0]
    if not job_id:
        return
    name = type(exception).__name__ if exception else "error"
    reason = "timeout" if "TimeLimit" in name or "Time" in name else "retries"
    retries = 0
    max_retries = None
    if sender is not None:
        max_retries = getattr(sender, "max_retries", None)
        if getattr(sender, "request", None) is not None:
            retries = sender.request.retries or 0
    if max_retries is not None and retries < max_retries:
        return
    try:
        if _loop is not None:
            _loop.run_until_complete(core.mark_failed(str(job_id), f"{reason} {name} retries={retries}"))
        else:
            asyncio.run(core.mark_failed(str(job_id), f"{reason} {name} retries={retries}"))
    except Exception:
        return


@app.task(name="probe", max_retries=0, time_limit=30)
def probe(token: str) -> None:
    path = os.environ.get("PROBE_PATH", "/tmp/bench-probe")
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(token + "\n")


@app.task(name="item", max_retries=3, time_limit=120, soft_time_limit=110)
def item(job_id: str, n: int) -> None:
    who, meta = _who()
    _run(core.do_item(job_id, int(n), worker=who, note=meta))


@app.task(name="fanout", max_retries=2, time_limit=120)
def fanout(job_id: str, n: int) -> None:
    who, _meta = _who()
    _run(core.note_attempt(job_id, -2, who, "fanout"))
    for i in range(int(n)):
        item.apply_async(args=(job_id, i), queue="bench")


@app.task(name="chain", max_retries=3, time_limit=120)
def chain(job_id: str, step: int, total: int) -> None:
    who, meta = _who()
    _run(core.note_attempt(job_id, int(step), who, "chain " + meta))
    _run(core.mark_progress(job_id, int(step)))
    nxt = int(step) + 1
    if nxt < int(total):
        countdown = 2 if int(step) == 1 else 0
        chain.apply_async(args=(job_id, nxt, int(total)), countdown=countdown, queue="bench")


@app.task(name="longjob", bind=True, max_retries=30, time_limit=600)
def longjob(self, job_id: str, steps: int, step_s: float) -> None:
    who, meta = _who()
    _run(core.note_attempt(job_id, -1, who, "long " + meta))
    try:
        _run(core.long_job(job_id, int(steps), float(step_s)))
    except core.Draining as exc:
        raise self.retry(exc=exc, countdown=1, max_retries=30)


@app.task(
    name="boom",
    max_retries=2,
    autoretry_for=(Exception,),
    retry_backoff=0.1,
    retry_jitter=False,
    time_limit=30,
)
def boom(job_id: str) -> None:
    who, meta = _who()
    _run(core.note_attempt(job_id, 0, who, "boom " + meta))
    raise RuntimeError("boom")


@app.task(
    name="limitjob",
    max_retries=1,
    autoretry_for=(Exception,),
    retry_backoff=0.1,
    retry_jitter=False,
    soft_time_limit=2,
    time_limit=10,
)
def limitjob(job_id: str) -> None:
    who, meta = _who()
    _run(core.note_attempt(job_id, 0, who, "limit " + meta))
    import time

    time.sleep(30)


_HOLD_OPTS = {
    "name": "hold",
    "max_retries": int(os.environ.get("LONGRUN_MAX_RETRIES", "0")),
    "time_limit": int(os.environ.get("LONGRUN_TIME_LIMIT_S", "3000")),
}
if os.environ.get("LONGRUN_SOFT_LIMIT_S"):
    _HOLD_OPTS["soft_time_limit"] = int(os.environ["LONGRUN_SOFT_LIMIT_S"])


@app.task(**_HOLD_OPTS)
def hold(job_id: str, steps: int, step_s: float, kind: str = "sleep") -> None:
    from bench.longrun.workload import hold as run_hold

    _run(run_hold(job_id, int(steps), float(step_s), kind))


@app.task(name="snooze", max_retries=0, time_limit=600)
def snooze(job_id: str, n: int, seconds: float) -> None:
    who, _meta = _who()

    async def _body() -> None:
        await core.note_attempt(job_id, int(n), who, "snooze-start")
        await asyncio.sleep(float(seconds))
        await core.note_attempt(job_id, int(n), who, "snooze-end")

    _run(_body())


@app.task(name="blocker", max_retries=0, time_limit=600)
def blocker(job_id: str, seconds: float) -> None:
    import time

    who, _meta = _who()
    _run(core.note_attempt(job_id, 0, who, "block-start"))
    time.sleep(float(seconds))
    _run(core.note_attempt(job_id, 1, who, "block-end"))


@app.task(name="fast", max_retries=0, time_limit=60)
def fast(job_id: str, n: int, sent_ms: int) -> None:
    core.fast_record(job_id, int(n), int(sent_ms))


@app.task(name="grouped", max_retries=0)
def grouped(results, job_id: str) -> None:
    count = len(results) if isinstance(results, list) else 0
    _run(core.note(str(job_id), "group_done", f"celery n={count}"))


@app.task(name="rated", rate_limit="2/s", max_retries=0, time_limit=30)
def rated(job_id: str, n: int) -> None:
    who, _meta = _who()
    _run(core.note_attempt(job_id, int(n), who, "rated"))


def enqueue(op: str, payload: dict) -> dict:
    kwargs = payload.get("kwargs") or {}
    delay_ms = payload.get("delay_ms")
    countdown = None if not delay_ms else delay_ms / 1000
    if op == "group":
        job_id = payload["job_id"]
        n = int(payload["n"])
        header = group(item.s(job_id, i).set(queue="bench") for i in range(n))
        chord(header)(grouped.s(job_id).set(queue="bench"))
        return {"accepted": True, "id": "chord", "native_dedup": False, "native_group": True}
    if op == "rated":
        message = rated.apply_async(kwargs=kwargs, queue="bench")
        return {"accepted": True, "id": message.id, "native_dedup": False}
    task = app.tasks.get(op) or app.tasks.get(f"bench.{op}")
    named = {
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
        "hold": hold,
    }[op]
    task = named
    message = task.apply_async(kwargs=kwargs, countdown=countdown, queue="bench")
    return {"accepted": True, "id": message.id, "native_dedup": False}


def admin_snapshot() -> dict:
    inspect = app.control.inspect(timeout=1.5)
    return {
        "active": inspect.active(),
        "reserved": inspect.reserved(),
        "stats": inspect.stats(),
        "ping": inspect.ping(),
    }
