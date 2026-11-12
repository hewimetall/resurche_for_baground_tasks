"""Общая логика задач. Очередь сюда не импортируется."""

from __future__ import annotations

import asyncio
import os
import socket
import time

import asyncpg
import httpx

DRAIN_FLAG = os.environ.get("DRAIN_FLAG", "/tmp/draining")

_pool: asyncpg.Pool | None = None
_http: httpx.AsyncClient | None = None
_held: list[bytearray] = []
_open_lock: asyncio.Lock | None = None


class Draining(Exception):
    """Долгая задача сохранила чекпоинт. Адаптер ставит повтор."""


def _lock() -> asyncio.Lock:
    global _open_lock
    if _open_lock is None:
        _open_lock = asyncio.Lock()
    return _open_lock


async def open_deps() -> None:
    global _pool, _http
    if _pool is not None:
        return
    async with _lock():
        if _pool is None:
            _pool = await asyncpg.create_pool(os.environ["PG_DSN"], min_size=1, max_size=2)
            _http = httpx.AsyncClient(base_url=os.environ["MOCK_URL"], timeout=10)


async def close_deps() -> None:
    global _pool, _http, _open_lock
    if _pool is not None:
        await _pool.close()
        await _http.aclose()
        _pool = _http = None
    _open_lock = None


async def chaos(point: str) -> None:
    raw = os.getenv(f"CHAOS_{point.upper()}", "")
    mode, _, arg = raw.partition(":")
    if mode == "sleep":
        await asyncio.sleep(float(arg))
    elif mode == "block":
        time.sleep(float(arg))
    elif mode == "alloc":
        _held.append(bytearray(int(arg) * 2**20))
    elif mode == "exit":
        os._exit(137)


async def note_attempt(job_id: str, item: int, worker: str | None, note: str | None = None) -> None:
    await open_deps()
    await _pool.execute(
        "insert into attempts (job_id, item, worker, pid, note) values ($1, $2, $3, $4, $5)",
        job_id,
        item,
        worker or socket.gethostname(),
        os.getpid(),
        note,
    )


async def note(job_id: str | None, kind: str, body: str) -> None:
    await open_deps()
    await _pool.execute(
        "insert into notes (job_id, kind, body) values ($1, $2, $3)",
        job_id,
        kind,
        body[:4000],
    )


async def do_item(job_id: str, n: int, worker: str | None = None, note: str | None = None) -> None:
    await open_deps()
    await note_attempt(job_id, n, worker, note)
    await chaos("start")
    if await _pool.fetchval("select cancelled from jobs where id = $1", job_id):
        await _pool.execute(
            "insert into notes (job_id, kind, body) values ($1, 'skip', $2)",
            job_id,
            str(n),
        )
        return
    await _http.post("/call", json={"job": job_id, "item": n})
    await chaos("after_call")
    await _pool.execute(
        "insert into results (job_id, item) values ($1, $2) "
        "on conflict (job_id, item) do update set executions = results.executions + 1, "
        "finished_at = clock_timestamp()",
        job_id,
        n,
    )
    await _pool.execute(
        """
        update jobs set status = 'done'
        where id = $1 and status = 'running'
          and expected_n is not null
          and (select count(*) from results r where r.job_id = $1) >= jobs.expected_n
        """,
        job_id,
    )


async def long_job(job_id: str, steps: int, step_s: float) -> None:
    await open_deps()
    start = await _pool.fetchval(
        "select coalesce(max(step) + 1, 0) from progress where job_id = $1",
        job_id,
    )
    for step in range(int(start), int(steps)):
        if os.path.exists(DRAIN_FLAG):
            raise Draining(f"{job_id}: checkpoint at step {step}")
        await asyncio.sleep(float(step_s))
        await _pool.execute(
            "insert into progress values ($1, $2) on conflict do nothing",
            job_id,
            step,
        )


async def mark_progress(job_id: str, step: int) -> None:
    await open_deps()
    await _pool.execute(
        "insert into progress values ($1, $2) on conflict do nothing",
        job_id,
        int(step),
    )


async def mark_failed(job_id: str, reason: str) -> None:
    await open_deps()
    await _pool.execute(
        "update jobs set status = 'failed', fail_reason = $2 where id = $1 and status <> 'done'",
        job_id,
        reason,
    )
    await note(job_id, "failed", reason)


def fast_record(job_id: str, n: int, sent_ms: int) -> None:
    path = os.environ.get("LAT_PATH", f"/tmp/lat-{job_id}.txt")
    now = int(time.time() * 1000)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"{n},{sent_ms},{now}\n")
