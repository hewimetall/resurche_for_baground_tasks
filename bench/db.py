"""Схема, сброс и инвариант. Вызывается стендом, не воркером."""

from __future__ import annotations

import os
from pathlib import Path

import asyncpg

SCHEMA = Path(__file__).with_name("schema.sql").read_text()


def dsn() -> str:
    return os.environ["PG_DSN"]


async def connect() -> asyncpg.Connection:
    return await asyncpg.connect(dsn())


async def ensure_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(SCHEMA)


async def reset(conn: asyncpg.Connection) -> None:
    await conn.execute(
        "truncate jobs, expected, attempts, results, progress, notes restart identity"
    )


async def invariant(conn: asyncpg.Connection, job_id: str) -> dict:
    row = await conn.fetchrow(
        """
        select count(*) as expected,
               count(*) filter (where r.item is null) as lost,
               count(*) filter (where r.executions > 1) as duplicated
        from expected e
        left join results r using (job_id, item)
        where e.job_id = $1
        """,
        job_id,
    )
    calls = await conn.fetchrow(
        """
        select count(*) as attempts,
               count(distinct item) as distinct_items,
               min(started_at) as first_start,
               max(started_at) as last_start
        from attempts where job_id = $1 and item >= 0
        """,
        job_id,
    )
    job = await conn.fetchrow("select status, fail_reason, cancelled, expected_n from jobs where id = $1", job_id)
    progress = await conn.fetchval("select count(*) from progress where job_id = $1", job_id)
    skips = await conn.fetchval(
        "select count(*) from notes where job_id = $1 and kind = 'skip'", job_id
    )
    return {
        "expected": row["expected"],
        "lost": row["lost"],
        "duplicated": row["duplicated"],
        "attempts": calls["attempts"],
        "distinct_items": calls["distinct_items"],
        "progress_steps": progress,
        "skips": skips,
        "status": None if job is None else job["status"],
        "fail_reason": None if job is None else job["fail_reason"],
        "cancelled": None if job is None else job["cancelled"],
        "expected_n": None if job is None else job["expected_n"],
    }


async def gap_s(conn: asyncpg.Connection, job_id: str, sigterm_epoch: float) -> dict:
    row = await conn.fetchrow(
        """
        select extract(epoch from min(started_at)) - $2 as gap,
               count(*) as starts_after
        from attempts
        where job_id = $1 and started_at > to_timestamp($2)
        """,
        job_id,
        sigterm_epoch,
    )
    return {
        "gap_s": None if row["gap"] is None else float(row["gap"]),
        "starts_after": row["starts_after"],
    }


async def latency(conn: asyncpg.Connection, job_id: str) -> dict:
    rows = await conn.fetch(
        """
        select extract(epoch from (a.started_at - e.sent_at)) * 1000 as start_ms,
               extract(epoch from (r.finished_at - e.sent_at)) * 1000 as done_ms
        from expected e
        left join lateral (
            select started_at from attempts
            where job_id = e.job_id and item = e.item
            order by started_at asc limit 1
        ) a on true
        left join results r using (job_id, item)
        where e.job_id = $1
        """,
        job_id,
    )
    start = sorted(float(r["start_ms"]) for r in rows if r["start_ms"] is not None)
    done = sorted(float(r["done_ms"]) for r in rows if r["done_ms"] is not None)
    return {"start_ms": _pct(start), "done_ms": _pct(done), "n_start": len(start), "n_done": len(done)}


def _pct(values: list[float]) -> dict:
    if not values:
        return {}

    def pick(q: float) -> float:
        idx = min(len(values) - 1, max(0, int(round(q * (len(values) - 1)))))
        return round(values[idx], 2)

    return {"p50": pick(0.50), "p99": pick(0.99), "max": round(values[-1], 2)}
