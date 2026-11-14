"""Retries-based retry actor for enqueue-fault research.

``retries=0`` always returns a retry (raises). ``retries>=1`` returns ok.
No global failonce counter: recovery of A with retries=0 still retries.
"""

from __future__ import annotations

import json
import os
import time
import uuid

import simple_q
from simple_q import App, Client, Config, current_message

_client: Client | None = None


def client() -> Client:
    global _client
    if _client is None:
        _client = Client(os.environ["REDIS_URL"], Config.from_env())
    return _client


def _journal(phase: str, execution_id: str, extra: dict | None = None) -> None:
    path = os.environ.get("EFFECT_PATH", "/tmp/retry-by-retries.effect")
    current = current_message()
    now = time.time()
    record = {
        "phase": phase,
        "execution_id": execution_id,
        "t": now,
        "utc": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now))
        + f".{int(now % 1 * 1000):03d}Z",
        "pid": os.getpid(),
        "message_id": None if current is None else current.message_id,
        "retries": None if current is None else current.retries,
        "max_retries": None if current is None else current.max_retries,
        "deliveries": None if current is None else current.deliveries,
        "worker": None if current is None else current.worker_name,
        "actor": None if current is None else current.actor_name,
        "token": (None if current is None else current.kwargs.get("token")),
    }
    if extra:
        record.update(extra)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()


@simple_q.actor(
    name="retry_by_retries",
    queue="bench",
    max_retries=3,
    min_backoff=50,
    max_backoff=80,
    time_limit=30_000,
    max_deliveries=5,
)
async def retry_by_retries(token: str) -> str:
    execution_id = str(uuid.uuid4())
    current = current_message()
    retries = 0 if current is None else int(current.retries)
    _journal("handler_start", execution_id, {"token": token})
    if retries == 0:
        _journal(
            "handler_end",
            execution_id,
            {"token": token, "result": "retry", "reason": "retries==0"},
        )
        raise RuntimeError("retry because retries==0")
    _journal(
        "handler_end",
        execution_id,
        {"token": token, "result": "ok", "reason": "retries>=1"},
    )
    return "ok"


@simple_q.actor(
    name="delay_probe",
    queue="bench",
    max_retries=0,
    min_backoff=50,
    max_backoff=80,
    time_limit=30_000,
    max_deliveries=3,
)
async def delay_probe(token: str, delay_ms: int) -> str:
    execution_id = str(uuid.uuid4())
    _journal(
        "handler_start",
        execution_id,
        {"token": token, "requested_delay_ms": delay_ms},
    )
    _journal(
        "handler_end",
        execution_id,
        {"token": token, "requested_delay_ms": delay_ms, "result": "ok"},
    )
    return "ok"


def create_app() -> App:
    return App(client=client(), actors=[retry_by_retries, delay_probe])
