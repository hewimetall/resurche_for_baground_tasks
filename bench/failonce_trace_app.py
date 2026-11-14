"""Same failonce contract as bench.simpleq_app, with a timestamped entry line.

The decorator arguments match failonce. The line is JSON so a run can tell
a new message id from a second entry of the same id. The first entry still
raises RuntimeError("fail once").
"""

from __future__ import annotations

import json
import os
import time

import simple_q
from simple_q import App, Client, Config, current_message

from bench import core

_client: Client | None = None


def client() -> Client:
    global _client
    if _client is None:
        _client = Client(os.environ["REDIS_URL"], Config.from_env())
    return _client


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
    current = current_message()
    path = os.environ.get("EFFECT_PATH", "/tmp/bench-effect")
    now = time.time()
    record = {
        "t": now,
        "utc": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)) + f".{int(now % 1 * 1000):03d}Z",
        "pid": os.getpid(),
        "token": token,
        "message_id": None if current is None else current.message_id,
        "retries": None if current is None else current.retries,
        "max_retries": None if current is None else current.max_retries,
        "deliveries": None if current is None else current.deliveries,
        "worker": None if current is None else current.worker_name,
    }
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()
    seen = [line for line in open(path, encoding="utf-8") if token in line]
    if len(seen) == 1:
        raise RuntimeError("fail once")


def create_app() -> App:
    return App(
        client=client(),
        actors=[failonce],
        on_start=[core.open_deps],
        on_stop=[core.close_deps],
    )
