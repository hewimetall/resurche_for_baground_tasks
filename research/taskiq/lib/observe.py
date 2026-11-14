"""Наблюдение Redis Streams без изменения production-алгоритма Taskiq."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from redis import Redis


@dataclass
class PendingEntry:
    message_id: str
    consumer: str
    idle_ms: int
    delivery_count: int


@dataclass
class StreamSnapshot:
    ts_mono: float
    stream: str
    group: str
    xlen: int
    pending_count: int
    pending: list[PendingEntry]
    consumers: list[dict[str, Any]]
    lock_exists: bool
    lock_ttl_ms: int
    autoclaim_cursor_hint: str | None = None


def connect(url: str) -> Redis:
    return Redis.from_url(url, decode_responses=True)


def snapshot(
    client: Redis,
    stream: str,
    group: str,
    *,
    lock_key: str | None = None,
) -> StreamSnapshot:
    lock_key = lock_key or f"autoclaim:{group}:{stream}"
    try:
        pending_raw = client.xpending_range(stream, group, min="-", max="+", count=200)
    except Exception:
        pending_raw = []
    try:
        summary = client.xpending(stream, group)
        pending_count = int(summary.get("pending", 0) if isinstance(summary, dict) else summary[0])
    except Exception:
        pending_count = len(pending_raw)
    pending: list[PendingEntry] = []
    for row in pending_raw:
        if isinstance(row, dict):
            pending.append(
                PendingEntry(
                    message_id=str(row["message_id"]),
                    consumer=str(row["consumer"]),
                    idle_ms=int(row["time_since_delivered"]),
                    delivery_count=int(row["times_delivered"]),
                )
            )
        else:
            pending.append(
                PendingEntry(
                    message_id=str(row[0]),
                    consumer=str(row[1]),
                    idle_ms=int(row[2]),
                    delivery_count=int(row[3]),
                )
            )
    try:
        consumers_raw = client.xinfo_consumers(stream, group)
    except Exception:
        consumers_raw = []
    consumers = []
    for c in consumers_raw:
        if isinstance(c, dict):
            consumers.append({k: (v.decode() if isinstance(v, bytes) else v) for k, v in c.items()})
        else:
            consumers.append({"name": c[0], "pending": c[1], "idle": c[2]})
    lock_exists = bool(client.exists(lock_key))
    ttl = int(client.pttl(lock_key)) if lock_exists else -2
    try:
        xlen = int(client.xlen(stream))
    except Exception:
        xlen = 0
    return StreamSnapshot(
        ts_mono=time.monotonic(),
        stream=stream,
        group=group,
        xlen=xlen,
        pending_count=pending_count,
        pending=pending,
        consumers=consumers,
        lock_exists=lock_exists,
        lock_ttl_ms=ttl,
    )


def dump_snapshot(snap: StreamSnapshot) -> dict[str, Any]:
    data = asdict(snap)
    return data


def append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
