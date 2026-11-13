"""Админка Dramatiq читает те типы ключей, которые пишет брокер 2.2."""

import os

import pytest
import redis

os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/15")

from bench.dramatiq_app import redis_snapshot  # noqa: E402

_KEYS = (
    "dramatiq:bench",
    "dramatiq:bench.msgs",
    "dramatiq:bench.DQ",
    "dramatiq:bench.XQ",
    "dramatiq:default",
    "dramatiq:default.msgs",
    "dramatiq:default.DQ",
    "dramatiq:default.XQ",
    "dramatiq:__heartbeats__",
)


@pytest.fixture
def client():
    connection = redis.Redis.from_url("redis://127.0.0.1:6379/15")
    try:
        connection.ping()
    except redis.ConnectionError:
        pytest.skip("Redis на 127.0.0.1:6379 недоступен")
    connection.delete(*_KEYS)
    yield connection
    connection.delete(*_KEYS)


def test_snapshot_reads_delay_list_and_heartbeat_zset(client) -> None:
    client.rpush("dramatiq:bench.DQ", "delayed")
    client.hset("dramatiq:bench.msgs", mapping={"m": b"{}"})
    client.rpush("dramatiq:bench", "m")
    client.zadd("dramatiq:bench.XQ", {"dead": 10})
    client.zadd("dramatiq:__heartbeats__", {"worker-a": 1_700_000_000_000})

    with pytest.raises(redis.ResponseError):
        client.zcard("dramatiq:bench.DQ")
    with pytest.raises(redis.ResponseError):
        client.hgetall("dramatiq:__heartbeats__")

    snap = redis_snapshot(client)
    assert snap["queues"]["bench"] == {"msgs": 1, "queue": 1, "dq": 1, "xq": 1}
    assert snap["queues"]["default"] == {"msgs": 0, "queue": 0, "dq": 0, "xq": 0}
    assert snap["heartbeats"] == {"worker-a": 1_700_000_000_000}


def test_snapshot_on_empty_keys(client) -> None:
    snap = redis_snapshot(client)
    assert snap["queues"]["bench"]["dq"] == 0
    assert snap["heartbeats"] == {}
