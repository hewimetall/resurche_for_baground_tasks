"""MemoryClient.drain: дедуп при отправке и повтор без ожидания backoff."""

import asyncio

import simple_q
from simple_q import App, MemoryClient


@simple_q.actor(
    name="ping",
    queue="mem",
    max_retries=2,
    min_backoff=50_000,
    max_backoff=50_000,
    time_limit=5_000,
)
async def ping(n: int, sink: list) -> None:
    sink.append(n)
    if n < 0:
        raise RuntimeError("boom")


def test_unique_key_and_drain() -> None:
    asyncio.run(_unique_key_and_drain())


async def _unique_key_and_drain() -> None:
    client = MemoryClient()
    app = App(client=client, actors=[ping])
    sink: list[int] = []
    first = await client.send_with_options(ping, kwargs={"n": 1, "sink": sink}, unique_key="k")
    second = await client.send_with_options(ping, kwargs={"n": 1, "sink": sink}, unique_key="k")
    assert first is not None
    assert second is None
    assert await client.worker_alive("worker-x") is False
    client.set_worker_alive("worker-x", True)
    assert await client.worker_alive("worker-x") is True
    client.set_worker_alive("worker-x", False)
    assert await client.worker_alive("worker-x") is False
    await client.drain(app)
    assert sink == [1]
    third = await client.send_with_options(ping, kwargs={"n": 2, "sink": sink}, unique_key="k")
    assert third is not None


def test_retry_is_immediate() -> None:
    asyncio.run(_retry_is_immediate())


async def _retry_is_immediate() -> None:
    client = MemoryClient()
    app = App(client=client, actors=[ping])
    sink: list[int] = []
    await client.send_with_options(ping, kwargs={"n": -1, "sink": sink})
    await client.drain(app)
    # max_retries=2: попытки n=-1 три раза, затем on_dead нет и ключ снят.
    assert sink == [-1, -1, -1]
