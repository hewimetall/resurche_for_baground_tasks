"""Минимальный актор для сравнения Dramatiq 1.18.0 и 2.2.1.

Импортируется воркером той версии, чей интерпретатор его запустил.
"""

from __future__ import annotations

import os
import time

import dramatiq
from dramatiq.brokers.redis import RedisBroker

broker = RedisBroker(url=os.environ["REDIS_URL"])
dramatiq.set_broker(broker)


@dramatiq.actor(queue_name="bench", max_retries=0)
def hit(path: str, sent: float) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(f"{sent} {time.time()}\n")
