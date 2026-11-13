"""Актор для проверки порядка prefetch Dramatiq.

Первое сообщение держит рабочий поток. Потребитель за это время
кладёт остальные в PriorityQueue. Файл порядка пишет номер после сна.
"""

from __future__ import annotations

import os
import time

import dramatiq
from dramatiq.brokers.redis import RedisBroker

broker = RedisBroker(url=os.environ["REDIS_URL"])
dramatiq.set_broker(broker)


@dramatiq.actor(queue_name="bench", max_retries=0)
def seq(n: int, hold: float, path: str) -> None:
    time.sleep(hold)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(f"{n}\n")
