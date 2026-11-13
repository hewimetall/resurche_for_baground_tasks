# Celery 5.7.0: что делает пустой воркер

Стенд запускает `celery -A bench.celery_app worker -Q bench -c 1 --without-gossip --without-mingle`.
Документация брокера: [Using Redis](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html).
Таймаут видимости Redis там описан отдельно и к ожиданию пустой очереди не сводится. На стенде это 1 час по умолчанию.

Цикл воркера — `asynloop` в `celery/worker/loops.py`: `consumer.consume()`, затем `hub.create_loop()`.
Команда очереди живёт в kombu. `_register_BRPOP` в `kombu/transport/redis.py` регистрирует сокет в поллере и отправляет `BRPOP`. Рядом есть `_register_LISTEN` для pub/sub. Это другой канал.

## Трейс

`results/traces/celery.json`. Простой 2.818 с, `pickup_ms` 230.

| Процесс | Потоки | wchan | py-spy |
| --- | --- | --- | --- |
| родитель | 1 | `ep_poll` | `_epoll.poll` в `kombu/utils/eventio.py`, вызывающий — `asynloop` |
| ребёнок пула | 1 | `pipe_read` | `billiard` `Worker.workloop`, приём из пайпа |

`MONITOR`, команды ожидания воркера:

```text
BRPOP bench bench\x06\x163 bench\x06\x166 bench\x06\x169 1
```

Аргумент таймаута — `1` секунда. За простой таких `BRPOP` несколько: таймаут истекает, команда уходит снова.
`PSUBSCRIBE /0.celery.pidbox` есть и при `--without-gossip --without-mingle`. Это пульт, не очередь задач.

`SUBSCRIBE celery-task-meta-<id>` шлёт процесс `bench.cli`. `on_task_call` в `celery/backends/redis.py` подписывается, когда `task_join_will_block()` ложен, а `cancel_for` снимает подписку из `AsyncResult.__del__`. В разрезе {doc}`/reference/phase-series` подписка снята через 2 мс после `LPUSH` и раньше файла. Воркер задачу забирает через `BRPOP`. `LPUSH` до файла в том разрезе — 4 мс. 230 мс этого одиночного трейса стоят до `LPUSH`.
