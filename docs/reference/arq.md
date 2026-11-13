# arq 0.28.0: что делает пустой воркер

Пакет во втором venv, `redis` 5.3.1, потому что arq требует `redis<6`. Сервер тот же Redis 7.
Документация: [arq 0.28](https://arq-docs.helpmanual.io/). Параметр воркера `poll_delay` описан у `arq.worker.Worker`.

В установленном `arq/worker.py` значение по умолчанию `poll_delay` — 0.5 с.
`main` идёт `async for _ in poll(self.poll_delay_s)` и на каждом шаге вызывает `_poll_iteration`.
`_poll_iteration` читает `zrangebyscore` по очереди с верхней границей «сейчас».
`poll` в `arq/utils.py` после шага делает `await asyncio.sleep` на остаток `step`.

`BLPOP` в пакете arq нет. Очередь — sorted set `arq:queue`.

## Трейс

`results/traces/arq.json`. Простой 2.409 с, `pickup_ms` 261.

Один процесс, один поток, wchan `ep_poll`. py-spy: `EpollSelector.select` → `BaseEventLoop.run_forever` → `Worker.run` в `arq/worker.py`.
За простой в `MONITOR` есть `ZRANGEBYSCORE` и `ZRANGE`. `BRPOP` и `XREADGROUP` нет.
После send клиент делает `ZADD` в `arq:queue`, следующий `ZRANGEBYSCORE` забирает id.

Стенд arq в kind не запускался: это ограничение образа, оно описано в `docs/bench.md` и сюда не переносится.
