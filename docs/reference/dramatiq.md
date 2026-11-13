# Dramatiq 2.2.1: что делает пустой воркер

Пакет стенда: `.venv`, Dramatiq 2.2.1.
Исходники, на которые опирается таблица: `dramatiq/brokers/redis.py`, `dramatiq/worker.py`, `dramatiq/common.py`, `dramatiq/asyncio.py`.
Теги: [v2.2.1 redis.py](https://github.com/Bogdanp/dramatiq/blob/v2.2.1/dramatiq/brokers/redis.py), [changelog](https://dramatiq.io/changelog.html).

## Цикл потребителя

`_RedisConsumer.__next__` при пустом кэше вызывает `do_fetch`. Скрипт делает `LPOP`. Если сообщений нет, `compute_backoff(self.misses, max_backoff=self.timeout)` и `time.sleep(backoff_ms / 1000)`.

`Worker.consume` имеет параметр `timeout=5000`. Живой воркер передаёт другое значение: `ConsumerThread.run` вызывает `broker.consume(..., timeout=self.worker_timeout)`. `WORKER_TIMEOUT` — `dramatiq_worker_timeout`, по умолчанию 1000 мс (`dramatiq/worker.py`).

Потоков потребителя два: очередь `bench` и `bench.DQ`. У каждого свой счётчик промахов. `WorkerThread` в это время блокируется в `work_queue.get`. `AsyncIO` поднимает `EventLoopThread` с `loop.run_forever()` ещё до задач.

`compute_backoff` в 2.2.1: база `factor * 2**attempts` (`factor` по умолчанию 5), jitter умножает на `uniform(1, 2)`, выше `max_backoff` значение сбрасывается в `uniform(0.5, 1) * max_backoff`.

## Трейс

`results/traces/dramatiq.json`. Простой до send 9.225 с, `pickup_ms` 557.

Разложение 557 мс по `dramatiq.monitor.txt`, очередь `dramatiq:bench`:

| Метка | Секунда | Смысл |
| --- | --- | --- |
| `sent_ms` | 1794556799.118 | часы клиента в аргументе задачи |
| `RPUSH` | 1794556799.291 | сообщение уже в списке, +173 мс |
| `LPOP` | 1794556799.674 | следующий `fetch` забрал id, +382 мс от `RPUSH` |
| предыдущий `LPOP` | 1794556798.689 | до него 985 мс, из них 602 мс прошли до `RPUSH` |

Процесс воркера, py-spy:

| Поток | wchan | Кадр |
| --- | --- | --- |
| MainThread | `hrtimer_nanosleep` | `worker_process` в `dramatiq/cli.py` |
| Thread-1 | `hrtimer_nanosleep` | `_CtypesTimeoutManager.run` |
| Thread-2 | `ep_poll` | `EventLoopThread.run` → `EpollSelector.select` |
| Thread-3, Thread-4 | `hrtimer_nanosleep` | `_RedisConsumer.__next__`, строка `time.sleep` |
| Thread-5 | `futex_wait_queue` | `WorkerThread.run`, `work_queue.get` |

Отдельный процесс prometheus сидит в `do_poll` на exposition-сервере.

`MONITOR`: десятки `LPOP` внутри `EVAL`. `BRPOP`, `BLPOP`, `SUBSCRIBE` у этого воркера нет. `RPUSH` появляется после send.

Переключений контекста у процесса воркера за окно 0.4 с в этом снимке: 0. Окно попало в сон.

## Heartbeat и maintenance

Это две разные проверки. `DEFAULT_HEARTBEAT_TIMEOUT` в `dramatiq/brokers/redis.py` — 60000 мс. `DEFAULT_MAINTENANCE_CHANCE` — 1000. Розыгрыш идёт до `MAINTENANCE_SCALE` 1_000_000, то есть по умолчанию 1000 команд из миллиона. `ack` и `nack` в `MAINTENANCE_COMMAND_BLACKLIST` и maintenance не запускают.

`bench/dramatiq_app.py` передаёт `heartbeat_timeout=DRAMATIQ_HEARTBEAT_MS` и не передаёт `maintenance_chance`. Без переменной heartbeat остаётся 60000, шанс остаётся 1000. `sigkill`, `five_kills` и `prefetch` ставят `DRAMATIQ_HEARTBEAT_MS=8000`. Kind ставит 15000. Шанс в этих командах не меняется.

Каждый `dispatch` сначала делает `ZADD` heartbeat. Перекладка чужих сообщений в `dramatiq/brokers/redis/dispatch.lua` выполняется только при `do_maintenance == "1"`: `ZRANGEBYSCORE` по `dramatiq:__heartbeats__` до `timestamp - heartbeat_timeout`, затем `SMEMBERS` ключа `__acks__.<id>.<queue>` и `RPUSH` id обратно в очередь. Команда Lua `requeue` — другой путь: её вызывает живой воркер на остановке, возраст heartbeat она не смотрит.

В `results/traces/dramatiq-2.2.1-idle.monitor.txt` все 56 `fetch` несут heartbeat `60000` и `do_maintenance` `0`.

Один диагностический прогон на `127.0.0.1:6397`: `heartbeat_timeout=8000`, `maintenance_chance=1_000_000`. Это контроль, не настройка стенда. Задача спала 30 с. После SIGKILL score убитого воркера `4b9277f7-ec65-4abf-8c66-ff82fb76dc88` застыл на `1794567751880`. 33 maintenance-обхода имели cutoff ниже этого score и сообщение не двигали. Следующий `fetch` по очереди `diag` пришёл со штампом `1794567760341`, cutoff `1794567752341` (на 461 мс позже застывшего score). В том же скрипте: `SMEMBERS` ack-ключа, `HEXISTS`, `RPUSH dramatiq:diag`, `DEL` ack-ключа, `ZREM` heartbeat, `LPOP` уже новым воркером. `RPUSH` через 8.460 с после SIGKILL. Новый актор стартовал через 1.1 мс после `RPUSH`. Команды `requeue` в мониторе не было. Числа — `results/traces/dramatiq-maintenance.json`. Сырой `MONITOR` в репозиторий не положен. Серии с шансом 1000 этот прогон не заменяет: он показывает, что перекладка срабатывает на первом обходе после истечения heartbeat, когда обход вынужден на каждом `fetch`.
