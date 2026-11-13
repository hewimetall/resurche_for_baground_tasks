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
