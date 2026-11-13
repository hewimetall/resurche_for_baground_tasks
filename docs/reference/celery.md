# Celery 5.7.0: что делает пустой воркер

Стенд запускает `celery -A bench.celery_app worker -Q bench -c 1 --without-gossip --without-mingle`.
Документация брокера: [Using Redis](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html).
Таймаут видимости Redis там описан отдельно и к ожиданию пустой очереди не сводится. Kombu 5.7.0 держит на классе `visibility_timeout = 3600`. Адаптер это значение не оставляет.

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

## Какое число стоит на restore

`bench/celery_app.py` кладёт `CELERY_VISIBILITY_TIMEOUT` в `broker_transport_options`. Переменной нет — туда уходит 2100. `result_backend_transport_options` и `app.conf.visibility_timeout` адаптер не задаёт. В установленном Celery 5.7.0 строки `visibility_timeout` нет: на канал kombu копируется словарь брокера (`celery/app/base.py`). Бэкенд результата это поле не читает.

`QoS.append` в `kombu/transport/redis.py` пишет в `unacked_index` время выдачи. `restore_visible` забирает записи со score не выше `time() - channel.visibility_timeout` и возвращает сообщение в список очереди. Первый обход идёт при старте цикла. Дальше таймер зовёт обход каждые `unacked_restore_interval` 10 с, а внутри срабатывает каждый `unacked_restore_throttle` 10-й вызов. Следующий `ZRANGE` после стартового — примерно через 100 с.

В `results/traces/celery.json` воркер поднят без `CELERY_VISIBILITY_TIMEOUT`. Стартовый обход:

```text
ZRANGE unacked_index 1794554708.0407414 0 BYSCORE REV
```

Время команды `1794556808.057940`. Верхняя граница на 2100.017 с раньше команды. `inspect` в `results/celery/admin.json` показывает `transport_options.visibility_timeout` 2100.

Сценарии, где переменная задана: `blocking` 8, `sigkill` 12, `five_kills` 15, второй воркер `prefetch` 20, kind 30, `long_on` и `long_off` 400. В их JSON границы `ZRANGE` нет, поэтому само число restore эти файлы не доказывают.

`results/celery/sigkill.json` при переменной 12 записал один `block-start` и закончился за 16.6 с. Стартовый обход в таком окне видит сообщение моложе 12 с, следующий обход по дросселю — около 100 с. Отсутствие второй попытки здесь не называет 12 сработавшим сроком.

`results/celery/five_kills.json` при переменной 15 хранит две заметки `long retries=0`. Первый kill прошёл после подтверждённого старта. Второй, через 40.6 с, помечен `saw_attempt: false`. В хвосте процесса, который стенд записал как неудачный рестарт, строка `Task longjob[...] received` стоит в ту же миллисекунду, что и `celery@cursor ready`. Сообщению к этому моменту уже больше 15 с. Порог score прогон не записал, поэтому 15 как граница `ZRANGE` из файла не следует. Стартовый обход со сроком 2100 или 3600 эту выдачу на 41-й секунде не объясняет.
