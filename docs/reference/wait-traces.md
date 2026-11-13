# Что снято с процессов

Один воркер, очередь пустая, затем одна задача `fast`. Redis `127.0.0.1:6391`.
Дата прогона: 2026-11-13. Сырые файлы: `results/traces/`.

`pickup_ms` — от `sent_ms` в аргументах задачи до миллисекунды в строке `fast_record`. `sent_ms` ставится до процесса `bench.cli`, поэтому в число входит старт интерпретатора. Где именно кончается этот старт и начинается ожидание воркера, разобрано серией фазы и штампами `MONITOR` в {doc}`/reference/phase-series`.

| Библиотека | Простой до send, с | pickup, мс | Команда ожидания в MONITOR | Поток, который ждёт |
| --- | --- | --- | --- | --- |
| simple-q, heartbeat 5 с | 2.796 | 2484 | `EVALSHA` / `LRANGE`, без `BLPOP` и без `PSUBSCRIBE` | tokio, `futures-timer` в `futex_wait_queue` |
| Dramatiq 2.2.1 | 9.225 | 557 | `EVAL` → `LPOP`, 68 `LPOP` за окно | `Thread-3` и `Thread-4`, `hrtimer_nanosleep`, кадр `redis.py:370` |
| Celery 5.7.0 | 2.818 | 230 | `BRPOP ... 1` | родитель, `ep_poll`, кадр `kombu/utils/eventio.py` |
| arq 0.28.0 | 2.409 | 261 | `ZRANGEBYSCORE` | единственный поток, `ep_poll`, `Worker.run` |
| taskiq 0.13.0 | 2.820 | 405 | `XREADGROUP ... BLOCK 2000` | дочерний процесс, `ep_poll` |

Первый прогон simple-q и arq без `PG_DSN` файла не записал: старт и `around_dispatch` зовут Postgres. Числа в таблице — повтор с `PG_DSN`. У simple-q в логе `running actor` и следующий `SIGTERM` разделены 57 мс (`results/traces/simpleq.worker.log`), так что почти все 2484 мс прошли до входа в актор.

Частота переключений за 0.4 с в простое:

| Процесс | voluntary + nonvoluntary, 1/с |
| --- | --- |
| simple-q, супервизор | 0 |
| simple-q, ребёнок | 10 |
| Dramatiq, процесс воркера | 0 в этом окне (потоки уже в сне) |
| Celery, родитель | 7.5 |
| Celery, ребёнок пула | 0 |
| arq | 7.5 |
| taskiq, родитель CLI | 0, `hrtimer_nanosleep` |
| taskiq, ребёнок | 2.5 |

Ноль переключений за 0.4 с значит, что процесс в этом окне не крутил цикл. У Dramatiq это сон `time.sleep` длиннее окна. У simple-q супервизор спит, ребёнок шевелится: в нём tokio и встроенный цикл Python.
