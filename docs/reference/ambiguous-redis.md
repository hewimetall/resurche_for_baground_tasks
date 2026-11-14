# Потеря ответа Redis у simple-q 1.2.0

Класс `connection_loss`. `counts_as_2400` ложный. Это короткий разбор одной границы: воркер отправил команду, Redis мог её выполнить, соединение оборвалось до ответа. Прогон не выбирает backend и не заменяет контроль на 2400 с.

Измерена одна библиотека: simple-q 1.2.0 (runtime и worker), `agent-worker` из `.venv-sq120`. Redis 7.0.15 слушал `127.0.0.1:6415`. Воркеры ходили на прокси `127.0.0.1:6414`. Порты 6379 и 6380 не открывались. Основной набор шёл на процессе Redis pid `126504`. Сценарий `retry_request_dropped` после удлинения наблюдения повторён на новом процессе pid `131242`. Контроль `failonce` и повтор потерянного finish шли на отдельном процессе pid `142603`, порт `6416`. Во время разрыва ни один из этих процессов не перезапускался: каталог и pid внутри сценария те же. После трассы `6416` был остановлен через `SHUTDOWN SAVE`.

Отдельный стенд на портах `6420`/`6421` закрывает границу **retry enqueue B** (не finish A): см. раздел «Retry enqueue: неизвестный результат создания B» и `results/long/retry-enqueue/`.

Прокси либо закрывает сокет клиента до записи команды в Redis, либо передаёт команду, читает полный ответ и только потом закрывает клиент или держит ответ. Данные Redis при этом остаются. Скрипты узнаются по SHA1 файлов apalis-redis 1.1.1, которые worker уже держит в кэше Redis после `FLUSHDB`.

Готовая задача сразу после `send` лежит в zset `bench:scheduled`, не в списке `bench:active`. `fetch_next` переносит её в `bench:inflight:{worker_name}`. Имя воркера имеет вид `agent-worker.bench.{host}.{pid}.{8 hex}`. Окно сироты на этом стенде — `heartbeat_secs 15` × `missed_heartbeats 4` = 60 с. Бюджет наблюдения после обрыва в основном наборе — 90 с.

Наблюдаемый путь после обрыва в этих прогонах — ошибка соединения, выход child, затем recovery другим именем. Отдельного состояния «результат Redis неизвестен» логи не показали: child переходил в `Dead` по `broken pipe` или `timed out`.

Сырые файлы основного набора: `results/long/ambiguous/`. Трасса `failonce`: `results/long/failonce-trace/`.

## Матрица

| Сценарий | Что проверяет | Что сделал simple-q 1.2.0 |
| --- | --- | --- |
| Команда не дошла | Неисполненная операция не меняет учёт | `fetch` оставил задачу в `scheduled` / Pending, handler не запускался, затем один проход в `done`. `renew` не сдвинул `locked_at` и не снял inflight. `finish` оставил тот же inflight после уже выполненного handler |
| Команда выполнена, ответ потерян | Воркер не знает, применил ли Redis команду | Ответ `fetch` потерян: задача только в inflight, handler молчит, score владельца не растёт, на 60.8 с другой воркер выполняет её один раз. Ответ `renew` потерян: `locked_at` уже новый, child выходит. Ответ `finish` потерян: задача `done`, одна строка handler. Повторный finish от живого процесса не отправлялся: child уже вышел |
| Краткий разрыв: child завершается, задача остаётся в учёте до orphan recovery | Выход при разрыве короче окна сироты и сохранность задачи | После 3 с отказа прокси оба child вышли. Score владельца застыл. За 44.6 с задача осталась в его inflight, в файле один `start` и нет `end`. 44.6 с короче окна 60 с, reap в этом сценарии не наступил |
| После выхода child другой worker забирает задачу по окну сироты | Recovery другим именем после остановки score | Через ~61 с inflight принадлежит другому имени, и handler запускается там. Отложенный на 75 с ответ `renew` дописан уже после выхода child и не возвращает задачу на старый ключ. Старого живого исполнителя в момент передачи уже не было |

## Что эти прогоны оставляют открытым

Следующие проверки в этом наборе не ставились.

- Живой процесс теряет сокет и после восстановления сверяет свой локальный буфер с inflight. В трёхсекундном разрыве child выходил.
- Старый процесс остаётся живым, владение уже у другого имени, и старый процесс отправляет `renew` или `finish`. В `renew_late_reply` child вышел примерно через две секунды после удержания ответа. Ответ ушёл в сокет на 75-й секунде, когда исполнял уже другой worker. Поздний успех не сдвинул задачу обратно. Это наблюдение про ответ, дописанный после смерти процесса.
- Тот же worker ещё раз отправляет `finish` после потерянного ответа. В `finish_response_dropped` Redis уже записал `done`, а child вышел по `broken pipe`. Идемпотентность повторного finish этим файлом не измерялась.

Отдельное поле с именем `generation` само по себе здесь не разбиралось. Имя процесса содержит pid и 8 hex. Попытка в трассе `failonce` видна как пара `message_id` и `retries` внутри нового task id.

Для следующего этапа это три отдельных требования к контракту. Эти прогоны их не исполняют:

1. Неизвестный результат операции Redis — отдельное состояние. В логах этой границы виден переход в `Dead`, а не отдельная ветка reconciliation.
2. Выход child оставляет в Redis задачи, которые ещё ждут recovery. Локальное завершение handler этого не закрывает.
3. Handler уже завершён, а finish не подтверждён — окно повторного исполнения. Его нужно видеть отдельно от backend `attempts`.

## Счётчики

`attempts` в meta пишет скрипт `handle_results`. Это не число входов в handler. В одном и том же результате они расходятся.

| Прогон | Входы в handler | Task id в финале | `attempts` у id | Прикладной retry |
| --- | --- | --- | --- | --- |
| `finish_request_dropped` | 2 строки одного id | тот же id, `done` | 1 | нет |
| `finish_response_dropped` | 1 строка | тот же id, `done` | 1 | нет |
| `long_unavailable` | 2 пары `start`/`end` | тот же id, `done` | 1 | нет |
| `retry_request_dropped` | 3 строки общего effect-файла | в файле результата записан один id | 1 | второй id этот harness не снимал |
| `failonce` без fault | 2 входа, один pid | два id, оба `done` | 1 и 1 | второй id, `retries=1`, тот же `message_id` |
| finish не дошёл, трасса | 3 входа | два id, оба `done` | 1 и 1 | второй id через 0.296 с; третий вход — тот же исходный id через 60.290 с |

## Claim

`claim_request_dropped`, задача `01M7BGKPEMRVP5PG25ZGZ15NX1`, message `8487dad0-fa3e-4aa2-b6fb-89004fa77ce2`. Прокси закрыл `EVALSHA` `fetch_next` до Redis. Сразу после обрыва задача в `scheduled`, status `Pending`, attempts `0`, строка handler пустая. Следующий воркер при открытом прокси записал одну строку и перевёл задачу в `done`.

`claim_response_dropped`, задача `01M7BGKQ6QZFFSJG3C14DZA7DN`, message `839292c6-6851-47ab-88ad-9a7349507f7c`. Redis уже перенёс id в `bench:inflight:agent-worker.bench.cursor.126749.aa250ffc`. Ответ клиенту не отдан. В ту же секунду handler не записал строку, второго места у id нет, поле data на месте. Score этого имени дальше не менялся. На 60.846 с id в `done`, в файле одна строка `claim-response`. Лог child: `poll_next: fetch_next failed -> Dead`, `broken pipe`, процесс вышел.

Восстановление здесь — выход старого child и orphan recovery. Heartbeat этого имени score не обновлял. Живой worker после восстановления сокета в этом сценарии не оставался.

## Renewal

`observe_lease` без обрыва: за 22 с `locked_at` сдвинулся с `1794608959` на `1794608974`, прокси увидел один `renew` с id задачи, финал — один `start` и один `end`, место `done`.

`renew_request_dropped`, задача `01M7BGNJZ07D7SAH8YGMEYCCF0`. `renew_leases` с id задачи оборван до Redis. `locked_at` остался прежним, id остался в inflight этого воркера. Child записал `HeartBeat failed -> Dead` / `broken pipe` и вышел в ту же секунду. На 60.878 с id лежит в inflight другого имени, и в файле вторая строка `start`. Наблюдение закончилось на 89 с, пока второй handler ещё был в `asyncio.sleep`: status `Running`, attempts `0`.

`renew_response_dropped`, задача `01M7BGRZYQ09C397HYH3T0YWTC`. Redis применил renew: `locked_at` стал `1794609298`. Членство inflight в эту секунду сохранилось. Child всё равно вышел по `broken pipe`. На 60.87 с вторая строка `start` и другой ключ inflight.

`renew_late_reply`, задача `01M7BGWCY0DXKYNKXA8BEQHPQE`. Прокси прочитал ответ renew и отдал его клиенту через 75 с: удержание с `2026-11-13T22:36:49.867Z`, запись ответа в `22:38:04.868Z`. Child в `22:36:50.368Z` записал `HeartBeat failed -> Dead`, `Database(timed out)`, и в `22:36:51.908Z` вышел. Вторая строка `start` появилась на 60.9 с наблюдения, раньше записи отложенного ответа. К концу наблюдения id остаётся в inflight нового имени `...128502.c8eb2ae6`. Отложенный успех не вернул задачу на старый ключ. Процесса, который мог бы принять этот ответ и продолжить исполнение, уже не было.

## Finish

`finish_request_dropped`, задача `01M7BGZXVCXFYGEGD5975YFWHN`. Handler записал строку, затем `handle_results` не дошёл до Redis. Id остался в исходном inflight, status `Running`, attempts `0`. Child: `ProcessEvents failed -> Dead`. На 60.815 с тот же id в `done`, в файле две строки handler, attempts `1`, результат `ok`. Задача не пропала из учёта. Повторный проход — другой worker name.

`finish_response_dropped`, задача `01M7BH1SJM64JEPE7Y5GZYGRKF`. Redis записал `done` и `attempts=1` до того, как клиент увидел ответ. Строка handler одна. В следующие секунды второй строки нет, в `scheduled` id нет. Child вышел по `ProcessEvents failed`.

## Retry: контроль без fault

Актор `failonce` объявлен с `max_retries=1`, `min_backoff=100`, `max_backoff=300`. На задаче в Redis при этом `max_attempts=1`. Это разные бюджеты. `handle_results` переставляет тот же id только при status `Failed` или `Pending`, если `attempt < max_attempts`, и умеет прочитать задержку из `RetryAfterError`. В этой трассе finish уходил со status `Done`, поэтому ветка Lua для того же id не использовалась.

Контроль без прокси, Redis pid `142603` до и после, порт `6416`. Один worker. `MONITOR` и обход всех id в `bench:data`. Актор тот же контракт, строка входа пишет `message_id`, `retries`, `deliveries` и pid. Первый вход бросает `RuntimeError("fail once")`.

Message `032630d0-7385-4871-8ec9-f805dd238bbd`.

| Шаг | Время Redis | Что записано |
| --- | --- | --- |
| `INCR delivery:{message}:0` | `1794639902.984490` | счётчик доставки пары message/`retries=0` |
| Вход handler, `retries=0`, pid `142782` | `1794639902.999821` | effect |
| `EVALSHA` enqueue нового id `01M7CDZE8RD0CM868GCVZ9DA0Z` | `1794639903.001052` | тот же `message_id`, `retries=1`, score расписания `1794639903` |
| `DEL delivery:{message}:0` | `1794639903.001747` | ключ доставки первой попытки снят до finish |
| `handle_results` исходного id `01M7CDZE1JXEPV96NHEFXZRHAR` | `1794639903.002457` | status `Done`, attempts `1`, result `{"Ok":{"delay_ms":61,"outcome":"retry"}}` |
| fetch забирает новый id | `1794639903.002683` | `zrangebyscore` до `1794639903`, затем inflight того же worker |
| Вход handler, `retries=1`, тот же pid | `1794639903.003492` | effect |
| `handle_results` нового id | `1794639903.003887` | status `Done`, attempts `1`, result `{"Ok":{"outcome":"ok","result":null}}` |

Оба id в финале лежат в `done`. Второго `message_id` нет. `delay_ms` 61 остался внутри result первой задачи. Score, который enqueue записал в `bench:scheduled`, равен целой секунде самой команды, поэтому 61 мс не перенесли задачу на следующую секунду. Второй вход начался через 4 мс после первого.

Лог child на этих двух входах: `running actor` с `retries=0`, затем `retries=1`, один `message_id`.

Прежний вывод по `retry_response_dropped`, что второго id нет, шире того файла. Harness вызывал `_locate` только для исходного id и останавливал наблюдение, когда этот id уже `done` (`t_s` 0.027). Прокси в том прогоне насчитал `seen:enqueue` 1 при том, что клиентский `send` шёл в Redis мимо прокси. Это совместимо с enqueue повтора, но сам id в том файле не снят. Контроль выше показывает, где этот id появляется: до finish, отдельным ключом, с тем же `message_id` и `retries=1`.

## Retry: finish не дошёл

Тот же Redis pid `142603`. Прокси `6417` закрыл первый `handle_results` до записи в Redis. Клиентский enqueue снова шёл напрямую.

Message `cbef47a6-ab41-418d-8e8c-3aa5d2fb705e`. Исходный id `01M7CDZFJNH7Y7DZ1P0NPRCVS9`.

Сразу после обрыва, ещё до второго входа:

- исходный id в `bench:inflight:agent-worker.bench.cursor.142821.52da4781`, status `Running`, attempts `0`;
- новый id `01M7CDZFTXPP8MY34VYPSRPXC4` уже в `scheduled`, в данных `retries=1`, status `Pending`;
- в `MONITOR` нет `handle_results` исходного id. Прокси записал `drop_request` в `1794639904.606709`. Enqueue нового id прошёл в `1794639904.606057`, `DEL delivery:{message}:0` — в `1794639904.606494`.

Дальше это два разных механизма.

Второй вход, `+0.296` с от первого, pid `142849`, worker `...142849.167dec7b`, `retries=1`, `deliveries=1`. К сэмплу `t_s=0.313` новый id уже `done`, attempts `1`, result `ok`. Исходный id всё ещё в inflight вышедшего имени. До окна 60 с здесь далеко. Это прикладной retry, уже подтверждённый Redis до потерянного finish.

Третий вход, `+60.290` с, тот же pid `142849`, `retries=0`, `deliveries=1`. К сэмплу `t_s=60.306` исходный id в `done`, attempts `1`, result `ok`, владелец `...142849.167dec7b`. Это orphan recovery исходной попытки: finish так и не был применён, id дождался окна сироты. К этому моменту effect-файл уже содержал два входа, поэтому актор не бросил исключение и finish записал `ok`.

Score имени `142821.52da4781` оставался `1794639904` до сэмпла 60.3 с. Supervisor после выхода pid `142821` в `07:05:04.632Z` поднял новый child pid `142858` в `07:05:05.635Z`. Это другое имя, `...142858.52bbd0ce`. Его score с `t_s=1.199` обновлялся. Строк handler от него нет. Новый child того же supervisor не подхватил inflight умершего имени.

Старый прогон `retry_request_dropped` на pid `131242` видел вторую строку effect на 2.065 с при том, что исходный id ещё лежал в прежнем inflight, и третью строку на 60.973 с. Harness того прогона не снимал остальные id, поэтому вторая строка там не привязана к конкретному новому ключу. По времени она не попадает в окно сироты. Трасса на pid `142603` разделяет эти две границы: вторая строка — новый id с `retries=1`, третья — исходный id после 60 с.

`retry_response_dropped` на pid `126504` оставил у исходного id result `{"Ok":{"delay_ms":77,"outcome":"retry"}}`, status `Done`, attempts `1`, одну строку handler. Это совпадает с тем, что контроль записывает на первый id, когда finish применился. Второе тело в том файле не появилось: наблюдение кончилось на 0.027 с, child уже вышел.

## Retry enqueue: неизвестный результат создания B

Отдельный стенд закрывает границу, которую трасса `failonce` оставляла открытой: отказ на **enqueue B**, а не на `handle_results` исходной A. Production-код simple-q не менялся. Артефакты: `results/long/retry-enqueue/`. Код: `scripts/simpleq_retry_enqueue_fault.py`, `bench/retry_by_retries_app.py`, доработка `scripts/redis_fault_proxy.py` (в событиях enqueue теперь есть `enqueue_task_id` / `message_id` / `retries`).

### Baseline

| Поле | Значение |
| --- | --- |
| SHA стенда (начало прогона) | `2828a095d79d123fc5400155bbb045e5afefee2d` |
| simple-q-runtime / worker | 1.2.0 / 1.2.0 |
| apalis-redis | 1.1.1 (SHA скриптов в `KNOWN_SHA`) |
| Redis | 7.0.15, порт `6420`, dir `results/long/retry-enqueue-6420` |
| Redis pid (control + delay) | `8348` — без перезапуска внутри этих сценариев |
| Redis pid (три fault) | `8943` — новый процесс после `SHUTDOWN SAVE` в конце первого suite; внутри каждого fault pid не менялся |
| Proxy | `6421` → `6420` |
| Orphan | `heartbeat_secs 15` × `missed_heartbeats 4` = **60 с** |
| Redis `timeout` | `0`; client timeout виден как `timed out` / `broken pipe` у worker |
| Effective config | `batch_size=1`, `poll_interval_ms=500`, `max_deliveries=5`, остальное — `Config.from_env()` defaults (`results/long/retry-enqueue/baseline.json`) |

Клиентский `send` идёт **напрямую** в Redis. Worker ходит только через proxy. Fault вооружается **после** регистрации worker, kind=`enqueue`, once — клиентский send trigger не расходует.

Handler `retry_by_retries`: `retries==0` → всегда retry; `retries>=1` → ok. Глобального failonce нет. Журнал пишет `execution_id`, `message_id`, `retries`, `pid`, `worker`, фазы `handler_start` / `handler_end`. **Ограничение (observed):** `CurrentMessage` не содержит `task_id`; связка execution↔task_id — через `retries` + proxy `enqueue_task_id` + Redis timeline, не прямой bridge.

### Граница enqueue

**Observed:** создание retry-задачи — один атомарный `EVALSHA` enqueue (data/meta/scheduled|active/signal).

**Observed:** после успешного ответа worker ещё делает `DEL delivery:{message}:{retries}` и затем `handle_results` для A. Это отдельные команды. При потере ответа enqueue proxy закрывает клиентский сокет, follow-up на этом соединении не выполняются. Называть сценарий «enqueue применён полностью» для всего retry-path нельзя — атомарна только сама enqueue-скрипта.

### Контроль без fault

Redis pid `8348`. Message одного прогона, ids `01M7CGPH9RYF51CA7EYG0JDSDN` (A, retries=0) и `01M7CGPHPZDSHAKGVD6HRT73MR` (B, retries=1).

| Шаг | Observed |
| --- | --- |
| Handler A | `handler_start`/`end`, result=`retry`, execution_id свой |
| B в Redis | да, до finish A |
| Finish A | `Done`, result `{"Ok":{"delay_ms":…,"outcome":"retry"}}`, attempts=1 |
| Handler B | result=`ok` |
| Входы | retries=0: 1; retries=1: 1 |
| C | нет |

`observe_status=complete`. Exit code 0.

### Три fault на enqueue B

Fault-прогоны — Redis pid `8943`. Второй worker стартует после срабатывания fault (ёмкость для orphan recovery).

| Сценарий | B из faulted enqueue в Redis? | Что увидел Rust | Finish A сразу? | Child | A после orphan? | C? | retries=1 ids | Входы handler (0 / 1) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Enqueue не дошёл (`drop_request`) | **Нет** (proxy `enqueue_task_id=01M7CGV7A4C0HP0Q1DDEYHHSE6` отсутствует в `bench:data`) | `retry enqueue failed; leaving the attempt inflight` / `broken pipe` | Нет (A inflight) | да | да, второй `retries=0` | нет | 1 (создан после recovery A) | 2 / 1 |
| Enqueue применён, ответ потерян (`drop_response`) | **Да** (`01M7CGX3WN6BB7FPEC5BBB58TC` = proxy `enqueue_task_id`) | то же, `broken pipe` | Нет | да | да | **да** `01M7CGYYK8TYF1R05QCBMQGRBN` | **2** | 2 / 2 |
| Enqueue применён, ответ задержан 10 с (`delay_response`) | **Да** (`01M7CGZ0F46RAGB2E9MKJPMYTX`) | `… timed out` (не broken pipe) | Нет | да | да | **да** `01M7CH0V5JKEEHBZHBJWC119TR` | **2** | 2 / 2 |

Во всех трёх финалах незавершённых id нет (`observe_status=complete`). Finish A после recovery пишет `Done` с `outcome=retry` (повторный успешный retry-path), не «ok» с первой попытки.

#### Гипотеза «B создана, ответ потерян → … → C?»

**Observed на `enqueue_response_dropped`:**

1. Proxy: `drop_response` enqueue, `reply_len=12`, Redis уже создал B.
2. Worker: `retry enqueue failed; leaving the attempt inflight`; finish A не уходит.
3. B исполняется (`retries=1` → ok) другим/тем же живым worker’ом, пока A ещё в inflight умершего имени.
4. ~60 с: orphan recovery A; handler снова `retries=0` → retry.
5. Создаётся **C** — второй task id с `retries=1` для того же `message_id`.
6. Итог: **два** прикладных retry-тела и **два** входа с `retries=0` на одну доставку message_id.

`enqueue_response_delayed` (удержание 10 с > client timeout) дал ту же форму: B есть, Rust видит `timed out`, A inflight, затем C. Proxy позже пишет `release_response` в уже мёртвый сокет — на учёт это не влияет (**inferred** из порядка событий).

`enqueue_request_dropped` отличается: B из faulted команды в Redis нет; после orphan A создаётся ровно один retries=1 id. Дубликата прикладного retry нет.

### delay_ms (отдельно от fault)

Актор `delay_probe`, send `delay=…`. n=1 на значение. Score в `bench:scheduled` — **целые секунды**.

| delay_ms | observed_lag_ms (enqueue→handler_start) | score→handler_ms |
| --- | --- | --- |
| 61 | 423.2 | 1355.3 |
| 999 | 521.9 | 39.0 |
| 1000 | 1020.4 | 227.4 |
| 1001 | 923.5 | 395.3 |
| 1500 | 1522.5 | 51.0 |

**Observed:** субсекундные delay часто схлопываются в ту же unix-секунду score; `poll_interval_ms=500` добавляет джиттер. **Untested:** статистика по многим прогонам. Не выводить точность deadline из одного замера.

### Команды

```bash
python3 -m venv .venv-sq120 && . .venv-sq120/bin/activate
pip install 'simple-q-runtime==1.2.0' 'simple-q-worker==1.2.0' redis
python scripts/simpleq_retry_enqueue_fault.py
# exit 0 если все сценарии complete и fault сработали; иначе 2
```

Сводка: `results/long/retry-enqueue/run.json`. Таймлайны: `*.timeline.json`. Сырые proxy/worker/monitor: рядом в том же каталоге.

### Claim (ответ на целевой вопрос)

**Что происходит с исходной доставкой и числом прикладных retry, если Redis создал retry-задачу, но worker не получил подтверждение?**

**Observed:** исходная A остаётся inflight (`leaving the attempt inflight`); finish A в этот момент не применяется. Уже созданная B исполняется как обычная задача с `retries=1`. После orphan window A выполняется снова с `retries=0`, снова возвращает retry и **создаёт ещё один** task id с `retries=1` (C). На один `message_id` получается **два** прикладных retry-исполнения и повторный вход в handler исходной попытки. Backend `attempts` по-прежнему не считает входы handler.

**Inferred:** тот же класс поведения при client timeout на задержанном ответе enqueue (подтверждено одним прогоном `delay_response` 10 с).

**Untested:** живой тот же child после восстановления сокета без выхода; идемпотентный ключ на retry enqueue; другое значение orphan window.

## Короткий и длинный обрыв

`brief_disconnect`, 3 с, задача `01M7BH21JKWV1XB5GS63DHJRWQ`. Оба child записали `broken pipe` и вышли. Score исходного владельца `...129866.02439655` остался `1794609579` до конца наблюдения 44.6 с. Id всё это время в его inflight. В файле одна строка `start` и нет `end`. Окно 60 с этим сценарием не пройдено. Сценарий показывает выход child и сохранность задачи внутри окна.

`long_unavailable`, прокси молчал 70 с, задача `01M7BH3K02VGTNZ5BS0SKAK30T`. К моменту, когда сокеты снова пустили, файл уже содержал `start` и `end`, а Redis всё ещё держал id в исходном inflight `...130214.02eb0378`. На 18.263 с после открытия id в inflight другого имени и в файле второй `start`. На 38.548 с id в `done`, в файле две пары `start`/`end`, attempts `1`. Локальное завершение первого handler и членство Redis разошлись, затем тело выполнилось ещё раз.

## CI

В репозитории нет workflow GitHub Actions: каталог `.github/workflows` пуст, API списка workflow возвращает ноль. На head `35a659f` единственный check — CodeRabbit, `completed` / `skipped`. Тестового прогона на этом PR нет.
