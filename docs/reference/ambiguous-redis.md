# Потеря ответа Redis у simple-q 1.2.0

Класс `connection_loss`. `counts_as_2400` ложный. Это короткий разбор одной границы: воркер отправил команду, Redis мог её выполнить, соединение оборвалось до ответа. Прогон не выбирает backend и не заменяет контроль на 2400 с.

Измерена одна библиотека: simple-q 1.2.0 (runtime и worker), `agent-worker` из `.venv-sq120`. Redis 7.0.15 слушал `127.0.0.1:6415`. Воркеры ходили на прокси `127.0.0.1:6414`. Порты 6379 и 6380 не открывались. Основной набор шёл на процессе Redis pid `126504`. Сценарий `retry_request_dropped` после удлинения наблюдения повторён на новом процессе pid `131242`. Во время разрыва ни один из этих процессов не перезапускался: каталог и pid внутри сценария те же.

Прокси либо закрывает сокет клиента до записи команды в Redis, либо передаёт команду, читает полный ответ и только потом закрывает клиент или держит ответ. Данные Redis при этом остаются. Скрипты узнаются по SHA1 файлов apalis-redis 1.1.1, которые worker уже держит в кэше Redis после `FLUSHDB`.

Готовая задача сразу после `send` лежит в zset `bench:scheduled`, не в списке `bench:active`. `fetch_next` переносит её в `bench:inflight:{worker_name}`. Имя воркера имеет вид `agent-worker.bench.{host}.{pid}.{8 hex}` и не получает отдельного поля generation. У живого ключа два двоеточия: `bench:inflight` и имя. Окно сироты на этом стенде — `heartbeat_secs 15` × `missed_heartbeats 4` = 60 с. Бюджет наблюдения после обрыва — 90 с.

Сырые файлы: `results/long/ambiguous/`. Сводка собирается из них в `results/long/ambiguous/summary.json`.

## Матрица

| Сценарий | Что проверяет | Что сделал simple-q 1.2.0 |
| --- | --- | --- |
| Команда не дошла | Неисполненная операция не меняет учёт | `fetch` оставил задачу в `scheduled` / Pending, handler не запускался, затем один проход в `done`. `renew` не сдвинул `locked_at` и не снял inflight. `finish` оставил тот же inflight после уже выполненного handler |
| Команда выполнена, ответ потерян | Воркер не знает, применил ли Redis команду | Ответ `fetch` потерян: задача только в inflight, handler молчит, score владельца не растёт, на 60.8 с другой воркер выполняет её один раз. Ответ `renew` потерян: `locked_at` уже новый, дочерний процесс всё равно выходит. Ответ `finish` потерян: задача `done`, одна строка handler, повторного тела нет |
| Соединение вернулось, владелец прежний | Локальное состояние и Redis снова про одного владельца | После 3 с отказа прокси тот же child вышел. Его score застыл. За 44 с имя не возобновило handler. Задача осталась в его inflight |
| Соединение вернулось после передачи владения | Старый исполнитель не продолжает владение | Через ~61 с inflight принадлежит другому имени, и handler запускается там. Отложенный на 75 с ответ `renew` дописан уже после выхода child и не возвращает задачу на старый ключ |

## Claim

`claim_request_dropped`, задача `01M7BGKPEMRVP5PG25ZGZ15NX1`, message `8487dad0-fa3e-4aa2-b6fb-89004fa77ce2`. Прокси закрыл `EVALSHA` `fetch_next` до Redis. Сразу после обрыва задача в `scheduled`, status `Pending`, attempts `0`, строка handler пустая. Следующий воркер при открытом прокси записал одну строку и перевёл задачу в `done`.

`claim_response_dropped`, задача `01M7BGKQ6QZFFSJG3C14DZA7DN`, message `839292c6-6851-47ab-88ad-9a7349507f7c`. Redis уже перенёс id в `bench:inflight:agent-worker.bench.cursor.126749.aa250ffc`. Ответ клиенту не отдан. В ту же секунду handler не записал строку, второго места у id нет, поле data на месте. Score этого имени дальше не менялся. На 60.846 с id в `done`, в файле одна строка `claim-response`. Лог child: `poll_next: fetch_next failed -> Dead`, `broken pipe`, процесс вышел.

Невидимое владение здесь не продлевается heartbeat. Имя, которое не получило ответ, остаётся в zset со старым score, пока другое имя не забирает задачу по окну 60 с.

## Renewal

`observe_lease` без обрыва: за 22 с `locked_at` сдвинулся с `1794608959` на `1794608974`, прокси увидел один `renew` с id задачи, финал — один `start` и один `end`, место `done`.

`renew_request_dropped`, задача `01M7BGNJZ07D7SAH8YGMEYCCF0`. `renew_leases` с id задачи оборван до Redis. `locked_at` остался прежним, id остался в inflight этого воркера. Child записал `HeartBeat failed -> Dead` / `broken pipe` и вышел в ту же секунду. На 60.878 с id лежит в inflight другого имени, и в файле вторая строка `start`. Наблюдение закончилось на 89 с, пока второй handler ещё был в `asyncio.sleep`: status `Running`, attempts `0`.

`renew_response_dropped`, задача `01M7BGRZYQ09C397HYH3T0YWTC`. Redis применил renew: `locked_at` стал `1794609298`. Членство inflight в эту секунду сохранилось. Child всё равно вышел по `broken pipe`. На 60.87 с вторая строка `start` и другой ключ inflight.

`renew_late_reply`, задача `01M7BGWCY0DXKYNKXA8BEQHPQE`. Прокси прочитал ответ renew и отдал его клиенту через 75 с: удержание с `2026-11-13T22:36:49.867Z`, запись ответа в `22:38:04.868Z`. Child в `22:36:50.368Z` записал `HeartBeat failed -> Dead`, `Database(timed out)`, и в `22:36:51.908Z` вышел. Вторая строка `start` появилась на 60.9 с наблюдения, раньше записи отложенного ответа. К концу наблюдения id остаётся в inflight нового имени `...128502.c8eb2ae6`. Отложенный успех не вернул задачу на старый ключ.

Отдельного generation, который поздний ответ мог бы восстановить, в ключе нет.

## Finish

`finish_request_dropped`, задача `01M7BGZXVCXFYGEGD5975YFWHN`. Handler записал строку, затем `handle_results` не дошёл до Redis. Id остался в исходном inflight, status `Running`, attempts `0`. Child: `ProcessEvents failed -> Dead`. На 60.815 с тот же id в `done`, в файле две строки handler, attempts `1`, результат `ok`. Задача не пропала из учёта. Повторный проход — это уже другой worker name.

`finish_response_dropped`, задача `01M7BH1SJM64JEPE7Y5GZYGRKF`. Redis записал `done` и `attempts=1` до того, как клиент увидел ответ. Строка handler одна. В следующие секунды второй строки нет, в `scheduled` id нет. Child вышел по `ProcessEvents failed`. Повторный finish нового прохода не создал.

## Retry и повтор тела

Актор `failonce` с `max_retries=1` на этом стенде получает `max_attempts=1`. Скрипт `handle_results` ставит повтор только при status `Failed` или `Pending` и `attempt < max_attempts`. При `attempt == 1` этого бюджета нет.

`retry_response_dropped`, задача `01M7BH215RQ25HAPRNZJPJ2CEQ`. В meta одна запись: status `Done`, attempts `1`, result `{"Ok":{"delay_ms":77,"outcome":"retry"}}`, id в zset `done`. Строка handler одна. Продолжения в `scheduled` нет. Child после потерянного ответа вышел. Второго тела и второго id нет. Поле result говорит `retry`, а место задачи — `done`, потому что в команду ушёл status `Done`.

`retry_request_dropped` повторён на Redis pid `131242`, задача `01M7BHFN4EDC6JNR9V923J9XVY`. `handle_results` не дошёл до Redis. Сразу после обрыва id в исходном inflight, status `Running`, attempts `0`. На 2.065 с в файле вторая строка, а id всё ещё в том же inflight. На 60.973 с третья строка, id в `done`, attempts `1`, владелец уже `...131330.0c0098cb`, result `ok`. Тот же id дошёл до терминального состояния. Тело выполнилось три раза.

## Короткий и длинный обрыв

`brief_disconnect`, 3 с, задача `01M7BH21JKWV1XB5GS63DHJRWQ`. Оба child записали `broken pipe` и вышли. Score исходного владельца `...129866.02439655` остался `1794609579` до конца наблюдения 44.6 с. Id всё это время в его inflight. В файле одна строка `start` и нет `end`. 44 с короче окна 60 с, поэтому забор сироты этим сценарием не наступил. Прежнее имя handler не продолжило.

`long_unavailable`, прокси молчал 70 с, задача `01M7BH3K02VGTNZ5BS0SKAK30T`. К моменту, когда сокеты снова пустили, файл уже содержал `start` и `end`, а Redis всё ещё держал id в исходном inflight `...130214.02eb0378`. На 18.263 с после открытия id в inflight другого имени и в файле второй `start`. На 38.548 с id в `done`, в файле две пары `start`/`end`, attempts `1`. Локальное завершение первого handler и членство Redis разошлись, затем тело выполнилось ещё раз.
