# Rollout

Два слоя измерений. `docker-compose.yml` описывает Redis, Postgres, mock и воркеры, но числа в `results/` получены процессами на хосте и однонодовым kind, не этим compose.

Процессы на хосте, шкала `short`: долгая задача — 4 шага по 1 с, grace сценария 8 с (`scripts/run_suite.py`, `scale_params`). Это не эмуляция 30 минут. JSON: `results/<lib>/long_on.json`, `long_off.json`, `early_sigterm.json`, `double_early.json`, `sigterm_infinite.json`, `shell_sigterm.json`, `five_kills.json`.

Kubernetes: kind `0.33.0`, один control-plane, образ узла `kindest/node:v1.37.0`, Docker `29.1.3`. Конфиг — `deploy/kind/cluster.yaml`. Память контейнера узла ограничена 3 ГиБ. За время сценариев `docker stats` показывал 820–932 МиБ, свободная память хоста не опускалась ниже примерно 4 ГБ. Второй узел не поднимался: у kind ноды делят память хоста и не изолируют её. arq в кластер не входил: ему нужен `redis<6`, образ собран на redis 8. Сырые ответы API — `results/kind/rollout.json`. Сэмплы памяти — `results/kind/admin-*.jsonl`.

`metadata.deletionTimestamp` у Pod — дедлайн grace, а не момент SIGTERM. Момент сигнала считается как дедлайн минус `terminationGracePeriodSeconds`. `T_drain` ниже для kind посчитан так. Поле `t_drain_s` в уже записанном JSON — это `finishedAt − deletionTimestamp` без этой поправки (у чекпоинта simple-q там −40 при grace 40, то есть выход в ту же секунду, что и сигнал).

## Процессы, чекпоинт включён

Флаг drain выставляется до SIGTERM. Задача поднимает `Draining`, адаптер превращает это в retry.

| Библиотека | T_drain, с | Код | T_gap, с | Прогресс | Что изменилось в попытке |
| --- | --- | --- | --- | --- | --- |
| simple-q | 1.201 | 0 | 1.46 | 4/4 | тот же message id, `retries` 0 → 1, `deliveries` остался 1 |
| Dramatiq | 8.010 | −9 | 2.26 | 4/4 | retries 0…4, несколько повторов, пока старый процесс ещё умирал |
| Celery | 1.201 | 0 | 1.60 | 4/4 | retries 0 → 1 |
| arq | 1.601 | 0 | 2.26 | 4/4 | try 1 → 2, тот же job id |
| taskiq | 1.201 | 0 | 1.66 | 4/4 | вторая запись `long` на новом процессе |

Dramatiq на этом прогоне не уложился в grace и получил SIGKILL, хотя чекпоинт в итоге доехал. У simple-q ключ дедупа на этом retry не мешал: повтор использует то же сообщение.

## Процессы, чекпоинт выключен

На шкале `short` задача из 4×1 с успевала закончиться на старом процессе до осмысленного разрыва. У всех пяти `gap_s` пустой, прогресс 4/4, второй попытки нет. Это не ответ на вопрос «под ждёт 30 минут». Ответ даёт kind-сценарий `kill`: блокировка на 180 с, grace 12 с, preStop нет.

Отдельный бесконечный `time.sleep` и короткий grace (`sigterm_infinite`, grace 6 с): simple-q, Dramatiq, arq и taskiq убиты по истечении grace (код −9). Celery в этом JSON вышел с кодом 0 за 1.2 с — для блокирующей задачи это другой сценарий (`blocking`), см. `docs/resilience.md`.

## Kind, RollingUpdate maxSurge 1, maxUnavailable 0

Образ `bench-worker:local`, один реплика-под, лимит контейнера 512 МиБ, команда в exec-форме. preStop для чекпоинта: `touch /tmp/draining`. Очередь — Redis на порту 6380, Postgres — база `bench_k8s`, чтобы `FLUSHALL` процессного стенда их не задел.

Чекпоинт, 6 шагов по 2 с, grace 40 с, heartbeat simple-q 5×3:

| Библиотека | T_drain, с | Код | `rollout status`, с | Шаги | Вторая попытка |
| --- | --- | --- | --- | --- | --- |
| simple-q | 0 (та же секунда, что SIGTERM) | 0 | 2.175 | 6/6 | через 5.0 с, `retries=1`, `deliveries=1`, другой pod |
| Dramatiq | под снят за 3.9 с вместе со status; `finishedAt` в последний опрос не попал | — | 3.896 | 6/6 | через 4.9 с, retries 0 → 1 |
| Celery | 2 | 0 | 2.857 | 6/6 | через 5.1 с |
| taskiq | под снят за 1.1 с; `finishedAt` не попал в опрос | — | 1.145 | 6/6 | через 4.8 с |

Без чекпоинта, `blocker` на 180 с, grace 12 с:

| Библиотека | T_drain, с | Код | `rollout status`, с | Повтор за 75 с после дедлайна |
| --- | --- | --- | --- | --- |
| simple-q | 12 | 137 | 13.755 | да, +14.6 с от SIGKILL, `deliveries=2`, `retries=0` |
| Dramatiq | 12 | 137 | 13.236 | нет |
| Celery | 12 | 137 | 13.409 | нет. В поде стоял `CELERY_VISIBILITY_TIMEOUT=30` |
| taskiq | 12 | 137 | 13.067 | нет. Воркер запущен с `--ack-type when_executed`: сообщение подтверждается в момент старта |

Пик памяти узла в этих секундах — до 932 МиБ при потолке 3 ГиБ. Два пода (старый и новый) помещались. `rollout status` без чекпоинта ждал grace и становился зелёным сразу после SIGKILL, потому что новый под к этому моменту уже был Ready.

Двойной rollout в первую секунду жизни пода (задачи нет, grace 20 с). Возраст контейнера на момент `rollout restart` — 1.0 с.

| Библиотека | Первое поколение | Второе поколение | `rollout status` |
| --- | --- | --- | --- |
| simple-q | T_drain 1 с, код 0 | момент выхода не снят, деплой дошёл до Ready | успешно |
| Dramatiq | 3 с, код 0 | 3 с, код 0 | успешно |
| Celery | 3 с, код 0 | 4 с, код 0 | успешно |
| taskiq | 2 с, код 0 | 2 с, код 0 | успешно |

Потери сигнала в первые 1–3 с на rc7 в этом кластере не воспроизвелись. Пять rollout подряд на одной долгой задаче в kind не делались. На процессах пять SIGKILL подряд по одной задаче simple-q довели её до poison: `fail_reason = poison deliveries=6 retries=0` (`results/simpleq/five_kills.json`). У Dramatiq, Celery, arq и taskiq за окно того же сценария статус остался `running`, отдельной причины poison нет.

## Что из этого следует для пода с задачей до 30 минут

Чекпоинт между шагами уводит сообщение в retry, и старый под выходит за секунды, не дожидаясь grace. Без чекпоинта под живёт до `terminationGracePeriodSeconds`, затем kubelet шлёт SIGKILL. Дальше срок возврата задачи — окно владения брокера:

- simple-q: heartbeat × пропущенные heartbeat после смерти процесса, и каждая такая смерть увеличивает `deliveries`;
- Dramatiq: heartbeat воркера, а фактическая перекладка сообщения зависит от maintenance брокера (на kind за 75 с её не случилось);
- Celery: `visibility_timeout` от момента выдачи. Короткий timeout при живой длинной задаче даёт второе исполнение. Длинный timeout после kill заставляет ждать почти всю длительность задачи;
- arq: TTL in-progress от `job_timeout`, та же развилка;
- taskiq: `idle_timeout` для `XAUTOCLAIM`. С `--ack-type when_executed` kill после старта задачу не возвращает.

Prefetch. На процессах simple-q вернул недовыполненные элементы за 0.26 с после SIGTERM, потерь 0 (`results/simpleq/prefetch.json`). Dramatiq потерял 2 элемента из пачки, Celery 1, taskiq 6. Это короткая шкала, не нагрузка на 1000 элементов.
