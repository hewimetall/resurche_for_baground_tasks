# Очередь для фоновых задач async-сервиса

Стенд сравнивает simple-q `1.0.0rc7` с Dramatiq 2.2.1, Celery 5.7.0, arq 0.28.0 и taskiq 0.13.0 + taskiq-redis 1.2.4. Сервис — async Python 3.12, брокер Redis, состояние в Postgres. Цифры ниже сняты на шкале `BENCH_SCALE=short` и на однонодовом kind. Таблицы — в `docs/`, сырые ответы — в `results/`.

## Что подошло

Для обязательного набора подходит **simple-q 1.0.0rc7**.

Она одна закрывает его средствами библиотеки: async-handler, Redis, дедуп на `send`, отдельный счётчик `deliveries`, poison и хук `on_dead`, супервизор с лимитом RSS, `MemoryClient` для тестов без Redis. На kind с чекпоинтом `kubectl rollout status` завершился за 2.2 с, 6 из 6 шагов на новом поколении. На процессе с чекпоинтом `T_drain` 1.2 с и `T_gap` 1.46 с, растёт `retries`, `deliveries` остаётся 1.

В сервис её ставить при трёх условиях.

1. Heartbeat 5 с и 3 пропуска, не умолчание 15×4. Пустая очередь опрашивается с периодом heartbeat: 4.8 с при 5 с и 14.9 с при 15 с (`results/simpleq/idle_gap.json`). На пачке из 80 задач при heartbeat 15 с это 6.8 задачи/с и p50 11.6 с: задачи ждут следующий опрос.
2. Длинная задача между шагами смотрит флаг drain и бросает `Draining`. С этим флагом rollout короткий. Под, который флаг не проверяет, на kind дожил grace 12 с и получил SIGKILL (137). simple-q вернула ту же задачу через 14.6 с с `deliveries=2`. За следующие 75 с из четырёх библиотек в kind повторно стартовала только она.
3. `max_deliveries` выше числа подряд идущих SIGKILL на одном сообщении. Пять kill дали `deliveries=6` и вызов `on_dead`.

До поставки остаются два ограничения вне поведения очереди. У репозитория [hewimetall/simple-redis-q](https://github.com/hewimetall/simple-redis-q) GitHub API не нашёл файл лицензии (`/license` вернул 404). На PyPI эта версия — release candidate.

Ранний SIGTERM simple-q на процессе не теряет работу: через 0.4, 1.5 и 2.5 с после старта выход за 0.2 с, код 0. На kind сигнал на первой секунде жизни пода дал код 0 за 1–4 с у simple-q, Dramatiq, Celery и taskiq.

## Когда уместна другая библиотека

| Библиотека | Когда она уместна |
| --- | --- |
| Dramatiq 2.2.1 | Сервис уже на ней, чекпоинт и grace пода устраивают, а дедуп и poison закрываются своим кодом. На стенде второй `send` исполнился, лимита RSS нет, пять SIGKILL оставили job в `running`. |
| Celery 5.7.0 | Важны короткая пачка и `inspect`. На тех же 80 задачах Celery дал 244/с и p50 17 мс. Handler синхронный, дедупа на send нет, одно `visibility_timeout` одновременно держит живую длинную задачу и возвращает её после kill. |
| arq 0.28.0 | Нужны нативный async и отсечение по `_job_id`. Ключ остаётся занятым после успеха. Пакет требует `redis<6`, в один образ с taskiq-redis не встаёт. В kind этот воркер не запускался. |
| taskiq 0.13.0 | Нужны нативный async и Redis Streams. Дедупа на send нет. С `--ack-type when_executed` kill после старта потерял задачу. `--max-tasks-per-child` на пачке из 12 доставил 1 и потерял 11. |

## Что измерялось

Шкала `short`: 80 коротких задач, долгая задача — 4 шага по 1 с. 10 000 задач, 12 минут и 5 минут из шагов по 5 с не запускались.

| Документ | О чём он |
| --- | --- |
| `docs/requirements.md` | Критерии M1–M14 и S1–S5 |
| `docs/matrix.md` | Библиотека × требование: нативно, конфигурация, свой код, нет |
| `docs/rollout.md` | SIGTERM, чекпоинт, однонодовый kind |
| `docs/resilience.md` | Потеря Redis, kill, poison, дедуп |
| `docs/bench.md` | Короткая пачка: throughput, задержка, RSS |
| `docs/extensibility.md` | Сколько дописывать и риск лицензии |
| `docs/migration.md` | Переход с Dramatiq и обратный путь |
| `docs/index.md` | Как пустой воркер узнаёт о задаче: Diátaxis, трейсы, Dramatiq 1.18 |
| `results/` | JSON прогонов, включая `results/kind/rollout.json` и `results/traces/` |

## Как воркер ждёт задачу

Подписки на очередь у simple-q нет. Пустой воркер спит до следующего heartbeat: при 5 с один замер дал 2484 мс, потому что сообщение ушло на 2.8 с жизни процесса. Celery в том же трейсе стоит в `BRPOP` с таймаутом 1 с (230 мс). Dramatiq делает `LPOP` и `time.sleep`; потолок сна на воркере — `worker_timeout` 1000 мс, не 5000 мс из сигнатуры `consume`. После 16 с простоя Dramatiq 1.18.0 и 2.2.1 оба уложились в 132–776 мс, оба через `LPOP`.

Страницы фактов и причин: `docs/index.md`. Сборка: `sphinx-build -b html docs docs/_build/html`. Трейс: `scripts/wait_trace.py`. Сравнение 1.18: `scripts/dramatiq_118_gap.py`.

Код стенда: `bench/core.py` и адаптеры `bench/simpleq_app.py`, `bench/dramatiq_app.py`, `bench/celery_app.py`, `bench/arq_app.py`, `bench/taskiq_app.py`. Образ kind — `deploy/kind/`.

## Воспроизведение

Процессы, основной интерпретатор (simple-q, Dramatiq, Celery, taskiq):

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
BENCH_SCALE=short .venv/bin/python scripts/run_suite.py --lib simpleq --phase smoke
```

arq живёт во втором venv:

```bash
python3.12 -m venv .venv-arq
.venv-arq/bin/pip install -r requirements-arq.txt
```

Сюите нужны Redis 7 и Postgres 16 на localhost, Redis без пароля: `scripts/run_suite.py` ходит в `redis://127.0.0.1:6379/0`. Это другой контур, чем compose ниже.

Kind, один узел, потолок контейнера 3 ГиБ:

```bash
scripts/kind-up.sh
.venv/bin/python scripts/kind_rollout.py
```

Compose ставит зависимости в образ и публикует Redis, Postgres и mock только на `127.0.0.1`. Пароль обоих — `bench`. Одновременно включается один профиль: `simpleq`, `dramatiq`, `celery`, `taskiq` или `arq`.

```bash
docker compose --profile simpleq up --build
```

arq собирается из `deploy/compose/Dockerfile.arq`, остальные воркеры — из `deploy/compose/Dockerfile`. Порт 6379 у compose совпадает с Redis сюиты, вместе их не поднимать. Стек для одной машины: стендовый пароль и mock без аутентификации наружу не выставляются.
