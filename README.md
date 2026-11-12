# Очередь для async-сервиса

simple-q `1.0.0rc7` закрывает обязательный набор (нативный async, Redis, дедуп при `send`, отдельный счётчик `deliveries`, poison, супервизор с лимитом памяти). Брать её в сервис можно при трёх условиях: heartbeat короче 15 с, долгая задача по флагу drain сама пишет чекпоинт и уходит в retry, `max_deliveries` выше числа подряд идущих SIGKILL на одном сообщении.

Главный риск поставки: `GET https://api.github.com/repos/hewimetall/simple-redis-q/license` вернул 404, распознанной лицензии нет, на PyPI это rc. Технический риск простоя: воркер без задач берёт новую с периодом heartbeat. Измерено 14.9 с при 15 с и 4.8 с при 5 с (`results/simpleq/idle_gap.json`).

Rollout с чекпоинтом короткий. Процессы: simple-q `T_drain` 1.2 с, `T_gap` 1.46 с, растёт `retries`, не `deliveries`. Kind (один узел, Kubernetes 1.37): `kubectl rollout status` 2.2 с, код 0, 6/6 шагов. Без чекпоинта kind держал под весь grace 12 с и прислал SIGKILL (137); simple-q вернул задачу через 14.6 с с `deliveries=2`. Dramatiq, Celery и taskiq с чекпоинтом тоже отпускали под за 1–4 с. Без чекпоинта все четверо доживали grace; за следующие 75 с повторно стартовал только simple-q. arq в kind не ставился: arq 0.28 требует `redis<6`.

Ранний SIGTERM simple-q rc7 не теряет: через 0.4, 1.5 и 2.5 с после старта выход за 0.2 с, код 0. На kind сигнал на первой секунде жизни пода дал код 0 за 1–4 с у всех четырёх библиотек.

Шкала стенда — `BENCH_SCALE=short`: 80 коротких задач, долгая задача 4 шага по 1 с. 10 000 задач, 12 минут и 5 минут из шагов по 5 с не прогонялись. Пропускная способность simple-q на этих 80 задачах, 6.8/с и p50 11.6 с, — это период опроса, а не потолок исполнителя. Celery на том же размере дал 244/с и p50 17 мс.

Воспроизведение. Процессы: Redis 7 и Postgres 16 на localhost, два venv из `requirements.lock` и `requirements-arq.lock`, `BENCH_SCALE=short python scripts/run_suite.py --lib simpleq --phase smoke`. Kind: `scripts/kind-up.sh`, затем `scripts/kind_rollout.py`. Сырые JSON лежат в `results/`. Разбор — в `docs/`.
