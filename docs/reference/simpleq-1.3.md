# simple-q 1.3.0 на тех же сценариях

Проверка версии 1.3.0 на сценариях, которые уже записаны для 1.0.0rc7.
Дерево rc7 воспроизводится с коммита `6dc91f0`. Новые файлы лежат в
`results/simpleq-1.3.0/`, `results/traces/simpleq-1.3.0*`,
`results/long/simpleq-1.3.0/` и `results/kind/simpleq-1.3.0/`.
Каталоги rc7 и 1.2.0 не переписывались.

Пакеты: `simple-q-runtime==1.3.0`, `simple-q-worker==1.3.0`.
Исходники: тег `v1.3.0`, коммит `64d01e7`.
Крейты: apalis 1.2.0, apalis-core 1.2.0, apalis-redis 1.1.1.

Адаптер `bench/simpleq_app.py` вызывает то же, что и на rc7:
`Client(url, config)`, `send_with_options`, `QueueAdmin.snapshot/dead/peek`.
`around_dispatch` по-прежнему получает dict. Менялась только строка версии в docstring.

Сюита: `BENCH_SCALE=short scripts/run_suite.py --lib simpleq` по фазам
smoke, resilience, rollout, redis, bench, плюс сценарий `block_lease`,
которого нет ни в одной фазе. Kind не запускался: в машине нет `docker` и `kind`.

`Config()` установленного пакета без переменных окружения:
`poll_interval_ms=500`, `ping_interval_ms=2000`, `ping_timeout_ms=3000`,
`lifecycle_interval_ms=200`, `sweep_interval_secs=60`, `sweep_batch=100`,
`batch_size=1`, `idempotency_wait_ms=20`, `idempotency_wait_max_ms=5000`,
`ping_budget_secs=1860`, `heartbeat_secs=15`, `missed_heartbeats=4`,
`max_deliveries=5`, `dead_ttl_secs=604800`, `unique_ttl_secs=86400`,
`memory_soft_ratio=0.8`, `memory_hard_ratio=0.95`.
Файл: `results/simpleq-1.3.0/mechanisms.json`, проверка `defaults`.

## Ожидание и выборка

CHANGELOG 1.2.0: пустой воркер берёт задачу по `SIMPLE_Q_POLL_INTERVAL_MS`
(500 мс) через Apalis `poll_with_interval`. Heartbeat, ack, retry и окно сироты
не меняются.
Код: [`redis_api.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/simple-q-runtime/src/redis_api.rs#L44-L58),
[`names.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/simple-q-runtime/src/names.rs#L16-L17).

Измерено, `results/simpleq-1.3.0/idle_gap.json`:

| Heartbeat | Сразу | Задержка send 1 с |
| --- | --- | --- |
| 15 с | 0.332 с | 0.909 с |
| 5 с | 0.326 с | 0.414 с |

На rc7 те же ячейки были 14.866 / 14.69 с и 4.838 / 4.91 с.

Трейс `scripts/wait_trace.py`: `pickup_ms` 536 при heartbeat 5×3
(`results/traces/simpleq-1.3.0-h5.json`) и 535 при умолчаниях
(`results/traces/simpleq-1.3.0-default.json`). py-spy в машине нет, кадр
стека не снят. Серия фаз: при heartbeat 5 с pickup p50 141 мс, сумма фазы и
pickup p50 2636 мс (`results/traces/simpleq-1.3.0-idle-dist.json`); при
умолчаниях pickup p50 147 мс, сумма p50 2636 мс
(`results/traces/simpleq-1.3.0-idle-default.json`). Сумма идёт за фазой 0–4.5 с,
а не за сеткой heartbeat. Push-to-pop: client 107.1 мс и pickup 250 мс при
heartbeat 5 с, client 108.0 мс и pickup 252 мс при умолчаниях.

Короткая пачка 80 задач, `results/simpleq-1.3.0/fast.json`: 0.529 с,
151.21 задачи/с, p50 36 мс, p99 42 мс, max 43 мс. На rc7: 11.786 с, 6.79/с,
p50 11551 мс.

## Time limit и ping budget

CHANGELOG 1.2.0: пропущенный ping budget — 1860 с, встроенные 30 минут плюс
60 с, не `time_limit` актора.
Код: [`names.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/simple-q-runtime/src/names.rs#L28-L40).
`Config()` вернул 1860. Сюита ставит актору `time_limit` сценария и этот
бюджет не переопределяет. Зачётный контроль 2400 с оставляет
`SIMPLE_Q_PING_BUDGET_SECS` пустым и ставит актору `time_limit` 3000000 мс.
Результат: passed, span 2404.414 с, один старт и один финиш handler, 240
прогрессов, 240 эффектов, дублей 0
(`results/long/simpleq-1.3.0/lr-20261119T150521Z-simpleq-A1_control_40-scored`).
Бюджет 1860 с не оборвал сон `asyncio.sleep`: цикл отвечал на ping. В логе нет
строки про неотвечающий Python loop.

Незачётный прогон `results/long/simpleq-1.3.0/interrupted-lr-20261119T145009Z`
получил SIGKILL `-9` на шаге 22. Параллельный `scripts/simpleq_ambiguous.py`
останавливает процессы `bench.simpleq_app`. Этот каталог не зачёт.

## Ack относительно возврата handler

На apalis-core 1.2.0 `TaskPersistService` шлёт `TaskEvent::Lock`, затем ждёт
future handler, и при `should_ack` (`persist_results`) шлёт `TaskEvent::Complete`
([`service.rs`](https://docs.rs/apalis-core/1.2.0/src/apalis_core/backend/persistence/service.rs.html#84),
строки 133–148 того же файла).

Ключ `unique_key` снимается после видимых `done` или `dead`, пока процесс жив
или не вышел `SIMPLE_Q_UNIQUE_TTL_SECS`. Ошибка Redis в этом ожидании
повторяется.
Код: [`worker.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/agent-worker/src/worker.rs#L530-L555),
[`redis_api.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/simple-q-runtime/src/redis_api.rs#L320-L334).

Измерено (`mechanisms.json`):

- после `done` ключ исчез через 0.061 с, второй send с тем же ключом выполнился;
- после SIGKILL процесса ключ остался (`exists` 1), TTL на месте;
- чёрная дыра Redis 2.5 с во время ожидания: ключ снят после возврата Redis;
- один отложенный finish: ключ держался до 3.539 с и был снят, не оставлен на 86400 с.

## Heartbeat и lease

Окно сироты по-прежнему `heartbeat_secs * missed_heartbeats`.
По умолчанию 15×4 = 60 с ([`names.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/simple-q-runtime/src/names.rs#L14-L15)).

`sigkill` при 5×2: разрыв 10.25 с, `deliveries` 1 затем 2, `retries` 0,
тот же message id (`results/simpleq-1.3.0/sigkill.json`).
`blocking` 4 с при окне 10 с: одна доставка, `block-start` и `block-end`,
12.113 с (`results/simpleq-1.3.0/blocking.json`).

Потерянный или просроченный ответ на heartbeat, renewal или ack повторяется
до двух минут. Fetch и enqueue не повторяются.
Код: [`redis_retry.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/simple-q-runtime/src/redis_retry.rs#L19-L29).

Короткие разрывы (`mechanisms.json`): renew, finish и keepalive сбросом
соединения — handler дошёл до конца, один start, тот же pid ребёнка.
Чёрная дыра ack 8 с: handler закончился во время дыры, один start, ребёнок
остался жив. Карточка `bench:done` в этом замере была пуста, поэтому сам
факт записи ack этим файлом не подтверждён.

## Retry и повторная доставка

Retry — новая постановка. Apalis `max_attempts` остаётся 1
([`redis_api.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/simple-q-runtime/src/redis_api.rs#L200),
строка 231 того же файла). TTL счётчика доставок следует
`SIMPLE_Q_DEAD_TTL_SECS`.

Измерено: в meta задачи `max_attempts` равен 1. При `SIMPLE_Q_DEAD_TTL_SECS=120`
TTL ключа доставки 118 с. Два старта одного message id: `retries` 0 затем 1,
`deliveries` 1 на каждом, один и тот же pid.

Чекпоинт, `results/simpleq-1.3.0/long_on.json`: `T_drain` 1.201 с, код 0,
`T_gap` 1.452 с, прогресс 4/4, `retries` 0 → 1, `deliveries` остался 1.
Без чекпоинта, `long_off.json`: `T_drain` 2.001 с, одна попытка.

## Poison

Пять SIGKILL, `results/simpleq-1.3.0/five_kills.json`: статус `failed`,
`fail_reason` `poison deliveries=6 retries=0`.

## Дедуп

Сюита `dedup`: второй send не принят, после ack тот же ключ принимается снова
(`results/simpleq-1.3.0/dedup.json`). Снятие ключа — в разделе про ack.

## Recycle и память

CHANGELOG 1.1.0. Выбор по RSS. Per-child soft — `SIGUSR1` каждому ребёнку
выше своего порога, per-child hard — `SIGKILL`, несколько детей в одну секунду.
80% cgroup — `SIGUSR1` одному ещё не дренируемому с наибольшим RSS, через
секунду следующему. 95% — `SIGKILL` самому большому, если его RSS не меньше
половины рабочего набора; если он уже убивается по своему hard, второго не
убивают. Если самый большой меньше половины — никого не убивают, `SIGUSR1`
самому большому ещё не дренируемому. Недоставленный `SIGUSR1` не записывается.
Код: [`supervisor.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/agent-worker/src/supervisor.rs#L670-L734)
и запись drain только после успешного `kill`
([строки 1044–1048](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/agent-worker/src/supervisor.rs#L1044-L1048)).
`signal_child` возвращает false, если `libc::kill` не прошёл
([строки 432–438](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/agent-worker/src/supervisor.rs#L432-L438)).

Soft во время delivery пропускает занятого ребёнка (счётчик на memfd).
Hard занятость не читает. После SIGKILL цифра слота обнуляется.

Измерено:

- per-child soft: три pid в одну секунду, `draining child reason=memory soft`, RSS около 30 МБ при пороге 1 МБ;
- per-child hard: три `SIGKILL` в одну секунду;
- soft во время сна 6 с: handler дошёл до конца за 6.132 с, drain через ~1 с после возврата (`mechanisms-follow.json`, `soft_during_delivery`);
- hard во время сна: один `killing child`, handler до конца не дошёл;
- после SIGKILL занятого (RSS 85532672, hard 50 МБ) следующий ребёнок в слоте получил soft-drain при RSS около 30 МБ и soft 20 МБ — занятым не считался;
- недоставленный `SIGUSR1`: строка `failed to signal`, через секунду drain того же pid, `same_pid_retried` true;
- 80% cgroup (`mechanisms-cgroup.json`, `cgroup_80`): три разных pid, по одному в секунду, `draining the largest child`, kill нет. usage около 54 МБ, limit 63164416;
- 95% и RSS не меньше половины (`cgroup_95_half`): `killing the largest child`, pid 18454, rss 30281728, usage 41385984, limit 42844160;
- 95% и самый большой меньше половины (`mechanisms-follow.json`, `cgroup_ratios`): kill нет, `draining the largest child` для pid 16948 и через секунду 16949. RSS около 30 МБ, `memory.current` 66158592, max 68915200;
- «второго не убивают, если первый уже уходит по своему hard»: не отделено. RSS остался около 30 МБ, ниже per-child hard 46 МБ, и сработал один ratio-kill (`cgroup_95_no_second_kill`).

Первые три проверки cgroup в `mechanisms.json` воркер в cgroup не ставили.
Цифры выше — из `mechanisms-cgroup.json` и `mechanisms-follow.json`.
Контейнер — вложенный cgroup v2 с `memory.max`, не kind.

Сюита `recycle`: 12 элементов, 5 pid, потерь 0 (`results/simpleq-1.3.0/recycle.json`).
Сюита `soft_memory`: потерян 1 из 8, статус `running`. В логе метрик
`Address already in use` на порту 9191. Это не чистый ноль потерь rc7.

## SIGTERM и drain

Ранний SIGTERM 0.4 / 1.5 / 2.5 с: `T_drain` 0.2 с, код 0
(`results/simpleq-1.3.0/early_sigterm.json`).
`shell_sigterm` и `double_early` тоже код 0.
`sigterm_short`: `T_drain` 5.004 с, код 0.

## Смерть supervisor

CHANGELOG 1.3.0: `SIGKILL` supervisor переводит детей в drain через pidfd.
Принятая delivery доделывается и подтверждается, новых fetch нет.
Код: [`parent_pidfd.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/agent-worker/src/parent_pidfd.rs#L1-L21),
[`child.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/agent-worker/src/child.rs#L182-L184).

Измерено, `results/simpleq-1.3.0/mechanisms-cgroup.json`, `supervisor_sigkill`:
лог `supervisor exited; draining accepted work`, handler дошёл до конца,
вторая задача не взята, выход ребёнка через 4.067 с (сон принятой задачи 4 с).
Первый заход в `mechanisms.json` смешал чужие pid и как время выхода не используется.

## Потеря ответа Redis

CHANGELOG 1.3.0: heartbeat, renewal и ack повторяются до двух минут.
Fetch и enqueue не повторяются: потерянный ответ fetch завершает имя worker,
задачу берёт другое имя после окна сироты. Ошибка backend, которая завершает
worker, поднимает его в том же процессе под новым именем. Первый broken pipe
ребёнка процесс не завершает. Разрыв длиннее окна сироты может выполнить тело
дважды.
Код: [`redis_retry.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/simple-q-runtime/src/redis_retry.rs#L8-L24),
[`worker.rs`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/agent-worker/src/worker.rs#L133-L138)
и [`BACKEND_RESTARTS`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/crates/agent-worker/src/worker.rs#L211-L219).

Измерено:

- потерянный fetch (`mechanisms.json`, `fetch_lost`): `BackendError` broken pipe, `restarting worker`, новое имя, тот же pid. Тело через ~4.0 с при окне 2×2 = 4 с. Один start;
- разрыв длиннее окна (`gap_over_orphan`): окно 4 с, чёрная дыра 8 с, два процесса, два start с разницей около 11.5 с, один end;
- набор `scripts/simpleq_ambiguous.py` при окне 60 с, файлы `results/long/simpleq-1.3.0/ambiguous/`: `long_unavailable` 70 с — один start и `done`; `renew_late_reply` 75 с — два start. Потерянный renew не остановил старого владельца за 90 с: score продолжал двигаться. На 1.2.0 тот же скрипт ждал остановки владельца после бюджета. Это расхождение контракта скрипта с повтором 1.3.0, не сбой установки;
- `scripts/simpleq_failonce_trace.py` и `scripts/simpleq_retry_enqueue_fault.py` записаны в `results/long/simpleq-1.3.0/failonce-trace/` и `results/long/simpleq-1.3.0/retry-enqueue/`. Контроль `failonce` выполнил тело дважды (`retries` 0 и 1). Потерянный ответ enqueue оставил исходную попытку inflight и породил два task с `retries` 1 (4 входа handler). Подробности в `docs/reference/ambiguous-redis.md`.

Полные две минуты потолка `REPLAY_FOR` этим прогоном не выжидались. В коде константа 120 с.

## Admin и отложенные сообщения

CHANGELOG 1.3.0: `dramatiq_delayed_messages_inprogress` остаётся 0.
`App.dispatch` получает dict. Admin-чтения возвращают тот же вид.
`QueueAdmin` — `Snapshot`, `DeadPage`, `TaskPage`.
Код gauge: [`metrics.py`](https://github.com/hewimetall/simple-redis-q/blob/64d01e7/python/simple_q/metrics.py#L120-L126).

Измерено: `admin_shapes` — типы совпали, `dispatch_is_dict` true.
Снимок: `results/simpleq-1.3.0/admin-simpleq.jsonl`.
Скрейп после одной выполненной задачи и во время отложенной
(`mechanisms-cgroup.json`, `prom_delayed`) серии
`dramatiq_delayed_messages_inprogress` не содержит. Нуля в строке нет:
нетронутый gauge в multiprocess-сбор не попал.

## Что не запускалось

Kind, `scripts/kind_rollout.py`: в машине нет исполняемых `docker` и `kind`.
Файл: `results/kind/simpleq-1.3.0/kind.json`, `measured` false.
Сценарии cgroup, которым нужен лимит памяти контейнера, шли во вложенном
cgroup v2, не в kind.
