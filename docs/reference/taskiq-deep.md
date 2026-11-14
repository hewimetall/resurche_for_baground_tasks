# Taskiq deep research (Redis Streams)

Только Taskiq. Без сравнения с другими очередями, без Rust/supervisor/simple-q.

## 0. Версии и окружение

| Поле | Значение |
| --- | --- |
| Taskiq | `0.13.0` (`a00e4b0030f5ada13068da8644048867967aeda4`) |
| taskiq-redis | `1.2.4` (`8f70bc85dd6a469cae7d3b467af7dea720c92434`) |
| redis-py | `8.1.0` |
| Redis server | `7.0.15` |
| Python | `3.12.3` |
| ОС | Linux x86_64 (glibc 2.39) |
| Baseline PR | [PR №1](https://github.com/hewimetall/resurche_for_baground_tasks/pull/1) |
| Актуальные vs baseline | **совпадают** → дублирующие version-diff эксперименты не запускались |
| Стенд | `research/taskiq/` |
| Артефакты | `results/taskiq-deep/` |
| Redis стенда | `127.0.0.1:6413` |
| Запуск | `PYTHONPATH=. .venv/bin/python -m research.taskiq.run_all` |

Lock: `requirements.lock` (taskiq/taskiq-redis/redis pinned). Effective worker flags стенда по умолчанию: `--workers 1 --max-async-tasks 1 --max-prefetch 0 --ack-type when_executed`.

Классы утверждений: **измерено** / **исходники** / **документация** / **не проверено** / **гипотеза**.

---

## 1. Архитектура

```text
producer (kiq / kick / XADD)
  → RedisStreamBroker
  → XREADGROUP (COUNT=xread_count, BLOCK=xread_block, noack=False)
  → AckableMessage yield (batch уже в PEL)
  → Receiver.prefetcher (sem_prefetch = max_async_tasks + max_prefetch)
  → local asyncio.Queue
  → runner (sem = max_async_tasks)
  → middleware.pre_execute → DI → handler
  → on_error / retry.kiq
  → ACK? (when_executed)
  → post_execute → result_backend.set_result → post_save
  → ACK? (when_saved)
```

| Компонент | Ответственность | Публичные точки | Внутренние API |
| --- | --- | --- | --- |
| CLI `taskiq worker` | parse args, process manager | `--workers`, `--ack-type`, `--max-async-tasks`, `--max-prefetch`, `--shutdown-timeout`, `--hardkill-count`, `--max-tasks-per-child` | `taskiq/cli/worker/args.py`, `run.py` |
| ProcessManager | fork/reload/SIGTERM children | workers count, reload | `process_manager.py` |
| Execution worker | event loop, Receiver.listen | startup/shutdown broker hooks | `run.start_listen` |
| Receiver | prefetch + execute + ACK policy | `ack_type`, semaphores | `receiver.py` |
| RedisStreamBroker | XADD/XREADGROUP/XACK/XAUTOCLAIM | `idle_timeout`, `xread_count`, `unacknowledged_lock_timeout` | `taskiq_redis/redis_broker.py` |
| ListQueueBroker / PubSub | отдельные брокеры **без** Streams ACK/reclaim | `BRPOP` / pubsub | тот же пакет |
| Result backend | optional `set_result` | `RedisAsyncResultBackend` | не обязателен для ACK `when_executed` |
| Scheduler | cron/offset sources | `TaskiqScheduler`, schedule sources | core, отдельно от worker listen |
| Middleware / DI | pre/post/on_error, Depends | `TaskiqMiddleware`, `taskiq-dependencies` | вызываются из Receiver |

**Не смешивать:** core Taskiq ≠ taskiq-redis ≠ код стенда `research/taskiq/app/broker.py`.

---

## 2. Delivery, concurrency, prefetch

### RedisStreamBroker (основной фокус)

```text
утверждение: XREADGROUP COUNT зачисляет batch в PEL до локального исполнения
→ 1.2.4
→ https://github.com/taskiq-python/taskiq-redis/blob/8f70bc85dd6a469cae7d3b467af7dea720c92434/taskiq_redis/redis_broker.py#L276-L295
→ e01 `results/taskiq-deep/correctness/*e01-batch-pel/result.json`
→ pending_during_hold=1; после освобождения слота max_pending=8 при handler_starts=1
→ измерено + исходники. Граница: xread_count=10, max_async=1, max_prefetch=0.
```

```text
утверждение: --max-async-tasks=1 ≠ «worker держит одно сообщение в Redis»
→ 0.13.0 delivery_capacity = max_async + max_prefetch
→ https://github.com/taskiq-python/taskiq/blob/a00e4b0030f5ada13068da8644048867967aeda4/taskiq/receiver/receiver.py#L121-L138
→ e01
→ локальный semaphore ограничивает yield/execute; PEL может содержать весь COUNT-batch
→ измерено.
```

Связь:

```text
worker_processes × max_async_tasks          — параллельные исполнения
+ max_prefetch                               — локально буферизованные, ещё не running
+ (xread_count − уже yielded из текущего batch) — уже в PEL, ещё в generator
```

Backpressure Taskiq: `sem_prefetch`. Backpressure Redis Stream: нет штатного (кроме maxlen на XADD).

### Другие Redis brokers (отдельно)

| Broker | Модель | ACK/reclaim |
| --- | --- | --- |
| ListQueueBroker | `BRPOP` | нет; pop удаляет сообщение |
| PubSubBroker | pub/sub | нет; потеря при отсутствии подписчика |
| RedisStreamClusterBroker / Sentinel stream | XREADGROUP | **нет XAUTOCLAIM** в 1.2.4 ([#130](https://github.com/taskiq-python/taskiq-redis/issues/130)) |

---

## 3. ACK, results, retries

Порядок из `Receiver.callback` / `run_task`:

```text
parse → pre_execute → [ACK when_received]
→ DI resolve → handler → dep close
→ on_error (SimpleRetryMiddleware.kiq здесь)
→ [ACK when_executed]
→ post_execute → set_result → post_save
→ [ACK when_saved]
```

Ссылки:

- ACK modes: [receiver.py#L204-L234](https://github.com/taskiq-python/taskiq/blob/a00e4b0030f5ada13068da8644048867967aeda4/taskiq/receiver/receiver.py#L204-L234)
- on_error before return: [receiver.py#L399-L408](https://github.com/taskiq-python/taskiq/blob/a00e4b0030f5ada13068da8644048867967aeda4/taskiq/receiver/receiver.py#L399-L408)
- retry publication: [simple_retry_middleware.py#L97-L107](https://github.com/taskiq-python/taskiq/blob/a00e4b0030f5ada13068da8644048867967aeda4/taskiq/middlewares/simple_retry_middleware.py#L97-L107)

| Вопрос | Ответ | Класс |
| --- | --- | --- |
| ACK после ошибки handler? | Да для when_received/executed/saved (после run_task / save), если сообщение ackable | исходники + e05 |
| Без result backend | Dummy path; `when_saved` всё равно ACK после попытки save; post_save вызывается | e05 norb |
| Ошибка middleware/DI | DI exception → on_error → дальше по ack_type; parse/unknown task → ACK сразу | исходники |
| Retry до ACK оригинала | Да при when_executed/when_saved: `kiq` в on_error, затем ACK оригинала | исходники + e05 boom |
| Сохранение task_id/labels | SimpleRetry: `with_task_id(message.task_id)`, labels копируются, `_retries`++ | исходники |
| Delivery counter / poison | Redis PEL `times_delivered` растёт на reclaim; **отдельной poison policy в Taskiq нет** | измерено e03 (delivery_count=2) |
| manual ACK | `Context.ack()`; обязанности на приложении; без ack сообщение остаётся pending | e05 manual (после фикса ready-handshake) |

Failure boundaries (сжато):

1. До handler (parse/unknown): ACK, нет исполнения.
2. Во время handler + SIGKILL: нет ACK → PEL.
3. После эффекта до конца handler + kill: эффект есть, PEL остаётся.
4. После handler до set_result: при when_executed уже ACK.
5. После save до ACK: только when_saved окно.
6. После ACK потеря ответа producer: результат зависит от result backend TTL; доставка уже закрыта.
7. Retry опубликован, оригинал не ACK: две записи в stream до ACK оригинала.

---

## 4. Redis Streams ownership / recovery

```text
утверждение: пустой XREADGROUP делает continue до XAUTOCLAIM
→ 1.2.4 lines 287-288
→ https://github.com/taskiq-python/taskiq-redis/blob/8f70bc85dd6a469cae7d3b467af7dea720c92434/taskiq_redis/redis_broker.py#L287-L288
→ e02 (idle_ms=3000, observe 11s): reclaimed_by_b=False, pending остался у мёртвого consumer, idle>idle_timeout
→ измерено. Совпадает с обсуждением в issue/PR #129.
```

| Сценарий | Результат | Артефакт |
| --- | --- | --- |
| A. Смерть, нет новых сообщений | pending не reclaim'ится живым consumer | `recovery/*e02*` |
| B. Смерть + новый трафик | XAUTOCLAIM срабатывает; victim стартует на B; delivery_count=2 | `recovery/*e03*` |
| C. Смерть с lock TTL | lock с `ex=TTL` исчезает; default `unacknowledged_lock_timeout=None` — lock может висеть вечно | `recovery/*e04*` + [#131](https://github.com/taskiq-python/taskiq-redis/issues/131) / [#107](https://github.com/taskiq-python/taskiq-redis/pull/107)→1.2.2 |

`idle_timeout` default `600000` ms — это `min_idle_time` XAUTOCLAIM, **не** heartbeat и не execution timeout. Pending idle **не** обновляется во время handler (e06: idle 557→10388 ms).

Штатного продления владения нет. API extend из handler нет (issue [#86](https://github.com/taskiq-python/taskiq-redis/issues/86); рекомендация мейнтейнера — `when_received`).

---

## 5. Долгая живая задача

Конфигурация: 2 worker × `max_async=1`, `xread_count=1`, keepalive traffic (иначе quiet-gate).

| kind | idle_ms | task_s | starts A/B | overlap | reclaim_delay_s |
| --- | --- | --- | --- | --- | --- |
| sleep_async | 4000 | 12 | 1/1 | да | ~10.8 |
| sleep_block | 4000 | 12 | 1/1 | да | ~10.8 |
| sleep_sync | 4000 | 12 | 1/1 | да | ~10.8 |
| tick | 4000 | 12 | 1/1 | да | ~10.8 |
| tick diagnostic | 15000 | 90 | 1/1 | да | ~15.0 |
| tick **full 2400** | 600000 | 2400 | 1/1 | да | **600.978** |

```text
утверждение: второй consumer может забрать живую задачу после idle_timeout при наличии трафика; исполнения пересекаются
→ 0.13.0 + 1.2.4
→ e06-matrix, e11-diagnostic
→ нет ownership extension; старый handler не узнаёт о потере владения штатно; его поздний XACK снимает PEL id уже после reclaim (семантика Redis XACK по id)
→ измерено. Граница: when_executed; keepalive traffic включён.
```

Полный контроль 2400 с при `idle_timeout=600000`, 2 worker × max_async=1, keepalive traffic: каталог `results/taskiq-deep/long/20261113T215617Z-e06-long-tick/`.

| Метрика | Значение |
| --- | --- |
| `span_a_s` | 2403.749 |
| `reclaim_delay_s` | 600.978 |
| `starts_a` / `starts_b` | 1 / 1 |
| `finishes_a` / `finishes_b` | 1 / 0 (окно наблюдения закрылось после finish A) |
| `overlap_while_a_unfinished` | true |
| pending idle sampled | 562 … 3_074_111 ms |
| pending после ACK A | 0 |

Файлы: `result.json`, `final.json`, `milestone-reclaim.json`, `e11-full2400-final.json`. Одноворкерный 2400 с из PR1 **не** проверяет reclaim другим consumer.

---

## 6. Prefetch / recycle — первопричина PR1

PR1: recycle 1/12; prefetch «потеря» 6/8.

```text
утверждение: --max-tasks-per-child=1 + xread_count=100 → один XREADGROUP кладёт ≥N в PEL; после recycle child новый consumer не видит pending на тихом потоке
→ e07_recycle: finished=1, lost_in_window=11, final_pending=11 у старого consumer
→ не называть «безвозвратно потеряны»: oracle XAUTOCLAIM / новый трафик могут вернуть
→ измерено. Минимальный reproducer: research/taskiq/experiments/e07_prefetch_recycle.py
```

Upstream: [#129](https://github.com/taskiq-python/taskiq-redis/issues/129) (draft; reclaim до XREADGROUP, backpressure, abandoned buffer) — **не в 1.2.4**. Отдельный пакет `taskiq-redis-streams` (heartbeat liveness) — вне проверяемого release.

---

## 7. Shutdown / lifecycle

| Кейс | Поведение | Измерено |
| --- | --- | --- |
| idle SIGTERM | drain ~0.35 s, exit 0 | e08 idle |
| during handler | grace 5 s → SIGKILL group, pending остаётся | e08 during_handler |
| prefetched (batch not yet in PEL while slot busy) | pending=hold only; unfetched `>` доступны новому worker | e08 with_prefetched |
| early SIGTERM | быстрый выход | e08 early |
| repeated signals | hardkill_count=3 путь в worker | e08 double_signal |

Измерение через `Popen.poll()`/`wait()` + `killpg` (не `kill(pid,0)`).

Получение новых задач прекращается по `finish_event` в Receiver.listen. Prefetch state закрывается; late delivery может быть discarded. Shutdown hooks broker: `shutdown_timeout` (manager→worker). Manager: workers/reload; execution worker: ack/prefetch/async/shutdown/wait_tasks/hardkill/max_tasks_per_child.

---

## 8. Матрица возможностей

| Возможность | Core / пакет | Настройка | Ограничения | Подтверждение |
| --- | --- | --- | --- | --- |
| async handlers | core | `@broker.task` async | — | e01/e06 |
| sync handlers | core | sync + Thread/ProcessPool | timeout «don't work well» | e06 sleep_sync; receiver warning |
| DI / resources | taskiq-dependencies | `TaskiqDepends` | ошибка DI → on_error | исходники |
| middleware / hooks | core | `with_middlewares`, startup/shutdown | — | EventMiddleware стенда |
| timeout / cancel | core label `timeout` | `anyio.fail_after` | нет default; sync слабо | исходники |
| retry / backoff | SimpleRetry / SmartRetry | `retry_on_error`, `_retries` | не default; backoff в SmartRetry | e05 boom |
| delayed execution | labels / schedule | зависит от broker | Stream broker без native delay | документация / не проверено глубоко |
| scheduling | TaskiqScheduler + sources | CLI `taskiq scheduler` | отдельно от worker reclaim | исходники |
| send-dedup | нет штатного в stream | app-level task_id | — | стенд PR1 `native_dedup: false` |
| execution lock / keyed concurrency | нет в core stream | app-level | — | не проверено как фича |
| rate limiting | нет штатного | middleware/app | — | — |
| workflow | kiq chaining | app | нет canvas/workflow DSL | app code |
| result backend + TTL | taskiq-redis | `result_ex_time` | optional | e05 rb |
| tracing/metrics/errors | PrometheusMiddleware, OTEL ecosystem | optional packages | — | пакеты есть; deep не измерялись |
| admin | нет UI | XPENDING/XINFO | — | observe.py |
| readiness/liveness | нет штатного probe | app handshake | — | harness readiness |
| InMemoryBroker / tests | core | unit tests | не Redis semantics | исходники |
| Python | ≥3.10,<4 | — | 3.12 измерено | env |
| License | MIT (документация PyPI) | — | metadata License пустой в wheel | документация |

---

## 9. Отказоустойчивость Redis

| Сценарий | Producer | Worker | Messages | Артефакт |
| --- | --- | --- | --- | --- |
| краткий outage (SIGSTOP 2s) | enqueue после resume OK | жив | задача выполнена | e09 brief |
| data loss restart (SHUTDOWN NOSAVE) | — | hung→kill | stream/PEL пусты | e09 restart |
| maxmemory + noeviction | XADD → OOM после N | — | отказ записи | e09 maxmemory (12 XADD @1mb) |
| длительная недоступность / reconnect under load | | | | **не проверено** |
| outage during result save / retry | | | | **не проверено** |

Outage ≠ data loss — разделены в e09.

---

## 10. Performance (отдельно от correctness)

Событие percentile: `enqueue.sent_mono → handler_start.start_mono` (`lag_s`).

| Profile | n | wall_s | throughput/s | p50 lag | p95 lag | p99 lag |
| --- | --- | --- | --- | --- | --- | --- |
| idle_single | 1 | 2.42 (incl. 2s idle) | 0.41 | 1.1 ms | 1.1 ms | 1.1 ms |
| short_burst | 50 | 0.41 | 121 | 9.1 ms | 16.1 ms | 16.6 ms |
| sustained | 100 | 2.36 | 42.4 | 4.2 ms | 7.9 ms | 9.1 ms |
| mixed | 40 | 16.9 | 1.9 (fast starts 32) | 1.0 ms | 1.4 ms | 2.7 ms |

Артефакты: `results/taskiq-deep/performance/`. Не смешивать с short PR1 throughput.

---

## 11. Подтверждённые проблемы и цепочки issue→fix

| Проблема | Reproducer | Upstream | Release | Проверка |
| --- | --- | --- | --- | --- |
| Quiet-queue: XAUTOCLAIM недостижим | e02 | [#129](https://github.com/taskiq-python/taskiq-redis/issues/129) (open/draft) | нет в 1.2.4 | e02 fail reclaim |
| Live task reclaim → duplicate execution | e06 | by design idle PEL | — | e06 overlap=true |
| COUNT batch ≫ local concurrency | e01, e07 | [#84](https://github.com/taskiq-python/taskiq-redis/issues/84)→[#85](https://github.com/taskiq-python/taskiq-redis/pull/85) добавил count; backpressure в #129 | count в 1.2.x; backpressure нет | e01 pending=8 |
| Autoclaim lock default None | e04 | [#87](https://github.com/taskiq-python/taskiq-redis/issues/87), [#131](https://github.com/taskiq-python/taskiq-redis/issues/131); fix param [#107](https://github.com/taskiq-python/taskiq-redis/pull/107) | 1.2.2+ param; **default всё ещё None** | e04 + source |
| Cluster/Sentinel no reclaim | — | [#130](https://github.com/taskiq-python/taskiq-redis/issues/130) | open | исходники |
| Recycle leaves PEL stranded | e07 | следствие quiet-gate + COUNT | — | finished 1/12 |

Baseline → актуальна: **0.13.0 / 1.2.4 = latest stable на момент исследования**.

---

## 12. Рекомендуемые конфигурации

| Цель | Конфиг | Компромисс |
| --- | --- | --- |
| Минимум дублей на crash | `--ack-type when_executed` или `when_saved`; `unacknowledged_lock_timeout` задать явно; `xread_count` ≈ concurrency | at-least-once; нужен traffic или внешний reclaim для quiet recovery |
| Долгие задачи без duplicate | `idle_timeout` ≫ max task; **или** `--ack-type when_received` + идемпотентность; **или** app lease | when_received теряет crash-retry исходной доставки |
| Fairness / меньше stranded PEL | `xread_count` маленький (1..concurrency); `max_prefetch` низкий | больше Redis round-trips |
| Recycle workers | не полагаться на quiet reclaim; inject traffic / external XAUTOCLAIM; или ждать #129 | — |
| Lock safety | всегда `unacknowledged_lock_timeout>0` | default опасен |

Прикладной код обязателен для: идемпотентности при reclaim, quiet-queue recovery, poison policy, ownership extension, readiness/liveness.

Изменение библиотеки нужно для: reclaim на пустом чтении, listener backpressure = COUNT, heartbeat ownership, cluster parity.

---

## 13. Непроверенное

- Outage во время set_result / retry.kiq
- Длительная недоступность Redis + reconnect worker
- Sentinel/Cluster brokers end-to-end
- SmartRetryMiddleware backoff измерение
- Scheduler end-to-end
- OTEL/Prometheus deep

---

## 14. Итоговый вывод

Taskiq + `RedisStreamBroker` **корректно исполняет** долгие задачи, пока один consumer удерживает сообщение и `timeout` label / process живы: pending idle растёт, handler не отменяется сам по себе.

**Восстановление после смерти worker** работает только если живой listener получает непустой `XREADGROUP` (есть новый трафик) и `idle_timeout` истёк; на тихом потоке pending зависает. Default autoclaim lock без TTL — дополнительный риск.

**Shutdown** останавливает prefetch; активный handler при коротком grace уходит в SIGKILL, оставляя PEL. Prefetched-in-generator / COUNT-batch сообщения не «теряются» из Redis, но могут быть недоступны до reclaim/traffic.

**Ограничения, требующие прикладного кода или патча библиотеки:** quiet-queue reclaim, защита от duplicate execution живой задачи, poison policy, явный lock TTL, выравнивание `xread_count` с concurrency, идемпотентность handler.
