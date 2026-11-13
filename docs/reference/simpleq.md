# simple-q: что делает пустой воркер

Версия стенда: 1.0.0rc7, apalis и apalis-redis 1.1.1.

## Код, который запускает воркер

`agent-worker` строит бэкенд так:

```rust
let backend = RedisStorage::<Job>::new(shared.redis.clone())
    .with_config(queue_config(&queue, &shared.config)?);
```

Это `RedisStorage`, не `RedisStorageFactory`.
Файл: [crates/agent-worker/src/worker.rs](https://github.com/hewimetall/simple-redis-q/blob/main/crates/agent-worker/src/worker.rs), функция `run`.

`queue_config` задаёт `batch_size(1)`, `lock_tasks(true)`, `emit_events(false)`, `persist_results(true)` и `heartbeat_interval` из `settings.heartbeat_secs`.
Файл: [crates/simple-q-runtime/src/redis_api.rs](https://github.com/hewimetall/simple-redis-q/blob/main/crates/simple-q-runtime/src/redis_api.rs), функция `queue_config`.

Отдельный цикл `announce_worker` в том же `worker.rs` пишет метаданные `tasks_done` с тем же периодом `heartbeat_secs`. Это другой таймер, не выборка задачи.

## Код apalis, который решает, когда снова читать Redis

Локальная копия крейтов: apalis-core 1.1.1 и apalis-redis 1.1.1.
Документация фабрики: [RedisStorageFactory](https://docs.rs/apalis-redis/1.1.1/apalis_redis/struct.RedisStorageFactory.html).
Обзор библиотеки: [apalis.dev](https://apalis.dev/).

| Место | Поведение |
| --- | --- |
| `Persisted::poll_ready` | в `State::Ready` опрашивает `Delay` периода heartbeat. Если таймер не сработал, регистрирует waker и возвращает `Ready` |
| `Persisted::poll_next` | пустой `fetch_next` переводит состояние в `Ready`, зовёт `register_waker` и возвращает `Pending` |
| `CallAll::poll_next` | на каждом проходе сначала `poll_ready`, потом `poll_next` |
| `RedisStorageFactory::new` | делает `PSUBSCRIBE tasks:*:available` и будит воркер из push-callback |
| `batch_push.lua` | `PUBLISH tasks:{queue}:available` только при `emit_events`. `LPUSH` в список сигнала выполняется всегда, если что-то положили в активный список |
| `fetch_next.lua` | читает активный список через `LRANGE`. Если пачка короче лимита, `DEL` списка сигнала. `BLPOP` в скрипте нет |

simple-q фабрику не вызывает и `emit_events` выключает. `PSUBSCRIBE` в бинарник apalis входит, в этот путь запуска нет.

## Трейс

`results/traces/simpleq.json`, heartbeat 5 с.

| Наблюдение | Значение |
| --- | --- |
| `pickup_ms` | 2484 |
| `PSUBSCRIBE`, `SUBSCRIBE`, `BLPOP`, `BRPOP` | в `MONITOR` нет |
| После send | `EVALSHA` постановки, внутри Lua `hsetnx` / `zadd` в `bench:scheduled`, затем `LPUSH` сигнала |
| Ребёнок, потоки | `tokio-rt-worker`, `python-asyncio` (`ep_poll`), `futures-timer` (`futex_wait_queue`) |
| py-spy ребёнка | `EpollSelector.select` на потоке встроенного цикла. Кадра выборки очереди в Python нет |

Супервизор в этом окне: 0 переключений контекста за 0.4 с, потоки в `futex_wait_queue` и один `ep_poll`.
