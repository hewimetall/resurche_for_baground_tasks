# Long-job semantics (2400 seconds)

Reference for one job that runs for 2400 seconds. Versions are the packages installed in this workspace. Every statement below is from that source, or from constructing `Config()` on the installed `simple_q._runtime` extension. Nothing here is taken only from documentation, so nothing is marked `read_in_docs`. No wall-clock measurement is included. Arithmetic uses only constants in the source (`1800000 / 1000`, `30 * 60 + 60`, `max_timeout + 10`).

| Package | Version | Import location |
| --- | --- | --- |
| simple-q-runtime and simple-q-worker | 1.0.0rc7 | `/workspace/.venv` (`simple_q`, `_runtime`, `/workspace/.venv/bin/agent-worker.bin`) |
| dramatiq | 2.2.1 | `/workspace/.venv` |
| celery | 5.7.0 | `/workspace/.venv` |
| kombu | 5.7.0 | `/workspace/.venv` |
| billiard (Celery's prefork pool) | installed with celery | `/workspace/.venv` |
| arq | 0.28.0 | `/workspace/.venv-arq` |
| taskiq | 0.13.0 | `/workspace/.venv` |
| taskiq-redis | 1.2.4 | `/workspace/.venv` |

The simple-q wheel does not ship `.rs` files. The Rust and Lua line numbers are from the tree whose workspace version is `1.0.0-rc7` (`/tmp/simple-redis-q/Cargo.toml`) and from the apalis crates that tree links (`apalis-redis` 1.1.1 with feature `task-tracking`, `/tmp/simple-redis-q/crates/agent-worker/Cargo.toml`). `Config()` on the installed extension returns `heartbeat_secs=15`, `missed_heartbeats=4`, `ping_budget_secs=1860`, `max_deliveries=5`. The installed `agent-worker.bin` contains the strings `Python loop is unresponsive; leaving inflight messages for redelivery` and `Heartbeat failed: worker not registered`.

2400 seconds is longer than the simple-q default execution limit (1800 s) and its default ping budget (1860 s), longer than the dramatiq default time limit (600 s), longer than the arq default job timeout (300 s) and in-progress TTL (310 s), and longer than the taskiq-redis stream idle time (600 s). It is shorter than the kombu Redis default visibility timeout (3600 s). Celery's execution limits default to disabled.

---

## simple-q 1.0.0rc7

### Execution timeout

There is one execution limit, `time_limit`, in milliseconds. The actor default is `DEFAULT_TIME_LIMIT_MS = 1_800_000` (`/workspace/.venv/lib/python3.12/site-packages/simple_q/dispatch.py` lines 54 and 93). The same constant is `30 * 60 * 1000` in `/tmp/simple-redis-q/crates/simple-q-runtime/src/names.rs` line 16.

`dispatch.py` divides that value by 1000 and passes seconds to `asyncio.wait_for` (lines 336–337). A 2400-second await exceeds 1800 seconds, so the default limit fires before the await finishes.

CPython 3.12 `asyncio.wait_for` cancels the task and raises `TimeoutError`. If the task swallows the cancellation and returns a value, `wait_for` returns that value (`/usr/lib/python3.12/asyncio/tasks.py` lines 472–484). `TimeoutError` is an `Exception`. `CancelledError` is a `BaseException`. The handler can catch `CancelledError`. `dispatch.py` catches only `(TimeoutError, asyncio.TimeoutError)` and then `Exception` (lines 338–367), so a `CancelledError` that escapes the handler is not that timeout branch.

The timeout is applied on the `python-asyncio` thread (`/tmp/simple-redis-q/crates/agent-worker/src/bridge.rs` lines 27–32), which runs the loop with `run_forever`. A blocking call on that thread is not interrupted until the thread runs the loop again. `simple_q/tracking.py` lines 6–8 say cancelling the named task does not stop native code that holds the interpreter.

When the timeout is caught and `current.retries >= current.max_retries`, the outcome is `dead` with reason `timeout` (lines 340–351). The actor default `max_retries` is 0 (line 92), so the first timeout is dead. Otherwise the outcome is `retry` (line 352).

A soft execution timeout is unsupported. `memory_soft_ratio` and `memory_hard_ratio` on `Config` are memory ratios, not time limits.

### ACK relative to handler return

Fetch and the lease happen before the handler. `TaskPersistService` sends `TaskEvent::Lock` and then awaits the handler future (`/tmp/apalis/apalis-core-1.1.1/src/backend/persistence/service.rs` lines 131–139). After that future completes, and only when `should_ack` is true, it sends `TaskEvent::Complete` (lines 140–154). `should_ack` is `persist_results` (line 84). simple-q sets `lock_tasks(true)` and `persist_results(true)` (`/tmp/simple-redis-q/crates/simple-q-runtime/src/redis_api.rs` lines 30–34).

`handle_results.lua` then `SREM`s the task from the worker inflight set. Status `Done` goes to the done set. Status `Failed` or `Pending` is rescheduled only when `attempt < max_attempts`. Every other status, including `Killed`, goes to the dead set (`/tmp/apalis/apalis-redis-1.1.1/lua/handle_results.lua` lines 78–110). If `max_attempts` is missing, the script falls back to 25 (lines 85–88). simple-q enqueue sets `max_attempts(1)` (`redis_api.rs` lines 181 and 212), so a `Failed` result is not rescheduled by that script.

Lifecycle maps `Ok` to `Done`, `AbortError` to `Killed`, and other errors to `Failed` (`/tmp/apalis/apalis-core-1.1.1/src/worker/lifecycle.rs` lines 202–211).

`handle_job` returns only after Python returns (`/tmp/simple-redis-q/crates/agent-worker/src/worker.rs` lines 381–454):

- outcome `retry`: enqueue a new job with `retries + 1`, delete `delivery:{message_id}:{retries}`, return `Ok` (lines 404–431). That `Ok` becomes `Done` for the current task. The new job is a separate task.
- outcome `dead`: return `AbortError` (lines 433–447). That becomes `Killed`, and the Lua script writes the dead set. The delivery key is not deleted on this path.
- any other outcome, including `ok`: delete the delivery key and return `Ok` (lines 449–452).

The comment at `worker.rs` lines 498–500 states that the handler returns before Apalis writes `done` or `dead`. The idempotency key is deleted only after that write is visible, in a spawned loop of up to 50 sleeps of 20 ms (lines 488–533). A crash before that delete, including `SIGKILL`, leaves the key until its TTL (`worker.rs` lines 8–12).

### Heartbeat, visibility, idle

There is no per-message visibility timeout. Lease renewal is automatic. A handler call that extends an inflight lease is unsupported.

Worker heartbeat interval is `HEARTBEAT_INTERVAL_SECS = 15` and `MISSED_HEARTBEATS = 4` (`names.rs` lines 14–15). `Config::defaults` copies those fields (`/tmp/simple-redis-q/crates/simple-q-runtime/src/config.rs` lines 68–70), and `queue_config` passes them to apalis (`redis_api.rs` lines 31–32). The orphan window is `15 * 4 = 60` seconds (`names.rs` lines 90–91).

`keep_alive.lua` `ZADD`s the worker into `{queue}:workers` (`/tmp/apalis/apalis-redis-1.1.1/lua/keep_alive.lua` lines 8–16). `renew_leases.lua` `HSET`s `locked_at` (`renew_leases.lua` line 44). `reenqueue_orphaned.lua` `RPUSH`es inflight task ids of workers whose score is at or below the cutoff and deletes that inflight set (lines 27–38). It does not change `retries`.

Apalis arms that heartbeat from `poll_ready` (`/tmp/apalis/apalis-core-1.1.1/src/backend/persistence/mod.rs` lines 263–278). `CallAll` polls in-flight futures and then `poll_ready` (`/tmp/apalis/apalis-core-1.1.1/src/worker/call_all.rs` lines 179–180). The heartbeat therefore runs on the Tokio runtime while a handler future is pending, including during `asyncio.sleep`.

The child pings the Python loop every `PING_EVERY` of 2 seconds with `PING_TIMEOUT` of 3 seconds (`/tmp/simple-redis-q/crates/agent-worker/src/child.rs` lines 27–28 and 165–171). The ping is `call_soon_threadsafe` onto `python-asyncio`. Default `ping_budget_secs` is `DEFAULT_TIME_LIMIT_MS / 1000 + PING_MARGIN_SECS` with `PING_MARGIN_SECS = 60` (`names.rs` lines 16–19, `config.rs` line 70), which is 1860 seconds. The installed `Config()` returns 1860.

`lock.rs` lines 51–58 and 133: a missed ping does not stop lease renewal. Renewal stops when the ping budget has elapsed. `child.rs` lines 170–178 then pause the pool, request cancel, and return code 2. The caller logs `Python loop is unresponsive; leaving inflight messages for redelivery` and `std::process::exit(2)` without flushing (lines 113–120). Those inflight ids stay for orphan requeue.

A blocking `time.sleep` on `python-asyncio` fails those pings. Tokio still renews the lease until the 1860-second budget elapses. An `asyncio.sleep` lets the pings succeed, so the budget does not start. The default 1800-second `wait_for` fires first. 2400 seconds is past both 1800 and 1860.

### Retry versus redelivery

These are different counters.

`retries` is the job field. The actor default `max_retries` is 0 (`dispatch.py` line 92). Timeout and `Exception` compare `current.retries >= current.max_retries` (lines 340 and 355). A retry outcome increments `retries` by one and enqueues a new task (`worker.rs` lines 409–416). Backoff uses the count from before that increment (`dispatch.py` lines 387–393).

`deliveries` is `INCR` on `delivery:{message_id}:{retries}` at the start of `handle_job` (`worker.rs` lines 332–345, `redis_api.rs` lines 310–327). The comment says orphan requeue does not call `incr_delivery`. Poison is `deliveries > max_deliveries` (`job.rs` lines 104–112). The default `max_deliveries` is 5 (`dispatch.py` line 57, `names.rs` line 23). The attempt whose count equals the limit still runs. The next delivery of the same `(message_id, retries)` is buried before the handler (`dispatch.py` lines 305–318, `worker.rs` lines 346–371) and returns `AbortError`.

Apalis `max_attempts` is 1, so the Lua retry branch (`attempt < max_attempts`) is not the simple-q retry path. simple-q retry returns `Ok`, which is `Done` for the current task, and the next attempt is a newly enqueued job.

### Unsupported

A soft execution timeout is unsupported. A per-message visibility timeout is unsupported. A handler API that extends the inflight ack is unsupported. The worker renews `locked_at` itself on its heartbeat. Process-kill of the handler at the execution limit is unsupported: the limit is `asyncio.wait_for` on the Python loop. `except Exception` in `dispatch.py` does not catch `CancelledError`.

---

## dramatiq 2.2.1

### Execution timeout

There is one time limit, in milliseconds. `TimeLimit` defaults to `600000` (10 minutes) and checks every `1000` ms (`/workspace/.venv/lib/python3.12/site-packages/dramatiq/middleware/time_limit.py` lines 56–67). `TimeLimit` is in `default_middleware` (`dramatiq/middleware/__init__.py` lines 56–63). The per-message or per-actor `time_limit` overrides it; otherwise the middleware value is used (`time_limit.py` lines 89–92).

A 2400-second handler exceeds 600000 ms, so the default limit fires.

`TimeLimitExceeded` subclasses `Interrupt`, and `Interrupt` subclasses `BaseException` (`dramatiq/threading.py` lines 52–59). The checker thread calls `PyThreadState_SetAsyncExc` on the worker thread. The exception is delivered the next time that thread runs Python bytecode. It cannot cancel a system call (`time_limit.py` lines 50–54, `threading.py` lines 67–71). A sync handler can catch `TimeLimitExceeded` or `Interrupt`. `except Exception` does not.

`process_message` catches `BaseException` and emits `after_process_message` with that exception (`dramatiq/worker.py` lines 581–600). `Retries.after_process_message` then treats it as a failure (`dramatiq/middleware/retries.py` lines 97–148).

A soft time limit is unsupported. A second hard limit that `SIGKILL`s the process is unsupported.

Async actors are wrapped with `async_to_sync` (`dramatiq/actor.py` line 78). `AsyncIO` is not in `default_middleware`. `EventLoopThread.run_coroutine` polls `future.result` every `interrupt_check_ival` (0.1 s), catches `Interrupt`, cancels the future, and waits up to 1 second (`dramatiq/asyncio.py` lines 144–179). The coroutine observes `CancelledError`. The worker thread still sees `TimeLimitExceeded`, because the exception is injected into the worker thread identity recorded in `before_process_message` (`time_limit.py` line 92), not into the event-loop thread. A `time.sleep` on the event-loop thread does not run the cancel callback until that sleep returns. If the wrapped coroutine is still not done after 1 second, `run_coroutine` raises `RuntimeError`.

### ACK relative to handler return

`process_message` runs the actor, then its `finally` calls `post_process_message` (`worker.py` lines 602–607). That is after the handler returns or raises. If `message.failed` is set, the consumer nacks. Otherwise it acks (lines 409–419).

Redis fetch is `LPOP` plus `SADD` onto `__acks__.{worker}.{queue}` (`dramatiq/brokers/redis/dispatch.lua` lines 154–167). Ack is `SREM` of that set and `HDEL` of the message hash (lines 191–196). Nack moves the hash entry onto the dead-letter sorted set (lines 200–211). `Retries` calls `message.fail()` when retries are exhausted (`retries.py` lines 103–124), and that failed flag selects nack.

`AgeLimit` default `max_age` is `None` (`dramatiq/middleware/age_limit.py` line 37). With no max age, `before_process_message` returns without failing the message (lines 45–49).

### Heartbeat, visibility, idle

The Redis broker heartbeat timeout is `DEFAULT_HEARTBEAT_TIMEOUT = 60000` milliseconds (`dramatiq/brokers/redis.py` lines 47–49). Every `dispatch.lua` command `ZADD`s `__heartbeats__` with the worker id before running the command (`dispatch.lua` lines 58). `ack` and `nack` are on `MAINTENANCE_COMMAND_BLACKLIST` (`redis.py` line 37), so they refresh the score but do not run maintenance.

Maintenance runs on about `DEFAULT_MAINTENANCE_CHANCE` of 1000 out of `MAINTENANCE_SCALE` of 1_000_000 commands (`redis.py` lines 36–41). It `ZRANGEBYSCORE`s workers older than `heartbeat_timeout` and `RPUSH`es their unacked ids (`dispatch.lua` lines 110–124). That requeue does not change `options["retries"]`.

The consumer thread is separate from worker threads. Default `worker_threads` is 8 (`worker.py` line 85). Default queue prefetch is `worker_threads * 2`, so 16, unless `dramatiq_queue_prefetch` is set (`worker.py` lines 50 and 95). `worker_timeout` defaults to 1000 ms (line 58).

`RedisConsumer.__next__` calls `do_fetch` only while `outstanding_message_count < prefetch` (`redis.py` lines 356–364). An empty fetch still enters `dispatch.lua`, so it refreshes the heartbeat. A blocked handler and an `asyncio.sleep` both leave the consumer thread free. With the default prefetch of 16, one in-flight message does not stop that refresh. If outstanding messages fill the prefetch, that consumer does not call `do_fetch` and does not `ZADD` until some message is acked or another dispatch runs.

A per-message visibility timeout is unsupported. An API that extends one message's ack lease is unsupported. The score is the worker heartbeat, rewritten by later broker commands.

### Retry versus redelivery

These are different paths.

`Retries` is default middleware. `max_retries` defaults to 20, `min_backoff` to 15000 ms, `max_backoff` to 7 days (`retries.py` lines 72–83). On an exception it stores `options["retries"]` starting at 0, then adds 1 (lines 108–110). If `retries` before that increment is still below `max_retries`, it `broker.enqueue`s the message (line 148). `RedisBroker.enqueue` copies the message and assigns a new `redis_message_id` (`redis.py` lines 163–170). The original message is then acked, unless `message.fail()` was set.

Maintenance requeue and shutdown `requeue` (`dispatch.lua` lines 176–187) push the same Redis id back. They do not increment `options["retries"]`.

### Unsupported

A soft time limit is unsupported. A process-killing hard timeout is unsupported. Per-message ack extension is unsupported. A separate delivery counter is unsupported. `AsyncIO` is not default middleware, so an async actor does not get that middleware unless the application adds it. `TimeLimit` does not interrupt a system call that has not returned to the interpreter.

---

## celery 5.7.0 and kombu 5.7.0

Celery's default pool is `prefork` (`/workspace/.venv/lib/python3.12/site-packages/celery/app/defaults.py` lines 11 and 360). The child processes are billiard. kombu's Redis transport is the visibility mechanism. kombu itself has no task execution timeout.

### Execution timeout

`task_soft_time_limit` and `task_time_limit` are `Option(type='float')` with no numeric default (`defaults.py` lines 321–326), so the default is `None`. The task attributes `time_limit` and `soft_time_limit` also default to `None` (`celery/app/task.py` lines 133–134 and 304–308) and are copied from that config when still `None` (lines 435–445 and 479–481). `None` means the limit is disabled (`billiard/pool.py` `TimeoutHandler._timed_out`, lines 789–791: a missing timeout returns false). With the defaults, a 2400-second job is not killed by Celery.

The unit is seconds. The clock starts at the child's ACK time, which is before `fun()` (`billiard/pool.py` `workloop` lines 361–369, `handle_timeouts` lines 807–814).

Soft limit: the parent `TimeoutHandler` sends `SIGUSR1` (`pool.py` `on_soft_timeout`, lines 724–734). The child handler raises `SoftTimeLimitExceeded` (lines 228–229). That class subclasses `Exception` (`billiard/exceptions.py` lines 29–31). The handler can catch it, including with `except Exception`. If the handler catches it and returns, the job is a normal success. The parent soft path only invokes the timeout callback. It does not set a failure result.

Hard limit: the parent raises `TimeLimitExceeded` in the parent and stores that as the job result (`pool.py` lines 739–755), then `SIGTERM`s the child and, if it is still alive after `wait(timeout=0.1)`, `SIGKILL`s it or its process group when the child is the group leader (lines 760–779). `TimeLimitExceeded` subclasses `Exception` (`billiard/exceptions.py` lines 22–23), but it is raised in the parent. The child cannot catch `SIGKILL`.

`TimeoutHandler.body` sleeps 1.0 second between scans (`billiard/pool.py` lines 821–825). `handle_timeouts` starts at line 783.

### ACK relative to handler return

Default `task_acks_late` is `False` (`defaults.py` line 271). The task class copies that onto `acks_late` (`task.py` lines 326–335 and 442).

Prefork accept runs before the task body. `apply_target` calls `accept_callback` and then `target` (`celery/concurrency/base.py` lines 27–30). `Request.on_accepted` broker-acks when `acks_late` is false (`celery/worker/request.py` lines 525–532). The billiard child sends its pool ACK, waits for the syn, and only then calls `fun` (`billiard/pool.py` lines 361–369). So the default broker ACK is before the handler body.

`Request.execute`, the non-pool path, does the same: ack first when `acks_late` is false, and ack again after `trace_task` returns (lines 391–412).

With `acks_late` true:

- `on_success` acks after the task returns (`request.py` lines 616–629).
- `on_retry` acks (`request.py` lines 633–636).
- `on_failure` acks when the matching flag is true, otherwise rejects (`request.py` lines 749–768).
- hard timeout uses `acks_on_timeout`: ack, or `reject(requeue=True)` (`request.py` lines 610–614).

`task_acks_on_failure_or_timeout` defaults to `True` (`defaults.py` lines 272–276). `acks_on_failure` and `acks_on_timeout` default to `None` (lines 277–278). When those are `None`, the task copies the combined flag, and if that is also `None` it uses `True` (`task.py` lines 483–494). Default late-ack failure and timeout therefore ack.

kombu Redis ack is `ZREM` of the unacked index and `HDEL` of the payload (`kombu/transport/redis.py` lines 499–501).

### Heartbeat, visibility, idle

On the Redis transport, `visibility_timeout` is `3600` seconds (`kombu/transport/redis.py` line 1070). `QoS.append` writes the score once, as `time()` at append (`redis.py` lines 476–491). No later write updates that score. `restore_visible` requeues tags whose score is older than `visibility_timeout` (lines 523–547).

The async restore timer uses `unacked_restore_interval = 10` seconds and `unacked_restore_throttle = 10` (lines 1063–1069 and 2216–2220). The module comment says a real scan is about `interval * throttle` seconds, so about 100 seconds (lines 40–47). 2400 is less than 3600, so a message that stays unacked for 2400 seconds is still inside the default visibility window. Extending the score during the handler is unsupported. Neither a blocked child nor `asyncio.sleep` in the child updates the score. The parent process is the one that runs `restore_visible`.

Default early ack removes the message from the unacked index before the body, so visibility does not apply to that message after accept.

Redis transport `implements` does not enable heartbeats (`redis.py` lines 2141–2145). The virtual transport default is `heartbeats=False` (`kombu/transport/virtual/base.py` lines 940–944). Celery `broker_heartbeat` defaults to 120 (`celery/app/defaults.py` line 97). That is the broker-heartbeat setting. The Redis transport does not implement heartbeats, and it does not refresh the visibility score.

### Retry versus redelivery

These are different paths.

`Task.retry` defaults `max_retries` to 3 (`task.py` line 265) and `default_retry_delay` to `3 * 60` seconds (line 269). It sets `retries = request.retries + 1` and republishes (lines 879 and 934). With `acks_late`, `on_retry` acks the current message (`request.py` lines 633–636). With the default early ack, the current message was already acked at accept.

Visibility restore requeues the same stored payload (`redis.py` `restore_by_tag` from `restore_visible`). It does not increment `request.retries`.

`task_retry_on_worker_lost` defaults to `False` (`defaults.py` line 313).

### Unsupported

With the default config, both the soft and the hard execution limits are unset, so neither fires. Ack extension of the kombu visibility score is unsupported. A Redis visibility heartbeat is unsupported. The Redis transport does not implement kombu heartbeats. `SIGKILL` on the hard path is not catchable in the child.

---

## arq 0.28.0

Installed at `/workspace/.venv-arq/lib/python3.12/site-packages/arq`.

### Execution timeout

`job_timeout` defaults to 300 seconds (`arq/worker.py` lines 204 and 248). The function's own `timeout_s` replaces it when set (line 574). The unit is seconds.

There is one `asyncio.wait_for` around the job task (lines 595–599). A soft timeout is unsupported. A process-killing hard timeout is unsupported.

`wait_for` cancels the task and raises `TimeoutError`. The handler can catch `CancelledError`. If it swallows the cancellation and returns, `wait_for` returns that value (`/usr/lib/python3.12/asyncio/tasks.py` lines 477–484). If `TimeoutError` propagates, it is an `Exception`, so `run_job` takes the failure branch: `finish = True` and `jobs_failed` increments (`worker.py` lines 610–634). That path is not the retry path. The retry path is `Retry`, or `CancelledError` / `RetryJob` when `retry_jobs` is true (lines 613–627).

The worker is one asyncio loop. A blocking `time.sleep` in the job blocks that loop, so this process's `wait_for` and its poll cannot run until the sleep returns. `asyncio.sleep` yields, so `wait_for` fires at `timeout_s`. The default 300 seconds is less than 2400, so an async job hits the timeout.

### ACK relative to handler return

Starting a job `PSETEX`s `arq:in-progress:{job_id}` once, for `in_progress_timeout_s * 1000` milliseconds (`worker.py` lines 450–465). That key is not refreshed during the run.

`in_progress_timeout_s` is `max(function timeout or job_timeout) + 10` (lines 276–277). With the defaults that is 310 seconds.

`finish_job` runs after the attempt returns (`run_job` lines 667–676). When `finish` is true it `ZREM`s the job id from the queue and deletes the in-progress key unless a cron job asked to keep it (lines 692–706). Success sets `finish = True` (line 639). A `TimeoutError` also sets `finish = True`. A retry leaves `finish` false, so the queue member stays and the in-progress key is deleted (the default `keep_in_progress` is `None`, lines 540–543 and 695–696).

So the queue removal is after the handler returns, and only for a finished attempt. The in-progress key is a separate lease, written before the handler and not extended.

### Heartbeat, visibility, idle

The in-progress key is the job lease. It is written once at start. Nobody rewrites it while the handler is blocked or while it `asyncio.sleep`s. After 310 seconds Redis expires it. Another worker's `start_jobs` treats a missing key as available (lines 450–465). That expiry is Redis TTL. It does not depend on this process's event loop. A blocked loop still loses the key at the TTL.

`heart_beat` runs from the worker poll and calls `record_health` (`worker.py` lines 729–734). `record_health` writes the health key only when `health_check_interval` has elapsed (lines 773–785). That interval defaults to 3600 seconds (lines 210 and 255). The health key is worker liveness. It is not the job lease, and it is not refreshed on every poll. A blocked loop does not reach `heart_beat`. An `asyncio.sleep` in the job lets the loop poll.

There is no per-message visibility score and no ack-extend API. Both are unsupported.

### Retry versus redelivery

These share one counter.

`run_job` `INCR`s `arq:retry:{job_id}` at the start of every run (lines 481–483) and expires it at 88400 seconds. `max_tries` defaults to 5 (lines 209 and 254). If `job_try > max_tries`, the job is finished as a failure without calling the function (lines 549–569).

`retry_jobs` defaults to true (line 213). `Retry` and `RetryJob`, and `CancelledError` while `retry_jobs` is true, leave the queue member in place (lines 613–627, `finish` stays false). The next run `INCR`s the same key. That includes a run that starts because the in-progress key expired. There is no separate delivery counter.

A timeout is not this retry path. `TimeoutError` finishes the job and deletes the retry key (`finish_job` lines 700–704).

### Unsupported

A soft timeout is unsupported. A hard process kill at the job timeout is unsupported. Refreshing or extending `arq:in-progress:{job_id}` during the handler is unsupported. A visibility timeout distinct from that key is unsupported. A delivery counter distinct from `arq:retry:{job_id}` is unsupported.

---

## taskiq 0.13.0 and taskiq-redis 1.2.4

### Execution timeout

No execution timeout is installed by default. `run_task` reads `message.labels["timeout"]` and, only when that label is present, wraps the await in `anyio.fail_after` (`/workspace/.venv/lib/python3.12/site-packages/taskiq/receiver/receiver.py` lines 351–364). `fail_after` takes seconds (`anyio/_core/_tasks.py` lines 159–179). At the deadline it cancels the scope and, if that cancellation was caught by the scope and the deadline has passed, raises `TimeoutError` (lines 147–155).

`run_task` catches `BaseException` (receiver.py lines 374–376), so the receiver stores the `TimeoutError` as the task error. An async handler can observe the cancellation inside the scope. A sync task is run in an executor (lines 335–350). The source logs `Timeouts for sync tasks don't work in python well.` (lines 353–354) because cancelling the awaiting coroutine does not stop that thread.

A label-less 2400-second job is not cancelled by taskiq.

A soft timeout is unsupported. A hard timeout that kills the process is unsupported.

### ACK relative to handler return

The CLI and `Receiver` default is `AcknowledgeType.WHEN_SAVED` (`taskiq/acks.py` lines 11–26, `taskiq/cli/worker/args.py` lines 53 and 246–250, `taskiq/receiver/receiver.py` line 110).

`callback` (`receiver.py` lines 204–234):

- `WHEN_RECEIVED`: ack before `run_task`.
- `WHEN_EXECUTED`: ack after `run_task` returns and before the result is saved.
- `WHEN_SAVED`: ack after `result_backend.set_result`. If `set_result` raises, that ack is skipped.
- `MANUAL`: the task acks through `Context.ack`. The callback does not ack.

`RedisStreamBroker._ack_generator` is `XACK` of the stream id (`taskiq_redis/redis_broker.py` lines 260–269). `XREADGROUP` uses `noack=False` (lines 276–286), so a new message enters the pending entries list when it is read, before the handler.

`ListQueueBroker.listen` is `BRPOP` (`redis_broker.py` lines 119–135). `BRPOP` removes the value when it is returned. There is no later ack. A crash after the pop drops the message. That broker's listen path has no ack callback.

### Heartbeat, visibility, idle

`RedisStreamBroker` `idle_timeout` defaults to `600000` milliseconds (`redis_broker.py` line 163). That value is `min_idle_time` for `XAUTOCLAIM` (lines 304–308). Nothing in the handler, and nothing in the listen loop, rewrites a pending message's idle time. An ack-extend API is unsupported.

`XAUTOCLAIM` runs only after `XREADGROUP` returns at least one message. An empty read `continue`s (lines 287–288) and skips reclaim. A quiet queue does not claim a pending message on that iteration. 2400 seconds is greater than 600 seconds, so a pending entry is old enough for `min_idle_time` once a later non-empty read reaches `XAUTOCLAIM`.

`ListQueueBroker` has no idle timeout and no reclaim.

`RedisStreamClusterBroker.listen` and the sentinel stream listen call `XREADGROUP` and on an empty result `continue` (`taskiq_redis/redis_cluster_broker.py` lines 187–201, `taskiq_redis/redis_sentinel_broker.py` around lines 250–257). They do not call `XAUTOCLAIM`. Idle reclaim on those brokers is unsupported.

A sync task runs in the executor, so it does not block the listen loop. An async task's `asyncio.sleep` also yields the loop. In both cases the pending-entry idle clock is Redis's clock. Nobody writes a heartbeat during either.

`unacknowledged_lock_timeout` defaults to `None` on `RedisStreamBroker`, so the autoclaim lock timeout is unlimited when left at that default (`redis_broker.py` constructor; the `pipe.lock` call uses that attribute at line 301).

### Retry versus redelivery

These are different paths, and retry is not on by default.

`Broker.middlewares` starts empty (`taskiq/abc/broker.py` line 100). `SimpleRetryMiddleware` is not installed unless the application adds it. Its `default_retry_label` is `False` and `default_retry_count` is 3 (`taskiq/middlewares/simple_retry_middleware.py` lines 19–25). With the label off, `on_error` returns without requeueing (lines 85–89). When the label is on, it `kiq`s a new message and sets the label `_retries` to the previous value plus one (lines 97–107). The original message is still acked according to `ack_type` after `run_task` returns.

`XAUTOCLAIM` redelivers the same stream id. It does not increment `_retries`. With `WHEN_SAVED` or `WHEN_EXECUTED`, a finished `run_task` acks, so the id leaves the pending list. A message that was not acked stays pending and can be claimed later as the same id, with the same labels.

`ListQueueBroker` has no redelivery path. The pop already removed the value.

### Unsupported

A default execution timeout is unsupported. A soft timeout is unsupported. A hard process kill is unsupported. Ack extension of a Redis stream pending entry is unsupported. Idle reclaim is unsupported on `ListQueueBroker`, `RedisStreamClusterBroker`, and the sentinel stream listen. `SimpleRetryMiddleware` is not part of the default broker. `ListQueueBroker` has no ack after the handler.
