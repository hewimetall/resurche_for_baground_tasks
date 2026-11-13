"""Long-run scenario matrix.

Each item of SCENARIOS is one row of data. This module does not start workers,
brokers, or faults.
"""

from __future__ import annotations

LIBS = ["simpleq", "dramatiq", "celery", "arq", "taskiq"]

# Broker lease used by ownership rows. It is not time_limit_s.
REDELIVERY_WINDOW_S = 600

_KEYS = (
    "scenario_id",
    "group",
    "libraries",
    "planned_s",
    "steps",
    "step_s",
    "kind",
    "time_limit_s",
    "soft_limit_s",
    "max_retries",
    "workers",
    "fault",
    "counts_as_2400",
    "note",
)

_KINDS = frozenset({"sleep", "block", "cpu", "http_hang"})

_TIMEOUT_PLAN = (
    "Planned workload stays 2400s (240 steps of 10s). "
    "time_limit_s is 600. Do not rewrite planned_s as 600. "
    "counts_as_2400 is false: a limit firing is not a finished 2400s run. "
    "simple-q and Dramatiq take the limit in milliseconds (600000); "
    "Celery, arq and taskiq take seconds."
)

_SINGLE_LIMIT = (
    "simple-q has one actor time_limit and no soft limit. "
    "The short limit run reached on_dead with reason timeout "
    "(deliveries=1, retries=1 when max_retries allowed a retry). "
    "Dramatiq has one time_limit. TimeLimit middleware raises TimeLimitExceeded "
    "in the worker thread the next time that thread acquires the GIL, and its "
    "own note says this cannot cancel a system call. There is no soft/hard pair. "
    "The short limit run recorded TimeLimitExceeded and status failed. "
    "arq has one function timeout (asyncio wait on the worker loop) and the "
    "in-progress TTL is that same job_timeout, so a soft warning cannot be "
    "split from the abort. The short limit run left the job running and wrote "
    "no failure hook. "
    "taskiq has one timeout. The short limit run raised TimeoutError and the "
    "adapter hook marked the job failed. No soft/hard pair."
)


def _row(
    scenario_id: str,
    group: str,
    *,
    kind: str = "sleep",
    planned_s: int = 2400,
    steps: int = 240,
    step_s: int = 10,
    time_limit_s: int = 3000,
    soft_limit_s: int | None = None,
    max_retries: int = 0,
    workers: int = 1,
    fault: str | None = None,
    counts_as_2400: bool = False,
    note: str,
) -> dict:
    return {
        "scenario_id": scenario_id,
        "group": group,
        "libraries": list(LIBS),
        "planned_s": planned_s,
        "steps": steps,
        "step_s": step_s,
        "kind": kind,
        "time_limit_s": time_limit_s,
        "soft_limit_s": soft_limit_s,
        "max_retries": max_retries,
        "workers": workers,
        "fault": fault,
        "counts_as_2400": counts_as_2400,
        "note": note,
    }


SCENARIOS: list[dict] = [
    _row(
        "A0_wire",
        "wire",
        planned_s=3,
        steps=3,
        step_s=1,
        time_limit_s=30,
        note=(
            "short wiring only. Three steps of 1s on all five libraries. "
            "Confirms import, enqueue, and one ack. Not a 2400s sample."
        ),
    ),
    _row(
        "A1_control_40",
        "control",
        time_limit_s=3000,
        counts_as_2400=True,
        note=(
            "Control sample. One worker, kind sleep (asyncio.sleep), 240 steps "
            "of 10s, max_retries 0, no fault. time_limit_s 3000 is above the "
            "plan, so the limit must not fire. A finished single run counts as "
            "a 2400s sample. simple-q and Dramatiq express 3000s as 3000000 ms. "
            "Celery, arq and taskiq take seconds. Celery has no native async "
            "worker: the adapter runs the coroutine with run_until_complete "
            "inside a prefork child."
        ),
    ),
    _row(
        "timeout_without_retry",
        "timeout",
        time_limit_s=600,
        max_retries=0,
        note=(
            _TIMEOUT_PLAN
            + " Retry axis, no retry. The limit fires and the message is not "
            "scheduled again. arq counts the first attempt inside max_tries, "
            "so this row is max_tries 1. taskiq retry_on_error stays off. "
            "Celery hard time_limit kills the prefork child with SIGKILL, which "
            "is not an exception retry. simple-q on_dead is the final timeout "
            "when max_retries is 0."
        ),
    ),
    _row(
        "timeout_with_retry",
        "timeout",
        time_limit_s=600,
        max_retries=2,
        note=(
            _TIMEOUT_PLAN
            + " Retry axis, with retry. max_retries is 2. Each attempt is again "
            "a 2400s plan cut at 600s. simple-q counts a timeout toward retries "
            "(short limit with max_retries 1 ended retries=1) and on_dead runs "
            "on the final failure. Dramatiq retries TimeLimitExceeded and "
            "middleware sees the intermediate attempts. Celery can autoretry "
            "SoftTimeLimitExceeded; the hard SIGKILL of the child is not an "
            "exception retry. arq max_tries includes the first attempt, so "
            "max_retries 2 maps to max_tries 3. taskiq SimpleRetryMiddleware "
            "retries a TimeoutError from the handler; it does not count a "
            "dead process."
        ),
    ),
    _row(
        "timeout_soft",
        "timeout",
        time_limit_s=600,
        soft_limit_s=480,
        note=(
            _TIMEOUT_PLAN
            + " Soft limit is soft_limit_s 480, hard cap stays time_limit_s 600. "
            "Celery is the library that has both: soft_time_limit raises "
            "SoftTimeLimitExceeded in the prefork child, and time_limit is the "
            "later SIGKILL. "
            + _SINGLE_LIMIT
            + " Those four stay on this row. soft_limit_s is not a parameter "
            "they accept; their only limit is time_limit_s."
        ),
    ),
    _row(
        "timeout_hard",
        "timeout",
        time_limit_s=600,
        note=(
            _TIMEOUT_PLAN
            + " Hard axis. soft_limit_s is unset, so only the hard or only limit "
            "is armed. Celery time_limit sends SIGKILL to the prefork child. "
            "The handler cannot catch that signal. "
            + _SINGLE_LIMIT
            + " For those four this row is that single limit. They have no "
            "second, harder stage past it."
        ),
    ),
    _row(
        "timeout_catch",
        "timeout",
        time_limit_s=600,
        soft_limit_s=480,
        note=(
            _TIMEOUT_PLAN
            + " The handler catches the timeout exception, records it, and "
            "returns. It does not continue the rest of the 2400s plan. "
            "Celery SoftTimeLimitExceeded at soft_limit_s 480 is catchable. "
            "If the handler keeps running, time_limit_s 600 still SIGKILLs the "
            "child, and that kill is not catchable. "
            + _SINGLE_LIMIT
            + " Dramatiq TimeLimitExceeded can be caught once it is raised in "
            "Python code; a swallowed exception acks the message, because the "
            "middleware then sees a normal return. A blocked system call never "
            "enters the except. simple-q publishes timeout through on_dead. "
            "There is no second catchable soft exception. If the runtime "
            "cancels the task from outside the coroutine, the handler except "
            "does not see a soft error, and on_dead still records timeout. "
            "arq's timeout is raised around the coroutine by the worker. "
            "Catching CancelledError inside the job does not by itself write "
            "the failure hook the short limit run lacked. taskiq's uncaught "
            "TimeoutError was marked failed; a catch that stays inside the "
            "function is a success only when the timeout is raised in that "
            "function."
        ),
    ),
    _row(
        "timeout_async_sleep",
        "timeout",
        kind="sleep",
        time_limit_s=600,
        note=(
            _TIMEOUT_PLAN
            + " Kind axis: the handler waits with asyncio.sleep, one step at "
            "a time. simple-q, arq and taskiq run a native coroutine, so an "
            "asyncio timeout can cancel the sleep. Dramatiq async is the "
            "AsyncIO middleware and a loop in a thread; the time-limit "
            "exception is still delivered at the next GIL acquisition. Celery "
            "has no native async worker. The adapter blocks a prefork child in "
            "run_until_complete, so the limit is the soft signal or the hard "
            "SIGKILL, not an asyncio timeout wrapped around the task."
        ),
    ),
    _row(
        "timeout_blocking_sleep",
        "timeout",
        kind="block",
        time_limit_s=600,
        note=(
            _TIMEOUT_PLAN
            + " Kind axis: the handler waits with time.sleep and does not "
            "await. Dramatiq cannot cancel that system call; TimeLimitExceeded "
            "waits until the sleep returns and the thread takes the GIL. "
            "arq and taskiq arm the timeout on the asyncio loop. time.sleep "
            "blocks that loop, so the timeout callback does not run until the "
            "sleep returns. Celery soft_time_limit is SIGUSR1 and interrupts "
            "time.sleep; the hard time_limit SIGKILLs the child without "
            "waiting for the sleep. simple-q's supervisor heartbeat keeps "
            "running during a child time.sleep (measured: 20s sleep, 10s "
            "window, one delivery). That heartbeat is not this time_limit. "
            "The short stand did not measure actor time_limit against "
            "time.sleep."
        ),
    ),
    _row(
        "timeout_cpu_burn",
        "timeout",
        kind="cpu",
        time_limit_s=600,
        note=(
            _TIMEOUT_PLAN
            + " Kind axis: a pure Python CPU loop, no await and no sleep, for "
            "the planned 2400s. Dramatiq can raise TimeLimitExceeded when the "
            "thread next acquires the GIL, which a Python loop does between "
            "bytecodes. A C extension loop that does not take the GIL is not "
            "interrupted. Celery soft limit is a signal checked between "
            "bytecodes; the hard limit SIGKILLs the child either way. arq and "
            "taskiq do not get a turn on the event loop while the coroutine "
            "never awaits, so their timeout does not fire until the loop "
            "returns. simple-q has one time_limit and no separate CPU "
            "preemption knob. The short stand did not measure it against a "
            "CPU burn."
        ),
    ),
    _row(
        "timeout_http_hang",
        "timeout",
        kind="http_hang",
        time_limit_s=600,
        note=(
            _TIMEOUT_PLAN
            + " Kind axis: one HTTP call that does not return, held for the "
            "planned 2400s. The step grid keeps planned_s at 2400; the handler "
            "does not finish those steps. The client read timeout must not be "
            "shorter than time_limit_s, or the client timeout wins and this "
            "is no longer a hang. A blocking socket is a system call: Dramatiq "
            "does not cancel it, and arq/taskiq cannot fire an asyncio timeout "
            "while the loop is blocked. An async client can be cancelled on "
            "simple-q, arq and taskiq. Dramatiq still delivers TimeLimitExceeded "
            "only at the next GIL acquisition. Celery has no native async "
            "worker. A blocking socket needs SIGUSR1 (soft) or SIGKILL (hard)."
        ),
    ),
    _row(
        "timeout_completion_race",
        "timeout",
        time_limit_s=600,
        note=(
            _TIMEOUT_PLAN
            + " Completion versus the limit. Step 60 ends at t=600s, the same "
            "second time_limit_s fires. The handler is writing that step and "
            "would continue through step 240 if the limit loses. Keep "
            "planned_s at 2400. Record which side wins: the step write, the "
            "message ack, or the limit. Celery can both raise "
            "SoftTimeLimitExceeded and SIGKILL the child in this window if "
            "soft and hard are the same instant; this row leaves soft unset "
            "so the hard kill is the racer. Dramatiq races TimeLimitExceeded "
            "against the step. arq races wait_for against the step coroutine. "
            "taskiq races its timeout against the step. simple-q races "
            "time_limit against the step and then on_dead."
        ),
    ),
    _row(
        "ownership_longer_than_redelivery",
        "ownership",
        workers=2,
        note=(
            "The task runs 2400s. The redelivery window is "
            f"{REDELIVERY_WINDOW_S}s, with a second worker up. time_limit_s "
            "stays 3000 so a library that can separate the knobs does not "
            "kill the handler at the lease. max_retries is 0: a second start "
            "is redelivery, not an application retry. "
            "simple-q's lease is missed supervisor heartbeats, not a TTL "
            "taken at fetch. A live child should keep the message. Measured "
            "short case: time.sleep(20) with a 10s window, one delivery and "
            "block-end. "
            "Dramatiq's heartbeat is the process score. Another worker "
            "requeues only when maintenance runs. Default maintenance_chance "
            "is 1000 of 1_000_000. Kind did not redeliver within 75s of a "
            "dead worker. A live 2400s task does not extend a per-message "
            "lease. "
            "Celery has one visibility_timeout from delivery. kombu "
            "restore_visible returns entries older than that, and the next "
            "scan after startup is about 100s apart (interval 10s, every "
            "10th call). Progress does not extend it. Set the window to "
            "600s. Worker 2 can receive the message while worker 1 is still "
            "in the handler. The adapter uses 2100 when "
            "CELERY_VISIBILITY_TIMEOUT is unset. "
            "arq's in-progress TTL and the function timeout are the same "
            "job_timeout. A 600s lease also aborts the job. Lease 600 and "
            "handler budget 3000 is one knob on arq. arq stays on this row "
            "for that reason. "
            "taskiq acks with when_executed, after the handler returns, so "
            "pending idle time grows during the run. idle_timeout is "
            "min_idle_time for XAUTOCLAIM. An empty XREADGROUP does not call "
            "XAUTOCLAIM, so worker 2 can fail to take a live message. The "
            "short second worker did not."
        ),
    ),
    _row(
        "ownership_heartbeat_blocking",
        "ownership",
        kind="block",
        workers=2,
        note=(
            "Heartbeat while the handler is inside time.sleep for the full "
            "2400s. time_limit_s is 3000. Two workers. The redelivery window "
            f"is {REDELIVERY_WINDOW_S}s. The question is whether liveness "
            "keeps being written during the blocking call. "
            "simple-q: the supervisor writes the heartbeat, not the child. "
            "A measured 20s time.sleep with a 10s window stayed one delivery. "
            "Dramatiq: the process heartbeat is updated from broker commands "
            "on the consumer side. The blocked worker thread does not refresh "
            "a per-task lease, and maintenance stays probabilistic. "
            "Celery: nothing extends visibility_timeout while the child is "
            "blocked. The delivery clock runs anyway. "
            "arq: time.sleep blocks the loop, so this process cannot refresh "
            "the in-progress key. job_timeout still bounds both the abort and "
            "the lease. "
            "taskiq: the blocked loop does not touch the pending entry. Idle "
            "time still grows because XACK is after the handler returns."
        ),
    ),
    _row(
        "ownership_redelivery_vs_exception_retry",
        "ownership",
        max_retries=2,
        workers=2,
        note=(
            "Two arms, one plan of 2400s, time_limit_s 3000, two workers. "
            "max_retries 2 is only the exception arm. "
            "Redelivery arm: the process dies and the broker hands the same "
            "message out again. simple-q increases deliveries and leaves "
            "retries at 0 (short sigkill: deliveries 2 about 10s later with "
            "heartbeat 5s times 2; five kills: poison deliveries=6 retries=0). "
            "Dramatiq requeue needs heartbeat_timeout and a maintenance pass. "
            "The short five-kill window stayed running. "
            "Celery redelivery is visibility_timeout. The short sigkill window "
            "was shorter than the about-100s restore period, so no second "
            "start was recorded. "
            "arq has job_try and no separate delivery counter. "
            "taskiq's retry middleware counts handler exceptions. A kill "
            "leaves XPENDING and XACK 0; the short port-6396 kill during "
            "sleep had XPENDING 1, XACK 0, XAUTOCLAIM 0. "
            "Exception arm: the handler raises. That increases retries on "
            "simple-q, Dramatiq and Celery, job_try on arq, and "
            "SimpleRetryMiddleware on taskiq. Score that arm on max_retries. "
            "Do not score it as a delivery or as poison. "
            "arq max_tries includes the first attempt (max_retries 2 maps to "
            "max_tries 3)."
        ),
    ),
    _row(
        "sigkill_child",
        "fault",
        workers=2,
        fault="sigkill_child",
        note=(
            "SIGKILL the process that is inside the handler. Leave the "
            "supervisor when the library has one. Planned work is 2400s and "
            "time_limit_s is 3000, so the limit is not the killer. "
            "simple-q: kill the child, leave agent-worker. deliveries "
            "increases, retries stays 0. Short sigkill redelivered in about "
            "10s at heartbeat 5s times 2. "
            "Dramatiq: kill one worker process. The heartbeat score freezes. "
            "Requeue still needs maintenance. Default chance did not "
            "redeliver on the short sigkill. With maintenance_chance "
            "1_000_000 and heartbeat 8000 ms, RPUSH was 8.460s after SIGKILL. "
            "Celery: kill a prefork child, leave the parent. The message "
            "waits for visibility_timeout. The short window recorded one "
            "block-start. "
            "arq is one process. There is no child to kill apart from the "
            "worker. SIGKILL of that process is the whole worker. There is "
            "no delivery counter. The in-progress key expires with "
            "job_timeout. arq stays on this row for that limitation. "
            "taskiq --workers spawns children and when_executed acks after "
            "the handler. A kill during sleep left the entry pending."
        ),
    ),
    _row(
        "sigkill_tree",
        "fault",
        workers=2,
        fault="sigkill_tree",
        note=(
            "SIGKILL the process group: supervisor and children. Planned "
            "work is 2400s, time_limit_s 3000. "
            "simple-q: agent-worker dies with the child, so the heartbeat "
            "stops and no parent respawns the child. Redelivery is missed "
            "heartbeats, and deliveries increases. "
            "Dramatiq: the worker process tree dies. Requeue is the same "
            "maintenance path as sigkill_child. The library has no separate "
            "supervisor beyond the dramatiq process. "
            "Celery: parent and prefork children die. The next worker "
            "redelivers only through visibility_timeout. "
            "arq has no process tree. This kill is the same process as "
            "sigkill_child. arq stays on the row. "
            "taskiq: parent and worker children die. The pending entry stays "
            "unacked because when_executed did not run."
        ),
    ),
    _row(
        "sigterm_grace",
        "fault",
        fault="sigterm_grace",
        note=(
            "SIGTERM, then grace, then SIGKILL if the 2400s handler is still "
            "running. time_limit_s is 3000. No checkpoint flag on this row. "
            "simple-q early SIGTERM on rc7 exited 0 in about 0.2s. An "
            "infinite block with grace 6s exited -9 near 6s. Kind with a "
            "live block and grace 12s exited 137 on the deadline. "
            "Dramatiq uses --worker-shutdown-timeout. A short early stop "
            "became SIGKILL around 5s. Kind with no task exited 0 in 3s. "
            "Kind with a live block exited 137 at grace. "
            "Celery uses worker_soft_shutdown_timeout. Early SIGTERM was "
            "about 1s, code 0. The short infinite-block file exited 0 in "
            "1.2s. Kind with a live block and grace 12s exited 137. "
            "arq uses job_completion_wait. Early SIGTERM was 0.2s, code 0. "
            "arq was not in the kind cluster. "
            "taskiq uses --shutdown-timeout. Early SIGTERM was about 1.2s, "
            "code 0. Kind with a live block exited 137 at grace."
        ),
    ),
    _row(
        "rollout_checkpoint",
        "fault",
        fault="rollout_checkpoint",
        note=(
            "Rolling replace with a drain flag set before SIGTERM. The "
            "handler stops at a step boundary and the adapter turns Draining "
            "into a retry. The 2400s plan must not pin the old process until "
            "grace. Steady workers is 1; maxSurge 1 means two pods overlap. "
            "Short process run, checkpoint on: simple-q same message id, "
            "retries 0 to 1, deliveries stayed 1. Dramatiq missed grace and "
            "was SIGKILL'd, and the checkpoint still landed, with several "
            "retries while the old process died. Celery retries 0 to 1. "
            "arq try 1 to 2, same job id. taskiq started again on a new "
            "process. Kind checkpoint (6 steps of 2s, grace 40s) exited in "
            "seconds and the second attempt was about 5s later for simple-q, "
            "Dramatiq, Celery and taskiq. arq was not in that cluster."
        ),
    ),
    _row(
        "rollout_plain",
        "fault",
        fault="rollout_plain",
        note=(
            "Rolling replace with no drain flag. The 2400s handler will not "
            "finish inside a normal grace, so the old process lives until "
            "terminationGracePeriodSeconds and then receives SIGKILL. "
            "The short 4 times 1s task finished before the gap and does not "
            "answer a 30 minute job. Kind blocker of 180s, grace 12s: "
            "simple-q, Dramatiq, Celery and taskiq exited 137 on the "
            "deadline. simple-q redelivered about 14.6s after SIGKILL, "
            "deliveries=2, retries=0. Dramatiq, Celery (visibility 30s) and "
            "taskiq did not redeliver within 75s. taskiq when_executed acks "
            "after the handler; the kind run did not check that the entry "
            "was still pending. arq was not in the cluster. arq's plain "
            "stop waits job_completion_wait and then the in-progress key "
            "expires with job_timeout."
        ),
    ),
    _row(
        "memory_soft",
        "fault",
        fault="memory_soft",
        note=(
            "During the 2400s job the handler grows RSS past the soft "
            "threshold. kind stays sleep; the allocation is the fault "
            "stimulus. "
            "simple-q has --child-memory-soft. The short alloc:30 run "
            "replaced the child pid and lost 0. "
            "Celery has worker_max_memory_per_child, one limit, checked "
            "after the task rather than as a mid-task soft signal. The "
            "short run replaced the pid and lost 0. It does not read cgroup "
            "usage. Celery has no separate soft RSS knob. It stays on this "
            "row; the one after-task limit is what this row can arm. "
            "Dramatiq, arq and taskiq have no process RSS limit. A "
            "systemd or cgroup cap is not this knob. They stay on the row. "
            "taskiq's --max-tasks-per-child is a task count, not RSS."
        ),
    ),
    _row(
        "memory_hard",
        "fault",
        fault="memory_hard",
        note=(
            "During the 2400s job the handler grows RSS past the hard "
            "threshold. "
            "simple-q has --child-memory-hard and kills the child. "
            "Celery's only memory knob is worker_max_memory_per_child, "
            "checked after the task. There is no harder mid-task RSS kill "
            "and no cgroup accounting. Celery stays on the row for that "
            "limitation. "
            "Dramatiq, arq and taskiq have no hard RSS limit. They stay on "
            "the row. An external cgroup kill is the oom row, not this one."
        ),
    ),
    _row(
        "oom",
        "fault",
        workers=2,
        fault="oom",
        note=(
            "The kernel OOM killer, or a cgroup OOM, kills the worker "
            "during the 2400s job. This is outside every library's RSS "
            "knob. The short stand did not run an OOM on each delivery. "
            "All five stay on the row because the kernel can kill any of "
            "them. Aftermath matches a SIGKILL: simple-q increases "
            "deliveries and can reach poison; Dramatiq requeues only after "
            "heartbeat_timeout and a maintenance pass; Celery waits for "
            "visibility_timeout; arq waits for the in-progress TTL; taskiq "
            "leaves the stream entry pending until something XAUTOCLAIMs it. "
            "None of the five read the pod cgroup to do this themselves."
        ),
    ),
    _row(
        "recycle_prefetch",
        "fault",
        fault="recycle_prefetch",
        note=(
            "Recycle the worker that still holds prefetched messages, while "
            "a 2400s job is in progress. "
            "simple-q --max-tasks-per-process replaced pids on the short "
            "recycle and lost 0. After SIGTERM, prefetched work returned in "
            "0.26s with 0 loss. batch_size on the stand is 1. "
            "Celery worker_max_tasks_per_child replaced the pid and the "
            "recycle invariant was 0. The short prefetch batch lost 1. "
            "Dramatiq has no recycle knob. The short prefetch batch lost 2. "
            "Dramatiq stays on the row. "
            "arq has no recycle and no broker prefetch pile. max_jobs is "
            "in-process concurrency. arq stays on the row for that "
            "limitation. "
            "taskiq --max-tasks-per-child finished 1 of 12 and lost 11, "
            "because the process exited with unacked work. The short "
            "prefetch batch lost 6. --max-prefetch is the prefetch knob. "
            "Ack is when_executed."
        ),
    ),
    _row(
        "repeated_kill_after_start",
        "fault",
        fault="repeated_kill_after_start",
        note=(
            "Kill the worker shortly after each start, then start it again, "
            "several times, on one 2400s message. max_retries is 0, so only "
            "redelivery can move the message. "
            "simple-q: five SIGKILLs reached poison, fail_reason "
            "poison deliveries=6 retries=0, via on_dead. The threshold is "
            "max_deliveries, not max_retries. "
            "Dramatiq, Celery, arq and taskiq stayed status running on that "
            "short window. No poison reason was recorded. Dramatiq "
            "maintenance may not have run. taskiq showed several long "
            "starts and stayed running; the hook counts handler exceptions, "
            "not process death. arq has no poison counter."
        ),
    ),
    _row(
        "net_partition",
        "redis",
        fault="net_partition",
        note=(
            "The link between the worker and Redis drops while the 2400s "
            "job is running, then returns. Redis itself is not restarted. "
            "Short redis_down of 5s: simple-q stayed up and finished 0 of 6, "
            "because the empty poll waits out the heartbeat period and the "
            "pause fit inside it. A partition longer than the heartbeat "
            "lease looks like a dead worker to the rest of the fleet. "
            "Dramatiq, Celery and taskiq stayed up and finished 6 of 6. "
            "arq exited with code 1 and finished 0 of 6."
        ),
    ),
    _row(
        "restart_save",
        "redis",
        fault="restart_save",
        note=(
            "Restart Redis with persistence (AOF or RDB) while a 2400s job "
            "is queued or in flight. Short redis restart with AOF: the "
            "three queued jobs were kept on simple-q, Dramatiq, Celery, arq "
            "and taskiq (lost 0). In-flight work still follows each "
            "library's ack rule after the socket returns."
        ),
    ),
    _row(
        "restart_nosave",
        "redis",
        fault="restart_nosave",
        note=(
            "Restart Redis with no persistence while a 2400s job is queued "
            "or in flight. Short restart without AOF: three queued jobs "
            "were lost on all five libraries, attempts 0. A message that "
            "existed only in Redis memory is gone. A side effect already "
            "performed is the effect rows, not this one."
        ),
    ),
    _row(
        "disconnect_during_ack",
        "redis",
        fault="disconnect_during_ack",
        note=(
            "Drop the Redis connection after the handler has finished the "
            "2400s plan and before the ack command completes. "
            "simple-q: a missed ack leaves the same message id. deliveries "
            "increases when it is handed out again. unique_key is released "
            "only after a successful ack. "
            "Dramatiq: ack clears the unacked set. A failed ack leaves the "
            "id for a later maintenance requeue. "
            "Celery: ack removes the unacked entry. A failed ack leaves it "
            "for restore_visible. "
            "arq: completion writes the result and clears in-progress. A "
            "drop can leave the job in progress until job_timeout. "
            "taskiq: XACK runs after the handler (when_executed). A drop "
            "before XACK leaves XPENDING."
        ),
    ),
    _row(
        "disconnect_during_retry",
        "redis",
        max_retries=2,
        fault="disconnect_during_retry",
        note=(
            "Drop the Redis connection while a retry is being written. "
            "max_retries is 2 so a retry exists to interrupt. The plan is "
            "still 2400s. "
            "simple-q updates the same message for a retry. A lost write "
            "can keep the old copy or drop the retry. "
            "Dramatiq re-enqueues on retry. A lost write can drop the "
            "message or leave the original. "
            "Celery publishes the retry as another message. The original "
            "visibility and the new copy can overlap. "
            "arq Retry(defer) requeues. A lost write leaves the job at the "
            "failed command. "
            "taskiq RedisStreamBroker has no delayed delivery. The retry "
            "middleware sends again. A drop can duplicate or lose that "
            "send. arq max_tries includes the first attempt, so max_retries "
            "2 maps to max_tries 3."
        ),
    ),
    _row(
        "redis_cmd_not_applied",
        "redis",
        planned_s=1,
        steps=1,
        step_s=1,
        workers=2,
        fault="redis_cmd_not_applied",
        note=(
            "Short fault test, not a 2400s run. Only simple-q 1.2.0 was "
            "executed, on Redis 7.0.15 at 127.0.0.1:6415 via a proxy on "
            "6414. The server process was not restarted. The proxy closed "
            "the client before forwarding fetch_next, renew_leases, or "
            "handle_results. A dropped fetch left the task in "
            "bench:scheduled with status Pending and no handler line; a "
            "later worker ran it once into done. A dropped renew did not "
            "change locked_at and did not remove the inflight member. The "
            "child exited, its worker score stayed put, and about 61s "
            "later another worker name started the handler. A dropped "
            "finish left the same inflight member; the handler had already "
            "run, and about 61s later the same task id was done after a "
            "second handler line. See docs/reference/ambiguous-redis.md."
        ),
    ),
    _row(
        "redis_cmd_applied_reply_lost",
        "redis",
        planned_s=1,
        steps=1,
        step_s=1,
        workers=2,
        fault="redis_cmd_applied_reply_lost",
        note=(
            "The proxy forwarded the command, read Redis's reply, then "
            "closed the client. Redis pid 126504 stayed up. A dropped "
            "fetch reply left the task only in bench:inflight:{worker}. "
            "The handler did not run and that worker's score did not move. "
            "At 60.8s another worker ran it once and it was done. A "
            "dropped renew reply did change locked_at; the child still "
            "exited on broken pipe, and about 61s later the handler "
            "started under a different inflight key. A dropped finish "
            "reply left the task done, attempts 1, with one handler line. "
            "The child logged ProcessEvents failed and did not run the "
            "body again. The live key is bench:inflight:{worker_name}, "
            "with no generation suffix."
        ),
    ),
    _row(
        "redis_reconnect_same_owner",
        "redis",
        planned_s=1,
        steps=1,
        step_s=1,
        workers=2,
        fault="redis_reconnect_same_owner",
        note=(
            "The proxy refused worker sockets for 3s and then accepted "
            "them again. Redis was not restarted. The owner child logged "
            "HeartBeat failed and fetch_next failed, then exited. Its "
            "zset score stayed at the pre-gap value for the next 44s. The "
            "task remained in that worker's inflight set, with one start "
            "line and no end line. 44s is inside the 60s orphan window, so "
            "this row does not show the later reap. The same worker name "
            "did not resume the handler after the socket returned."
        ),
    ),
    _row(
        "redis_reconnect_after_transfer",
        "redis",
        planned_s=1,
        steps=1,
        step_s=1,
        workers=2,
        fault="redis_reconnect_after_transfer",
        note=(
            "After a lost fetch reply, a lost renew, or a 70s proxy "
            "blackhole, a different worker name held the task once the old "
            "score was about 60s old, and that worker ran the handler. "
            "During the 70s blackhole the first handler wrote its end "
            "line while Redis still showed the original inflight member; "
            "the second worker then ran the body again and the task "
            "reached done. A renew reply held for 75s was written back "
            "after the child had already exited on a timeout; the task "
            "stayed on the new inflight key. There is no generation field "
            "for a late reply to restore."
        ),
    ),
    _row(
        "kill_after_effect",
        "effect",
        workers=2,
        fault="sigkill_child",
        note=(
            "The handler performs an external effect (HTTP or a DB write), "
            "then the child is SIGKILL'd before ack. The 2400s plan is the "
            "job that effect sits inside. The effect is already visible. "
            "Redelivery runs it again. "
            "simple-q redelivery keeps the message id and increases "
            "deliveries. "
            "Dramatiq requeues through maintenance, so the second run can "
            "be late or absent at the default maintenance chance. "
            "Celery redelivers after visibility_timeout, and the first "
            "child may still have been alive if the window is short; this "
            "row kills the child, so the duplicate is the restored message. "
            "arq can run the job again after the in-progress TTL. There is "
            "no separate delivery counter. "
            "taskiq leaves the entry pending (XACK is after the handler), "
            "so a later claim runs the effect again. None of the five make "
            "the effect itself exactly-once."
        ),
    ),
    _row(
        "kill_after_result_before_ack",
        "effect",
        workers=2,
        fault="sigkill_child",
        note=(
            "The handler writes the result, then the child is SIGKILL'd "
            "before ack. The result row exists and the message is still "
            "unacked. Planned work is 2400s. "
            "simple-q has no result backend. The result in this row is the "
            "application row, and the broker message is still outstanding, "
            "so deliveries can increase on the same id. "
            "Dramatiq's result middleware is not what the adapter uses. "
            "The application result can exist while the message is still "
            "in the unacked set. "
            "Celery's result backend can hold a result while the unacked "
            "entry is still restored. "
            "arq stores the job result in Redis for the life of the job-id "
            "key. A kill before that write finishes leaves in-progress set. "
            "A kill after the result write and before in-progress is "
            "cleared is the race. "
            "taskiq's adapter does not enable a result backend. The "
            "application result can exist with XACK still 0."
        ),
    ),
    _row(
        "effect_with_idempotency_key",
        "effect",
        workers=2,
        fault="sigkill_child",
        note=(
            "Same kill as kill_after_effect, and the effect carries an "
            "application idempotency key so the second delivery does not "
            "apply it twice. The key is not a broker feature. "
            "simple-q unique_key dedups a second send until ack, then "
            "accepts a new send. That is send dedup, not the effect key. "
            "A redelivery is the same message id. "
            "arq _job_id rejects another enqueue while the result key "
            "lives, including after ack. That is also send dedup. "
            "Dramatiq, Celery and taskiq execute a duplicate send. taskiq "
            "with_task_id did not dedupe (3 runs, mock calls 3). "
            "All five stay on the row. The idempotency key under test is "
            "the one on the effect."
        ),
    ),
    _row(
        "effect_without_idempotency_key",
        "effect",
        workers=2,
        fault="sigkill_child",
        note=(
            "Same kill as kill_after_effect, with no idempotency key on "
            "the effect. A second delivery applies the effect again. "
            "simple-q, Dramatiq, Celery, arq and taskiq all stay on the "
            "row. None of them attach an effect idempotency key. Send "
            "dedup, where a library has it, does not collapse two runs of "
            "the same unacked message into one effect."
        ),
    ),
    _row(
        "mixed_long_short",
        "load",
        workers=2,
        note=(
            "One job is this 2400s plan. Short jobs of 1s are offered on "
            "the same queue while it runs. Two workers. "
            "simple-q batch_size is 1, so it does not hold a prefetch pile. "
            "An empty poll still waits until the next heartbeat. "
            "Dramatiq prefetch holds later messages in the process (2 at "
            "--threads 1, more when prefetch is raised). A blocked or long "
            "actor delays those messages. "
            "Celery prefork prefetch can hold the same kind of pile. The "
            "short prefetch batch lost 1 on stop. "
            "arq max_jobs is in-process concurrency. A blocking long job "
            "occupies the loop and the short jobs behind it. "
            "taskiq can hold unacked messages up to --max-prefetch. The "
            "short prefetch batch lost 6 on stop. Ack waits for the handler."
        ),
    ),
    _row(
        "offer_below",
        "load",
        workers=2,
        note=(
            "Offer under capacity. Two workers, one long job of this 2400s "
            "plan. A slot stays free. Queue wait for that job should be "
            "the idle pickup of the library, not a backlog. simple-q idle "
            "pickup waits for the heartbeat period. Celery, Dramatiq, arq "
            "and taskiq pick up a waiting message without that heartbeat "
            "period on the short fast batch."
        ),
    ),
    _row(
        "offer_near",
        "load",
        workers=2,
        note=(
            "Offer at the worker slots. Two workers, two long jobs of this "
            "2400s plan, one each. No spare slot and no queue of extra "
            "long jobs. Observe whether either job is redelivered while "
            "both workers are busy for 2400s. That redelivery is the "
            "ownership window, not a short plan."
        ),
    ),
    _row(
        "offer_above",
        "load",
        workers=2,
        note=(
            "Offer above capacity. Two workers, eight long jobs of this "
            "2400s plan. Six wait. The observation is queue time and "
            "whether prefetch pulls waiting 2400s jobs into a busy process. "
            "Do not shorten the waiting jobs. Dramatiq and Celery prefetch "
            "can pin a waiting long job inside a process that is already "
            "inside one. taskiq --max-prefetch can do the same and those "
            "copies stay unacked. simple-q batch_size 1 leaves them in "
            "Redis. arq max_jobs leaves them in Redis until a slot frees."
        ),
    ),
    _row(
        "clean_only",
        "load",
        workers=2,
        note=(
            "Load shape with no fault, no short jobs, and no poison. Two "
            "workers, one 2400s job, time_limit_s 3000. Success here is a "
            "clean completion. The counted 2400s sample remains "
            "A1_control_40 (one worker). This row is the clean side of "
            "faulted_separate."
        ),
    ),
    _row(
        "faulted_separate",
        "load",
        workers=2,
        note=(
            "The clean 2400s job stays on its own queue and its own worker. "
            "The other worker runs a different queue whose fault is "
            "repeated_kill_after_start. fault on this row is unset so the "
            "clean job is not killed. The clean job uses time_limit_s 3000 "
            "and max_retries 0. "
            "simple-q can isolate that with a second queue. Dramatiq, "
            "Celery, arq and taskiq can too. Sharing one queue lets a "
            "prefetched poison message sit in the same process as the "
            "clean job (Dramatiq prefetch, Celery prefetch, taskiq "
            "--max-prefetch). arq shares the loop inside one process via "
            "max_jobs, so the faulted job has to be a separate process."
        ),
    ),
    _row(
        "drain_after_stop",
        "load",
        workers=2,
        note=(
            "Stop the workers with SIGTERM after the 2400s job has started "
            "and short jobs are already prefetched. Then drain what "
            "remains. This row's fault is unset; sigterm_grace is the "
            "signal row, and this one counts what is left in the queue. "
            "Short prefetch stop: simple-q returned unacked work in 0.26s "
            "with 0 loss. Dramatiq lost 2, Celery lost 1, taskiq lost 6. "
            "arq has no prefetch pile of that kind. In-flight work waits "
            "for job_completion_wait. A 2400s job will not finish inside a "
            "short grace, so drain means the message is visible again, "
            "not that the handler ran to step 240."
        ),
    ),
]


def _validate(rows: list[dict]) -> None:
    ids = [row["scenario_id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise RuntimeError("duplicate scenario_id")
    required_ids = {
        "A0_wire",
        "A1_control_40",
        "timeout_without_retry",
        "timeout_with_retry",
        "timeout_soft",
        "timeout_hard",
        "timeout_catch",
        "timeout_async_sleep",
        "timeout_blocking_sleep",
        "timeout_cpu_burn",
        "timeout_http_hang",
        "timeout_completion_race",
        "ownership_longer_than_redelivery",
        "ownership_heartbeat_blocking",
        "ownership_redelivery_vs_exception_retry",
        "sigkill_child",
        "sigkill_tree",
        "sigterm_grace",
        "rollout_checkpoint",
        "rollout_plain",
        "memory_soft",
        "memory_hard",
        "oom",
        "recycle_prefetch",
        "repeated_kill_after_start",
        "net_partition",
        "restart_save",
        "restart_nosave",
        "disconnect_during_ack",
        "disconnect_during_retry",
        "kill_after_effect",
        "kill_after_result_before_ack",
        "effect_with_idempotency_key",
        "effect_without_idempotency_key",
        "mixed_long_short",
        "offer_below",
        "offer_near",
        "offer_above",
        "clean_only",
        "faulted_separate",
        "drain_after_stop",
    }
    missing = required_ids.difference(ids)
    if missing:
        raise RuntimeError(f"missing scenarios: {sorted(missing)}")
    timeout_ids = {
        "timeout_without_retry",
        "timeout_with_retry",
        "timeout_soft",
        "timeout_hard",
        "timeout_catch",
        "timeout_async_sleep",
        "timeout_blocking_sleep",
        "timeout_cpu_burn",
        "timeout_http_hang",
        "timeout_completion_race",
    }
    for row in rows:
        if tuple(row) != _KEYS:
            raise RuntimeError(f"key order/set mismatch: {row['scenario_id']}")
        if row["libraries"] != LIBS:
            raise RuntimeError(f"libraries dropped: {row['scenario_id']}")
        if row["kind"] not in _KINDS:
            raise RuntimeError(row["scenario_id"])
        if row["steps"] * row["step_s"] != row["planned_s"]:
            raise RuntimeError(f"plan mismatch: {row['scenario_id']}")
        if not isinstance(row["counts_as_2400"], bool):
            raise RuntimeError(row["scenario_id"])
        soft = row["soft_limit_s"]
        if soft is not None and soft > row["time_limit_s"]:
            raise RuntimeError(row["scenario_id"])
        if row["scenario_id"] in timeout_ids:
            if row["planned_s"] != 2400 or row["time_limit_s"] != 600:
                raise RuntimeError(row["scenario_id"])
            if row["counts_as_2400"]:
                raise RuntimeError(row["scenario_id"])
    control = next(row for row in rows if row["scenario_id"] == "A1_control_40")
    if (
        control["planned_s"] != 2400
        or control["steps"] != 240
        or control["step_s"] != 10
        or control["kind"] != "sleep"
        or control["time_limit_s"] != 3000
        or control["max_retries"] != 0
        or control["workers"] != 1
        or control["fault"] is not None
        or control["counts_as_2400"] is not True
    ):
        raise RuntimeError("A1_control_40 drifted")
    wire = next(row for row in rows if row["scenario_id"] == "A0_wire")
    if (
        wire["planned_s"] != 3
        or wire["steps"] != 3
        or wire["step_s"] != 1
        or wire["counts_as_2400"] is not False
        or "short wiring only" not in wire["note"]
    ):
        raise RuntimeError("A0_wire drifted")


_validate(SCENARIOS)
