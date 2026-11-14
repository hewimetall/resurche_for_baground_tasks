"""Fault the applied-retry enqueue path for simple-q 1.2.0.

Client send goes direct to Redis. Workers go through FaultProxy. The armed
fault targets script kind ``enqueue`` only, so the initial client send cannot
consume the trigger.

Observation covers every task id that shares the researched message_id.
Stop condition is not ``A == Done`` alone: all discovered ids, unfinished
proxy ops, and the orphan window are considered. Exhausted budgets mark the
scenario ``incomplete``.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable

import redis

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bench.longrun.stand import ensure_redis, flush_owned, shutdown_save
from scripts.redis_fault_proxy import KNOWN_SHA, FaultProxy

REDIS_PORT = 6420
PROXY_PORT = 6421
REDIS_DIR = "/workspace/results/long/retry-enqueue-6420"
OUT = ROOT / "results" / "long" / "retry-enqueue"
WORKER = str(ROOT / ".venv-sq120" / "bin" / "agent-worker")
DIRECT = f"redis://127.0.0.1:{REDIS_PORT}/0"
VIA_PROXY = f"redis://127.0.0.1:{PROXY_PORT}/0"
ORPHAN_S = 15 * 4
CLIENT_TIMEOUT_PROBE_S = 10.0
WATCH_BUDGET_CONTROL_S = 45.0
WATCH_BUDGET_FAULT_S = ORPHAN_S + 50.0
APP = "bench.retry_by_retries_app:create_app"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def redis_pid(client: redis.Redis) -> int:
    return int(client.info("server")["process_id"])


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _decode_job(raw: str) -> dict | str:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw[:500]
    if not isinstance(parsed, dict):
        return raw[:500]
    keep = {
        key: parsed.get(key)
        for key in (
            "retries",
            "max_retries",
            "actor_name",
            "message_id",
            "kwargs",
            "delay_ms",
            "deliveries",
        )
        if key in parsed
    }
    return keep or raw[:500]


class Observer:
    def __init__(self, port: int, name: str = "retry-enqueue-observer") -> None:
        self.client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
        self.client.client_setname(name)
        self.addr = ""
        for row in self.client.client_list():
            if row.get("name") == name:
                self.addr = str(row.get("addr") or "")
                break

    def inventory(self) -> dict:
        ids = sorted(self.client.hkeys("bench:data"))
        inflight: dict[str, list[str]] = {}
        for key in self.client.scan_iter("bench:inflight:*", count=200):
            inflight[_text(key)] = sorted(self.client.smembers(key))
        scheduled = [
            {"id": member, "score": score}
            for member, score in self.client.zrange("bench:scheduled", 0, -1, withscores=True)
        ]
        active = self.client.lrange("bench:active", 0, -1)
        done = [
            {"id": member, "score": score}
            for member, score in self.client.zrange("bench:done", 0, -1, withscores=True)
        ]
        dead = [
            {"id": member, "score": score}
            for member, score in self.client.zrange("bench:dead", 0, -1, withscores=True)
        ]
        workers = [
            {"name": member, "score": score}
            for member, score in self.client.zrange("bench:workers", 0, -1, withscores=True)
        ]
        deliveries = {}
        for key in self.client.scan_iter("delivery:*", count=200):
            deliveries[_text(key)] = self.client.get(key)
        tasks = []
        for task_id in ids:
            meta = self.client.hgetall(f"bench:meta:{task_id}")
            raw = self.client.hget("bench:data", task_id) or ""
            job = _decode_job(raw)
            places = []
            if any(item["id"] == task_id for item in scheduled):
                places.append("scheduled")
            if task_id in active:
                places.append("active")
            for key, members in inflight.items():
                if task_id in members:
                    places.append(key)
            if any(item["id"] == task_id for item in done):
                places.append("done")
            if any(item["id"] == task_id for item in dead):
                places.append("dead")
            tasks.append(
                {
                    "id": task_id,
                    "places": places,
                    "meta": meta,
                    "job": job,
                    "message_id": job.get("message_id") if isinstance(job, dict) else None,
                    "retries": job.get("retries") if isinstance(job, dict) else None,
                }
            )
        return {
            "tasks": tasks,
            "inflight": inflight,
            "scheduled": scheduled,
            "active": active,
            "done": done,
            "dead": dead,
            "workers": workers,
            "deliveries": deliveries,
            "redis_time": self.client.time(),
        }

    def tasks_for_message(self, message_id: str) -> list[dict]:
        return [task for task in self.inventory()["tasks"] if task.get("message_id") == message_id]


def read_effects(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def _changed(previous: Any, current: Any) -> bool:
    return json.dumps(previous, sort_keys=True, default=str) != json.dumps(
        current, sort_keys=True, default=str
    )


def start_monitor(path: Path) -> subprocess.Popen[str]:
    handle = path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        ["redis-cli", "-p", str(REDIS_PORT), "MONITOR"],
        stdout=handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    proc._handle = handle  # type: ignore[attr-defined]
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and path.stat().st_size == 0:
        time.sleep(0.05)
    return proc


def stop_monitor(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
    handle = getattr(proc, "_handle", None)
    if handle is not None:
        handle.close()


def interesting_monitor(path: Path, observer_addr: str) -> list[dict]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if observer_addr and observer_addr in line:
            continue
        if " MONITOR" in line or line.endswith('"MONITOR"'):
            continue
        upper = line.upper()
        if not any(
            token in upper
            for token in ("EVAL", "SCRIPT", "ZADD", "SADD", "SREM", "HSET", "DEL", "INCR", "LPUSH", "RPUSH")
        ):
            continue
        if (
            "bench:" not in line
            and "delivery:" not in line
            and "EVAL" not in upper
            and "SCRIPT" not in upper
        ):
            continue
        kind = None
        for digest, name in KNOWN_SHA.items():
            if digest in line:
                kind = name
                break
        rows.append({"line": line[:700], "kind": kind})
    return rows


class Workers:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen[bytes]] = []
        self.logs: list[object] = []
        self.metrics = 6440

    def start(self, effect: Path, redis_url: str, label: str) -> subprocess.Popen[bytes]:
        log_path = OUT / f"worker-{label}-{len(self.procs)}.log"
        handle = log_path.open("w", encoding="utf-8")
        env = os.environ.copy()
        env.update(
            {
                "PYTHONPATH": str(ROOT),
                "PYTHONUNBUFFERED": "1",
                "REDIS_URL": redis_url,
                "EFFECT_PATH": str(effect),
                "RUST_LOG": "warn,agent_worker=info",
                "DRAMATIQ_PROM_HOST": "127.0.0.1",
                "DRAMATIQ_PROM_PORT": str(self.metrics),
            }
        )
        self.metrics += 1
        proc = subprocess.Popen(
            [
                WORKER,
                "worker",
                "--app",
                APP,
                "--factory",
                "--queues",
                "bench",
                "--processes",
                "1",
                "--concurrency",
                "1",
            ],
            cwd=str(ROOT),
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        proc._log_path = str(log_path)  # type: ignore[attr-defined]
        proc._label = label  # type: ignore[attr-defined]
        self.procs.append(proc)
        self.logs.append(handle)
        return proc

    def stop(self) -> None:
        for proc in self.procs:
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 5
        for proc in self.procs:
            try:
                proc.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for handle in self.logs:
            handle.close()
        self.procs.clear()
        self.logs.clear()


def _log_excerpt(proc: subprocess.Popen[bytes] | None) -> str:
    if proc is None:
        return ""
    path = Path(getattr(proc, "_log_path", ""))
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    keep = [
        line
        for line in lines
        if any(
            token in line
            for token in (
                "retry enqueue",
                "enqueue failed",
                "leaving the attempt",
                "ProcessEvents",
                "poll_next",
                "broken pipe",
                "timed out",
                "Dead",
                "re-enqueued",
                "ERROR",
                "error",
                "outcome",
            )
        )
    ]
    return "\n".join(keep[-120:])


def enqueue_retry_actor(token: str) -> dict:
    os.environ["REDIS_URL"] = DIRECT
    import bench.retry_by_retries_app as app

    app._client = None

    async def _send() -> object:
        return await app.client().send_with_options(
            app.retry_by_retries, kwargs={"token": token}
        )

    before = set(Observer(REDIS_PORT).client.hkeys("bench:data"))
    t0 = time.time()
    accepted = asyncio.run(_send())
    t1 = time.time()
    after = set(Observer(REDIS_PORT).client.hkeys("bench:data"))
    new = sorted(after - before)
    return {
        "message_id": None if accepted is None else str(accepted),
        "task_ids": new,
        "enqueue_unix": t0,
        "accepted_unix": t1,
    }


def enqueue_delay_probe(token: str, delay_ms: int) -> dict:
    os.environ["REDIS_URL"] = DIRECT
    import bench.retry_by_retries_app as app

    app._client = None

    async def _send() -> object:
        return await app.client().send_with_options(
            app.delay_probe,
            kwargs={"token": token, "delay_ms": delay_ms},
            delay=delay_ms,
        )

    before = set(Observer(REDIS_PORT).client.hkeys("bench:data"))
    t0 = time.time()
    accepted = asyncio.run(_send())
    t1 = time.time()
    after = set(Observer(REDIS_PORT).client.hkeys("bench:data"))
    new_ids = sorted(after - before)
    obs = Observer(REDIS_PORT)
    scheduled = []
    for task_id in new_ids:
        score = obs.client.zscore("bench:scheduled", task_id)
        scheduled.append({"id": task_id, "score": score})
    return {
        "message_id": None if accepted is None else str(accepted),
        "task_ids": new_ids,
        "enqueue_unix": t0,
        "accepted_unix": t1,
        "scheduled": scheduled,
        "delay_ms": delay_ms,
    }


def message_complete(
    snapshot: dict,
    message_id: str,
    effects: list[dict],
    proxy: FaultProxy | None,
    *,
    min_ok_ends: int | None = None,
) -> bool:
    tasks = [task for task in snapshot["tasks"] if task.get("message_id") == message_id]
    if not tasks:
        return False
    if any(
        place.startswith("bench:inflight:")
        or place in {"scheduled", "active"}
        for task in tasks
        for place in task["places"]
    ):
        return False
    if not all("done" in task["places"] or "dead" in task["places"] for task in tasks):
        return False
    if snapshot.get("deliveries"):
        # deliveries for this message_id only
        prefix = f"delivery:{message_id}:"
        if any(key.startswith(prefix) for key in snapshot["deliveries"]):
            return False
    if proxy is not None:
        snap = proxy.snapshot()
        armed = snap.get("armed")
        if armed:
            return False
        # a delay still holding would appear as delay without release for last event
        events = snap.get("events") or []
        if events:
            last = events[-1]
            if last.get("action") == "delay_response":
                return False
    if min_ok_ends is not None:
        ok_ends = [
            row
            for row in effects
            if row.get("phase") == "handler_end"
            and row.get("message_id") == message_id
            and row.get("result") == "ok"
        ]
        if len(ok_ends) < min_ok_ends:
            return False
    return True


def watch_message(
    observer: Observer,
    effect: Path,
    message_id: str,
    budget_s: float,
    *,
    proxy: FaultProxy | None = None,
    stop_when: Callable[[dict, list[dict]], bool] | None = None,
) -> tuple[list[dict], str]:
    samples: list[dict] = []
    previous = None
    previous_effects = None
    started = time.monotonic()
    status = "incomplete"
    while time.monotonic() - started < budget_s:
        current = observer.inventory()
        effects = read_effects(effect)
        related = [task for task in current["tasks"] if task.get("message_id") == message_id]
        if _changed(previous, related) or effects != previous_effects:
            samples.append(
                {
                    "t_s": round(time.monotonic() - started, 3),
                    "unix": time.time(),
                    "tasks": related,
                    "all_task_count": len(current["tasks"]),
                    "workers": current["workers"],
                    "deliveries": {
                        key: value
                        for key, value in current["deliveries"].items()
                        if message_id in key
                    },
                    "scheduled": [
                        item
                        for item in current["scheduled"]
                        if any(task["id"] == item["id"] for task in related)
                    ],
                    "effects": [
                        row for row in effects if row.get("message_id") == message_id
                    ],
                    "proxy_events": None if proxy is None else list(proxy.snapshot().get("events") or []),
                }
            )
            previous = related
            previous_effects = effects
        if stop_when is not None and stop_when(current, effects):
            status = "complete"
            break
        if message_complete(current, message_id, effects, proxy, min_ok_ends=1):
            status = "complete"
            break
        time.sleep(0.05)
    return samples, status


def summarize_message(snapshot: dict, effects: list[dict], message_id: str) -> dict:
    tasks = [task for task in snapshot["tasks"] if task.get("message_id") == message_id]
    by_retries: dict[str, list[str]] = {}
    for task in tasks:
        key = str(task.get("retries"))
        by_retries.setdefault(key, []).append(task["id"])
    entries = [row for row in effects if row.get("message_id") == message_id]
    starts = [row for row in entries if row.get("phase") == "handler_start"]
    ends = [row for row in entries if row.get("phase") == "handler_end"]
    per_id_guess: dict[str, dict] = {}
    for task in tasks:
        # Python API has no task_id; bind by (retries, worker, time vs places) as best effort.
        matching_starts = [
            row
            for row in starts
            if row.get("retries") == task.get("retries")
        ]
        matching_ends = [
            row
            for row in ends
            if row.get("retries") == task.get("retries")
        ]
        per_id_guess[task["id"]] = {
            "retries": task.get("retries"),
            "places": task["places"],
            "meta": task["meta"],
            "handler_starts_same_retries": len(matching_starts),
            "handler_ends_same_retries": len(matching_ends),
            "execution_ids_start": [row.get("execution_id") for row in matching_starts],
            "execution_ids_end": [row.get("execution_id") for row in matching_ends],
            "binding": "inferred_by_retries_not_direct_task_id",
        }
    return {
        "message_id": message_id,
        "task_ids": [task["id"] for task in tasks],
        "task_ids_by_retries": by_retries,
        "retries_eq_1_count": len(by_retries.get("1", [])),
        "handler_starts": len(starts),
        "handler_ends": len(ends),
        "execution_ids": sorted({row.get("execution_id") for row in entries if row.get("execution_id")}),
        "per_task": per_id_guess,
        "effects": entries,
        "limitation": (
            "CurrentMessage has no task_id; execution_id↔task_id is inferred from "
            "retries + worker + Redis timeline / proxy enqueue_task_id, not a direct bridge."
        ),
    }


def answers_for_scenario(
    *,
    scenario_id: str,
    summary: dict,
    proxy_snap: dict | None,
    owner_log: str,
    other_log: str,
    final: dict,
    observe_status: str,
) -> dict:
    tasks = summary["per_task"]
    ids = summary["task_ids"]
    retries0 = summary["task_ids_by_retries"].get("0", [])
    retries1 = summary["task_ids_by_retries"].get("1", [])
    a_id = retries0[0] if retries0 else (ids[0] if ids else None)
    b_ids = retries1[:]
    c_extra = retries1[1:] if len(retries1) > 1 else []

    enqueue_events = []
    if proxy_snap:
        enqueue_events = [
            event
            for event in proxy_snap.get("events") or []
            if event.get("kind") == "enqueue"
        ]

    rust_enqueue_seen = None
    if "retry enqueue failed" in owner_log or "leaving the attempt inflight" in owner_log:
        rust_enqueue_seen = "error_leaving_attempt_inflight"
    elif any(event.get("action") == "pass" and event.get("kind") == "enqueue" for event in (proxy_snap or {}).get("events") or []):
        rust_enqueue_seen = "proxy_passed_enqueue"
    elif not enqueue_events and scenario_id == "control_no_fault":
        rust_enqueue_seen = "enqueue_observed_via_monitor_not_proxy"

    unfinished = [
        task_id
        for task_id, info in tasks.items()
        if "done" not in info["places"] and "dead" not in info["places"]
    ]

    return {
        "scenario_id": scenario_id,
        "observe_status": observe_status,
        "B_appeared_in_redis": len(b_ids) > 0,
        "B_task_ids": b_ids,
        "rust_enqueue_result_observed": rust_enqueue_seen,
        "enqueue_proxy_events": enqueue_events,
        "finish_A": {
            "task_id": a_id,
            "places": None if a_id is None else tasks.get(a_id, {}).get("places"),
            "meta": None if a_id is None else tasks.get(a_id, {}).get("meta"),
        },
        "child_exited_hint": any(
            token in owner_log for token in ("Dead", "broken pipe", "timed out", "exited")
        ),
        "owner_log_excerpt": owner_log[-2000:],
        "other_log_excerpt": other_log[-2000:],
        "A_recovered_after_orphan_window": None,  # filled by timeline analysis below
        "C_created": len(c_extra) > 0,
        "C_task_ids": c_extra,
        "retries_eq_1_task_id_count": len(retries1),
        "handler_entries_by_retries": {
            "0": sum(
                1
                for row in summary["effects"]
                if row.get("phase") == "handler_start" and row.get("retries") == 0
            ),
            "1": sum(
                1
                for row in summary["effects"]
                if row.get("phase") == "handler_start" and row.get("retries") == 1
            ),
        },
        "unfinished_task_ids": unfinished,
        "final_workers": final.get("workers"),
    }


def build_timeline(samples: list[dict], proxy_events: list[dict], effects: list[dict]) -> list[dict]:
    events: list[dict] = []
    for sample in samples:
        events.append(
            {
                "source": "observer",
                "t_s": sample.get("t_s"),
                "unix": sample.get("unix"),
                "tasks": [
                    {
                        "id": task["id"],
                        "retries": task.get("retries"),
                        "places": task.get("places"),
                        "status": (task.get("meta") or {}).get("status"),
                        "attempts": (task.get("meta") or {}).get("attempts"),
                    }
                    for task in sample.get("tasks") or []
                ],
            }
        )
    for event in proxy_events:
        events.append(
            {
                "source": "proxy",
                "unix": event.get("t"),
                "action": event.get("action"),
                "kind": event.get("kind"),
                "enqueue_task_id": event.get("enqueue_task_id"),
                "message_id": event.get("message_id"),
                "retries": event.get("retries"),
            }
        )
    for row in effects:
        events.append(
            {
                "source": "handler",
                "unix": row.get("t"),
                "phase": row.get("phase"),
                "execution_id": row.get("execution_id"),
                "retries": row.get("retries"),
                "result": row.get("result"),
                "worker": row.get("worker"),
                "pid": row.get("pid"),
            }
        )
    events.sort(key=lambda item: (item.get("unix") is None, item.get("unix") or 0))
    return events


def wait_workers_registered(observer: Observer, timeout: float = 8.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if observer.inventory()["workers"]:
            return True
        time.sleep(0.1)
    return False


def run_scenario(
    observer: Observer,
    workers: Workers,
    *,
    scenario_id: str,
    fault: dict | None,
    budget_s: float,
    start_second_worker_after_fault: bool,
) -> dict:
    flush_owned(REDIS_PORT, REDIS_DIR)
    pid_start = redis_pid(observer.client)
    effect = OUT / f"{scenario_id}.effect"
    effect.write_text("", encoding="utf-8")
    monitor_path = OUT / f"{scenario_id}.monitor"
    monitor = start_monitor(monitor_path)
    proxy = FaultProxy(PROXY_PORT, REDIS_PORT)
    proxy.start()
    time.sleep(0.15)
    owner = None
    other = None
    sent = None
    samples: list[dict] = []
    observe_status = "incomplete"
    try:
        owner = workers.start(effect, VIA_PROXY, scenario_id)
        if not wait_workers_registered(observer):
            raise RuntimeError("worker did not register")
        # Arm only after worker is up so client send (direct) cannot consume the trigger,
        # and so register/script-load traffic is not the armed target.
        if fault is not None:
            proxy.arm(fault["kind"], fault["mode"], **{k: v for k, v in fault.items() if k not in {"kind", "mode"}})
        token = f"{scenario_id}-{uuid.uuid4().hex[:8]}"
        sent = enqueue_retry_actor(token)
        message_id = sent["message_id"]
        fired = True
        if fault is not None:
            fired = proxy.wait(25)
            if start_second_worker_after_fault:
                # Keep recovery capacity if the first child dies on the broken connection.
                other = workers.start(effect, VIA_PROXY, f"{scenario_id}-recovery")
            proxy.disarm()
        else:
            fired = False

        if fault is not None and not fired:
            observe_status = "incomplete"
            samples = []
            final = observer.inventory()
            effects = read_effects(effect)
            summary = summarize_message(final, effects, message_id)
            proxy_snap = proxy.snapshot()
            owner_log = _log_excerpt(owner)
            other_log = _log_excerpt(other)
            answers = answers_for_scenario(
                scenario_id=scenario_id,
                summary=summary,
                proxy_snap=proxy_snap,
                owner_log=owner_log,
                other_log=other_log,
                final=final,
                observe_status=observe_status,
            )
            answers["fault_required_but_not_fired"] = True
        else:

            def _stop(snap: dict, effects: list[dict]) -> bool:
                return message_complete(snap, message_id, effects, proxy, min_ok_ends=1)

            samples, observe_status = watch_message(
                observer,
                effect,
                message_id,
                budget_s,
                proxy=proxy,
                stop_when=_stop,
            )
            # Extra settle second for late writes.
            time.sleep(1.0)
            final = observer.inventory()
            effects = read_effects(effect)
            summary = summarize_message(final, effects, message_id)
            proxy_snap = proxy.snapshot()
            owner_log = _log_excerpt(owner)
            other_log = _log_excerpt(other)
            answers = answers_for_scenario(
                scenario_id=scenario_id,
                summary=summary,
                proxy_snap=proxy_snap,
                owner_log=owner_log,
                other_log=other_log,
                final=final,
                observe_status=observe_status,
            )
        # Infer A recovery: second handler_start with retries=0 after first, and/or
        # A places moving after orphan window.
        starts0 = [
            row
            for row in summary["effects"]
            if row.get("phase") == "handler_start" and row.get("retries") == 0
        ]
        answers["A_recovered_after_orphan_window"] = len(starts0) >= 2
        answers["A_handler_start_count"] = len(starts0)
        timeline = build_timeline(samples, proxy_snap.get("events") or [], summary["effects"])
        (OUT / f"{scenario_id}.timeline.json").write_text(
            json.dumps(timeline, indent=2, default=str) + "\n", encoding="utf-8"
        )
        (OUT / f"{scenario_id}.summary.json").write_text(
            json.dumps(
                {
                    "answers": answers,
                    "summary": summary,
                    "fault_fired": fired,
                    "observe_status": observe_status,
                },
                indent=2,
                default=str,
            )
            + "\n",
            encoding="utf-8",
        )
        return {
            "scenario_id": scenario_id,
            "redis_pid_start": pid_start,
            "redis_pid_end": redis_pid(observer.client),
            "redis_restarted": False,
            "fault": fault,
            "fault_fired": fired,
            "enqueue": sent,
            "observe_status": observe_status,
            "answers": answers,
            "summary": summary,
            "proxy": proxy_snap,
            "samples": samples,
            "final_tasks": [task for task in final["tasks"] if task.get("message_id") == message_id],
            "owner_log": owner_log,
            "other_log": other_log,
            "monitor": interesting_monitor(monitor_path, observer.addr),
            "timeline_path": str(OUT / f"{scenario_id}.timeline.json"),
            "finished_at": utc_now(),
        }
    finally:
        workers.stop()
        stop_monitor(monitor)
        try:
            proxy.stop()
        except Exception:
            pass


def run_delay_probe(observer: Observer, workers: Workers) -> dict:
    """Separate from fault experiments. One delay value per flush."""
    values = [61, 999, 1000, 1001, 1500]
    rows = []
    for delay_ms in values:
        flush_owned(REDIS_PORT, REDIS_DIR)
        effect = OUT / f"delay-{delay_ms}.effect"
        effect.write_text("", encoding="utf-8")
        owner = workers.start(effect, DIRECT, f"delay-{delay_ms}")
        wait_workers_registered(observer)
        try:
            sent = enqueue_delay_probe(f"delay-{delay_ms}", delay_ms)
            message_id = sent["message_id"]
            samples, status = watch_message(
                observer,
                effect,
                message_id,
                max(8.0, delay_ms / 1000 + 5.0),
                stop_when=lambda snap, effects: message_complete(
                    snap, message_id, effects, None, min_ok_ends=1
                ),
            )
            effects = read_effects(effect)
            starts = [
                row
                for row in effects
                if row.get("phase") == "handler_start" and row.get("message_id") == message_id
            ]
            score = None
            if sent["scheduled"]:
                score = sent["scheduled"][0].get("score")
            # Redis TIME is seconds; scheduled score from apalis is typically ms epoch or secs.
            enqueue_unix = sent["enqueue_unix"]
            handler_unix = starts[0]["t"] if starts else None
            lag_ms = None
            if handler_unix is not None:
                lag_ms = round((handler_unix - enqueue_unix) * 1000, 1)
            score_lag_ms = None
            if score is not None and handler_unix is not None:
                # Heuristic: score may be unix seconds or ms.
                if score > 1e12:
                    score_lag_ms = round(handler_unix * 1000 - score, 1)
                else:
                    score_lag_ms = round((handler_unix - score) * 1000, 1)
            rows.append(
                {
                    "delay_ms": delay_ms,
                    "enqueue_unix": enqueue_unix,
                    "scheduled_score": score,
                    "handler_start_unix": handler_unix,
                    "observed_lag_ms": lag_ms,
                    "score_to_handler_ms": score_lag_ms,
                    "observe_status": status,
                    "effects": effects,
                    "enqueue": sent,
                    "note": (
                        "Single run per value. Separate deadline rounding from poll_interval_ms=500; "
                        "do not claim precision from n=1."
                    ),
                }
            )
        finally:
            workers.stop()
    (OUT / "delay-probe.json").write_text(
        json.dumps({"runs": rows, "finished_at": utc_now()}, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    return {"runs": rows}


def baseline_block(observer: Observer) -> dict:
    from simple_q import Config
    import importlib.metadata as metadata

    cfg = Config.from_env()
    effective = {
        name: getattr(cfg, name)
        for name in dir(cfg)
        if not name.startswith("_") and not callable(getattr(cfg, name))
    }
    redis_info = observer.client.info("server")
    timeout = observer.client.config_get("timeout")
    return {
        "repo_sha": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=str(ROOT), text=True
        ).strip(),
        "simple_q_runtime": metadata.version("simple-q-runtime"),
        "simple_q_worker": metadata.version("simple-q-worker"),
        "apalis_redis": "1.1.1",
        "agent_worker": WORKER,
        "effective_config": effective,
        "orphan_window_s": ORPHAN_S,
        "orphan_formula": "heartbeat_secs * missed_heartbeats = 15 * 4",
        "redis": {
            "port": REDIS_PORT,
            "pid": redis_pid(observer.client),
            "dir": REDIS_DIR,
            "version": redis_info.get("redis_version"),
            "timeout": timeout,
            "appendonly": observer.client.config_get("appendonly"),
            "save": observer.client.config_get("save"),
        },
        "proxy": {"listen": PROXY_PORT, "upstream": REDIS_PORT},
        "client_timeout_note": (
            f"Fault delay_response uses {CLIENT_TIMEOUT_PROBE_S}s hold; "
            "prior PR#5 renew delay showed Database(timed out) within ~2s."
        ),
        "enqueue_boundary": {
            "observed": (
                "Creating the retry task is one atomic EVALSHA enqueue script "
                "(HSETNX data + meta + scheduled|active + signal)."
            ),
            "worker_followup_commands": (
                "After a successful enqueue reply the worker also DELs delivery:{message}:{retries} "
                "and later runs handle_results/finish for A. Those are separate Redis ops. "
                "Losing the enqueue reply closes the proxy client socket, so follow-ups on that "
                "connection do not run. Do not call a dropped-reply case 'enqueue fully applied' "
                "for the whole retry path — only the enqueue script itself is atomic."
            ),
        },
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    ensure_redis(REDIS_PORT, REDIS_DIR)
    observer = Observer(REDIS_PORT)
    workers = Workers()
    baseline = baseline_block(observer)
    (OUT / "baseline.json").write_text(
        json.dumps(baseline, indent=2, default=str) + "\n", encoding="utf-8"
    )
    results: dict[str, Any] = {
        "library": "simple-q 1.2.0",
        "started_at": utc_now(),
        "baseline": baseline,
        "scenarios": {},
    }
    exit_codes: dict[str, int] = {}
    try:
        control = run_scenario(
            observer,
            workers,
            scenario_id="control_no_fault",
            fault=None,
            budget_s=WATCH_BUDGET_CONTROL_S,
            start_second_worker_after_fault=False,
        )
        results["scenarios"]["control_no_fault"] = control
        exit_codes["control_no_fault"] = 0 if control["observe_status"] == "complete" else 2

        for scenario_id, fault in [
            (
                "enqueue_request_dropped",
                {"kind": "enqueue", "mode": "drop_request"},
            ),
            (
                "enqueue_response_dropped",
                {"kind": "enqueue", "mode": "drop_response"},
            ),
            (
                "enqueue_response_delayed",
                {
                    "kind": "enqueue",
                    "mode": "delay_response",
                    "delay_s": CLIENT_TIMEOUT_PROBE_S,
                },
            ),
        ]:
            row = run_scenario(
                observer,
                workers,
                scenario_id=scenario_id,
                fault=fault,
                budget_s=WATCH_BUDGET_FAULT_S,
                start_second_worker_after_fault=True,
            )
            results["scenarios"][scenario_id] = row
            exit_codes[scenario_id] = 0 if row["observe_status"] == "complete" else 2

        delay = run_delay_probe(observer, workers)
        results["delay_probe"] = delay
        exit_codes["delay_probe"] = 0
    finally:
        workers.stop()

    end_pid = redis_pid(observer.client)
    results["redis_pid_end"] = end_pid
    results["redis_restarted"] = end_pid != baseline["redis"]["pid"]
    results["exit_codes"] = exit_codes
    results["finished_at"] = utc_now()
    (OUT / "run.json").write_text(json.dumps(results, indent=2, default=str) + "\n", encoding="utf-8")

    # Compact console summary
    compact = {
        "baseline_sha": baseline["repo_sha"],
        "redis_pid": baseline["redis"]["pid"],
        "redis_restarted": results["redis_restarted"],
        "exit_codes": exit_codes,
        "answers": {
            key: value.get("answers") for key, value in results["scenarios"].items()
        },
        "delay_probe": [
            {
                "delay_ms": row["delay_ms"],
                "observed_lag_ms": row["observed_lag_ms"],
                "score_to_handler_ms": row["score_to_handler_ms"],
                "status": row["observe_status"],
            }
            for row in delay.get("runs", [])
        ],
    }
    print(json.dumps(compact, indent=2, default=str))
    shutdown_save(REDIS_PORT, REDIS_DIR)
    # Non-zero if any scenario incomplete
    return 0 if all(code == 0 for code in exit_codes.values()) else 2


if __name__ == "__main__":
    raise SystemExit(main())
