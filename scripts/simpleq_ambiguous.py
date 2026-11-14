"""Connection-loss cases for simple-q 1.2.0 where the worker cannot tell
whether Redis applied the command.

Redis stays up on 127.0.0.1:6415. Workers talk to a proxy on 6414. The proxy
either closes before forwarding a command, or forwards it, reads the reply,
and closes or holds that reply. This is not a 2400s run and not a Redis restart.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bench.longrun.stand import ensure_redis, flush_owned, shutdown_save
from scripts.redis_fault_proxy import FaultProxy

REDIS_PORT = 6415
PROXY_PORT = 6414
REDIS_DIR = Path("/workspace/results/long/ambiguous-redis-6415")
OUT = ROOT / "results" / "long" / "ambiguous"
WORKER = "/workspace/.venv-sq120/bin/agent-worker"
DIRECT = f"redis://127.0.0.1:{REDIS_PORT}/0"
VIA_PROXY = f"redis://127.0.0.1:{PROXY_PORT}/0"
HEARTBEAT_S = 15
MISSED = 4
ORPHAN_S = HEARTBEAT_S * MISSED
RECOVERY_BUDGET_S = ORPHAN_S + 30
LATE_REPLY_S = ORPHAN_S + 15
PG_DSN = "postgresql://ubuntu@127.0.0.1:5432/longrun"
MOCK_URL = "http://127.0.0.1:6411"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def redis(*args: str) -> str:
    proc = subprocess.run(
        ["redis-cli", "-p", str(REDIS_PORT), *args],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip())
    return proc.stdout.rstrip("\n")


def redis_pid() -> int:
    for line in redis("INFO", "server").splitlines():
        if line.startswith("process_id:"):
            return int(line.split(":", 1)[1])
    raise RuntimeError("redis pid missing")


def resp(*parts: str) -> bytes:
    out = f"*{len(parts)}\r\n".encode()
    for part in parts:
        blob = part.encode()
        out += f"${len(blob)}\r\n".encode() + blob + b"\r\n"
    return out


def talk(payload: bytes, timeout: float = 2) -> bytes:
    sock = socket.create_connection(("127.0.0.1", PROXY_PORT), timeout=timeout)
    try:
        sock.sendall(payload)
        sock.settimeout(timeout)
        chunks = []
        while True:
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                break
            if not chunk:
                break
            chunks.append(chunk)
            if len(b"".join(chunks)) > 16:
                break
        return b"".join(chunks)
    except (ConnectionError, OSError):
        return b""
    finally:
        sock.close()


class Suite:
    def __init__(self) -> None:
        self.proxy = FaultProxy(PROXY_PORT, REDIS_PORT)
        self.workers: list[subprocess.Popen[bytes]] = []
        self.logs: list[object] = []
        self.metrics_port = 6420
        self.results: list[dict] = []
        self.pid = 0

    def run(self) -> int:
        OUT.mkdir(parents=True, exist_ok=True)
        ensure_redis(REDIS_PORT, str(REDIS_DIR))
        self.pid = redis_pid()
        self.proxy.start()
        time.sleep(0.2)
        flush_owned(REDIS_PORT, str(REDIS_DIR))
        steps = [
            self.proxy_self_test,
            self.observe_lease,
            self.claim_request_dropped,
            self.claim_response_dropped,
            self.renew_request_dropped,
            self.renew_response_dropped,
            self.renew_late_reply,
            self.finish_request_dropped,
            self.finish_response_dropped,
            self.retry_request_dropped,
            self.retry_response_dropped,
            self.brief_disconnect,
            self.long_unavailable,
        ]
        only = getattr(self, "_only", None)
        if only:
            steps = [step for step in steps if step.__name__ in only]
        try:
            for step in steps:
                self.isolate(step)
                if step is self.proxy_self_test and not self.results[-1].get("contract_held"):
                    break
        finally:
            self.stop_workers()
        same = {item.get("redis_pid") for item in self.results}
        summary = {
            "class": "connection_loss",
            "counts_as_2400": False,
            "redis_port": REDIS_PORT,
            "proxy_port": PROXY_PORT,
            "redis_pid": self.pid,
            "redis_pid_unchanged": same == {self.pid},
            "orphan_window_s": ORPHAN_S,
            "recovery_budget_s": RECOVERY_BUDGET_S,
            "late_reply_s": LATE_REPLY_S,
            "library": "simple-q",
            "scenarios": [
                {
                    "scenario_id": item["scenario_id"],
                    "status": item["status"],
                    "contract_held": item.get("contract_held"),
                    "checks": item.get("checks", []),
                }
                for item in self.results
            ],
        }
        (OUT / "summary.json").write_text(_dump(summary))
        print(_dump(summary))
        shutdown_save(REDIS_PORT, str(REDIS_DIR))
        incomplete = [item["scenario_id"] for item in self.results if item["status"] != "complete"]
        return 1 if incomplete else 0

    def isolate(self, step) -> None:
        self.stop_workers()
        self.proxy.disarm()
        self.proxy.reset_stats()
        flush_owned(REDIS_PORT, str(REDIS_DIR))
        try:
            result = step()
        except Exception:
            result = {
                "scenario_id": step.__name__,
                "status": "incomplete",
                "error": traceback.format_exc(),
                "redis_pid": _safe_pid(),
            }
        result.setdefault("scenario_id", step.__name__)
        result.setdefault("status", "complete")
        result["redis_pid"] = result.get("redis_pid") or _safe_pid()
        result["finished_at"] = utc_now()
        path = OUT / f"{result['scenario_id']}.json"
        path.write_text(_dump(result))
        self.results.append(result)
        held = result.get("contract_held")
        print(f"{result['scenario_id']}: {result['status']} contract_held={held}", flush=True)

    def proxy_self_test(self) -> dict:
        talk(resp("SET", "proxy-self-test", "1"))
        passed = redis("GET", "proxy-self-test") == "1"
        self.proxy.arm("set", "drop_request")
        dropped = talk(resp("SET", "proxy-self-test", "9"))
        stayed = redis("GET", "proxy-self-test") == "1"
        request_fired = self.proxy.wait(1)
        self.proxy.arm("incr", "drop_response")
        reply = talk(resp("INCR", "proxy-self-test"))
        applied = redis("GET", "proxy-self-test") == "2"
        response_fired = self.proxy.wait(1)
        redis("DEL", "proxy-self-test")
        checks = [
            _check("pass_through", passed, "SET through the proxy is visible on Redis"),
            _check("request_not_applied", request_fired and stayed and not dropped.startswith(b"+"), "SET dropped before forward left the old value"),
            _check("response_applied", response_fired and applied and not reply.startswith(b":"), "INCR ran and the client did not receive the integer"),
            _check("same_pid", redis_pid() == self.pid, str(self.pid)),
        ]
        return _result(
            "proxy_self_test",
            checks,
            arrange="Redis is up. The proxy is a pass-through.",
            act="SET through the proxy, drop the next SET before forward, drop the INCR reply after Redis runs it.",
            assertion="The first SET sticks. The dropped SET does not. The INCR changes the value without a client reply.",
            extra={"dropped_reply": dropped[:40].decode("ascii", "replace"), "incr_reply": reply[:40].decode("ascii", "replace")},
        )

    def observe_lease(self) -> dict:
        token = "observe-lease"
        effect = self._effect(token)
        task_id = self._enqueue("pause", {"token": token, "seconds": 22})
        before_names = set(self._names())
        proc = self._start(effect)
        self._wait_line(effect, f"start {token}", 20)
        first = self._locate(task_id)
        time.sleep(18)
        second = self._locate(task_id)
        self._wait_line(effect, f"end {token}", 15)
        final = self._locate(task_id)
        keys = [line for line in redis("--scan", "--pattern", "bench:*").splitlines() if line]
        inflight = list(first["inflight"])
        counts = self.proxy.snapshot()["counts"]
        locked_moved = (first.get("meta") or {}).get("locked_at") != (second.get("meta") or {}).get("locked_at")
        checks = [
            _check("handler_once", self._lines(effect) == [f"start {token}", f"end {token}"], self._lines(effect)),
            _check("one_place_while_running", len(first["places"]) == 1 and first["places"][0].startswith("bench:inflight:"), first["places"]),
            _check("finished_in_done", final["places"] == ["done"], final["places"]),
            _check("renew_seen", counts.get("seen:renew", 0) > 0, counts),
            _check("locked_at_moved", locked_moved, {"before": first.get("meta"), "after": second.get("meta")}),
            _check(
                "no_generation_suffix",
                bool(inflight) and all(key.count(":") == 2 for key in inflight),
                inflight,
            ),
        ]
        return _result(
            "observe_lease",
            checks,
            arrange="One known task id, one worker, no injected fault.",
            act="Hold the handler for 22s so a 15s heartbeat can run.",
            assertion="The task sits in one inflight set, renew_leases is sent with the task id, and done is the only terminal place.",
            extra={
                "task_id": task_id,
                "message_id": self.message_id,
                "worker_pid": proc.pid,
                "names_before": sorted(before_names),
                "inflight_keys": inflight,
                "keys": keys,
                "counts": counts,
                "first": first,
                "second": second,
                "final": final,
                "log_tail": self._tail(proc),
            },
        )

    def claim_request_dropped(self) -> dict:
        token = "claim-request"
        effect = self._effect(token)
        task_id = self._enqueue("probe", {"token": token})
        queued = self._locate(task_id)
        self.proxy.arm("fetch", "drop_request", once=False)
        proc = self._start(effect)
        fired = self.proxy.wait(20)
        immediate = self._locate(task_id)
        immediate_lines = self._lines(effect)
        self.proxy.disarm()
        owner_alive = proc.poll()
        self.stop_workers()
        self.proxy.disarm()
        restored = self._start(effect)
        self._wait_line(effect, token, 20)
        final = self._locate(task_id)
        checks = [
            _check("fault_fired", fired, self.proxy.snapshot()["events"]),
            _check("was_ready", _ready(queued), queued["places"]),
            _check("command_not_applied", _ready(immediate) and not _inflight(immediate), immediate["places"]),
            _check("not_accepted_locally", immediate_lines == [], immediate_lines),
            _check("still_present", immediate["data"] or immediate["places"], immediate),
            _check("runs_once_after_restore", self._lines(effect) == [token], self._lines(effect)),
            _check("terminal_done", final["places"] == ["done"], final["places"]),
        ]
        return _result(
            "claim_request_dropped",
            checks,
            arrange=f"Task {task_id} is in the ready list. The proxy is armed to drop the next fetch before Redis sees it.",
            act="Start the worker, let the fetch command die, stop that worker, then start a worker with the proxy open.",
            assertion="The task stays ready, the handler does not run, and one later execution finishes it.",
            extra=self._extra(task_id, proc, restored, immediate, final, effect, owner_alive=owner_alive),
        )

    def claim_response_dropped(self) -> dict:
        token = "claim-response"
        effect = self._effect(token)
        task_id = self._enqueue("probe", {"token": token})
        self.proxy.arm(
            "fetch",
            "drop_response",
            require_payload=True,
            payload_contains=task_id.encode(),
        )
        owner = self._start(effect)
        fired = self.proxy.wait(20)
        immediate = self._locate(task_id)
        immediate_lines = self._lines(effect)
        other = self._start(effect)
        samples = self._watch(
            task_id,
            effect,
            RECOVERY_BUDGET_S,
            lambda snap, lines: token in lines and snap.get("places") == ["done"],
        )
        final = samples[-1] if samples else self._locate(task_id)
        refreshed = _score_refreshed(samples, immediate)
        checks = [
            _check("fault_fired", fired, self.proxy.snapshot()["events"]),
            _check("moved_to_inflight", _inflight(immediate) and "active" not in immediate["places"], immediate["places"]),
            _check("not_accepted_locally", immediate_lines == [], immediate_lines),
            _check("not_in_two_places", len(immediate["places"]) == 1, immediate["places"]),
            _check("data_kept", immediate["data"], immediate),
            _check("no_infinite_renew", not refreshed, _scores(samples)),
            _check(
                "recovered_within_budget",
                token in self._lines(effect) and final.get("places") == ["done"],
                {"lines": self._lines(effect), "places": final.get("places"), "wait_s": final.get("t_s")},
            ),
            _check("single_execution", self._lines(effect) == [token], self._lines(effect)),
        ]
        return _result(
            "claim_response_dropped",
            checks,
            arrange=f"Task {task_id} is ready. Two workers. The proxy drops a non-empty fetch reply after Redis has moved the batch.",
            act="The first worker's fetch reply is discarded. A second worker stays up through the orphan window.",
            assertion="The handler does not run on the lost reply. The task remains stored. Heartbeats do not keep refreshing an invisible hold. One execution completes inside the recovery budget.",
            extra=self._extra(task_id, owner, other, immediate, final, effect, samples),
        )

    def renew_request_dropped(self) -> dict:
        return self._renew("renew_request_dropped", "drop_request", False)

    def renew_response_dropped(self) -> dict:
        return self._renew("renew_response_dropped", "drop_response", True)

    def _renew(self, scenario_id: str, mode: str, applied: bool) -> dict:
        token = scenario_id
        effect = self._effect(token)
        task_id = self._enqueue("pause", {"token": token, "seconds": 150})
        owner = self._start(effect)
        self._wait_line(effect, f"start {token}", 20)
        before = self._locate(task_id)
        self.proxy.arm("renew", mode, require_task_ids=True)
        fired = self.proxy.wait(25)
        immediate = self._locate(task_id)
        other = self._start(effect)
        samples = self._watch(task_id, effect, RECOVERY_BUDGET_S, lambda _snap, lines: f"end {token}" in lines)
        final = samples[-1] if samples else self._locate(task_id)
        before_lock = (before.get("meta") or {}).get("locked_at")
        after_lock = (immediate.get("meta") or {}).get("locked_at")
        lock_changed = before_lock != after_lock and after_lock not in {None, ""}
        starts = [line for line in self._lines(effect) if line.startswith("start ")]
        checks = [
            _check("fault_fired", fired, self.proxy.snapshot()["events"]),
            _check("held_before", _inflight(before), before["places"]),
            _check(
                "transport_is_not_an_extension" if not applied else "redis_did_extend",
                (not lock_changed) if not applied else lock_changed,
                {"before": before_lock, "after": after_lock},
            ),
            _check("transport_did_not_drop_ownership", _inflight(immediate), immediate["places"]),
            _check("one_local_start", starts == [f"start {token}"], starts),
            _check(
                "old_owner_stopped_after_budget",
                not _same_owner_still_scoring(samples, before),
                _scores(samples),
            ),
        ]
        return _result(
            scenario_id,
            checks,
            arrange=f"Task {task_id} is in a handler. Two workers. Renewal commands with task ids are the boundary.",
            act=f"The next renew_leases is {mode}. The reply is not treated as a confirmed extension by the worker.",
            assertion="A lost request does not move locked_at. A lost reply can move locked_at. Neither by itself removes the inflight membership, and the old score does not stay fresh for the whole budget.",
            extra=self._extra(task_id, owner, other, immediate, final, effect, samples, before=before),
        )

    def renew_late_reply(self) -> dict:
        token = "renew-late"
        effect = self._effect(token)
        task_id = self._enqueue("pause", {"token": token, "seconds": 180})
        owner = self._start(effect)
        other = self._start(effect)
        self._wait_line(effect, f"start {token}", 20)
        before = self._locate(task_id)
        self.proxy.arm("renew", "delay_response", require_task_ids=True, delay_s=LATE_REPLY_S)
        fired = self.proxy.wait(25)
        samples = self._watch(task_id, effect, LATE_REPLY_S + 20, lambda _snap, _lines: False)
        final = samples[-1] if samples else self._locate(task_id)
        starts = [line for line in self._lines(effect) if line.startswith("start ")]
        released = any(item.get("action") == "release_response" for item in self.proxy.snapshot()["events"])
        owner_key = next((place for place in before["places"] if place.startswith("bench:inflight:")), "")
        moved = any(owner_key and owner_key not in snap["places"] for snap in samples)
        checks = [
            _check("fault_fired", fired, self.proxy.snapshot()["events"]),
            _check("reply_released", released, self.proxy.snapshot()["events"]),
            _check("ownership_moved_before_release", moved, [snap["places"] for snap in samples]),
            _check("late_success_did_not_duplicate", starts == [f"start {token}"], starts),
        ]
        return _result(
            "renew_late_reply",
            checks,
            arrange=f"Task {task_id} is running. A second worker can reap an expired score. The renew reply is held for {LATE_REPLY_S}s.",
            act="Redis applies renew_leases. The worker does not see the reply until after the orphan window. The held success is then delivered.",
            assertion="The late success does not create a second handler, and it does not put the task back under the worker that lost the window.",
            extra=self._extra(task_id, owner, other, before, final, effect, samples),
        )

    def finish_request_dropped(self) -> dict:
        return self._finish("finish_request_dropped", "drop_request", False)

    def finish_response_dropped(self) -> dict:
        return self._finish("finish_response_dropped", "drop_response", True)

    def _finish(self, scenario_id: str, mode: str, applied: bool) -> dict:
        token = scenario_id
        effect = self._effect(token)
        self.proxy.arm("finish", mode)
        task_id = self._enqueue("probe", {"token": token})
        owner = self._start(effect)
        fired = self.proxy.wait(20)
        immediate = self._locate(task_id)
        immediate_lines = list(self._lines(effect))
        other = None
        samples = []
        if not applied:
            self.proxy.disarm()
            other = self._start(effect)
            samples = self._watch(
                task_id,
                effect,
                RECOVERY_BUDGET_S,
                lambda snap, lines: snap.get("places") == ["done"] or lines.count(token) > 1,
            )
        else:
            time.sleep(5)
            samples = [self._locate(task_id)]
        final = samples[-1] if samples else self._locate(task_id)
        lines = self._lines(effect)
        checks = [
            _check("fault_fired", fired, self.proxy.snapshot()["events"]),
            _check("handler_ran", immediate_lines == [token], immediate_lines),
            _check(
                "finish_not_applied" if not applied else "finish_applied",
                ("done" not in immediate["places"]) if not applied else (immediate["places"] == ["done"]),
                immediate["places"],
            ),
            _check("data_kept_if_unfinished", applied or immediate["data"] or immediate["places"], immediate),
            _check("no_extra_attempt", lines == [token], lines),
            _check("ends_done", final.get("places") == ["done"], final.get("places")),
            _check("not_scheduled", "scheduled" not in (final.get("places") or []), final.get("places")),
        ]
        return _result(
            scenario_id,
            checks,
            arrange=f"Task {task_id}. The handler records the token before finish. The proxy arms the first handle_results.",
            act=f"handle_results is {mode}. A second worker is started only when the command was not forwarded.",
            assertion="A lost request leaves the task recoverable and does not mark it done. A lost reply leaves it done, and a repeated finish does not add an execution or a retry.",
            extra=self._extra(task_id, owner, other, immediate, final, effect, samples),
        )

    def retry_request_dropped(self) -> dict:
        return self._retry("retry_request_dropped", "drop_request", False)

    def retry_response_dropped(self) -> dict:
        return self._retry("retry_response_dropped", "drop_response", True)

    def _retry(self, scenario_id: str, mode: str, applied: bool) -> dict:
        token = scenario_id
        effect = self._effect(token)
        self.proxy.arm("finish", mode)
        task_id = self._enqueue("failonce", {"token": token})
        owner = self._start(effect)
        fired = self.proxy.wait(20)
        immediate = self._locate(task_id)
        self.proxy.disarm()
        other = self._start(effect)
        samples = self._watch(
            task_id,
            effect,
            RECOVERY_BUDGET_S,
            lambda snap, _lines: snap.get("places") == ["done"],
        )
        final = samples[-1] if samples else self._locate(task_id)
        lines = self._lines(effect)
        scheduled_now = "scheduled" in immediate["places"] or "active" in immediate["places"]
        checks = [
            _check("fault_fired", fired, self.proxy.snapshot()["events"]),
            _check("first_execution_recorded", lines[:1] == [token] or immediate["places"], {"lines": lines, "places": immediate["places"]}),
            _check(
                "continuation_applied" if applied else "continuation_not_applied",
                scheduled_now if applied else ("scheduled" not in immediate["places"] and "done" not in immediate["places"]),
                immediate["places"],
            ),
            _check("present", bool(immediate["places"]) or immediate["data"], immediate),
            _check("retries_once", lines == [token, token], lines),
            _check("ends_done", final.get("places") == ["done"], final.get("places")),
        ]
        return _result(
            scenario_id,
            checks,
            arrange=f"Task {task_id} fails its first execution and would schedule one retry inside handle_results.",
            act=f"That handle_results is {mode}. The link is then restored and a second worker can take whatever remains.",
            assertion="If the script ran, the retry exists once and a repeated finish does not add another copy. If it did not run, the original task is still recoverable and is not deleted.",
            extra=self._extra(task_id, owner, other, immediate, final, effect, samples),
        )

    def brief_disconnect(self) -> dict:
        return self._blackhole("brief_disconnect", 3, 45)

    def long_unavailable(self) -> dict:
        return self._blackhole("long_unavailable", ORPHAN_S + 10, RECOVERY_BUDGET_S)

    def _blackhole(self, scenario_id: str, seconds: float, watch_s: float) -> dict:
        token = scenario_id
        effect = self._effect(token)
        task_id = self._enqueue("pause", {"token": token, "seconds": 20})
        owner = self._start(effect)
        other = self._start(effect)
        self._wait_line(effect, f"start {token}", 20)
        before = self._locate(task_id)
        self.proxy.blackhole(seconds)
        time.sleep(seconds + 0.5)
        samples = self._watch(
            task_id,
            effect,
            watch_s,
            lambda snap, lines: f"end {token}" in lines and snap.get("places") == ["done"],
        )
        final = samples[-1] if samples else self._locate(task_id)
        starts = [line for line in self._lines(effect) if line.startswith("start ")]
        ends = [line for line in self._lines(effect) if line.startswith("end ")]
        same_owner = seconds < ORPHAN_S
        checks = [
            _check("was_inflight", _inflight(before), before["places"]),
            _check("redis_pid_same", redis_pid() == self.pid, redis_pid()),
            _check("one_start", starts == [f"start {token}"], starts),
            _check("one_end", ends == [f"end {token}"], ends),
            _check("done", final.get("places") == ["done"], final.get("places")),
        ]
        if not same_owner:
            checks.append(
                _check(
                    "recovered_after_long_gap",
                    final.get("places") == ["done"] and starts == [f"start {token}"],
                    {"starts": starts, "places": final.get("places")},
                )
            )
        return _result(
            scenario_id,
            checks,
            arrange=f"Task {task_id} is running on one of two workers. Redis is not restarted.",
            act=f"The proxy closes worker sockets and refuses new ones for {seconds}s, then accepts again.",
            assertion="A gap shorter than the orphan window keeps one owner and one execution. A longer gap still finishes the task once after the link returns.",
            extra=self._extra(task_id, owner, other, before, final, effect, samples),
        )

    def _enqueue(self, op: str, kwargs: dict) -> str:
        os.environ["REDIS_URL"] = DIRECT
        import bench.simpleq_app as app

        before = set(_lines_of(redis("HKEYS", "bench:data")))
        app._client = None
        accepted = asyncio.run(app.enqueue(op, {"kwargs": kwargs}))
        after = set(_lines_of(redis("HKEYS", "bench:data")))
        new = after - before
        if len(new) != 1:
            raise RuntimeError(f"redis task id diff {sorted(before)} -> {sorted(after)} client={accepted}")
        self.message_id = str(accepted.get("id") or "")
        return new.pop()

    def _effect(self, token: str) -> Path:
        path = OUT / f"{token}.effect"
        if path.exists():
            path.unlink()
        return path

    def _start(self, effect: Path) -> subprocess.Popen[bytes]:
        log_path = OUT / f"worker-{effect.stem}-{len(self.workers)}.log"
        handle = open(log_path, "w", encoding="utf-8")
        env = os.environ.copy()
        env.update(
            {
                "PYTHONPATH": str(ROOT),
                "PYTHONUNBUFFERED": "1",
                "REDIS_URL": VIA_PROXY,
                "PG_DSN": PG_DSN,
                "MOCK_URL": MOCK_URL,
                "PROBE_PATH": str(effect),
                "EFFECT_PATH": str(effect),
                "RUST_LOG": "warn",
                "DRAMATIQ_PROM_HOST": "127.0.0.1",
                "DRAMATIQ_PROM_PORT": str(self.metrics_port),
            }
        )
        self.metrics_port += 1
        proc = subprocess.Popen(
            [
                WORKER,
                "worker",
                "--app",
                "bench.simpleq_app:create_app",
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
        self.workers.append(proc)
        self.logs.append(handle)
        return proc

    def stop_workers(self) -> None:
        for proc in self.workers:
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 5
        for proc in self.workers:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
        for handle in self.logs:
            handle.close()
        self.workers.clear()
        self.logs.clear()
        for pid in _worker_pids():
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def _names(self) -> dict[str, str]:
        raw = redis("ZRANGE", "bench:workers", "0", "-1", "WITHSCORES")
        lines = [line for line in raw.splitlines() if line]
        names = {}
        for index in range(0, len(lines), 2):
            names[lines[index]] = lines[index + 1]
        return names

    def _locate(self, task_id: str) -> dict:
        places = []
        if task_id in _lines_of(redis("LRANGE", "bench:active", "0", "-1")):
            places.append("active")
        inflight = {}
        listed = redis("--scan", "--pattern", "bench:inflight:*")
        for key in _lines_of(listed):
            members = _lines_of(redis("SMEMBERS", key))
            inflight[key] = members
            if task_id in members:
                places.append(key)
        for name in ("done", "dead", "scheduled"):
            if _holds(name, task_id):
                places.append(name)
        meta = _hgetall(f"bench:meta:{task_id}")
        return {
            "places": places,
            "inflight": inflight,
            "workers": self._names(),
            "meta": meta,
            "data": redis("HEXISTS", "bench:data", task_id) == "1",
        }

    def _lines(self, path: Path) -> list[str]:
        if not path.exists():
            return []
        return [line.rstrip("\n") for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def _wait_line(self, path: Path, text: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if text in self._lines(path):
                return
            time.sleep(0.2)
        raise RuntimeError(f"timed out waiting for {text!r} in {path}")

    def _watch(self, task_id: str, effect: Path, seconds: float, done) -> list[dict]:
        samples = []
        started = time.monotonic()
        while time.monotonic() - started < seconds:
            snap = self._locate(task_id)
            snap["t_s"] = round(time.monotonic() - started, 3)
            snap["effects"] = self._lines(effect)
            samples.append(snap)
            if done(snap, snap["effects"]):
                break
            time.sleep(2)
        return samples

    def _tail(self, proc: subprocess.Popen[bytes] | None) -> str:
        if proc is None:
            return ""
        path = getattr(proc, "_log_path", "")
        if not path or not os.path.exists(path):
            return ""
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
        return "\n".join(lines[-80:])

    def _extra(
        self,
        task_id,
        owner,
        other,
        immediate,
        final,
        effect,
        samples=None,
        before=None,
        owner_alive=None,
    ) -> dict:
        return {
            "task_id": task_id,
            "message_id": getattr(self, "message_id", ""),
            "owner_alive_after_fault": owner_alive,
            "owner_pid": None if owner is None else owner.pid,
            "other_pid": None if other is None else other.pid,
            "owner_exit": None if owner is None else owner.poll(),
            "other_exit": None if other is None else other.poll(),
            "before": before,
            "immediate": immediate,
            "final": {key: value for key, value in final.items() if key != "inflight"},
            "effects": self._lines(effect),
            "samples": [
                {
                    "t_s": item.get("t_s"),
                    "places": item.get("places"),
                    "workers": item.get("workers"),
                    "locked_at": (item.get("meta") or {}).get("locked_at"),
                    "effects": item.get("effects"),
                }
                for item in (samples or [])
            ],
            "proxy": self.proxy.snapshot(),
            "owner_log": self._tail(owner),
            "other_log": self._tail(other),
        }


def _result(scenario_id: str, checks: list[dict], arrange: str, act: str, assertion: str, extra: dict) -> dict:
    return {
        "scenario_id": scenario_id,
        "status": "complete",
        "class": "connection_loss",
        "counts_as_2400": False,
        "library": "simple-q 1.2.0",
        "started_at": utc_now(),
        "contract_held": all(item["ok"] for item in checks),
        "checks": checks,
        "aaa": {"arrange": arrange, "act": act, "assert": assertion},
        "redis_restarted": False,
        **extra,
    }


def _check(name: str, ok: bool, detail) -> dict:
    return {"name": name, "ok": bool(ok), "detail": detail}


def _lines_of(text: str) -> list[str]:
    return [line for line in text.splitlines() if line and line != "(empty array)"]


def _hgetall(key: str) -> dict[str, str]:
    lines = redis("HGETALL", key).splitlines()
    if len(lines) % 2:
        lines.append("")
    return {lines[index]: lines[index + 1] for index in range(0, len(lines), 2)}


def _worker_pids() -> list[int]:
    proc = subprocess.run(["ps", "-eo", "pid,args"], capture_output=True, text=True, check=False)
    found = []
    for line in proc.stdout.splitlines():
        if "agent-worker.bin" not in line:
            continue
        if "bench.simpleq_app:create_app" not in line and " metrics" not in line:
            continue
        found.append(int(line.split()[0]))
    return found


def _dump(payload: dict) -> str:
    return json.dumps(payload, indent=2, default=_json_default) + "\n"


def _json_default(value):
    if isinstance(value, bytes):
        return value.decode("ascii", "replace")
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _holds(name: str, task_id: str) -> bool:
    key = f"bench:{name}"
    kind = redis("TYPE", key)
    if kind == "zset":
        score = redis("ZSCORE", key, task_id)
        return score not in {"", "(nil)"}
    if kind == "set":
        return redis("SISMEMBER", key, task_id) == "1"
    if kind == "list":
        return task_id in _lines_of(redis("LRANGE", key, "0", "-1"))
    return False


def _inflight(snap: dict) -> bool:
    return any(str(place).startswith("bench:inflight:") for place in snap.get("places") or [])


def _ready(snap: dict) -> bool:
    return "active" in (snap.get("places") or []) or "scheduled" in (snap.get("places") or [])


def _scores(samples: list[dict]) -> list[dict]:
    return [{"t_s": item.get("t_s"), "workers": item.get("workers"), "places": item.get("places")} for item in samples]


def _score_refreshed(samples: list[dict], immediate: dict) -> bool:
    """True when an inflight owner with no local effect keeps getting a newer score."""
    if not samples:
        return False
    owner = next((place for place in immediate.get("places") or [] if place.startswith("bench:inflight:")), "")
    if not owner:
        return False
    name = owner.split("bench:inflight:", 1)[1]
    seen = []
    for item in samples:
        if item.get("effects") or owner not in (item.get("places") or []):
            break
        score = (item.get("workers") or {}).get(name)
        if score is not None:
            seen.append(float(score))
    return len(seen) >= 2 and max(seen) - min(seen) > 10


def _same_owner_still_scoring(samples: list[dict], before: dict) -> bool:
    owner = next((place for place in before.get("places") or [] if place.startswith("bench:inflight:")), "")
    if not owner or not samples:
        return False
    name = owner.split("bench:inflight:", 1)[1]
    last = samples[-1]
    if owner not in (last.get("places") or []):
        return False
    first_score = (samples[0].get("workers") or {}).get(name)
    last_score = (last.get("workers") or {}).get(name)
    if first_score is None or last_score is None:
        return False
    return float(last_score) - float(first_score) > 10


def _safe_pid() -> int | None:
    try:
        return redis_pid()
    except (OSError, RuntimeError, subprocess.TimeoutExpired):
        return None


def main() -> None:
    selected = sys.argv[1:]
    suite = Suite()
    if selected:
        suite._only = set(selected)
    raise SystemExit(suite.run())


if __name__ == "__main__":
    main()
