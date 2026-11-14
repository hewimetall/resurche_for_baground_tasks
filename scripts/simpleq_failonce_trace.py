"""Trace one failonce task with Redis left running.

Two passes, same Redis process:

- control: no proxy fault. One worker, direct connection.
- finish_request_dropped: the first handle_results is closed before Redis
  sees it. A second worker starts only after that drop.

Neither pass restarts Redis. Ports 6379 and 6380 are not used.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import redis

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bench.longrun.stand import ensure_redis, flush_owned, shutdown_save
from scripts.redis_fault_proxy import KNOWN_SHA, FaultProxy

REDIS_PORT = 6416
PROXY_PORT = 6417
REDIS_DIR = "/workspace/results/long/failonce-trace-6416"
OUT = ROOT / "results" / "long" / "failonce-trace"
WORKER = "/workspace/.venv-sq120/bin/agent-worker"
DIRECT = f"redis://127.0.0.1:{REDIS_PORT}/0"
VIA_PROXY = f"redis://127.0.0.1:{PROXY_PORT}/0"
PG_DSN = "postgresql://ubuntu@127.0.0.1:5432/longrun"
MOCK_URL = "http://127.0.0.1:6411"
ORPHAN_S = 15 * 4


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def redis_pid(client: redis.Redis) -> int:
    info = client.info("server")
    return int(info["process_id"])


def _text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _decode_data(raw: str) -> dict | str:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw[:500]
    if isinstance(parsed, dict):
        keep = {
            key: parsed.get(key)
            for key in ("retries", "max_retries", "actor_name", "message_id", "kwargs")
            if key in parsed
        }
        if keep:
            return keep
    return raw[:500]


class Observer:
    def __init__(self, port: int) -> None:
        self.client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
        self.client.client_setname("failonce-observer")
        self.addr = ""
        for row in self.client.client_list():
            if row.get("name") == "failonce-observer":
                self.addr = str(row.get("addr") or "")
                break

    def inventory(self) -> dict:
        ids = sorted(self.client.hkeys("bench:data"))
        inflight: dict[str, list[str]] = {}
        for key in self.client.scan_iter("bench:inflight:*", count=100):
            inflight[_text(key)] = sorted(self.client.smembers(key))
        scheduled = [
            {"id": member, "score": score}
            for member, score in self.client.zrange("bench:scheduled", 0, -1, withscores=True)
        ]
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
        for key in self.client.scan_iter("delivery:*", count=100):
            deliveries[_text(key)] = self.client.get(key)
        tasks = []
        for task_id in ids:
            meta = self.client.hgetall(f"bench:meta:{task_id}")
            raw = self.client.hget("bench:data", task_id) or ""
            places = []
            if any(item["id"] == task_id for item in scheduled):
                places.append("scheduled")
            if task_id in self.client.lrange("bench:active", 0, -1):
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
                    "job": _decode_data(raw),
                }
            )
        return {
            "tasks": tasks,
            "inflight": inflight,
            "scheduled": scheduled,
            "done": done,
            "dead": dead,
            "workers": workers,
            "deliveries": deliveries,
        }


def _changed(previous: dict | None, current: dict) -> bool:
    if previous is None:
        return True
    return json.dumps(previous, sort_keys=True, default=str) != json.dumps(current, sort_keys=True, default=str)


def read_effects(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


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
        if not any(token in upper for token in ("EVAL", "SCRIPT", "ZADD", "SADD", "SREM", "HSET", "DEL", "INCR")):
            continue
        if "bench:" not in line and "delivery:" not in line and "EVAL" not in upper and "SCRIPT" not in upper:
            continue
        kind = None
        for digest, name in KNOWN_SHA.items():
            if digest in line:
                kind = name
                break
        rows.append({"line": line[:500], "kind": kind})
    return rows


class Workers:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen[bytes]] = []
        self.logs: list[object] = []
        self.metrics = 6430

    def start(self, effect: Path, redis_url: str) -> subprocess.Popen[bytes]:
        log_path = OUT / f"worker-{effect.stem}-{len(self.procs)}.log"
        handle = log_path.open("w", encoding="utf-8")
        env = os.environ.copy()
        env.update(
            {
                "PYTHONPATH": str(ROOT),
                "PYTHONUNBUFFERED": "1",
                "REDIS_URL": redis_url,
                "PG_DSN": PG_DSN,
                "MOCK_URL": MOCK_URL,
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
                "bench.failonce_trace_app:create_app",
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


def enqueue(token: str) -> dict:
    os.environ["REDIS_URL"] = DIRECT
    os.environ["PG_DSN"] = PG_DSN
    os.environ["MOCK_URL"] = MOCK_URL
    import bench.failonce_trace_app as app

    app._client = None

    async def _send() -> object:
        return await app.client().send_with_options(app.failonce, kwargs={"token": token})

    before = set(Observer(REDIS_PORT).client.hkeys("bench:data"))
    accepted = asyncio.run(_send())
    after = set(Observer(REDIS_PORT).client.hkeys("bench:data"))
    new = sorted(after - before)
    return {"message_id": None if accepted is None else str(accepted), "task_ids": new}


def watch(observer: Observer, effect: Path, seconds: float, stop_when) -> list[dict]:
    samples = []
    previous = None
    previous_effects = None
    started = time.monotonic()
    while time.monotonic() - started < seconds:
        current = observer.inventory()
        effects = read_effects(effect)
        if _changed(previous, current) or effects != previous_effects:
            samples.append(
                {
                    "t_s": round(time.monotonic() - started, 3),
                    "unix": time.time(),
                    "tasks": current["tasks"],
                    "workers": current["workers"],
                    "deliveries": current["deliveries"],
                    "scheduled": current["scheduled"],
                    "effects": effects,
                }
            )
            previous = current
            previous_effects = effects
        if stop_when(current, effects):
            break
        time.sleep(0.05)
    return samples


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
                "retry",
                "enqueue",
                "outcome",
                "broken",
                "Dead",
                "Error",
                "fail once",
                "ProcessEvents",
                "HeartBeat",
                "fetch_next",
            )
        )
    ]
    return "\n".join(keep[-80:])


def both_terminal(snapshot: dict) -> bool:
    tasks = snapshot["tasks"]
    if len(tasks) < 2:
        return False
    return all("done" in task["places"] or "dead" in task["places"] for task in tasks)


def run_control(observer: Observer, workers: Workers) -> dict:
    flush_owned(REDIS_PORT, REDIS_DIR)
    pid = redis_pid(observer.client)
    effect = OUT / "control.effect"
    effect.write_text("", encoding="utf-8")
    monitor_path = OUT / "control.monitor"
    monitor = start_monitor(monitor_path)
    owner = None
    try:
        owner = workers.start(effect, DIRECT)
        time.sleep(1.5)
        sent = enqueue("failonce-control")
        samples = watch(
            observer,
            effect,
            20,
            lambda snap, effects: both_terminal(snap)
            or (len(effects) >= 2 and all("done" in task["places"] or "dead" in task["places"] for task in snap["tasks"])),
        )
        time.sleep(1.0)
        final = observer.inventory()
        effects = read_effects(effect)
        samples.append(
            {
                "t_s": None,
                "unix": time.time(),
                "tasks": final["tasks"],
                "workers": final["workers"],
                "deliveries": final["deliveries"],
                "scheduled": final["scheduled"],
                "effects": effects,
                "note": "after one extra second",
            }
        )
    finally:
        workers.stop()
        stop_monitor(monitor)
    return {
        "scenario_id": "failonce_control",
        "redis_pid_start": pid,
        "redis_pid_end": redis_pid(observer.client),
        "redis_restarted": False,
        "enqueue": sent,
        "effects": effects,
        "samples": samples,
        "final": final,
        "owner_log": _log_excerpt(owner),
        "monitor": interesting_monitor(monitor_path, observer.addr),
        "finished_at": utc_now(),
    }


def run_dropped_finish(observer: Observer, workers: Workers) -> dict:
    flush_owned(REDIS_PORT, REDIS_DIR)
    pid = redis_pid(observer.client)
    proxy = FaultProxy(PROXY_PORT, REDIS_PORT)
    proxy.start()
    time.sleep(0.2)
    effect = OUT / "finish-request-dropped.effect"
    effect.write_text("", encoding="utf-8")
    monitor_path = OUT / "finish-request-dropped.monitor"
    monitor = start_monitor(monitor_path)
    proxy.arm("finish", "drop_request")
    sent = enqueue("failonce-dropped-finish")
    owner = workers.start(effect, VIA_PROXY)
    fired = proxy.wait(20)
    immediate = observer.inventory()
    immediate_effects = read_effects(effect)
    proxy.disarm()
    other = workers.start(effect, VIA_PROXY)
    started = time.monotonic()
    samples = [
        {
            "t_s": 0.0,
            "unix": time.time(),
            "tasks": immediate["tasks"],
            "workers": immediate["workers"],
            "deliveries": immediate["deliveries"],
            "scheduled": immediate["scheduled"],
            "effects": immediate_effects,
            "note": "immediate after dropped finish",
        }
    ]
    previous = immediate
    previous_effects = immediate_effects
    while time.monotonic() - started < ORPHAN_S + 35:
        current = observer.inventory()
        effects = read_effects(effect)
        if _changed(previous, current) or effects != previous_effects:
            samples.append(
                {
                    "t_s": round(time.monotonic() - started, 3),
                    "unix": time.time(),
                    "tasks": current["tasks"],
                    "workers": current["workers"],
                    "deliveries": current["deliveries"],
                    "scheduled": current["scheduled"],
                    "effects": effects,
                }
            )
            previous = current
            previous_effects = effects
        if both_terminal(current) and len(effects) >= 2:
            break
        time.sleep(0.05)
    final = observer.inventory()
    workers.stop()
    stop_monitor(monitor)
    return {
        "scenario_id": "failonce_finish_request_dropped",
        "redis_pid_start": pid,
        "redis_pid_end": redis_pid(observer.client),
        "redis_restarted": False,
        "fault_fired": fired,
        "proxy": proxy.snapshot(),
        "enqueue": sent,
        "effects": read_effects(effect),
        "samples": samples,
        "final": final,
        "owner_log": _log_excerpt(owner),
        "other_log": _log_excerpt(other),
        "monitor": interesting_monitor(monitor_path, observer.addr),
        "finished_at": utc_now(),
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    ensure_redis(REDIS_PORT, REDIS_DIR)
    observer = Observer(REDIS_PORT)
    pid = redis_pid(observer.client)
    workers = Workers()
    try:
        control = run_control(observer, workers)
        dropped = run_dropped_finish(observer, workers)
    finally:
        workers.stop()
    end_pid = redis_pid(observer.client)
    payload = {
        "library": "simple-q 1.2.0",
        "started_at": utc_now(),
        "redis_port": REDIS_PORT,
        "redis_pid": pid,
        "redis_pid_end": end_pid,
        "redis_restarted": pid != end_pid,
        "observer_addr": observer.addr,
        "control": control,
        "dropped_finish": dropped,
    }
    (OUT / "trace.json").write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "redis_pid": pid,
        "redis_pid_end": end_pid,
        "control_tasks": [
            {"id": task["id"], "places": task["places"], "meta": task["meta"], "job": task["job"]}
            for task in control["final"]["tasks"]
        ],
        "control_effects": control["effects"],
        "dropped_tasks": [
            {"id": task["id"], "places": task["places"], "meta": task["meta"], "job": task["job"]}
            for task in dropped["final"]["tasks"]
        ],
        "dropped_effects": dropped["effects"],
        "dropped_fault": dropped["fault_fired"],
        "sample_counts": {"control": len(control["samples"]), "dropped": len(dropped["samples"])},
    }, indent=2, default=str))
    shutdown_save(REDIS_PORT, REDIS_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
