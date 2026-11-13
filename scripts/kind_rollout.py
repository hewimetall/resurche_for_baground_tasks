#!/usr/bin/env python3
"""Rollout на однонодовом kind.

Redis :6380 и база bench_k8s не пересекаются с процессным стендом.
arq в кластер не ставится: ему нужен другой redis, это уже измерено процессами.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, "/workspace")

import asyncpg

ROOT = Path("/workspace")
RESULTS = ROOT / "results" / "kind"
KUBECONFIG = "/tmp/kind.kubeconfig"
IMAGE = "bench-worker:local"
PG_DSN = "postgresql://ubuntu@127.0.0.1:5432/bench_k8s"
REDIS_LOCAL = "redis://127.0.0.1:6380/0"


def mem_available_mb() -> float:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) / 1024
    return 0.0


def kubectl(args: list[str], timeout: float = 90, check: bool = True) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["KUBECONFIG"] = KUBECONFIG
    proc = subprocess.run(
        ["kubectl", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"kubectl {args} -> {proc.returncode}\n{proc.stderr[-1500:]}\n{proc.stdout[-800:]}")
    return proc


def kubectl_json(args: list[str]) -> dict:
    proc = kubectl([*args, "-o", "json"])
    return json.loads(proc.stdout)


def docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    proc = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=60, check=False)
    if check and proc.returncode != 0:
        raise RuntimeError(proc.stderr[-800:] or proc.stdout[-800:])
    return proc


def gateway() -> str:
    """IPv4 шлюза сети kind. Первый адрес в IPAM часто IPv6, поды до него не ходят."""
    proc = docker("network", "inspect", "kind")
    configs = json.loads(proc.stdout)[0]["IPAM"]["Config"]
    for item in configs:
        address = item.get("Gateway") or ""
        if "." in address:
            return address
    raise RuntimeError(f"нет IPv4 gateway: {configs}")


def parse_ts(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def ensure_socat(host: str) -> None:
    """Проброс только на адрес моста kind, не на 0.0.0.0."""
    spec = [
        (6380, 6380),
        (5432, 5432),
        (8090, 8090),
    ]
    for listen, target in spec:
        pattern = f"TCP-LISTEN:{listen},bind={host},"
        if subprocess.run(["pgrep", "-f", pattern], capture_output=True).returncode == 0:
            continue
        subprocess.Popen(
            [
                "socat",
                f"TCP-LISTEN:{listen},bind={host},fork,reuseaddr",
                f"TCP:127.0.0.1:{target}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    time.sleep(0.3)


def node_mem() -> str:
    proc = docker("stats", "--no-stream", "--format", "{{.MemUsage}}", "bench-control-plane", check=False)
    return (proc.stdout or proc.stderr).strip()


class Sampler:
    def __init__(self, path: Path, scenario: str) -> None:
        self.path = path
        self.scenario = scenario
        self.stop = threading.Event()
        self.peak_host = mem_available_mb()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self.stop.is_set():
            free = mem_available_mb()
            self.peak_host = min(self.peak_host, free)
            row = {"t": time.time(), "scenario": self.scenario, "host_available_mb": round(free, 1), "node": node_mem()}
            try:
                listed = kubectl(["get", "pods", "-l", "app=bench-worker", "-o", "wide"], check=False)
                row["pods"] = listed.stdout
            except Exception as exc:
                row["pods_error"] = str(exc)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row) + "\n")
            self.stop.wait(2)

    def __enter__(self) -> "Sampler":
        self.thread.start()
        return self

    def __exit__(self, *_args) -> None:
        self.stop.set()
        self.thread.join(timeout=3)


def pods() -> list[dict]:
    data = kubectl_json(["get", "pods", "-l", "app=bench-worker"])
    return data.get("items") or []


def summarize_pod(item: dict) -> dict:
    status = item.get("status") or {}
    containers = status.get("containerStatuses") or [{}]
    state = (containers[0].get("state") or {}) if containers else {}
    running = state.get("running") or {}
    terminated = state.get("terminated") or {}
    return {
        "name": item["metadata"]["name"],
        "phase": status.get("phase"),
        "deletion": item["metadata"].get("deletionTimestamp"),
        "started": running.get("startedAt"),
        "finished": terminated.get("finishedAt"),
        "exit": terminated.get("exitCode"),
        "reason": terminated.get("reason") or (state.get("waiting") or {}).get("reason"),
    }


def apply_worker(lib: str, host: str, grace: int, prestop: bool, extra_env: dict[str, str]) -> None:
    command = {
        "simpleq": [
            "agent-worker",
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
            "--child-memory-soft",
            "320M",
            "--child-memory-hard",
            "400M",
        ],
        "dramatiq": [
            "dramatiq",
            "bench.dramatiq_app",
            "--queues",
            "bench",
            "--processes",
            "1",
            "--threads",
            "1",
            "--worker-shutdown-timeout",
            str(grace * 1000),
        ],
        "celery": [
            "celery",
            "-A",
            "bench.celery_app",
            "worker",
            "-Q",
            "bench",
            "-c",
            "1",
            "--loglevel",
            "info",
            "--without-gossip",
            "--without-mingle",
        ],
        "taskiq": [
            "taskiq",
            "worker",
            "bench.taskiq_app:broker",
            "--workers",
            "1",
            "--max-async-tasks",
            "1",
            "--shutdown-timeout",
            str(grace),
            "--ack-type",
            "when_executed",
            "--max-prefetch",
            "0",
        ],
    }[lib]
    env = [
        {"name": "REDIS_URL", "value": f"redis://{host}:6380/0"},
        {"name": "PG_DSN", "value": f"postgresql://ubuntu@{host}:5432/bench_k8s"},
        {"name": "MOCK_URL", "value": f"http://{host}:8090"},
        {"name": "DRAIN_FLAG", "value": "/tmp/draining"},
        {"name": "PYTHONUNBUFFERED", "value": "1"},
        {"name": "PYTHONPATH", "value": "/app"},
        {"name": "PROBE_PATH", "value": "/tmp/bench-probe"},
    ]
    for key, value in extra_env.items():
        env.append({"name": key, "value": value})
    container: dict = {
        "name": "worker",
        "image": IMAGE,
        "imagePullPolicy": "IfNotPresent",
        "command": command,
        "env": env,
        "resources": {
            "requests": {"cpu": "100m", "memory": "192Mi"},
            "limits": {"cpu": "1", "memory": "512Mi"},
        },
    }
    if prestop:
        container["lifecycle"] = {"preStop": {"exec": {"command": ["/bin/sh", "-c", "touch /tmp/draining"]}}}
    manifest = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "bench-worker", "labels": {"app": "bench-worker", "lib": lib}},
        "spec": {
            "replicas": 1,
            "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}},
            "selector": {"matchLabels": {"app": "bench-worker"}},
            "template": {
                "metadata": {"labels": {"app": "bench-worker", "lib": lib}},
                "spec": {
                    "terminationGracePeriodSeconds": grace,
                    "containers": [container],
                },
            },
        },
    }
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"manifest-{lib}.json"
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    kubectl(["apply", "-f", str(path)])


def delete_worker() -> None:
    kubectl(["delete", "deployment", "bench-worker", "--wait=true", "--timeout=45s"], check=False, timeout=60)
    kubectl(["delete", "pod", "-l", "app=bench-worker", "--force", "--grace-period=0"], check=False, timeout=40)


def wait_started(timeout: float = 90) -> dict:
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        for item in pods():
            if item["metadata"].get("deletionTimestamp"):
                continue
            summary = summarize_pod(item)
            if summary["started"]:
                return summary
            last = json.dumps(summary)
        time.sleep(0.2)
    raise TimeoutError("pod did not start: " + last + "\n" + kubectl(["get", "pods", "-o", "wide"], check=False).stdout)


def wait_probe(timeout: float = 70) -> bool:
    token = f"k8s-{time.time_ns()}"
    enqueue_payload = {"op": "probe", "kwargs": {"token": token}}
    sent = enqueue("simpleq", enqueue_payload) if False else None
    # Библиотека передаётся снаружи: токен кладём в файл через enqueue вызывающего кода.
    del sent
    return token


def enqueue(lib: str, payload: dict) -> dict:
    env = os.environ.copy()
    env.update(
        {
            "REDIS_URL": REDIS_LOCAL,
            "PG_DSN": PG_DSN,
            "MOCK_URL": "http://127.0.0.1:8090",
            "PYTHONPATH": "/workspace",
        }
    )
    proc = subprocess.run(
        ["/workspace/.venv/bin/python", "-m", "bench.cli", "enqueue", "--lib", lib, "--payload", json.dumps(payload)],
        cwd="/workspace",
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or "")[-1500:] + (proc.stdout or "")[-500:])
    lines = [line for line in proc.stdout.splitlines() if line.strip().startswith("{")]
    if not lines:
        raise RuntimeError("enqueue без json: " + proc.stdout[-400:])
    return json.loads(lines[-1])


def flush_queue() -> None:
    subprocess.run(["redis-cli", "-p", "6380", "flushdb"], check=False, capture_output=True)


def rollout_and_watch(grace: int) -> dict:
    before = {item["metadata"]["name"]: summarize_pod(item) for item in pods() if not item["metadata"].get("deletionTimestamp")}
    env = os.environ.copy()
    env["KUBECONFIG"] = KUBECONFIG
    started = time.time()
    status = subprocess.Popen(
        ["kubectl", "rollout", "restart", "deployment/bench-worker"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    status.wait(timeout=30)
    restart_rc = status.returncode
    watch = subprocess.Popen(
        ["kubectl", "rollout", "status", "deployment/bench-worker", "--timeout=150s"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    old_last: dict[str, dict] = {}
    deadline = time.time() + grace + 40
    while time.time() < deadline:
        current = pods()
        names = set()
        for item in current:
            name = item["metadata"]["name"]
            names.add(name)
            if name in before:
                old_last[name] = summarize_pod(item)
        if before and set(before).isdisjoint(names):
            break
        if watch.poll() is not None and set(before).isdisjoint(names):
            break
        time.sleep(0.3)
    try:
        out, _ = watch.communicate(timeout=grace + 20)
    except subprocess.TimeoutExpired:
        watch.kill()
        out, _ = watch.communicate()
    finished = time.time()
    new_pods = []
    for item in pods():
        if item["metadata"]["name"] not in before:
            new_pods.append(summarize_pod(item))
    return {
        "restart_rc": restart_rc,
        "rollout_status_s": round(finished - started, 3),
        "rollout_rc": watch.returncode,
        "rollout_out": (out or "")[-500:],
        "old": list(before.values()),
        "old_last": list(old_last.values()),
        "new": new_pods,
        "restart_at": started,
    }


def timings(info: dict, grace: int | None = None) -> dict:
    """deletionTimestamp в Kubernetes — дедлайн grace, не момент SIGTERM.

    Момент сигнала ≈ deletionTimestamp − terminationGracePeriodSeconds.
    T_drain — от этого момента до finishedAt. Поле t_drain_raw_s хранит
    finishedAt − deletionTimestamp до вычитания grace, чтобы сырой JSON
    не скрывал формулу.
    """
    old = (info.get("old_last") or info.get("old") or [{}])[0]
    deletion = parse_ts(old.get("deletion"))
    finished = parse_ts(old.get("finished"))
    restart = info.get("restart_at")
    raw = None
    drain = None
    sigterm = deletion or restart
    if deletion and finished:
        raw = round(finished - deletion, 3)
        if grace:
            sigterm = deletion - grace
            drain = round(finished - sigterm, 3)
        else:
            drain = raw
    elif restart and finished:
        drain = round(finished - restart, 3)
    return {
        "t_drain_s": drain,
        "t_drain_raw_s": raw,
        "deletion_at": deletion,
        "finished_at": finished,
        "exit": old.get("exit"),
        "reason": old.get("reason"),
        "sigterm_at": sigterm,
    }


async def prepare(conn: asyncpg.Connection, job_id: str) -> None:
    await conn.execute(
        "insert into jobs (id, status) values ($1, 'running') on conflict (id) do update set status = 'running'",
        job_id,
    )


async def wait_attempt(conn: asyncpg.Connection, job_id: str, timeout: float) -> int:
    deadline = time.time() + timeout
    count = 0
    while time.time() < deadline:
        count = await conn.fetchval("select count(*) from attempts where job_id = $1", job_id)
        if count:
            return count
        await asyncio_sleep(0.3)
    return count


async def asyncio_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


async def wait_progress(conn: asyncpg.Connection, job_id: str, steps: int, timeout: float) -> int:
    deadline = time.time() + timeout
    count = 0
    while time.time() < deadline:
        count = await conn.fetchval("select count(*) from progress where job_id = $1", job_id)
        if count >= steps:
            return count
        await asyncio_sleep(0.4)
    return count


async def snapshot_job(conn: asyncpg.Connection, job_id: str, sigterm_at: float | None) -> dict:
    attempts = await conn.fetch(
        "select note, worker, pid, extract(epoch from started_at) as ts from attempts where job_id = $1 order by id",
        job_id,
    )
    progress = await conn.fetchval("select count(*) from progress where job_id = $1", job_id)
    gap = None
    if sigterm_at is not None:
        row = await conn.fetchrow(
            """
            select extract(epoch from min(started_at)) - $2 as gap, count(*) as n
            from attempts where job_id = $1 and started_at > to_timestamp($2)
            """,
            job_id,
            sigterm_at,
        )
        if row and row["gap"] is not None:
            gap = {"gap_s": float(row["gap"]), "starts_after": row["n"]}
        else:
            gap = {"gap_s": None, "starts_after": 0}
    return {
        "progress": progress,
        "attempts": [dict(row) for row in attempts],
        "gap": gap,
    }


def logs() -> str:
    proc = kubectl(["logs", "deployment/bench-worker", "--tail=60", "--all-containers"], check=False, timeout=20)
    return ((proc.stdout or "") + (proc.stderr or ""))[-2000:]


def connectivity(host: str) -> str:
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "netcheck"},
        "spec": {
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "netcheck",
                    "image": IMAGE,
                    "imagePullPolicy": "IfNotPresent",
                    "command": [
                        "python",
                        "-c",
                        (
                            "import socket\n"
                            f"for port in (6380, 5432, 8090):\n"
                            f"    socket.create_connection(('{host}', port), 4).close()\n"
                            "print('net_ok')\n"
                        ),
                    ],
                }
            ],
        },
    }
    path = RESULTS / "netcheck.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    kubectl(["delete", "pod", "netcheck", "--force", "--grace-period=0"], check=False, timeout=20)
    kubectl(["apply", "-f", str(path)])
    deadline = time.time() + 60
    while time.time() < deadline:
        item = kubectl_json(["get", "pod", "netcheck"])
        phase = item.get("status", {}).get("phase")
        if phase in {"Succeeded", "Failed"}:
            text = kubectl(["logs", "pod/netcheck"], check=False).stdout
            kubectl(["delete", "pod", "netcheck", "--force", "--grace-period=0"], check=False, timeout=20)
            if phase != "Succeeded":
                raise RuntimeError(f"нет маршрута до хоста {host}: {phase} {text}")
            return text.strip()
        time.sleep(0.5)
    raise TimeoutError(kubectl(["describe", "pod", "netcheck"], check=False).stdout[-1500:])


def _restart() -> float:
    env = os.environ.copy()
    env["KUBECONFIG"] = KUBECONFIG
    started = time.time()
    proc = subprocess.run(
        ["kubectl", "rollout", "restart", "deployment/bench-worker"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-800:] or proc.stdout[-400:])
    return started


def _watch_until(names: set[str], grace: int) -> dict[str, dict]:
    old_last: dict[str, dict] = {}
    deadline = time.time() + grace + 25
    while time.time() < deadline and names:
        alive = set()
        for item in pods():
            name = item["metadata"]["name"]
            if name in names:
                old_last[name] = summarize_pod(item)
                alive.add(name)
        if names.isdisjoint(alive):
            break
        time.sleep(0.2)
    return old_last


async def scenario_early(conn: asyncpg.Connection, lib: str, host: str) -> dict:
    """Два rollout подряд. Второй SIGTERM — в первые секунды нового пода."""
    grace = 20
    flush_queue()
    apply_worker(lib, host, grace, prestop=False, extra_env=_lease_env(lib))
    first = wait_started()
    started = parse_ts(first["started"]) or time.time()
    delay = 1.0 - (time.time() - started)
    if delay > 0:
        time.sleep(delay)
    age1 = time.time() - started
    with Sampler(RESULTS / f"admin-{lib}-early.jsonl", "early"):
        names1 = {item["metadata"]["name"] for item in pods() if not item["metadata"].get("deletionTimestamp")}
        t1 = _restart()
        # Новое поколение ловим по startedAt и сразу шлём второй SIGTERM.
        second = None
        deadline = time.time() + 40
        while time.time() < deadline:
            for item in pods():
                if item["metadata"]["name"] in names1 or item["metadata"].get("deletionTimestamp"):
                    continue
                summary = summarize_pod(item)
                if summary["started"]:
                    second = summary
                    break
            if second:
                break
            time.sleep(0.15)
        age2 = None
        t2 = None
        names2: set[str] = set()
        if second:
            started2 = parse_ts(second["started"]) or time.time()
            hold = 1.0 - (time.time() - started2)
            if hold > 0:
                time.sleep(hold)
            age2 = time.time() - started2
            names2 = {item["metadata"]["name"] for item in pods() if item["metadata"]["name"] not in names1 and not item["metadata"].get("deletionTimestamp")}
            t2 = _restart()
        last1 = _watch_until(names1, grace)
        last2 = _watch_until(names2, grace) if names2 else {}
        env = os.environ.copy()
        env["KUBECONFIG"] = KUBECONFIG
        status = subprocess.run(
            ["kubectl", "rollout", "status", "deployment/bench-worker", "--timeout=90s"],
            env=env,
            capture_output=True,
            text=True,
            timeout=100,
            check=False,
        )
    def _one(last: dict, stamp: float | None) -> dict:
        info = {"old_last": list(last.values()), "restart_at": stamp}
        return timings(info, grace)

    return {
        "lib": lib,
        "scenario": "early",
        "grace_s": grace,
        "age_at_first_restart_s": round(age1, 3),
        "first": _one(last1, t1),
        "age_at_second_restart_s": None if age2 is None else round(age2, 3),
        "second": None if t2 is None else _one(last2, t2),
        "rollout_rc": status.returncode,
        "rollout_out": (status.stdout or status.stderr or "")[-400:],
    }


async def scenario_checkpoint(conn: asyncpg.Connection, lib: str, host: str) -> dict:
    grace = 40
    steps, step_s = 6, 2.0
    job_id = f"k8s-{lib}-on"
    flush_queue()
    await prepare(conn, job_id)
    apply_worker(lib, host, grace, prestop=True, extra_env=_lease_env(lib))
    wait_started()
    _wait_consume(lib)
    enqueue(lib, {"op": "longjob", "kwargs": {"job_id": job_id, "steps": steps, "step_s": step_s}, "unique_key": f"long:{job_id}"})
    got = await wait_attempt(conn, job_id, 40)
    if not got:
        return {"lib": lib, "scenario": "checkpoint", "error": "задача не стартовала", "logs": logs()}
    await asyncio_sleep(3)
    with Sampler(RESULTS / f"admin-{lib}-checkpoint.jsonl", "checkpoint"):
        info = rollout_and_watch(grace)
    times = timings(info, grace)
    progress = await wait_progress(conn, job_id, steps, timeout=steps * step_s + 50)
    job = await snapshot_job(conn, job_id, times["sigterm_at"])
    return {
        "lib": lib,
        "scenario": "checkpoint",
        "grace_s": grace,
        "steps": steps,
        "started_before": got,
        "progress_after": progress,
        **times,
        "job": job,
        "rollout": _public(info),
    }


async def scenario_kill(conn: asyncpg.Connection, lib: str, host: str) -> dict:
    grace = 12
    job_id = f"k8s-{lib}-off"
    flush_queue()
    await prepare(conn, job_id)
    apply_worker(lib, host, grace, prestop=False, extra_env=_lease_env(lib))
    wait_started()
    _wait_consume(lib)
    enqueue(lib, {"op": "blocker", "kwargs": {"job_id": job_id, "seconds": 180}})
    got = await wait_attempt(conn, job_id, 40)
    if not got:
        return {"lib": lib, "scenario": "kill", "error": "blocker не стартовал", "logs": logs()}
    await asyncio_sleep(1)
    with Sampler(RESULTS / f"admin-{lib}-kill.jsonl", "kill"):
        info = rollout_and_watch(grace)
    times = timings(info, grace)
    # Ждём повторную доставку, но не дольше окна lease + запас.
    deadline = time.time() + 75
    while time.time() < deadline:
        count = await conn.fetchval("select count(*) from attempts where job_id = $1", job_id)
        if count and count >= 2:
            break
        await asyncio_sleep(1)
    job = await snapshot_job(conn, job_id, times["sigterm_at"])
    return {
        "lib": lib,
        "scenario": "kill",
        "grace_s": grace,
        "started_before": got,
        **times,
        "job": job,
        "rollout": _public(info),
        "node": node_mem(),
    }


def _lease_env(lib: str) -> dict[str, str]:
    if lib == "simpleq":
        return {"SIMPLE_Q_HEARTBEAT_SECS": "5", "SIMPLE_Q_MISSED_HEARTBEATS": "3"}
    if lib == "dramatiq":
        return {"DRAMATIQ_HEARTBEAT_MS": "15000"}
    if lib == "celery":
        return {"CELERY_VISIBILITY_TIMEOUT": "30", "CELERY_SOFT_SHUTDOWN": "12"}
    if lib == "taskiq":
        return {"TASKIQ_IDLE_MS": "25000"}
    return {}


def _wait_consume(lib: str) -> None:
    token = f"ready-{lib}-{time.time_ns()}"
    enqueue(lib, {"op": "probe", "kwargs": {"token": token}})
    deadline = time.time() + 50
    while time.time() < deadline:
        proc = kubectl(
            ["exec", "deployment/bench-worker", "--", "python", "-c", "print(open('/tmp/bench-probe').read())"],
            check=False,
            timeout=15,
        )
        if token in (proc.stdout or ""):
            return
        time.sleep(0.5)
    raise TimeoutError("воркер не забрал probe\n" + logs())


def _public(info: dict) -> dict:
    skip = {"restart_at"}
    return {key: value for key, value in info.items() if key not in skip}


async def main() -> None:
    import asyncio

    if mem_available_mb() < 900:
        raise SystemExit(f"мало памяти: {mem_available_mb():.0f} MB")
    RESULTS.mkdir(parents=True, exist_ok=True)
    host = gateway()
    ensure_socat(host)
    conn = await asyncpg.connect(PG_DSN)
    from bench import db

    await db.ensure_schema(conn)
    net = connectivity(host)
    print("net", net, "gateway", host, flush=True)
    libs = ["simpleq", "dramatiq", "celery", "taskiq"]
    report: dict = {
        "gateway": host,
        "image": IMAGE,
        "node_cap": "3Gi",
        "redis": "127.0.0.1:6380 via socat on the kind bridge",
        "postgres": "bench_k8s",
        "arq": "не запускался в kind: arq 0.28 требует redis<6, образ собран на redis 8",
        "scenarios": [],
    }
    try:
        for lib in libs:
            if mem_available_mb() < 800:
                report["scenarios"].append({"lib": lib, "error": "остановлено: свободно меньше 800 MB"})
                break
            for runner in (scenario_early, scenario_checkpoint, scenario_kill):
                if mem_available_mb() < 800:
                    report["scenarios"].append({"lib": lib, "scenario": runner.__name__, "error": "память"})
                    break
                print(f"RUN {lib} {runner.__name__} free={mem_available_mb():.0f}MB", flush=True)
                try:
                    row = await runner(conn, lib, host)
                except Exception as exc:
                    row = {"lib": lib, "scenario": runner.__name__, "error": str(exc)[-1500:], "logs": logs()[-1000:]}
                report["scenarios"].append(row)
                print(json.dumps({k: row.get(k) for k in ("lib", "scenario", "t_drain_s", "exit", "error")}, ensure_ascii=False), flush=True)
                delete_worker()
                await asyncio.sleep(1)
    finally:
        delete_worker()
        await conn.close()
        out = RESULTS / "rollout.json"
        out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print("wrote", out, flush=True)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
