#!/usr/bin/env python3
"""Сценарии стенда. Одна библиотека за раз. Сырые JSON — в results/."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import asyncio
import json
import os
import signal
import subprocess
import threading
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import psutil

from bench import db
from bench.workers import command, python_for

ROOT = Path("/workspace")
RESULTS = ROOT / "results"
LOGS = RESULTS / "logs"
REDIS_URL = "redis://127.0.0.1:6379/0"
PG_DSN = "postgresql://ubuntu@127.0.0.1:5432/bench"
MOCK = "http://127.0.0.1:8090"
LIBS = ["simpleq", "dramatiq", "celery", "arq", "taskiq"]


def scale_params() -> dict:
    name = os.environ.get("BENCH_SCALE", "short")
    table = {
        "short": dict(
            name="short", batch=8, fast=80, service=16, async_n=16, async_s=0.25, async_c=4,
            block=4, long_steps=4, long_step=1.0, grace=8, infinite_grace=6, redis_down=5,
            sigkill_sleep=6,
        ),
        "medium": dict(
            name="medium", batch=40, fast=2000, service=300, async_n=80, async_s=0.5, async_c=8,
            block=25, long_steps=12, long_step=5.0, grace=20, infinite_grace=15, redis_down=15,
            sigkill_sleep=12,
        ),
        "full": dict(
            name="full", batch=100, fast=10000, service=2000, async_n=1000, async_s=1.0, async_c=8,
            block=90, long_steps=60, long_step=5.0, grace=60, infinite_grace=30, redis_down=30,
            sigkill_sleep=20,
        ),
    }
    if name not in table:
        raise SystemExit(f"BENCH_SCALE must be one of {list(table)}")
    return table[name]


def mem_available_mb() -> float:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) / 1024
    return 0


def base_env(extra: dict | None = None) -> dict:
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": "/workspace",
            "REDIS_URL": REDIS_URL,
            "PG_DSN": PG_DSN,
            "MOCK_URL": MOCK,
            "PYTHONUNBUFFERED": "1",
        }
    )
    if extra:
        env.update({k: str(v) for k, v in extra.items()})
    return env


def http_json(method: str, path: str, body: dict | None = None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(MOCK + path, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode())


class Worker:
    def __init__(self, proc: subprocess.Popen, log_path: Path, started: float):
        self.proc = proc
        self.log_path = log_path
        self.started = started

    @property
    def pid(self) -> int:
        return self.proc.pid

    def alive(self) -> bool:
        return self.proc.poll() is None

    def stop(self, grace: float) -> dict:
        if not self.alive():
            return {
                "sigterm_at": None,
                "exit_at": time.time(),
                "t_drain_s": 0,
                "killed_after_grace": False,
                "code": self.proc.returncode,
                "already_exited": True,
            }
        sigterm_at = time.time()
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return {"sigterm_at": sigterm_at, "t_drain_s": 0, "code": self.proc.poll(), "killed_after_grace": False}
        deadline = time.time() + grace
        while time.time() < deadline and self.alive():
            time.sleep(0.2)
        killed = False
        if self.alive():
            killed = True
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
        else:
            self.proc.wait(timeout=5)
        exit_at = time.time()
        return {
            "sigterm_at": sigterm_at,
            "exit_at": exit_at,
            "t_drain_s": round(exit_at - sigterm_at, 3),
            "killed_after_grace": killed,
            "code": self.proc.returncode,
            "already_exited": False,
        }


class Suite:
    def __init__(self, conn: asyncpg.Connection, params: dict):
        self.conn = conn
        self.params = params
        self.workers: list[Worker] = []
        self.lib = ""
        self.env: dict = {}
        self.generation = 0

    def log(self, text: str) -> None:
        print(text, flush=True)

    async def reset_state(self) -> None:
        self.stop_all(grace=2)
        subprocess.run(["redis-cli", "flushall"], check=False, capture_output=True)
        await db.reset(self.conn)
        try:
            http_json("POST", "/reset", {})
        except Exception:
            pass
        for path in Path("/tmp").glob("drain-*"):
            path.unlink(missing_ok=True)
        for path in Path("/tmp").glob("lat-*.txt"):
            path.unlink(missing_ok=True)
        for path in Path("/tmp").glob("bench-probe-*"):
            path.unlink(missing_ok=True)

    def stop_all(self, grace: float) -> list[dict]:
        info = [worker.stop(grace) for worker in self.workers]
        self.workers.clear()
        return info

    def start(self, opts: dict | None = None, env: dict | None = None) -> list[Worker]:
        opts = dict(opts or {})
        self.generation += 1
        probe = f"/tmp/bench-probe-{self.lib}-{self.generation}"
        Path(probe).write_text("")
        extra = {"PROBE_PATH": probe, "LAT_PATH": f"/tmp/lat-{self.lib}.txt"}
        if env:
            extra.update(env)
        if self.lib == "taskiq":
            extra.setdefault("TASKIQ_METRICS", "1")
        if self.lib == "celery" and opts.get("max_tasks"):
            extra["CELERY_MAX_TASKS"] = str(opts["max_tasks"])
        if self.lib == "celery" and opts.get("mem_kb"):
            extra["CELERY_MAX_MEMORY_KB"] = str(opts["mem_kb"])
        self.env = base_env(extra)
        count = 1
        if self.lib == "arq":
            count = int(opts.get("processes", 1))
            opts = dict(opts)
            opts["processes"] = 1
        cmd = command(self.lib, opts)
        started = []
        LOGS.mkdir(parents=True, exist_ok=True)
        for index in range(count):
            log_path = LOGS / f"{self.lib}-g{self.generation}-{index}.log"
            handle = open(log_path, "ab")
            proc = subprocess.Popen(
                cmd,
                cwd="/workspace",
                env=self.env,
                stdout=handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            worker = Worker(proc, log_path, time.time())
            self.workers.append(worker)
            started.append(worker)
        return started

    def enqueue(self, payload: dict, timeout: float = 60) -> dict:
        proc = subprocess.run(
            [python_for(self.lib), "-m", "bench.cli", "enqueue", "--lib", self.lib, "--payload", json.dumps(payload)],
            cwd="/workspace",
            env=self.env or base_env(),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or "")[-2000:] + "\n" + (proc.stdout or "")[-800:])
        lines = [line for line in proc.stdout.splitlines() if line.strip().startswith("{")]
        if not lines:
            raise RuntimeError("no json from enqueue: " + proc.stdout[-500:] + proc.stderr[-500:])
        return json.loads(lines[-1])

    def enqueue_many(self, payload: dict, timeout: float = 180) -> dict:
        proc = subprocess.run(
            [python_for(self.lib), "-m", "bench.cli", "many", "--lib", self.lib, "--payload", json.dumps(payload)],
            cwd="/workspace",
            env=self.env or base_env(),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or "")[-2000:] + "\n" + (proc.stdout or "")[-800:])
        lines = [line for line in proc.stdout.splitlines() if line.strip().startswith("{")]
        return json.loads(lines[-1])

    def admin(self) -> dict:
        proc = subprocess.run(
            [python_for(self.lib), "-m", "bench.cli", "admin", "--lib", self.lib],
            cwd="/workspace",
            env=self.env or base_env(),
            capture_output=True,
            text=True,
            timeout=15,
        )
        lines = [line for line in (proc.stdout or "").splitlines() if line.strip().startswith("{")]
        if not lines:
            return {"error": (proc.stderr or proc.stdout or "")[-500:], "code": proc.returncode}
        try:
            return json.loads(lines[-1])
        except json.JSONDecodeError:
            return {"error": lines[-1][:500]}

    def wait_ready(self, timeout: float = 40) -> bool:
        probe = self.env["PROBE_PATH"]
        token = f"ready-{self.generation}-{time.time()}"
        deadline = time.time() + timeout
        try:
            self.enqueue({"op": "probe", "kwargs": {"token": token}})
        except Exception as exc:
            self.log(f"probe enqueue failed: {exc}")
            return False
        while time.time() < deadline:
            if token in Path(probe).read_text(errors="ignore"):
                return True
            if self.workers and not any(worker.alive() for worker in self.workers):
                return False
            time.sleep(0.3)
        return False

    async def prepare(self, job_id: str, n: int | None = None, items: list[int] | None = None) -> None:
        await self.conn.execute(
            "insert into jobs (id, expected_n) values ($1, $2) on conflict (id) do update set expected_n = $2",
            job_id,
            n,
        )
        if items:
            await self.conn.executemany(
                "insert into expected (job_id, item) values ($1, $2) on conflict do nothing",
                [(job_id, item) for item in items],
            )

    async def wait_status(self, job_id: str, status: str, timeout: float) -> str | None:
        deadline = time.time() + timeout
        seen = None
        while time.time() < deadline:
            seen = await self.conn.fetchval("select status from jobs where id = $1", job_id)
            if seen == status:
                return seen
            await asyncio.sleep(0.3)
        return seen

    async def wait_progress(self, job_id: str, steps: int, timeout: float) -> int:
        deadline = time.time() + timeout
        count = 0
        while time.time() < deadline:
            count = await self.conn.fetchval("select count(*) from progress where job_id = $1", job_id)
            if count >= steps:
                return count
            await asyncio.sleep(0.4)
        return count

    async def wait_results(self, job_id: str, n: int, timeout: float) -> int:
        deadline = time.time() + timeout
        count = 0
        while time.time() < deadline:
            count = await self.conn.fetchval("select count(*) from results where job_id = $1", job_id)
            if count >= n:
                return count
            await asyncio.sleep(0.3)
        return count

    def touch_drain(self) -> str:
        flag = self.env.get("DRAIN_FLAG", "/tmp/draining")
        Path(flag).write_text("1")
        return flag

    def rss(self) -> int:
        total = 0
        for worker in self.workers:
            try:
                proc = psutil.Process(worker.pid)
                total += proc.memory_info().rss
                for child in proc.children(recursive=True):
                    try:
                        total += child.memory_info().rss
                    except psutil.Error:
                        pass
            except psutil.Error:
                pass
        return total

    def tail(self, limit: int = 40) -> str:
        if not self.workers:
            return ""
        text = self.workers[-1].log_path.read_text(errors="ignore")
        return "\n".join(text.splitlines()[-limit:])


def job_id(lib: str, scenario: str) -> str:
    return f"{scenario}-{lib}-{int(time.time())}"


async def run_one(suite: Suite, lib: str, scenario: str) -> dict:
    if mem_available_mb() < 800:
        return {"library": lib, "scenario": scenario, "measured": False, "error": "MemAvailable < 800MB, прогон остановлен"}
    suite.lib = lib
    await suite.reset_state()
    started = time.time()
    result = {
        "library": lib,
        "scenario": scenario,
        "scale": suite.params["name"],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "measured": False,
    }
    try:
        body = await SCENARIOS[scenario](suite, lib)
        result.update(body)
        result["measured"] = body.get("measured", True)
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        result["traceback"] = traceback.format_exc()[-4000:]
        result["log_tail"] = suite.tail()
        result["measured"] = False
    finally:
        if suite.workers:
            result.setdefault("stop", suite.stop_all(grace=3))
    result["duration_s"] = round(time.time() - started, 3)
    out = RESULTS / lib / f"{scenario}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, default=str))
    suite.log(
        f"{lib} {scenario} measured={result['measured']} "
        f"dur={result['duration_s']} err={result.get('error', '')[:180]}"
    )
    return result


async def smoke(suite: Suite, lib: str) -> dict:
    n = suite.params["batch"]
    jid = job_id(lib, "smoke")
    await suite.prepare(jid, n, list(range(n)))
    suite.start({"processes": 2, "concurrency": 2})
    ready = suite.wait_ready()
    sent = suite.enqueue({"op": "fanout", "kwargs": {"job_id": jid, "n": n}})
    got = await suite.wait_results(jid, n, timeout=60)
    inv = await db.invariant(suite.conn, jid)
    mock = http_json("GET", "/stats")
    return {"ready": ready, "send": sent, "got": got, "invariant": inv, "mock": mock, "rss": suite.rss()}


async def dedup(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "dedup")
    await suite.prepare(jid, 1, [1])
    suite.start({"processes": 1, "concurrency": 2})
    assert suite.wait_ready()
    key = f"item:{jid}:1"
    first = suite.enqueue({"op": "item", "kwargs": {"job_id": jid, "n": 1}, "unique_key": key})
    second = suite.enqueue({"op": "item", "kwargs": {"job_id": jid, "n": 1}, "unique_key": key})
    await suite.wait_results(jid, 1, timeout=30)
    await asyncio.sleep(1.5)
    third = suite.enqueue({"op": "item", "kwargs": {"job_id": jid, "n": 1}, "unique_key": key})
    await asyncio.sleep(2)
    inv = await db.invariant(suite.conn, jid)
    return {"first": first, "second": second, "after_ack": third, "invariant": inv, "mock": http_json("GET", "/stats")}


async def chain(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "chain")
    await suite.prepare(jid, None, None)
    suite.start({"processes": 1, "concurrency": 2})
    assert suite.wait_ready()
    sent = suite.enqueue(
        {"op": "chain", "kwargs": {"job_id": jid, "step": 0, "total": 4}, "unique_key": f"chain:{jid}:0"}
    )
    steps = await suite.wait_progress(jid, 4, timeout=40)
    rows = await suite.conn.fetch("select step from progress where job_id = $1 order by step", jid)
    return {"send": sent, "steps": steps, "rows": [row["step"] for row in rows]}


async def cancel_before(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "cancel-before")
    await suite.prepare(jid, 5, list(range(5)))
    await suite.conn.execute("update jobs set cancelled = true where id = $1", jid)
    suite.start({"processes": 1, "concurrency": 2})
    assert suite.wait_ready()
    suite.enqueue({"op": "fanout", "kwargs": {"job_id": jid, "n": 5}})
    deadline = time.time() + 45
    skips = 0
    while time.time() < deadline:
        skips = await suite.conn.fetchval(
            "select count(*) from notes where job_id = $1 and kind = 'skip'", jid
        )
        if skips >= 5:
            break
        await asyncio.sleep(0.4)
    inv = await db.invariant(suite.conn, jid)
    return {"skips": skips, "invariant": inv, "mock": http_json("GET", "/stats")}


async def cancel_mid(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "cancel-mid")
    n = 12
    await suite.prepare(jid, n, list(range(n)))
    env = {"CHAOS_START": "sleep:0.4"}
    suite.start({"processes": 1, "concurrency": 2}, env=env)
    assert suite.wait_ready()
    suite.enqueue({"op": "fanout", "kwargs": {"job_id": jid, "n": n}})
    # Пустой воркер simple-q берёт следующую задачу только на heartbeat.
    started = await _wait_attempt_count(suite, jid, 2, timeout=40)
    await suite.conn.execute("update jobs set cancelled = true where id = $1", jid)
    deadline = time.time() + 40
    while time.time() < deadline:
        done = await suite.conn.fetchval(
            """
            select count(*) from attempts a
            where a.job_id = $1 and a.item >= 0
            """,
            jid,
        )
        if done >= n:
            break
        await asyncio.sleep(0.4)
    inv = await db.invariant(suite.conn, jid)
    return {"saw_start": started, "invariant": inv, "mock": http_json("GET", "/stats")}


async def boom(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "boom")
    await suite.prepare(jid, None, None)
    suite.start({"processes": 1, "concurrency": 1})
    assert suite.wait_ready()
    suite.enqueue({"op": "boom", "kwargs": {"job_id": jid}})
    status = await suite.wait_status(jid, "failed", timeout=30)
    notes = await suite.conn.fetch("select kind, body from notes where job_id = $1 order by id", jid)
    attempts = await suite.conn.fetchval("select count(*) from attempts where job_id = $1", jid)
    return {
        "status": status,
        "attempts": attempts,
        "notes": [f"{row['kind']}:{row['body']}" for row in notes],
    }


async def limit(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "limit")
    await suite.prepare(jid, None, None)
    suite.start({"processes": 1, "concurrency": 1})
    assert suite.wait_ready()
    suite.enqueue({"op": "limitjob", "kwargs": {"job_id": jid}})
    status = await suite.wait_status(jid, "failed", timeout=40)
    notes = await suite.conn.fetch("select kind, body from notes where job_id = $1 order by id", jid)
    attempts = await suite.conn.fetchval("select count(*) from attempts where job_id = $1", jid)
    return {"status": status, "attempts": attempts, "notes": [f"{row['kind']}:{row['body']}" for row in notes]}


async def group(suite: Suite, lib: str) -> dict:
    if lib not in {"dramatiq", "celery"}:
        return {"measured": True, "native_group": False, "reason": "нет group/chord"}
    n = 6
    jid = job_id(lib, "group")
    await suite.prepare(jid, n, list(range(n)))
    suite.start({"processes": 1, "concurrency": 4})
    assert suite.wait_ready()
    sent = suite.enqueue({"op": "group", "job_id": jid, "n": n, "kwargs": {}})
    got = await suite.wait_results(jid, n, timeout=40)
    await asyncio.sleep(2)
    note = await suite.conn.fetchval(
        "select body from notes where job_id = $1 and kind = 'group_done' order by id desc limit 1", jid
    )
    inv = await db.invariant(suite.conn, jid)
    return {"send": sent, "got": got, "group_note": note, "invariant": inv, "native_group": True}


async def parallel_batch(suite: Suite, lib: str) -> dict:
    n = suite.params["batch"]
    jid = job_id(lib, "parallel")
    await suite.prepare(jid, n, list(range(n)))
    suite.start({"processes": 2, "concurrency": 2})
    assert suite.wait_ready()
    t0 = time.time()
    suite.enqueue({"op": "fanout", "kwargs": {"job_id": jid, "n": n}})
    got = await suite.wait_results(jid, n, timeout=90)
    inv = await db.invariant(suite.conn, jid)
    pids = await suite.conn.fetch("select distinct pid from attempts where job_id = $1 and item >= 0", jid)
    return {
        "seconds": round(time.time() - t0, 3),
        "got": got,
        "pids": [row["pid"] for row in pids],
        "invariant": inv,
    }


async def recycle(suite: Suite, lib: str) -> dict:
    n = max(suite.params["batch"], 12)
    jid = job_id(lib, "recycle")
    await suite.prepare(jid, n, list(range(n)))
    opts = {"processes": 1, "concurrency": 1, "max_tasks": 3}
    if lib == "arq":
        return {"measured": True, "native": False, "reason": "у arq нет recycle по числу задач"}
    suite.start(opts)
    if not suite.wait_ready():
        return {"measured": True, "ready": False, "log_tail": suite.tail(), "native": lib in {"simpleq", "celery", "taskiq"}}
    suite.enqueue({"op": "fanout", "kwargs": {"job_id": jid, "n": n}})
    got = await suite.wait_results(jid, n, timeout=90)
    inv = await db.invariant(suite.conn, jid)
    pids = await suite.conn.fetch("select distinct pid from attempts where job_id = $1 and item >= 0", jid)
    return {"got": got, "pids": [row["pid"] for row in pids], "invariant": inv, "native": True}


async def sigkill(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "sigkill")
    await suite.prepare(jid, None, None)
    seconds = suite.params["sigkill_sleep"]
    env = {}
    if lib == "simpleq":
        env = {"SIMPLE_Q_HEARTBEAT_SECS": "5", "SIMPLE_Q_MISSED_HEARTBEATS": "2"}
    if lib == "dramatiq":
        env = {"DRAMATIQ_HEARTBEAT_MS": "8000"}
    if lib == "celery":
        env = {"CELERY_VISIBILITY_TIMEOUT": "12"}
    if lib == "arq":
        env = {"ARQ_JOB_TIMEOUT": "20", "ARQ_BLOCK_TIMEOUT": "20", "ARQ_MAX_JOBS": "2"}
    if lib == "taskiq":
        env = {"TASKIQ_IDLE_MS": "8000"}
    suite.start({"processes": 1, "concurrency": 1}, env=env)
    assert suite.wait_ready(), suite.tail()
    suite.enqueue({"op": "blocker", "kwargs": {"job_id": jid, "seconds": seconds}})
    await _wait_attempt(suite, jid, timeout=30)
    old = list(suite.workers)
    kill_at = time.time()
    for worker in old:
        os.killpg(worker.pid, signal.SIGKILL)
        worker.proc.wait(timeout=10)
    suite.workers.clear()
    suite.start({"processes": 1, "concurrency": 1}, env=env)
    assert suite.wait_ready(), suite.tail()
    await asyncio.sleep(min(seconds + 8, 40))
    gap = await db.gap_s(suite.conn, jid, kill_at)
    attempts = await suite.conn.fetch(
        "select item, note, extract(epoch from started_at) as ts from attempts where job_id = $1 order by id",
        jid,
    )
    return {
        "kill_at": kill_at,
        "gap": gap,
        "attempts": [dict(row) for row in attempts],
        "heartbeat_env": env,
    }


async def sigterm_short(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "sigterm-short")
    await suite.prepare(jid, None, None)
    env = {"ARQ_JOB_COMPLETION_WAIT": str(suite.params["grace"])} if lib == "arq" else {}
    if lib == "celery":
        env["CELERY_SOFT_SHUTDOWN"] = str(suite.params["grace"])
    suite.start({"processes": 1, "concurrency": 1, "shutdown_ms": int(suite.params["grace"] * 1000), "shutdown_s": suite.params["grace"], "wait_tasks": suite.params["grace"]}, env=env)
    assert suite.wait_ready()
    suite.enqueue({"op": "blocker", "kwargs": {"job_id": jid, "seconds": 5}})
    await _wait_attempt(suite, jid, timeout=15)
    sigterm_at = time.time()
    stop = suite.stop_all(grace=suite.params["grace"] + 15)
    attempts = await suite.conn.fetchval("select count(*) from attempts where job_id = $1", jid)
    return {"sigterm_at": sigterm_at, "stop": stop, "attempts": attempts}


async def early_sigterm(suite: Suite, lib: str) -> dict:
    delays = [0.4, 1.5, 2.5]
    rows = []
    for delay in delays:
        await suite.reset_state()
        suite.lib = lib
        started_workers = suite.start({"processes": 1, "concurrency": 1})
        time.sleep(delay)
        info = started_workers[0].stop(grace=5)
        rows.append({"delay_s": delay, "stop": info, "alive_after_stop_call": False})
        suite.workers.clear()
    return {"rows": rows}


async def shell_sigterm(suite: Suite, lib: str) -> dict:
    rows = []
    for mode in ("bash", "exec"):
        await suite.reset_state()
        suite.lib = lib
        workers = suite.start({"processes": 1, "concurrency": 1, "shell": mode})
        time.sleep(1.2)
        info = workers[0].stop(grace=6)
        rows.append({"shell": mode, "signal": "process_group", "stop": info})
        suite.workers.clear()
    # docker stop и kubelet сигналят только PID 1. killpg выше бьёт и потомка.
    for mode in ("bash", "exec"):
        await suite.reset_state()
        suite.lib = lib
        workers = suite.start({"processes": 1, "concurrency": 1, "shell": mode})
        time.sleep(1.2)
        info = _signal_pid1(workers[0], grace=6)
        rows.append({"shell": mode, "signal": "pid1", "stop": info})
        suite.workers.clear()
    return {"rows": rows}


def _signal_pid1(worker: Worker, grace: float) -> dict:
    if not worker.alive():
        return {"already_exited": True, "code": worker.proc.returncode}
    sigterm_at = time.time()
    os.kill(worker.proc.pid, signal.SIGTERM)
    deadline = time.time() + grace
    while time.time() < deadline and worker.alive():
        time.sleep(0.2)
    killed = False
    if worker.alive():
        killed = True
        os.kill(worker.proc.pid, signal.SIGKILL)
        worker.proc.wait(timeout=5)
    else:
        worker.proc.wait(timeout=5)
    return {
        "sigterm_at": sigterm_at,
        "t_drain_s": round(time.time() - sigterm_at, 3),
        "killed_after_grace": killed,
        "code": worker.proc.returncode,
    }


async def async_parallel(suite: Suite, lib: str) -> dict:
    n = suite.params["async_n"]
    seconds = suite.params["async_s"]
    conc = suite.params["async_c"]
    jid = job_id(lib, "async")
    await suite.prepare(jid, None, None)
    opts = {"processes": 1, "concurrency": conc}
    env = {"ARQ_MAX_JOBS": str(conc)} if lib == "arq" else {}
    if lib == "celery":
        opts = {"processes": 1, "concurrency": conc}
    suite.start(opts, env=env)
    assert suite.wait_ready(), suite.tail()
    t0 = time.time()
    sent = suite.enqueue_many({"op": "snooze", "count": n, "job_id": jid, "seconds": seconds}, timeout=180)
    deadline = time.time() + n * seconds / conc + 40
    ends = 0
    while time.time() < deadline:
        ends = await suite.conn.fetchval(
            "select count(*) from attempts where job_id = $1 and note = 'snooze-end'", jid
        )
        if ends >= n:
            break
        await asyncio.sleep(0.4)
    elapsed = time.time() - t0
    serial = n * seconds
    span = await suite.conn.fetchrow(
        """
        select extract(epoch from min(started_at)) as first_start,
               extract(epoch from max(started_at)) as last_end,
               count(*) filter (where note = 'snooze-start') as starts,
               count(*) filter (where note = 'snooze-end') as ends
        from attempts where job_id = $1
        """,
        jid,
    )
    work_s = None
    if span and span["first_start"] and span["last_end"]:
        work_s = round(float(span["last_end"]) - float(span["first_start"]), 3)
    return {
        "sent": sent,
        "ends": ends,
        "elapsed_s": round(elapsed, 3),
        "work_span_s": work_s,
        "serial_s": serial,
        "parallel_floor_s": round(serial / conc, 3),
        "concurrency": conc,
    }


async def blocking(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "block")
    seconds = suite.params["block"]
    await suite.prepare(jid, None, None)
    env = {}
    if lib == "simpleq":
        env = {"SIMPLE_Q_HEARTBEAT_SECS": "5", "SIMPLE_Q_MISSED_HEARTBEATS": "2"}
    if lib == "dramatiq":
        env = {"DRAMATIQ_HEARTBEAT_MS": "8000"}
    if lib == "celery":
        env = {"CELERY_VISIBILITY_TIMEOUT": str(max(8, int(seconds / 2)))}
    if lib == "arq":
        env = {"ARQ_BLOCK_TIMEOUT": str(max(30, int(seconds + 30))), "ARQ_JOB_TIMEOUT": str(max(30, int(seconds + 30)))}
    if lib == "taskiq":
        env = {"TASKIQ_IDLE_MS": str(max(8000, int(seconds * 500)))}
    suite.start({"processes": 2 if lib != "arq" else 2, "concurrency": 1}, env=env)
    assert suite.wait_ready(), suite.tail()
    t0 = time.time()
    suite.enqueue({"op": "blocker", "kwargs": {"job_id": jid, "seconds": seconds}})
    await asyncio.sleep(seconds + 8)
    attempts = await suite.conn.fetch(
        "select item, note, extract(epoch from started_at) as ts from attempts where job_id = $1 order by id", jid
    )
    return {"elapsed_s": round(time.time() - t0, 3), "block_s": seconds, "env": env, "attempts": [dict(row) for row in attempts]}


async def fast_bench(suite: Suite, lib: str) -> dict:
    n = suite.params["fast"]
    jid = job_id(lib, "fast")
    path = Path(f"/tmp/lat-{lib}.txt")
    path.write_text("")
    opts = {"processes": 2, "concurrency": 4}
    env = {"ARQ_MAX_JOBS": "8", "LAT_PATH": str(path)}
    suite.start(opts, env=env)
    assert suite.wait_ready(), suite.tail()
    idle = _sample(suite, 3)
    t0 = time.time()
    sent = suite.enqueue_many({"op": "fast", "count": n, "job_id": jid}, timeout=300)
    deadline = time.time() + 180
    lines = 0
    load_peak = idle["rss_max"]
    while time.time() < deadline:
        load_peak = max(load_peak, suite.rss())
        if path.exists():
            lines = sum(1 for _ in path.open())
        if lines >= n:
            break
        await asyncio.sleep(0.4)
    elapsed = time.time() - t0
    lat = _latency_file(path)
    return {
        "sent": sent,
        "done": lines,
        "elapsed_s": round(elapsed, 3),
        "throughput_per_s": round(lines / elapsed, 2) if elapsed else None,
        "latency_ms": lat,
        "idle_rss": idle["rss_max"],
        "load_rss_peak": load_peak,
    }


async def sigterm_infinite(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "infinite")
    await suite.prepare(jid, None, None)
    grace = suite.params["infinite_grace"]
    env = {}
    if lib == "arq":
        env = {"ARQ_JOB_COMPLETION_WAIT": "0", "ARQ_BLOCK_TIMEOUT": "600", "ARQ_JOB_TIMEOUT": "600"}
    if lib == "celery":
        env = {"CELERY_SOFT_SHUTDOWN": "0"}
    suite.start(
        {"processes": 1, "concurrency": 1, "shutdown_ms": int(grace * 1000), "shutdown_s": grace},
        env=env,
    )
    assert suite.wait_ready()
    suite.enqueue({"op": "blocker", "kwargs": {"job_id": jid, "seconds": 600}})
    await _wait_attempt(suite, jid, timeout=20)
    stop = suite.stop_all(grace=grace)
    return {"grace_s": grace, "stop": stop}


async def long_rollout(suite: Suite, lib: str) -> dict:
    checkpoint = scenario_checkpoint.get("on", True)
    return await _long(suite, lib, checkpoint)


scenario_checkpoint: dict[str, bool] = {}


async def long_on(suite: Suite, lib: str) -> dict:
    return await _long(suite, lib, True)


async def long_off(suite: Suite, lib: str) -> dict:
    return await _long(suite, lib, False)


async def _long(suite: Suite, lib: str, checkpoint: bool) -> dict:
    steps = suite.params["long_steps"]
    step_s = suite.params["long_step"]
    grace = suite.params["grace"]
    jid = job_id(lib, "long-on" if checkpoint else "long-off")
    await suite.prepare(jid, None, None)
    flag = f"/tmp/drain-{jid}"
    env = {"DRAIN_FLAG": flag}
    if lib == "arq":
        env["ARQ_JOB_COMPLETION_WAIT"] = str(grace)
        env["ARQ_LONG_TIMEOUT"] = "600"
    if lib == "celery":
        env["CELERY_SOFT_SHUTDOWN"] = str(grace)
        env["CELERY_VISIBILITY_TIMEOUT"] = "400"
    if lib == "simpleq":
        env["SIMPLE_Q_HEARTBEAT_SECS"] = os.environ.get("SIMPLE_Q_HEARTBEAT_SECS", "15")
        env["SIMPLE_Q_MISSED_HEARTBEATS"] = os.environ.get("SIMPLE_Q_MISSED_HEARTBEATS", "4")
    if lib == "taskiq":
        env["TASKIQ_IDLE_MS"] = "120000"
    suite.start(
        {"processes": 1, "concurrency": 1, "shutdown_ms": grace * 1000, "shutdown_s": grace, "wait_tasks": grace},
        env=env,
    )
    assert suite.wait_ready(), suite.tail()
    suite.enqueue(
        {
            "op": "longjob",
            "kwargs": {"job_id": jid, "steps": steps, "step_s": step_s},
            "unique_key": f"long:{jid}",
        }
    )
    await _wait_attempt(suite, jid, timeout=40)
    admin_path = RESULTS / f"admin-{lib}.jsonl"
    stop_admin = threading.Event()

    def _sample_admin() -> None:
        while not stop_admin.is_set():
            row = {"t": time.time(), "scenario": "long-on" if checkpoint else "long-off", "rss": suite.rss()}
            try:
                row["admin"] = suite.admin()
            except Exception as exc:
                row["admin_error"] = str(exc)
            with admin_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, default=str) + "\n")
            stop_admin.wait(2)

    sampler = threading.Thread(target=_sample_admin, daemon=True)
    sampler.start()
    try:
        await asyncio.sleep(min(8, step_s * 2))
        if checkpoint:
            Path(flag).write_text("1")
        sigterm_at = time.time()
        old = list(suite.workers)
        stop = [worker.stop(grace) for worker in old]
        suite.workers = [worker for worker in suite.workers if worker not in old]
        new_flag = f"/tmp/drain-{jid}-new"
        env["DRAIN_FLAG"] = new_flag
        suite.start({"processes": 1, "concurrency": 1, "shutdown_ms": grace * 1000, "shutdown_s": grace}, env=env)
        ready = suite.wait_ready()
        progress = await suite.wait_progress(jid, steps, timeout=steps * step_s + grace + 90)
        gap = await db.gap_s(suite.conn, jid, sigterm_at)
        attempts = await suite.conn.fetch(
            "select note, extract(epoch from started_at) as ts from attempts where job_id = $1 order by id", jid
        )
        return {
            "checkpoint": checkpoint,
            "ready_new": ready,
            "stop": stop,
            "gap": gap,
            "progress": progress,
            "steps": steps,
            "attempts": [dict(row) for row in attempts],
            "sigterm_at": sigterm_at,
            "rss_peak": suite.rss(),
            "admin_log": str(admin_path),
        }
    finally:
        stop_admin.set()
        sampler.join(timeout=3)


async def five_kills(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "five")
    await suite.prepare(jid, None, None)
    env = {}
    if lib == "simpleq":
        env = {"SIMPLE_Q_HEARTBEAT_SECS": "5", "SIMPLE_Q_MISSED_HEARTBEATS": "2"}
    if lib == "dramatiq":
        env = {"DRAMATIQ_HEARTBEAT_MS": "8000"}
    if lib == "celery":
        env = {"CELERY_VISIBILITY_TIMEOUT": "15"}
    if lib == "arq":
        env = {"ARQ_LONG_TIMEOUT": "30", "ARQ_JOB_TIMEOUT": "30"}
    if lib == "taskiq":
        env = {"TASKIQ_IDLE_MS": "8000"}
    kills = []
    op = "poisonjob" if lib == "simpleq" else "longjob"
    suite.start({"processes": 1, "concurrency": 1}, env=env)
    assert suite.wait_ready(), suite.tail()
    suite.enqueue({"op": op, "kwargs": {"job_id": jid, "steps": 80, "step_s": 2}, "unique_key": f"poison:{jid}"})
    for index in range(5):
        ok = await _wait_attempt_count(suite, jid, index + 1, timeout=40)
        kill_at = time.time()
        for worker in list(suite.workers):
            if worker.alive():
                os.killpg(worker.pid, signal.SIGKILL)
                worker.proc.wait(timeout=10)
        suite.workers.clear()
        kills.append({"i": index, "saw_attempt": ok, "kill_at": kill_at})
        if index == 4:
            break
        suite.start({"processes": 1, "concurrency": 1}, env=env)
        if not suite.wait_ready():
            kills[-1]["restart_failed"] = suite.tail()
            break
    if lib == "simpleq":
        suite.start({"processes": 1, "concurrency": 1}, env=env)
        suite.wait_ready()
        # Окно 5×2 = 10 с. Шестая доставка — poison, она приходит после окна.
        await asyncio.sleep(20)
    status = await suite.conn.fetchrow("select status, fail_reason from jobs where id = $1", jid)
    attempts = await suite.conn.fetch("select note from attempts where job_id = $1 order by id", jid)
    return {
        "kills": kills,
        "status": None if status is None else status["status"],
        "fail_reason": None if status is None else status["fail_reason"],
        "attempt_notes": [row["note"] for row in attempts],
        "env": env,
    }


async def prefetch(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "prefetch")
    n = 8
    await suite.prepare(jid, n, list(range(n)))
    suite.start({"processes": 1, "concurrency": 1, "prefetch": 4}, env={"CHAOS_START": "sleep:1"})
    assert suite.wait_ready()
    suite.enqueue({"op": "fanout", "kwargs": {"job_id": jid, "n": n}})
    await _wait_attempt(suite, jid, timeout=20)
    kill_at = time.time()
    for worker in list(suite.workers):
        os.killpg(worker.pid, signal.SIGKILL)
        worker.proc.wait(timeout=10)
    suite.workers.clear()
    env = {}
    if lib == "simpleq":
        env = {"SIMPLE_Q_HEARTBEAT_SECS": "5", "SIMPLE_Q_MISSED_HEARTBEATS": "2"}
    if lib == "celery":
        env = {"CELERY_VISIBILITY_TIMEOUT": "20"}
    if lib == "arq":
        env = {"ARQ_JOB_TIMEOUT": "30"}
    if lib == "taskiq":
        env = {"TASKIQ_IDLE_MS": "8000"}
    if lib == "dramatiq":
        env = {"DRAMATIQ_HEARTBEAT_MS": "8000"}
    suite.start({"processes": 1, "concurrency": 2}, env=env)
    suite.wait_ready()
    await suite.wait_results(jid, n, timeout=50)
    gap = await db.gap_s(suite.conn, jid, kill_at)
    inv = await db.invariant(suite.conn, jid)
    return {"gap": gap, "invariant": inv}


async def soft_memory(suite: Suite, lib: str) -> dict:
    if lib == "simpleq":
        n = 8
        jid = job_id(lib, "softmem")
        await suite.prepare(jid, n, list(range(n)))
        suite.start(
            {"processes": 1, "concurrency": 1, "mem_soft": "160M", "mem_hard": "220M"},
            env={"CHAOS_START": "alloc:30", "CHAOS_AFTER_CALL": "sleep:0.5"},
        )
        ready = suite.wait_ready()
        if ready:
            suite.enqueue({"op": "fanout", "kwargs": {"job_id": jid, "n": n}})
            await suite.wait_results(jid, n, timeout=60)
        inv = await db.invariant(suite.conn, jid)
        pids = await suite.conn.fetch("select distinct pid from attempts where job_id = $1 and item >= 0", jid)
        snap = suite.admin()
        return {
            "ready": ready,
            "pids": [row["pid"] for row in pids],
            "invariant": inv,
            "admin_keys": list(snap)[:12],
            "log_tail": suite.tail(40),
        }
    if lib == "celery":
        n = 6
        jid = job_id(lib, "softmem")
        await suite.prepare(jid, n, list(range(n)))
        suite.start({"processes": 1, "concurrency": 1}, env={"CELERY_MAX_MEMORY_KB": "180000", "CHAOS_START": "alloc:30"})
        ready = suite.wait_ready()
        if ready:
            suite.enqueue({"op": "fanout", "kwargs": {"job_id": jid, "n": n}})
            await suite.wait_results(jid, n, timeout=60)
        pids = await suite.conn.fetch("select distinct pid from attempts where job_id = $1", jid)
        return {"ready": ready, "pids": [row["pid"] for row in pids], "invariant": await db.invariant(suite.conn, jid)}
    return {"measured": True, "native": False, "reason": "лимита RSS процесса в библиотеке нет"}


async def worker_alive(suite: Suite, lib: str) -> dict:
    suite.start({"processes": 1, "concurrency": 1})
    ready = suite.wait_ready()
    snap = suite.admin()
    text = json.dumps(snap)[:2000]
    return {"ready": ready, "admin_excerpt": text}


async def prom_probe(suite: Suite, lib: str) -> dict:
    port = {"simpleq": 9191, "dramatiq": 9191, "taskiq": 9000}.get(lib)
    suite.start({"processes": 1, "concurrency": 1})
    ready = suite.wait_ready()
    await asyncio.sleep(1.5)
    if port is None:
        return {"ready": ready, "tcp": False, "reason": "порт метрик не объявлен"}
    import socket

    tcp = False
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            tcp = True
    except OSError as exc:
        tcp_error = str(exc)
    else:
        tcp_error = ""
    body = ""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as response:
            body = response.read(500).decode(errors="ignore")
            code = response.status
    except Exception as exc:
        code = None
        body = str(exc)
    return {"ready": ready, "port": port, "tcp": tcp, "tcp_error": tcp_error, "http": code, "body": body[:300]}


async def trace_header(suite: Suite, lib: str) -> dict:
    if lib != "simpleq":
        return {"measured": True, "native_header": False, "reason": "в этом прогоне заголовок проверяется у simple-q"}
    jid = job_id(lib, "trace")
    await suite.prepare(jid, 1, [0])
    suite.start({"processes": 1, "concurrency": 1})
    assert suite.wait_ready()
    suite.enqueue({"op": "item", "kwargs": {"job_id": jid, "n": 0}, "unique_key": f"item:{jid}:0"})
    await suite.wait_results(jid, 1, timeout=20)
    rows = await suite.conn.fetch("select kind, body from notes where job_id = $1", jid)
    return {"notes": [f"{row['kind']}:{row['body']}" for row in rows]}


async def redis_down(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "redis-down")
    n = 6
    await suite.prepare(jid, n, list(range(n)))
    suite.start({"processes": 1, "concurrency": 2})
    assert suite.wait_ready()
    suite.enqueue({"op": "fanout", "kwargs": {"job_id": jid, "n": n}})
    await asyncio.sleep(0.5)
    subprocess.run(["redis-cli", "shutdown", "nosave"], check=False)
    try:
        await asyncio.sleep(suite.params["redis_down"])
    finally:
        _start_redis(aof=False)
    await asyncio.sleep(2)
    got = await suite.wait_results(jid, n, timeout=40)
    alive = any(worker.alive() for worker in suite.workers)
    inv = await db.invariant(suite.conn, jid)
    return {"down_s": suite.params["redis_down"], "got": got, "worker_alive": alive, "invariant": inv}


async def redis_loss(suite: Suite, lib: str) -> dict:
    """Очередь без воркера: рестарт Redis без AOF и с AOF."""
    rows = []
    for aof in (False, True):
        await suite.reset_state()
        suite.lib = lib
        _start_redis(aof=aof)
        suite.env = base_env()
        jid = job_id(lib, f"redis-{'aof' if aof else 'noaof'}")
        await suite.prepare(jid, 3, [0, 1, 2])
        for item in range(3):
            suite.enqueue({"op": "item", "kwargs": {"job_id": jid, "n": item}, "unique_key": f"item:{jid}:{item}"})
        if aof:
            subprocess.run(["redis-cli", "shutdown"], check=False)
        else:
            subprocess.run(["redis-cli", "shutdown", "nosave"], check=False)
            for name in ("dump.rdb", "appendonly.aof", "appendonlydir"):
                path = Path("/tmp/redis-data") / name
                if path.is_dir():
                    subprocess.run(["rm", "-rf", str(path)], check=False)
                else:
                    path.unlink(missing_ok=True)
        await asyncio.sleep(1)
        _start_redis(aof=aof)
        await asyncio.sleep(0.5)
        appendonly = subprocess.run(["redis-cli", "config", "get", "appendonly"], capture_output=True, text=True).stdout
        dbsize = subprocess.run(["redis-cli", "dbsize"], capture_output=True, text=True).stdout.strip()
        suite.start({"processes": 1, "concurrency": 1})
        ready = suite.wait_ready()
        got = await suite.wait_results(jid, 3, timeout=20) if ready else 0
        rows.append(
            {
                "aof": aof,
                "appendonly": " ".join(appendonly.split()),
                "dbsize_before_worker": dbsize,
                "ready": ready,
                "got": got,
                "invariant": await db.invariant(suite.conn, jid),
            }
        )
        suite.stop_all(2)
    return {"rows": rows}


async def maxmemory(suite: Suite, lib: str) -> dict:
    suite.env = base_env()
    subprocess.run(["redis-cli", "config", "set", "maxmemory", "1000000"], check=False)
    subprocess.run(["redis-cli", "config", "set", "maxmemory-policy", "noeviction"], check=False)
    errors = 0
    accepted = 0
    jid = job_id(lib, "mem")
    try:
        for n in range(50):
            try:
                result = suite.enqueue({"op": "fast", "kwargs": {"job_id": jid, "n": n, "sent_ms": n}})
                if result.get("accepted", True):
                    accepted += 1
            except Exception:
                errors += 1
                break
    finally:
        subprocess.run(["redis-cli", "config", "set", "maxmemory", "0"], check=False)
        subprocess.run(["redis-cli", "flushall"], check=False)
    return {"accepted": accepted, "errors": errors}


async def periodic(suite: Suite, lib: str) -> dict:
    jid = job_id(lib, "periodic")
    await suite.prepare(jid, 1, [0])
    suite.start({"processes": 1, "concurrency": 1})
    assert suite.wait_ready()
    sends = []
    for _ in range(4):
        sends.append(
            suite.enqueue({"op": "item", "kwargs": {"job_id": jid, "n": 0}, "unique_key": f"periodic:{jid}"})
        )
        await asyncio.sleep(0.05)
    got = await suite.wait_results(jid, 1, timeout=45)
    inv = await db.invariant(suite.conn, jid)
    return {"sends": sends, "got": got, "invariant": inv}


async def double_early(suite: Suite, lib: str) -> dict:
    first = suite.start({"processes": 1, "concurrency": 1})
    time.sleep(0.8)
    # Второе поколение почти сразу, затем SIGTERM первому и через 2 с второму.
    second = suite.start({"processes": 1, "concurrency": 1})
    time.sleep(1.5)
    stop1 = first[0].stop(grace=4)
    time.sleep(2)
    stop2 = second[0].stop(grace=4)
    suite.workers = [worker for worker in suite.workers if worker.alive()]
    return {"first": stop1, "second": stop2}


async def _wait_attempt(suite: Suite, job_id: str, timeout: float) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        count = await suite.conn.fetchval("select count(*) from attempts where job_id = $1", job_id)
        if count:
            return
        await asyncio.sleep(0.2)
    raise TimeoutError(f"no attempt for {job_id}: {suite.tail(20)}")


async def _wait_attempt_count(suite: Suite, job_id: str, count: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        got = await suite.conn.fetchval("select count(*) from attempts where job_id = $1", job_id)
        if got >= count:
            return True
        await asyncio.sleep(0.3)
    return False


def _sample(suite: Suite, seconds: float) -> dict:
    peak = 0
    end = time.time() + seconds
    while time.time() < end:
        peak = max(peak, suite.rss())
        time.sleep(0.4)
    return {"rss_max": peak}


def _latency_file(path: Path) -> dict:
    if not path.exists():
        return {}
    deltas = []
    for line in path.read_text().splitlines():
        parts = line.split(",")
        if len(parts) != 3:
            continue
        try:
            deltas.append(int(parts[2]) - int(parts[1]))
        except ValueError:
            continue
    if not deltas:
        return {"n": 0}
    deltas.sort()

    def pick(q: float) -> int:
        return deltas[min(len(deltas) - 1, int(round(q * (len(deltas) - 1))))]

    return {"n": len(deltas), "p50": pick(0.5), "p99": pick(0.99), "max": deltas[-1]}


async def idle_gap(suite: Suite, lib: str) -> dict:
    """Пауза пустой очереди. У simple-q сверяется с heartbeat."""
    if lib != "simpleq":
        return {"measured": False, "reason": "интервал пустого опроса измеряется на simple-q"}
    rows = []
    for heartbeat in (15, 5):
        await suite.reset_state()
        suite.lib = lib
        jid = job_id(lib, f"idle{heartbeat}")
        await suite.prepare(jid, 1, [0])
        suite.start(
            {"processes": 1, "concurrency": 1},
            env={"SIMPLE_Q_HEARTBEAT_SECS": str(heartbeat), "SIMPLE_Q_MISSED_HEARTBEATS": "4"},
        )
        if not suite.wait_ready():
            rows.append({"heartbeat_s": heartbeat, "ready": False, "log_tail": suite.tail(15)})
            suite.stop_all(2)
            continue
        enqueued_at = time.time()
        sent = suite.enqueue(
            {"op": "item", "kwargs": {"job_id": jid, "n": 0}, "unique_key": f"item:{jid}:0"}
        )
        got = await suite.wait_results(jid, 1, timeout=heartbeat + 25)
        started = await suite.conn.fetchval(
            "select extract(epoch from min(started_at)) from attempts where job_id = $1 and item = 0",
            jid,
        )
        gap = None if started is None else round(float(started) - enqueued_at, 3)
        delayed_at = time.time()
        delayed = suite.enqueue(
            {
                "op": "item",
                "kwargs": {"job_id": jid, "n": 0},
                "unique_key": f"item:{jid}:delayed",
                "delay_ms": 1000,
            }
        )
        await suite.prepare(jid, None, None)
        await suite.conn.execute(
            "insert into expected (job_id, item) values ($1, 7) on conflict do nothing", jid
        )
        # Отдельный ключ и item=0 уже занят результатом. Вторая задача пишет тот же item
        # и увеличит executions. Ждём вторую попытку.
        deadline = time.time() + heartbeat + 25
        second = None
        while time.time() < deadline:
            second = await suite.conn.fetchval(
                "select extract(epoch from max(started_at)) from attempts where job_id = $1 and item = 0",
                jid,
            )
            if second is not None and float(second) > delayed_at:
                break
            await asyncio.sleep(0.3)
        delay_gap = None if second is None or float(second) <= delayed_at else round(float(second) - delayed_at, 3)
        rows.append(
            {
                "heartbeat_s": heartbeat,
                "ready": True,
                "immediate": sent,
                "immediate_gap_s": gap,
                "got": got,
                "delayed_send": delayed,
                "delay_requested_s": 1.0,
                "delay_gap_s": delay_gap,
            }
        )
        suite.stop_all(2)
    return {"rows": rows}


async def block_lease(suite: Suite, lib: str) -> dict:
    """Блокирующий sleep длиннее окна повторной доставки."""
    if lib != "simpleq":
        return {"measured": False, "reason": "отдельный прогон simple-q: sleep дольше heartbeat-окна"}
    jid = job_id(lib, "block-lease")
    await suite.prepare(jid, None, None)
    seconds = 20
    env = {"SIMPLE_Q_HEARTBEAT_SECS": "5", "SIMPLE_Q_MISSED_HEARTBEATS": "2"}
    suite.start({"processes": 1, "concurrency": 1}, env=env)
    assert suite.wait_ready(), suite.tail()
    suite.enqueue({"op": "blocker", "kwargs": {"job_id": jid, "seconds": seconds}})
    await _wait_attempt(suite, jid, timeout=20)
    await asyncio.sleep(seconds + 3)
    attempts = await suite.conn.fetch(
        "select item, note from attempts where job_id = $1 order by id", jid
    )
    return {
        "block_s": seconds,
        "window_s": 10,
        "attempts": [dict(row) for row in attempts],
        "env": env,
    }


def _start_redis(aof: bool) -> None:
    subprocess.run(["redis-cli", "shutdown", "nosave"], check=False, capture_output=True)
    for _ in range(25):
        if subprocess.run(["redis-cli", "ping"], capture_output=True).returncode != 0:
            break
        time.sleep(0.2)
    if subprocess.run(["redis-cli", "ping"], capture_output=True).returncode == 0:
        mode = "yes" if aof else "no"
        subprocess.run(["redis-cli", "config", "set", "appendonly", mode], check=False)
        return
    Path("/tmp/redis-data").mkdir(exist_ok=True)
    subprocess.run(
        [
            "redis-server",
            "--bind",
            "127.0.0.1",
            "--port",
            "6379",
            "--dir",
            "/tmp/redis-data",
            "--dbfilename",
            "dump.rdb",
            "--appendonly",
            "yes" if aof else "no",
            "--appendfsync",
            "everysec",
            "--save",
            "",
            "--daemonize",
            "yes",
            "--protected-mode",
            "yes",
        ],
        check=True,
    )
    for _ in range(20):
        if subprocess.run(["redis-cli", "ping"], capture_output=True).returncode == 0:
            return
        time.sleep(0.2)
    raise RuntimeError("redis did not come back")


SCENARIOS = {
    "smoke": smoke,
    "dedup": dedup,
    "chain": chain,
    "cancel_before": cancel_before,
    "cancel_mid": cancel_mid,
    "boom": boom,
    "limit": limit,
    "group": group,
    "parallel": parallel_batch,
    "recycle": recycle,
    "sigkill": sigkill,
    "sigterm_short": sigterm_short,
    "sigterm_infinite": sigterm_infinite,
    "early_sigterm": early_sigterm,
    "shell_sigterm": shell_sigterm,
    "async": async_parallel,
    "blocking": blocking,
    "fast": fast_bench,
    "long_on": long_on,
    "long_off": long_off,
    "five_kills": five_kills,
    "prefetch": prefetch,
    "soft_memory": soft_memory,
    "admin": worker_alive,
    "prom": prom_probe,
    "trace": trace_header,
    "redis_down": redis_down,
    "redis_loss": redis_loss,
    "maxmemory": maxmemory,
    "periodic": periodic,
    "double_early": double_early,
    "idle_gap": idle_gap,
    "block_lease": block_lease,
}

PHASES = {
    "smoke": ["smoke", "dedup", "chain", "cancel_before", "cancel_mid", "boom", "limit", "group", "periodic", "trace", "prom", "admin", "idle_gap"],
    "resilience": ["parallel", "recycle", "sigkill", "sigterm_short", "sigterm_infinite", "early_sigterm", "shell_sigterm", "prefetch", "soft_memory", "blocking", "async"],
    "rollout": ["long_off", "long_on", "five_kills", "double_early"],
    "redis": ["redis_loss", "redis_down", "maxmemory"],
    "bench": ["fast"],
}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lib", default="simpleq")
    parser.add_argument("--phase", default="smoke")
    parser.add_argument("--only", default="")
    args = parser.parse_args()
    params = scale_params()
    libs = LIBS if args.lib == "all" else [args.lib]
    names = [part for part in args.only.split(",") if part] or PHASES[args.phase]
    RESULTS.mkdir(parents=True, exist_ok=True)
    _ensure_mock()
    conn = await asyncpg.connect(PG_DSN)
    await db.ensure_schema(conn)
    suite = Suite(conn, params)
    try:
        for lib in libs:
            for name in names:
                await run_one(suite, lib, name)
    finally:
        suite.stop_all(2)
        await conn.close()


def _ensure_mock() -> None:
    try:
        http_json("GET", "/stats")
        return
    except Exception:
        pass
    subprocess.Popen(
        ["/workspace/.venv/bin/python", "-m", "bench.mock_http"],
        cwd="/workspace",
        env=base_env(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    for _ in range(30):
        try:
            http_json("GET", "/stats")
            return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("mock http did not start")


if __name__ == "__main__":
    asyncio.run(main())
