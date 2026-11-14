"""Общий AAA-харнесс: readiness, enqueue, наблюдение, cleanup."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Sequence

from research.taskiq.lib import config, observe, process

ROOT = config.ROOT
TASKIQ_BIN = str(config.VENV_BIN / "taskiq")
PYTHON = str(config.VENV_BIN / "python")
BROKER_TARGET = "research.taskiq.app.broker:broker"


def base_env(
    *,
    redis_url: str,
    stream: str,
    group: str,
    events_path: Path,
    idle_ms: int = 600000,
    xread_count: int = 100,
    xread_block: int = 500,
    lock_s: str | float | None = 5,
    result_backend: bool = False,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    env = {
        "PYTHONPATH": str(ROOT),
        "TASKIQ_RESEARCH_REDIS_URL": redis_url,
        "TASKIQ_RESEARCH_STREAM": stream,
        "TASKIQ_RESEARCH_GROUP": group,
        "TASKIQ_EVENTS_PATH": str(events_path),
        "TASKIQ_IDLE_MS": str(idle_ms),
        "TASKIQ_XREAD_COUNT": str(xread_count),
        "TASKIQ_XREAD_BLOCK": str(xread_block),
        "TASKIQ_RESULT_BACKEND": "1" if result_backend else "0",
        "TASK_TIMEOUT_S": os.environ.get("TASK_TIMEOUT_S", "3600"),
    }
    if lock_s is None:
        env["TASKIQ_UNACK_LOCK_S"] = ""
    else:
        env["TASKIQ_UNACK_LOCK_S"] = str(lock_s)
    if extra:
        env.update(extra)
    return env


def worker_cmd(
    *,
    workers: int = 1,
    max_async: int = 1,
    max_prefetch: int = 0,
    ack_type: str = "when_executed",
    shutdown_s: float = 10,
    max_tasks_per_child: int | None = None,
    wait_tasks_timeout: float | None = None,
) -> list[str]:
    cmd = [
        TASKIQ_BIN,
        "worker",
        BROKER_TARGET,
        "--workers",
        str(workers),
        "--max-async-tasks",
        str(max_async),
        "--max-prefetch",
        str(max_prefetch),
        "--ack-type",
        ack_type,
        "--shutdown-timeout",
        str(shutdown_s),
        "--log-level",
        "INFO",
    ]
    if max_tasks_per_child is not None:
        cmd += ["--max-tasks-per-child", str(max_tasks_per_child)]
    if wait_tasks_timeout is not None:
        cmd += ["--wait-tasks-timeout", str(wait_tasks_timeout)]
    return cmd


def reset_stream(redis_url: str, stream: str, group: str) -> None:
    client = observe.connect(redis_url)
    try:
        client.delete(stream)
        client.delete(f"autoclaim:{group}:{stream}")
    finally:
        client.close()


def read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def wait_event(
    path: Path,
    predicate,
    *,
    timeout_s: float,
    poll_s: float = 0.05,
) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for row in read_events(path):
            if predicate(row):
                return row
        time.sleep(poll_s)
    return None


def enqueue(
    task_name: str,
    *args,
    redis_url: str,
    env: dict[str, str] | None = None,
    **kwargs,
) -> str:
    ids = enqueue_batch(
        task_name,
        count=1,
        args=list(args),
        kwargs=kwargs,
        redis_url=redis_url,
        env=env,
    )
    return ids[0]


def enqueue_batch(
    task_name: str,
    *,
    count: int,
    args: list | None = None,
    kwargs: dict | None = None,
    redis_url: str,
    env: dict[str, str] | None = None,
    fast_stagger: bool = False,
) -> list[str]:
    """Постановка через subprocess: env до импорта брокера."""
    import subprocess

    merged = os.environ.copy()
    if env:
        merged.update(env)
    merged["TASKIQ_RESEARCH_REDIS_URL"] = redis_url
    merged["PYTHONPATH"] = str(ROOT)
    cmd = [
        PYTHON,
        "-m",
        "research.taskiq.lib.enqueue_cli",
        task_name,
        "--args-json",
        json.dumps(args or []),
        "--kwargs-json",
        json.dumps(kwargs or {}),
        "--count",
        str(count),
    ]
    if fast_stagger:
        cmd.append("--fast-stagger")
    out = subprocess.check_output(cmd, env=merged, cwd=str(ROOT), text=True)
    payload = json.loads(out.strip().splitlines()[-1])
    return list(payload["ids"])


def enqueue_many(task_name: str, items: Sequence[tuple], *, redis_url: str, env: dict[str, str] | None = None) -> list[str]:
    ids: list[str] = []
    for args in items:
        ids.append(enqueue(task_name, *args, redis_url=redis_url, env=env))
    return ids


def unique_ns(prefix: str) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:10]
    return f"{prefix}:{suffix}", f"g-{suffix}"


def readiness_handshake(
    *,
    redis_url: str,
    stream: str,
    group: str,
    events_path: Path,
    env: dict[str, str],
    workers: int = 1,
    max_async: int = 1,
    ack_type: str = "when_executed",
    timeout_s: float = 30.0,
) -> tuple[Any, str]:
    """Start worker, enqueue ready token, wait for handler_ready."""
    token = uuid.uuid4().hex
    log_path = events_path.with_suffix(".worker.log")
    cmd = worker_cmd(workers=workers, max_async=max_async, ack_type=ack_type)
    proc = process.start(cmd, env=env, cwd=ROOT, log_path=log_path)
    process.wait_alive(proc, timeout_s=10)
    # Give broker.startup / XGROUP a moment
    time.sleep(0.4)
    enqueue("ready", token, redis_url=redis_url, env=env)
    row = wait_event(
        events_path,
        lambda r: r.get("event") == "handler_ready" and r.get("token") == token,
        timeout_s=timeout_s,
    )
    if row is None:
        process.stop_group(proc, grace_s=2)
        raise RuntimeError("readiness handshake failed")
    return proc, token


def new_run_dir(kind: str, name: str) -> Path:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    path = config.ARTIFACTS / kind / f"{stamp}-{name}"
    path.mkdir(parents=True, exist_ok=True)
    return path
