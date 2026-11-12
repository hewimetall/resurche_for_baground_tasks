"""Команды воркеров. Стенд запускает их как отдельные процессы."""

from __future__ import annotations

import shlex

ROOT_PY = "/workspace/.venv/bin"
ARQ_PY = "/workspace/.venv-arq/bin"


def command(lib: str, opts: dict) -> list[str]:
    processes = str(opts.get("processes", 2))
    concurrency = str(opts.get("concurrency", 2))
    if lib == "simpleq":
        cmd = [
            f"{ROOT_PY}/agent-worker",
            "worker",
            "--app",
            "bench.simpleq_app:create_app",
            "--factory",
            "--queues",
            "bench",
            "--processes",
            processes,
            "--concurrency",
            concurrency,
            "--max-tasks-per-process",
            str(opts.get("max_tasks", 500)),
        ]
        if opts.get("mem_soft"):
            cmd += ["--child-memory-soft", str(opts["mem_soft"])]
        if opts.get("mem_hard"):
            cmd += ["--child-memory-hard", str(opts["mem_hard"])]
        return _shell(cmd, opts.get("shell"))
    if lib == "dramatiq":
        cmd = [
            f"{ROOT_PY}/dramatiq",
            "bench.dramatiq_app",
            "--queues",
            "bench",
            "--processes",
            processes,
            "--threads",
            concurrency,
            "--worker-shutdown-timeout",
            str(int(opts.get("shutdown_ms", 60000))),
        ]
        return _shell(cmd, opts.get("shell"))
    if lib == "celery":
        cmd = [
            f"{ROOT_PY}/celery",
            "-A",
            "bench.celery_app",
            "worker",
            "-Q",
            "bench",
            "-c",
            concurrency,
            "--loglevel",
            "info",
            "--without-gossip",
            "--without-mingle",
        ]
        return _shell(cmd, opts.get("shell"))
    if lib == "arq":
        n = int(processes)
        one = [f"{ARQ_PY}/arq", "bench.arq_app.WorkerSettings"]
        one = _shell(one, opts.get("shell"))
        return one if n <= 1 else one
    if lib == "taskiq":
        cmd = [
            f"{ROOT_PY}/taskiq",
            "worker",
            "bench.taskiq_app:broker",
            "--workers",
            processes,
            "--max-async-tasks",
            concurrency,
            "--shutdown-timeout",
            str(opts.get("shutdown_s", 60)),
            "--ack-type",
            "when_executed",
            "--max-prefetch",
            str(opts.get("prefetch", 0)),
        ]
        if opts.get("max_tasks"):
            cmd += ["--max-tasks-per-child", str(opts["max_tasks"])]
        if opts.get("wait_tasks") is not None:
            cmd += ["--wait-tasks-timeout", str(opts["wait_tasks"])]
        return _shell(cmd, opts.get("shell"))
    raise SystemExit(lib)


def python_for(lib: str) -> str:
    if lib == "arq":
        return f"{ARQ_PY}/python"
    return f"{ROOT_PY}/python"


def _shell(cmd: list[str], mode: str | None) -> list[str]:
    if mode == "bash":
        return ["bash", "-c", " ".join(shlex.quote(part) for part in cmd)]
    if mode == "exec":
        return ["bash", "-c", "exec " + " ".join(shlex.quote(part) for part in cmd)]
    return cmd
