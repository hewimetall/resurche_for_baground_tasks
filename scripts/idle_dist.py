"""Распределение задержки, пока очередь пустая.

Один воркер живёт всю серию. Порт 6393. Redis на 6379 не трогаем.
После записи fast_record скрипт ждёт park_s, чтобы wake по завершению
успел сделать пустой fetch, и только потом ждёт phase и ставит следующую
задачу. Вторая серия, reidle, ждёт дольше периода, чтобы сон снова вырос.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path("/workspace")
sys.path.insert(0, str(ROOT))
OUT = ROOT / "results" / "traces"
PORT = "6393"
URL = f"redis://127.0.0.1:{PORT}/0"
PY = ROOT / ".venv" / "bin" / "python"
ARQ_PY = ROOT / ".venv-arq" / "bin" / "python"


def stats(values: list[float]) -> dict:
    ordered = sorted(values)

    def pct(q: float) -> float:
        if not ordered:
            return 0.0
        index = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
        return round(ordered[index], 1)

    return {
        "n": len(ordered),
        "min": round(ordered[0], 1) if ordered else None,
        "p50": pct(0.50),
        "p90": pct(0.90),
        "max": round(ordered[-1], 1) if ordered else None,
    }


def ensure_redis() -> None:
    ping = subprocess.run(
        ["redis-cli", "-p", PORT, "PING"],
        text=True,
        capture_output=True,
    )
    if ping.returncode == 0 and ping.stdout.strip() == "PONG":
        return
    subprocess.run(
        [
            "redis-server",
            "--port",
            PORT,
            "--bind",
            "127.0.0.1",
            "--save",
            "",
            "--appendonly",
            "no",
            "--daemonize",
            "yes",
            "--pidfile",
            "/tmp/redis-6393.pid",
            "--dir",
            "/tmp",
            "--dbfilename",
            "dist.rdb",
            "--logfile",
            "/tmp/redis-6393.log",
        ],
        check=True,
    )


def flush() -> None:
    subprocess.run(["redis-cli", "-p", PORT, "FLUSHALL"], check=True, capture_output=True)


def read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def enqueue(lib: str, n: int, sent_ms: int) -> None:
    payload = {
        "op": "fast",
        "kwargs": {"job_id": f"dist-{lib}-{n}", "n": n, "sent_ms": sent_ms},
    }
    env = os.environ.copy()
    env.update(base_env(lib))
    py = ARQ_PY if lib == "arq" else PY
    subprocess.run(
        [str(py), "-m", "bench.cli", "enqueue", "--lib", lib, "--payload", json.dumps(payload)],
        cwd=ROOT,
        env=env,
        check=True,
        timeout=20,
    )


def base_env(lib: str, heartbeat: str | None = None) -> dict[str, str]:
    env = {
        "REDIS_URL": URL,
        "PYTHONPATH": str(ROOT),
        "PG_DSN": "postgresql://ubuntu@127.0.0.1:5432/bench",
        "MOCK_URL": "http://127.0.0.1:8090",
        "LAT_PATH": f"/tmp/idle-dist-{lib}.txt",
        "PYTHONUNBUFFERED": "1",
    }
    if heartbeat:
        env["SIMPLE_Q_HEARTBEAT_SECS"] = heartbeat
    return env


def start_worker(lib: str, heartbeat: str | None) -> subprocess.Popen:
    from bench.workers import command

    env = os.environ.copy()
    env.update(base_env(lib, heartbeat))
    log = open(f"/tmp/idle-dist-{lib}.log", "w", encoding="utf-8")
    return subprocess.Popen(
        ["stdbuf", "-oL", "-eL", *command(lib, {"processes": 1, "concurrency": 1, "shutdown_ms": 3000, "shutdown_s": 5})],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def stop_worker(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=5)


def wait_line(path: Path, have: int, timeout: float) -> str | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        lines = read_lines(path)
        if len(lines) > have:
            return lines[-1]
        time.sleep(0.02)
    return None


def run_series(
    lib: str,
    *,
    heartbeat: str | None,
    warmup: float,
    park_s: float,
    phases: list[float],
    timeout: float,
) -> dict:
    lat = Path(f"/tmp/idle-dist-{lib}.txt")
    lat.unlink(missing_ok=True)
    flush()
    worker = start_worker(lib, heartbeat)
    rows = []
    try:
        time.sleep(warmup)
        have = 0
        for index, phase in enumerate(phases):
            if index:
                time.sleep(park_s + phase)
            else:
                time.sleep(phase)
            sent_ms = int(time.time() * 1000)
            t_send = time.time()
            enqueue(lib, index, sent_ms)
            line = wait_line(lat, have, timeout)
            have = len(read_lines(lat))
            if line is None:
                rows.append({"i": index, "phase_s": phase, "pickup_ms": None, "error": "timeout"})
                continue
            n_s, sent_s, done_s = line.split(",")
            pickup = int(done_s) - int(sent_s)
            # enqueue_ms включает wait_line: это стена до файла, не время процесса постановки.
            # Время одного subprocess — в client_ms у scripts/push_to_pop.py.
            rows.append(
                {
                    "i": index,
                    "phase_s": phase,
                    "pickup_ms": pickup,
                    "enqueue_ms": round((time.time() - t_send) * 1000, 1),
                    "n": int(n_s),
                }
            )
    finally:
        stop_worker(worker)
    picked = [row["pickup_ms"] for row in rows if row["pickup_ms"] is not None]
    sums = [
        round(row["phase_s"] * 1000 + row["pickup_ms"], 1)
        for row in rows
        if row["pickup_ms"] is not None and row["i"] > 0
    ]
    return {
        "rows": rows,
        "pickup": stats([float(v) for v in picked]),
        "phase_plus_pickup_ms": stats([float(v) for v in sums]),
    }


def count_running(log_path: Path) -> int:
    if not log_path.exists():
        return 0
    return log_path.read_text(encoding="utf-8", errors="replace").count("running actor")


def run_hold(repeats: int = 6) -> dict:
    """Сон 2 с внутри задачи. Таймер, взведённый до задачи, и таймер после неё расходятся."""
    lib = "simpleq"
    log_path = Path("/tmp/idle-dist-simpleq-hold.log")
    lat = Path("/tmp/idle-dist-simpleq-hold.txt")
    lat.unlink(missing_ok=True)
    flush()
    from bench.workers import command

    env = os.environ.copy()
    env.update(base_env(lib, "5"))
    env["LAT_PATH"] = str(lat)
    log = log_path.open("w", encoding="utf-8")
    worker = subprocess.Popen(
        ["stdbuf", "-oL", "-eL", *command(lib, {"processes": 1, "concurrency": 1, "shutdown_ms": 3000, "shutdown_s": 5})],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    rows = []
    try:
        time.sleep(2.0)
        for index in range(repeats):
            before = count_running(log_path)
            payload = {
                "op": "snooze",
                "kwargs": {"job_id": f"hold-{index}", "n": index, "seconds": 2.0},
            }
            subprocess.run(
                [
                    str(PY),
                    "-m",
                    "bench.cli",
                    "enqueue",
                    "--lib",
                    lib,
                    "--payload",
                    json.dumps(payload),
                ],
                cwd=ROOT,
                env=env,
                check=True,
                timeout=20,
            )
            t_send_hold = time.time()
            deadline = time.time() + 8
            while time.time() < deadline and count_running(log_path) <= before:
                time.sleep(0.02)
            t_actor = time.time()
            # Задача спит 2 с. Ставим fast через 2.4 с после входа в актор.
            time.sleep(2.4)
            have = len(read_lines(lat))
            sent_ms = int(time.time() * 1000)
            t_fast = time.time()
            enqueue(lib, index, sent_ms)
            line = wait_line(lat, have, 8)
            pickup = None
            if line:
                pickup = int(line.split(",")[2]) - int(line.split(",")[1])
            rows.append(
                {
                    "i": index,
                    "hold_wait_ms": round((t_actor - t_send_hold) * 1000, 1),
                    "fast_after_actor_s": round(t_fast - t_actor, 3),
                    "fast_pickup_ms": pickup,
                }
            )
            if pickup is None:
                break
            time.sleep(0.5)
    finally:
        stop_worker(worker)
    picked = [row["fast_pickup_ms"] for row in rows if row["fast_pickup_ms"] is not None]
    return {"rows": rows, "fast_pickup": stats([float(v) for v in picked])}


def main() -> None:
    ensure_redis()
    OUT.mkdir(parents=True, exist_ok=True)
    heartbeat = 5.0
    phases_5 = [i * 0.5 for i in range(10)] * 2
    report = {
        "redis": URL,
        "note": (
            "phase — сон после park 0.35 с. "
            "Первая точка серии не имеет предыдущего fetch, её phase_plus_pickup не входит в сумму."
        ),
        "simpleq_phase": run_series(
            "simpleq",
            heartbeat="5",
            warmup=2.0,
            park_s=0.35,
            phases=phases_5,
            timeout=heartbeat + 4,
        ),
        "dramatiq_phase": run_series(
            "dramatiq",
            heartbeat=None,
            warmup=3.0,
            park_s=0.35,
            phases=[i * 0.1 for i in range(10)] * 2,
            timeout=4,
        ),
        "dramatiq_reidle": run_series(
            "dramatiq",
            heartbeat=None,
            warmup=10.0,
            park_s=8.0,
            phases=[0.0] * 8,
            timeout=4,
        ),
        "celery_phase": run_series(
            "celery",
            heartbeat=None,
            warmup=3.0,
            park_s=0.35,
            phases=[i * 0.1 for i in range(10)] * 2,
            timeout=4,
        ),
        "arq_phase": run_series(
            "arq",
            heartbeat=None,
            warmup=2.0,
            park_s=0.2,
            phases=[i * 0.05 for i in range(10)] * 2,
            timeout=3,
        ),
        "simpleq_hold2s": run_hold(),
        "taskiq_phase": run_series(
            "taskiq",
            heartbeat=None,
            warmup=3.0,
            park_s=0.35,
            phases=[i * 0.2 for i in range(10)] * 2,
            timeout=6,
        ),
    }
    path = OUT / "idle-dist.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(path)
    for name, block in report.items():
        if not isinstance(block, dict) or "pickup" not in block:
            continue
        print(name, block["pickup"], "sum", block["phase_plus_pickup_ms"])


if __name__ == "__main__":
    main()
