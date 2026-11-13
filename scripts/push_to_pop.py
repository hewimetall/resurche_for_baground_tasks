"""От команды Redis до fast_record, отдельно от жизни процесса постановки.

pickup_ms в серии phase считается от sent_ms, который берётся до
запуска ``python -m bench.cli``. Этот скрипт делит то же окно на
время процесса постановки и интервал между командой в MONITOR и
записью файла. Порт 6391. Redis на 6379 не трогаем.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path

ROOT = Path("/workspace")
PORT = "6391"
URL = f"redis://127.0.0.1:{PORT}/0"
PY = ROOT / ".venv" / "bin" / "python"
ARQ_PY = ROOT / ".venv-arq" / "bin" / "python"
INTERESTING = {
    "LPUSH",
    "RPUSH",
    "BRPOP",
    "BLPOP",
    "XADD",
    "XREADGROUP",
    "XAUTOCLAIM",
    "XACK",
    "ZADD",
    "ZRANGEBYSCORE",
    "LPOP",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PUBLISH",
    "PSUBSCRIBE",
}


def ensure_redis() -> None:
    ping = subprocess.run(["redis-cli", "-p", PORT, "PING"], text=True, capture_output=True)
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
            "/tmp/redis-6391.pid",
            "--dir",
            "/tmp",
            "--dbfilename",
            "pushpop.rdb",
            "--logfile",
            "/tmp/redis-6391.log",
        ],
        check=True,
    )


def command_and_key(line: str) -> tuple[str, str, str] | None:
    if "]" not in line:
        return None
    bracket, rest = line.split("]", 1)
    where = "lua" if "[0 lua]" in line else "client"
    rest = rest.strip()
    if not rest.startswith('"'):
        return None
    parts = rest.split('"')
    cmd = parts[1].upper() if len(parts) > 1 else ""
    key = parts[3] if len(parts) > 3 else ""
    if cmd not in INTERESTING:
        return None
    try:
        stamp = float(bracket.split()[0])
    except ValueError:
        return None
    return cmd, where, f"{stamp:.6f} {key}"


def parse_monitor(path: Path) -> list[dict]:
    events = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        parsed = command_and_key(line)
        if parsed is None:
            continue
        cmd, where, tail = parsed
        stamp_s, _, key = tail.partition(" ")
        events.append({"t": float(stamp_s), "cmd": cmd, "where": where, "key": key[:80]})
    return events


def run_one(lib: str, py: Path, heartbeat: str | None, idle_s: float) -> dict:
    from bench.workers import command

    lat = Path(f"/tmp/push-pop-{lib}.txt")
    mon_path = Path(f"/tmp/push-pop-{lib}.monitor.txt")
    log_path = Path(f"/tmp/push-pop-{lib}.log")
    lat.unlink(missing_ok=True)
    subprocess.run(["redis-cli", "-p", PORT, "FLUSHALL"], check=True, capture_output=True)
    monitor = subprocess.Popen(
        ["redis-cli", "-p", PORT, "MONITOR"],
        stdout=mon_path.open("w"),
        stderr=subprocess.DEVNULL,
    )
    env = os.environ.copy()
    env.update(
        {
            "REDIS_URL": URL,
            "PYTHONPATH": str(ROOT),
            "PG_DSN": "postgresql://ubuntu@127.0.0.1:5432/bench",
            "MOCK_URL": "http://127.0.0.1:8090",
            "LAT_PATH": str(lat),
            "PYTHONUNBUFFERED": "1",
        }
    )
    if heartbeat:
        env["SIMPLE_Q_HEARTBEAT_SECS"] = heartbeat
    log = log_path.open("w", encoding="utf-8")
    worker = subprocess.Popen(
        command(lib, {"processes": 1, "concurrency": 1, "shutdown_ms": 3000, "shutdown_s": 5}),
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    time.sleep(idle_s)
    sent_ms = int(time.time() * 1000)
    t0 = time.time()
    proc = subprocess.run(
        [
            str(py),
            "-m",
            "bench.cli",
            "enqueue",
            "--lib",
            lib,
            "--payload",
            json.dumps({"op": "fast", "kwargs": {"job_id": f"pp-{lib}", "n": 1, "sent_ms": sent_ms}}),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    client_ms = round((time.time() - t0) * 1000, 1)
    deadline = time.time() + 12
    while time.time() < deadline and not lat.exists():
        time.sleep(0.02)
    time.sleep(0.4)
    try:
        os.killpg(worker.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        worker.wait(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(worker.pid, signal.SIGKILL)
        worker.wait(timeout=3)
    monitor.terminate()
    monitor.wait(timeout=3)
    log.close()
    pickup = None
    done_ms = None
    if lat.exists() and lat.read_text(encoding="utf-8").strip():
        _n, sent_s, done_s = lat.read_text(encoding="utf-8").strip().splitlines()[-1].split(",")
        done_ms = int(done_s)
        pickup = done_ms - int(sent_s)
    events = parse_monitor(mon_path)
    sent_s = sent_ms / 1000
    window = [ev for ev in events if sent_s - 3 <= ev["t"] <= sent_s + 8]

    def rel(ev: dict) -> dict:
        return {
            "cmd": ev["cmd"],
            "where": ev["where"],
            "key": ev["key"],
            "ms_after_sent": round((ev["t"] - sent_s) * 1000, 1),
        }

    return {
        "lib": lib,
        "idle_s": idle_s,
        "heartbeat": heartbeat,
        "sent_ms": sent_ms,
        "done_ms": done_ms,
        "pickup_ms": pickup,
        "client_ms": client_ms,
        "client_rc": proc.returncode,
        "client_stdout": proc.stdout.strip()[:300],
        "client_stderr": proc.stderr.strip()[:400],
        "near_send": [rel(ev) for ev in window],
    }


def main() -> None:
    ensure_redis()
    runs = [
        run_one("celery", PY, None, 3.0),
        run_one("taskiq", PY, None, 3.0),
        run_one("dramatiq", PY, None, 8.0),
        run_one("arq", ARQ_PY, None, 2.0),
        run_one("simpleq", PY, "5", 2.0),
    ]
    path = ROOT / "results" / "traces" / "push-to-pop.json"
    path.write_text(json.dumps({"redis": URL, "runs": runs}, indent=2), encoding="utf-8")
    print(path)
    for run in runs:
        interesting = [
            ev
            for ev in run["near_send"]
            if ev["cmd"] in {"LPUSH", "RPUSH", "BRPOP", "XADD", "XREADGROUP", "XAUTOCLAIM", "ZADD", "ZRANGEBYSCORE", "LPOP", "SUBSCRIBE", "UNSUBSCRIBE"}
        ]
        print(run["lib"], "pickup", run["pickup_ms"], "client", run["client_ms"], "rc", run["client_rc"])
        for ev in interesting[-12:]:
            print(" ", ev)


if __name__ == "__main__":
    main()
