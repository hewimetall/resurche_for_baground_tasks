"""Пустое ожидание Dramatiq 1.18.0 и 2.2.1 на одном Redis.

Воркер стартует, очередь пустая, затем одно сообщение. Холодный прогон
ждёт 1 с (backoff ещё миллисекунды). Прогретый ждёт 16 с, чтобы сон
упёрся в consumer timeout 5000 мс. Redis стенда на :6379 не используется.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path

ROOT = Path("/workspace")
OUT = ROOT / "results" / "traces"
PORT = "6392"
URL = f"redis://127.0.0.1:{PORT}/0"
PY_221 = ROOT / ".venv" / "bin" / "python"
PY_118 = Path("/tmp/venv-d118/bin/python")
RUNS = 3


def ensure_redis() -> None:
    ping = subprocess.run(["redis-cli", "-p", PORT, "PING"], text=True, capture_output=True)
    if ping.stdout.strip() == "PONG":
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
            "/tmp/redis-6392.pid",
            "--dir",
            "/tmp",
            "--dbfilename",
            "trace118.rdb",
            "--logfile",
            "/tmp/redis-6392.log",
        ],
        check=True,
    )


def backoff_sample(python: Path) -> dict:
    code = r"""
import json
from dramatiq.common import compute_backoff
cold = [compute_backoff(0, max_backoff=5000)[1] for _ in range(2000)]
hot = []
for _ in range(400):
    attempts = 0
    delay = 0
    for _step in range(24):
        attempts, delay = compute_backoff(attempts, max_backoff=5000)
    hot.append(delay)
def pack(xs):
    xs = sorted(xs)
    return {"n": len(xs), "min": xs[0], "p50": xs[len(xs)//2], "max": xs[-1]}
print(json.dumps({"cold_ms": pack(cold), "after_24_misses_ms": pack(hot)}))
"""
    proc = subprocess.run([str(python), "-c", code], text=True, capture_output=True, check=True)
    return json.loads(proc.stdout)


def one_gap(python: Path, label: str, idle_s: float, capture_monitor: bool) -> dict:
    path = Path(f"/tmp/d118-{label}-{idle_s}.txt")
    path.unlink(missing_ok=True)
    subprocess.run(["redis-cli", "-p", PORT, "FLUSHALL"], check=True, capture_output=True)
    mon_path = OUT / f"dramatiq-{label}-idle.monitor.txt"
    monitor = None
    if capture_monitor:
        monitor = subprocess.Popen(
            ["redis-cli", "-p", PORT, "MONITOR"],
            stdout=mon_path.open("w"),
            stderr=subprocess.DEVNULL,
        )
    env = os.environ.copy()
    env["REDIS_URL"] = URL
    env["PYTHONPATH"] = str(ROOT / "scripts")
    log_path = OUT / f"dramatiq-{label}.worker.log"
    log = log_path.open("w")
    worker = subprocess.Popen(
        [
            str(python),
            "-m",
            "dramatiq",
            "d118_probe",
            "--queues",
            "bench",
            "--processes",
            "1",
            "--threads",
            "1",
        ],
        cwd=ROOT / "scripts",
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    time.sleep(idle_s)
    sent = time.time()
    subprocess.run(
        [
            str(python),
            "-c",
            "import os,time,d118_probe; d118_probe.hit.send(os.environ['PATH_OUT'], time.time())",
        ],
        cwd=ROOT / "scripts",
        env={**env, "PATH_OUT": str(path)},
        check=True,
        timeout=20,
    )
    deadline = time.time() + 12
    gap = None
    while time.time() < deadline:
        if path.exists() and path.stat().st_size:
            sent_s, done_s = path.read_text().split()
            gap = round((float(done_s) - float(sent_s)) * 1000, 1)
            break
        time.sleep(0.02)
    os.killpg(worker.pid, signal.SIGTERM)
    try:
        worker.wait(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(worker.pid, signal.SIGKILL)
        worker.wait(timeout=5)
    log.close()
    counts: dict[str, int] = {}
    if monitor is not None:
        monitor.send_signal(signal.SIGINT)
        try:
            monitor.wait(timeout=3)
        except subprocess.TimeoutExpired:
            monitor.kill()
        for line in mon_path.read_text(errors="replace").splitlines():
            if '"' not in line:
                continue
            cmd = line.split('"')[1].upper()
            if cmd in {"LPOP", "BRPOP", "BLPOP", "XREADGROUP", "PSUBSCRIBE", "SUBSCRIBE", "RPUSH", "EVAL", "EVALSHA"}:
                counts[cmd] = counts.get(cmd, 0) + 1
    return {"idle_s": idle_s, "pickup_ms": gap, "redis_commands": counts, "wall_send": sent}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    ensure_redis()
    report = {}
    for label, python in (("1.18.0", PY_118), ("2.2.1", PY_221)):
        print("version", label, flush=True)
        item = {
            "python": str(python),
            "backoff": backoff_sample(python),
            "cold": [],
            "idle": [],
        }
        for i in range(RUNS):
            print(f"  cold {i}", flush=True)
            item["cold"].append(one_gap(python, label, 1.5, capture_monitor=False))
        for i in range(RUNS):
            print(f"  idle {i}", flush=True)
            item["idle"].append(one_gap(python, label, 16.0, capture_monitor=(i == 0)))
        report[label] = item
    (OUT / "dramatiq-118.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
