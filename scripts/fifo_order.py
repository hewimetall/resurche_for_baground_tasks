"""Порядок равного приоритета при prefetch > 1.

Dramatiq 1.18 кладёт (priority, message), и MessageProxy.__lt__ всегда True.
2.2.1 кладёт (priority, monotonic_ns). Пачка из 16 сообщений, prefetch 16,
первое спит 1.5 с, остальные 0.01 с. Порт 6394.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

ROOT = Path("/workspace")
PORT = "6394"
URL = f"redis://127.0.0.1:{PORT}/0"
N = 16


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
            "/tmp/redis-6394.pid",
            "--dir",
            "/tmp",
            "--dbfilename",
            "fifo.rdb",
            "--logfile",
            "/tmp/redis-6394.log",
        ],
        check=True,
    )


def run_one(python: str, dramatiq_bin: str, label: str) -> dict:
    path = Path(f"/tmp/fifo-{label}.txt")
    path.unlink(missing_ok=True)
    subprocess.run(["redis-cli", "-p", PORT, "FLUSHALL"], check=True, capture_output=True)
    env = os.environ.copy()
    env["REDIS_URL"] = URL
    env["PYTHONPATH"] = str(ROOT)
    env["dramatiq_queue_prefetch"] = str(N)
    log = open(f"/tmp/fifo-{label}.log", "w", encoding="utf-8")
    worker = subprocess.Popen(
        [
            dramatiq_bin,
            "scripts.fifo_probe",
            "--queues",
            "bench",
            "--processes",
            "1",
            "--threads",
            "1",
        ],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        time.sleep(2.5)
        code = (
            "import os\n"
            "os.environ['REDIS_URL']=%r\n"
            "from scripts.fifo_probe import seq\n"
            "for n in range(%d):\n"
            "    hold = 1.5 if n == 0 else 0.01\n"
            "    seq.send(n, hold, %r)\n"
        ) % (URL, N, str(path))
        subprocess.run([python, "-c", code], cwd=ROOT, env=env, check=True, timeout=20)
        deadline = time.time() + 20
        while time.time() < deadline:
            if path.exists() and len(path.read_text(encoding="utf-8").splitlines()) >= N:
                break
            time.sleep(0.05)
        order = []
        if path.exists():
            order = [int(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return {
            "label": label,
            "python": python,
            "order": order,
            "in_send_order": order == list(range(N)),
            "inversions": sum(
                1 for i in range(len(order)) for j in range(i + 1, len(order)) if order[i] > order[j]
            ),
        }
    finally:
        try:
            os.killpg(worker.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            worker.wait(timeout=8)
        except subprocess.TimeoutExpired:
            os.killpg(worker.pid, signal.SIGKILL)


def main() -> None:
    import json

    ensure_redis()
    runs = []
    for label, python, binary in (
        ("1.18.0", "/tmp/venv-d118/bin/python", "/tmp/venv-d118/bin/dramatiq"),
        ("2.2.1", "/workspace/.venv/bin/python", "/workspace/.venv/bin/dramatiq"),
    ):
        for repeat in range(3):
            row = run_one(python, binary, f"{label}-{repeat}")
            row["version"] = label
            row["repeat"] = repeat
            runs.append(row)
    report = {
        "n": N,
        "prefetch": N,
        "hold0_s": 1.5,
        "runs": runs,
    }
    out = ROOT / "results" / "traces" / "fifo-order.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(out.read_text())


if __name__ == "__main__":
    main()
