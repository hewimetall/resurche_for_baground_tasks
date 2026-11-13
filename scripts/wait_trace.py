"""Трейс ожидания задачи: Redis MONITOR, /proc wchan и py-spy.

Один воркер на изолированный Redis :6391. Основной Redis :6379 не трогаем.
Для каждого фреймворка фиксируем, чем заняты потоки, пока очередь пуста,
и сколько проходит от enqueue до записи fast_record.
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
OUT = ROOT / "results" / "traces"
PORT = "6391"
URL = f"redis://127.0.0.1:{PORT}/0"
PY = ROOT / ".venv" / "bin" / "python"
PYSPY = ROOT / ".venv" / "bin" / "py-spy"
ARQ_PY = ROOT / ".venv-arq" / "bin" / "python"

CASES = [
    {
        "lib": "simpleq",
        "heartbeat": "5",
        "idle_s": 1.5,
        "wait_s": 14,
    },
    {
        "lib": "dramatiq",
        "idle_s": 8.0,
        "wait_s": 12,
    },
    {
        "lib": "celery",
        "idle_s": 2.0,
        "wait_s": 8,
    },
    {
        "lib": "arq",
        "idle_s": 2.0,
        "wait_s": 8,
    },
    {
        "lib": "taskiq",
        "idle_s": 2.0,
        "wait_s": 10,
    },
]


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, text=True, capture_output=True, **kwargs)


def ensure_redis() -> None:
    ping = _run(["redis-cli", "-p", PORT, "PING"])
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
            "/tmp/redis-trace.pid",
            "--dir",
            "/tmp",
            "--dbfilename",
            "trace.rdb",
            "--logfile",
            "/tmp/redis-trace.log",
        ],
        check=True,
    )
    deadline = time.time() + 5
    while time.time() < deadline:
        ping = _run(["redis-cli", "-p", PORT, "PING"])
        if ping.stdout.strip() == "PONG":
            return
        time.sleep(0.1)
    raise SystemExit("redis 6391 did not start")


def descendants(pid: int) -> list[int]:
    found = [pid]
    queue = [pid]
    while queue:
        parent = queue.pop()
        listing = _run(["ps", "-o", "pid=", "--ppid", str(parent)])
        for line in listing.stdout.split():
            child = int(line)
            if child not in found:
                found.append(child)
                queue.append(child)
    return found


def read_text(path: Path, limit: int = 4000) -> str:
    try:
        data = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return f"<unreadable {exc}>"
    if len(data) > limit:
        return data[:limit] + "\n…\n"
    return data


def thread_snapshot(pid: int) -> dict:
    task_root = Path(f"/proc/{pid}/task")
    threads = []
    if not task_root.is_dir():
        return {"pid": pid, "alive": False, "threads": []}
    for task in sorted(task_root.iterdir(), key=lambda p: p.name):
        comm = read_text(task / "comm", 80).strip()
        wchan = read_text(task / "wchan", 120).strip()
        status = read_text(task / "status", 2000)
        state = ""
        for line in status.splitlines():
            if line.startswith("State:"):
                state = line.split(":", 1)[1].strip()
                break
        threads.append({"tid": int(task.name), "comm": comm, "state": state, "wchan": wchan})
    cmdline = read_text(Path(f"/proc/{pid}/cmdline"), 500).replace("\x00", " ").strip()
    switches = {}
    status = read_text(Path(f"/proc/{pid}/status"), 4000)
    for key in ("voluntary_ctxt_switches", "nonvoluntary_ctxt_switches", "Threads"):
        for line in status.splitlines():
            if line.startswith(key + ":"):
                switches[key] = line.split(":", 1)[1].strip()
    return {"pid": pid, "cmdline": cmdline, "threads": threads, **switches}


def pyspy(pid: int) -> str:
    if not PYSPY.exists():
        return "py-spy missing"
    proc = _run([str(PYSPY), "dump", "--pid", str(pid), "--nonblocking"], timeout=15)
    text = (proc.stdout or "") + (proc.stderr or "")
    if "permission" in text.lower() or proc.returncode != 0:
        native = _run(
            [str(PYSPY), "dump", "--pid", str(pid), "--native", "--nonblocking"],
            timeout=15,
        )
        extra = (native.stdout or "") + (native.stderr or "")
        text = text + "\n-- native --\n" + extra
    if len(text) > 8000:
        return text[:8000] + "\n…\n"
    return text


def switch_rate(pid: int, window: float = 0.4) -> dict | None:
    before = thread_snapshot(pid)
    if not before.get("alive", True) or "voluntary_ctxt_switches" not in before:
        return None
    time.sleep(window)
    after = thread_snapshot(pid)
    if "voluntary_ctxt_switches" not in after:
        return None

    def num(snap: dict, key: str) -> int:
        return int(snap.get(key, "0"))

    vol = num(after, "voluntary_ctxt_switches") - num(before, "voluntary_ctxt_switches")
    nvol = num(after, "nonvoluntary_ctxt_switches") - num(before, "nonvoluntary_ctxt_switches")
    return {
        "window_s": window,
        "voluntary": vol,
        "nonvoluntary": nvol,
        "per_s": round((vol + nvol) / window, 1),
    }


def interesting(line: str) -> bool:
    noise = (" PING", " INFO", " CLIENT", " COMMAND", " SELECT")
    return not any(token in line for token in noise)


def summarize_monitor(path: Path, t_start: float, t_send: float | None, t_end: float) -> dict:
    idle = []
    after = []
    counts: dict[str, int] = {}
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not interesting(raw):
            continue
        parts = raw.split(" ", 3)
        if len(parts) < 4:
            continue
        try:
            ts = float(parts[0])
        except ValueError:
            continue
        cmd = parts[3].strip().strip('"').split()[0].upper() if parts[3].strip() else "?"
        counts[cmd] = counts.get(cmd, 0) + 1
        item = raw[:240]
        if t_send is not None and ts >= t_send:
            if len(after) < 40:
                after.append(item)
        elif t_start <= ts < (t_send or t_end):
            if len(idle) < 40:
                idle.append(item)
    return {"command_counts": counts, "idle_sample": idle, "after_send_sample": after}


def enqueue(lib: str, lat_path: Path) -> None:
    sent_ms = int(time.time() * 1000)
    payload = {
        "op": "fast",
        "kwargs": {"job_id": f"trace-{lib}", "n": 1, "sent_ms": sent_ms},
    }
    env = os.environ.copy()
    env["REDIS_URL"] = URL
    env["LAT_PATH"] = str(lat_path)
    env["PYTHONPATH"] = str(ROOT)
    # Как в scripts/run_suite.py: старт simple-q и arq открывает пул Postgres.
    env["PG_DSN"] = "postgresql://ubuntu@127.0.0.1:5432/bench"
    env["MOCK_URL"] = "http://127.0.0.1:8090"
    py = ARQ_PY if lib == "arq" else PY
    subprocess.run(
        [str(py), "-m", "bench.cli", "enqueue", "--lib", lib, "--payload", json.dumps(payload)],
        cwd=ROOT,
        env=env,
        check=True,
        timeout=20,
    )


def latency_ms(path: Path) -> int | None:
    if not path.exists():
        return None
    line = path.read_text(encoding="utf-8").strip().splitlines()
    if not line:
        return None
    parts = line[-1].split(",")
    if len(parts) != 3:
        return None
    return int(parts[2]) - int(parts[1])


def run_case(spec: dict) -> dict:
    lib = spec["lib"]
    lat = Path(f"/tmp/bench-trace-{lib}.txt")
    mon_path = OUT / f"{lib}.monitor.txt"
    log_path = OUT / f"{lib}.worker.log"
    lat.unlink(missing_ok=True)
    _run(["redis-cli", "-p", PORT, "FLUSHALL"])
    monitor = subprocess.Popen(
        ["redis-cli", "-p", PORT, "MONITOR"],
        stdout=mon_path.open("w"),
        stderr=subprocess.DEVNULL,
    )
    from bench.workers import command

    opts = {"processes": 1, "concurrency": 1, "shutdown_ms": 3000, "shutdown_s": 5}
    env = os.environ.copy()
    env["REDIS_URL"] = URL
    env["LAT_PATH"] = str(lat)
    env["PYTHONPATH"] = str(ROOT)
    env["PG_DSN"] = "postgresql://ubuntu@127.0.0.1:5432/bench"
    env["MOCK_URL"] = "http://127.0.0.1:8090"
    if spec.get("heartbeat"):
        env["SIMPLE_Q_HEARTBEAT_SECS"] = spec["heartbeat"]
    t_start = time.time()
    log = log_path.open("w")
    worker = subprocess.Popen(
        command(lib, opts),
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    time.sleep(spec["idle_s"])
    pids = [pid for pid in descendants(worker.pid) if Path(f"/proc/{pid}").exists()]
    idle = []
    for pid in pids:
        snap = thread_snapshot(pid)
        snap["switch_rate"] = switch_rate(pid)
        snap["pyspy"] = pyspy(pid)
        idle.append(snap)
    t_send = time.time()
    enqueue_error = None
    try:
        enqueue(lib, lat)
    except Exception as exc:  # noqa: BLE001 — стенд должен записать сбой, а не оборваться
        enqueue_error = str(exc)
    time.sleep(0.35)
    waiting = [thread_snapshot(pid) for pid in pids if Path(f"/proc/{pid}").exists()]
    deadline = time.time() + spec["wait_s"]
    while time.time() < deadline and latency_ms(lat) is None:
        time.sleep(0.05)
    gap = latency_ms(lat)
    t_end = time.time()
    os.killpg(worker.pid, signal.SIGTERM)
    try:
        worker.wait(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(worker.pid, signal.SIGKILL)
        worker.wait(timeout=5)
    log.close()
    monitor.send_signal(signal.SIGINT)
    try:
        monitor.wait(timeout=3)
    except subprocess.TimeoutExpired:
        monitor.kill()
    return {
        "lib": lib,
        "redis": URL,
        "heartbeat_s": spec.get("heartbeat"),
        "idle_before_send_s": round(t_send - t_start, 3),
        "enqueue_error": enqueue_error,
        "pickup_ms": gap,
        "wall_after_send_s": round(t_end - t_send, 3),
        "idle_processes": idle,
        "waiting_processes": waiting,
        "monitor": summarize_monitor(mon_path, t_start, t_send, t_end),
        "worker_log_tail": read_text(log_path, 2500),
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    ensure_redis()
    only = set(sys.argv[1:])
    results = []
    for spec in CASES:
        if only and spec["lib"] not in only:
            continue
        print(f"trace {spec['lib']}", flush=True)
        item = run_case(spec)
        results.append(item)
        (OUT / f"{spec['lib']}.json").write_text(
            json.dumps(item, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"  pickup_ms={item['pickup_ms']} err={item['enqueue_error']}", flush=True)
    rows = []
    for spec in CASES:
        path = OUT / f"{spec['lib']}.json"
        if not path.exists():
            continue
        item = json.loads(path.read_text(encoding="utf-8"))
        rows.append(
            {
                "lib": item["lib"],
                "pickup_ms": item["pickup_ms"],
                "idle_before_send_s": item["idle_before_send_s"],
                "enqueue_error": item["enqueue_error"],
                "commands": item["monitor"]["command_counts"],
            }
        )
    (OUT / "summary.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
