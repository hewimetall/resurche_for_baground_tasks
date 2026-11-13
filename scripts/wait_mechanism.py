"""Щель между блокирующими командами и срок simple-q через сон задачи.

Порт 6395. Redis на 6379 не трогаем.

simple-q: одна задача на 2 с, затем fast через 2.4 с после «running actor».
MONITOR отличает чтение по heartbeat (перед ним ZADD bench:workers)
от чтения по wake() (его нет).

Остальные воркеры просто стоят. Интервал между стартами BRPOP / XREADGROUP
минус таймаут блока — окно, когда блокирующей команды в Redis нет.
Интервал LPOP по dramatiq:bench — сам сон, уже после прогрева.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import time
from pathlib import Path

ROOT = Path("/workspace")
PORT = "6395"
URL = f"redis://127.0.0.1:{PORT}/0"
PY = ROOT / ".venv" / "bin" / "python"
ARQ_PY = ROOT / ".venv-arq" / "bin" / "python"
LINE = re.compile(r"^(\d+\.\d+) \[0 ([^\]]+)\] \"([^\"]+)\"(?: \"([^\"]*)\")?")


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
            "/tmp/redis-6395.pid",
            "--dir",
            "/tmp",
            "--dbfilename",
            "mech.rdb",
            "--logfile",
            "/tmp/redis-6395.log",
        ],
        check=True,
    )


def parse_monitor(path: Path) -> list[dict]:
    events = []
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = LINE.match(raw)
        if match is None:
            continue
        events.append(
            {
                "t": float(match.group(1)),
                "who": match.group(2),
                "cmd": match.group(3).upper(),
                "key": match.group(4) or "",
            }
        )
    return events


def stats(values: list[float]) -> dict:
    ordered = sorted(values)
    if not ordered:
        return {"n": 0}

    def pct(q: float) -> float:
        index = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
        return round(ordered[index], 1)

    return {
        "n": len(ordered),
        "min": round(ordered[0], 1),
        "p50": pct(0.5),
        "p90": pct(0.9),
        "max": round(ordered[-1], 1),
    }


def stop(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=3)


def start_worker(lib: str, extra: dict[str, str]) -> tuple[subprocess.Popen, Path]:
    from bench.workers import command

    env = os.environ.copy()
    env.update(
        {
            "REDIS_URL": URL,
            "PYTHONPATH": str(ROOT),
            "PG_DSN": "postgresql://ubuntu@127.0.0.1:5432/bench",
            "MOCK_URL": "http://127.0.0.1:8090",
            "LAT_PATH": f"/tmp/mech-{lib}.txt",
            "PYTHONUNBUFFERED": "1",
        }
    )
    env.update(extra)
    log_path = Path(f"/tmp/mech-{lib}.log")
    log = log_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        ["stdbuf", "-oL", "-eL", *command(lib, {"processes": 1, "concurrency": 1, "shutdown_ms": 3000, "shutdown_s": 5})],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return proc, log_path


def enqueue(lib: str, op: str, kwargs: dict, extra: dict[str, str]) -> tuple[int, float]:
    env = os.environ.copy()
    env.update(
        {
            "REDIS_URL": URL,
            "PYTHONPATH": str(ROOT),
            "PG_DSN": "postgresql://ubuntu@127.0.0.1:5432/bench",
            "MOCK_URL": "http://127.0.0.1:8090",
            "PYTHONUNBUFFERED": "1",
        }
    )
    env.update(extra)
    sent_ms = int(time.time() * 1000)
    if op == "fast":
        kwargs = {**kwargs, "sent_ms": sent_ms}
    t0 = time.time()
    subprocess.run(
        [
            str(PY),
            "-m",
            "bench.cli",
            "enqueue",
            "--lib",
            lib,
            "--payload",
            json.dumps({"op": op, "kwargs": kwargs}),
        ],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    return sent_ms, round((time.time() - t0) * 1000, 1)


def with_monitor(lib: str, extra: dict[str, str], idle_s: float) -> list[dict]:
    subprocess.run(["redis-cli", "-p", PORT, "FLUSHALL"], check=True, capture_output=True)
    mon_path = Path(f"/tmp/mech-{lib}.monitor.txt")
    monitor = subprocess.Popen(
        ["redis-cli", "-p", PORT, "MONITOR"],
        stdout=mon_path.open("w"),
        stderr=subprocess.DEVNULL,
    )
    worker, _log = start_worker(lib, extra)
    try:
        time.sleep(idle_s)
    finally:
        stop(worker)
        monitor.terminate()
        monitor.wait(timeout=3)
    return parse_monitor(mon_path)


def gaps(events: list[dict], cmd: str, key_exact: str, who_substr: str | None = None) -> list[float]:
    stamps = [
        ev["t"]
        for ev in events
        if ev["cmd"] == cmd
        and ev["key"] == key_exact
        and (who_substr is None or who_substr in ev["who"])
    ]
    return [round((b - a) * 1000, 1) for a, b in zip(stamps, stamps[1:])]


def same_client_between(events: list[dict], cmd: str, key_exact: str) -> list[dict]:
    hits = [ev for ev in events if ev["cmd"] == cmd and ev["key"] == key_exact]
    if len(hits) < 2:
        return []
    who = hits[0]["who"]
    rows = []
    for left, right in zip(hits, hits[1:]):
        mid = [
            ev["cmd"] + " " + ev["key"][:40]
            for ev in events
            if ev["who"] == who and left["t"] < ev["t"] < right["t"] and not (ev["cmd"] == cmd and ev["key"] == key_exact)
        ]
        rows.append({"gap_ms": round((right["t"] - left["t"]) * 1000, 1), "between": mid[:8]})
    return rows


def run_simpleq() -> dict:
    extra = {"SIMPLE_Q_HEARTBEAT_SECS": "5", "LAT_PATH": "/tmp/mech-simpleq.txt"}
    lat = Path("/tmp/mech-simpleq.txt")
    lat.unlink(missing_ok=True)
    subprocess.run(["redis-cli", "-p", PORT, "FLUSHALL"], check=True, capture_output=True)
    mon_path = Path("/tmp/mech-simpleq.monitor.txt")
    monitor = subprocess.Popen(
        ["redis-cli", "-p", PORT, "MONITOR"],
        stdout=mon_path.open("w"),
        stderr=subprocess.DEVNULL,
    )
    worker, log_path = start_worker("simpleq", extra)
    try:
        time.sleep(2.0)
        _sent_hold, client_hold = enqueue(
            "simpleq",
            "snooze",
            {"job_id": "mech-hold", "n": 0, "seconds": 2.0},
            extra,
        )
        t_enq = time.time()
        deadline = time.time() + 8
        while time.time() < deadline and log_path.read_text(encoding="utf-8", errors="replace").count("running actor") < 1:
            time.sleep(0.02)
        t_actor = time.time()
        time.sleep(2.4)
        sent_fast, client_fast = enqueue(
            "simpleq",
            "fast",
            {"job_id": "mech-fast", "n": 1},
            extra,
        )
        deadline = time.time() + 8
        while time.time() < deadline and not lat.exists():
            time.sleep(0.02)
        time.sleep(0.4)
    finally:
        stop(worker)
        monitor.terminate()
        monitor.wait(timeout=3)
    events = parse_monitor(mon_path)
    interesting = [
        ev
        for ev in events
        if ev["key"] in {"bench:scheduled", "bench:workers", "bench:active", "bench:signal", "bench:done"}
        and ev["cmd"] in {"ZADD", "ZRANGEBYSCORE", "RPUSH", "LPUSH"}
    ]
    if not interesting:
        return {"error": "no queue commands", "client_hold_ms": client_hold}
    t0 = interesting[0]["t"]
    rel = [
        {
            "ms": round((ev["t"] - t0) * 1000, 1),
            "cmd": ev["cmd"],
            "key": ev["key"],
            "who": "lua" if ev["who"] == "lua" else "client",
        }
        for ev in interesting
    ]
    reads = [ev for ev in rel if ev["cmd"] == "ZRANGEBYSCORE" and ev["key"] == "bench:scheduled"]
    classified = []
    previous_read = None
    for read in reads:
        prior_zadd = [
            item["ms"]
            for item in rel
            if item["cmd"] == "ZADD" and item["key"] == "bench:workers" and item["ms"] <= read["ms"]
        ]
        last_zadd = prior_zadd[-1] if prior_zadd else None
        heartbeat = last_zadd is not None and (previous_read is None or last_zadd > previous_read)
        push = any(
            item["cmd"] == "RPUSH"
            and item["key"] == "bench:active"
            and 0 <= item["ms"] - read["ms"] <= 2
            for item in rel
        )
        classified.append(
            {
                "ms": read["ms"],
                "workers_zadd_ms": last_zadd,
                "heartbeat_tick": heartbeat,
                "moved_to_active": push,
            }
        )
        previous_read = read["ms"]
    pickup = None
    if lat.exists() and lat.read_text(encoding="utf-8").strip():
        _n, sent_s, done_s = lat.read_text(encoding="utf-8").strip().splitlines()[-1].split(",")
        pickup = int(done_s) - int(sent_s)
    intervals = [round(b["ms"] - a["ms"], 1) for a, b in zip(reads, reads[1:])]
    return {
        "client_hold_ms": client_hold,
        "actor_after_enqueue_ms": round((t_actor - t_enq) * 1000, 1),
        "client_fast_ms": client_fast,
        "fast_pickup_ms": pickup,
        "fast_sent_ms": sent_fast,
        "scheduled_reads": classified,
        "read_intervals_ms": intervals,
        "events": rel,
    }


def main() -> None:
    ensure_redis()
    report = {
        "redis": URL,
        "simpleq_hold": run_simpleq(),
        "celery_idle": None,
        "taskiq_idle": None,
        "dramatiq_idle": None,
        "arq_idle": None,
    }
    celery = with_monitor("celery", {}, 22.0)
    celery_rows = same_client_between(celery, "BRPOP", "bench")
    report["celery_idle"] = {
        "brpop_gaps_ms": [row["gap_ms"] for row in celery_rows],
        "gap_stats": stats([row["gap_ms"] for row in celery_rows]),
        "between_same_client": celery_rows[:8],
        "excess_over_1000_ms": stats([row["gap_ms"] - 1000 for row in celery_rows]),
    }
    taskiq = with_monitor("taskiq", {}, 28.0)
    taskiq_gaps = gaps(taskiq, "XREADGROUP", "GROUP")
    report["taskiq_idle"] = {
        "xreadgroup_gaps_ms": taskiq_gaps,
        "gap_stats": stats(taskiq_gaps),
        "excess_over_2000_ms": stats([g - 2000 for g in taskiq_gaps]),
    }
    dramatiq = with_monitor("dramatiq", {}, 28.0)
    bench_gaps = gaps(dramatiq, "LPOP", "dramatiq:bench", who_substr="lua")
    # Первые секунды — набор misses. Хвост после 8 с от первого LPOP.
    bench_times = [ev["t"] for ev in dramatiq if ev["cmd"] == "LPOP" and ev["key"] == "dramatiq:bench"]
    late = []
    if bench_times:
        origin = bench_times[0]
        late_times = [t for t in bench_times if t - origin >= 8]
        late = [round((b - a) * 1000, 1) for a, b in zip(late_times, late_times[1:])]
    report["dramatiq_idle"] = {
        "bench_lpop_gaps_ms": bench_gaps,
        "after_8s_gaps_ms": late,
        "after_8s_stats": stats(late),
    }
    arq = with_monitor("arq", {}, 12.0)
    arq_gaps = gaps(arq, "ZRANGEBYSCORE", "arq:queue")
    report["arq_idle"] = {"zrange_gaps_ms": arq_gaps, "gap_stats": stats(arq_gaps)}
    path = ROOT / "results" / "traces" / "mechanism.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(path)
    print("simpleq reads", report["simpleq_hold"].get("scheduled_reads"))
    print("simpleq intervals", report["simpleq_hold"].get("read_intervals_ms"), "pickup", report["simpleq_hold"].get("fast_pickup_ms"))
    print("celery", report["celery_idle"]["gap_stats"], "excess", report["celery_idle"]["excess_over_1000_ms"])
    print("taskiq", report["taskiq_idle"]["gap_stats"], "excess", report["taskiq_idle"]["excess_over_2000_ms"])
    print("dramatiq after 8s", report["dramatiq_idle"]["after_8s_stats"])
    print("arq", report["arq_idle"]["gap_stats"])


if __name__ == "__main__":
    main()
