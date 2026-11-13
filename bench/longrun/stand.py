"""Изолированные Redis и приёмник внешних эффектов для длительного прогона.

Порты 6379 и 6380 этот модуль не открывает и не очищает.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

OWNED_PREFIX = "/workspace/results/long/"
FORBIDDEN_PORTS = {6379, 6380, 5432, 8090}


def assert_port(port: int) -> None:
    if port in FORBIDDEN_PORTS or port < 6400 or port > 6499:
        raise SystemExit(f"longrun refuses port {port}")


def ensure_redis(port: int, directory: str) -> None:
    assert_port(port)
    directory = os.path.realpath(directory)
    if not directory.startswith(os.path.realpath(OWNED_PREFIX)):
        raise SystemExit(f"redis dir must stay under {OWNED_PREFIX}")
    os.makedirs(directory, exist_ok=True)
    if _ping(port):
        got = _config_dir(port)
        if os.path.realpath(got) != directory:
            raise SystemExit(f"port {port} already serves dir {got}")
        return
    log_path = os.path.join(directory, "redis.log")
    proc = subprocess.Popen(
        [
            "redis-server",
            "--bind",
            "127.0.0.1",
            "--port",
            str(port),
            "--dir",
            directory,
            "--appendonly",
            "yes",
            "--appendfilename",
            "appendonly.aof",
            "--dbfilename",
            "dump.rdb",
            "--save",
            "",
            "--protected-mode",
            "yes",
            "--logfile",
            log_path,
            "--pidfile",
            os.path.join(directory, "redis.pid"),
            "--daemonize",
            "yes",
        ]
    )
    code = proc.wait(timeout=10)
    if code != 0 or not _wait_ping(port):
        raise SystemExit(f"redis on {port} did not start, exit {code}")


def flush_owned(port: int, directory: str) -> None:
    assert_port(port)
    got = os.path.realpath(_config_dir(port))
    if got != os.path.realpath(directory):
        raise SystemExit(f"refusing FLUSHDB on {port}: dir {got}")
    subprocess.check_call(["redis-cli", "-p", str(port), "FLUSHDB"])


def shutdown_save(port: int, directory: str) -> None:
    assert_port(port)
    if not _ping(port):
        return
    got = os.path.realpath(_config_dir(port))
    if got != os.path.realpath(directory):
        raise SystemExit(f"refusing SHUTDOWN on {port}: dir {got}")
    subprocess.check_call(["redis-cli", "-p", str(port), "SHUTDOWN", "SAVE"])


def _ping(port: int) -> bool:
    result = subprocess.run(
        ["redis-cli", "-p", str(port), "PING"],
        capture_output=True,
        text=True,
        timeout=2,
    )
    return result.stdout.strip() == "PONG"


def _wait_ping(port: int) -> bool:
    for _ in range(50):
        if _ping(port):
            return True
        time.sleep(0.1)
    return False


def _config_dir(port: int) -> str:
    result = subprocess.run(
        ["redis-cli", "-p", str(port), "CONFIG", "GET", "dir"],
        capture_output=True,
        text=True,
        check=True,
        timeout=2,
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if len(lines) < 2:
        raise SystemExit(f"CONFIG GET dir failed: {result.stdout!r}")
    return lines[1]


class _EffectHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/health":
            self.send_error(404)
            return
        raw = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        parsed = urlparse(self.path)
        delay = 0.0
        if parsed.path == "/hang":
            delay = float(parse_qs(parsed.query).get("s", ["30"])[0])
            time.sleep(delay)
        elif parsed.path != "/effect":
            self.send_error(404)
            return
        key = self.headers.get("X-Idempotency-Key")
        seen: dict = self.server.seen  # type: ignore[attr-defined]
        duplicate = bool(key and key in seen)
        record = {
            "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "mono_ns": time.monotonic_ns(),
            "path": parsed.path,
            "delay_s": delay,
            "idempotency_key": key,
            "duplicate": duplicate,
            "body": body.decode("utf-8", "replace")[:4000],
        }
        with self.server.lock:  # type: ignore[attr-defined]
            if key and not duplicate:
                seen[key] = record["utc"]
            handle = self.server.journal  # type: ignore[attr-defined]
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            handle.flush()
        raw = json.dumps({"ok": True, "duplicate": duplicate}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args: object) -> None:
        return


def start_effect(port: int, journal_path: str) -> ThreadingHTTPServer:
    assert_port(port)
    os.makedirs(os.path.dirname(journal_path), exist_ok=True)
    server = ThreadingHTTPServer(("127.0.0.1", port), _EffectHandler)
    server.seen = {}  # type: ignore[attr-defined]
    server.lock = threading.Lock()  # type: ignore[attr-defined]
    server.journal = open(journal_path, "a", encoding="utf-8")  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, name="longrun-effect", daemon=True)
    thread.start()
    return server


def stop_effect(server: ThreadingHTTPServer) -> None:
    server.shutdown()
    server.journal.close()  # type: ignore[attr-defined]
