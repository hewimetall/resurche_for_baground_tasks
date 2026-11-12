"""Mock HTTP: считает вызовы по (job, item)."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

COUNTS: dict[tuple[str, int], int] = {}
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        return

    def _read(self) -> dict:
        size = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(size) if size else b"{}"
        return json.loads(raw.decode() or "{}")

    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:
        if self.path == "/reset":
            with LOCK:
                COUNTS.clear()
            self._send(200, {"ok": True})
            return
        if self.path != "/call":
            self._send(404, {"error": "not found"})
            return
        payload = self._read()
        key = (str(payload["job"]), int(payload["item"]))
        with LOCK:
            COUNTS[key] = COUNTS.get(key, 0) + 1
            n = COUNTS[key]
        self._send(200, {"ok": True, "n": n})

    def do_GET(self) -> None:
        if self.path != "/stats":
            self._send(404, {"error": "not found"})
            return
        with LOCK:
            dup = sum(1 for v in COUNTS.values() if v > 1)
            body = {"keys": len(COUNTS), "calls": sum(COUNTS.values()), "duplicated_keys": dup}
        self._send(200, body)


def main() -> None:
    port = int(__import__("os").environ.get("MOCK_PORT", "8090"))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
