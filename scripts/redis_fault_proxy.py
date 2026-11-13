"""TCP proxy that can drop a Redis command or only its reply.

The Redis process stays up. A dropped request is never written to the server.
A dropped or delayed reply is written, the full reply is read back, and only
then the client is closed or made to wait. RESP2 and the RESP3 types Redis 7
sends after HELLO are forwarded as raw bytes.
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
import time
from typing import Any

# SHA1 of the apalis-redis 1.1.1 scripts embedded in simple-q 1.2.0.
# Confirmed by matching EVALSHA on a live worker. Redis keeps this cache
# across FLUSHDB, so a new proxy would otherwise see only the hash.
KNOWN_SHA = {
    "f053fd9b41b61a2d32b8b7d8dba3f1a96e3aa6e1": "fetch",
    "d30754a560d0f0ae884decdcb986ceb755020d07": "renew",
    "9a702ec86f2a2dcdd3f53342b4e7cdce332871cd": "finish",
    "5c1d697dfa0d68de2954ceefa235f87162ecc839": "keepalive",
    "f7ff133460a6cd6a739e294b0f4f7a584599f1fc": "acquire",
    "069de2f0e15b5d3b604f2282151a8bed17d0ea52": "release",
    "6b6f3808de063cd4ca38e9e11b6968a6cc826069": "reenqueue",
    "0d5fbce91e1dcff1121ece41f7169da3c0e39194": "register",
    "3db3e4f4b96d92c5dd8874e33f1dd4c0152c0682": "enqueue",
}

MARKERS = (
    (b"Cant fetch next", "fetch"),
    (b"Cant renew leases", "renew"),
    (b"Cant acquire lease", "acquire"),
    (b"task not executed by worker", "finish"),
    (b"Heartbeat failed: worker not registered", "keepalive"),
    (b"worker is still active within threshold", "register"),
    (b"specific job IDs to release", "release"),
    (b"the idempotency_ttl", "enqueue"),
    (b"the timestamp before which a worker is considered expired", "reenqueue"),
)


def script_kind(script: bytes) -> str | None:
    for marker, kind in MARKERS:
        if marker in script:
            return kind
    return None


class FaultProxy:
    def __init__(self, listen_port: int, redis_port: int) -> None:
        self.listen_port = listen_port
        self.redis_port = redis_port
        self.rule: dict[str, Any] | None = None
        self.sha: dict[bytes, str] = {}
        for digest, kind in KNOWN_SHA.items():
            encoded = digest.encode()
            self.sha[encoded] = kind
            self.sha[encoded.upper()] = kind
        self.events: list[dict[str, Any]] = []
        self.counts: dict[str, int] = {}
        self.total = 0
        self.ops: list[dict[str, Any]] = []
        self.fired = threading.Event()
        self._lock = threading.Lock()
        self._blackhole_until = 0.0
        self._writers: set[asyncio.StreamWriter] = set()
        self._pending_delay = 0.0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="redis-fault-proxy", daemon=True)
        self._thread.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self._loop is not None:
                return
            time.sleep(0.01)
        raise RuntimeError("proxy loop did not start")

    def arm(self, kind: str, mode: str, **extra: Any) -> None:
        self.fired.clear()
        with self._lock:
            self.rule = {"kind": kind, "mode": mode, "once": True, **extra}

    def disarm(self) -> None:
        with self._lock:
            self.rule = None

    def reset_stats(self) -> None:
        self.fired.clear()
        with self._lock:
            self.events.clear()
            self.counts.clear()
            self.total = 0
            self.ops.clear()

    def blackhole(self, seconds: float) -> None:
        until = time.monotonic() + seconds
        with self._lock:
            self._blackhole_until = until
            writers = list(self._writers)
        loop = self._loop
        if loop is not None:
            for writer in writers:
                loop.call_soon_threadsafe(writer.close)
        self._note("blackhole", {"seconds": seconds})

    def blackholed(self) -> bool:
        with self._lock:
            return time.monotonic() < self._blackhole_until

    def wait(self, timeout: float) -> bool:
        return self.fired.wait(timeout)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            rule = dict(self.rule) if self.rule else None
            if rule and isinstance(rule.get("payload_contains"), bytes):
                rule["payload_contains"] = rule["payload_contains"].decode("ascii", "replace")
            return {
                "armed": rule,
                "counts": dict(self.counts),
                "events": list(self.events),
                "commands": self.total,
                "ops": list(self.ops),
            }

    def _note(self, action: str, fields: dict[str, Any]) -> None:
        item = {"t": time.time(), "mono": time.monotonic(), "action": action, **fields}
        with self._lock:
            self.counts[action] = self.counts.get(action, 0) + 1
            kind = fields.get("kind")
            if isinstance(kind, str):
                self.counts[f"seen:{kind}"] = self.counts.get(f"seen:{kind}", 0) + 1
            if action != "pass":
                self.events.append(item)
            if action in {"drop_request", "drop_response", "delay_response", "release_response"}:
                self.fired.set()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._serve())

    async def _serve(self) -> None:
        server = await asyncio.start_server(self._client, "127.0.0.1", self.listen_port)
        async with server:
            await server.serve_forever()

    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.blackholed():
            writer.close()
            return
        self._writers.add(writer)
        upstream_writer = None
        try:
            remote_reader, upstream_writer = await asyncio.open_connection(
                "127.0.0.1", self.redis_port
            )
            while not self.blackholed():
                raw, parts = await read_command(reader)
                with self._lock:
                    self.total += 1
                    if len(self.ops) < 40:
                        head = parts[0] if parts else b""
                        self.ops.append(
                            {
                                "op": head.decode("ascii", "replace") if isinstance(head, bytes) else str(type(head)),
                                "argc": len(parts),
                                "prefix": raw[:80].decode("ascii", "replace"),
                            }
                        )
                kind = self._remember(parts, raw)
                action, delay = self._match(parts, kind)
                if action == "drop_request":
                    self._note("drop_request", {"kind": kind, "op": _op(parts)})
                    break
                upstream_writer.write(raw)
                await upstream_writer.drain()
                reply = await read_reply(remote_reader, writer)
                if action == "inspect":
                    if self._reply_matches(reply):
                        self._note(
                            "drop_response",
                            {"kind": kind, "op": _op(parts), "reply_len": len(reply)},
                        )
                        break
                    action = "pass"
                if action == "drop_response":
                    self._note(
                        "drop_response",
                        {"kind": kind, "op": _op(parts), "reply_len": len(reply)},
                    )
                    break
                if action == "delay_response":
                    self._note("delay_response", {"kind": kind, "op": _op(parts), "delay_s": delay})
                    await asyncio.sleep(delay)
                    if self.blackholed():
                        break
                    writer.write(reply)
                    await writer.drain()
                    self._note("release_response", {"kind": kind, "op": _op(parts)})
                    continue
                writer.write(reply)
                await writer.drain()
                if kind:
                    self._note("pass", {"kind": kind})
        except (asyncio.IncompleteReadError, ConnectionError, BrokenPipeError, OSError):
            return
        finally:
            self._writers.discard(writer)
            writer.close()
            if upstream_writer is not None:
                upstream_writer.close()

    def _remember(self, parts: list[bytes], raw: bytes = b"") -> str | None:
        if not parts or not isinstance(parts[0], bytes):
            return script_kind(raw)
        op = parts[0].upper()
        script = None
        if op == b"EVAL" and len(parts) > 1 and isinstance(parts[1], bytes):
            script = parts[1]
        elif (
            op == b"SCRIPT"
            and len(parts) > 2
            and isinstance(parts[1], bytes)
            and parts[1].upper() == b"LOAD"
            and isinstance(parts[2], bytes)
        ):
            script = parts[2]
        if script is not None:
            kind = script_kind(script)
            if kind:
                digest = hashlib.sha1(script).hexdigest().encode()
                with self._lock:
                    self.sha[digest] = kind
                    self.sha[digest.upper()] = kind
            if op == b"SCRIPT":
                return None
            return kind
        if op == b"EVALSHA" and len(parts) > 1 and isinstance(parts[1], bytes):
            with self._lock:
                return self.sha.get(parts[1].lower())
        if op in {b"SET", b"INCR", b"GET"}:
            return op.decode().lower()
        found = script_kind(raw)
        return found

    def _match(self, parts: list[bytes], kind: str | None) -> tuple[str, float]:
        with self._lock:
            rule = dict(self.rule) if self.rule else None
            if not rule or kind != rule.get("kind"):
                return "pass", 0.0
            if rule.get("require_task_ids") and not _has_task_ids(parts):
                return "pass", 0.0
            mode = str(rule["mode"])
            delay = float(rule.get("delay_s") or 0.0)
            if mode == "drop_response" and rule.get("require_payload"):
                return "inspect", 0.0
            if rule.get("once", True):
                self.rule = None
        if mode == "drop_request":
            return "drop_request", 0.0
        if mode == "drop_response":
            return "drop_response", 0.0
        if mode == "delay_response":
            return "delay_response", delay
        return "pass", 0.0

    def _reply_matches(self, reply: bytes) -> bool:
        with self._lock:
            rule = self.rule
            if not rule or rule.get("mode") != "drop_response":
                return False
            if reply.startswith(b"-"):
                return False
            needle = rule.get("payload_contains")
            matched = needle in reply if needle is not None else b"$" in reply
            if matched and rule.get("once", True):
                self.rule = None
            return matched


def _op(parts: list[bytes]) -> str:
    if not parts:
        return ""
    return parts[0].decode("ascii", "replace")


def _has_task_ids(parts: list[bytes]) -> bool:
    if len(parts) < 3 or not isinstance(parts[0], bytes) or not isinstance(parts[2], bytes):
        return False
    if parts[0].upper() not in {b"EVAL", b"EVALSHA"}:
        return False
    try:
        numkeys = int(parts[2])
    except ValueError:
        return False
    args = parts[3 + numkeys :]
    return len(args) > 3


async def read_command(reader: asyncio.StreamReader) -> tuple[bytes, list[bytes]]:
    raw = await read_value(reader)
    return raw, _parse_array(raw)


async def read_reply(reader: asyncio.StreamReader, client: asyncio.StreamWriter) -> bytes:
    while True:
        reply = await read_value(reader)
        if reply.startswith(b">"):
            client.write(reply)
            await client.drain()
            continue
        return reply


async def read_value(reader: asyncio.StreamReader) -> bytes:
    prefix = await reader.readexactly(1)
    if prefix in b"+-:_#,(":
        return prefix + await reader.readuntil(b"\r\n")
    if prefix in b"$!=":
        return prefix + await _read_bulk(reader)
    if prefix in b"*~>":
        line = await reader.readuntil(b"\r\n")
        count = int(line[:-2])
        out = prefix + line
        if count < 0:
            return out
        for _ in range(count):
            out += await read_value(reader)
        return out
    if prefix == b"%":
        line = await reader.readuntil(b"\r\n")
        count = int(line[:-2])
        out = prefix + line
        for _ in range(count * 2):
            out += await read_value(reader)
        return out
    if prefix == b"|":
        line = await reader.readuntil(b"\r\n")
        count = int(line[:-2])
        out = prefix + line
        for _ in range(count * 2):
            out += await read_value(reader)
        out += await read_value(reader)
        return out
    raise RuntimeError(f"unknown RESP prefix {prefix!r}")


async def _read_bulk(reader: asyncio.StreamReader) -> bytes:
    line = await reader.readuntil(b"\r\n")
    size = int(line[:-2])
    if size < 0:
        return line
    return line + await reader.readexactly(size + 2)


def _parse_array(raw: bytes) -> list[bytes]:
    if not raw.startswith(b"*"):
        return []
    items, _ = _walk(raw, 0)
    return items if isinstance(items, list) else []


def _walk(raw: bytes, index: int) -> tuple[Any, int]:
    prefix = raw[index : index + 1]
    end = raw.index(b"\r\n", index)
    header = raw[index + 1 : end]
    cursor = end + 2
    if prefix == b"*":
        count = int(header)
        items = []
        for _ in range(count):
            item, cursor = _walk(raw, cursor)
            items.append(item)
        return items, cursor
    if prefix == b"$":
        size = int(header)
        if size < 0:
            return None, cursor
        blob = raw[cursor : cursor + size]
        return blob, cursor + size + 2
    return header, cursor
