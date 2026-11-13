"""Одна строка JSON на событие. Дописывание короче PIPE_BUF не рвёт строку."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone


def emit(path: str, event: str, **fields: object) -> None:
    row = {
        "event": event,
        "utc": datetime.now(timezone.utc).isoformat(),
        "mono_ns": time.monotonic_ns(),
        "pid": os.getpid(),
    }
    row.update(fields)
    data = (json.dumps(row, ensure_ascii=False, default=str) + "\n").encode()
    fd = os.open(path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)
