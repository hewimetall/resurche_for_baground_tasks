"""Постановка одной задачи hold. Процесс отделён от воркера."""

from __future__ import annotations

import asyncio
import json
import os


def main() -> None:
    lib = os.environ["LONGRUN_LIB"]
    payload = json.loads(os.environ["LONGRUN_PAYLOAD"])
    if lib == "simpleq":
        from bench.simpleq_app import enqueue

        result = asyncio.run(enqueue("hold", payload))
    elif lib == "arq":
        from bench.arq_app import enqueue

        result = asyncio.run(enqueue("hold", payload))
    elif lib == "taskiq":
        from bench.taskiq_app import enqueue

        result = asyncio.run(enqueue("hold", payload))
    elif lib == "dramatiq":
        from bench.dramatiq_app import enqueue

        result = enqueue("hold", payload)
    elif lib == "celery":
        from bench.celery_app import enqueue

        result = enqueue("hold", payload)
    else:
        raise SystemExit(lib)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
