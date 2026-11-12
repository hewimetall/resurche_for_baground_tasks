"""Точка входа стенда: enqueue и снимок админки. Библиотека выбирается флагом."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys


def _load(lib: str):
    if lib == "simpleq":
        from bench import simpleq_app as mod
    elif lib == "dramatiq":
        from bench import dramatiq_app as mod
    elif lib == "celery":
        from bench import celery_app as mod
    elif lib == "arq":
        from bench import arq_app as mod
    elif lib == "taskiq":
        from bench import taskiq_app as mod
    else:
        raise SystemExit(f"unknown lib {lib}")
    return mod


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    enqueue = sub.add_parser("enqueue")
    enqueue.add_argument("--lib", required=True)
    enqueue.add_argument("--payload", required=True)

    admin = sub.add_parser("admin")
    admin.add_argument("--lib", required=True)

    many = sub.add_parser("many")
    many.add_argument("--lib", required=True)
    many.add_argument("--payload", required=True)

    args = parser.parse_args()
    if args.cmd == "admin":
        result = _admin(args.lib)
    elif args.cmd == "many":
        result = asyncio.run(_many(args.lib, json.loads(args.payload)))
    else:
        payload = json.loads(args.payload)
        result = asyncio.run(_one(args.lib, payload))
    json.dump(result, sys.stdout, default=str)
    sys.stdout.write("\n")


async def _one(lib: str, payload: dict) -> dict:
    mod = _load(lib)
    fn = mod.enqueue
    if asyncio.iscoroutinefunction(fn):
        return await fn(payload["op"], payload)
    return fn(payload["op"], payload)


async def _many(lib: str, payload: dict) -> dict:
    """Один процесс отправляет пачку, чтобы не платить за старт интерпретатора."""
    mod = _load(lib)
    if hasattr(mod, "enqueue_many"):
        result = mod.enqueue_many(payload)
        if asyncio.iscoroutine(result):
            result = await result
        return result
    op = payload["op"]
    count = int(payload["count"])
    job_id = payload["job_id"]
    unique = bool(payload.get("unique"))
    accepted = 0
    rejected = 0
    ids = []
    t0 = asyncio.get_event_loop().time()
    for n in range(count):
        item = {
            "op": op,
            "kwargs": _kwargs(op, job_id, n, payload),
        }
        if unique:
            item["unique_key"] = f"{op}:{job_id}:{n}"
        result = mod.enqueue(op, item)
        if asyncio.iscoroutine(result):
            result = await result
        if result.get("accepted", True):
            accepted += 1
        else:
            rejected += 1
        if n < 3:
            ids.append(result.get("id"))
    return {
        "accepted": accepted,
        "rejected": rejected,
        "seconds": round(asyncio.get_event_loop().time() - t0, 3),
        "sample_ids": ids,
        "native_dedup": bool(payload.get("native_dedup_probe")),
    }


def _kwargs(op: str, job_id: str, n: int, payload: dict) -> dict:
    if op == "fast":
        import time

        return {"job_id": job_id, "n": n, "sent_ms": int(time.time() * 1000)}
    if op == "item":
        return {"job_id": job_id, "n": n}
    if op == "snooze":
        return {"job_id": job_id, "n": n, "seconds": payload.get("seconds", 1)}
    raise SystemExit(f"many does not know {op}")


def _admin(lib: str):
    mod = _load(lib)
    fn = mod.admin_snapshot
    if asyncio.iscoroutinefunction(fn):
        return asyncio.run(fn())
    return fn()


if __name__ == "__main__":
    main()
