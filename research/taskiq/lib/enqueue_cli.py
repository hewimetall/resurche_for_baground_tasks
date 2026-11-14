"""CLI постановки задач. Env должен быть задан до импорта брокера."""

from __future__ import annotations

import argparse
import asyncio
import json
import time


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("task")
    parser.add_argument("--args-json", default="[]")
    parser.add_argument("--kwargs-json", default="{}")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--fast-stagger", action="store_true")
    ns = parser.parse_args()
    args = json.loads(ns.args_json)
    kwargs = json.loads(ns.kwargs_json)

    from research.taskiq.app.broker import broker

    await broker.startup()
    try:
        task = broker.find_task(ns.task)
        if task is None:
            raise SystemExit(f"unknown task {ns.task}")
        ids = []
        for i in range(ns.count):
            call_args = list(args)
            call_kwargs = dict(kwargs)
            if ns.task == "fast":
                # args: job_id, n, sent_mono
                if len(call_args) >= 3:
                    call_args[1] = i
                    call_args[2] = time.monotonic()
                else:
                    call_kwargs["n"] = i
                    call_kwargs["sent_mono"] = time.monotonic()
            elif ns.count > 1 and ns.task in {"sleep_async", "sleep_block", "sleep_sync", "tick"}:
                # make job ids unique when batching identical templates
                if call_args:
                    call_args[0] = f"{call_args[0]}-{i}"
            sent = await task.kiq(*call_args, **call_kwargs)
            ids.append(sent.task_id)
            if ns.fast_stagger:
                await asyncio.sleep(0)
        print(json.dumps({"ids": ids}))
    finally:
        await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
