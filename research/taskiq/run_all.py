"""Запуск набора Taskiq-only экспериментов."""

from __future__ import annotations

import json
import traceback
from pathlib import Path

from research.taskiq.lib import config, observe


def main() -> None:
    from research.taskiq.experiments import (
        e01_batch_pel,
        e02_quiet_reclaim,
        e03_traffic_reclaim,
        e04_lock_death,
        e05_ack_order,
        e06_long_ownership,
        e07_prefetch_recycle,
        e08_shutdown,
        e09_redis_fault,
        e10_perf,
        e11_long_control,
    )

    env = config.as_dict(config.env_snapshot())
    observe.write_json(config.ARTIFACTS / "env.json", env)

    plan = [
        ("e01", e01_batch_pel.run),
        ("e04", e04_lock_death.run),
        ("e02", e02_quiet_reclaim.run),
        ("e03", e03_traffic_reclaim.run),
        ("e05", e05_ack_order.run),
        ("e07", e07_prefetch_recycle.run),
        ("e08", e08_shutdown.run),
        ("e06_matrix", e06_long_ownership.run_matrix),
        ("e09", e09_redis_fault.run),
        ("e10", e10_perf.run),
        ("e11_diagnostic", e11_long_control.run),
    ]
    results = {}
    for name, fn in plan:
        print(f"=== RUN {name} ===", flush=True)
        try:
            results[name] = fn()
            print(f"=== DONE {name}: {results[name].get('status', 'ok')} ===", flush=True)
        except Exception as exc:
            results[name] = {
                "status": "environment_error" if "redis" in str(exc).lower() else "error",
                "error": repr(exc),
                "traceback": traceback.format_exc(),
            }
            print(f"=== FAIL {name}: {exc!r} ===", flush=True)

    observe.write_json(config.ARTIFACTS / "summary.json", {"env": env, "results": results})
    print(json.dumps({k: (v.get("status") if isinstance(v, dict) else "ok") for k, v in results.items()}, indent=2))


if __name__ == "__main__":
    main()
