"""
E11: контроль долгой задачи.

По умолчанию diagnostic (короткий). --full запускает 2400 с при выбранной конфигурации.
Два worker × max_async=1; idle_timeout короче длительности задачи; есть keepalive traffic,
иначе quiet-gate блокирует reclaim (это отдельный факт e02).
"""

from __future__ import annotations

import argparse
import time

from research.taskiq.experiments import e06_long_ownership as e06
from research.taskiq.lib import observe, config


def run(full: bool = False) -> dict:
    if full:
        # 2400s task, idle 600s (library default scale), keepalive so reclaim path reachable
        return e06.run(
            kind="tick",
            idle_ms=600_000,
            task_s=2400.0,
            observe_s=2500.0,
        )
    # diagnostic: 90s task, idle 15s
    result = e06.run(kind="tick", idle_ms=15_000, task_s=90.0, observe_s=120.0)
    result["control_class"] = "diagnostic_not_2400"
    observe.write_json(config.ARTIFACTS / "long" / "e11-diagnostic.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true")
    ns = parser.parse_args()
    print(run(full=ns.full))
