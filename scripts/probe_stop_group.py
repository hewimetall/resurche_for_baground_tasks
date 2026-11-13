#!/usr/bin/env python3
"""Short check of leader-exit detection. Not a 2400s run.

A session leader that exits on SIGTERM stays a zombie until its parent
waits. kill(pid, 0) still succeeds. The replacement stop path uses
Popen.poll and ignores zombies when it scans the process group.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path("/workspace")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from long_run import _proc_stat, _stop_group  # noqa: E402

EXIT_ON_TERM = """
import os, signal, time
signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
time.sleep(60)
"""

IGNORE_TERM = """
import os, signal, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(60)
"""


def _kill0(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def old_check(code: str) -> dict:
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        start_new_session=True,
    )
    time.sleep(0.2)
    os.killpg(proc.pid, signal.SIGTERM)
    time.sleep(0.3)
    before = {"kill0": _kill0(proc.pid), "state": _proc_stat(proc.pid), "poll": None}
    waited = proc.poll()
    after = {"kill0": _kill0(proc.pid), "state": _proc_stat(proc.pid), "poll": waited}
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=2)
    return {"before_poll": before, "after_poll": after}


def new_check(code: str, grace_s: float) -> dict:
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        start_new_session=True,
    )
    time.sleep(0.2)
    started = time.monotonic()
    result = _stop_group(proc, grace_s=grace_s)
    result["elapsed_s"] = round(time.monotonic() - started, 3)
    result["state_after"] = _proc_stat(proc.pid)
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=2)
    return result


def main() -> None:
    report = {
        "class": "short_bench",
        "counts_as_2400": False,
        "old_kill0_on_process_that_exits": old_check(EXIT_ON_TERM),
        "new_stop_on_process_that_exits": new_check(EXIT_ON_TERM, 2.0),
        "new_stop_on_process_that_ignores_sigterm": new_check(IGNORE_TERM, 0.6),
    }
    out = ROOT / "results" / "long" / "stop-group-probe.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
