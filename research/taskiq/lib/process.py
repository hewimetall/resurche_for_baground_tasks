"""Жизненный цикл процессов: Popen.poll/wait, не kill(pid,0)."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence


@dataclass
class StopRecord:
    sigterm_at: float | None
    exit_at: float | None
    code: int | None
    killed_after_grace: bool
    t_drain_s: float | None
    already_exited: bool
    children_reaped: list[dict]


def start(
    cmd: Sequence[str],
    *,
    env: dict[str, str] | None = None,
    cwd: str | Path | None = None,
    log_path: Path | None = None,
) -> subprocess.Popen:
    log_handle = None
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handle = log_path.open("w", encoding="utf-8")
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.Popen(
        list(cmd),
        env=merged,
        cwd=str(cwd) if cwd else None,
        stdout=log_handle or subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )


def poll(proc: subprocess.Popen) -> int | None:
    return proc.poll()


def wait_alive(proc: subprocess.Popen, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if poll(proc) is not None:
            raise RuntimeError(f"process exited early code={proc.returncode}")
        time.sleep(0.05)


def stop_group(
    proc: subprocess.Popen,
    *,
    grace_s: float = 5.0,
    final_sig: int = signal.SIGKILL,
) -> StopRecord:
    """SIGTERM на process group, затем reap через poll/wait."""
    already = poll(proc) is not None
    sigterm_at = None
    killed = False
    children: list[dict] = []
    if not already:
        sigterm_at = time.monotonic()
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + grace_s
    while poll(proc) is None and time.monotonic() < deadline:
        time.sleep(0.05)
    if poll(proc) is None:
        killed = True
        try:
            os.killpg(proc.pid, final_sig)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    else:
        # Reap leader
        try:
            proc.wait(timeout=0.1)
        except subprocess.TimeoutExpired:
            pass
    exit_at = time.monotonic()
    children.append(
        {
            "pid": proc.pid,
            "code": proc.returncode,
            "reaped": True,
        }
    )
    drain = None if sigterm_at is None else round(exit_at - sigterm_at, 3)
    return StopRecord(
        sigterm_at=sigterm_at,
        exit_at=exit_at,
        code=proc.returncode,
        killed_after_grace=killed,
        t_drain_s=drain,
        already_exited=already,
        children_reaped=children,
    )


def sigkill_execution_child(leader: subprocess.Popen) -> list[int]:
    """Убить детей leader (execution workers), leader оставить если возможно."""
    killed: list[int] = []
    try:
        import psutil
    except ImportError:
        # fallback: kill process group member by scanning /proc
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                status = (entry / "status").read_text(encoding="utf-8")
            except OSError:
                continue
            ppid = None
            for line in status.splitlines():
                if line.startswith("PPid:"):
                    ppid = int(line.split()[1])
                    break
            if ppid == leader.pid:
                pid = int(entry.name)
                os.kill(pid, signal.SIGKILL)
                killed.append(pid)
        return killed
    parent = psutil.Process(leader.pid)
    for child in parent.children(recursive=True):
        try:
            child.kill()
            killed.append(child.pid)
        except psutil.Error:
            pass
    return killed


def as_dict(record: StopRecord) -> dict:
    return asdict(record)
