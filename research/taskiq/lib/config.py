"""Единый источник конфигурации исследовательского стенда Taskiq."""

from __future__ import annotations

import os
import platform
import socket
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import importlib.metadata as metadata

ROOT = Path(__file__).resolve().parents[3]
RESEARCH = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "results" / "taskiq-deep"
VENV_BIN = ROOT / ".venv" / "bin"

# Baseline from PR #1; актуально на 2026-11-13 совпадает с PyPI latest stable.
BASELINE_TASKIQ = "0.13.0"
BASELINE_TASKIQ_REDIS = "1.2.4"
TASKIQ_GIT_SHA = "a00e4b0030f5ada13068da8644048867967aeda4"
TASKIQ_REDIS_GIT_SHA = "8f70bc85dd6a469cae7d3b467af7dea720c92434"

DEFAULT_REDIS_URL = os.environ.get("TASKIQ_RESEARCH_REDIS_URL", "redis://127.0.0.1:6413/0")
DEFAULT_STREAM = os.environ.get("TASKIQ_RESEARCH_STREAM", "tqresearch")
DEFAULT_GROUP = os.environ.get("TASKIQ_RESEARCH_GROUP", "tqresearch")


@dataclass(frozen=True)
class EnvSnapshot:
    python: str
    platform: str
    hostname: str
    cwd: str
    redis_url: str
    taskiq: str
    taskiq_redis: str
    redis_py: str
    taskiq_sha: str
    taskiq_redis_sha: str
    repo_sha: str
    baseline_matches_current: bool


def env_snapshot(redis_url: str = DEFAULT_REDIS_URL) -> EnvSnapshot:
    taskiq_v = metadata.version("taskiq")
    tr_v = metadata.version("taskiq-redis")
    return EnvSnapshot(
        python=sys.version.split()[0],
        platform=platform.platform(),
        hostname=socket.gethostname(),
        cwd=str(Path.cwd()),
        redis_url=redis_url,
        taskiq=taskiq_v,
        taskiq_redis=tr_v,
        redis_py=metadata.version("redis"),
        taskiq_sha=TASKIQ_GIT_SHA,
        taskiq_redis_sha=TASKIQ_REDIS_GIT_SHA,
        repo_sha=_git_sha(),
        baseline_matches_current=(
            taskiq_v == BASELINE_TASKIQ and tr_v == BASELINE_TASKIQ_REDIS
        ),
    )


def _git_sha() -> str:
    head = ROOT / ".git" / "HEAD"
    try:
        import subprocess

        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
        ).strip()
        return out
    except Exception:
        return head.read_text(encoding="utf-8").strip() if head.exists() else "unknown"


def as_dict(snapshot: EnvSnapshot) -> dict:
    return asdict(snapshot)
