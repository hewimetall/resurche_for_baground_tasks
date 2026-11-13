#!/usr/bin/env python3
"""Rebuild results/long/stress/summary.json from manifests and latency files."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path("/workspace/results/long/stress")
OUT = ROOT / "summary.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _latency(path: Path) -> list[tuple[int, int, int]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        n, sent, done = line.split(",")
        rows.append((int(n), int(sent), int(done)))
    return rows


def _log_counts(library: str, path: Path) -> dict:
    if not path.exists():
        return {}
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    counts = {"lines": len(lines)}
    if library == "simpleq":
        counts["deliveries_1"] = sum(1 for line in lines if "deliveries=1" in line)
        counts["deliveries_other"] = sum(
            1 for line in lines if "deliveries=" in line and "deliveries=1" not in line
        )
    elif library == "celery":
        counts["received"] = sum(1 for line in lines if " received" in line)
        counts["succeeded"] = sum(1 for line in lines if "succeeded" in line)
    elif library == "taskiq":
        counts["executing"] = sum(1 for line in lines if "executing" in line.lower())
    elif library == "arq":
        counts["handler_done"] = sum(1 for line in lines if "←" in line and ":fast" in line)
    return counts


def main() -> None:
    runs = []
    for manifest_path in sorted(ROOT.glob("st-*/manifest.json")):
        run_dir = manifest_path.parent
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows = _latency(run_dir / "latency.txt")
        seen = [row[0] for row in rows]
        unique = set(seen)
        missing = [n for n in range(int(manifest["count"])) if n not in unique]
        extra = sorted(n for n in unique if n < 0 or n >= int(manifest["count"]))
        samples = []
        sample_path = run_dir / "samples.jsonl"
        if sample_path.exists():
            samples = [json.loads(line) for line in sample_path.read_text(encoding="utf-8").splitlines() if line]
        rss = [row.get("rss_kb") or 0 for row in samples]
        sent = [row[1] for row in rows]
        done = [row[2] for row in rows]
        item = {
            "run_id": manifest["run_id"],
            "library": manifest["library"],
            "class": manifest["class"],
            "counts_as_2400": manifest["counts_as_2400"],
            "status": manifest["status"],
            "count": manifest["count"],
            "done_lines": len(rows),
            "unique_n": len(unique),
            "duplicate_lines": len(rows) - len(unique),
            "missing_n": len(missing),
            "extra_n": extra[:5],
            "span_s": manifest["span_s"],
            "throughput_per_s": manifest["throughput_per_s"],
            "latency_ms": manifest["latency_ms"],
            "enqueue_s": manifest.get("enqueue", {}).get("seconds"),
            "enqueue_window_ms": (max(sent) - min(sent)) if sent else None,
            "complete_window_ms": (max(done) - min(done)) if done else None,
            "command": manifest["command"],
            "versions": manifest["versions"],
            "max_rss_kb": max(rss) if rss else None,
            "last_broker_sample": samples[-1].get("broker") if samples else None,
            "worker_exit_during_run": manifest["worker_exit_during_run"],
            "sha256_latency": _sha256(run_dir / "latency.txt"),
            "sha256_manifest": _sha256(manifest_path),
            "log_counts": _log_counts(manifest["library"], run_dir / "worker.log"),
        }
        runs.append(item)
    payload = {
        "class": "stress",
        "counts_as_2400": False,
        "bench_commit_at_run": "c81f29f",
        "scenario_id": "S_burst_fast",
        "redis_port": 6412,
        "note": "fast_record appends one latency line. No HTTP and no Postgres write. Not a 2400-second control.",
        "runs": runs,
    }
    OUT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"{'lib':8} {'n':5} {'dup':4} {'span':8} {'per_s':8} {'p50':6} {'enq_ms':7} {'done_ms':7} status")
    for row in runs:
        if row["count"] != 5000:
            continue
        print(
            f"{row['library']:8} {row['unique_n']:5} {row['duplicate_lines']:4} "
            f"{row['span_s']:8} {row['throughput_per_s']:8} {row['latency_ms']['p50']:6} "
            f"{row['enqueue_window_ms']:7} {row['complete_window_ms']:7} {row['status']}"
        )


if __name__ == "__main__":
    main()
