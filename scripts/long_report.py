"""Сводка results/long из сырых events.jsonl. Повторный запуск пишет те же числа."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path("/workspace/results/long")


def _rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def one(run_dir: Path) -> dict:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    events = _rows(run_dir / "events.jsonl")
    effects = _rows(run_dir / "effects.jsonl")
    counts: dict[str, int] = {}
    for row in events:
        counts[row.get("event", "?")] = counts.get(row.get("event", "?"), 0) + 1
    return {
        "run_id": manifest.get("run_id"),
        "scenario_id": manifest.get("scenario_id"),
        "library": manifest.get("library"),
        "class": manifest.get("class"),
        "status": manifest.get("status"),
        "source_commit": manifest.get("source_commit"),
        "planned_s": manifest.get("planned_s"),
        "span_s": manifest.get("span_s"),
        "time_limit_s": manifest.get("time_limit_s"),
        "handler_starts": counts.get("handler_started", 0),
        "handler_finishes": counts.get("handler_finished", 0),
        "progress": counts.get("progress", 0),
        "effects_logged": len(effects),
        "effect_duplicates": sum(1 for row in effects if row.get("duplicate")),
        "event_counts": counts,
        "dir": str(run_dir),
    }


def main() -> None:
    runs = []
    if ROOT.exists():
        for path in sorted(p for p in ROOT.iterdir() if (p / "manifest.json").exists()):
            runs.append(one(path))
    text = json.dumps(runs, indent=2) + "\n"
    out = ROOT / "summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
