"""Paper log for harvest scan (JSONL)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from poly_bot.harvest.scan import HarvestOpportunity
from poly_bot.settings import Settings


def paper_log_path(settings: Settings) -> Path:
    return settings.poly_bot_data_dir / "harvest_paper.jsonl"


def append_paper_log(settings: Settings, opportunities: list[HarvestOpportunity]) -> Path:
    """Append one JSON line per opportunity (scan snapshot)."""
    path = paper_log_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    scanned_at = datetime.now(UTC).isoformat()
    with path.open("a", encoding="utf-8") as fh:
        for o in opportunities:
            row = {
                "scanned_at": scanned_at,
                "condition_id": o.condition_id,
                "slug": o.slug,
                "domain": o.domain,
                "side": o.side,
                "token_id": o.token_id,
                "ask": o.ask,
                "mid": o.mid,
                "net_yield_pct": round(o.net_yield_pct * 100, 4),
                "fillable_usd": round(o.fillable_usd, 2),
                "minutes_left": round(o.minutes_left, 1),
                "window_end_ts": o.window_end_ts.isoformat(),
                "question": o.question[:200],
            }
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return path


def read_paper_log(settings: Settings, *, limit: int = 50) -> list[dict[str, object]]:
    path = paper_log_path(settings)
    if not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    out: list[dict[str, object]] = []
    for line in lines[-limit:]:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out
