"""Append-only JSONL decision log for post-session analysis.

Each line is a JSON object with at minimum:
  ts        — ISO-8601 UTC timestamp
  event     — event type (see EVENT_* constants below)
  ...fields — event-specific payload

Read with:
  import json
  rows = [json.loads(l) for l in open("data/engine_decisions.jsonl")]
  # or: jq 'select(.event=="order_placed")' data/engine_decisions.jsonl
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from poly_bot.settings import Settings

log = logging.getLogger(__name__)

# ── event type constants ───────────────────────────────────────────────────
WATCHLIST_ADD       = "watchlist_add"
WATCHLIST_DROP      = "watchlist_drop"
DIRECTION_SKIP      = "direction_skip"
BOOK_SANITY_REJECT  = "book_sanity_reject"
CANDIDATE_EMIT      = "candidate_emit"
POLICY_REJECT       = "policy_reject"
BAND_REJECT         = "band_reject"
ORDER_PLACED        = "order_placed"
ORDER_FAILED        = "order_failed"
STOP_LOSS           = "stop_loss"
GROUP_SKIP           = "group_skip"
CANCELLED_REJECT     = "cancelled_reject"
DEPTH_REJECT         = "depth_reject"
CHAIN_TICKET_CREATED = "chain_ticket_created"
CHAIN_ORDER_PLACED   = "chain_order_placed"


def append(settings: Settings, event: str, **fields: Any) -> None:
    """Append one decision record to data/engine_decisions.jsonl."""
    path = settings.poly_bot_data_dir / "engine_decisions.jsonl"
    row: dict[str, Any] = {
        "ts": datetime.now(UTC).isoformat(),
        "event": event,
    }
    row.update({k: v for k, v in fields.items() if v is not None})
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")
    except OSError as exc:
        log.warning("decision_log write failed: %s", exc)
