"""Shared state types for the live engine."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import IntEnum


class MarketDuration(IntEnum):
    MIN5 = 300
    MIN15 = 900
    HOUR1 = 3600
    UNKNOWN = -1


_UNDERLYING_RE = re.compile(
    r"\b(bitcoin|btc|ethereum|eth|solana|sol|xrp|ripple|bnb|dogecoin|doge|"
    r"hyperliquid|hype|cardano|ada|avalanche|avax|chainlink|link|matic|polygon)\b",
    re.IGNORECASE,
)
_UNDERLYING_CANONICAL = {
    "bitcoin": "btc", "btc": "btc",
    "ethereum": "eth", "eth": "eth",
    "solana": "sol", "sol": "sol",
    "xrp": "xrp", "ripple": "xrp",
    "bnb": "bnb",
    "dogecoin": "doge", "doge": "doge",
    "hyperliquid": "hype", "hype": "hype",
    "cardano": "ada", "ada": "ada",
    "avalanche": "avax", "avax": "avax",
    "chainlink": "link", "link": "link",
    "matic": "matic", "polygon": "matic",
}


def parse_underlying(question: str) -> str | None:
    m = _UNDERLYING_RE.search(question)
    if not m:
        return None
    return _UNDERLYING_CANONICAL.get(m.group(0).lower())


def infer_duration(start_ts: datetime | None, end_ts: datetime) -> MarketDuration:
    if start_ts is None:
        return MarketDuration.UNKNOWN
    delta_s = int((end_ts - start_ts).total_seconds())
    # allow ±30s tolerance
    for d in (MarketDuration.MIN5, MarketDuration.MIN15, MarketDuration.HOUR1):
        if abs(delta_s - int(d)) <= 30:
            return d
    return MarketDuration.UNKNOWN


@dataclass(frozen=True)
class MonitoredMarket:
    condition_id: str
    question: str
    domain: str
    yes_token_id: str
    no_token_id: str | None
    end_ts: datetime
    start_ts: datetime | None
    duration: MarketDuration
    underlying: str | None  # "btc", "eth", …

    @property
    def minutes_left(self) -> float:
        return max(0.0, (self.end_ts - datetime.now(UTC)).total_seconds() / 60.0)

    @property
    def expired(self) -> bool:
        return datetime.now(UTC) >= self.end_ts


@dataclass
class WatchlistEntry:
    market: MonitoredMarket
    side: str               # "yes" | "no"
    token_id: str
    alert_price: float      # midpoint that triggered Stage-2
    added_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_price: float | None = None


@dataclass
class ExecutionCandidate:
    """A market that passed all guards and is ready for order placement."""
    market: MonitoredMarket
    side: str
    token_id: str
    ask: float              # confirmed ask from /book
    mid: float
    fillable_usd: float
    net_yield_pct: float
    minutes_left: float


@dataclass
class EngineStats:
    stage1_markets: int = 0
    watchlist_size: int = 0
    orders_placed: int = 0
    orders_paper: int = 0
    orders_failed: int = 0
    last_stage1_at: datetime | None = None
    last_order_at: datetime | None = None
