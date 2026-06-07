"""SQLModel schema for the market catalog."""

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Column, Index, Text
from sqlalchemy.types import JSON
from sqlmodel import Field, SQLModel


class Event(SQLModel, table=True):
    """Gamma event — groups one or more markets."""

    __tablename__ = "events"

    id: str = Field(primary_key=True, description="Gamma event id")
    slug: str = Field(index=True)
    title: str = Field(default="")
    series_id: str | None = Field(default=None, index=True)
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    game_start_ts: datetime | None = Field(
        default=None, description="Sports kickoff (Gamma gameStartTime)"
    )
    active: bool = Field(default=True, index=True)
    closed: bool = Field(default=False, index=True)
    tags_json: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    raw_payload: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    # Fee rollup from child markets (refreshed after discovery / link)
    fees_enabled: bool = Field(default=False)
    fee_type: str | None = Field(default=None, index=True)
    maker_base_fee: int | None = None
    taker_base_fee: int | None = None
    fee_schedule: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    first_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class Series(SQLModel, table=True):
    """Recurring market series (e.g. btc-updown-5m)."""

    __tablename__ = "series"

    id: str = Field(primary_key=True)
    slug: str = Field(index=True)
    title: str = Field(default="")
    recurrence_seconds: int | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    first_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class Market(SQLModel, table=True):
    """Single tradeable market (condition)."""

    __tablename__ = "markets"
    __table_args__ = (
        Index("ix_markets_active_closed", "active", "closed"),
        Index("ix_markets_end_ts", "end_ts"),
        Index("ix_markets_game_start_ts", "game_start_ts"),
    )

    condition_id: str = Field(primary_key=True, description="CLOB condition id")
    question_id: str | None = Field(default=None)
    slug: str = Field(index=True)
    question: str = Field(default="")
    event_id: str | None = Field(default=None, index=True)
    series_id: str | None = Field(default=None, index=True)

    # Classification (filled by classifier stage)
    structure: str = Field(default="unknown", index=True)
    domain: str = Field(default="unknown", index=True)
    horizon: str = Field(default="unknown")
    recurrence: str = Field(default="one_off")
    resolution_source: str = Field(default="unknown")
    liquidity_tier: str = Field(default="unknown", index=True)
    neg_risk: bool = Field(default=False)
    neg_risk_market_id: str | None = None

    # Lifecycle
    active: bool = Field(default=True, index=True)
    closed: bool = Field(default=False, index=True)
    accepting_orders: bool = Field(default=True)
    enable_order_book: bool = Field(default=True)
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    game_start_ts: datetime | None = Field(
        default=None,
        description="Scheduled game start; for sports in-play window",
    )

    # CLOB trading params + fees (from Gamma discovery; CLOB backfill if missing)
    tick_size: str | None = None
    min_order_size: str | None = None
    rewards_eligible: bool = Field(default=False)
    fees_enabled: bool = Field(default=False)
    fee_type: str | None = Field(default=None, index=True)
    maker_base_fee: int | None = None
    taker_base_fee: int | None = None
    fee_schedule: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))

    raw_payload: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    first_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class Token(SQLModel, table=True):
    """Outcome token for a market."""

    __tablename__ = "tokens"

    token_id: str = Field(primary_key=True)
    condition_id: str = Field(foreign_key="markets.condition_id", index=True)
    outcome_index: int = Field(default=0)
    outcome_label: str = Field(default="")


class MarketTag(SQLModel, table=True):
    """Many-to-many market ↔ tag."""

    __tablename__ = "market_tags"

    condition_id: str = Field(foreign_key="markets.condition_id", primary_key=True)
    tag: str = Field(primary_key=True, index=True)


class TokenQuote(SQLModel, table=True):
    """Latest CLOB quote per outcome token (updated by REST + WebSocket)."""

    __tablename__ = "token_quotes"

    token_id: str = Field(primary_key=True)
    condition_id: str = Field(foreign_key="markets.condition_id", index=True)
    bid: float | None = None
    ask: float | None = None
    mid: float | None = None
    spread: float | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC), index=True)


class MarketEnrichment(SQLModel, table=True):
    """Liquidity and CLOB metadata (REST refresh)."""

    __tablename__ = "market_enrichment"

    condition_id: str = Field(primary_key=True, foreign_key="markets.condition_id")
    open_interest: float | None = None
    volume_24h: float | None = None
    fee_rate_bps: int | None = None  # feeSchedule.rate * 10000 when present
    maker_base_fee: int | None = None
    taker_base_fee: int | None = None
    rewards_min_size: float | None = None
    rewards_max_spread: float | None = None
    enriched_at: datetime | None = Field(default=None, index=True)


class TaxonomyOverride(SQLModel, table=True):
    """Manual classification overrides (UI / admin)."""

    __tablename__ = "taxonomy_overrides"

    id: int | None = Field(default=None, primary_key=True)
    kind: str = Field(index=True)  # "tag" | "condition_id" | "slug_prefix"
    key: str = Field(index=True)  # tag slug, condition_id, or slug prefix
    domain: str | None = None
    structure: str | None = None
    resolution_source: str | None = None
    note: str = Field(default="")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    __table_args__ = (Index("ix_taxonomy_overrides_kind_key", "kind", "key", unique=True),)


class HarvestOrder(SQLModel, table=True):
    """Execution record for every harvest trade attempt (paper or live)."""

    __tablename__ = "harvest_orders"

    id: str = Field(primary_key=True)  # UUID4
    condition_id: str = Field(index=True)
    token_id: str = Field(index=True)
    side: str  # "yes" | "no"
    domain: str = Field(default="")
    question: str = Field(default="")
    duration_s: int | None = None  # market duration in seconds (300/900/3600)
    underlying: str | None = None  # "btc", "eth", … parsed from question
    price_target: float  # ask used for order sizing
    size_usd: float  # HARVEST_ORDER_USD
    size_shares: float  # size_usd / price_target
    paper: bool = Field(default=True)
    status: str = Field(default="pending")  # pending|filled|cancelled|failed
    clob_order_id: str | None = None
    reject_reason: str | None = None
    net_yield_pct: float | None = None
    pnl_usd: float | None = None
    minutes_left_at_entry: float | None = None
    midpoint_at_entry: float | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC), index=True)
    filled_at: datetime | None = None
    resolved_at: datetime | None = None


class CatalogRun(SQLModel, table=True):
    """Audit log for discovery scan runs."""

    __tablename__ = "catalog_runs"

    id: int | None = Field(default=None, primary_key=True)
    run_type: str = Field(index=True)  # full | delta | bootstrap_closed
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    markets_upserted: int = Field(default=0)
    events_upserted: int = Field(default=0)
    series_upserted: int = Field(default=0)
    pages_fetched: int = Field(default=0)
    errors: int = Field(default=0)
    notes: str = Field(default="", sa_column=Column(Text))
