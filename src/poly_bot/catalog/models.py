"""View models for expiring-market catalog."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ExpiringMarket(BaseModel):
    """Market resolving within the configured horizon, with live attributes."""

    model_config = ConfigDict(frozen=True)

    condition_id: str
    slug: str
    question: str
    domain: str
    structure: str
    recurrence: str
    end_ts: datetime
    game_start_ts: datetime | None = None
    is_live: bool = False
    hours_left: float
    event_id: str | None
    event_title: str | None
    series_slug: str | None
    tags: list[str]
    yes_label: str
    no_label: str
    yes_mid: float | None
    no_mid: float | None
    yes_spread: float | None
    no_spread: float | None
    open_interest: float | None
    volume_24h: float | None
    rewards_eligible: bool
    fees_enabled: bool = False
    fee_type: str | None = None
    fee_schedule: dict[str, object] | None = None
    maker_base_fee: int | None = None
    taker_base_fee: int | None = None
    event_fees_enabled: bool = False
    event_taker_base_fee: int | None = None
    quote_updated_at: datetime | None
    enriched_at: datetime | None
