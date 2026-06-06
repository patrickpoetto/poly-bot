"""Scan expiring catalog for strategy-priced outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from sqlmodel import Session

from poly_bot.catalog.expiring import list_expiring_markets
from poly_bot.catalog.models import ExpiringMarket
from poly_bot.harvest.pricing import buy_yield_pct, passes_harvest_price_band
from poly_bot.settings import Settings


@dataclass(frozen=True, slots=True)
class HarvestOpportunity:
    condition_id: str
    slug: str
    question: str
    domain: str
    side: Literal["yes", "no"]
    outcome_label: str
    price: float
    gross_yield_pct: float
    net_yield_pct: float
    fee_per_share: float
    hours_left: float
    hours_to_kickoff: float | None
    window_end_ts: datetime
    resolution_kind: str
    is_live: bool
    quote_age_s: float | None
    event_id: str | None
    event_title: str | None


def _quote_age_s(m: ExpiringMarket, now: datetime) -> float | None:
    if m.quote_updated_at is None:
        return None
    ts = m.quote_updated_at
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return max(0.0, (now - ts.astimezone(UTC)).total_seconds())


def _evaluate_side(
    m: ExpiringMarket,
    *,
    side: Literal["yes", "no"],
    price: float,
    label: str,
    settings: Settings,
    quote_age_s: float | None,
) -> HarvestOpportunity | None:
    if not passes_harvest_price_band(
        price,
        min_buy_price=settings.harvest_min_buy_price,
        max_buy_price=settings.harvest_max_buy_price,
    ):
        return None

    gross, net, fee = buy_yield_pct(
        price,
        fees_enabled=m.fees_enabled,
        fee_type=m.fee_type,
        fee_schedule=m.fee_schedule,
        domain=m.domain,
        event_fees_enabled=m.event_fees_enabled,
        event_fee_type=None,
        event_fee_schedule=None,
    )
    if net < settings.harvest_min_net_yield_pct:
        return None

    window_end = m.window_end_ts or m.end_ts
    return HarvestOpportunity(
        condition_id=m.condition_id,
        slug=m.slug,
        question=m.question,
        domain=m.domain,
        side=side,
        outcome_label=label,
        price=price,
        gross_yield_pct=gross,
        net_yield_pct=net,
        fee_per_share=fee,
        hours_left=m.hours_left,
        hours_to_kickoff=m.hours_to_kickoff,
        window_end_ts=window_end,
        resolution_kind=m.resolution_kind,
        is_live=m.is_live,
        quote_age_s=quote_age_s,
        event_id=m.event_id,
        event_title=m.event_title,
    )


def scan_harvest_opportunities(
    session: Session,
    settings: Settings,
    *,
    hours: float | None = None,
    hot_only: bool = True,
    require_fresh_quotes: bool = True,
) -> list[HarvestOpportunity]:
    """Markets in window whose Yes or No mid fits the harvest price band."""
    horizon = hours if hours is not None else settings.catalog_expiring_hours
    markets = list_expiring_markets(
        session,
        hours=horizon,
        sports_in_play_hours=settings.catalog_sports_in_play_hours,
    )
    now = datetime.now(UTC)
    hot_cutoff = settings.catalog_quote_hot_hours
    max_quote_age = settings.harvest_quote_max_age_seconds

    out: list[HarvestOpportunity] = []
    for m in markets:
        if m.hours_left <= 0:
            continue
        if hot_only and m.hours_left > hot_cutoff:
            continue

        quote_age = _quote_age_s(m, now)
        if require_fresh_quotes and max_quote_age > 0:
            if quote_age is None or quote_age > max_quote_age:
                continue

        sides: list[tuple[Literal["yes", "no"], float | None, str]] = [
            ("yes", m.yes_mid, m.yes_label),
            ("no", m.no_mid, m.no_label),
        ]
        for side, mid, label in sides:
            if mid is None:
                continue
            opp = _evaluate_side(
                m,
                side=side,
                price=mid,
                label=label,
                settings=settings,
                quote_age_s=quote_age,
            )
            if opp is not None:
                out.append(opp)

    out.sort(key=lambda o: (-o.net_yield_pct, o.hours_left, o.condition_id))
    return out
