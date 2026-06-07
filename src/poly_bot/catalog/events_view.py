"""Aggregate expiring markets by event (fee visibility)."""

from __future__ import annotations

from dataclasses import dataclass

from poly_bot.catalog.models import ExpiringMarket
from poly_bot.core.fees import effective_fee_rate, fee_summary, format_taker_fee


@dataclass(frozen=True)
class ExpiringEvent:
    event_id: str
    title: str
    slug: str | None
    domain: str
    market_count: int
    min_hours_left: float
    max_hours_left: float
    total_volume_24h: float
    fees_enabled: bool
    fee_type: str | None
    fee_rate: float | None
    markets_with_fees: int


def group_expiring_by_event(markets: list[ExpiringMarket]) -> list[ExpiringEvent]:
    by_event: dict[str, list[ExpiringMarket]] = {}
    for m in markets:
        if not m.event_id:
            continue
        by_event.setdefault(m.event_id, []).append(m)

    out: list[ExpiringEvent] = []
    for event_id, children in by_event.items():
        first = children[0]
        vol = sum(c.volume_24h or 0 for c in children)
        fee_types = {c.fee_type for c in children if c.fee_type}
        fee_type = next(iter(fee_types)) if len(fee_types) == 1 else None
        schedules = [c.fee_schedule for c in children if c.fee_schedule]
        schedule = schedules[0] if len(schedules) == 1 else None
        rate = effective_fee_rate(
            fee_type=fee_type,
            fee_schedule=schedule,
            domain=first.domain,
        )
        out.append(
            ExpiringEvent(
                event_id=event_id,
                title=first.event_title or event_id,
                slug=first.series_slug,
                domain=first.domain,
                market_count=len(children),
                min_hours_left=min(c.hours_left for c in children),
                max_hours_left=max(c.hours_left for c in children),
                total_volume_24h=vol,
                fees_enabled=any(c.fees_enabled or c.event_fees_enabled for c in children),
                fee_type=fee_type,
                fee_rate=rate,
                markets_with_fees=sum(1 for c in children if c.fees_enabled),
            )
        )
    out.sort(key=lambda e: e.min_hours_left)
    return out


def market_fee_label(m: ExpiringMarket) -> str:
    mid = m.yes_mid if m.yes_mid is not None else m.no_mid
    summary = fee_summary(
        price=mid,
        fees_enabled=m.fees_enabled or m.event_fees_enabled,
        fee_type=m.fee_type,
        fee_schedule=m.fee_schedule,
        domain=m.domain,
        maker_base_fee=m.maker_base_fee,
        taker_base_fee=m.taker_base_fee or m.event_taker_base_fee,
    )
    return format_taker_fee(mid, summary)
