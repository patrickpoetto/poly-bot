"""Scan expiring catalog for strategy-priced outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from sqlmodel import Session

from poly_bot.catalog.expiring import list_expiring_markets
from poly_bot.catalog.models import ExpiringMarket
from poly_bot.catalog.time_window import in_hot_window
from poly_bot.harvest.book_sanity import (
    book_sanity_reject_reason,
    max_bid_ask_spread_for_domain,
)
from poly_bot.harvest.pricing import buy_yield_pct, passes_harvest_price_band
from poly_bot.harvest.quotes import SideQuote, refresh_side_quotes, side_quotes_from_db
from poly_bot.settings import Settings


@dataclass(frozen=True, slots=True)
class HarvestOpportunity:
    condition_id: str
    slug: str
    question: str
    domain: str
    side: Literal["yes", "no"]
    outcome_label: str
    token_id: str
    mid: float | None
    ask: float
    price: float
    gross_yield_pct: float
    net_yield_pct: float
    fee_per_share: float
    fillable_usd: float
    est_profit_usd: float
    hours_left: float
    minutes_left: float
    hours_to_kickoff: float | None
    window_end_ts: datetime
    resolution_kind: str
    is_live: bool
    quote_age_s: float | None
    event_id: str | None
    event_title: str | None


def _time_ok(
    m: ExpiringMarket,
    settings: Settings,
    *,
    hot_only: bool,
    max_minutes_left: float | None,
) -> bool:
    if m.hours_left <= 0:
        return False
    if hot_only and not in_hot_window(m, settings):
        return False
    cap = max_minutes_left
    if cap is None and settings.harvest_max_minutes_left > 0:
        cap = settings.harvest_max_minutes_left
    return not (cap is not None and cap > 0 and m.hours_left * 60.0 > cap)


def _evaluate_side(
    m: ExpiringMarket,
    *,
    side: Literal["yes", "no"],
    sq: SideQuote,
    other_sq: SideQuote | None,
    settings: Settings,
    require_liquidity: bool,
    require_book_sanity: bool,
) -> HarvestOpportunity | None:
    price = sq.vwap_ask if sq.vwap_ask is not None else sq.ask
    if price is None:
        price = sq.mid
    if price is None:
        return None

    if not passes_harvest_price_band(
        price,
        min_buy_price=settings.harvest_min_buy_price,
        max_buy_price=settings.harvest_max_buy_price,
    ):
        return None

    if require_book_sanity:
        if not sq.book_fetched or sq.ask is None:
            return None
        max_book_age = settings.harvest_max_book_age_seconds
        if max_book_age > 0 and (sq.book_age_s is None or sq.book_age_s > max_book_age):
            return None
        sanity = book_sanity_reject_reason(
            ask=price,
            bid=sq.bid,
            other_bid=other_sq.bid if other_sq else None,
            max_bid_ask_spread=max_bid_ask_spread_for_domain(settings, m.domain),
            max_complement_sum=settings.harvest_max_complement_sum,
        )
        if sanity is not None:
            return None

    gross, net, fee = buy_yield_pct(
        price,
        fees_enabled=m.fees_enabled,
        fee_type=m.fee_type,
        fee_schedule=m.fee_schedule,
        domain=m.domain,
        event_fees_enabled=m.event_fees_enabled,
    )
    if net < settings.harvest_min_net_yield_pct:
        return None

    fillable = sq.fillable_usd
    if fillable <= 0 and price is not None and not require_book_sanity:
        fillable = settings.harvest_order_usd
    if require_liquidity and fillable < settings.harvest_min_fill_usd:
        return None

    order_usd = min(settings.harvest_order_usd, fillable)
    est_profit = order_usd * net
    window_end = m.window_end_ts or m.end_ts
    minutes_left = m.hours_left * 60.0

    return HarvestOpportunity(
        condition_id=m.condition_id,
        slug=m.slug,
        question=m.question,
        domain=m.domain,
        side=side,
        outcome_label=sq.outcome_label,
        token_id=sq.token_id,
        mid=sq.mid,
        ask=price,
        price=price,
        gross_yield_pct=gross,
        net_yield_pct=net,
        fee_per_share=fee,
        fillable_usd=fillable,
        est_profit_usd=est_profit,
        hours_left=m.hours_left,
        minutes_left=minutes_left,
        hours_to_kickoff=m.hours_to_kickoff,
        window_end_ts=window_end,
        resolution_kind=m.resolution_kind,
        is_live=m.is_live,
        quote_age_s=sq.quote_age_s,
        event_id=m.event_id,
        event_title=m.event_title,
    )


def _collect_opportunities(
    markets: list[ExpiringMarket],
    side_quotes: dict[str, dict[str, SideQuote]],
    settings: Settings,
    *,
    require_fresh_quotes: bool,
    require_liquidity: bool,
    require_book_sanity: bool,
) -> list[HarvestOpportunity]:
    max_age = settings.harvest_quote_max_age_seconds
    out: list[HarvestOpportunity] = []

    for m in markets:
        quotes = side_quotes.get(m.condition_id, {})
        for side in ("yes", "no"):
            sq = quotes.get(side)
            if sq is None:
                continue
            stale = sq.quote_age_s is None or sq.quote_age_s > max_age
            if require_fresh_quotes and max_age > 0 and stale:
                continue
            other = quotes.get("no" if side == "yes" else "yes")
            opp = _evaluate_side(
                m,
                side=side,
                sq=sq,
                other_sq=other,
                settings=settings,
                require_liquidity=require_liquidity,
                require_book_sanity=require_book_sanity,
            )
            if opp is not None:
                out.append(opp)

    out.sort(
        key=lambda o: (
            o.window_end_ts,
            -o.net_yield_pct,
            o.condition_id,
        )
    )
    return out


def scan_harvest_opportunities(
    session: Session,
    settings: Settings,
    *,
    hours: float | None = None,
    hot_only: bool = True,
    max_minutes_left: float | None = None,
    require_fresh_quotes: bool = True,
) -> list[HarvestOpportunity]:
    """Sync scan using DB quotes (ask/mid from token_quotes)."""
    horizon = hours if hours is not None else settings.catalog_expiring_hours
    markets = list_expiring_markets(
        session,
        hours=horizon,
        sports_in_play_hours=settings.catalog_sports_in_play_hours,
    )
    filtered = [
        m
        for m in markets
        if _time_ok(m, settings, hot_only=hot_only, max_minutes_left=max_minutes_left)
    ]
    cids = [m.condition_id for m in filtered]
    quotes = side_quotes_from_db(session, cids)
    return _collect_opportunities(
        filtered,
        quotes,
        settings,
        require_fresh_quotes=require_fresh_quotes,
        require_liquidity=False,
        require_book_sanity=False,
    )


async def scan_harvest_opportunities_live(
    session: Session,
    settings: Settings,
    *,
    hours: float | None = None,
    hot_only: bool = True,
    max_minutes_left: float | None = None,
    require_fresh_quotes: bool = False,
) -> list[HarvestOpportunity]:
    """Live CLOB /book refresh for ask + fillable depth."""
    horizon = hours if hours is not None else settings.catalog_expiring_hours
    markets = list_expiring_markets(
        session,
        hours=horizon,
        sports_in_play_hours=settings.catalog_sports_in_play_hours,
    )
    filtered = [
        m
        for m in markets
        if _time_ok(m, settings, hot_only=hot_only, max_minutes_left=max_minutes_left)
    ]
    cids = [m.condition_id for m in filtered]
    quotes = await refresh_side_quotes(
        session,
        settings,
        condition_ids=cids,
        order_usd=settings.harvest_order_usd,
        max_price=settings.harvest_max_buy_price,
    )
    return _collect_opportunities(
        filtered,
        quotes,
        settings,
        require_fresh_quotes=require_fresh_quotes,
        require_liquidity=True,
        require_book_sanity=True,
    )
