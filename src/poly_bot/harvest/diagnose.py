"""Harvest scan funnel diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlmodel import Session

from poly_bot.catalog.expiring import list_expiring_markets
from poly_bot.catalog.models import ExpiringMarket
from poly_bot.harvest.book_sanity import (
    book_sanity_reject_reason,
    max_bid_ask_spread_for_domain,
)
from poly_bot.harvest.pricing import buy_yield_pct, passes_harvest_price_band
from poly_bot.harvest.quotes import SideQuote, refresh_side_quotes, side_quotes_from_db
from poly_bot.harvest.scan import _time_ok
from poly_bot.settings import Settings


@dataclass
class HarvestNearMiss:
    condition_id: str
    question: str
    domain: str
    side: str
    price: float
    price_source: str
    net_yield_pct: float
    fillable_usd: float
    hours_left: float
    reject_reason: str


@dataclass
class HarvestDiagnosis:
    horizon_hours: float
    hot_hours: float
    markets_in_horizon: int = 0
    markets_in_hot: int = 0
    sides_with_quote: int = 0
    sides_price_ge_95: int = 0
    sides_in_price_band: int = 0
    sides_pass_net_yield: int = 0
    sides_pass_book_sanity: int = 0
    sides_pass_liquidity: int = 0
    opportunities: int = 0
    stale_skipped: int = 0
    books_404: int = 0
    near_misses: list[HarvestNearMiss] = field(default_factory=list)


def _effective_price(sq: SideQuote) -> tuple[float | None, str]:
    if sq.vwap_ask is not None:
        return sq.vwap_ask, "vwap_ask"
    if sq.ask is not None:
        return sq.ask, "ask"
    if sq.mid is not None:
        return sq.mid, "mid"
    return None, ""


def _reject_reason(
    m: ExpiringMarket,
    sq: SideQuote,
    other_sq: SideQuote | None,
    settings: Settings,
    *,
    require_liquidity: bool,
    require_book_sanity: bool,
) -> str | None:
    price, src = _effective_price(sq)
    if price is None:
        return "no_price"
    if not passes_harvest_price_band(
        price,
        min_buy_price=settings.harvest_min_buy_price,
        max_buy_price=settings.harvest_max_buy_price,
    ):
        return f"price_{src}_{price:.3f}_outside_band"
    _, net, _ = buy_yield_pct(
        price,
        fees_enabled=m.fees_enabled,
        fee_type=m.fee_type,
        fee_schedule=m.fee_schedule,
        domain=m.domain,
        event_fees_enabled=m.event_fees_enabled,
    )
    if net < settings.harvest_min_net_yield_pct:
        return f"net_{net * 100:.2f}pct_below_{settings.harvest_min_net_yield_pct * 100:.1f}"
    if require_book_sanity:
        if not sq.book_fetched or sq.ask is None:
            return "no_live_book"
        max_book_age = settings.harvest_max_book_age_seconds
        if max_book_age > 0 and (sq.book_age_s is None or sq.book_age_s > max_book_age):
            age = sq.book_age_s if sq.book_age_s is not None else -1
            return f"book_age_{age:.0f}s"
        sanity = book_sanity_reject_reason(
            ask=price,
            bid=sq.bid,
            other_bid=other_sq.bid if other_sq else None,
            max_bid_ask_spread=max_bid_ask_spread_for_domain(settings, m.domain),
            max_complement_sum=settings.harvest_max_complement_sum,
        )
        if sanity is not None:
            return sanity
    fillable = sq.fillable_usd
    if fillable <= 0 and price is not None:
        fillable = settings.harvest_order_usd
    if require_liquidity and fillable < settings.harvest_min_fill_usd:
        return f"fillable_{fillable:.1f}_usd"
    return None


async def diagnose_harvest(
    session: Session,
    settings: Settings,
    *,
    hours: float | None = None,
    hot_only: bool = True,
    live_books: bool = True,
    require_fresh_quotes: bool = False,
) -> HarvestDiagnosis:
    horizon = hours if hours is not None else settings.catalog_expiring_hours
    all_markets = list_expiring_markets(
        session,
        hours=horizon,
        sports_in_play_hours=settings.catalog_sports_in_play_hours,
    )
    hot_markets = [
        m for m in all_markets if _time_ok(m, settings, hot_only=hot_only, max_minutes_left=None)
    ]
    diag = HarvestDiagnosis(
        horizon_hours=horizon,
        hot_hours=settings.catalog_quote_hot_hours,
        markets_in_horizon=len(all_markets),
        markets_in_hot=len(hot_markets),
    )
    cids = [m.condition_id for m in hot_markets]
    if live_books:
        quotes = await refresh_side_quotes(
            session,
            settings,
            condition_ids=cids,
            order_usd=settings.harvest_order_usd,
            max_price=settings.harvest_max_buy_price,
        )
        require_liquidity = True
        require_book_sanity = True
    else:
        quotes = side_quotes_from_db(session, cids)
        require_liquidity = False
        require_book_sanity = False

    max_age = settings.harvest_quote_max_age_seconds
    near: list[HarvestNearMiss] = []

    for m in hot_markets:
        for side in ("yes", "no"):
            sq = quotes.get(m.condition_id, {}).get(side)
            if sq is None:
                continue
            if require_fresh_quotes and max_age > 0:
                if sq.quote_age_s is None or sq.quote_age_s > max_age:
                    diag.stale_skipped += 1
                    continue
            diag.sides_with_quote += 1
            other_sq = quotes.get(m.condition_id, {}).get("no" if side == "yes" else "yes")
            price, src = _effective_price(sq)
            if price is not None and price >= 0.95:
                diag.sides_price_ge_95 += 1
            if price is not None and passes_harvest_price_band(
                price,
                min_buy_price=settings.harvest_min_buy_price,
                max_buy_price=settings.harvest_max_buy_price,
            ):
                diag.sides_in_price_band += 1
                _, net, _ = buy_yield_pct(
                    price,
                    fees_enabled=m.fees_enabled,
                    fee_type=m.fee_type,
                    fee_schedule=m.fee_schedule,
                    domain=m.domain,
                    event_fees_enabled=m.event_fees_enabled,
                )
                if net >= settings.harvest_min_net_yield_pct:
                    diag.sides_pass_net_yield += 1
                    if (
                        not require_book_sanity
                        or book_sanity_reject_reason(
                            ask=price,
                            bid=sq.bid,
                            other_bid=other_sq.bid if other_sq else None,
                            max_bid_ask_spread=max_bid_ask_spread_for_domain(settings, m.domain),
                            max_complement_sum=settings.harvest_max_complement_sum,
                        )
                        is None
                    ):
                        diag.sides_pass_book_sanity += 1
            reason = _reject_reason(
                m,
                sq,
                other_sq,
                settings,
                require_liquidity=require_liquidity,
                require_book_sanity=require_book_sanity,
            )
            if reason is None:
                diag.sides_pass_liquidity += 1
                diag.opportunities += 1
            elif price is not None and price >= 0.90:
                _, net, _ = buy_yield_pct(
                    price,
                    fees_enabled=m.fees_enabled,
                    fee_type=m.fee_type,
                    fee_schedule=m.fee_schedule,
                    domain=m.domain,
                    event_fees_enabled=m.event_fees_enabled,
                )
                near.append(
                    HarvestNearMiss(
                        condition_id=m.condition_id,
                        question=m.question[:80],
                        domain=m.domain,
                        side=side,
                        price=price,
                        price_source=src,
                        net_yield_pct=net,
                        fillable_usd=sq.fillable_usd,
                        hours_left=m.hours_left,
                        reject_reason=reason,
                    )
                )

    near.sort(key=lambda x: (-x.price, -x.net_yield_pct))
    diag.near_misses = near[:15]
    return diag
