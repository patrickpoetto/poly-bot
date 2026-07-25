"""Harvest pricing and scan tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import Session

from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.harvest.book_sanity import book_sanity_reject_reason
from poly_bot.harvest.liquidity import fillable_usd_at_ask
from poly_bot.harvest.pricing import buy_yield_pct, passes_harvest_price_band
from poly_bot.harvest.quotes import SideQuote
from poly_bot.harvest.scan import _evaluate_side, scan_harvest_opportunities
from poly_bot.persistence.schema import Market, Token, TokenQuote


def test_buy_yield_at_985_politics_fee() -> None:
    gross, net, fee = buy_yield_pct(
        0.985,
        fees_enabled=True,
        fee_type="politics",
        fee_schedule=None,
        domain="politics",
    )
    assert gross == pytest.approx(0.01523, rel=1e-3)
    assert fee > 0
    assert net >= 0.012
    assert net < gross


def test_fillable_usd_at_ask() -> None:
    asks = [(0.985, 5.0), (0.990, 100.0)]
    filled, vwap = fillable_usd_at_ask(asks, order_usd=10.0, max_price=0.995)
    assert filled == pytest.approx(10.0, abs=0.01)
    assert vwap == pytest.approx(0.9875, abs=0.001)


def test_harvest_price_band() -> None:
    assert passes_harvest_price_band(0.985, min_buy_price=0.983, max_buy_price=0.995)
    assert not passes_harvest_price_band(0.90, min_buy_price=0.983, max_buy_price=0.995)


def test_book_sanity_rejects_stale_loser_ask() -> None:
    """Over 4.5 losing at 0-0: bid 2¢ ask 98¢ while Under bid is 97¢."""
    reason = book_sanity_reject_reason(
        ask=0.984,
        bid=0.02,
        other_bid=0.97,
        max_bid_ask_spread=0.05,
        max_complement_sum=1.02,
    )
    assert reason is not None
    assert reason.startswith("spread_")


def test_book_sanity_accepts_tight_winning_book() -> None:
    reason = book_sanity_reject_reason(
        ask=0.984,
        bid=0.978,
        other_bid=0.02,
        max_bid_ask_spread=0.05,
        max_complement_sum=1.02,
    )
    assert reason is None


def test_book_sanity_wide_spread_ok_when_complement_holds() -> None:
    """Wide spread (bid 83c ask 98c) is fine if the other side bids ~1c."""
    reason = book_sanity_reject_reason(
        ask=0.984,
        bid=0.830,
        other_bid=0.010,
        max_bid_ask_spread=0.0,
        max_complement_sum=1.02,
    )
    assert reason is None


def test_book_sanity_complement_only_catches_sports_trap() -> None:
    reason = book_sanity_reject_reason(
        ask=0.984,
        bid=0.02,
        other_bid=0.97,
        max_bid_ask_spread=0.0,
        max_complement_sum=1.02,
    )
    assert reason is not None
    assert reason.startswith("complement_")


def test_evaluate_side_rejects_stale_book(settings) -> None:
    from poly_bot.catalog.models import ExpiringMarket

    now = datetime.now(UTC)
    settings.harvest_max_book_age_seconds = 300
    m = ExpiringMarket(
        condition_id="0xstale",
        slug="stale",
        question="Stale book?",
        domain="sports",
        structure="binary",
        recurrence="one_off",
        end_ts=now + timedelta(hours=1),
        hours_left=1.0,
        window_end_ts=now + timedelta(hours=1),
        resolution_kind="in_play",
        is_live=True,
        event_id=None,
        event_title=None,
        series_slug=None,
        tags=[],
        yes_label="Yes",
        no_label="No",
        yes_mid=0.98,
        no_mid=0.02,
        yes_spread=None,
        no_spread=None,
        open_interest=None,
        volume_24h=None,
        rewards_eligible=False,
        quote_updated_at=now,
        enriched_at=None,
    )
    sq = SideQuote(
        token_id="t-y",
        outcome_label="Yes",
        mid=0.98,
        bid=0.97,
        ask=0.985,
        quote_age_s=5.0,
        book_age_s=3600.0,
        book_fetched=True,
        fillable_usd=10.0,
    )
    other = SideQuote(
        token_id="t-n",
        outcome_label="No",
        mid=0.02,
        bid=0.01,
        ask=0.03,
        quote_age_s=5.0,
        book_age_s=3600.0,
        book_fetched=True,
    )
    assert (
        _evaluate_side(
            m,
            side="yes",
            sq=sq,
            other_sq=other,
            settings=settings,
            require_liquidity=True,
            require_book_sanity=True,
        )
        is None
    )


def test_evaluate_side_rejects_wide_spread(settings) -> None:
    from poly_bot.catalog.models import ExpiringMarket

    now = datetime.now(UTC)
    m = ExpiringMarket(
        condition_id="0xtrap",
        slug="ou-trap",
        question="Team A vs B: O/U 4.5",
        domain="sports",
        structure="binary",
        recurrence="one_off",
        end_ts=now + timedelta(hours=1),
        hours_left=1.0,
        window_end_ts=now + timedelta(hours=1),
        resolution_kind="in_play",
        is_live=True,
        event_id=None,
        event_title=None,
        series_slug=None,
        tags=[],
        yes_label="Over",
        no_label="Under",
        yes_mid=0.02,
        no_mid=0.97,
        yes_spread=None,
        no_spread=None,
        open_interest=None,
        volume_24h=None,
        rewards_eligible=False,
        quote_updated_at=now,
        enriched_at=None,
    )
    over = SideQuote(
        token_id="t-over",
        outcome_label="Over",
        mid=0.50,
        bid=0.02,
        ask=0.984,
        quote_age_s=0.0,
        book_age_s=1.0,
        book_fetched=True,
        fillable_usd=10.0,
    )
    under = SideQuote(
        token_id="t-under",
        outcome_label="Under",
        mid=0.97,
        bid=0.97,
        ask=0.985,
        quote_age_s=0.0,
        book_age_s=1.0,
        book_fetched=True,
        fillable_usd=10.0,
    )
    assert (
        _evaluate_side(
            m,
            side="yes",
            sq=over,
            other_sq=under,
            settings=settings,
            require_liquidity=True,
            require_book_sanity=True,
        )
        is None
    )
    assert (
        _evaluate_side(
            m,
            side="no",
            sq=under,
            other_sq=over,
            settings=settings,
            require_liquidity=True,
            require_book_sanity=True,
        )
        is not None
    )


def test_scan_finds_high_no_ask(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    now = datetime.now(UTC)
    settings.harvest_min_net_yield_pct = 0.005
    settings.harvest_min_buy_price = 0.97
    settings.catalog_quote_hot_hours = 6.0
    settings.harvest_quote_max_age_seconds = 0
    settings.harvest_min_fill_usd = 0

    with Session(runner._engine) as session:
        session.add(
            Market(
                condition_id="0xharv1",
                slug="test-harvest",
                question="Will X happen?",
                domain="politics",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=1),
            )
        )
        session.add(
            Token(
                token_id="ht-yes",
                condition_id="0xharv1",
                outcome_index=0,
                outcome_label="Yes",
            )
        )
        session.add(
            Token(
                token_id="ht-no",
                condition_id="0xharv1",
                outcome_index=1,
                outcome_label="No",
            )
        )
        session.add(
            TokenQuote(
                token_id="ht-yes",
                condition_id="0xharv1",
                mid=0.02,
                ask=0.03,
                updated_at=now,
            )
        )
        session.add(
            TokenQuote(
                token_id="ht-no",
                condition_id="0xharv1",
                mid=0.986,
                ask=0.987,
                updated_at=now,
            )
        )
        session.commit()

        opps = scan_harvest_opportunities(
            session,
            settings,
            hours=6,
            hot_only=False,
            require_fresh_quotes=False,
        )
    assert len(opps) == 1
    assert opps[0].side == "no"
    assert opps[0].ask == pytest.approx(0.987)
    assert opps[0].est_profit_usd > 0
