"""Harvest pricing and scan tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import Session

from poly_bot.catalog.models import ExpiringMarket
from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.harvest.pricing import buy_yield_pct, passes_harvest_price_band
from poly_bot.harvest.scan import scan_harvest_opportunities
from poly_bot.persistence.schema import Market, Token, TokenQuote


def test_buy_yield_at_985_weather_fee() -> None:
    gross, net, fee = buy_yield_pct(
        0.985,
        fees_enabled=True,
        fee_type="weather",
        fee_schedule=None,
        domain="weather",
    )
    assert gross == pytest.approx(0.01523, rel=1e-3)
    assert fee > 0
    assert net >= 0.012
    assert net < gross


def test_harvest_price_band() -> None:
    assert passes_harvest_price_band(0.985, min_buy_price=0.983, max_buy_price=0.995)
    assert not passes_harvest_price_band(0.90, min_buy_price=0.983, max_buy_price=0.995)


def test_scan_finds_high_no_mid(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    now = datetime.now(UTC)
    settings.harvest_min_net_yield_pct = 0.005
    settings.harvest_min_buy_price = 0.97
    settings.catalog_quote_hot_hours = 6.0
    settings.harvest_quote_max_age_seconds = 0

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
                updated_at=now,
            )
        )
        session.add(
            TokenQuote(
                token_id="ht-no",
                condition_id="0xharv1",
                mid=0.986,
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
    assert opps[0].price == pytest.approx(0.986)
