"""HOT window rules and catalog funnel stats."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import Session

from poly_bot.catalog.expiring import list_expiring_markets
from poly_bot.catalog.time_window import in_hot_window
from poly_bot.catalog.window_stats import compute_window_stats
from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.persistence.schema import Market, Token, TokenQuote


def test_sports_live_in_hot_from_kickoff(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    now = datetime.now(UTC)
    settings.catalog_quote_hot_hours = 2.0

    with Session(runner._engine) as session:
        session.add(
            Market(
                condition_id="0xs-live",
                slug="fb-live",
                question="A vs B",
                domain="sports",
                active=True,
                closed=False,
                end_ts=now - timedelta(hours=1),
                game_start_ts=now - timedelta(hours=1),
            )
        )
        session.commit()
        rows = list_expiring_markets(session, hours=6, sports_in_play_hours=4)
        assert len(rows) == 1
        m = rows[0]
        assert m.is_live is True
        assert m.hours_left == pytest.approx(3.0, abs=0.05)
        assert in_hot_window(m, settings) is True


def test_sports_pre_match_hot_only_near_kickoff(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    now = datetime.now(UTC)
    settings.catalog_quote_hot_hours = 2.0

    with Session(runner._engine) as session:
        session.add(
            Market(
                condition_id="0xs-soon",
                slug="fb-soon",
                question="C vs D",
                domain="sports",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=1),
                game_start_ts=now + timedelta(hours=1),
            )
        )
        session.add(
            Market(
                condition_id="0xs-later",
                slug="fb-later",
                question="E vs F",
                domain="sports",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=5),
                game_start_ts=now + timedelta(hours=5),
            )
        )
        session.commit()
        rows = {r.condition_id: r for r in list_expiring_markets(session, hours=6)}
        assert in_hot_window(rows["0xs-soon"], settings) is True
        assert in_hot_window(rows["0xs-later"], settings) is False


def test_non_sports_uses_hours_left(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    now = datetime.now(UTC)
    settings.catalog_quote_hot_hours = 2.0

    with Session(runner._engine) as session:
        session.add(
            Market(
                condition_id="0xwx",
                slug="wx-today",
                question="NYC high temp?",
                domain="weather",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=5),
            )
        )
        session.commit()
        rows = list_expiring_markets(session, hours=6)
        assert len(rows) == 1
        assert in_hot_window(rows[0], settings) is False


def test_window_stats_funnel(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    now = datetime.now(UTC)
    settings.catalog_quote_hot_hours = 2.0
    settings.catalog_expiring_hours = 6.0

    with Session(runner._engine) as session:
        session.add(
            Market(
                condition_id="0xws1",
                slug="far-away",
                question="Far",
                domain="politics",
                active=True,
                closed=False,
                end_ts=now + timedelta(days=30),
            )
        )
        session.add(
            Market(
                condition_id="0xws2",
                slug="soon",
                question="Soon",
                domain="weather",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=1),
            )
        )
        session.add(
            Token(
                token_id="ws2-y",
                condition_id="0xws2",
                outcome_index=0,
                outcome_label="Yes",
            )
        )
        session.add(
            TokenQuote(
                token_id="ws2-y",
                condition_id="0xws2",
                mid=0.99,
                updated_at=now,
            )
        )
        session.commit()

        stats = compute_window_stats(session, settings)
        assert stats.total_markets == 2
        assert stats.active_open == 2
        assert stats.in_catalog_window == 1
        assert stats.in_hot_window == 1
        assert stats.with_tokens == 1
        assert stats.with_any_quote == 1
        assert stats.mid_ge_95 == 1
        assert stats.in_harvest_band == 1
        assert stats.global_quotes_total == 1
        assert any(d.domain == "weather" for d in stats.by_domain)
