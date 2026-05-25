"""Expiring window query tests."""

from datetime import UTC, datetime, timedelta

from sqlmodel import Session

from poly_bot.catalog.expiring import count_expiring_markets, list_expiring_markets
from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.persistence.schema import Event, Market, Token


def test_list_expiring_markets(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()

    now = datetime.now(UTC)
    with Session(runner._engine) as session:
        session.add(
            Event(
                id="ev-exp",
                slug="ev",
                title="Soon",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=12),
            )
        )
        session.add(
            Market(
                condition_id="0xexp",
                slug="test-expiring",
                question="Ends soon?",
                event_id="ev-exp",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=12),
            )
        )
        session.add(
            Market(
                condition_id="0xfar",
                slug="test-far",
                question="Ends later",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=100),
            )
        )
        session.add(
            Token(
                token_id="t0",
                condition_id="0xexp",
                outcome_index=0,
                outcome_label="Yes",
            )
        )
        session.commit()

        assert count_expiring_markets(session, hours=48) == 1
        rows = list_expiring_markets(session, hours=48)
        assert len(rows) == 1
        assert rows[0].condition_id == "0xexp"
        assert rows[0].hours_left < 48


def test_expiring_window_space_separated_timestamps(settings) -> None:
    """DB stores end_ts as 'YYYY-MM-DD HH:MM:SS.ffffff' — must not use ISO string compare."""
    from sqlalchemy import text

    runner = DiscoveryRunner(settings)
    runner.init_database()

    now = datetime.now(UTC)
    near = now + timedelta(hours=2)
    far = now + timedelta(days=30)
    with Session(runner._engine) as session:
        session.add(
            Market(
                condition_id="0xspace",
                slug="space-ts",
                question="Space format",
                active=True,
                closed=False,
                end_ts=near,
            )
        )
        session.add(
            Market(
                condition_id="0xfar2",
                slug="far-ts",
                question="Far away",
                active=True,
                closed=False,
                end_ts=far,
            )
        )
        session.commit()
        session.execute(
            text("UPDATE markets SET end_ts = :ts WHERE condition_id = :cid"),
            {
                "ts": near.strftime("%Y-%m-%d %H:%M:%S.000000"),
                "cid": "0xspace",
            },
        )
        session.execute(
            text("UPDATE markets SET end_ts = :ts WHERE condition_id = :cid"),
            {
                "ts": far.strftime("%Y-%m-%d %H:%M:%S.000000"),
                "cid": "0xfar2",
            },
        )
        session.commit()

        assert count_expiring_markets(session, hours=6) == 1
        rows = list_expiring_markets(session, hours=6)
        assert [r.condition_id for r in rows] == ["0xspace"]


def test_stale_filter_skips_fresh_quotes(settings) -> None:
    """Only stale or missing quotes should be selected for refresh."""
    from poly_bot.catalog.expiring import conditions_needing_quote_refresh
    from poly_bot.persistence.schema import TokenQuote

    runner = DiscoveryRunner(settings)
    runner.init_database()

    now = datetime.now(UTC)
    with Session(runner._engine) as session:
        session.add(
            Market(
                condition_id="0xfresh",
                slug="fresh",
                question="Fresh",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=1),
            )
        )
        session.add(
            Market(
                condition_id="0xstale",
                slug="stale",
                question="Stale",
                active=True,
                closed=False,
                end_ts=now + timedelta(hours=1),
            )
        )
        session.add(
            Token(
                token_id="t-fresh",
                condition_id="0xfresh",
                outcome_index=0,
                outcome_label="Yes",
            )
        )
        session.add(
            Token(
                token_id="t-stale",
                condition_id="0xstale",
                outcome_index=0,
                outcome_label="Yes",
            )
        )
        session.add(
            TokenQuote(
                token_id="t-fresh",
                condition_id="0xfresh",
                mid=0.5,
                updated_at=now,
            )
        )
        session.add(
            TokenQuote(
                token_id="t-stale",
                condition_id="0xstale",
                mid=0.4,
                updated_at=now - timedelta(minutes=10),
            )
        )
        session.commit()

        ids = ["0xfresh", "0xstale"]
        needing = conditions_needing_quote_refresh(session, ids, stale_seconds=120, now=now)
        assert needing == ["0xstale"]
