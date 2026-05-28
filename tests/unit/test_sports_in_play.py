"""Sports kickoff + in-play catalog window."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlmodel import Session

from poly_bot.catalog.expiring import count_expiring_markets, list_expiring_markets
from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.persistence.schema import Market


def test_sports_in_play_after_kickoff(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    now = datetime.now(UTC)
    kickoff = now - timedelta(hours=1)

    with Session(runner._engine) as session:
        session.add(
            Market(
                condition_id="0xsport-live",
                slug="nba-live",
                question="Team A vs B — live",
                domain="sports",
                active=True,
                closed=False,
                accepting_orders=True,
                end_ts=kickoff,
                game_start_ts=kickoff,
            )
        )
        session.commit()

        # Without in-play window: kickoff in past → excluded by end_ts window.
        assert count_expiring_markets(session, hours=6, sports_in_play_hours=0) == 0
        # With 4h in-play: still visible.
        assert count_expiring_markets(session, hours=6, sports_in_play_hours=4) == 1
        rows = list_expiring_markets(session, hours=6, sports_in_play_hours=4)
        assert rows[0].condition_id == "0xsport-live"
        assert rows[0].is_live is True
        assert rows[0].hours_left < 0
