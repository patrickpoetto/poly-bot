"""Repository backfill tests."""

from sqlmodel import Session

from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.persistence.repo import CatalogRepository
from poly_bot.persistence.schema import Event, Market, Series


def test_backfill_market_series_from_events(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()

    with Session(runner._engine) as session:
        repo = CatalogRepository(session)
        session.add(
            Series(id="test-series-1", slug="btc-5m", title="BTC 5m", recurrence_seconds=300)
        )
        session.add(
            Event(
                id="test-event-1",
                slug="ev-slug",
                title="Event",
                series_id="test-series-1",
                active=True,
                closed=False,
            )
        )
        session.add(
            Market(
                condition_id="0xtestbackfill",
                slug="m-slug",
                question="Q?",
                event_id="test-event-1",
                active=True,
                closed=False,
            )
        )
        session.commit()

        updated = repo.backfill_market_series_from_events()
        market = session.get(Market, "0xtestbackfill")

    assert updated == 1
    assert market is not None
    assert market.series_id == "test-series-1"
