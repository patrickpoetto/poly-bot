"""Tests for delta-driven series refresh: orphan detection."""

from sqlmodel import Session

from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.persistence.schema import Event, Series


def test_count_unknown_event_series_zero_when_all_known(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    with Session(runner._engine) as session:
        session.add(Series(id="s-1", slug="btc-5m", title="BTC 5m", recurrence_seconds=300))
        session.add(
            Event(
                id="e-1", slug="ev1", title="Ev1", series_id="s-1", active=True, closed=False
            )
        )
        session.commit()
    assert runner.count_unknown_event_series() == 0


def test_count_unknown_event_series_detects_orphans(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    with Session(runner._engine) as session:
        session.add(
            Event(
                id="e-1", slug="ev1", title="Ev1", series_id="missing-1", active=True, closed=False
            )
        )
        session.add(
            Event(
                id="e-2", slug="ev2", title="Ev2", series_id="missing-2", active=True, closed=False
            )
        )
        session.add(
            Event(
                id="e-3", slug="ev3", title="Ev3", series_id="missing-1", active=True, closed=False
            )
        )
        session.commit()
    assert runner.count_unknown_event_series() == 2  # DISTINCT


def test_count_unknown_event_series_ignores_blank_series_id(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    with Session(runner._engine) as session:
        session.add(
            Event(id="e-x", slug="evx", title="Evx", series_id="", active=True, closed=False)
        )
        session.add(
            Event(id="e-y", slug="evy", title="Evy", series_id=None, active=True, closed=False)
        )
        session.commit()
    assert runner.count_unknown_event_series() == 0
