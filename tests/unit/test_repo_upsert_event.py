"""Event upsert idempotency."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlmodel import Session

from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.persistence.repo import CatalogRepository
from poly_bot.persistence.schema import Event


def test_upsert_event_updates_existing(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()
    now = datetime.now(UTC)
    with Session(runner._engine) as session:
        repo = CatalogRepository(session)
        repo.upsert_event(
            Event(
                id="ev-dup",
                slug="highest-temperature-in-london-on-june-5-2026",
                title="Old title",
                first_seen_at=now,
                last_seen_at=now,
            )
        )
        session.commit()

    with Session(runner._engine) as session:
        repo = CatalogRepository(session)
        repo.upsert_event(
            Event(
                id="ev-dup",
                slug="highest-temperature-in-london-on-june-5-2026",
                title="New title",
            )
        )
        session.commit()
        row = session.get(Event, "ev-dup")
        assert row is not None
        assert row.title == "New title"
