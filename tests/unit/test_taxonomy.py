"""Taxonomy override store + classifier integration."""

from __future__ import annotations

from sqlmodel import Session

from poly_bot.catalog.taxonomy import (
    TaxonomyOverrideStore,
    delete_override,
    list_overrides,
    upsert_override,
)
from poly_bot.discovery.classifier.rules import classify_market
from poly_bot.discovery.runner import DiscoveryRunner


def test_upsert_and_lookup(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()

    with Session(runner._engine) as session:
        row = upsert_override(
            session,
            kind="tag",
            key="custom-tag",
            domain="culture",
            note="manual",
        )
        assert row.id is not None
        assert row.domain == "culture"

        store = TaxonomyOverrideStore.load(session)
        hit = store.lookup(tags=["unknown", "custom-tag"])
        assert hit is not None
        assert hit.domain == "culture"

        rows = list_overrides(session)
        assert len(rows) == 1
        assert delete_override(session, rows[0].id) is True  # type: ignore[arg-type]
        assert list_overrides(session) == []


def test_classify_market_uses_override(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()

    with Session(runner._engine) as session:
        upsert_override(
            session,
            kind="condition_id",
            key="0xabc",
            domain="sports",
        )
        store = TaxonomyOverrideStore.load(session)

    raw = {"conditionId": "0xabc", "outcomes": '["Yes", "No"]', "tags": []}
    cls = classify_market(raw, slug="random-slug", overrides=store)
    assert cls["domain"] == "sports"


def test_classify_market_slug_prefix_override(settings) -> None:
    runner = DiscoveryRunner(settings)
    runner.init_database()

    with Session(runner._engine) as session:
        upsert_override(
            session,
            kind="slug_prefix",
            key="custom-event-",
            domain="science",
        )
        store = TaxonomyOverrideStore.load(session)

    raw = {"conditionId": "0xdef", "outcomes": '["Yes", "No"]'}
    cls = classify_market(raw, slug="custom-event-42", overrides=store)
    assert cls["domain"] == "science"
