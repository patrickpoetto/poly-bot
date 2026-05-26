"""Tests for the reconcile scheduler loop."""

from __future__ import annotations

import json

from sqlmodel import Session

from poly_bot.ops.reconcile_scheduler import _run_once
from poly_bot.persistence.catalog_state import (
    KEY_LAST_RECONCILE_AT,
    KEY_LAST_RECONCILE_REPORT,
    CatalogStateStore,
)
from poly_bot.persistence.db import get_engine, init_db
from poly_bot.persistence.schema import Event, Market


def test_run_once_writes_state_and_report(settings) -> None:
    engine = get_engine(settings)
    init_db(engine)
    with Session(engine) as session:
        session.add(
            Event(id="e-1", slug="ev1", title="Ev1", series_id="s-1", active=True, closed=False)
        )
        session.add(
            Market(
                condition_id="0xCID",
                slug="m-slug",
                title="Market",
                event_id="e-1",
                active=True,
                closed=False,
                fee_type=None,
            )
        )
        session.commit()

    _run_once(engine)

    with Session(engine) as session:
        state = CatalogStateStore(session)
        last_at = state.get_datetime(KEY_LAST_RECONCILE_AT)
        report_raw = state.get(KEY_LAST_RECONCILE_REPORT)

    assert last_at is not None
    assert report_raw is not None
    report = json.loads(report_raw)
    assert report["markets_total"] == 1
    assert report["markets_null_fee_type"] == 1
    assert report["events_total"] == 1
