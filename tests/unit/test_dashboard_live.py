"""Smoke tests for the dashboard SSE/health endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient

from poly_bot.catalog.dashboard import _live_snapshot, _quotes_payload, create_app
from poly_bot.catalog.models import ExpiringMarket
from poly_bot.persistence.db import get_engine, init_db


def test_live_snapshot_shape(settings) -> None:
    init_db(get_engine(settings))
    snap = _live_snapshot(settings, hours=6.0)
    assert set(snap.keys()) >= {
        "now",
        "hours",
        "quote_age_s",
        "delta_age_s",
        "full_age_s",
        "reconcile_age_s",
        "counts",
    }
    counts = snap["counts"]
    assert isinstance(counts, dict)
    for key in (
        "markets_active",
        "markets_with_fees",
        "expiring_window",
        "events_window",
        "fees_in_window",
    ):
        assert key in counts


def test_api_health_endpoint(settings) -> None:
    init_db(get_engine(settings))
    app = create_app(settings)
    with TestClient(app) as client:
        resp = client.get("/api/health?hours=6")
    assert resp.status_code == 200
    body = resp.json()
    assert "counts" in body
    assert "quote_age_s" in body


def test_api_reconcile_endpoint_empty_state(settings) -> None:
    init_db(get_engine(settings))
    app = create_app(settings)
    with TestClient(app) as client:
        resp = client.get("/api/reconcile")
    assert resp.status_code == 200
    body = resp.json()
    assert body["last_at"] is None
    assert body["report"] == {}


def test_live_snapshot_includes_quotes_when_requested(settings) -> None:
    init_db(get_engine(settings))
    snap = _live_snapshot(settings, hours=6.0, include_quotes=True)
    assert "quotes" in snap
    assert isinstance(snap["quotes"], dict)


def test_quotes_payload_keys(settings) -> None:
    from datetime import UTC, datetime

    m = ExpiringMarket(
        condition_id="0xabc",
        slug="s",
        question="q",
        domain="crypto",
        structure="binary",
        recurrence="one_off",
        end_ts=datetime.now(UTC),
        hours_left=2.0,
        event_id=None,
        event_title=None,
        series_slug=None,
        tags=[],
        yes_label="Yes",
        no_label="No",
        yes_mid=0.55,
        no_mid=0.45,
        yes_spread=0.02,
        no_spread=0.02,
        open_interest=None,
        volume_24h=None,
        rewards_eligible=False,
        quote_updated_at=None,
        enriched_at=None,
    )
    payload = _quotes_payload([m])
    assert payload["0xabc"] == {"y": 0.55, "n": 0.45, "s": 0.02}


def test_api_reconcile_endpoint_after_run(settings) -> None:
    from poly_bot.ops.reconcile_scheduler import _run_once

    engine = get_engine(settings)
    init_db(engine)
    _run_once(engine)

    app = create_app(settings)
    with TestClient(app) as client:
        resp = client.get("/api/reconcile")
    body = resp.json()
    assert body["last_at"] is not None
    assert "markets_total" in body["report"]
