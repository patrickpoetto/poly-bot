"""Smoke tests for the dashboard SSE/health endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient
from sqlmodel import Session

from poly_bot.catalog.dashboard import _live_snapshot, create_app
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
