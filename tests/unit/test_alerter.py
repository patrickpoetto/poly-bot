"""Tests for the Telegram alerter rule engine + scheduler.

We never hit the real Telegram API — ``run_alerter_loop`` is parametrised
to accept a custom ``snapshot_fn`` and ``sender``.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from poly_bot.ops.alerter import (
    AlerterContext,
    default_rules,
    render_message,
    run_alerter_loop,
    transitions,
)
from poly_bot.settings import Settings


@pytest.fixture
def alerter_settings(settings: Settings) -> Settings:
    """Settings with alerter on and pretend Telegram creds."""
    return settings.model_copy(
        update={
            "alerter_enabled": True,
            "telegram_token": None,
            "telegram_chat_id": "0",
            "alerter_interval_seconds": 10,
            "alert_quote_age_threshold": 600,
            "alert_delta_age_threshold": 600,
            "alert_reconcile_age_threshold": 14400,
        }
    )


def test_quote_stale_rule_fires_above_threshold(alerter_settings: Settings) -> None:
    rules = {r.name: r for r in default_rules(alerter_settings)}
    ctx = AlerterContext()
    firing, detail = rules["quotes_stale"].evaluate({"quote_age_s": 1200}, ctx)
    assert firing is True
    assert "1200" in detail

    firing, _ = rules["quotes_stale"].evaluate({"quote_age_s": 60}, ctx)
    assert firing is False


def test_db_down_rule(alerter_settings: Settings) -> None:
    rules = {r.name: r for r in default_rules(alerter_settings)}
    ctx = AlerterContext()
    firing, detail = rules["db_down"].evaluate({"db": "down: OperationalError"}, ctx)
    assert firing is True
    assert "down" in detail
    firing, _ = rules["db_down"].evaluate({"db": "ok"}, ctx)
    assert firing is False


def test_reconcile_regression_needs_baseline(alerter_settings: Settings) -> None:
    rules = {r.name: r for r in default_rules(alerter_settings)}
    ctx = AlerterContext()
    snap1 = {"reconcile_report": {"issues": {"markets_null_fee_type": 0}}}
    firing, _ = rules["reconcile_regression"].evaluate(snap1, ctx)
    assert firing is False
    snap2 = {"reconcile_report": {"issues": {"markets_null_fee_type": 5}}}
    firing, detail = rules["reconcile_regression"].evaluate(snap2, ctx)
    assert firing is True
    assert "markets_null_fee_type" in detail


def test_transitions_only_on_state_change() -> None:
    prev = {"quotes_stale": False, "db_down": True}
    current = {
        "quotes_stale": (False, "ok"),
        "db_down": (True, "still down"),
        "discovery_stale": (True, "discovery_activity_age_s=900"),
    }
    out = transitions(prev, current)
    out_by_name = {n: kind for n, kind, *_ in out}
    # db_down was already firing → no transition; quotes_stale unchanged.
    assert "db_down" not in out_by_name
    assert "quotes_stale" not in out_by_name
    # discovery_stale is new → open.
    assert out_by_name["discovery_stale"] == "open"


def test_discovery_stale_uses_heartbeat_not_delta_completion(
    alerter_settings: Settings,
) -> None:
    rules = {r.name: r for r in default_rules(alerter_settings)}
    ctx = AlerterContext()
    # Long full scan: delta_age high but heartbeat fresh → no page.
    snap = {"discovery_activity_age_s": 45, "delta_age_s": 900}
    firing, _ = rules["discovery_stale"].evaluate(snap, ctx)
    assert firing is False
    snap_stuck = {"discovery_activity_age_s": 900, "delta_age_s": 900}
    firing, detail = rules["discovery_stale"].evaluate(snap_stuck, ctx)
    assert firing is True
    assert "900" in detail


def test_render_message_open_and_resolved() -> None:
    text = render_message("quotes_stale", "open", "quote_age_s=700", host="h")
    assert "FIRING" in text
    assert "quotes_stale" in text
    text2 = render_message("quotes_stale", "resolved", "back ok", host="h")
    assert "RESOLVED" in text2


def test_run_alerter_loop_pages_on_open_and_resolved(
    alerter_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    sent: list[str] = []

    async def fake_sender(message: str) -> bool:
        sent.append(message)
        return True

    async def fake_sleep(_seconds: float) -> None:  # avoid 10s sleeps in tests
        return None

    monkeypatch.setattr("poly_bot.ops.alerter.asyncio.sleep", fake_sleep)

    snapshots = iter(
        [
            {"db": "ok", "quote_age_s": 60, "delta_age_s": 60, "reconcile_age_s": 60},
            {"db": "ok", "quote_age_s": 1200, "delta_age_s": 60, "reconcile_age_s": 60},
            {"db": "ok", "quote_age_s": 1200, "delta_age_s": 60, "reconcile_age_s": 60},
            {"db": "ok", "quote_age_s": 60, "delta_age_s": 60, "reconcile_age_s": 60},
        ]
    )

    def snapshot_fn(_settings: Settings) -> dict[str, Any]:
        return next(snapshots)

    asyncio.run(
        run_alerter_loop(
            alerter_settings,
            snapshot_fn=snapshot_fn,
            sender=fake_sender,
            iterations=4,
        )
    )

    kinds = [("FIRING" in m, "RESOLVED" in m) for m in sent]
    # 1st cycle: nothing fires.
    # 2nd cycle: quotes_stale opens → FIRING.
    # 3rd cycle: still firing → no new message.
    # 4th cycle: resolves → RESOLVED.
    assert kinds == [(True, False), (False, True)]


def test_run_alerter_loop_skipped_when_disabled(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If ALERTER_ENABLED is false, the loop returns immediately."""

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("poly_bot.ops.alerter.asyncio.sleep", fake_sleep)

    sent: list[str] = []

    async def fake_sender(message: str) -> bool:
        sent.append(message)
        return True

    asyncio.run(
        run_alerter_loop(
            settings.model_copy(update={"alerter_enabled": False}),
            snapshot_fn=lambda _s: {"db": "ok"},
            sender=fake_sender,
            iterations=5,
        )
    )
    assert sent == []
