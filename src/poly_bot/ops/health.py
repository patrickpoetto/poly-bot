"""Healthcheck for Docker / Kubernetes — DB ping + activity freshness."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

from sqlalchemy import text
from sqlmodel import Session

from poly_bot.persistence.catalog_state import (
    KEY_LAST_DELTA_SCAN_AT,
    KEY_LAST_DISCOVERY_ACTIVITY_AT,
    KEY_LAST_FULL_SCAN_AT,
    KEY_LAST_RECONCILE_AT,
    KEY_LAST_RECONCILE_REPORT,
    CatalogStateStore,
)
from poly_bot.persistence.db import get_engine
from poly_bot.settings import Settings

ServiceKind = Literal["any", "discovery", "watch", "ws", "dashboard"]


def health_snapshot(settings: Settings) -> dict[str, object]:
    """Lightweight, side-effect-free snapshot for alerter + dashboard.

    Returns ages (in seconds) of the freshest signal per pipeline, the last
    reconcile report (best-effort JSON), and basic counters. Never raises —
    if DB is unreachable, ``db`` is set to the error class name and the
    rest of the keys are ``None``.
    """

    snapshot: dict[str, object] = {
        "db": "ok",
        "quote_age_s": None,
        "delta_age_s": None,
        "full_age_s": None,
        "discovery_activity_age_s": None,
        "reconcile_age_s": None,
        "reconcile_report": None,
    }

    engine = get_engine(settings)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).one()
            row = conn.execute(text("SELECT MAX(updated_at) FROM token_quotes")).one()
        latest_quote = _parse_dt(row[0] if row else None)
        snapshot["quote_age_s"] = _age_seconds(latest_quote)
    except Exception as exc:
        snapshot["db"] = f"down: {exc.__class__.__name__}"
        return snapshot

    try:
        with Session(engine) as session:
            store = CatalogStateStore(session)
            snapshot["delta_age_s"] = _age_seconds(store.get_datetime(KEY_LAST_DELTA_SCAN_AT))
            snapshot["full_age_s"] = _age_seconds(store.get_datetime(KEY_LAST_FULL_SCAN_AT))
            snapshot["discovery_activity_age_s"] = _age_seconds(
                store.get_datetime(KEY_LAST_DISCOVERY_ACTIVITY_AT)
            )
            snapshot["reconcile_age_s"] = _age_seconds(
                store.get_datetime(KEY_LAST_RECONCILE_AT)
            )
            report_raw = store.get(KEY_LAST_RECONCILE_REPORT)
            if report_raw:
                import json as _json

                try:
                    snapshot["reconcile_report"] = _json.loads(report_raw)
                except (ValueError, TypeError):
                    snapshot["reconcile_report"] = None
    except Exception as exc:
        snapshot["db"] = f"down: {exc.__class__.__name__}"

    return snapshot


def check_health(
    settings: Settings,
    *,
    service: ServiceKind = "any",
    max_age_seconds: int = 600,
) -> tuple[bool, dict[str, str | int | bool]]:
    """Return (ok, details). Used by ``poly-bot ops health``."""
    details: dict[str, str | int | bool] = {"service": service}
    engine = get_engine(settings)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).one()
    except Exception as exc:
        details["db"] = f"down: {exc.__class__.__name__}"
        return False, details
    details["db"] = "ok"

    if service in ("discovery", "any"):
        with Session(engine) as session:
            state = CatalogStateStore(session)
            for key in (KEY_LAST_DELTA_SCAN_AT, KEY_LAST_FULL_SCAN_AT):
                ts = state.get_datetime(key)
                age = _age_seconds(ts)
                details[key] = age if age is not None else "never"
            activity_age = _age_seconds(
                state.get_datetime(KEY_LAST_DISCOVERY_ACTIVITY_AT)
            )
            details[KEY_LAST_DISCOVERY_ACTIVITY_AT] = (
                activity_age if activity_age is not None else "never"
            )
            if service == "discovery" and activity_age is not None:
                if activity_age > max_age_seconds:
                    details["reason"] = (
                        f"discovery inactive ({activity_age}s > {max_age_seconds}s)"
                    )
                    return False, details

    if service in ("watch", "ws", "any"):
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT MAX(updated_at) FROM token_quotes")
            ).one()
        latest = _parse_dt(row[0] if row else None)
        age = _age_seconds(latest)
        details["quote_age_s"] = age if age is not None else "never"
        if (
            service in ("watch", "ws")
            and age is not None
            and age > max_age_seconds
        ):
            details["reason"] = f"quotes stale ({age}s > {max_age_seconds}s)"
            return False, details

    return True, details


def _age_seconds(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    delta: timedelta = datetime.now(UTC) - dt
    return int(delta.total_seconds())


def _parse_dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
