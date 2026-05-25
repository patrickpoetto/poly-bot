"""Healthcheck for Docker / Kubernetes — DB ping + activity freshness."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

from sqlalchemy import text
from sqlmodel import Session

from poly_bot.persistence.catalog_state import (
    KEY_LAST_DELTA_SCAN_AT,
    KEY_LAST_FULL_SCAN_AT,
    CatalogStateStore,
)
from poly_bot.persistence.db import get_engine
from poly_bot.settings import Settings

ServiceKind = Literal["any", "discovery", "watch", "ws", "dashboard"]


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
                if (
                    service == "discovery"
                    and age is not None
                    and age > max_age_seconds
                ):
                    details["reason"] = f"{key} stale ({age}s > {max_age_seconds}s)"
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
