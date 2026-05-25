"""Key-value state for discovery watermarks and scheduler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlmodel import Field, Session, SQLModel, select


class CatalogState(SQLModel, table=True):
    __tablename__ = "catalog_state"

    key: str = Field(primary_key=True)
    value: str = Field(default="")


KEY_MARKETS_DELTA_WATERMARK = "markets_delta_watermark"
KEY_EVENTS_DELTA_WATERMARK = "events_delta_watermark"
KEY_LAST_FULL_SCAN_AT = "last_full_scan_at"
KEY_LAST_DELTA_SCAN_AT = "last_delta_scan_at"
KEY_LAST_SERIES_SCAN_AT = "last_series_scan_at"


class CatalogStateStore:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get(self, key: str) -> str | None:
        row = self._session.get(CatalogState, key)
        return row.value if row else None

    def set(self, key: str, value: str) -> None:
        row = self._session.get(CatalogState, key)
        if row is None:
            row = CatalogState(key=key, value=value)
        else:
            row.value = value
        self._session.add(row)

    def get_datetime(self, key: str) -> datetime | None:
        raw = self.get(key)
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except ValueError:
            return None

    def set_datetime(self, key: str, value: datetime) -> None:
        self.set(key, value.astimezone(UTC).isoformat())

    def get_watermark(self, key: str, *, default_lookback_hours: float = 2.0) -> datetime:
        dt = self.get_datetime(key)
        if dt is None:
            return datetime.now(UTC) - timedelta(hours=default_lookback_hours)
        return dt

    def set_watermark_now(self, key: str) -> datetime:
        now = datetime.now(UTC)
        self.set_datetime(key, now)
        return now
