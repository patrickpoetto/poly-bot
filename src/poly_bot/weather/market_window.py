"""Market-resolution windows for daily weather events."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo


def market_day_window_utc(forecast_date: date, timezone: str) -> tuple[datetime, datetime]:
    """Canonical market window in UTC for a station local forecast day.

    Polymarket daily temperature markets resolve on the local calendar day
    [00:00, 24:00) in the station timezone.
    """
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        tz = UTC
    start_local = datetime.combine(forecast_date, time.min, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def forecast_window_meta(
    *,
    station_id: str,
    forecast_date: date,
    timezone: str,
    context: dict | None = None,
) -> dict[str, str]:
    """Metadata attached to every per-day forecast row."""
    windows: dict[tuple[str, date], tuple[datetime, datetime]] = {}
    if context:
        raw = context.get("market_windows")
        if isinstance(raw, dict):
            windows = raw
    start_utc, end_utc = windows.get(
        (station_id, forecast_date), market_day_window_utc(forecast_date, timezone)
    )
    return {
        "forecast_date": forecast_date.isoformat(),
        "market_timezone": timezone or "UTC",
        "window_start_utc": start_utc.isoformat(),
        "window_end_utc": end_utc.isoformat(),
    }


def weather_reference_date() -> date:
    """UTC calendar day for horizon, pills, and days-ahead labels."""
    return datetime.now(UTC).date()


def weather_horizon_bounds(*, horizon_days: int) -> tuple[date, date]:
    ref = weather_reference_date()
    return ref, ref + timedelta(days=horizon_days)


def iter_horizon_dates(*, horizon_days: int) -> list[date]:
    min_d, max_d = weather_horizon_bounds(horizon_days=horizon_days)
    days: list[date] = []
    d = min_d
    while d <= max_d:
        days.append(d)
        d += timedelta(days=1)
    return days


def days_ahead_of_today(forecast_date: date, *, ref: date | None = None) -> int:
    """Calendar days from ref (default: UTC today) to the market forecast day."""
    ref = ref or weather_reference_date()
    return (forecast_date - ref).days


def country_flag_emoji(country_code: str) -> str:
    """ISO 3166-1 alpha-2 (e.g. gb, us) → regional-indicator flag emoji."""
    code = country_code.strip().upper()
    if len(code) != 2 or not code.isalpha():
        return ""
    return "".join(chr(ord(ch) + 127397) for ch in code)


def format_days_ahead_label(forecast_date: date, *, ref: date | None = None) -> str:
    """Short Italian label for day pills and cards."""
    d = days_ahead_of_today(forecast_date, ref=ref)
    if d == 0:
        return "oggi"
    if d == 1:
        return "domani"
    if d < 0:
        return f"{d}g"
    return f"+{d}g"
