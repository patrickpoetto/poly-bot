"""Condition IDs and live CLOB quotes for daily temperature bins."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlmodel import Session, col, select

from poly_bot.exchange.clob_public import ClobPublicClient
from poly_bot.persistence.schema import Token, TokenQuote
from poly_bot.settings import Settings
from poly_bot.weather.schema import WeatherBin, WeatherDailyEvent


def weather_forecast_date_range(settings: Settings) -> tuple[date, date]:
    ref = date.today()
    return ref, ref + timedelta(days=settings.weather_forecast_horizon_days)


def weather_condition_ids(
    session: Session,
    settings: Settings,
    *,
    min_date: date | None = None,
    max_date: date | None = None,
) -> list[str]:
    """All active weather bin markets in the synced forecast range (ignores 6h window)."""
    if min_date is None or max_date is None:
        d0, d1 = weather_forecast_date_range(settings)
        min_date = min_date or d0
        max_date = max_date or d1
    rows = session.exec(
        select(WeatherBin.condition_id)
        .join(WeatherDailyEvent, WeatherDailyEvent.id == WeatherBin.weather_event_id)
        .where(WeatherDailyEvent.forecast_date >= min_date)
        .where(WeatherDailyEvent.forecast_date <= max_date)
        .where(WeatherBin.active == True)  # noqa: E712
    ).all()
    return [str(r) for r in rows]


async def refresh_event_quotes_from_clob(
    session: Session,
    settings: Settings,
    *,
    event_id: str,
) -> int:
    """REST midpoints for one city's bins (~20 tokens) — works without ws-daemon."""
    bins = session.exec(
        select(WeatherBin).where(WeatherBin.weather_event_id == event_id)
    ).all()
    if not bins:
        return 0
    token_ids = [
        tid
        for b in bins
        for tid in (b.yes_token_id, b.no_token_id)
        if tid
    ]
    if not token_ids:
        return 0

    clob = ClobPublicClient(settings)
    mids = await clob.fetch_midpoints(token_ids)
    spreads = await clob.fetch_spreads(token_ids)
    now = datetime.now(UTC)
    written = 0
    for tid in token_ids:
        mid = mids.get(tid)
        spread = spreads.get(tid)
        if mid is None and spread is None:
            continue
        token = session.get(Token, tid)
        if token is None:
            continue
        row = session.get(TokenQuote, tid) or TokenQuote(
            token_id=tid,
            condition_id=token.condition_id,
        )
        row.mid = mid
        row.spread = spread
        if mid is not None and spread is not None:
            row.bid = max(0.0, mid - spread / 2)
            row.ask = min(1.0, mid + spread / 2)
        row.updated_at = now
        session.add(row)
        written += 1
    session.commit()
    return written
