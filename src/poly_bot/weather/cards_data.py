"""JSON payload for weather dashboard cards (WU station + quotes + health)."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime

from sqlmodel import Session, col, select

from poly_bot.persistence.schema import TokenQuote
from poly_bot.settings import Settings
from poly_bot.weather.dashboard_views import BinTick, WeatherCityRow, load_bin_ticks, load_city_rows
from poly_bot.weather.forecast_views import load_forecast_context_for_rows
from poly_bot.weather.quotes import refresh_event_quotes_from_clob
from poly_bot.weather.schema import WeatherStationWuSnapshot
from poly_bot.weather.station_sync import latest_wu_sync_time
from poly_bot.weather.temperature import format_temp_pair
from poly_bot.weather.market_window import (
    country_flag_emoji,
    days_ahead_of_today,
    format_days_ahead_label,
    market_day_window_utc,
)


def _utc_dt(dt: datetime | None) -> datetime | None:
    """Normalize DB timestamps for comparison with datetime.now(UTC)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _quote_health(session: Session, rows: list[WeatherCityRow]) -> dict[str, object]:
    token_ids = [
        tid
        for row in rows
        for b in row.bins
        for tid in (b.yes_token_id, b.no_token_id)
        if tid
    ]
    if not token_ids:
        return {"last_at": None, "stale_s": None, "ok": 0, "missing": len(rows)}
    quotes = list(
        session.exec(select(TokenQuote).where(col(TokenQuote.token_id).in_(token_ids))).all()
    )
    if not quotes:
        return {"last_at": None, "stale_s": None, "ok": 0, "missing": len(rows)}
    now = datetime.now(UTC)
    newest: datetime | None = None
    stale_count = 0
    for q in quotes:
        ts = _utc_dt(q.updated_at)
        if ts and (newest is None or ts > newest):
            newest = ts
        if ts and (now - ts).total_seconds() > 600:
            stale_count += 1
    stale_s = (now - newest).total_seconds() if newest else None
    return {
        "last_at": newest.isoformat() if newest else None,
        "stale_s": round(stale_s, 1) if stale_s is not None else None,
        "ok": len(quotes),
        "stale_tokens": stale_count,
    }


def _wu_health(session: Session, rows: list[WeatherCityRow], forecast_date: date) -> dict[str, object]:
    station_ids = {r.event.station_id for r in rows}
    snaps = list(
        session.exec(
            select(WeatherStationWuSnapshot).where(
                col(WeatherStationWuSnapshot.station_id).in_(list(station_ids)),
                WeatherStationWuSnapshot.forecast_date == forecast_date,
            )
        ).all()
    )
    now = datetime.now(UTC)
    ok = sum(1 for s in snaps if s.status == "ok")
    err = sum(1 for s in snaps if s.status == "error")
    partial = sum(1 for s in snaps if s.status == "partial")
    missing = len(station_ids) - len(snaps)
    newest = max(
        (_utc_dt(s.fetched_at) for s in snaps if s.fetched_at),
        default=None,
    )
    global_last = _utc_dt(latest_wu_sync_time(session))
    stale_s = (now - newest).total_seconds() if newest else None
    return {
        "last_at": (newest or global_last).isoformat() if (newest or global_last) else None,
        "stale_s": round(stale_s, 1) if stale_s is not None else None,
        "ok": ok,
        "error": err,
        "partial": partial,
        "missing": missing,
    }


def _price_bars(row: WeatherCityRow, ticks: dict[str, BinTick]) -> list[dict[str, object]]:
    """Compact distribution: exact bins by yes mid (¢), tails as No mid."""
    exact = [b for b in row.bins if b.bin_kind == "exact" and b.temp_c is not None]
    exact.sort(key=lambda b: b.temp_c or 0)
    bars: list[dict[str, object]] = []
    for b in exact:
        t = ticks.get(b.condition_id)
        yes = t.yes_mid if t else None
        bars.append(
            {
                "label": f"{b.temp_c}°",
                "temp_c": b.temp_c,
                "yes_mid": yes,
                "no_mid": t.no_mid if t else None,
                "kind": "exact",
            }
        )
    for b in row.bins:
        if b.bin_kind not in ("at_or_below", "at_or_above"):
            continue
        t = ticks.get(b.condition_id)
        label = b.bin_label[:12] if b.bin_label else b.bin_kind
        bars.append(
            {
                "label": label,
                "temp_c": b.temp_c_upper or b.temp_c_lower,
                "yes_mid": t.yes_mid if t else None,
                "no_mid": t.no_mid if t else None,
                "kind": b.bin_kind,
            }
        )
    return bars


def _favorite_bin(bars: list[dict[str, object]]) -> dict[str, object] | None:
    best: dict[str, object] | None = None
    best_yes = -1.0
    for bar in bars:
        if bar.get("kind") != "exact":
            continue
        y = bar.get("yes_mid")
        if y is not None and float(y) > best_yes:
            best_yes = float(y)
            best = bar
    return best


def build_weather_cards_payload(
    session: Session,
    settings: Settings,
    *,
    forecast_date: date,
    city_q: str = "",
    refresh_quotes: bool = False,
) -> dict[str, object]:
    rows = load_city_rows(session, forecast_date=forecast_date, city_q=city_q)
    if refresh_quotes and rows:

        async def _refresh_all() -> None:
            for row in rows:
                await refresh_event_quotes_from_clob(
                    session, settings, event_id=row.event.id
                )

        asyncio.run(_refresh_all())
        session.commit()

    fc_ctx = load_forecast_context_for_rows(session, rows, forecast_date=forecast_date)
    snaps: dict[tuple[str, date], WeatherStationWuSnapshot] = {}
    if rows:
        station_ids = [r.event.station_id for r in rows]
        for snap in session.exec(
            select(WeatherStationWuSnapshot).where(
                col(WeatherStationWuSnapshot.station_id).in_(station_ids),
                WeatherStationWuSnapshot.forecast_date == forecast_date,
            )
        ).all():
            snaps[(snap.station_id, snap.forecast_date)] = snap

    cards: list[dict[str, object]] = []
    for row in rows:
        ev = row.event
        st = row.station
        ticks = load_bin_ticks(session, row.bins)
        snap = snaps.get((ev.station_id, forecast_date))
        unit = st.unit if st else "C"
        tz = st.timezone if st else "UTC"
        win_start, win_end = market_day_window_utc(forecast_date, tz)

        wu_block: dict[str, object] = {
            "status": snap.status if snap else "missing",
            "error": snap.error_message if snap else "",
            "fetched_at": snap.fetched_at.isoformat() if snap and snap.fetched_at else None,
            "current_c": snap.current_temp_c if snap else None,
            "current": format_temp_pair(snap.current_temp_c if snap else None, resolution_unit=unit),
            "tmax_c": snap.tmax_c if snap else None,
            "tmax": format_temp_pair(float(snap.tmax_c) if snap and snap.tmax_c is not None else None, resolution_unit=unit),
            "tmin_c": snap.tmin_c if snap else None,
            "tmax_hours_local": (snap.tmax_hours_local.split(",") if snap and snap.tmax_hours_local else []),
            "source": snap.source if snap else "",
            "url": st.wunderground_url if st else "",
        }

        bars = _price_bars(row, ticks)
        fav = _favorite_bin(bars)
        fc = fc_ctx.get(ev.station_id)
        model_tmax = None
        if fc is not None:
            model_tmax = getattr(fc, "merged_tmax_c", None)

        cards.append(
            {
                "event_id": ev.id,
                "city_key": ev.city_key,
                "display_name": (row.city.display_name if row.city else ev.city_key),
                "country_code": (st.country_code if st else "") or "",
                "flag": country_flag_emoji(st.country_code if st else ""),
                "forecast_date": forecast_date.isoformat(),
                "days_ahead": days_ahead_of_today(forecast_date),
                "days_ahead_label": format_days_ahead_label(forecast_date),
                "station": {
                    "id": st.id if st else "",
                    "name": (st.station_name if st else "") or "",
                    "icao": st.icao if st else None,
                    "lat": st.latitude if st else None,
                    "lng": st.longitude if st else None,
                    "timezone": tz,
                    "unit": unit,
                    "wunderground_url": st.wunderground_url if st else "",
                },
                "market_window": {
                    "start_utc": win_start.isoformat(),
                    "end_utc": win_end.isoformat(),
                },
                "wu": wu_block,
                "model_tmax_c": model_tmax,
                "price_bars": bars,
                "favorite": fav,
                "detail_url": f"/weather/event?city={ev.city_key}&day={forecast_date.isoformat()}",
                "bin_count": len(row.bins),
            }
        )

    return {
        "forecast_date": forecast_date.isoformat(),
        "server_time": datetime.now(UTC).isoformat(),
        "poll_seconds": settings.weather_dashboard_poll_seconds,
        "health": {
            "wu": _wu_health(session, rows, forecast_date),
            "quotes": _quote_health(session, rows),
        },
        "cards": cards,
    }
