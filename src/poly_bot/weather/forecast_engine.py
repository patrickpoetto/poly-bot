"""Merge provider outputs and compute shadow trading signals."""

from __future__ import annotations

from datetime import date

from sqlmodel import Session

from poly_bot.weather.dashboard_views import load_bin_ticks
from poly_bot.weather.forecast_repo import WeatherForecastRepository
from poly_bot.weather.forecast_schema import WeatherStationForecastConfig, WeatherTradingSignal
from poly_bot.weather.providers.base import DailyForecastPoint
from poly_bot.weather.providers.registry import get_provider
from poly_bot.weather.repo import WeatherRepository
from poly_bot.weather.schema import WeatherBin, WeatherDailyEvent, WeatherStation


def forecast_dates_for_station(
    station: WeatherStation,
    *,
    ref_date: date,
    horizon_days: int,
) -> list[date]:
    from datetime import timedelta

    from poly_bot.weather.aviation_parse import ZoneInfo

    try:
        tz = ZoneInfo(station.timezone)
    except Exception:
        from datetime import UTC

        tz = UTC
    local_today = ref_date
    try:
        from datetime import datetime

        local_today = datetime.now(tz).date()
    except Exception:
        pass
    return [local_today + timedelta(days=i) for i in range(0, horizon_days + 1)]


def merge_points_for_station(
    repo: WeatherForecastRepository,
    station: WeatherStation,
    cfg: WeatherStationForecastConfig,
    points: list[DailyForecastPoint],
) -> DailyForecastPoint | None:
    by_key = {p.source_key: p for p in points}
    for key in [cfg.primary_source_key, *cfg.fallback_source_keys]:
        p = by_key.get(key)
        if p and p.tmax_c is not None:
            merged_meta = dict(p.meta or {})
            merged_meta["chosen_source"] = key
            merged = DailyForecastPoint(
                station_id=p.station_id,
                forecast_date=p.forecast_date,
                source_key="merged",
                tmax_value=p.tmax_value,
                unit=p.unit,
                tmax_c=p.tmax_c,
                coverage=p.coverage,
                issued_at=p.issued_at,
                valid_from=p.valid_from,
                valid_to=p.valid_to,
                meta=merged_meta,
            )
            repo.upsert_daily_forecast(merged, role="merged")
            return merged
    return None


def exact_bin_for_temp(bins: list[WeatherBin], temp_c: int) -> WeatherBin | None:
    for b in bins:
        if b.bin_kind == "exact" and b.temp_c == temp_c:
            return b
    return None


def market_favorite(bins: list[WeatherBin], ticks: dict) -> tuple[int | None, float | None]:
    best_mid = -1.0
    best_c: int | None = None
    for b in bins:
        if b.bin_kind != "exact" or b.temp_c is None:
            continue
        tick = ticks.get(b.condition_id)
        yes = tick.yes_mid if tick else None
        if yes is not None and yes > best_mid:
            best_mid = yes
            best_c = b.temp_c
    if best_c is None:
        return None, None
    return best_c, best_mid if best_mid >= 0 else None


def build_shadow_signal(
    ev: WeatherDailyEvent,
    station: WeatherStation,
    cfg: WeatherStationForecastConfig,
    bins: list[WeatherBin],
    *,
    merged_tmax_c: int | None,
    chosen_source: str,
    ticks: dict,
) -> WeatherTradingSignal:
    model_bin = exact_bin_for_temp(bins, merged_tmax_c) if merged_tmax_c is not None else None
    mkt_c, mkt_mid = market_favorite(bins, ticks)
    edge = "no_forecast"
    if merged_tmax_c is not None and mkt_c is None:
        edge = "no_quotes"
    elif merged_tmax_c is not None and mkt_c is not None:
        if merged_tmax_c == mkt_c:
            edge = "aligned"
        elif merged_tmax_c > mkt_c:
            edge = "model_higher"
        else:
            edge = "model_lower"
    return WeatherTradingSignal(
        weather_event_id=ev.id,
        station_id=station.id,
        forecast_date=ev.forecast_date,
        trading_mode=cfg.trading_mode,
        model_tmax_c=merged_tmax_c,
        model_bin_label=model_bin.bin_label
        if model_bin
        else (f"{merged_tmax_c}°C" if merged_tmax_c else ""),
        market_favorite_c=mkt_c,
        market_favorite_mid=mkt_mid,
        edge_hint=edge,
        primary_source_key=chosen_source,
        meta={"trading_enabled": cfg.trading_enabled},
    )


def refresh_trading_signals(
    session: Session,
    *,
    min_date: date,
    max_date: date,
) -> int:
    wrepo = WeatherRepository(session)
    frepo = WeatherForecastRepository(session)
    events = wrepo.list_daily_events(min_date=min_date, max_date=max_date)
    n = 0
    for ev in events:
        station = wrepo.get_station(ev.station_id)
        if station is None:
            continue
        cfg = session.get(WeatherStationForecastConfig, station.id)
        if cfg is None:
            continue
        merged = frepo.get_merged_forecast(station.id, ev.forecast_date)
        bins = wrepo.bins_for_event(ev.id)
        ticks = load_bin_ticks(session, bins)
        tmax_c = merged.tmax_c if merged else None
        source = merged.meta.get("chosen_source", cfg.primary_source_key) if merged else ""
        sig = build_shadow_signal(
            ev,
            station,
            cfg,
            bins,
            merged_tmax_c=tmax_c,
            chosen_source=str(source),
            ticks=ticks,
        )
        frepo.upsert_trading_signal(sig)
        n += 1
    return n


def produce_from_providers(
    frepo: WeatherForecastRepository,
    station: WeatherStation,
    cfg: WeatherStationForecastConfig,
    forecast_dates: list[date],
    context: dict,
) -> None:
    keys = [cfg.primary_source_key, *cfg.fallback_source_keys]
    all_points: list[DailyForecastPoint] = []
    for key in keys:
        provider = get_provider(key)
        if provider is None:
            continue
        points = provider.produce_daily_forecasts(station, forecast_dates, context=context)
        for p in points:
            role = "primary" if key == cfg.primary_source_key else "fallback"
            frepo.upsert_daily_forecast(p, role=role)
        all_points.extend(points)
    merge_points_for_station(frepo, station, cfg, all_points)
