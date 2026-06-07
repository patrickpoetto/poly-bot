"""Aviation TAF + METAR providers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from poly_bot.weather.aviation_parse import (
    epoch_to_utc,
    parse_tx_tn_groups,
    taf_tx_temp_unit,
    tx_tmax_for_utc_window,
)
from poly_bot.weather.market_window import forecast_window_meta, market_day_window_utc
from poly_bot.weather.providers.base import DailyForecastPoint
from poly_bot.weather.schema import WeatherStation


class AviationTafProvider:
    source_key = "aviation_taf"

    def produce_daily_forecasts(
        self,
        station: WeatherStation,
        forecast_dates: list[date],
        *,
        context: dict[str, Any],
    ) -> list[DailyForecastPoint]:
        taf_by_icao: dict[str, dict[str, Any]] = context.get("taf_by_icao") or {}
        icao = (station.icao or station.id).upper()
        payload = taf_by_icao.get(icao)
        if not payload:
            return [
                DailyForecastPoint(
                    station_id=station.id,
                    forecast_date=d,
                    source_key=self.source_key,
                    tmax_value=None,
                    unit=station.unit,
                    tmax_c=None,
                    coverage="missing",
                    meta=forecast_window_meta(
                        station_id=station.id,
                        forecast_date=d,
                        timezone=station.timezone,
                        context=context,
                    ),
                )
                for d in forecast_dates
            ]

        issue_time = epoch_to_utc(payload.get("issueTime") or payload.get("bulletinTime"))
        raw = str(payload.get("rawTAF") or "")
        tx_unit = taf_tx_temp_unit(station.icao)
        groups = parse_tx_tn_groups(raw, issue_time=issue_time, unit=tx_unit)
        valid_from = epoch_to_utc(payload.get("validTimeFrom"))
        valid_to = epoch_to_utc(payload.get("validTimeTo"))
        coverage_base = "temp_tx" if groups else "no_temp"

        out: list[DailyForecastPoint] = []
        for fd in forecast_dates:
            window_map: dict[tuple[str, date], tuple[datetime, datetime]] = (
                context.get("market_windows") or {}
            )
            window_start_utc, window_end_utc = window_map.get(
                (station.id, fd), market_day_window_utc(fd, station.timezone)
            )
            win_meta = forecast_window_meta(
                station_id=station.id,
                forecast_date=fd,
                timezone=station.timezone,
                context=context,
            )
            tmax_c, meta = tx_tmax_for_utc_window(
                groups,
                window_start_utc=window_start_utc,
                window_end_utc=window_end_utc,
                unit=tx_unit,
            )
            tmax_val = None
            if tmax_c is not None:
                from poly_bot.weather.aviation_parse import celsius_to_station_unit

                tmax_val = celsius_to_station_unit(tmax_c, station.unit)
                meta = {**meta, "taf_tx_unit": tx_unit}
            cov = coverage_base if tmax_c is not None else "no_temp"
            out.append(
                DailyForecastPoint(
                    station_id=station.id,
                    forecast_date=fd,
                    source_key=self.source_key,
                    tmax_value=tmax_val,
                    unit=station.unit,
                    tmax_c=tmax_c,
                    coverage=cov,
                    issued_at=issue_time,
                    valid_from=valid_from,
                    valid_to=valid_to,
                    meta={"raw_len": len(raw), **win_meta, **meta},
                )
            )
        return out


class AviationMetarRunningProvider:
    source_key = "aviation_metar_running"

    def produce_daily_forecasts(
        self,
        station: WeatherStation,
        forecast_dates: list[date],
        *,
        context: dict[str, Any],
    ) -> list[DailyForecastPoint]:
        running: dict[tuple[str, date], dict[str, Any]] = context.get("metar_running") or {}
        out: list[DailyForecastPoint] = []

        for fd in forecast_dates:
            win_meta = forecast_window_meta(
                station_id=station.id,
                forecast_date=fd,
                timezone=station.timezone,
                context=context,
            )
            stats = running.get((station.id, fd))
            if not stats:
                out.append(
                    DailyForecastPoint(
                        station_id=station.id,
                        forecast_date=fd,
                        source_key=self.source_key,
                        tmax_value=None,
                        unit=station.unit,
                        tmax_c=None,
                        coverage="none",
                        meta=win_meta,
                    )
                )
                continue
            tmax_c = stats.get("tmax_c")
            tmax_val = stats.get("tmax_value")
            out.append(
                DailyForecastPoint(
                    station_id=station.id,
                    forecast_date=fd,
                    source_key=self.source_key,
                    tmax_value=tmax_val,
                    unit=station.unit,
                    tmax_c=int(tmax_c) if tmax_c is not None else None,
                    coverage=stats.get("coverage", "partial"),
                    issued_at=stats.get("latest_obs"),
                    meta={
                        "obs_count": stats.get("obs_count", 0),
                        **win_meta,
                    },
                )
            )
        return out
