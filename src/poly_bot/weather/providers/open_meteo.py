"""Open-Meteo multi-model provider (free tier, station-coordinate based)."""

from __future__ import annotations

from datetime import date, datetime
from math import asin, cos, radians, sin, sqrt
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx

from poly_bot.weather.aviation_parse import celsius_to_station_unit
from poly_bot.weather.market_window import forecast_window_meta
from poly_bot.weather.providers.base import DailyForecastPoint
from poly_bot.weather.schema import WeatherStation

_MODEL_TO_SOURCE = {
    "temperature_2m_max_ecmwf_ifs025": "open_meteo_ecmwf",
    "temperature_2m_max_gfs_seamless": "open_meteo_gfs",
    "temperature_2m_max_icon_seamless": "open_meteo_icon",
}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * r * asin(sqrt(a))


class OpenMeteoMultiProvider:
    source_key = "open_meteo_blend"

    def produce_daily_forecasts(
        self,
        station: WeatherStation,
        forecast_dates: list[date],
        *,
        context: dict[str, Any],
    ) -> list[DailyForecastPoint]:
        if not forecast_dates:
            return []
        if station.latitude is None or station.longitude is None:
            return [
                DailyForecastPoint(
                    station_id=station.id,
                    forecast_date=d,
                    source_key=self.source_key,
                    tmax_value=None,
                    unit=station.unit,
                    tmax_c=None,
                    coverage="none",
                    meta={
                        "reason": "missing_coordinates",
                        **forecast_window_meta(
                            station_id=station.id,
                            forecast_date=d,
                            timezone=station.timezone,
                            context=context,
                        ),
                    },
                )
                for d in forecast_dates
            ]

        cache: dict[str, dict[str, Any]] = context.setdefault("open_meteo_cache", {})
        cache_key = f"{station.id}:{min(forecast_dates)}:{max(forecast_dates)}"
        payload = cache.get(cache_key)
        if payload is None:
            base_url = str(
                context.get("open_meteo_base_url") or "https://api.open-meteo.com/v1/forecast"
            )
            timeout = float(context.get("open_meteo_timeout", 20.0))
            model_arg = str(
                context.get("open_meteo_models", "ecmwf_ifs025,gfs_seamless,icon_seamless")
            )
            tz_name = station.timezone or "UTC"
            try:
                local_today = datetime.now(ZoneInfo(tz_name)).date()
            except Exception:
                local_today = date.today()
            max_fd = max(forecast_dates)
            forecast_days = max(1, (max_fd - local_today).days + 1)
            params = {
                "latitude": station.latitude,
                "longitude": station.longitude,
                "daily": "temperature_2m_max",
                "timezone": tz_name,
                "forecast_days": forecast_days,
                "models": model_arg,
            }
            with httpx.Client(timeout=timeout) as client:
                resp = client.get(base_url, params=params)
                resp.raise_for_status()
                payload = resp.json()
            cache[cache_key] = payload

        daily = payload.get("daily") or {}
        times = daily.get("time") or []
        index_by_date: dict[date, int] = {}
        for i, raw in enumerate(times):
            try:
                index_by_date[date.fromisoformat(str(raw)[:10])] = i
            except ValueError:
                continue

        api_lat = float(payload.get("latitude", station.latitude))
        api_lon = float(payload.get("longitude", station.longitude))
        dist_km = _haversine_km(station.latitude, station.longitude, api_lat, api_lon)

        out: list[DailyForecastPoint] = []
        for fd in forecast_dates:
            win_meta = forecast_window_meta(
                station_id=station.id,
                forecast_date=fd,
                timezone=station.timezone,
                context=context,
            )
            idx = index_by_date.get(fd)
            values_c: list[float] = []
            for field, source in _MODEL_TO_SOURCE.items():
                val = None
                arr = daily.get(field)
                if isinstance(arr, list) and idx is not None and idx < len(arr):
                    raw_val = arr[idx]
                    if raw_val is not None:
                        val = float(raw_val)
                tmax_c = round(val) if val is not None else None
                if val is not None:
                    values_c.append(val)
                out.append(
                    DailyForecastPoint(
                        station_id=station.id,
                        forecast_date=fd,
                        source_key=source,
                        tmax_value=(
                            celsius_to_station_unit(tmax_c, station.unit) if tmax_c is not None else None
                        ),
                        unit=station.unit,
                        tmax_c=tmax_c,
                        coverage="full" if tmax_c is not None else "none",
                        meta={
                            "provider": "open-meteo",
                            "model_field": field,
                            "distance_km": round(dist_km, 2),
                            "query": urlencode(
                                {"lat": station.latitude, "lon": station.longitude, "tz": station.timezone}
                            ),
                            **win_meta,
                        },
                    )
                )

            blend_c = round(sum(values_c) / len(values_c)) if values_c else None
            out.append(
                DailyForecastPoint(
                    station_id=station.id,
                    forecast_date=fd,
                    source_key=self.source_key,
                    tmax_value=(
                        celsius_to_station_unit(blend_c, station.unit) if blend_c is not None else None
                    ),
                    unit=station.unit,
                    tmax_c=blend_c,
                    coverage="full" if blend_c is not None else "none",
                    meta={
                        "provider": "open-meteo",
                        "models_used": len(values_c),
                        "distance_km": round(dist_km, 2),
                        "source_keys": [v for v in _MODEL_TO_SOURCE.values()],
                        **win_meta,
                    },
                )
            )
        return out

