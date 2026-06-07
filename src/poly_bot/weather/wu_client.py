"""Wunderground resolution station data via api.weather.com (same backend as WU pages)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

import httpx
import structlog

from poly_bot.settings import Settings
from poly_bot.weather.market_window import market_day_window_utc
from poly_bot.weather.schema import WeatherStation
from poly_bot.weather.temperature import (
    api_value_to_celsius,
    local_hour_label,
    round_c_for_resolution,
)

log = structlog.get_logger(__name__)

_WU_API_KEY_PATTERNS = (
    re.compile(r'"apiKey"\s*:\s*"([a-f0-9]{32})"', re.I),
    re.compile(r"apiKey=([a-f0-9]{32})", re.I),
    re.compile(r"'apiKey'\s*:\s*'([a-f0-9]{32})'", re.I),
)
_WU_KEY_PAGE_URLS = (
    "https://www.wunderground.com/",
    "https://www.wunderground.com/history",
)
_WU_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
}
_TWC_BASE = "https://api.weather.com"


def _extract_api_key_from_html(html: str) -> str | None:
    for pattern in _WU_API_KEY_PATTERNS:
        m = pattern.search(html)
        if m:
            return m.group(1)
    return None


@dataclass(frozen=True, slots=True)
class WuDayForecast:
    tmax_c: int | None
    tmin_c: int | None
    tmax_hours_local: list[str]
    source: str


@dataclass(frozen=True, slots=True)
class WuStationReading:
    current_temp_c: float | None
    day: WuDayForecast
    error: str | None = None


def _api_units_for_station(station: WeatherStation) -> str:
    """weather.com units: m=metric °C, e=imperial °F."""
    return "e" if station.unit.upper() == "F" else "m"


def _geocode(station: WeatherStation) -> str | None:
    if station.latitude is None or station.longitude is None:
        return None
    return f"{station.latitude:.4f},{station.longitude:.4f}"


def _pws_station_id(station: WeatherStation) -> str | None:
    """Last path segment for non-ICAO PWS history URLs."""
    if station.icao:
        return None
    parts = station.wunderground_path.strip("/").split("/")
    return parts[-1] if parts else station.id


async def resolve_wu_api_key(
    client: httpx.AsyncClient,
    settings: Settings,
    *,
    cache: dict[str, str],
) -> str:
    if settings.weather_wu_api_key:
        return settings.weather_wu_api_key.get_secret_value()
    if "key" in cache:
        return cache["key"]
    timeout = settings.weather_wu_request_timeout_seconds
    last_err = ""
    for url in _WU_KEY_PAGE_URLS:
        try:
            r = await client.get(url, timeout=timeout, headers=_WU_HTTP_HEADERS)
            r.raise_for_status()
            key = _extract_api_key_from_html(r.text)
            if key:
                cache["key"] = key
                return key
            last_err = f"no apiKey in {url} ({len(r.text)} bytes)"
        except httpx.HTTPError as exc:
            last_err = f"{url}: {exc}"
    msg = (
        "Could not extract api.weather.com key from wunderground.com "
        f"({last_err}). Set WEATHER_WU_API_KEY in .env."
    )
    raise RuntimeError(msg)


def _date_from_local_iso(iso_local: str) -> date | None:
    if len(iso_local) >= 10:
        try:
            return date.fromisoformat(iso_local[:10])
        except ValueError:
            return None
    return None


def _pick_day_from_arrays(
    *,
    valid_times: list[str],
    temps_max: list[float | None],
    temps_min: list[float | None] | None,
    target: date,
    api_units: str,
) -> tuple[int | None, int | None]:
    tmax: int | None = None
    tmin: int | None = None
    for i, vt in enumerate(valid_times):
        if _date_from_local_iso(vt) != target:
            continue
        if i < len(temps_max) and temps_max[i] is not None:
            tmax = round_c_for_resolution(
                api_value_to_celsius(float(temps_max[i]), api_units=api_units)
            )
        if temps_min and i < len(temps_min) and temps_min[i] is not None:
            tmin = round_c_for_resolution(
                api_value_to_celsius(float(temps_min[i]), api_units=api_units)
            )
        break
    return tmax, tmin


async def _fetch_json(
    client: httpx.AsyncClient,
    path: str,
    *,
    api_key: str,
    params: dict[str, str],
    timeout: float,
) -> dict:
    q = {"apiKey": api_key, "format": "json", **params}
    r = await client.get(f"{_TWC_BASE}{path}", params=q, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        msg = f"Unexpected JSON from {path}"
        raise TypeError(msg)
    return data


async def fetch_current_c(
    client: httpx.AsyncClient,
    settings: Settings,
    station: WeatherStation,
    *,
    api_key: str,
) -> float | None:
    api_units = _api_units_for_station(station)
    timeout = settings.weather_wu_request_timeout_seconds
    if station.icao:
        data = await _fetch_json(
            client,
            "/v3/wx/observations/current",
            api_key=api_key,
            params={"icaoCode": station.icao, "units": api_units, "language": "en-US"},
            timeout=timeout,
        )
        temp = data.get("temperature")
        if temp is not None:
            return api_value_to_celsius(float(temp), api_units=api_units)
        return None
    sid = _pws_station_id(station)
    if not sid:
        return None
    data = await _fetch_json(
        client,
        "/v2/pws/observations/current",
        api_key=api_key,
        params={"stationId": sid, "units": api_units},
        timeout=timeout,
    )
    obs = data.get("observations")
    if isinstance(obs, list) and obs:
        metric = obs[0].get("metric") or {}
        temp = metric.get("temp")
        if temp is not None:
            return float(temp)
    return None


async def fetch_day_tmax(
    client: httpx.AsyncClient,
    settings: Settings,
    station: WeatherStation,
    *,
    api_key: str,
    forecast_date: date,
) -> WuDayForecast:
    api_units = _api_units_for_station(station)
    timeout = settings.weather_wu_request_timeout_seconds
    geocode = _geocode(station)
    tmax: int | None = None
    tmin: int | None = None
    source = ""

    if station.icao:
        try:
            hist = await _fetch_json(
                client,
                "/v3/wx/conditions/historical/dailysummary/30day",
                api_key=api_key,
                params={"icaoCode": station.icao, "units": api_units, "language": "en-US"},
                timeout=timeout,
            )
            tmax, tmin = _pick_day_from_arrays(
                valid_times=list(hist.get("validTimeLocal") or []),
                temps_max=list(hist.get("temperatureMax") or []),
                temps_min=list(hist.get("temperatureMin") or []),
                target=forecast_date,
                api_units=api_units,
            )
            if tmax is not None:
                source = "wu_dailysummary"
        except httpx.HTTPError as exc:
            log.debug("wu_dailysummary_failed", station=station.id, err=str(exc))

    if tmax is None and geocode:
        try:
            daily = await _fetch_json(
                client,
                "/v3/wx/forecast/daily/15day",
                api_key=api_key,
                params={"geocode": geocode, "units": api_units, "language": "en-US"},
                timeout=timeout,
            )
            tmax, tmin = _pick_day_from_arrays(
                valid_times=list(daily.get("validTimeLocal") or []),
                temps_max=list(
                    daily.get("calendarDayTemperatureMax") or daily.get("temperatureMax") or []
                ),
                temps_min=list(
                    daily.get("calendarDayTemperatureMin") or daily.get("temperatureMin") or []
                ),
                target=forecast_date,
                api_units=api_units,
            )
            if tmax is not None:
                source = "wu_forecast_daily"
        except httpx.HTTPError as exc:
            log.debug("wu_forecast_daily_failed", station=station.id, err=str(exc))

    hours = await _hourly_tmax_hours(
        client,
        settings,
        station,
        api_key=api_key,
        forecast_date=forecast_date,
        api_units=api_units,
        geocode=geocode,
    )
    return WuDayForecast(
        tmax_c=tmax, tmin_c=tmin, tmax_hours_local=hours, source=source or "missing"
    )


async def _hourly_tmax_hours(
    client: httpx.AsyncClient,
    settings: Settings,
    station: WeatherStation,
    *,
    api_key: str,
    forecast_date: date,
    api_units: str,
    geocode: str | None,
) -> list[str]:
    if not geocode:
        return []
    timeout = settings.weather_wu_request_timeout_seconds
    try:
        data = await _fetch_json(
            client,
            "/v3/wx/forecast/hourly/48hour",
            api_key=api_key,
            params={"geocode": geocode, "units": api_units, "language": "en-US"},
            timeout=timeout,
        )
    except httpx.HTTPError:
        return []

    start_utc, end_utc = market_day_window_utc(forecast_date, station.timezone)
    temps: list[tuple[datetime, float]] = []
    for vt, temp in zip(
        list(data.get("validTimeLocal") or []),
        list(data.get("temperature") or []),
        strict=False,
    ):
        if temp is None:
            continue
        try:
            dt = datetime.fromisoformat(str(vt).replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            try:
                dt = dt.replace(tzinfo=ZoneInfo(station.timezone))
            except Exception:
                continue
        dt_utc = dt.astimezone(ZoneInfo("UTC"))
        if start_utc <= dt_utc < end_utc:
            temps.append((dt_utc, api_value_to_celsius(float(temp), api_units=api_units)))

    if not temps:
        return []
    peak = max(t for _, t in temps)
    labels: list[str] = []
    for dt_utc, t in temps:
        if abs(t - peak) < 0.25:
            labels.append(local_hour_label(dt_utc.isoformat(), station.timezone))
    # dedupe preserve order
    seen: set[str] = set()
    out: list[str] = []
    for h in labels:
        if h and h not in seen:
            seen.add(h)
            out.append(h)
    return out[:4]


async def fetch_station_reading(
    settings: Settings,
    station: WeatherStation,
    *,
    forecast_date: date,
    api_key: str | None = None,
    client: httpx.AsyncClient | None = None,
    key_cache: dict[str, str] | None = None,
) -> WuStationReading:
    """Current obs + daily max for the market local day."""
    cache = key_cache if key_cache is not None else {}

    async def _run(c: httpx.AsyncClient) -> WuStationReading:
        key = api_key or await resolve_wu_api_key(c, settings, cache=cache)
        current = await fetch_current_c(c, settings, station, api_key=key)
        day = await fetch_day_tmax(c, settings, station, api_key=key, forecast_date=forecast_date)
        return WuStationReading(current_temp_c=current, day=day)

    try:
        if client is not None:
            return await _run(client)
        async with httpx.AsyncClient(
            headers=_WU_HTTP_HEADERS,
            follow_redirects=True,
        ) as hc:
            return await _run(hc)
    except Exception as exc:
        log.warning("wu_station_fetch_failed", station_id=station.id, err=str(exc))
        return WuStationReading(
            current_temp_c=None,
            day=WuDayForecast(None, None, [], "error"),
            error=str(exc),
        )
