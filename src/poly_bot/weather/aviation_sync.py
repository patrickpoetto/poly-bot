"""Sync TAF/METAR for all catalog stations; refresh daily forecasts and shadow signals."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import structlog
from sqlmodel import Session, select

from poly_bot.persistence.db import get_engine
from poly_bot.settings import Settings
from poly_bot.weather.aviation_client import AviationWeatherClient
from poly_bot.weather.aviation_parse import (
    epoch_to_utc,
    metar_temp_values,
    parse_metar_temp_from_raw,
    parse_tx_tn_groups,
    taf_tx_temp_unit,
)
from poly_bot.weather.forecast_schema import WeatherMetarObservation
from poly_bot.weather.market_window import market_day_window_utc
from poly_bot.weather.parse import parse_resolution_unit
from poly_bot.weather.repo import WeatherRepository
from poly_bot.weather.forecast_engine import (
    forecast_dates_for_station,
    produce_from_providers,
    refresh_trading_signals,
)
from poly_bot.weather.forecast_repo import WeatherForecastRepository
logger = structlog.get_logger(__name__)

BATCH_SIZE = 25


def _parse_issue_time(payload: dict[str, Any]) -> datetime | None:
    return epoch_to_utc(payload.get("issueTime") or payload.get("bulletinTime"))


class WeatherAviationSync:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = AviationWeatherClient(base_url=settings.weather_aviation_api_base)

    def sync(self) -> dict[str, int]:
        ref = date.today()
        max_date = ref + timedelta(days=self._settings.weather_forecast_horizon_days)
        pilot_ids = set(self._settings.weather_pilot_station_ids)

        stats = {
            "stations": 0,
            "metar_new": 0,
            "taf_updated": 0,
            "sources_seeded": 0,
            "forecasts_written": 0,
            "signals": 0,
            "icao_missing": 0,
        }

        with Session(get_engine(self._settings)) as session:
            frepo = WeatherForecastRepository(session)
            stats["sources_seeded"] = frepo.ensure_builtin_sources()

            stations = frepo.list_stations_with_icao()
            stats["stations"] = len(stations)
            wrepo = WeatherRepository(session)
            events = wrepo.list_daily_events(min_date=ref, max_date=max_date)
            rules_by_station: dict[str, str] = {}
            for ev in events:
                if ev.station_id not in rules_by_station and ev.resolution_rules_text:
                    rules_by_station[ev.station_id] = ev.resolution_rules_text

            for st in stations:
                rules = rules_by_station.get(st.id, "")
                if rules:
                    correct_unit = parse_resolution_unit(rules)
                    if st.unit != correct_unit:
                        st.unit = correct_unit
                        session.add(st)
                frepo.ensure_station_config(
                    st,
                    trading_enabled=st.id in pilot_ids or st.icao in pilot_ids,
                )

            icao_list = [s.icao.upper() for s in stations if s.icao]
            taf_by_icao: dict[str, dict[str, Any]] = {}
            metar_by_icao: dict[str, dict[str, Any]] = {}

            for i in range(0, len(icao_list), BATCH_SIZE):
                chunk = icao_list[i : i + BATCH_SIZE]
                for row in self._client.fetch_taf(chunk):
                    icao = str(row.get("icaoId") or "").upper()
                    if icao:
                        taf_by_icao[icao] = row
                for row in self._client.fetch_metar(chunk):
                    icao = str(row.get("icaoId") or "").upper()
                    if icao:
                        metar_by_icao[icao] = row

            station_by_icao = {s.icao.upper(): s for s in stations if s.icao}
            station_by_id = {s.id: s for s in stations}

            for row in session.exec(select(WeatherMetarObservation)).all():
                st = station_by_icao.get(row.icao.upper()) if row.icao else None
                if st is None or not row.raw_ob:
                    continue
                parsed = parse_metar_temp_from_raw(row.raw_ob)
                if parsed is None:
                    continue
                temp_c, temp_f = metar_temp_values(parsed, resolution_unit=st.unit)
                row.temp_c = temp_c
                row.temp_f = temp_f
                session.add(row)

            for icao, payload in metar_by_icao.items():
                st = station_by_icao.get(icao)
                if st is None:
                    continue
                obs = epoch_to_utc(payload.get("obsTime") or payload.get("reportTime"))
                if obs is None:
                    continue
                temp = payload.get("temp")
                temp_c, temp_f = metar_temp_values(temp, resolution_unit=st.unit)
                if frepo.upsert_metar(
                    station_id=st.id,
                    icao=icao,
                    obs_time=obs,
                    temp_c=temp_c,
                    temp_f=temp_f,
                    raw_ob=str(payload.get("rawOb") or ""),
                ):
                    stats["metar_new"] += 1

            for icao, payload in taf_by_icao.items():
                st = station_by_icao.get(icao)
                if st is None:
                    continue
                issue_time = _parse_issue_time(payload)
                raw = str(payload.get("rawTAF") or "")
                tx_unit = taf_tx_temp_unit(st.icao)
                groups = parse_tx_tn_groups(raw, issue_time=issue_time, unit=tx_unit)
                parsed = [
                    {
                        "kind": g.kind,
                        "value": g.value,
                        "valid_at": g.valid_at.isoformat(),
                    }
                    for g in groups
                ]
                cov = "temp_tx" if groups else "no_temp"
                frepo.upsert_taf(
                    station_id=st.id,
                    icao=icao,
                    issue_time=issue_time,
                    valid_from=epoch_to_utc(payload.get("validTimeFrom")),
                    valid_to=epoch_to_utc(payload.get("validTimeTo")),
                    raw_taf=raw,
                    parsed_tx=parsed,
                    coverage=cov,
                )
                stats["taf_updated"] += 1

            station_dates: dict[str, set[date]] = {}
            market_windows: dict[tuple[str, date], tuple[datetime, datetime]] = {}
            for ev in events:
                station_dates.setdefault(ev.station_id, set()).add(ev.forecast_date)
                st = station_by_id.get(ev.station_id)
                market_windows[(ev.station_id, ev.forecast_date)] = market_day_window_utc(
                    ev.forecast_date, st.timezone if st else "UTC"
                )

            metar_running: dict[tuple[str, date], dict[str, Any]] = {}
            for st in stations:
                for fd in sorted(station_dates.get(st.id, set())):
                    window_start_utc, window_end_utc = market_day_window_utc(fd, st.timezone)
                    obs_rows = frepo.metar_for_window(
                        st.id,
                        window_start_utc=window_start_utc,
                        window_end_utc=window_end_utc,
                    )
                    if not obs_rows:
                        continue
                    tmax_c = max(
                        (o.temp_c for o in obs_rows if o.temp_c is not None),
                        default=None,
                    )
                    tmax_val = None
                    if tmax_c is not None:
                        from poly_bot.weather.aviation_parse import celsius_to_station_unit

                        tmax_val = celsius_to_station_unit(int(round(tmax_c)), st.unit)
                    metar_running[(st.id, fd)] = {
                        "tmax_c": int(round(tmax_c)) if tmax_c is not None else None,
                        "tmax_value": tmax_val,
                        "coverage": "full" if tmax_c is not None else "partial",
                        "obs_count": len(obs_rows),
                        "latest_obs": obs_rows[-1].obs_time,
                        "window_start_utc": window_start_utc.isoformat(),
                        "window_end_utc": window_end_utc.isoformat(),
                    }

            context: dict[str, Any] = {
                "taf_by_icao": taf_by_icao,
                "metar_running": metar_running,
                "reference_date": ref.isoformat(),
                "open_meteo_base_url": self._settings.weather_open_meteo_api_base,
                "open_meteo_timeout": self._settings.weather_open_meteo_timeout_seconds,
                "open_meteo_models": self._settings.weather_open_meteo_models,
                "market_windows": market_windows,
            }

            from poly_bot.weather.forecast_schema import WeatherStationForecastConfig

            for st in stations:
                cfg = session.get(WeatherStationForecastConfig, st.id)
                if cfg is None:
                    continue
                dates = forecast_dates_for_station(
                    st,
                    ref_date=ref,
                    horizon_days=self._settings.weather_forecast_horizon_days,
                )
                dates = [d for d in dates if ref <= d <= max_date]
                produce_from_providers(frepo, st, cfg, dates, context)
                stats["forecasts_written"] += len(dates)

            stats["signals"] = refresh_trading_signals(
                session,
                min_date=ref,
                max_date=max_date,
            )

            prune_before = datetime.now(UTC) - timedelta(
                days=self._settings.weather_metar_retention_days
            )
            frepo.prune_old_metar(before=prune_before)
            session.commit()

        logger.info("weather_aviation_sync_done", **stats)
        return stats
