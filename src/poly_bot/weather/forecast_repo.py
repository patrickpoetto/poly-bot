"""Persistence for forecast sources, aviation ingest, trading signals."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlmodel import Session, col, delete, select

from poly_bot.weather.forecast_schema import (
    WeatherDailyForecast,
    WeatherForecastSource,
    WeatherMetarObservation,
    WeatherStationForecastConfig,
    WeatherTafReport,
    WeatherTradingSignal,
)
from poly_bot.weather.forecast_sources import BUILTIN_SOURCES
from poly_bot.weather.providers.base import DailyForecastPoint
from poly_bot.weather.schema import WeatherStation


class WeatherForecastRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def ensure_builtin_sources(self) -> int:
        n = 0
        for src in BUILTIN_SOURCES:
            if self._session.get(WeatherForecastSource, src.source_key) is None:
                self._session.add(src)
                n += 1
        return n

    def ensure_station_config(self, station: WeatherStation, *, trading_enabled: bool) -> None:
        existing = self._session.get(WeatherStationForecastConfig, station.id)
        if existing:
            changed = False
            if existing.primary_source_key in ("aviation_taf", "", None):  # type: ignore[comparison-overlap]
                existing.primary_source_key = "open_meteo_blend"
                changed = True
            fallback = list(existing.fallback_source_keys or [])
            wanted = ["aviation_taf", "aviation_metar_running"]
            for key in wanted:
                if key not in fallback:
                    fallback.append(key)
                    changed = True
            if changed:
                existing.fallback_source_keys = fallback
            if trading_enabled and not existing.trading_enabled:
                existing.trading_enabled = True
                changed = True
            if changed:
                existing.updated_at = datetime.now(UTC)
            self._session.add(existing)
            return
        self._session.add(
            WeatherStationForecastConfig(
                station_id=station.id,
                primary_source_key="open_meteo_blend",
                fallback_source_keys=["aviation_taf", "aviation_metar_running"],
                trading_enabled=trading_enabled,
                trading_mode="shadow",
            )
        )

    def list_stations_with_icao(self) -> list[WeatherStation]:
        stmt = select(WeatherStation).where(col(WeatherStation.icao).is_not(None))
        return [s for s in self._session.exec(stmt).all() if s.icao]

    def upsert_metar(
        self,
        *,
        station_id: str,
        icao: str,
        obs_time: datetime,
        temp_c: float | None,
        temp_f: float | None,
        raw_ob: str,
    ) -> bool:
        stmt = select(WeatherMetarObservation).where(
            WeatherMetarObservation.icao == icao,
            WeatherMetarObservation.obs_time == obs_time,
        )
        existing = self._session.exec(stmt).first()
        if existing:
            if temp_c is not None:
                existing.temp_c = temp_c
            if temp_f is not None:
                existing.temp_f = temp_f
            if raw_ob:
                existing.raw_ob = raw_ob
            existing.ingested_at = datetime.now(UTC)
            self._session.add(existing)
            return True
        self._session.add(
            WeatherMetarObservation(
                station_id=station_id,
                icao=icao,
                obs_time=obs_time,
                temp_c=temp_c,
                temp_f=temp_f,
                raw_ob=raw_ob,
            )
        )
        return True

    def upsert_taf(
        self,
        *,
        station_id: str,
        icao: str,
        issue_time: datetime | None,
        valid_from: datetime | None,
        valid_to: datetime | None,
        raw_taf: str,
        parsed_tx: list[dict],
        coverage: str,
    ) -> None:
        row = self._session.get(WeatherTafReport, station_id)
        if row is None:
            row = WeatherTafReport(station_id=station_id, icao=icao)
        row.icao = icao
        row.issue_time = issue_time
        row.valid_time_from = valid_from
        row.valid_time_to = valid_to
        row.raw_taf = raw_taf
        row.parsed_tx = parsed_tx
        row.coverage = coverage
        row.ingested_at = datetime.now(UTC)
        self._session.add(row)

    def metar_for_local_date(
        self,
        station_id: str,
        local_date: date,
        timezone: str,
    ) -> list[WeatherMetarObservation]:
        from zoneinfo import ZoneInfo

        try:
            tz = ZoneInfo(timezone)
        except Exception:
            from datetime import UTC as _UTC

            tz = _UTC
        start = datetime.combine(local_date, datetime.min.time(), tzinfo=tz)
        end = datetime.combine(local_date, datetime.max.time(), tzinfo=tz)
        start_utc = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        stmt = (
            select(WeatherMetarObservation)
            .where(WeatherMetarObservation.station_id == station_id)
            .where(WeatherMetarObservation.obs_time >= start_utc)
            .where(WeatherMetarObservation.obs_time <= end_utc)
            .order_by(WeatherMetarObservation.obs_time)
        )
        return list(self._session.exec(stmt).all())

    def metar_for_window(
        self,
        station_id: str,
        *,
        window_start_utc: datetime,
        window_end_utc: datetime,
    ) -> list[WeatherMetarObservation]:
        stmt = (
            select(WeatherMetarObservation)
            .where(WeatherMetarObservation.station_id == station_id)
            .where(WeatherMetarObservation.obs_time >= window_start_utc)
            .where(WeatherMetarObservation.obs_time < window_end_utc)
            .order_by(WeatherMetarObservation.obs_time)
        )
        return list(self._session.exec(stmt).all())

    def upsert_daily_forecast(self, point: DailyForecastPoint, *, role: str) -> None:
        stmt = select(WeatherDailyForecast).where(
            WeatherDailyForecast.station_id == point.station_id,
            WeatherDailyForecast.forecast_date == point.forecast_date,
            WeatherDailyForecast.source_key == point.source_key,
        )
        row = self._session.exec(stmt).first()
        now = datetime.now(UTC)
        if row is None:
            row = WeatherDailyForecast(
                station_id=point.station_id,
                forecast_date=point.forecast_date,
                source_key=point.source_key,
                role=role,
            )
        row.role = role
        row.tmax_value = point.tmax_value
        row.unit = point.unit
        row.tmax_c = point.tmax_c
        row.coverage = point.coverage
        row.issued_at = point.issued_at
        row.valid_from = point.valid_from
        row.valid_to = point.valid_to
        row.meta = point.meta
        row.updated_at = now
        self._session.add(row)

    def get_merged_forecast(
        self,
        station_id: str,
        forecast_date: date,
    ) -> WeatherDailyForecast | None:
        cfg = self._session.get(WeatherStationForecastConfig, station_id)
        if cfg is None:
            return None
        for key in [cfg.primary_source_key, *cfg.fallback_source_keys]:
            stmt = select(WeatherDailyForecast).where(
                WeatherDailyForecast.station_id == station_id,
                WeatherDailyForecast.forecast_date == forecast_date,
                WeatherDailyForecast.source_key == key,
                WeatherDailyForecast.tmax_c.is_not(None),
            )
            row = self._session.exec(stmt).first()
            if row is not None:
                return row
        return None

    def upsert_trading_signal(self, signal: WeatherTradingSignal) -> None:
        signal.updated_at = datetime.now(UTC)
        existing = self._session.get(WeatherTradingSignal, signal.weather_event_id)
        if existing:
            for field in (
                "station_id",
                "forecast_date",
                "trading_mode",
                "model_tmax_c",
                "model_bin_label",
                "market_favorite_c",
                "market_favorite_mid",
                "edge_hint",
                "primary_source_key",
                "meta",
            ):
                setattr(existing, field, getattr(signal, field))
            existing.updated_at = signal.updated_at
            self._session.add(existing)
        else:
            self._session.add(signal)

    def prune_old_metar(self, *, before: datetime) -> int:
        result = self._session.exec(
            delete(WeatherMetarObservation).where(WeatherMetarObservation.obs_time < before)
        )
        return result.rowcount or 0

    def get_taf(self, station_id: str) -> WeatherTafReport | None:
        return self._session.get(WeatherTafReport, station_id)
