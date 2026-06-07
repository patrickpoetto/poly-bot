"""Persistence for weather catalog tables."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlmodel import Session, col, delete, select

from poly_bot.weather.forecast_schema import WeatherTradingSignal
from poly_bot.weather.schema import WeatherBin, WeatherCity, WeatherDailyEvent, WeatherStation


class WeatherRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_station(self, station: WeatherStation) -> None:
        now = datetime.now(UTC)
        existing = self._session.get(WeatherStation, station.id)
        if existing:
            for field in (
                "wunderground_path",
                "wunderground_url",
                "station_name",
                "icao",
                "city_display",
                "region",
                "country_code",
                "latitude",
                "longitude",
                "timezone",
                "unit",
                "precision",
                "raw_resolution_source",
            ):
                val = getattr(station, field)
                if val is not None and val != "" and val != existing.__dict__.get(field):
                    setattr(existing, field, val)
            if station.latitude is not None and existing.latitude is None:
                existing.latitude = station.latitude
            if station.longitude is not None and existing.longitude is None:
                existing.longitude = station.longitude
            if station.timezone and existing.timezone in ("", "UTC") and station.timezone != "UTC":
                existing.timezone = station.timezone
            existing.updated_at = now
            self._session.add(existing)
        else:
            station.created_at = now
            station.updated_at = now
            self._session.add(station)

    def upsert_city(self, city: WeatherCity) -> None:
        now = datetime.now(UTC)
        existing = self._session.get(WeatherCity, city.city_key)
        if existing:
            if city.display_name:
                existing.display_name = city.display_name
            if city.series_slug:
                existing.series_slug = city.series_slug
            if city.default_station_id:
                existing.default_station_id = city.default_station_id
            existing.market_count = city.market_count
            existing.updated_at = now
            self._session.add(existing)
        else:
            city.created_at = now
            city.updated_at = now
            self._session.add(city)

    def upsert_daily_event(self, event: WeatherDailyEvent) -> None:
        now = datetime.now(UTC)
        existing = self._session.get(WeatherDailyEvent, event.id)
        if existing:
            for field in (
                "event_slug",
                "city_key",
                "station_id",
                "forecast_date",
                "days_ahead",
                "title",
                "resolution_rules_text",
                "resolution_source_url",
                "neg_risk_market_id",
                "series_id",
                "end_ts",
                "active",
                "closed",
                "bin_count",
                "market_count",
            ):
                setattr(existing, field, getattr(event, field))
            existing.last_synced_at = now
            self._session.add(existing)
        else:
            event.first_seen_at = now
            event.last_synced_at = now
            self._session.add(event)

    def upsert_bin(self, bin_row: WeatherBin) -> None:
        now = datetime.now(UTC)
        existing = self._session.get(WeatherBin, bin_row.condition_id)
        if existing:
            for field in (
                "weather_event_id",
                "slug",
                "question",
                "bin_label",
                "bin_index",
                "bin_kind",
                "temp_c",
                "temp_c_upper",
                "temp_c_lower",
                "active",
                "closed",
                "yes_token_id",
                "no_token_id",
                "group_item_title",
                "raw_payload",
            ):
                setattr(existing, field, getattr(bin_row, field))
            existing.last_synced_at = now
            self._session.add(existing)
        else:
            bin_row.last_synced_at = now
            self._session.add(bin_row)

    def delete_events_outside_horizon(self, *, min_date, max_date) -> int:
        stale_event_ids = select(WeatherDailyEvent.id).where(
            (col(WeatherDailyEvent.forecast_date) < min_date)
            | (col(WeatherDailyEvent.forecast_date) > max_date)
        )
        self._session.exec(
            delete(WeatherTradingSignal).where(
                col(WeatherTradingSignal.weather_event_id).in_(stale_event_ids)
            )
        )
        self._session.exec(
            delete(WeatherBin).where(col(WeatherBin.weather_event_id).in_(stale_event_ids))
        )
        stmt = delete(WeatherDailyEvent).where(col(WeatherDailyEvent.id).in_(stale_event_ids))
        result = self._session.exec(stmt)
        return result.rowcount or 0

    def list_daily_events(
        self,
        *,
        min_date,
        max_date,
        city_key: str | None = None,
    ) -> list[WeatherDailyEvent]:
        stmt = (
            select(WeatherDailyEvent)
            .where(WeatherDailyEvent.forecast_date >= min_date)
            .where(WeatherDailyEvent.forecast_date <= max_date)
            .order_by(WeatherDailyEvent.forecast_date, WeatherDailyEvent.city_key)
        )
        if city_key:
            stmt = stmt.where(WeatherDailyEvent.city_key == city_key)
        return list(self._session.exec(stmt).all())

    def bins_for_event(self, weather_event_id: str) -> list[WeatherBin]:
        stmt = (
            select(WeatherBin)
            .where(WeatherBin.weather_event_id == weather_event_id)
            .order_by(WeatherBin.bin_index)
        )
        return list(self._session.exec(stmt).all())

    def get_station(self, station_id: str) -> WeatherStation | None:
        return self._session.get(WeatherStation, station_id)
