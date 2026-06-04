"""SQLModel tables for structured daily temperature markets."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import Column, Index, Text, UniqueConstraint
from sqlalchemy.types import JSON
from sqlmodel import Field, SQLModel


class WeatherStation(SQLModel, table=True):
    """Resolution weather station (Wunderground / airport)."""

    __tablename__ = "weather_stations"
    __table_args__ = (UniqueConstraint("wunderground_path", name="uq_weather_stations_wu_path"),)

    id: str = Field(primary_key=True, description="Stable id, usually ICAO or WU path slug")
    wunderground_path: str = Field(index=True, description="Path after /history/daily/")
    wunderground_url: str = Field(default="")
    station_name: str = Field(default="", description="Human name from market description")
    icao: str | None = Field(default=None, index=True)
    city_display: str = Field(default="")
    region: str = Field(default="")
    country_code: str = Field(default="", index=True)
    latitude: float | None = None
    longitude: float | None = None
    timezone: str = Field(default="UTC", description="IANA timezone for local forecast day")
    unit: str = Field(default="C", description="C or F — resolution unit")
    precision: str = Field(default="whole", description="whole | half — from market rules")
    raw_resolution_source: str = Field(default="")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WeatherCity(SQLModel, table=True):
    """City identity across recurring daily temperature series."""

    __tablename__ = "weather_cities"

    city_key: str = Field(primary_key=True, description="Slug segment, e.g. london, buenos-aires")
    display_name: str = Field(default="")
    series_slug: str | None = Field(default=None, index=True)
    default_station_id: str | None = Field(default=None, foreign_key="weather_stations.id")
    market_count: int = Field(default=0, description="Bins seen in last sync (latest day)")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WeatherDailyEvent(SQLModel, table=True):
    """One Polymarket event = one city × forecast day (all temperature bins)."""

    __tablename__ = "weather_daily_events"
    __table_args__ = (
        Index("ix_weather_daily_events_forecast_date", "forecast_date"),
        Index("ix_weather_daily_events_city_date", "city_key", "forecast_date"),
        Index("ix_weather_daily_events_days_ahead", "days_ahead"),
    )

    id: str = Field(primary_key=True, description="Gamma event id")
    event_slug: str = Field(index=True)
    city_key: str = Field(foreign_key="weather_cities.city_key", index=True)
    station_id: str = Field(foreign_key="weather_stations.id", index=True)
    forecast_date: date = Field(index=True)
    days_ahead: int = Field(
        default=0,
        description="Days from sync-day UTC to forecast_date",
    )
    title: str = Field(default="")
    resolution_rules_text: str = Field(default="", sa_column=Column(Text))
    resolution_source_url: str = Field(default="")
    neg_risk_market_id: str | None = Field(default=None, index=True)
    series_id: str | None = Field(default=None, index=True)
    end_ts: datetime | None = None
    active: bool = Field(default=True, index=True)
    closed: bool = Field(default=False, index=True)
    bin_count: int = Field(default=0)
    market_count: int = Field(default=0)
    first_seen_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_synced_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WeatherBin(SQLModel, table=True):
    """One binary market = one temperature outcome band for a daily event."""

    __tablename__ = "weather_bins"
    __table_args__ = (
        Index("ix_weather_bins_event_index", "weather_event_id", "bin_index"),
    )

    condition_id: str = Field(primary_key=True, foreign_key="markets.condition_id")
    weather_event_id: str = Field(foreign_key="weather_daily_events.id", index=True)
    slug: str = Field(index=True)
    question: str = Field(default="", sa_column=Column(Text))
    bin_label: str = Field(default="")
    bin_index: int = Field(default=0)
    bin_kind: str = Field(
        default="unknown",
        description="exact | at_or_below | at_or_above | unknown",
    )
    temp_c: int | None = Field(default=None, description="Exact Celsius bin when kind=exact")
    temp_c_upper: int | None = Field(
        default=None,
        description="Inclusive upper bound (at_or_below)",
    )
    temp_c_lower: int | None = Field(
        default=None,
        description="Inclusive lower bound (at_or_above)",
    )
    active: bool = Field(default=True)
    closed: bool = Field(default=False)
    yes_token_id: str | None = None
    no_token_id: str | None = None
    group_item_title: str = Field(default="")
    raw_payload: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    last_synced_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WeatherStationWuSnapshot(SQLModel, table=True):
    """Latest Wunderground station reading for a market local day (resolution link)."""

    __tablename__ = "weather_station_wu_snapshots"
    __table_args__ = (
        Index("ix_weather_wu_snap_forecast_date", "forecast_date"),
    )

    station_id: str = Field(foreign_key="weather_stations.id", primary_key=True)
    forecast_date: date = Field(primary_key=True)
    current_temp_c: float | None = None
    tmax_c: int | None = None
    tmin_c: int | None = None
    tmax_hours_local: str = Field(
        default="",
        description="Comma-separated local HH:MM when hourly peak occurs",
    )
    status: str = Field(default="pending", description="ok | error | partial")
    error_message: str = Field(default="", sa_column=Column(Text))
    source: str = Field(default="", description="wu_dailysummary | wu_forecast_daily | …")
    fetched_at: datetime | None = Field(default=None)
    meta: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
