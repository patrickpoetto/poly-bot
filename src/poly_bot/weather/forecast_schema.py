"""Forecast sources, aviation ingest, daily targets, trading shadow signals."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import Column, Index, Text, UniqueConstraint
from sqlalchemy.types import JSON
from sqlmodel import Field, SQLModel


class WeatherForecastSource(SQLModel, table=True):
    """Registry of forecast providers (shared or per-station capable)."""

    __tablename__ = "weather_forecast_sources"

    source_key: str = Field(primary_key=True, description="e.g. aviation_taf, open_meteo_ecmwf")
    display_name: str = Field(default="")
    provider_kind: str = Field(
        default="common",
        description="common | station_override — how configs attach",
    )
    horizon_hours: int = Field(default=30, description="Typical useful forecast window")
    enabled: bool = Field(default=True)
    config_schema: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WeatherStationForecastConfig(SQLModel, table=True):
    """Per-station source stack and trading gates."""

    __tablename__ = "weather_station_forecast_config"

    station_id: str = Field(primary_key=True, foreign_key="weather_stations.id")
    primary_source_key: str = Field(
        default="aviation_taf",
        foreign_key="weather_forecast_sources.source_key",
    )
    fallback_source_keys: list[str] = Field(
        default_factory=lambda: ["aviation_metar_running"],
        sa_column=Column(JSON),
    )
    extra_sources: dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSON),
        description="Per-station overrides: {source_key: {lat, lon, model, ...}}",
    )
    trading_enabled: bool = Field(default=False, index=True)
    trading_mode: str = Field(
        default="shadow",
        description="shadow | paper | live — execution respects this later",
    )
    notes: str = Field(default="")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WeatherMetarObservation(SQLModel, table=True):
    """METAR observation snapshot (append-only by icao + obs_time)."""

    __tablename__ = "weather_metar_observations"
    __table_args__ = (
        UniqueConstraint("icao", "obs_time", name="uq_weather_metar_icao_obs"),
        Index("ix_weather_metar_station_obs", "station_id", "obs_time"),
    )

    id: int | None = Field(default=None, primary_key=True)
    station_id: str = Field(foreign_key="weather_stations.id", index=True)
    icao: str = Field(index=True)
    obs_time: datetime = Field(index=True)
    temp_c: float | None = None
    temp_f: float | None = None
    raw_ob: str = Field(default="")
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WeatherTafReport(SQLModel, table=True):
    """Latest TAF bulletin per station (replaced on each sync)."""

    __tablename__ = "weather_taf_reports"

    station_id: str = Field(primary_key=True, foreign_key="weather_stations.id")
    icao: str = Field(index=True)
    issue_time: datetime | None = Field(default=None, index=True)
    valid_time_from: datetime | None = None
    valid_time_to: datetime | None = None
    raw_taf: str = Field(default="", sa_column=Column(Text))
    parsed_tx: list[dict[str, Any]] = Field(default_factory=list, sa_column=Column(JSON))
    coverage: str = Field(
        default="unknown",
        description="temp_tx | no_temp | missing",
    )
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WeatherDailyForecast(SQLModel, table=True):
    """Unified daily high forecast for trading (one row per station × day × source)."""

    __tablename__ = "weather_daily_forecasts"
    __table_args__ = (
        UniqueConstraint(
            "station_id",
            "forecast_date",
            "source_key",
            name="uq_weather_daily_forecast_station_date_source",
        ),
        Index("ix_weather_daily_forecast_date", "forecast_date"),
    )

    id: int | None = Field(default=None, primary_key=True)
    station_id: str = Field(foreign_key="weather_stations.id", index=True)
    forecast_date: date = Field(index=True)
    source_key: str = Field(foreign_key="weather_forecast_sources.source_key", index=True)
    role: str = Field(default="primary", description="primary | fallback | merged")
    tmax_value: float | None = None
    unit: str = Field(default="C")
    tmax_c: int | None = Field(default=None, description="Whole °C for bin mapping")
    coverage: str = Field(default="none", description="full | partial | none")
    issued_at: datetime | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    meta: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WeatherTradingSignal(SQLModel, table=True):
    """Shadow trading snapshot: model favorite bin vs market (no orders)."""

    __tablename__ = "weather_trading_signals"
    __table_args__ = (
        UniqueConstraint("weather_event_id", name="uq_weather_trading_signal_event"),
        Index("ix_weather_trading_signals_updated", "updated_at"),
    )

    weather_event_id: str = Field(primary_key=True, foreign_key="weather_daily_events.id")
    station_id: str = Field(foreign_key="weather_stations.id", index=True)
    forecast_date: date = Field(index=True)
    trading_mode: str = Field(default="shadow")
    model_tmax_c: int | None = None
    model_bin_label: str = Field(default="")
    market_favorite_c: int | None = None
    market_favorite_mid: float | None = None
    edge_hint: str = Field(
        default="",
        description="aligned | model_higher | model_lower | no_forecast | no_quotes",
    )
    primary_source_key: str = Field(default="")
    meta: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
