"""Weather forecast sources, aviation TAF/METAR, daily forecasts, trading signals."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "005_weather_forecasts"
down_revision = "004_weather_catalog"
branch_labels = None
depends_on = None


def _table_exists(bind, name: str) -> bool:
    return name in inspect(bind).get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, "weather_forecast_sources"):
        return

    op.create_table(
        "weather_forecast_sources",
        sa.Column("source_key", sa.String(), nullable=False),
        sa.Column("display_name", sa.String(), nullable=False),
        sa.Column("provider_kind", sa.String(), nullable=False),
        sa.Column("horizon_hours", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("config_schema", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("source_key"),
    )
    op.create_table(
        "weather_station_forecast_config",
        sa.Column("station_id", sa.String(), nullable=False),
        sa.Column("primary_source_key", sa.String(), nullable=False),
        sa.Column("fallback_source_keys", sa.JSON(), nullable=False),
        sa.Column("extra_sources", sa.JSON(), nullable=False),
        sa.Column("trading_enabled", sa.Boolean(), nullable=False),
        sa.Column("trading_mode", sa.String(), nullable=False),
        sa.Column("notes", sa.String(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["station_id"], ["weather_stations.id"]),
        sa.ForeignKeyConstraint(
            ["primary_source_key"],
            ["weather_forecast_sources.source_key"],
        ),
        sa.PrimaryKeyConstraint("station_id"),
    )
    op.create_index(
        "ix_weather_station_forecast_config_trading",
        "weather_station_forecast_config",
        ["trading_enabled"],
    )
    op.create_table(
        "weather_metar_observations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("station_id", sa.String(), nullable=False),
        sa.Column("icao", sa.String(), nullable=False),
        sa.Column("obs_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("temp_c", sa.Float(), nullable=True),
        sa.Column("temp_f", sa.Float(), nullable=True),
        sa.Column("raw_ob", sa.String(), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["station_id"], ["weather_stations.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("icao", "obs_time", name="uq_weather_metar_icao_obs"),
    )
    op.create_index("ix_weather_metar_observations_icao", "weather_metar_observations", ["icao"])
    op.create_index(
        "ix_weather_metar_station_obs",
        "weather_metar_observations",
        ["station_id", "obs_time"],
    )
    op.create_table(
        "weather_taf_reports",
        sa.Column("station_id", sa.String(), nullable=False),
        sa.Column("icao", sa.String(), nullable=False),
        sa.Column("issue_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_time_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_time_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("raw_taf", sa.Text(), nullable=False),
        sa.Column("parsed_tx", sa.JSON(), nullable=False),
        sa.Column("coverage", sa.String(), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["station_id"], ["weather_stations.id"]),
        sa.PrimaryKeyConstraint("station_id"),
    )
    op.create_index("ix_weather_taf_reports_icao", "weather_taf_reports", ["icao"])
    op.create_table(
        "weather_daily_forecasts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("station_id", sa.String(), nullable=False),
        sa.Column("forecast_date", sa.Date(), nullable=False),
        sa.Column("source_key", sa.String(), nullable=False),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("tmax_value", sa.Float(), nullable=True),
        sa.Column("unit", sa.String(), nullable=False),
        sa.Column("tmax_c", sa.Integer(), nullable=True),
        sa.Column("coverage", sa.String(), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["station_id"], ["weather_stations.id"]),
        sa.ForeignKeyConstraint(["source_key"], ["weather_forecast_sources.source_key"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "station_id",
            "forecast_date",
            "source_key",
            name="uq_weather_daily_forecast_station_date_source",
        ),
    )
    op.create_index(
        "ix_weather_daily_forecast_date",
        "weather_daily_forecasts",
        ["forecast_date"],
    )
    op.create_table(
        "weather_trading_signals",
        sa.Column("weather_event_id", sa.String(), nullable=False),
        sa.Column("station_id", sa.String(), nullable=False),
        sa.Column("forecast_date", sa.Date(), nullable=False),
        sa.Column("trading_mode", sa.String(), nullable=False),
        sa.Column("model_tmax_c", sa.Integer(), nullable=True),
        sa.Column("model_bin_label", sa.String(), nullable=False),
        sa.Column("market_favorite_c", sa.Integer(), nullable=True),
        sa.Column("market_favorite_mid", sa.Float(), nullable=True),
        sa.Column("edge_hint", sa.String(), nullable=False),
        sa.Column("primary_source_key", sa.String(), nullable=False),
        sa.Column("meta", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["weather_event_id"], ["weather_daily_events.id"]),
        sa.ForeignKeyConstraint(["station_id"], ["weather_stations.id"]),
        sa.PrimaryKeyConstraint("weather_event_id"),
    )
    op.create_index(
        "ix_weather_trading_signals_updated",
        "weather_trading_signals",
        ["updated_at"],
    )


def downgrade() -> None:
    for name in (
        "weather_trading_signals",
        "weather_daily_forecasts",
        "weather_taf_reports",
        "weather_metar_observations",
        "weather_station_forecast_config",
        "weather_forecast_sources",
    ):
        if _table_exists(op.get_bind(), name):
            op.drop_table(name)
