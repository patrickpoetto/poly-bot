"""Weather catalog: stations, cities, daily events, bins."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "004_weather_catalog"
down_revision = "003_game_start_ts"
branch_labels = None
depends_on = None


def _table_exists(bind, name: str) -> bool:
    return name in inspect(bind).get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, "weather_stations"):
        return

    op.create_table(
        "weather_stations",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("wunderground_path", sa.String(), nullable=False),
        sa.Column("wunderground_url", sa.String(), nullable=False),
        sa.Column("station_name", sa.String(), nullable=False),
        sa.Column("icao", sa.String(), nullable=True),
        sa.Column("city_display", sa.String(), nullable=False),
        sa.Column("region", sa.String(), nullable=False),
        sa.Column("country_code", sa.String(), nullable=False),
        sa.Column("latitude", sa.Float(), nullable=True),
        sa.Column("longitude", sa.Float(), nullable=True),
        sa.Column("timezone", sa.String(), nullable=False),
        sa.Column("unit", sa.String(), nullable=False),
        sa.Column("precision", sa.String(), nullable=False),
        sa.Column("raw_resolution_source", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("wunderground_path", name="uq_weather_stations_wu_path"),
    )
    op.create_index("ix_weather_stations_wunderground_path", "weather_stations", ["wunderground_path"])
    op.create_index("ix_weather_stations_icao", "weather_stations", ["icao"])
    op.create_index("ix_weather_stations_country_code", "weather_stations", ["country_code"])

    op.create_table(
        "weather_cities",
        sa.Column("city_key", sa.String(), nullable=False),
        sa.Column("display_name", sa.String(), nullable=False),
        sa.Column("series_slug", sa.String(), nullable=True),
        sa.Column("default_station_id", sa.String(), nullable=True),
        sa.Column("market_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["default_station_id"], ["weather_stations.id"]),
        sa.PrimaryKeyConstraint("city_key"),
    )
    op.create_index("ix_weather_cities_series_slug", "weather_cities", ["series_slug"])

    op.create_table(
        "weather_daily_events",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("event_slug", sa.String(), nullable=False),
        sa.Column("city_key", sa.String(), nullable=False),
        sa.Column("station_id", sa.String(), nullable=False),
        sa.Column("forecast_date", sa.Date(), nullable=False),
        sa.Column("days_ahead", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("resolution_rules_text", sa.Text(), nullable=False),
        sa.Column("resolution_source_url", sa.String(), nullable=False),
        sa.Column("neg_risk_market_id", sa.String(), nullable=True),
        sa.Column("series_id", sa.String(), nullable=True),
        sa.Column("end_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("closed", sa.Boolean(), nullable=False),
        sa.Column("bin_count", sa.Integer(), nullable=False),
        sa.Column("market_count", sa.Integer(), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["city_key"], ["weather_cities.city_key"]),
        sa.ForeignKeyConstraint(["station_id"], ["weather_stations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_weather_daily_events_event_slug", "weather_daily_events", ["event_slug"])
    op.create_index("ix_weather_daily_events_forecast_date", "weather_daily_events", ["forecast_date"])
    op.create_index("ix_weather_daily_events_city_date", "weather_daily_events", ["city_key", "forecast_date"])
    op.create_index("ix_weather_daily_events_days_ahead", "weather_daily_events", ["days_ahead"])
    op.create_index("ix_weather_daily_events_city_key", "weather_daily_events", ["city_key"])
    op.create_index("ix_weather_daily_events_station_id", "weather_daily_events", ["station_id"])
    op.create_index("ix_weather_daily_events_active", "weather_daily_events", ["active"])

    op.create_table(
        "weather_bins",
        sa.Column("condition_id", sa.String(), nullable=False),
        sa.Column("weather_event_id", sa.String(), nullable=False),
        sa.Column("slug", sa.String(), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("bin_label", sa.String(), nullable=False),
        sa.Column("bin_index", sa.Integer(), nullable=False),
        sa.Column("bin_kind", sa.String(), nullable=False),
        sa.Column("temp_c", sa.Integer(), nullable=True),
        sa.Column("temp_c_upper", sa.Integer(), nullable=True),
        sa.Column("temp_c_lower", sa.Integer(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("closed", sa.Boolean(), nullable=False),
        sa.Column("yes_token_id", sa.String(), nullable=True),
        sa.Column("no_token_id", sa.String(), nullable=True),
        sa.Column("group_item_title", sa.String(), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["condition_id"], ["markets.condition_id"]),
        sa.ForeignKeyConstraint(
            ["weather_event_id"],
            ["weather_daily_events.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("condition_id"),
    )
    op.create_index("ix_weather_bins_weather_event_id", "weather_bins", ["weather_event_id"])
    op.create_index("ix_weather_bins_event_index", "weather_bins", ["weather_event_id", "bin_index"])
    op.create_index("ix_weather_bins_slug", "weather_bins", ["slug"])


def downgrade() -> None:
    op.drop_table("weather_bins")
    op.drop_table("weather_daily_events")
    op.drop_table("weather_cities")
    op.drop_table("weather_stations")
