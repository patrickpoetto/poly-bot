"""Wunderground station snapshots for resolution-station forecasts."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "006_weather_wu_snapshots"
down_revision = "005_weather_forecasts"
branch_labels = None
depends_on = None


def _table_exists(bind, name: str) -> bool:
    return name in inspect(bind).get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    if _table_exists(bind, "weather_station_wu_snapshots"):
        return

    op.create_table(
        "weather_station_wu_snapshots",
        sa.Column("station_id", sa.String(), nullable=False),
        sa.Column("forecast_date", sa.Date(), nullable=False),
        sa.Column("current_temp_c", sa.Float(), nullable=True),
        sa.Column("tmax_c", sa.Integer(), nullable=True),
        sa.Column("tmin_c", sa.Integer(), nullable=True),
        sa.Column("tmax_hours_local", sa.String(), nullable=False, server_default=""),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("error_message", sa.Text(), nullable=False, server_default=""),
        sa.Column("source", sa.String(), nullable=False, server_default=""),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=False, server_default="{}"),
        sa.ForeignKeyConstraint(["station_id"], ["weather_stations.id"]),
        sa.PrimaryKeyConstraint("station_id", "forecast_date"),
    )
    op.create_index(
        "ix_weather_wu_snap_forecast_date",
        "weather_station_wu_snapshots",
        ["forecast_date"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    if not _table_exists(bind, "weather_station_wu_snapshots"):
        return
    op.drop_index("ix_weather_wu_snap_forecast_date", table_name="weather_station_wu_snapshots")
    op.drop_table("weather_station_wu_snapshots")
