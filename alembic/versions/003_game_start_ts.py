"""Add game_start_ts for sports kickoff / in-play window."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "003_game_start_ts"
down_revision = "002"
branch_labels = None
depends_on = None


def _column_names(bind, table: str) -> set[str]:
    return {c["name"] for c in inspect(bind).get_columns(table)}


def upgrade() -> None:
    bind = op.get_bind()
    market_cols = _column_names(bind, "markets")
    if "game_start_ts" not in market_cols:
        op.add_column("markets", sa.Column("game_start_ts", sa.DateTime(timezone=True), nullable=True))
    event_cols = _column_names(bind, "events")
    if "game_start_ts" not in event_cols:
        op.add_column("events", sa.Column("game_start_ts", sa.DateTime(timezone=True), nullable=True))
    indexes = {i["name"] for i in inspect(bind).get_indexes("markets")}
    if "ix_markets_game_start_ts" not in indexes:
        op.create_index("ix_markets_game_start_ts", "markets", ["game_start_ts"])
    # Sports: Gamma endDate is usually kickoff — seed until discovery re-ingests gameStartTime.
    op.execute(
        """
        UPDATE markets
        SET game_start_ts = end_ts
        WHERE domain = 'sports' AND game_start_ts IS NULL AND end_ts IS NOT NULL
        """
    )
    op.execute(
        """
        UPDATE events
        SET game_start_ts = end_ts
        WHERE game_start_ts IS NULL AND end_ts IS NOT NULL
          AND id IN (SELECT DISTINCT event_id FROM markets WHERE domain = 'sports' AND event_id IS NOT NULL)
        """
    )


def downgrade() -> None:
    bind = op.get_bind()
    indexes = {i["name"] for i in inspect(bind).get_indexes("markets")}
    if "ix_markets_game_start_ts" in indexes:
        op.drop_index("ix_markets_game_start_ts", table_name="markets")
    if "game_start_ts" in _column_names(bind, "events"):
        op.drop_column("events", "game_start_ts")
    if "game_start_ts" in _column_names(bind, "markets"):
        op.drop_column("markets", "game_start_ts")
