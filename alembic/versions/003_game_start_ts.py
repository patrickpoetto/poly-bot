"""Add game_start_ts for sports kickoff / in-play window."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "003_game_start_ts"
down_revision = "002_taxonomy_overrides"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("markets", sa.Column("game_start_ts", sa.DateTime(timezone=True), nullable=True))
    op.add_column("events", sa.Column("game_start_ts", sa.DateTime(timezone=True), nullable=True))
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
    op.drop_index("ix_markets_game_start_ts", table_name="markets")
    op.drop_column("events", "game_start_ts")
    op.drop_column("markets", "game_start_ts")
