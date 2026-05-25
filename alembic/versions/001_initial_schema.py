"""Initial catalog schema.

Revision ID: 001
Revises:
Create Date: 2026-05-24
"""

from __future__ import annotations

from alembic import op
from sqlmodel import SQLModel

from poly_bot.persistence.catalog_state import CatalogState  # noqa: F401
from poly_bot.persistence.schema import (  # noqa: F401
    CatalogRun,
    Event,
    Market,
    MarketEnrichment,
    MarketTag,
    Series,
    Token,
    TokenQuote,
)

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    SQLModel.metadata.create_all(bind)


def downgrade() -> None:
    bind = op.get_bind()
    SQLModel.metadata.drop_all(bind)
