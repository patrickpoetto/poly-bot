"""Taxonomy overrides table.

Revision ID: 002
Revises: 001
Create Date: 2026-05-25
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "taxonomy_overrides",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("kind", sa.String, nullable=False, index=True),
        sa.Column("key", sa.String, nullable=False, index=True),
        sa.Column("domain", sa.String, nullable=True),
        sa.Column("structure", sa.String, nullable=True),
        sa.Column("resolution_source", sa.String, nullable=True),
        sa.Column("note", sa.String, nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime, nullable=False),
    )
    op.create_index(
        "ix_taxonomy_overrides_kind_key",
        "taxonomy_overrides",
        ["kind", "key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_taxonomy_overrides_kind_key", table_name="taxonomy_overrides")
    op.drop_table("taxonomy_overrides")
