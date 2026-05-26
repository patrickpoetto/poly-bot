"""Taxonomy overrides table.

Revision ID: 002
Revises: 001
Create Date: 2026-05-25

Migration 001 uses ``SQLModel.metadata.create_all`` which materialises every
table from the current code (including ``taxonomy_overrides``). To stay
compatible with both fresh DBs and DBs already created by 001, skip when the
table already exists.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    bind = op.get_bind()
    return name in sa.inspect(bind).get_table_names()


def upgrade() -> None:
    if _has_table("taxonomy_overrides"):
        return
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
    if not _has_table("taxonomy_overrides"):
        return
    op.drop_index("ix_taxonomy_overrides_kind_key", table_name="taxonomy_overrides")
    op.drop_table("taxonomy_overrides")
