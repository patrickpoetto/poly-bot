"""Catalog data-quality checks."""

from __future__ import annotations

from sqlalchemy import func, text
from sqlmodel import Session, select

from poly_bot.persistence.schema import Event, Market, Token


def run_reconciliation(session: Session) -> dict[str, int | list[str]]:
    """Return counts of common catalog integrity issues."""
    issues: dict[str, int | list[str]] = {}

    bind = session.get_bind()
    is_pg = bind is not None and bind.dialect.name == "postgresql"
    empty_raw_clause = (
        "raw_payload IS NULL OR raw_payload::text IN ('{}', 'null')"
        if is_pg
        else "raw_payload IS NULL OR raw_payload = '{}'"
    )

    issues["markets_total"] = int(session.exec(select(func.count()).select_from(Market)).one())
    issues["markets_null_fee_type"] = int(
        session.exec(
            select(func.count()).select_from(Market).where(Market.fee_type == None)  # noqa: E711
        ).one()
    )
    issues["markets_missing_event"] = int(
        session.exec(
            select(func.count()).select_from(Market).where(Market.event_id == None)  # noqa: E711
        ).one()
    )
    issues["markets_empty_raw"] = int(
        session.exec(
            select(func.count()).select_from(Market).where(text(empty_raw_clause))
        ).one()
    )
    issues["markets_unknown_domain"] = int(
        session.exec(
            select(func.count()).select_from(Market).where(Market.domain == "unknown")
        ).one()
    )

    tokenless_count = session.execute(
        text(
            """
            SELECT COUNT(*) FROM markets m
            WHERE NOT EXISTS (
                SELECT 1 FROM tokens t WHERE t.condition_id = m.condition_id
            )
            """
        )
    ).scalar()
    issues["markets_without_tokens"] = int(tokenless_count or 0)

    sample = session.execute(
        text(
            """
            SELECT m.condition_id FROM markets m
            WHERE NOT EXISTS (
                SELECT 1 FROM tokens t WHERE t.condition_id = m.condition_id
            )
            LIMIT 20
            """
        )
    ).fetchall()
    issues["sample_tokenless"] = [str(r[0]) for r in sample]

    issues["events_total"] = int(session.exec(select(func.count()).select_from(Event)).one())
    issues["events_with_fees"] = int(
        session.exec(
            select(func.count()).select_from(Event).where(Event.fees_enabled == True)  # noqa: E712
        ).one()
    )

    stale_quotes = session.execute(
        text(
            """
            SELECT COUNT(*) FROM token_quotes q
            WHERE q.updated_at IS NULL OR q.mid IS NULL
            """
        )
    ).scalar()
    issues["quotes_incomplete"] = int(stale_quotes or 0)

    return issues
