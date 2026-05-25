"""Query markets expiring within a time horizon."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlmodel import Session

from poly_bot.catalog.models import ExpiringMarket
from poly_bot.persistence import dialect as sql_dialect


def _dialect_name(session: Session) -> str:
    bind = session.get_bind()
    if bind is None:
        return "sqlite"
    return bind.dialect.name


def list_expiring_markets(session: Session, *, hours: float) -> list[ExpiringMarket]:
    """Markets with effective end time in (now, now + hours]."""
    now = datetime.now(UTC)
    until = now + timedelta(hours=hours)
    dialect = _dialect_name(session)
    active = sql_dialect.active_market_filter(dialect)
    window = sql_dialect.end_ts_in_window(dialect)
    order_by = sql_dialect.order_by_end_ts(dialect)
    quote_ts = sql_dialect.quote_updated_at(dialect)
    params = sql_dialect.window_params(dialect, now, until)

    rows = session.execute(
        text(
            f"""
            SELECT
                m.condition_id,
                m.slug,
                m.question,
                m.domain,
                m.structure,
                m.recurrence,
                COALESCE(m.end_ts, e.end_ts) AS end_ts,
                m.event_id,
                e.title AS event_title,
                s.slug AS series_slug,
                m.rewards_eligible,
                t0.outcome_label AS yes_label,
                t1.outcome_label AS no_label,
                q0.mid AS yes_mid,
                q1.mid AS no_mid,
                q0.spread AS yes_spread,
                q1.spread AS no_spread,
                en.open_interest,
                en.volume_24h,
                m.fees_enabled,
                m.fee_type,
                m.fee_schedule,
                m.maker_base_fee,
                m.taker_base_fee,
                e.fees_enabled AS event_fees_enabled,
                e.fee_type AS event_fee_type,
                e.fee_schedule AS event_fee_schedule,
                e.taker_base_fee AS event_taker_base_fee,
                {quote_ts} AS quote_updated_at,
                en.enriched_at
            FROM markets m
            LEFT JOIN events e ON e.id = m.event_id
            LEFT JOIN series s ON s.id = m.series_id
            LEFT JOIN market_enrichment en ON en.condition_id = m.condition_id
            LEFT JOIN tokens t0 ON t0.condition_id = m.condition_id AND t0.outcome_index = 0
            LEFT JOIN tokens t1 ON t1.condition_id = m.condition_id AND t1.outcome_index = 1
            LEFT JOIN token_quotes q0 ON q0.token_id = t0.token_id
            LEFT JOIN token_quotes q1 ON q1.token_id = t1.token_id
            WHERE {active}
              AND {window}
            ORDER BY {order_by}
            """
        ),
        params,
    ).fetchall()

    tags_by_market = _load_tags(session, [r[0] for r in rows])
    result: list[ExpiringMarket] = []
    for row in rows:
        end_ts = _parse_dt(row[6])
        if end_ts is None:
            continue
        hours_left = (end_ts - now).total_seconds() / 3600.0
        cid = str(row[0])
        result.append(
            ExpiringMarket(
                condition_id=cid,
                slug=str(row[1] or ""),
                question=str(row[2] or ""),
                domain=str(row[3] or "unknown"),
                structure=str(row[4] or "unknown"),
                recurrence=str(row[5] or "one_off"),
                end_ts=end_ts,
                hours_left=round(hours_left, 2),
                event_id=str(row[7]) if row[7] else None,
                event_title=str(row[8]) if row[8] else None,
                series_slug=str(row[9]) if row[9] else None,
                tags=tags_by_market.get(cid, []),
                yes_label=str(row[11] or "Yes"),
                no_label=str(row[12] or "No"),
                yes_mid=_float_or_none(row[13]),
                no_mid=_float_or_none(row[14]),
                yes_spread=_float_or_none(row[15]),
                no_spread=_float_or_none(row[16]),
                open_interest=_float_or_none(row[17]),
                volume_24h=_float_or_none(row[18]),
                fees_enabled=bool(row[19]),
                fee_type=str(row[20]) if row[20] else None,
                fee_schedule=_parse_json_dict(row[21]),
                maker_base_fee=_int_or_none(row[22]),
                taker_base_fee=_int_or_none(row[23]),
                event_fees_enabled=bool(row[24]),
                event_taker_base_fee=_int_or_none(row[27]),
                rewards_eligible=bool(row[10]),
                quote_updated_at=_parse_dt(row[28]),
                enriched_at=_parse_dt(row[29]),
            )
        )
    return result


def count_expiring_markets(session: Session, *, hours: float) -> int:
    now = datetime.now(UTC)
    until = now + timedelta(hours=hours)
    dialect = _dialect_name(session)
    active = sql_dialect.active_market_filter(dialect)
    window = sql_dialect.end_ts_in_window(dialect)
    params = sql_dialect.window_params(dialect, now, until)
    row = session.execute(
        text(
            f"""
            SELECT COUNT(*) FROM markets m
            LEFT JOIN events e ON e.id = m.event_id
            WHERE {active} AND {window}
            """
        ),
        params,
    ).one()
    return int(row[0])


def expiring_condition_ids(session: Session, *, hours: float) -> list[str]:
    now = datetime.now(UTC)
    until = now + timedelta(hours=hours)
    dialect = _dialect_name(session)
    active = sql_dialect.active_market_filter(dialect)
    window = sql_dialect.end_ts_in_window(dialect)
    params = sql_dialect.window_params(dialect, now, until)
    rows = session.execute(
        text(
            f"""
            SELECT m.condition_id FROM markets m
            LEFT JOIN events e ON e.id = m.event_id
            WHERE {active} AND {window}
            """
        ),
        params,
    ).fetchall()
    return [str(r[0]) for r in rows]


def _in_params(condition_ids: list[str]) -> tuple[str, dict[str, object]]:
    placeholders = ",".join(f":c{i}" for i in range(len(condition_ids)))
    params: dict[str, object] = {f"c{i}": cid for i, cid in enumerate(condition_ids)}
    return placeholders, params


def _conditions_with_tokens(session: Session, condition_ids: list[str]) -> set[str]:
    if not condition_ids:
        return set()
    placeholders, params = _in_params(condition_ids)
    rows = session.execute(
        text(
            f"SELECT DISTINCT condition_id FROM tokens WHERE condition_id IN ({placeholders})"
        ),
        params,
    ).fetchall()
    return {str(r[0]) for r in rows}


def conditions_needing_quote_refresh(
    session: Session,
    condition_ids: list[str],
    *,
    stale_seconds: float,
    now: datetime | None = None,
) -> list[str]:
    """Conditions with any token missing a quote or older than stale_seconds."""
    if not condition_ids:
        return []
    now = now or datetime.now(UTC)
    dialect = _dialect_name(session)
    cutoff = sql_dialect.cutoff_param(dialect, now, stale_seconds)
    stale_expr = sql_dialect.ts_before_cutoff(dialect, "q.updated_at")
    placeholders, params = _in_params(condition_ids)
    params.update(cutoff)
    rows = session.execute(
        text(
            f"""
            SELECT DISTINCT t.condition_id
            FROM tokens t
            LEFT JOIN token_quotes q ON q.token_id = t.token_id
            WHERE t.condition_id IN ({placeholders})
              AND (
                q.token_id IS NULL
                OR q.mid IS NULL
                OR {stale_expr}
              )
            """
        ),
        params,
    ).fetchall()
    found = {str(r[0]) for r in rows}
    tokenless = set(condition_ids) - _conditions_with_tokens(session, condition_ids)
    return sorted(found | tokenless)


def conditions_needing_oi_refresh(
    session: Session,
    condition_ids: list[str],
    *,
    stale_seconds: float,
    now: datetime | None = None,
) -> list[str]:
    """Conditions without enrichment or with stale/missing open interest."""
    if not condition_ids:
        return []
    now = now or datetime.now(UTC)
    dialect = _dialect_name(session)
    cutoff = sql_dialect.cutoff_param(dialect, now, stale_seconds)
    fresh_expr = sql_dialect.ts_after_cutoff(dialect, "en.enriched_at")
    placeholders, params = _in_params(condition_ids)
    params.update(cutoff)
    rows = session.execute(
        text(
            f"""
            SELECT m.condition_id
            FROM markets m
            WHERE m.condition_id IN ({placeholders})
              AND NOT EXISTS (
                SELECT 1 FROM market_enrichment en
                WHERE en.condition_id = m.condition_id
                  AND en.enriched_at IS NOT NULL
                  AND {fresh_expr}
              )
            """
        ),
        params,
    ).fetchall()
    return [str(r[0]) for r in rows]


def token_ids_for_conditions(session: Session, condition_ids: list[str]) -> list[str]:
    if not condition_ids:
        return []
    placeholders, params = _in_params(condition_ids)
    rows = session.execute(
        text(f"SELECT token_id FROM tokens WHERE condition_id IN ({placeholders})"),
        params,
    ).fetchall()
    return [str(r[0]) for r in rows]


def _load_tags(session: Session, condition_ids: list[str]) -> dict[str, list[str]]:
    if not condition_ids:
        return {}
    placeholders, params = _in_params(condition_ids)
    rows = session.execute(
        text(
            f"SELECT condition_id, tag FROM market_tags "
            f"WHERE condition_id IN ({placeholders})"
        ),
        params,
    ).fetchall()
    out: dict[str, list[str]] = {}
    for cid, tag in rows:
        out.setdefault(str(cid), []).append(str(tag))
    return out


def _parse_dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _parse_json_dict(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None
