"""Query markets expiring within a time horizon."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlmodel import Session

from poly_bot.catalog.models import ExpiringMarket
from poly_bot.persistence import dialect as sql_dialect
from poly_bot.settings import Settings


def _dialect_name(session: Session) -> str:
    bind = session.get_bind()
    if bind is None:
        return "sqlite"
    return bind.dialect.name


def list_expiring_markets(
    session: Session,
    *,
    hours: float,
    sports_in_play_hours: float = 4.0,
) -> list[ExpiringMarket]:
    """Markets in catalog window: pre-match by end_ts, sports also in-play after kickoff."""
    now = datetime.now(UTC)
    until = now + timedelta(hours=hours)
    in_play_since = now - timedelta(hours=sports_in_play_hours)
    dialect = _dialect_name(session)
    active = sql_dialect.active_market_filter(dialect)
    window = sql_dialect.catalog_window_filter(dialect)
    kickoff_expr = "COALESCE(m.game_start_ts, e.game_start_ts, m.end_ts, e.end_ts)"
    order_by = sql_dialect.order_by_end_ts(dialect, end_expr=kickoff_expr)
    quote_ts = sql_dialect.quote_updated_at(dialect)
    params = sql_dialect.window_params(dialect, now, until, in_play_since=in_play_since)

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
                {kickoff_expr} AS kickoff_ts,
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
        kickoff_ts = _parse_dt(row[7])
        domain = str(row[3] or "unknown")
        if end_ts is None and kickoff_ts is None:
            continue
        effective_kickoff = kickoff_ts
        if domain == "sports" and effective_kickoff is None and end_ts is not None:
            effective_kickoff = end_ts
        is_sports_in_play = domain == "sports" and effective_kickoff is not None
        is_live = is_sports_in_play and effective_kickoff <= now
        resolution_kind = "in_play" if is_sports_in_play else "fixed"
        hours_to_kickoff: float | None = None
        if is_sports_in_play:
            assert effective_kickoff is not None
            window_end = effective_kickoff + timedelta(hours=sports_in_play_hours)
            hours_left = (window_end - now).total_seconds() / 3600.0
            if effective_kickoff > now:
                hours_to_kickoff = (effective_kickoff - now).total_seconds() / 3600.0
            display_end = end_ts or effective_kickoff
        else:
            window_end = end_ts or kickoff_ts
            assert window_end is not None
            hours_left = (window_end - now).total_seconds() / 3600.0
            display_end = window_end
        cid = str(row[0])
        result.append(
            ExpiringMarket(
                condition_id=cid,
                slug=str(row[1] or ""),
                question=str(row[2] or ""),
                domain=domain,
                structure=str(row[4] or "unknown"),
                recurrence=str(row[5] or "one_off"),
                end_ts=display_end,
                game_start_ts=effective_kickoff if is_sports_in_play else kickoff_ts,
                is_live=is_live,
                resolution_kind=resolution_kind,
                window_end_ts=window_end,
                hours_to_kickoff=round(hours_to_kickoff, 2)
                if hours_to_kickoff is not None
                else None,
                hours_left=round(hours_left, 2),
                event_id=str(row[8]) if row[8] else None,
                event_title=str(row[9]) if row[9] else None,
                series_slug=str(row[10]) if row[10] else None,
                tags=tags_by_market.get(cid, []),
                yes_label=str(row[12] or "Yes"),
                no_label=str(row[13] or "No"),
                yes_mid=_float_or_none(row[14]),
                no_mid=_float_or_none(row[15]),
                yes_spread=_float_or_none(row[16]),
                no_spread=_float_or_none(row[17]),
                open_interest=_float_or_none(row[18]),
                volume_24h=_float_or_none(row[19]),
                fees_enabled=bool(row[20]),
                fee_type=str(row[21]) if row[21] else None,
                fee_schedule=_parse_json_dict(row[22]),
                maker_base_fee=_int_or_none(row[23]),
                taker_base_fee=_int_or_none(row[24]),
                event_fees_enabled=bool(row[25]),
                event_taker_base_fee=_int_or_none(row[28]),
                rewards_eligible=bool(row[11]),
                quote_updated_at=_parse_dt(row[29]),
                enriched_at=_parse_dt(row[30]),
            )
        )
    return result


def count_expiring_markets(
    session: Session,
    *,
    hours: float,
    sports_in_play_hours: float = 4.0,
) -> int:
    now = datetime.now(UTC)
    until = now + timedelta(hours=hours)
    in_play_since = now - timedelta(hours=sports_in_play_hours)
    dialect = _dialect_name(session)
    active = sql_dialect.active_market_filter(dialect)
    window = sql_dialect.catalog_window_filter(dialect)
    params = sql_dialect.window_params(dialect, now, until, in_play_since=in_play_since)
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


def expiring_condition_ids(
    session: Session,
    *,
    hours: float,
    sports_in_play_hours: float = 4.0,
) -> list[str]:
    now = datetime.now(UTC)
    until = now + timedelta(hours=hours)
    in_play_since = now - timedelta(hours=sports_in_play_hours)
    dialect = _dialect_name(session)
    active = sql_dialect.active_market_filter(dialect)
    window = sql_dialect.catalog_window_filter(dialect)
    params = sql_dialect.window_params(dialect, now, until, in_play_since=in_play_since)
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


def subscription_condition_ids(
    session: Session,
    settings: Settings,
    *,
    hours: float | None = None,
) -> list[str]:
    """Markets to stream/enrich: standard expiring window + weather forecast bins."""
    from poly_bot.weather.quotes import weather_condition_ids

    horizon = hours if hours is not None else settings.catalog_quote_horizon_hours
    standard = expiring_condition_ids(
        session,
        hours=horizon,
        sports_in_play_hours=settings.catalog_sports_in_play_hours,
    )
    weather = weather_condition_ids(session, settings)
    seen: set[str] = set(standard)
    out = list(standard)
    for cid in weather:
        if cid not in seen:
            out.append(cid)
            seen.add(cid)
    return out
