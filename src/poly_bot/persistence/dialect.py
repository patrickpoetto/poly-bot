"""Dialect-specific SQL fragments for SQLite vs PostgreSQL."""

from __future__ import annotations

from datetime import datetime


def is_sqlite(dialect_name: str) -> bool:
    return dialect_name == "sqlite"


def active_market_filter(dialect_name: str) -> str:
    if is_sqlite(dialect_name):
        return "m.active = 1 AND m.closed = 0"
    return "m.active IS TRUE AND m.closed IS FALSE"


def catalog_window_filter(
    dialect_name: str,
    *,
    end_expr: str = "COALESCE(m.end_ts, e.end_ts)",
    kickoff_expr: str = "COALESCE(m.game_start_ts, e.game_start_ts, m.end_ts, e.end_ts)",
) -> str:
    """Markets in the live catalog window.

    Non-sports: ``end_ts`` in (now, now+hours].
    Sports pre-match: kickoff in (now, now+hours].
    Sports in-play: kickoff was within the last ``sports_in_play_hours`` (default 4h).
    """
    standard = end_ts_in_window(dialect_name, end_expr=end_expr)
    if is_sqlite(dialect_name):
        sports_pre = (
            f"m.domain = 'sports' AND {kickoff_expr} IS NOT NULL "
            f"AND datetime({kickoff_expr}) > datetime(:now) "
            f"AND datetime({kickoff_expr}) <= datetime(:until)"
        )
        sports_live = (
            "m.domain = 'sports' AND m.active = 1 AND m.closed = 0 "
            f"AND {kickoff_expr} IS NOT NULL "
            f"AND datetime({kickoff_expr}) <= datetime(:now) "
            f"AND datetime({kickoff_expr}) > datetime(:in_play_since)"
        )
    else:
        sports_pre = (
            f"m.domain = 'sports' AND {kickoff_expr} IS NOT NULL "
            f"AND {kickoff_expr} > :now AND {kickoff_expr} <= :until"
        )
        sports_live = (
            "m.domain = 'sports' AND m.active IS TRUE AND m.closed IS FALSE "
            f"AND {kickoff_expr} IS NOT NULL "
            f"AND {kickoff_expr} <= :now AND {kickoff_expr} > :in_play_since"
        )
    return f"(({standard}) OR ({sports_pre}) OR ({sports_live}))"


def end_ts_in_window(dialect_name: str, *, end_expr: str = "COALESCE(m.end_ts, e.end_ts)") -> str:
    if is_sqlite(dialect_name):
        return (
            f"{end_expr} IS NOT NULL "
            f"AND datetime({end_expr}) > datetime(:now) "
            f"AND datetime({end_expr}) <= datetime(:until)"
        )
    return (
        f"{end_expr} IS NOT NULL "
        f"AND {end_expr} > :now "
        f"AND {end_expr} <= :until"
    )


def ts_before_cutoff(dialect_name: str, column: str) -> str:
    if is_sqlite(dialect_name):
        return f"datetime({column}) <= datetime(:cutoff)"
    return f"{column} <= :cutoff"


def ts_after_cutoff(dialect_name: str, column: str) -> str:
    if is_sqlite(dialect_name):
        return f"datetime({column}) > datetime(:cutoff)"
    return f"{column} > :cutoff"


def quote_updated_at(dialect_name: str) -> str:
    if is_sqlite(dialect_name):
        return "MAX(q0.updated_at, q1.updated_at)"
    return "GREATEST(q0.updated_at, q1.updated_at)"


def order_by_end_ts(dialect_name: str, *, end_expr: str = "COALESCE(m.end_ts, e.end_ts)") -> str:
    if is_sqlite(dialect_name):
        return f"datetime({end_expr}) ASC"
    return f"{end_expr} ASC"


def window_params(
    dialect_name: str,
    now: datetime,
    until: datetime,
    *,
    in_play_since: datetime | None = None,
) -> dict[str, object]:
    if in_play_since is None:
        in_play_since = now
    if is_sqlite(dialect_name):
        fmt = "%Y-%m-%d %H:%M:%S"
        return {
            "now": now.strftime(fmt),
            "until": until.strftime(fmt),
            "in_play_since": in_play_since.strftime(fmt),
        }
    return {"now": now, "until": until, "in_play_since": in_play_since}


def cutoff_param(dialect_name: str, now: datetime, stale_seconds: float) -> dict[str, object]:
    from datetime import timedelta

    cutoff = now - timedelta(seconds=stale_seconds)
    if is_sqlite(dialect_name):
        return {"cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S")}
    return {"cutoff": cutoff}
