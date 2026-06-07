"""Catalog window funnel statistics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import text
from sqlmodel import Session

from poly_bot.catalog.expiring import _in_params, _parse_dt, list_expiring_markets
from poly_bot.catalog.models import ExpiringMarket
from poly_bot.catalog.time_window import in_hot_window
from poly_bot.harvest.pricing import passes_harvest_price_band
from poly_bot.persistence import dialect as sql_dialect
from poly_bot.settings import Settings


@dataclass
class DomainWindowStats:
    domain: str
    in_catalog: int = 0
    in_hot: int = 0
    live_sports: int = 0
    with_quote: int = 0
    mid_ge_95: int = 0
    in_harvest_band: int = 0


@dataclass(frozen=True, slots=True)
class HarvestBandRow:
    condition_id: str
    question: str
    domain: str
    in_hot: bool
    hours_left: float
    is_live: bool
    yes_mid: float | None
    no_mid: float | None
    band_side: str
    band_mid: float


@dataclass
class WindowStats:
    total_markets: int
    active_open: int
    closed_or_inactive: int
    horizon_hours: float
    hot_hours: float
    in_catalog_window: int
    in_hot_window: int
    with_any_quote: int
    mid_ge_95: int
    in_harvest_band: int
    with_tokens: int
    with_both_tokens: int
    global_quotes_total: int
    latest_quote_age_s: float | None
    by_domain: list[DomainWindowStats] = field(default_factory=list)


def _dialect_name(session: Session) -> str:
    bind = session.get_bind()
    if bind is None:
        return "sqlite"
    return bind.dialect.name  # pragma: no cover


def _db_counts(session: Session) -> tuple[int, int]:
    dialect = _dialect_name(session)
    active = sql_dialect.active_market_filter(dialect)
    total = int(session.execute(text("SELECT COUNT(*) FROM markets")).scalar_one())
    active_open = int(
        session.execute(text(f"SELECT COUNT(*) FROM markets m WHERE {active}")).scalar_one()
    )
    return total, active_open


def _quote_freshness(session: Session) -> tuple[int, float | None]:
    row = session.execute(text("SELECT COUNT(*), MAX(updated_at) FROM token_quotes")).one()
    total = int(row[0] or 0)
    latest = _parse_dt(row[1])
    age: float | None = None
    if latest is not None:
        age = max(0.0, (datetime.now(UTC) - latest).total_seconds())
    return total, age


def _token_coverage(session: Session, condition_ids: list[str]) -> tuple[int, int]:
    """(with_any_token, with_both_outcomes) for catalog condition_ids."""
    if not condition_ids:
        return 0, 0
    placeholders, params = _in_params(condition_ids)
    rows = session.execute(
        text(
            f"""
            SELECT t.condition_id, COUNT(*) AS n
            FROM tokens t
            WHERE t.condition_id IN ({placeholders})
            GROUP BY t.condition_id
            """
        ),
        params,
    ).fetchall()
    with_any = len(rows)
    with_both = sum(1 for r in rows if int(r[1]) >= 2)
    return with_any, with_both


def _side_prices(m: ExpiringMarket) -> list[float]:
    out: list[float] = []
    for p in (m.yes_mid, m.no_mid):
        if p is not None:
            out.append(p)
    return out


def _has_quote(m: ExpiringMarket) -> bool:
    return m.yes_mid is not None or m.no_mid is not None


def _mid_ge_95(m: ExpiringMarket) -> bool:
    return any(p >= 0.95 for p in _side_prices(m))


def _in_harvest_band_db(
    m: ExpiringMarket,
    *,
    min_buy: float,
    max_buy: float,
) -> bool:
    return any(
        passes_harvest_price_band(p, min_buy_price=min_buy, max_buy_price=max_buy)
        for p in _side_prices(m)
    )


def _accumulate_domain(
    by_domain: dict[str, DomainWindowStats],
    m: ExpiringMarket,
    *,
    catalog: bool,
    hot: bool,
    min_buy: float,
    max_buy: float,
) -> None:
    d = m.domain or "unknown"
    row = by_domain.setdefault(d, DomainWindowStats(domain=d))
    if catalog:
        row.in_catalog += 1
    if hot:
        row.in_hot += 1
    if m.domain == "sports" and m.is_live:
        row.live_sports += 1
    if _has_quote(m):
        row.with_quote += 1
    if _mid_ge_95(m):
        row.mid_ge_95 += 1
    if _in_harvest_band_db(m, min_buy=min_buy, max_buy=max_buy):
        row.in_harvest_band += 1


def compute_window_stats(
    session: Session,
    settings: Settings,
    *,
    hours: float | None = None,
) -> WindowStats:
    horizon = hours if hours is not None else settings.catalog_expiring_hours
    total, active_open = _db_counts(session)
    global_quotes, latest_quote_age_s = _quote_freshness(session)
    markets = list_expiring_markets(
        session,
        hours=horizon,
        sports_in_play_hours=settings.catalog_sports_in_play_hours,
    )
    hot_markets = [m for m in markets if in_hot_window(m, settings)]

    min_buy = settings.harvest_min_buy_price
    max_buy = settings.harvest_max_buy_price
    cids = [m.condition_id for m in markets]
    with_tokens, with_both_tokens = _token_coverage(session, cids)

    by_domain: dict[str, DomainWindowStats] = {}
    for m in markets:
        hot = in_hot_window(m, settings)
        _accumulate_domain(
            by_domain,
            m,
            catalog=True,
            hot=hot,
            min_buy=min_buy,
            max_buy=max_buy,
        )

    return WindowStats(
        total_markets=total,
        active_open=active_open,
        closed_or_inactive=total - active_open,
        horizon_hours=horizon,
        hot_hours=settings.catalog_quote_hot_hours,
        in_catalog_window=len(markets),
        in_hot_window=len(hot_markets),
        with_any_quote=sum(1 for m in markets if _has_quote(m)),
        mid_ge_95=sum(1 for m in markets if _mid_ge_95(m)),
        in_harvest_band=sum(
            1 for m in markets if _in_harvest_band_db(m, min_buy=min_buy, max_buy=max_buy)
        ),
        with_tokens=with_tokens,
        with_both_tokens=with_both_tokens,
        global_quotes_total=global_quotes,
        latest_quote_age_s=latest_quote_age_s,
        by_domain=sorted(by_domain.values(), key=lambda d: (-d.in_catalog, d.domain)),
    )


def list_harvest_band_rows(
    session: Session,
    settings: Settings,
    *,
    hours: float | None = None,
    hot_only: bool = False,
) -> list[HarvestBandRow]:
    """Markets in catalog window with a DB mid in the harvest price band."""
    horizon = hours if hours is not None else settings.catalog_expiring_hours
    min_buy = settings.harvest_min_buy_price
    max_buy = settings.harvest_max_buy_price
    markets = list_expiring_markets(
        session,
        hours=horizon,
        sports_in_play_hours=settings.catalog_sports_in_play_hours,
    )
    rows: list[HarvestBandRow] = []
    for m in markets:
        if hot_only and not in_hot_window(m, settings):
            continue
        for label, mid in ((m.yes_label, m.yes_mid), (m.no_label, m.no_mid)):
            if mid is None:
                continue
            if not passes_harvest_price_band(mid, min_buy_price=min_buy, max_buy_price=max_buy):
                continue
            rows.append(
                HarvestBandRow(
                    condition_id=m.condition_id,
                    question=m.question,
                    domain=m.domain,
                    in_hot=in_hot_window(m, settings),
                    hours_left=m.hours_left,
                    is_live=m.is_live,
                    yes_mid=m.yes_mid,
                    no_mid=m.no_mid,
                    band_side=label,
                    band_mid=mid,
                )
            )
    rows.sort(key=lambda r: (not r.in_hot, r.hours_left, -r.band_mid))
    return rows
