"""CLOB scanner — finds near-certain markets expiring within a short window."""

from __future__ import annotations

import contextlib
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

import httpx

log = logging.getLogger(__name__)

_FEE_RATE: dict[str, float] = {"sports": 0.05, "crypto": 0.07, "finance": 0.07}
_FEE_DEFAULT = 0.07


def _net_yield(
    ask: float,
    domain: str,
    fee_rate: float | None = None,
    fee_exponent: float = 1.0,
) -> float:
    """Net return per $1 of cost, using Polymarket's exact fee formula.

    fee_per_share = rate * min(p, 1-p)^exponent  (feeSchedule from Gamma payload).
    Falls back to per-domain worst-case rates when the payload has no schedule.
    """
    if ask <= 0 or ask >= 1:
        return 0.0
    rate: float = fee_rate if fee_rate is not None else _FEE_RATE.get(domain, _FEE_DEFAULT)
    fee: float = rate * float(min(ask, 1.0 - ask)) ** fee_exponent
    cost = ask + fee
    return (1.0 - cost) / cost


def _mins_left(end_ts_raw: object, now: datetime) -> float:
    try:
        if isinstance(end_ts_raw, datetime):
            ts = end_ts_raw if end_ts_raw.tzinfo else end_ts_raw.replace(tzinfo=UTC)
        else:
            ts = datetime.fromisoformat(str(end_ts_raw).replace("T", " ").rstrip("Z")).replace(
                tzinfo=UTC
            )
        return max(0.0, (ts - now).total_seconds() / 60.0)
    except Exception:
        return 0.0


def _parse_end_ts(end_ts_raw: object) -> datetime | None:
    try:
        if isinstance(end_ts_raw, datetime):
            return end_ts_raw if end_ts_raw.tzinfo else end_ts_raw.replace(tzinfo=UTC)
        ts = datetime.fromisoformat(str(end_ts_raw).replace("T", " ").rstrip("Z")).replace(
            tzinfo=UTC
        )
        return ts
    except Exception:
        return None


@dataclass
class NearctCandidate:
    condition_id: str
    token_id: str  # the token to buy — may be YES/UP or NO/DOWN
    question: str  # includes "[NO]" suffix when buying the down side
    domain: str
    ask: float
    net_yield_pct: float
    fillable_usd: float
    end_ts: datetime
    minutes_left: float
    outcome_index: int = 0  # CTF outcome slot — used for onchain resolution
    order_min_size: float = 5.0  # min shares per order (CLOB orderMinSize)
    outcome_label: str = ""  # raw label (up/down/yes/no) for entry verification
    event_start_iso: str | None = None  # updown window start (payload eventStartTime)


@dataclass
class ScanStats:
    markets_in_window: int = 0
    mid_passed: int = 0
    book_checked: int = 0
    candidates: int = 0
    rejected: list[dict[str, Any]] = field(default_factory=list)

    def _reject(
        self,
        cid: str,
        question: str,
        domain: str,
        minutes_left: float,
        reason: str,
        ask: float | None = None,
        depth: float | None = None,
    ) -> None:
        self.rejected.append(
            {
                "cid": cid,
                "question": question,
                "domain": domain,
                "minutes_left": round(minutes_left, 1),
                "mid": None,
                "ask": round(ask, 4) if ask is not None else None,
                "depth": round(depth, 2) if depth is not None else None,
                "reason": reason,
            }
        )


async def scan(
    *,
    ask_min: float,
    horizon_min: float,
    min_depth_usd: float,
    settings: object,
    min_yield_pct: float = 0.0,
    domains: tuple[str, ...] = (),
) -> tuple[list[NearctCandidate], ScanStats]:
    """Return (candidates sorted best-first, scan funnel stats).

    Checks both YES/UP and NO/DOWN sides for every market.
    """
    from sqlalchemy import text

    from poly_bot.persistence.db import get_engine

    stats = ScanStats()
    db_engine = get_engine(settings)  # type: ignore[arg-type]
    now = datetime.now(UTC)
    horizon = now + timedelta(minutes=horizon_min)

    # Fetch ALL tokens from markets in the window — no outcome_label filter.
    # This covers yes/no, up/down, over/under, team names, any binary market.
    # Also fetch raw_payload gameStartTime to detect slow-resolve observation markets.
    token_rows: Sequence[Any] = []
    try:
        with db_engine.connect() as conn:
            token_rows = conn.execute(
                text("""
            SELECT m.condition_id, m.question, m.domain, m.end_ts,
                   t.token_id, t.outcome_label, t.outcome_index,
                   substr(json_extract(m.raw_payload, '$.gameStartTime'), 1, 19) as gst_raw,
                   json_extract(m.raw_payload, '$.feeSchedule.rate') as fee_rate,
                   json_extract(m.raw_payload, '$.feeSchedule.exponent') as fee_exp,
                   json_extract(m.raw_payload, '$.feesEnabled') as fees_enabled,
                   json_extract(m.raw_payload, '$.orderMinSize') as order_min_size,
                   json_extract(m.raw_payload, '$.eventStartTime') as event_start
            FROM markets m
            JOIN tokens t ON t.condition_id = m.condition_id
            WHERE m.active = 1 AND m.closed = 0
              AND (:domains_csv = '' OR ',' || :domains_csv || ',' LIKE '%,' || m.domain || ',%')
              AND (
                m.game_start_ts IS NULL
                OR (m.game_start_ts < m.end_ts AND m.game_start_ts <= :now)
              )
              AND m.end_ts >= :now AND m.end_ts <= :horizon
            ORDER BY m.end_ts ASC, m.condition_id, t.outcome_index
        """),
                {
                    "now": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "horizon": horizon.strftime("%Y-%m-%d %H:%M:%S"),
                    "domains_csv": ",".join(domains),
                },
            ).fetchall()
    finally:
        db_engine.dispose()

    # Group tokens by market
    markets: dict[str, dict[str, Any]] = {}
    for r in token_rows:
        cid = r[0]
        if cid not in markets:
            fees_on = r[10]
            markets[cid] = {
                "question": r[1],
                "domain": r[2] or "",
                "end_ts": r[3],
                "tokens": [],  # list of (token_id, outcome_label, outcome_index)
                "gst_raw": r[7],  # gameStartTime from raw_payload (truncated, no tz)
                # feesEnabled false → 0; missing schedule → None (domain default)
                "fee_rate": 0.0
                if fees_on in (0, "0", False, "false")
                else (float(r[8]) if r[8] is not None else None),
                "fee_exp": float(r[9]) if r[9] is not None else 1.0,
                "order_min_size": float(r[11]) if r[11] is not None else 5.0,
                "event_start": r[12],  # updown window start (ISO) or None
            }
        markets[cid]["tokens"].append((r[4], r[5] or "", r[6]))

    stats.markets_in_window = len(markets)
    if not markets:
        return [], stats

    all_tids: set[str] = {tok[0] for m in markets.values() for tok in m["tokens"]}

    stats.mid_passed = len(markets)
    stats.book_checked = len(all_tids)

    clob_host = getattr(settings, "polymarket_clob_host", "https://clob.polymarket.com")
    async with httpx.AsyncClient(
        base_url=clob_host,
        timeout=httpx.Timeout(connect=4.0, read=8.0, write=4.0, pool=4.0),
    ) as client:
        books = await _fetch_books(client, list(all_tids))

    # Layer 1: keyword filter — observation-period markets that always name the metric
    _SLOW_RESOLVE_KEYWORDS = (
        "temperature",
        "°f",
        "°c",
        "degrees",
        "rainfall",
        "precipitation",
        "humidity",
        "wind speed",
        "wind gust",
        "uv index",
        "air quality",
        "aqi",
        "snowfall",
        "highest temp",
        "lowest temp",
        "high temp",
        "low temp",
    )

    candidates: list[NearctCandidate] = []

    for cid, meta in markets.items():
        question = meta["question"]
        domain = meta["domain"]
        mins = _mins_left(meta["end_ts"], now)
        end_ts = _parse_end_ts(meta["end_ts"])

        if end_ts is None:
            stats._reject(
                cid=cid, question=question, domain=domain, minutes_left=mins, reason="bad_ts"
            )
            continue

        # Layer 1: explicit keyword check for weather/observation markets
        q_lower = question.lower()
        if any(kw in q_lower for kw in _SLOW_RESOLVE_KEYWORDS):
            stats._reject(
                cid=cid, question=question, domain=domain, minutes_left=mins, reason="slow_resolve"
            )
            continue

        # Layer 2: structural check — markets where the observation window starts
        # significantly before end_ts (e.g., daily temperature: gameStartTime = midnight
        # local, end_ts = noon UTC = only 4-8h into a 24h observation period).
        # Sports/crypto are exempt: sports have game_start_ts stored; crypto has no
        # gameStartTime. Only fires for unknown-domain observation markets.
        gst_raw = meta.get("gst_raw")
        if gst_raw and domain not in ("sports", "crypto"):
            try:
                gst = datetime.fromisoformat(gst_raw).replace(tzinfo=UTC)
                if (end_ts - gst).total_seconds() > 3 * 3600:
                    stats._reject(
                        cid=cid,
                        question=question,
                        domain=domain,
                        minutes_left=mins,
                        reason="slow_resolve",
                    )
                    continue
            except Exception:  # noqa: S110
                pass

        token_list = meta["tokens"]  # [(token_id, label, index), ...]

        # For each token, check its book; complement = all OTHER tokens in the market
        for trade_tid, trade_label, trade_idx in token_list:
            comp_tids = [t[0] for t in token_list if t[0] != trade_tid]
            label_suffix = f" [{trade_label}]" if trade_label.lower() not in ("yes", "") else ""

            book = books.get(trade_tid)
            if book is None:
                stats._reject(
                    cid=cid,
                    question=question + label_suffix,
                    domain=domain,
                    minutes_left=mins,
                    reason="no_book",
                )
                continue

            ask, _bid, depth = book

            if ask is None or ask < ask_min:
                stats._reject(
                    cid=cid,
                    question=question + label_suffix,
                    domain=domain,
                    minutes_left=mins,
                    reason="ask_low",
                    ask=ask,
                    depth=depth,
                )
                continue

            if depth < min_depth_usd:
                stats._reject(
                    cid=cid,
                    question=question + label_suffix,
                    domain=domain,
                    minutes_left=mins,
                    reason="depth_low",
                    ask=ask,
                    depth=depth,
                )
                continue

            # complement sanity: our_ask + best comp_bid ≤ 1.03
            # For multi-outcome markets, check against the highest comp bid.
            comp_bid: float | None = None
            for ctid in comp_tids:
                cb = books.get(ctid)
                if cb and cb[1] is not None:
                    comp_bid = max(comp_bid or 0.0, cb[1])
            if comp_bid is not None and ask + comp_bid > 1.03:
                log.debug(
                    "nearct_scan: complement fail %s [%s] (%.3f+%.3f)",
                    cid[:12],
                    trade_label,
                    ask,
                    comp_bid,
                )
                stats._reject(
                    cid=cid,
                    question=question + label_suffix,
                    domain=domain,
                    minutes_left=mins,
                    reason="complement_fail",
                    ask=ask,
                    depth=depth,
                )
                continue

            net_yield = _net_yield(
                ask,
                domain,
                fee_rate=meta["fee_rate"],
                fee_exponent=meta["fee_exp"],
            )

            # Yield floor: an ask of 0.999 pays 0.09% but still risks 100% —
            # poor risk/reward AND it dilutes the compounding target.
            if net_yield < min_yield_pct:
                stats._reject(
                    cid=cid,
                    question=question + label_suffix,
                    domain=domain,
                    minutes_left=mins,
                    reason="yield_low",
                    ask=ask,
                    depth=depth,
                )
                continue

            candidates.append(
                NearctCandidate(
                    condition_id=cid,
                    token_id=trade_tid,
                    question=question + label_suffix,
                    domain=domain,
                    ask=ask,
                    net_yield_pct=net_yield,
                    fillable_usd=depth,
                    end_ts=end_ts,
                    minutes_left=mins,
                    outcome_index=trade_idx if trade_idx is not None else 0,
                    order_min_size=meta["order_min_size"],
                    outcome_label=trade_label,
                    event_start_iso=meta["event_start"],
                )
            )

    # best first: yield * depth score — rewards high yield AND liquid book equally
    candidates.sort(key=lambda c: -(c.net_yield_pct * c.fillable_usd))
    stats.candidates = len(candidates)
    return candidates, stats


def _parse_book_entry(
    raw: dict[str, Any],
) -> tuple[float | None, float | None, float]:
    """Parse a single book dict into (best_ask, best_bid, total_fillable_usd).

    depth = total USD across ALL ask levels, not just the single best tick.
    This is the true liquidity measure: thin markets with a single stale
    quote at 0.990 get a low depth and are rejected by the depth filter.
    """
    asks = raw.get("asks", [])
    bids = raw.get("bids", [])
    best_ask = min(float(x["price"]) for x in asks) if asks else None
    best_bid = max(float(x["price"]) for x in bids) if bids else None
    depth = sum(float(x.get("size", 0)) * float(x["price"]) for x in asks) if best_ask else 0.0
    return best_ask, best_bid, depth


async def _fetch_books(
    client: httpx.AsyncClient,
    token_ids: list[str],
) -> dict[str, tuple[float | None, float | None, float]]:
    """Return (best_ask, best_bid, fillable_usd) per token.

    Uses POST /books (batch) with fallback to concurrent GET /book per token.
    """
    import asyncio

    if not token_ids:
        return {}

    # ── Attempt 1: single batch request ────────────────────────────────────
    try:
        r = await client.post("/books", json=[{"token_id": tid} for tid in token_ids])
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            result: dict[str, tuple[float | None, float | None, float]] = {}
            for item in data:
                if not isinstance(item, dict):
                    continue
                # Polymarket may use "asset_id" or "token_id" as the key
                tid = item.get("asset_id") or item.get("token_id")
                if not tid:
                    continue
                with contextlib.suppress(Exception):
                    result[tid] = _parse_book_entry(item)
            if result:
                log.debug("nearct_books: batch OK, %d/%d tokens", len(result), len(token_ids))
                return result
    except Exception as exc:
        log.warning("nearct_books: batch failed (%s) — falling back to individual GETs", exc)

    # ── Fallback: concurrent individual GET /book ───────────────────────────
    log.debug("nearct_books: individual GET for %d tokens", len(token_ids))
    result = {}
    sem = asyncio.Semaphore(20)

    async def _one(tid: str) -> None:
        async with sem:
            try:
                r = await client.get("/book", params={"token_id": tid})
                r.raise_for_status()
                result[tid] = _parse_book_entry(r.json())
            except Exception as exc:
                log.debug("nearct book error %s: %s", tid[:16], exc)

    await asyncio.gather(*[_one(t) for t in token_ids])
    return result
