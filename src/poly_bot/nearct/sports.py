"""Sports maker — quote near-certain in-play outcomes using market consensus.

Measured (2026-07-04, 120 high-volume markets / 7 days): in the in-play final
phase, SELL prints at 0.85-0.955 under a stable >=0.90 prior consensus hit the
eventual winner 1002/1002 times (~$70k/week of capturable flow). Yields 7-14%.

Verification insight: every external feed (TV, streams, score APIs) lags the
book by 15-30s because informed traders act first. So the book IS the fastest
ground truth available — we verify with the market's own trade consensus and
protect with a dead-man switch: consensus breaks -> quotes cancelled within
one poll cycle. A >=0.96 consensus outcome flips only on TWO fast events;
the first print against us is survivable by construction.

Resolution: UMA lands onchain ~20-40 min after the final whistle (median
closedTime - kickoff = 131 min). The onchain resolver already handles it.
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

log = logging.getLogger(__name__)

_DATA_API = "https://data-api.polymarket.com"

# Estimated match span: quote window closes here, resolution follows ~30 min later.
_MATCH_SPAN_MIN = 135.0
_CONSENSUS_WINDOW = 8  # median of the last N prints
_CONSENSUS_MAX_AGE_S = 900  # prints older than this don't prove a live consensus


@dataclass
class SportsQuoteCandidate:
    """Duck-type compatible with maker.QuoteCandidate for the engine loop."""

    condition_id: str
    token_id: str
    question: str
    domain: str
    outcome_label: str
    outcome_index: int
    end_ts: datetime  # estimated final whistle (kickoff + 135 min)
    minutes_left: float
    dist_pct: float  # consensus - bid (informational)
    n_sigma: float  # consensus * 100 - 90 (status display proxy)
    best_bid: float | None
    best_ask: float | None
    bid_price: float
    net_yield_pct: float
    take_ask: bool
    order_min_size: float
    event_start_iso: str | None  # None -> engine uses consensus re-check
    recent_trades: int
    consensus: float = 0.0


def _fetch_sports_rows_sync(settings: Any, inplay_min: float, min_volume: float) -> list[Any]:
    """Blocking DB read — run in a thread (SQLite busy_timeout can block 30s)."""
    from sqlalchemy import text

    from poly_bot.persistence.db import get_engine

    db_engine = get_engine(settings)
    try:
        with db_engine.connect() as conn:
            return list(
                conn.execute(
                    text("""
                    SELECT m.condition_id, m.question, m.game_start_ts,
                           t.token_id, t.outcome_label, t.outcome_index,
                           json_extract(m.raw_payload, '$.orderMinSize') as order_min_size,
                           CAST(json_extract(m.raw_payload, '$.volumeNum') AS REAL) as vol
                    FROM markets m
                    JOIN tokens t ON t.condition_id = m.condition_id
                    WHERE m.active = 1 AND m.closed = 0 AND m.domain = 'sports'
                      AND m.game_start_ts IS NOT NULL
                      AND m.game_start_ts <= :inplay_cutoff
                      AND m.game_start_ts > :not_older_than
                      AND CAST(json_extract(m.raw_payload, '$.volumeNum') AS REAL) >= :min_vol
                    ORDER BY vol DESC
                    LIMIT 120
                    """),
                    {
                        "inplay_cutoff": (
                            datetime.now(UTC) - timedelta(minutes=inplay_min)
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                        "not_older_than": (
                            datetime.now(UTC) - timedelta(minutes=_MATCH_SPAN_MIN)
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                        "min_vol": min_volume,
                    },
                ).fetchall()
            )
    finally:
        db_engine.dispose()


async def trade_consensus(
    client: httpx.AsyncClient,
    condition_id: str,
    token_id: str,
) -> tuple[float, int] | None:
    """(median price of recent prints on token, prints in last 30 min), or None.

    The consensus must be LIVE: stale medians from minutes ago prove nothing
    while a match is running.
    """
    try:
        r = await client.get(
            f"{_DATA_API}/trades",
            params={"market": condition_id, "limit": 60},
        )
        r.raise_for_status()
        trades = r.json()
    except Exception as exc:
        log.debug("sports_consensus_error %s: %s", condition_id[:12], exc)
        return None

    now_ts = datetime.now(UTC).timestamp()
    tok = sorted(
        (t for t in trades if t.get("asset") == token_id),
        key=lambda t: t.get("timestamp", 0),
        reverse=True,
    )
    recent = [t for t in tok if now_ts - t.get("timestamp", 0) <= _CONSENSUS_MAX_AGE_S]
    if len(recent) < 4:
        return None
    prices = [float(t["price"]) for t in recent[:_CONSENSUS_WINDOW]]
    flow_30m = sum(1 for t in tok if now_ts - t.get("timestamp", 0) <= 1800)
    return statistics.median(prices), flow_30m


async def scan_sports_candidates(
    *,
    settings: Any,
) -> tuple[list[SportsQuoteCandidate], dict[str, int]]:
    """In-play sports markets whose live trade consensus clears the gate."""
    import asyncio

    funnel = {"markets": 0, "consensus_pass": 0, "quoted": 0}
    rows = await asyncio.get_running_loop().run_in_executor(
        None,
        _fetch_sports_rows_sync,
        settings,
        settings.nearct_sports_inplay_min,
        settings.nearct_sports_min_volume,
    )

    # group tokens per market
    mkts: dict[str, dict[str, Any]] = {}
    for cid, q, gst, tid, label, idx, min_size, _vol in rows:
        m = mkts.setdefault(
            cid,
            {"q": q, "gst": gst, "tokens": [], "min_size": float(min_size) if min_size else 5.0},
        )
        m["tokens"].append((tid, label or "", idx or 0))
    funnel["markets"] = len(mkts)
    if not mkts:
        return [], funnel

    target = settings.nearct_sports_target_yield_pct
    bid_price = round(1.0 / (1.0 + target), 3)
    consensus_gate = settings.nearct_sports_consensus
    now = datetime.now(UTC)

    candidates: list[SportsQuoteCandidate] = []
    clob = getattr(settings, "polymarket_clob_host", "https://clob.polymarket.com")

    async with httpx.AsyncClient(timeout=httpx.Timeout(8.0)) as client:
        for cid, m in mkts.items():
            gst = datetime.fromisoformat(str(m["gst"])).replace(tzinfo=UTC)
            whistle = gst + timedelta(minutes=_MATCH_SPAN_MIN)
            mins_left = (whistle - now).total_seconds() / 60
            if mins_left <= 1:
                continue

            for tid, label, idx in m["tokens"]:
                cons = await trade_consensus(client, cid, tid)
                if cons is None:
                    continue
                consensus, flow = cons
                if consensus < consensus_gate:
                    continue
                funnel["consensus_pass"] += 1

                best_bid = best_ask = None
                take = False
                try:
                    rb = await client.get(f"{clob}/book", params={"token_id": tid})
                    rb.raise_for_status()
                    book = rb.json()
                    asks = book.get("asks", [])
                    bids = book.get("bids", [])
                    best_ask = min((float(x["price"]) for x in asks), default=None)
                    best_bid = max((float(x["price"]) for x in bids), default=None)
                    # Sports takes are DISABLED: both session losses came from
                    # lifting anomalously discounted asks (7.7% and 20% below
                    # consensus — informed flow). The measured edge (1002/1002)
                    # is the resting bid capturing impatient sellers at OUR
                    # price; takes were an unbacked extrapolation. take stays False.
                except Exception as exc:
                    log.debug("sports_book_error %s: %s", cid[:12], exc)

                bid = bid_price
                if best_ask is not None and bid >= best_ask:
                    bid = round(best_ask - 0.001, 3)

                candidates.append(
                    SportsQuoteCandidate(
                        condition_id=cid,
                        token_id=tid,
                        question=m["q"]
                        + (f" [{label}]" if label.lower() not in ("yes", "") else ""),
                        domain="sports",
                        outcome_label=label,
                        outcome_index=idx,
                        end_ts=whistle,
                        minutes_left=mins_left,
                        dist_pct=consensus - bid,
                        n_sigma=consensus * 100 - 90,  # display proxy: 0.97 -> 7.0
                        best_bid=best_bid,
                        best_ask=best_ask,
                        bid_price=bid,
                        net_yield_pct=(1 - bid) / bid,
                        take_ask=take,
                        order_min_size=m["min_size"],
                        event_start_iso=None,
                        recent_trades=flow,
                        consensus=consensus,
                    )
                )

    funnel["quoted"] = len(candidates)
    candidates.sort(key=lambda c: (-min(c.recent_trades, 10), -c.consensus))
    return candidates, funnel


async def consensus_still_valid(
    client: httpx.AsyncClient,
    settings: Any,
    candidate: SportsQuoteCandidate,
) -> bool:
    """Dead-man switch: the quote survives only while the live consensus holds.

    Slightly looser than the entry gate (gate - 0.02) so routine noise doesn't
    churn quotes, but a real break (goal against) cancels within one poll.
    """
    cons = await trade_consensus(client, candidate.condition_id, candidate.token_id)
    if cons is None:
        return True  # no fresh prints — nothing traded, nothing changed
    consensus, _ = cons
    ok: bool = consensus >= settings.nearct_sports_consensus - 0.02
    return ok
