"""Maker-side quoting for near-certain outcomes.

The empirical shape of these books (2026-07-04 audit): the ITM side of a
10-40 sigma market has NO asks below 0.999 — nobody sells certainty cheap to
takers — but sellers who want liquidity NOW cross thin books and print at
0.90-0.99. The edge is providing that exit liquidity: rest a bid at the target
yield on a verified-certain outcome and let impatient holders come to us.

Maker economics: crypto feeSchedule is takerOnly → zero fee on our side, so
net yield = (1 - bid) / bid. A 0.982 bid nets 1.83%.

Certainty gate: distance between spot and strike measured in units of realized
1-minute volatility (Binance klines) scaled by sqrt(minutes left). Quotes are
cancelled within one poll cycle if the distance decays.
"""

from __future__ import annotations

import logging
import math
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

log = logging.getLogger(__name__)

_BINANCE = "https://data-api.binance.vision"
_DATA_API = "https://data-api.polymarket.com"

_SYMBOLS: dict[str, str] = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "solana": "SOLUSDT",
    "xrp": "XRPUSDT",
    "dogecoin": "DOGEUSDT",
    "bnb": "BNBUSDT",
}

_RE_UPDOWN = re.compile(r"^(\w+)\s+up or down\b", re.IGNORECASE)
_RE_ABOVE = re.compile(r"^(\w+)\s+above\s+([\d,]+(?:\.\d+)?)", re.IGNORECASE)

_SPOT_TTL_S = 4.0
_VOL_TTL_S = 120.0
_VOL_FLOOR = 0.0002  # 0.02%/min — quiet regimes still carry tail risk


class PriceFeed:
    """Cached Binance spot / realized vol / window-open reads."""

    def __init__(self) -> None:
        self._spots: dict[str, tuple[float, float]] = {}  # sym -> (price, ts)
        self._vols: dict[str, tuple[float, float]] = {}  # sym -> (vol_1m, ts)
        self._opens: dict[tuple[str, str], float] = {}  # (sym, iso) -> open

    async def spot(self, client: httpx.AsyncClient, symbol: str) -> float:
        cached = self._spots.get(symbol)
        now = time.monotonic()
        if cached and now - cached[1] < _SPOT_TTL_S:
            return cached[0]
        r = await client.get(f"{_BINANCE}/api/v3/ticker/price", params={"symbol": symbol})
        r.raise_for_status()
        price = float(r.json()["price"])
        self._spots[symbol] = (price, now)
        return price

    async def vol_1m(self, client: httpx.AsyncClient, symbol: str) -> float:
        """Robust realized 1m vol: 68th percentile of |log returns|, last 2h."""
        cached = self._vols.get(symbol)
        now = time.monotonic()
        if cached and now - cached[1] < _VOL_TTL_S:
            return cached[0]
        r = await client.get(
            f"{_BINANCE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1m", "limit": 120},
        )
        r.raise_for_status()
        closes = [float(x[4]) for x in r.json()]
        if len(closes) < 30:
            raise ValueError(f"not enough klines for {symbol}")
        rets = sorted(abs(math.log(closes[i + 1] / closes[i])) for i in range(len(closes) - 1))
        vol = max(rets[int(len(rets) * 0.68)], _VOL_FLOOR)
        self._vols[symbol] = (vol, now)
        return vol

    async def window_open(self, client: httpx.AsyncClient, symbol: str, start_iso: str) -> float:
        key = (symbol, start_iso)
        if key in self._opens:
            return self._opens[key]
        ts = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        ms = int(ts.astimezone(UTC).timestamp() * 1000)
        r = await client.get(
            f"{_BINANCE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1m", "startTime": ms, "limit": 1},
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            raise ValueError(f"no kline for {symbol} at {start_iso}")
        open_ = float(data[0][1])
        self._opens[key] = open_
        return open_


async def sigma_distance(
    client: httpx.AsyncClient,
    feed: PriceFeed,
    *,
    question: str,
    outcome_label: str,
    event_start_iso: str | None,
    minutes_left: float,
) -> tuple[float, float] | None:
    """Return (dist_pct, n_sigma) for a crypto candidate, None if unverifiable.

    dist_pct is signed: positive = our outcome is in the money.
    """
    label = outcome_label.strip().lower()
    m_ud = _RE_UPDOWN.match(question)
    m_ab = _RE_ABOVE.match(question)
    if m_ud is None and m_ab is None:
        return None
    name = (m_ud or m_ab).group(1).lower()  # type: ignore[union-attr]
    symbol = _SYMBOLS.get(name)
    if symbol is None:
        return None

    try:
        spot = await feed.spot(client, symbol)
        vol = await feed.vol_1m(client, symbol)
        if m_ab is not None:
            strike = float(m_ab.group(2).replace(",", ""))
            signed = (spot - strike) / strike
            if label in ("yes", ""):
                dist = signed
            elif label == "no":
                dist = -signed
            else:
                return None
        else:
            if not event_start_iso:
                return None
            start = datetime.fromisoformat(event_start_iso.replace("Z", "+00:00"))
            if start.astimezone(UTC) > datetime.now(UTC):
                return None  # window not started: no information yet
            open_ = await feed.window_open(client, symbol, event_start_iso)
            signed = (spot - open_) / open_
            if label == "up":
                dist = signed
            elif label == "down":
                dist = -signed
            else:
                return None
    except Exception as exc:
        log.debug("sigma_distance_error %s: %s", question[:30], exc)
        return None

    nsig = dist / (vol * math.sqrt(max(minutes_left, 0.5)))
    return dist, nsig


@dataclass
class QuoteCandidate:
    condition_id: str
    token_id: str
    question: str
    domain: str
    outcome_label: str
    outcome_index: int
    end_ts: datetime
    minutes_left: float
    dist_pct: float
    n_sigma: float
    best_bid: float | None
    best_ask: float | None
    bid_price: float  # our resting quote
    net_yield_pct: float  # maker yield at bid_price (no fee)
    take_ask: bool  # True if best_ask already meets the yield target
    order_min_size: float
    event_start_iso: str | None
    recent_trades: int = 0  # prints in the last 30 min — proxy for seller flow


def _fetch_market_rows_sync(settings: Any, lo: datetime, hi: datetime) -> list[Any]:
    """Blocking DB read — MUST run in a thread: SQLite busy_timeout can hold
    the connection for up to 30s while discovery writes, and doing that on
    the event loop freezes every dashboard endpoint."""
    from sqlalchemy import text

    from poly_bot.persistence.db import get_engine

    db_engine = get_engine(settings)
    try:
        with db_engine.connect() as conn:
            return list(
                conn.execute(
                    text("""
                    SELECT m.condition_id, m.question, m.end_ts,
                           t.token_id, t.outcome_label, t.outcome_index,
                           json_extract(m.raw_payload, '$.eventStartTime') as event_start,
                           json_extract(m.raw_payload, '$.orderMinSize') as order_min_size
                    FROM markets m
                    JOIN tokens t ON t.condition_id = m.condition_id
                    WHERE m.active = 1 AND m.closed = 0 AND m.domain = 'crypto'
                      AND m.end_ts > :lo AND m.end_ts <= :hi
                    ORDER BY m.end_ts ASC
                    """),
                    {
                        "lo": lo.strftime("%Y-%m-%d %H:%M:%S"),
                        "hi": hi.strftime("%Y-%m-%d %H:%M:%S"),
                    },
                ).fetchall()
            )
    finally:
        db_engine.dispose()


async def scan_quote_candidates(
    *,
    settings: Any,
    feed: PriceFeed,
) -> tuple[list[QuoteCandidate], dict[str, int]]:
    """Crypto markets in the quote window, sigma-verified, with books."""
    import asyncio

    now = datetime.now(UTC)
    lo = now + timedelta(minutes=settings.nearct_min_quote_minutes)
    hi = now + timedelta(minutes=settings.nearct_quote_horizon_min)
    funnel = {"markets": 0, "sigma_pass": 0, "quoted": 0}

    rows = await asyncio.get_running_loop().run_in_executor(
        None, _fetch_market_rows_sync, settings, lo, hi
    )

    funnel["markets"] = len({r[0] for r in rows})

    target_yield = settings.nearct_target_yield_pct
    quote_price = round(1.0 / (1.0 + target_yield), 3)  # e.g. 1.8% -> 0.982
    min_sigma = settings.nearct_min_sigma

    passed: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(8.0)) as client:
        for cid, q, end_ts, tid, label, idx, ev_start, min_size in rows:
            end = datetime.fromisoformat(str(end_ts)).replace(tzinfo=UTC)
            mins = (end - now).total_seconds() / 60
            sd = await sigma_distance(
                client,
                feed,
                question=q,
                outcome_label=label or "",
                event_start_iso=ev_start,
                minutes_left=mins,
            )
            if sd is None:
                continue
            dist, nsig = sd
            if nsig < min_sigma:
                continue
            passed.append(
                {
                    "cid": cid,
                    "q": q,
                    "tid": tid,
                    "label": label or "",
                    "idx": idx or 0,
                    "end": end,
                    "mins": mins,
                    "dist": dist,
                    "nsig": nsig,
                    "ev": ev_start,
                    "min_size": float(min_size) if min_size else 5.0,
                }
            )

        funnel["sigma_pass"] = len(passed)
        # cap the book fetches: strongest certainty first
        passed.sort(key=lambda p: -p["nsig"])
        passed = passed[:20]

        candidates: list[QuoteCandidate] = []
        clob = getattr(settings, "polymarket_clob_host", "https://clob.polymarket.com")
        for p in passed:
            try:
                rb = await client.get(f"{clob}/book", params={"token_id": p["tid"]})
                rb.raise_for_status()
                book = rb.json()
            except Exception as exc:
                log.debug("quote_book_error %s: %s", p["cid"][:12], exc)
                continue
            asks = book.get("asks", [])
            bids = book.get("bids", [])
            best_ask = min((float(x["price"]) for x in asks), default=None)
            best_bid = max((float(x["price"]) for x in bids), default=None)

            # taker shortcut: someone is already offering our yield or better
            take = False
            if best_ask is not None and best_ask <= quote_price:
                fee = 0.07 * (1 - best_ask)  # taker fee applies on takes
                take_yield = (1 - best_ask - fee) / (best_ask + fee)
                take = take_yield >= target_yield * 0.9

            bid = quote_price
            if best_ask is not None and bid >= best_ask:
                bid = round(best_ask - 0.001, 3)  # never cross with a resting bid

            # Seller-flow proxy: prints in the last 30 min. A 20-sigma market
            # that decided hours ago is dead — nobody left to sell to us.
            recent = 0
            try:
                rt = await client.get(
                    f"{_DATA_API}/trades",
                    params={"market": p["cid"], "limit": 50},
                )
                rt.raise_for_status()
                cutoff = datetime.now(UTC).timestamp() - 1800
                recent = sum(1 for t in rt.json() if t.get("timestamp", 0) > cutoff)
            except Exception as exc:
                log.debug("quote_flow_error %s: %s", p["cid"][:12], exc)

            candidates.append(
                QuoteCandidate(
                    condition_id=p["cid"],
                    token_id=p["tid"],
                    question=p["q"]
                    + (f" [{p['label']}]" if p["label"].lower() not in ("yes", "") else ""),
                    domain="crypto",
                    outcome_label=p["label"],
                    outcome_index=p["idx"],
                    end_ts=p["end"],
                    minutes_left=p["mins"],
                    dist_pct=p["dist"],
                    n_sigma=p["nsig"],
                    best_bid=best_bid,
                    best_ask=best_ask,
                    bid_price=bid,
                    net_yield_pct=(1 - bid) / bid,
                    take_ask=take,
                    order_min_size=p["min_size"],
                    event_start_iso=p["ev"],
                    recent_trades=recent,
                )
            )

    funnel["quoted"] = len(candidates)
    # Flow first (a bid nobody can hit earns nothing), then certainty,
    # then sooner expiry for faster capital recycling.
    candidates.sort(key=lambda c: (-min(c.recent_trades, 10), -c.n_sigma, c.minutes_left))
    return candidates, funnel


async def check_fills(
    client: httpx.AsyncClient,
    *,
    condition_id: str,
    token_id: str,
    bid_price: float,
    since_ts: float,
) -> float:
    """Shares sold at or through our bid since `since_ts` (paper fill proxy).

    A resting bid at P is reached when the book is eaten down to P: any public
    SELL print at price <= P on our token is flow we would have captured.
    """
    try:
        r = await client.get(
            f"{_DATA_API}/trades",
            params={"market": condition_id, "limit": 100},
        )
        r.raise_for_status()
        trades = r.json()
    except Exception as exc:
        log.debug("check_fills_error: %s", exc)
        return 0.0

    shares = 0.0
    for t in trades:
        if (
            t.get("asset") == token_id
            and t.get("side") == "SELL"
            and t.get("timestamp", 0) > since_ts
            and float(t.get("price", 1.0)) <= bid_price
        ):
            shares += float(t.get("size", 0.0))
    return shares
