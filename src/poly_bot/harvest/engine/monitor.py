"""Two-stage price monitor.

Stage 1: coarse sweep of all monitored markets every engine_stage1_interval_s.
          Promotes markets whose midpoint exceeds engine_alert_threshold to the watchlist.

Stage 2: high-frequency poll of watchlist only every engine_stage2_interval_s.
          Emits ExecutionCandidate objects to the execution queue when ask is confirmed.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx

from poly_bot.harvest.engine import decision_log as dlog
from poly_bot.harvest.engine.state import (
    EngineStats,
    ExecutionCandidate,
    MonitoredMarket,
    WatchlistEntry,
)
from poly_bot.settings import Settings

if TYPE_CHECKING:
    from poly_bot.persistence.schema import HarvestOrder

log = logging.getLogger(__name__)

# Polymarket taker fee formula: fee_per_share = p * (1 - p) * f
# where f = 0.05 for sports, 0.07 for crypto.
# Fee approaches 0 as p → 1, so near-certain outcomes are cheap to trade.
_FEE_RATE = {"sports": 0.05, "crypto": 0.07}
_FEE_RATE_DEFAULT = 0.07
_DIRECTION_TOLERANCE = 0.005  # allow up to 0.5¢ pullback from alert price before skipping


def _net_yield(ask: float, domain: str = "") -> float:
    """Net yield pct using Polymarket's p*(1-p)*f fee formula."""
    if ask <= 0 or ask >= 1:
        return 0.0
    f = _FEE_RATE.get(domain, _FEE_RATE_DEFAULT)
    fee_per_share = ask * (1 - ask) * f
    cost_per_share = ask + fee_per_share
    return (1.0 - cost_per_share) / cost_per_share


class PriceMonitor:
    def __init__(
        self,
        settings: Settings,
        candidate_queue: asyncio.Queue[ExecutionCandidate],
        stats: EngineStats,
        get_open: Callable[[], dict[str, HarvestOrder]] | None = None,
        on_stop_loss: Callable[[str, float], Awaitable[None]] | None = None,
        cancelled_conditions: set[str] | None = None,
    ) -> None:
        self._settings = settings
        self._queue = candidate_queue
        self._stats = stats
        self._get_open = get_open
        self._on_stop_loss = on_stop_loss
        self._cancelled_conditions: set[str] = (
            cancelled_conditions if cancelled_conditions is not None else set()
        )
        # mutable state — only read/written from async tasks (no thread safety needed)
        self._monitored: dict[str, MonitoredMarket] = {}   # condition_id → market
        self._watchlist: dict[str, WatchlistEntry] = {}    # token_id → entry
        self._emitted_conditions: set[str] = set()         # cleared each sweep
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------ public

    def set_markets(self, markets: list[MonitoredMarket]) -> None:
        """Replace the full set of monitored markets (called by engine after DB refresh)."""
        self._monitored = {m.condition_id: m for m in markets}
        # evict watchlist entries whose market is no longer monitored
        gone = [tid for tid, e in self._watchlist.items()
                if e.market.condition_id not in self._monitored]
        for tid in gone:
            del self._watchlist[tid]
        self._emitted_conditions.clear()
        # reset cancellation blacklist every refresh cycle: markets that oscillated and
        # re-entered the band should be tradeable again
        self._cancelled_conditions.clear()

    async def run_stage1(self) -> None:
        """Coarse sweep loop — runs until cancelled."""
        _SWEEP_TIMEOUT = 30.0  # hard cap: a single sweep must finish in 30s
        _HEARTBEAT_INTERVAL = 60  # log alive message every N ticks
        tick = 0
        async with httpx.AsyncClient(
            base_url="https://clob.polymarket.com",
            timeout=httpx.Timeout(connect=3.0, read=5.0, write=3.0, pool=3.0),
        ) as client:
            self._client = client
            while True:
                try:
                    await asyncio.wait_for(
                        self._sweep_stage1(client), timeout=_SWEEP_TIMEOUT
                    )
                except TimeoutError:
                    log.warning("stage1 sweep timeout after %.0fs — skipping tick", _SWEEP_TIMEOUT)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("stage1 sweep error: %s", exc)
                self._stats.last_stage1_at = datetime.now(UTC)
                tick += 1
                if tick % _HEARTBEAT_INTERVAL == 0:
                    log.info(
                        "engine heartbeat: stage1 tick=%d markets=%d watchlist=%d",
                        tick, self._stats.stage1_markets, self._stats.watchlist_size,
                    )
                await asyncio.sleep(self._settings.engine_stage1_interval_s)

    async def run_stage2(self) -> None:
        """High-frequency watchlist loop — runs until cancelled."""
        _SWEEP_TIMEOUT = 15.0
        while True:
            try:
                await asyncio.wait_for(
                    self._sweep_stage2(), timeout=_SWEEP_TIMEOUT
                )
            except TimeoutError:
                log.warning("stage2 sweep timeout after %.0fs — skipping tick", _SWEEP_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("stage2 sweep error: %s", exc)
            await asyncio.sleep(self._settings.engine_stage2_interval_s)

    # ------------------------------------------------------------------ private

    def _crypto_active(self) -> bool:
        """Return False if current UTC hour is outside the configured crypto trading window."""
        spec = self._settings.engine_crypto_active_hours.strip()
        if not spec:
            return True
        try:
            start_s, end_s = spec.split("-", 1)
            start_h, end_h = int(start_s), int(end_s)
        except (ValueError, AttributeError):
            return True
        hour = datetime.now(UTC).hour
        if start_h <= end_h:
            return start_h <= hour < end_h
        # overnight wrap-around (e.g. "22-6")
        return hour >= start_h or hour < end_h

    async def _sweep_stage1(self, client: httpx.AsyncClient) -> None:
        markets = list(self._monitored.values())
        if not markets:
            return

        crypto_ok = self._crypto_active()

        # collect all token IDs to probe
        token_to_market: dict[str, tuple[MonitoredMarket, str]] = {}  # tid → (market, side)
        for m in markets:
            if m.expired:
                continue
            if m.domain == "crypto" and not crypto_ok:
                continue
            token_to_market[m.yes_token_id] = (m, "yes")
            if m.no_token_id:
                token_to_market[m.no_token_id] = (m, "no")

        if not token_to_market:
            return

        mids = await self._fetch_midpoints(client, list(token_to_market.keys()))
        self._stats.stage1_markets = len(markets)

        threshold = self._settings.engine_alert_threshold
        for tid, mid in mids.items():
            if mid < threshold:
                continue
            market, side = token_to_market[tid]
            if market.expired:
                continue
            if tid not in self._watchlist:
                log.info(
                    "watchlist+: %s side=%s mid=%.4f mins_left=%.1f",
                    market.condition_id[:12], side, mid, market.minutes_left,
                )
                self._watchlist[tid] = WatchlistEntry(
                    market=market,
                    side=side,
                    token_id=tid,
                    alert_price=mid,
                    last_price=mid,
                )
                dlog.append(
                    self._settings, dlog.WATCHLIST_ADD,
                    condition_id=market.condition_id,
                    token_id=tid,
                    side=side,
                    domain=market.domain,
                    mid=round(mid, 4),
                    mins_left=round(market.minutes_left, 1),
                )
            else:
                self._watchlist[tid].last_price = mid
        self._stats.watchlist_size = len(self._watchlist)

    async def _sweep_stage2(self) -> None:
        self._emitted_conditions.clear()  # reset per-sweep; executor deduplicates actual orders
        watchlist_tids = list(self._watchlist.keys())

        # collect open position tokens for SL monitoring
        open_pos = self._get_open() if self._get_open is not None else {}
        sl_map: dict[str, tuple[str, object]] = {
            o.token_id: (cid, o) for cid, o in open_pos.items()
        }

        all_tids = list(set(watchlist_tids) | set(sl_map.keys()))
        if not all_tids:
            return

        async def _do(client: httpx.AsyncClient) -> None:
            mids = await self._fetch_midpoints(client, all_tids)

            # stop loss check — runs every tick for open positions
            if self._on_stop_loss is not None and sl_map:
                log.debug("sl_check: monitoring %d open positions", len(sl_map))
                for token_id, (cid, order) in list(sl_map.items()):
                    mid = mids.get(token_id)

                    # midpoints returns None for thinly-traded / near-expiry markets
                    # fall back to best bid from the order book
                    if mid is None:
                        mid = await self._fetch_best_bid(client, token_id)

                    if mid is None:
                        log.debug("sl_check: no price for %s", token_id[:16])
                        continue

                    sl = (
                        self._settings.engine_stop_loss_crypto
                        if getattr(order, "domain", "") in ("crypto", "finance")
                        else self._settings.engine_stop_loss_sports
                    )
                    log.debug("sl_check: %s mid=%.4f sl=%.4f", cid[:12], mid, sl)
                    if mid < sl:
                        # Verify with the order book before triggering.
                        # /midpoints can return 0.50 when the bid side is empty (near
                        # expiry or thin book) — not a genuine reversal.  best_bid=None
                        # means no buyers at all; skip rather than record a false exit.
                        bid = await self._fetch_best_bid(client, token_id)
                        if bid is None:
                            log.debug(
                                "sl_check: %s mid=%.4f < sl=%.4f but no bids — "
                                "skipping (empty book artifact)",
                                cid[:12], mid, sl,
                            )
                            continue
                        if bid >= sl:
                            log.debug(
                                "sl_check: %s mid=%.4f stale (bid=%.4f >= sl=%.4f) — skipping",
                                cid[:12], mid, bid, sl,
                            )
                            continue
                        log.info(
                            "stop_loss triggered %s domain=%s mid=%.4f bid=%.4f < sl=%.4f",
                            cid[:12], getattr(order, "domain", "?"), mid, bid, sl,
                        )
                        dlog.append(
                            self._settings, dlog.STOP_LOSS,
                            condition_id=cid,
                            token_id=token_id,
                            mid=round(mid, 4),
                            bid=round(bid, 4),
                            sl_threshold=round(sl, 4),
                            domain=getattr(order, "domain", None),
                        )
                        await self._on_stop_loss(cid, bid)

            # watchlist: emit buy candidates
            threshold = self._settings.engine_alert_threshold
            above_threshold = [t for t in watchlist_tids if mids.get(t, 0) >= threshold]

            # collect counterpart token ids for complement check
            counterpart_tids: dict[str, str | None] = {}  # tid → other_token_id
            for tid in above_threshold:
                entry = self._watchlist.get(tid)
                if entry is None:
                    continue
                other = (
                    entry.market.no_token_id
                    if entry.side == "yes"
                    else entry.market.yes_token_id
                )
                counterpart_tids[tid] = other

            all_book_tids = list(
                set(above_threshold)
                | {other for other in counterpart_tids.values() if other}
            )
            books = await self._fetch_books(
                client, all_book_tids,
                order_usd=self._settings.harvest_order_usd,
                max_price=self._settings.harvest_max_buy_price,
            )

            from poly_bot.harvest.book_sanity import (
                book_sanity_reject_reason,
                max_bid_ask_spread_for_domain,
            )

            for tid in list(watchlist_tids):
                entry = self._watchlist.get(tid)
                if entry is None:
                    continue
                if entry.market.expired:
                    del self._watchlist[tid]
                    continue
                mid = mids.get(tid)
                if mid is None:
                    continue
                entry.last_price = mid
                if mid < threshold:
                    del self._watchlist[tid]
                    self._cancelled_conditions.add(entry.market.condition_id)
                    log.debug("watchlist-: %s mid dropped to %.4f", tid[:16], mid)
                    dlog.append(
                        self._settings, dlog.WATCHLIST_DROP,
                        condition_id=entry.market.condition_id,
                        token_id=tid,
                        side=entry.side,
                        reason="mid_dropped",
                        mid=round(mid, 4),
                        threshold=threshold,
                    )
                    continue
                # direction filter: only buy if price hasn't declined more than noise tolerance
                if mid < entry.alert_price - _DIRECTION_TOLERANCE:
                    log.debug(
                        "direction_skip %s mid=%.4f < alert=%.4f",
                        tid[:16], mid, entry.alert_price,
                    )
                    dlog.append(
                        self._settings, dlog.DIRECTION_SKIP,
                        condition_id=entry.market.condition_id,
                        token_id=tid,
                        side=entry.side,
                        mid=round(mid, 4),
                        alert_price=round(entry.alert_price, 4),
                    )
                    continue
                book = books.get(tid)
                if book is None:
                    continue
                ask, bid, fillable_usd = book
                if ask is None:
                    continue
                # intra-sweep dedup: each condition emits at most once per stage2 tick
                if entry.market.condition_id in self._emitted_conditions:
                    continue

                # book sanity: spread + complement (catches illiquid zombie books)
                other_tid = counterpart_tids.get(tid)
                other_bid = books.get(other_tid, (None, None, 0.0))[1] if other_tid else None
                reject = book_sanity_reject_reason(
                    ask=ask,
                    bid=bid,
                    other_bid=other_bid,
                    max_bid_ask_spread=max_bid_ask_spread_for_domain(
                        self._settings, entry.market.domain
                    ),
                    max_complement_sum=self._settings.harvest_max_complement_sum,
                )
                if reject:
                    log.info(
                        "book_sanity rejected %s side=%s ask=%.4f bid=%s other_bid=%s reason=%s",
                        entry.market.condition_id[:12], entry.side, ask,
                        f"{bid:.4f}" if bid is not None else "None",
                        f"{other_bid:.4f}" if other_bid is not None else "None",
                        reject,
                    )
                    dlog.append(
                        self._settings, dlog.BOOK_SANITY_REJECT,
                        condition_id=entry.market.condition_id,
                        token_id=tid,
                        side=entry.side,
                        domain=entry.market.domain,
                        ask=round(ask, 4),
                        bid=round(bid, 4) if bid is not None else None,
                        other_bid=round(other_bid, 4) if other_bid is not None else None,
                        spread=round(ask - bid, 4) if bid is not None else None,
                        reason=reject,
                        question=entry.market.question,
                    )
                    del self._watchlist[tid]
                    continue

                mins_left = entry.market.minutes_left
                candidate = ExecutionCandidate(
                    market=entry.market,
                    side=entry.side,
                    token_id=tid,
                    ask=ask,
                    mid=mid,
                    fillable_usd=fillable_usd,
                    net_yield_pct=_net_yield(ask, entry.market.domain),
                    minutes_left=mins_left,
                )
                self._emitted_conditions.add(entry.market.condition_id)
                await self._queue.put(candidate)
                log.info(
                    "candidate: %s side=%s ask=%.4f bid=%.4f mid=%.4f yield=%.2f%% mins=%.1f",
                    entry.market.condition_id[:12], entry.side, ask,
                    bid if bid is not None else 0.0, mid,
                    candidate.net_yield_pct * 100, mins_left,
                )
                dlog.append(
                    self._settings, dlog.CANDIDATE_EMIT,
                    condition_id=entry.market.condition_id,
                    token_id=tid,
                    side=entry.side,
                    domain=entry.market.domain,
                    ask=round(ask, 4),
                    bid=round(bid, 4) if bid is not None else None,
                    mid=round(mid, 4),
                    net_yield_pct=round(candidate.net_yield_pct, 5),
                    mins_left=round(mins_left, 1),
                    question=entry.market.question,
                )

        if self._client is not None:
            await _do(self._client)
        else:
            async with httpx.AsyncClient(
                base_url="https://clob.polymarket.com",
                timeout=httpx.Timeout(connect=3.0, read=5.0, write=3.0, pool=3.0),
            ) as cl:
                await _do(cl)

        self._stats.watchlist_size = len(self._watchlist)

    # ------------------------------------------------------------------ CLOB helpers

    @staticmethod
    async def _fetch_best_bid(
        client: httpx.AsyncClient,
        token_id: str,
    ) -> float | None:
        """Return best bid price from order book (fallback when midpoints unavailable)."""
        try:
            r = await client.get("/book", params={"token_id": token_id})
            r.raise_for_status()
            book = r.json()
            bids = book.get("bids", [])
            if bids:
                best = max(bids, key=lambda x: float(x.get("price", 0)))
                return float(best["price"])
        except Exception as exc:
            log.debug("best_bid fetch error token=%s: %s", token_id[:16], exc)
        return None

    @staticmethod
    async def _fetch_midpoints(
        client: httpx.AsyncClient,
        token_ids: list[str],
    ) -> dict[str, float]:
        if not token_ids:
            return {}
        result: dict[str, float] = {}
        # CLOB /midpoints rejects payloads beyond ~500 tokens — chunk requests
        _CHUNK = 500
        for i in range(0, len(token_ids), _CHUNK):
            batch = token_ids[i : i + _CHUNK]
            try:
                r = await client.post("/midpoints", json=[{"token_id": t} for t in batch])
                r.raise_for_status()
                data = r.json()
            except Exception as exc:
                log.debug("midpoints fetch error (chunk %d): %s", i // _CHUNK, exc)
                continue
            if isinstance(data, list):
                for item in data:
                    tid = item.get("token_id")
                    mid = item.get("mid")
                    if tid and mid is not None:
                        with contextlib.suppress(TypeError, ValueError):
                            result[tid] = float(mid)
            elif isinstance(data, dict):
                for tid, mid in data.items():
                    if mid is not None:
                        with contextlib.suppress(TypeError, ValueError):
                            result[tid] = float(mid)
        return result

    @staticmethod
    async def _fetch_books(
        client: httpx.AsyncClient,
        token_ids: list[str],
        order_usd: float,
        max_price: float,
    ) -> dict[str, tuple[float | None, float | None, float]]:
        """Return (best_ask, best_bid, fillable_usd) for each token, max 20 concurrent requests."""
        from poly_bot.harvest.liquidity import fillable_usd_at_ask, parse_book_levels

        if not token_ids:
            return {}
        result: dict[str, tuple[float | None, float | None, float]] = {}
        sem = asyncio.Semaphore(20)

        async def _one(tid: str) -> None:
            async with sem:
                try:
                    r = await client.get("/book", params={"token_id": tid})
                    r.raise_for_status()
                    book = r.json()
                    asks = book.get("asks", [])
                    bids = book.get("bids", [])
                    best_ask = (
                        min(float(x["price"]) for x in asks) if asks else None
                    )
                    best_bid = (
                        max(float(x["price"]) for x in bids) if bids else None
                    )
                    levels = parse_book_levels(asks)
                    fillable, _ = fillable_usd_at_ask(
                        levels, order_usd=order_usd, max_price=max_price
                    )
                    result[tid] = (best_ask, best_bid, fillable)
                except Exception as exc:
                    log.debug("book fetch error token=%s: %s", tid[:16], exc)

        await asyncio.gather(*[_one(t) for t in token_ids])
        return result
