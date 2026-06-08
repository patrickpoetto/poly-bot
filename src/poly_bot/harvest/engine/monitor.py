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
from datetime import UTC, datetime

import httpx

from poly_bot.harvest.engine.state import (
    EngineStats,
    ExecutionCandidate,
    MonitoredMarket,
    WatchlistEntry,
)
from poly_bot.settings import Settings

log = logging.getLogger(__name__)

# worst-case taker fee across all Polymarket markets (0.42%)
_TAKER_FEE = 0.0042


def _net_yield(ask: float) -> float:
    """Net yield pct for a near-certain YES at given ask price."""
    if ask <= 0:
        return 0.0
    payout_per_share = 1.0
    cost_per_share = ask * (1 + _TAKER_FEE)
    return (payout_per_share - cost_per_share) / cost_per_share


class PriceMonitor:
    def __init__(
        self,
        settings: Settings,
        candidate_queue: asyncio.Queue[ExecutionCandidate],
        stats: EngineStats,
    ) -> None:
        self._settings = settings
        self._queue = candidate_queue
        self._stats = stats

        # mutable state — only read/written from async tasks (no thread safety needed)
        self._monitored: dict[str, MonitoredMarket] = {}       # condition_id → market
        self._watchlist: dict[str, WatchlistEntry] = {}        # token_id → entry
        self._emitted_tokens: set[str] = set()                 # tokens already queued this cycle
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
        self._emitted_tokens -= {tid for tid in self._emitted_tokens
                                  if tid not in self._monitored and
                                  tid not in self._watchlist}

    async def run_stage1(self) -> None:
        """Coarse sweep loop — runs until cancelled."""
        async with httpx.AsyncClient(
            base_url="https://clob.polymarket.com",
            timeout=httpx.Timeout(connect=3.0, read=5.0, write=3.0, pool=3.0),
        ) as client:
            self._client = client
            while True:
                try:
                    await self._sweep_stage1(client)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("stage1 sweep error: %s", exc)
                self._stats.last_stage1_at = datetime.now(UTC)
                await asyncio.sleep(self._settings.engine_stage1_interval_s)

    async def run_stage2(self) -> None:
        """High-frequency watchlist loop — runs until cancelled."""
        while True:
            try:
                if self._watchlist:
                    await self._sweep_stage2()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("stage2 sweep error: %s", exc)
            await asyncio.sleep(self._settings.engine_stage2_interval_s)

    # ------------------------------------------------------------------ private

    async def _sweep_stage1(self, client: httpx.AsyncClient) -> None:
        markets = list(self._monitored.values())
        if not markets:
            return

        # collect all token IDs to probe
        token_to_market: dict[str, tuple[MonitoredMarket, str]] = {}  # tid → (market, side)
        for m in markets:
            if m.expired:
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
            if tid in self._emitted_tokens:
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
            else:
                self._watchlist[tid].last_price = mid
        self._stats.watchlist_size = len(self._watchlist)

    async def _sweep_stage2(self) -> None:
        # use shared client if available, else create a short-lived one
        tids = list(self._watchlist.keys())
        if not tids:
            return

        async def _do(client: httpx.AsyncClient) -> None:
            mids = await self._fetch_midpoints(client, tids)
            # also fetch asks for confirmed candidates
            threshold = self._settings.engine_alert_threshold
            asks = await self._fetch_asks(client, [t for t in tids if mids.get(t, 0) >= threshold])
            for tid in list(tids):
                entry = self._watchlist.get(tid)
                if entry is None:
                    continue
                if entry.market.expired:
                    del self._watchlist[tid]
                    self._emitted_tokens.discard(tid)
                    continue
                mid = mids.get(tid)
                if mid is None:
                    continue
                entry.last_price = mid
                if mid < self._settings.engine_alert_threshold:
                    # price retreated — remove from watchlist
                    del self._watchlist[tid]
                    self._emitted_tokens.discard(tid)
                    log.debug("watchlist-: %s mid dropped to %.4f", tid[:16], mid)
                    continue
                ask = asks.get(tid)
                if ask is None:
                    continue
                if tid in self._emitted_tokens:
                    continue
                # confirmed: emit candidate
                mins_left = entry.market.minutes_left
                candidate = ExecutionCandidate(
                    market=entry.market,
                    side=entry.side,
                    token_id=tid,
                    ask=ask,
                    mid=mid,
                    fillable_usd=0.0,   # executor will compute from order book
                    net_yield_pct=_net_yield(ask),
                    minutes_left=mins_left,
                )
                self._emitted_tokens.add(tid)
                await self._queue.put(candidate)
                log.info(
                    "candidate: %s side=%s ask=%.4f mid=%.4f yield=%.2f%% mins=%.1f",
                    entry.market.condition_id[:12], entry.side, ask, mid,
                    candidate.net_yield_pct * 100, mins_left,
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
    async def _fetch_midpoints(
        client: httpx.AsyncClient,
        token_ids: list[str],
    ) -> dict[str, float]:
        if not token_ids:
            return {}
        try:
            r = await client.post("/midpoints", json=[{"token_id": t} for t in token_ids])
            r.raise_for_status()
            data = r.json()
        except Exception as exc:
            log.debug("midpoints fetch error: %s", exc)
            return {}
        result: dict[str, float] = {}
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
    async def _fetch_asks(
        client: httpx.AsyncClient,
        token_ids: list[str],
    ) -> dict[str, float]:
        """Return best ask for each token by fetching book depth=1."""
        if not token_ids:
            return {}
        result: dict[str, float] = {}
        # CLOB book endpoint is per-token; fire concurrently
        async def _one(tid: str) -> None:
            try:
                r = await client.get("/book", params={"token_id": tid})
                r.raise_for_status()
                book = r.json()
                asks = book.get("asks", [])
                if asks:
                    best = min(asks, key=lambda x: float(x.get("price", 999)))
                    result[tid] = float(best["price"])
            except Exception as exc:
                log.debug("ask fetch error token=%s: %s", tid[:16], exc)

        await asyncio.gather(*[_one(t) for t in token_ids])
        return result
