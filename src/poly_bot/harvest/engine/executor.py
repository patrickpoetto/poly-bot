"""Order executor — paper mode and live mode (py-clob-client)."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime

from poly_bot.harvest.engine.policies import PolicyVerdict, get_policy
from poly_bot.harvest.engine.state import EngineStats, ExecutionCandidate
from poly_bot.persistence.schema import HarvestOrder
from poly_bot.settings import Settings

log = logging.getLogger(__name__)

# size per order in USD (hard-coded; Phase 2 will expose as a setting)
_ORDER_USD = 20.0


class OrderExecutor:
    def __init__(self, settings: Settings, stats: EngineStats) -> None:
        self._settings = settings
        self._stats = stats
        # open positions tracked for risk guards: condition_id → HarvestOrder
        self._open: dict[str, HarvestOrder] = {}

    # ------------------------------------------------------------------ public

    async def consume(self, queue: asyncio.Queue[ExecutionCandidate]) -> None:
        """Drain the candidate queue and execute each one.  Runs until cancelled."""
        while True:
            candidate = await queue.get()
            try:
                await self._handle(candidate)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("executor error: %s", exc, exc_info=True)
                self._stats.orders_failed += 1
            finally:
                queue.task_done()

    # ------------------------------------------------------------------ private

    def _open_by_underlying(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for order in self._open.values():
            key = order.underlying or "unknown"
            counts[key] = counts.get(key, 0) + 1
        return counts

    async def _handle(self, c: ExecutionCandidate) -> None:
        market = c.market
        domain = market.domain

        # global risk caps
        if len(self._open) >= self._settings.engine_max_open_positions:
            log.debug("risk: max_open_positions=%d reached", self._settings.engine_max_open_positions)  # noqa: E501
            return
        notional = sum(o.size_usd for o in self._open.values())
        if notional >= self._settings.engine_max_notional_usd:
            log.debug("risk: max_notional=%.0f reached", self._settings.engine_max_notional_usd)
            return

        # price band guard — only execute within configured harvest band
        if c.ask < self._settings.harvest_min_buy_price:
            log.debug(
                "band_skip %s ask=%.4f < min=%.4f",
                market.condition_id[:12], c.ask, self._settings.harvest_min_buy_price,
            )
            return
        if c.ask > self._settings.harvest_max_buy_price:
            log.debug(
                "band_skip %s ask=%.4f > max=%.4f",
                market.condition_id[:12], c.ask, self._settings.harvest_max_buy_price,
            )
            return

        # domain policy check
        policy = get_policy(domain)
        verdict: PolicyVerdict = policy.check(c, self._open_by_underlying(), self._settings)
        if not verdict.approved:
            log.debug("policy rejected %s: %s", market.condition_id[:12], verdict.reason)
            return

        # place order (paper or live)
        order = _build_order(c, paper=self._settings.paper_trading)
        if self._settings.paper_trading:
            order = await self._place_paper(order)
        else:
            order = await self._place_live(order)

        if order.status in ("filled", "pending"):
            self._open[market.condition_id] = order
            self._stats.orders_placed += 1
            if order.paper:
                self._stats.orders_paper += 1

    async def _place_paper(self, order: HarvestOrder) -> HarvestOrder:
        log.info(
            "PAPER order: %s side=%s ask=%.4f usd=%.1f yield=%.2f%% mins=%.1f",
            order.condition_id[:12], order.side, order.price_target,
            order.size_usd, (order.net_yield_pct or 0) * 100,
            order.minutes_left_at_entry or 0,
        )
        order.status = "filled"
        order.filled_at = datetime.now(UTC)
        order.pnl_usd = round(order.size_usd * (order.net_yield_pct or 0), 4)
        self._stats.last_order_at = datetime.now(UTC)
        await _persist(order)
        return order

    async def _place_live(self, order: HarvestOrder) -> HarvestOrder:
        try:
            from poly_bot.exchange.clob_auth import place_market_buy
        except ImportError:
            log.error("live mode: py-clob-client not installed — falling back to paper")
            order.paper = True
            return await self._place_paper(order)

        try:
            clob_order_id = await place_market_buy(
                token_id=order.token_id,
                price=order.price_target,
                size=order.size_shares,
            )
            order.status = "filled"
            order.clob_order_id = clob_order_id
            order.filled_at = datetime.now(UTC)
            log.info(
                "LIVE order filled: %s clob_id=%s",
                order.condition_id[:12], clob_order_id,
            )
        except Exception as exc:
            order.status = "failed"
            order.reject_reason = str(exc)
            log.error("LIVE order failed: %s — %s", order.condition_id[:12], exc)
            self._stats.orders_failed += 1

        self._stats.last_order_at = datetime.now(UTC)
        await _persist(order)
        return order


# ------------------------------------------------------------------ helpers

def _build_order(c: ExecutionCandidate, *, paper: bool) -> HarvestOrder:
    size_shares = _ORDER_USD / c.ask if c.ask > 0 else 0.0
    return HarvestOrder(
        id=str(uuid.uuid4()),
        condition_id=c.market.condition_id,
        token_id=c.token_id,
        side=c.side,
        domain=c.market.domain,
        question=c.market.question,
        duration_s=int(c.market.duration) if c.market.duration.value > 0 else None,
        underlying=c.market.underlying,
        price_target=c.ask,
        size_usd=_ORDER_USD,
        size_shares=size_shares,
        paper=paper,
        status="pending",
        net_yield_pct=c.net_yield_pct,
        minutes_left_at_entry=c.minutes_left,
        midpoint_at_entry=c.mid,
    )


async def _persist(order: HarvestOrder) -> None:
    """Write order to DB (non-blocking via thread pool)."""

    def _write() -> None:
        from sqlmodel import Session

        from poly_bot.persistence.db import get_engine
        from poly_bot.settings import Settings

        with Session(get_engine(Settings())) as session:
            session.add(order)
            session.commit()

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _write)
