"""Order executor — paper mode and live mode (py-clob-client)."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime

from poly_bot.harvest.engine import decision_log as dlog
from poly_bot.harvest.engine.policies import PolicyVerdict, get_policy
from poly_bot.harvest.engine.state import ChainTicketData, EngineStats, ExecutionCandidate
from poly_bot.persistence.schema import HarvestOrder
from poly_bot.settings import Settings

log = logging.getLogger(__name__)


class OrderExecutor:
    def __init__(
        self,
        settings: Settings,
        stats: EngineStats,
        cancelled_conditions: set[str] | None = None,
    ) -> None:
        self._settings = settings
        self._stats = stats
        self._cancelled: set[str] = cancelled_conditions if cancelled_conditions is not None else set()  # noqa: E501
        # open positions tracked for risk guards: condition_id → HarvestOrder
        self._open: dict[str, HarvestOrder] = {}
        # (event_id:market_type) groups already traded — one order per group
        self._traded_events: set[str] = set()
        # pending compound chain credits (sorted on pop: highest level first, then FIFO)
        self._pending_chain_tickets: list[ChainTicketData] = []

    # ------------------------------------------------------------------ public

    async def consume(self, queue: asyncio.Queue[ExecutionCandidate]) -> None:
        """Drain the candidate queue and execute each one.  Runs until cancelled."""
        await asyncio.get_running_loop().run_in_executor(None, self._load_traded_events)
        await asyncio.get_running_loop().run_in_executor(None, self._load_chain_tickets)
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

    def _load_traded_events(self) -> None:
        """Populate _traded_events from DB so restarts don't re-enter the same event."""
        from sqlmodel import Session, select

        from poly_bot.persistence.db import get_engine
        from poly_bot.persistence.schema import HarvestOrder

        engine = get_engine(self._settings)
        with Session(engine) as session:
            orders = session.exec(
                select(HarvestOrder).where(
                    HarvestOrder.status.in_(["pending", "filled", "won", "lost", "stopped_out"])  # type: ignore[arg-type]
                )
            ).all()
            for o in orders:
                grp = _market_group(o.condition_id, self._settings)
                if grp:
                    self._traded_events.add(grp)
        log.debug("executor: loaded %d traded groups from DB", len(self._traded_events))

    def _load_chain_tickets(self) -> None:
        """Load pending chain tickets from DB, auto-consuming stale ones (order already placed)."""
        from sqlalchemy import text
        from sqlmodel import Session, select

        from poly_bot.persistence.db import get_engine
        from poly_bot.persistence.schema import ChainTicket

        engine = get_engine(self._settings)

        with engine.connect() as conn:
            # Consume stale tickets: pending but matching order already placed.
            stale = conn.execute(text("""
                SELECT ct.id, ho.id
                FROM chain_tickets ct
                JOIN harvest_orders ho
                  ON ho.chain_id = ct.chain_id AND ho.chain_level = ct.chain_level
                WHERE ct.status = 'pending'
                GROUP BY ct.id
            """)).fetchall()

            # Cancel orphan tickets: pending but chain belongs to an archived session
            # (no order for that chain_id with session_id IS NULL).
            conn.execute(text("""
                UPDATE chain_tickets SET status = 'cancelled'
                WHERE status = 'pending'
                  AND chain_id NOT IN (
                    SELECT DISTINCT chain_id FROM harvest_orders
                    WHERE chain_id IS NOT NULL AND session_id IS NULL
                  )
            """))
            conn.commit()

        if stale:
            log.info("executor: consuming %d stale chain tickets on startup", len(stale))
            for ticket_id, order_id in stale:
                _mark_ticket_consumed(ticket_id, order_id, self._settings)

        # Load genuinely unplaced tickets via SQLModel so types (datetime) are correct.
        with Session(engine) as session:
            tickets = session.exec(
                select(ChainTicket).where(ChainTicket.status == "pending")  # type: ignore[arg-type]
            ).all()
            for t in tickets:
                self._pending_chain_tickets.append(ChainTicketData(
                    ticket_id=t.id,
                    chain_id=t.chain_id,
                    chain_level=t.chain_level,
                    size_usd=t.size_usd,
                    created_at=t.created_at,
                ))
        log.debug("executor: loaded %d live chain tickets", len(self._pending_chain_tickets))

    def add_chain_ticket(self, ticket: ChainTicketData) -> None:
        """Add a chain ticket from the reconciler (called from asyncio loop)."""
        self._pending_chain_tickets.append(ticket)
        log.info(
            "chain_ticket queued: chain=%s level=%d size=%.4f",
            ticket.chain_id[:8], ticket.chain_level, ticket.size_usd,
        )

    def release_position(self, condition_id: str) -> None:
        """Remove a resolved position from the open-slot tracker (called by reconciler)."""
        removed = self._open.pop(condition_id, None)
        if removed is not None:
            log.debug("executor: released slot %s (open=%d)", condition_id[:12], len(self._open))

    def _pop_chain_ticket(self) -> ChainTicketData | None:
        if not self._pending_chain_tickets:
            return None
        # highest level first; FIFO at equal level
        self._pending_chain_tickets.sort(key=lambda t: (-t.chain_level, t.created_at))
        return self._pending_chain_tickets.pop(0)

    @property
    def open_positions(self) -> dict[str, HarvestOrder]:
        return self._open

    async def close_position(self, condition_id: str, exit_price: float) -> None:
        """Close an open position at exit_price (stop loss or manual close)."""
        order = self._open.pop(condition_id, None)
        if order is None:
            return
        exit_shares = order.size_usd / order.price_target if order.price_target else 0.0
        pnl = round(exit_shares * exit_price - order.size_usd, 4)
        log.info(
            "close_position %s exit=%.4f pnl=%.4f",
            condition_id[:12], exit_price, pnl,
        )
        await asyncio.get_running_loop().run_in_executor(
            None, _persist_close, order.id, exit_price, pnl, self._settings
        )

    def _open_by_underlying(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for order in self._open.values():
            key = order.underlying or "unknown"
            counts[key] = counts.get(key, 0) + 1
        return counts

    async def _handle(self, c: ExecutionCandidate) -> None:
        market = c.market
        domain = market.domain

        # reject if monitor dropped this market from watchlist after the candidate was emitted
        if market.condition_id in self._cancelled:
            log.info("cancelled_reject %s — watchlist dropped after emit", market.condition_id[:12])
            dlog.append(
                self._settings, dlog.CANCELLED_REJECT,
                condition_id=market.condition_id,
                side=c.side,
                ask=round(c.ask, 4),
            )
            return

        # per-condition guard: one open position per market
        if market.condition_id in self._open:
            return

        # slot cap: applies to all orders (base + chain)
        if len(self._open) >= self._settings.engine_max_open_positions:
            log.debug("risk: max_open_positions=%d reached", self._settings.engine_max_open_positions)  # noqa: E501
            return

        # notional cap: only applies to base orders; chain orders are self-funded from winnings
        has_pending_ticket = bool(self._pending_chain_tickets)
        if not has_pending_ticket:
            base_notional = sum(
                o.size_usd for o in self._open.values() if (o.chain_level or 0) == 0
            )
            if base_notional >= self._settings.engine_max_notional_usd:
                log.debug("risk: base_notional=%.0f >= max=%.0f", base_notional, self._settings.engine_max_notional_usd)  # noqa: E501
                return

        # price band guard — only execute within configured harvest band
        if c.ask < self._settings.harvest_min_buy_price:
            log.debug(
                "band_skip %s ask=%.4f < min=%.4f",
                market.condition_id[:12], c.ask, self._settings.harvest_min_buy_price,
            )
            dlog.append(
                self._settings, dlog.BAND_REJECT,
                condition_id=market.condition_id,
                side=c.side,
                ask=round(c.ask, 4),
                reason=f"ask<min({self._settings.harvest_min_buy_price})",
            )
            return
        if c.ask > self._settings.harvest_max_buy_price:
            log.debug(
                "band_skip %s ask=%.4f > max=%.4f",
                market.condition_id[:12], c.ask, self._settings.harvest_max_buy_price,
            )
            dlog.append(
                self._settings, dlog.BAND_REJECT,
                condition_id=market.condition_id,
                side=c.side,
                ask=round(c.ask, 4),
                reason=f"ask>max({self._settings.harvest_max_buy_price})",
            )
            return

        # depth guard — book must have enough notional at ask to fill the order (paper + live)
        if c.fillable_usd < self._settings.harvest_order_usd:
            log.debug(
                "depth_skip %s fillable=%.2f < order=%.2f",
                market.condition_id[:12], c.fillable_usd, self._settings.harvest_order_usd,
            )
            dlog.append(
                self._settings, dlog.DEPTH_REJECT,
                condition_id=market.condition_id,
                side=c.side,
                ask=round(c.ask, 4),
                fillable_usd=round(c.fillable_usd, 2),
                order_usd=self._settings.harvest_order_usd,
            )
            return

        # domain policy check
        policy = get_policy(domain)
        verdict: PolicyVerdict = policy.check(c, self._open_by_underlying(), self._settings)
        if not verdict.approved:
            log.debug("policy rejected %s: %s", market.condition_id[:12], verdict.reason)
            dlog.append(
                self._settings, dlog.POLICY_REJECT,
                condition_id=market.condition_id,
                side=c.side,
                domain=domain,
                ask=round(c.ask, 4),
                reason=verdict.reason,
            )
            return

        # one order per (event_id, market_type) guard — DB-backed, survives restarts
        group = await asyncio.get_running_loop().run_in_executor(
            None, _market_group, market.condition_id, self._settings
        )
        if group:
            if group in self._traded_events:
                log.debug("group_skip(mem) %s group=%s", market.condition_id[:12], group)
                dlog.append(
                    self._settings, dlog.GROUP_SKIP,
                    condition_id=market.condition_id,
                    side=c.side,
                    group=group,
                    source="mem",
                )
                return
            already = await asyncio.get_running_loop().run_in_executor(
                None, _db_group_has_order, group, self._settings
            )
            if already:
                self._traded_events.add(group)  # sync cache
                log.debug("group_skip(db) %s group=%s", market.condition_id[:12], group)
                dlog.append(
                    self._settings, dlog.GROUP_SKIP,
                    condition_id=market.condition_id,
                    side=c.side,
                    group=group,
                    source="db",
                )
                return

        # ── All guards passed — determine order size and chain metadata ──
        ticket = self._pop_chain_ticket()
        if ticket is not None:
            size_usd = ticket.size_usd
            chain_id = ticket.chain_id
            chain_level = ticket.chain_level
        else:
            # chain cap: limit how many distinct chains can start per session
            if self._settings.engine_max_chains > 0:
                chain_count = await asyncio.get_running_loop().run_in_executor(
                    None, _count_session_chains, self._settings
                )
                if chain_count >= self._settings.engine_max_chains:
                    log.debug(
                        "chain_cap: %d chains >= max %d, skipping new chain",
                        chain_count, self._settings.engine_max_chains,
                    )
                    return
            # re-check base notional after awaits (defensive)
            base_notional = sum(
                o.size_usd for o in self._open.values() if (o.chain_level or 0) == 0
            )
            if base_notional >= self._settings.engine_max_notional_usd:
                log.debug("risk: base_notional=%.0f >= max=%.0f (post-await)", base_notional, self._settings.engine_max_notional_usd)  # noqa: E501
                return
            size_usd = self._settings.harvest_order_usd
            chain_id = str(uuid.uuid4())
            chain_level = 0

        # place order (paper or live)
        order = _build_order(c, size_usd=size_usd, chain_id=chain_id, chain_level=chain_level, paper=self._settings.paper_trading)  # noqa: E501
        try:
            if self._settings.paper_trading:
                order = await self._place_paper(order)
            else:
                order = await self._place_live(order)
        except Exception:
            # placement failed before DB write — return ticket so next candidate can use it
            if ticket is not None:
                self._pending_chain_tickets.append(ticket)
            raise

        if order.status in ("filled", "pending"):
            self._open[market.condition_id] = order
            if group:
                self._traded_events.add(group)
            self._stats.orders_placed += 1
            if order.paper:
                self._stats.orders_paper += 1
            if ticket is not None:
                await asyncio.get_running_loop().run_in_executor(
                    None, _mark_ticket_consumed, ticket.ticket_id, order.id, self._settings
                )
                dlog.append(
                    self._settings, dlog.CHAIN_ORDER_PLACED,
                    chain_id=ticket.chain_id,
                    chain_level=ticket.chain_level,
                    size_usd=round(size_usd, 4),
                    condition_id=market.condition_id,
                    paper=self._settings.paper_trading,
                )
        elif ticket is not None:
            # order failed (live CLOB error) — return ticket to queue so next candidate can use it
            self._pending_chain_tickets.append(ticket)

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
        dlog.append(
            self._settings, dlog.ORDER_PLACED,
            condition_id=order.condition_id,
            token_id=order.token_id,
            side=order.side,
            domain=order.domain,
            ask=order.price_target,
            size_usd=order.size_usd,
            net_yield_pct=order.net_yield_pct,
            mins_left=order.minutes_left_at_entry,
            midpoint_at_entry=order.midpoint_at_entry,
            paper=True,
            question=order.question,
        )
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
            dlog.append(
                self._settings, dlog.ORDER_FAILED,
                condition_id=order.condition_id,
                side=order.side,
                ask=order.price_target,
                reason=str(exc),
                paper=False,
            )

        self._stats.last_order_at = datetime.now(UTC)
        await _persist(order)
        return order


# ------------------------------------------------------------------ helpers

def _build_order(
    c: ExecutionCandidate,
    *,
    size_usd: float,
    chain_id: str,
    chain_level: int,
    paper: bool,
) -> HarvestOrder:
    size_shares = size_usd / c.ask if c.ask > 0 else 0.0
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
        size_usd=size_usd,
        size_shares=size_shares,
        chain_id=chain_id,
        chain_level=chain_level,
        paper=paper,
        status="pending",
        net_yield_pct=c.net_yield_pct,
        minutes_left_at_entry=c.minutes_left,
        midpoint_at_entry=c.mid,
    )


def _db_group_has_order(group: str, settings: Settings) -> bool:
    """Return True if any non-failed order for this event group exists in DB."""
    import re

    from sqlmodel import Session, select

    from poly_bot.persistence.db import get_engine
    from poly_bot.persistence.schema import HarvestOrder, Market

    if ":" not in group:
        return False
    event_id, mtype = group.split(":", 1)

    _TYPE_PATTERNS = {
        "exact_score": re.compile(r"^Exact Score", re.I),
        "over_under":  re.compile(r"^Over/Under|^O/U", re.I),
        "btts":        re.compile(r"^Both Teams", re.I),
        "result":      re.compile(r"^(Will .* win|Match (Winner|Result))", re.I),
    }

    with Session(get_engine(settings)) as session:
        markets = session.exec(
            select(Market).where(Market.event_id == event_id)  # type: ignore[arg-type]
        ).all()
        pattern = _TYPE_PATTERNS.get(mtype)
        cids = [
            m.condition_id for m in markets
            if pattern is None or pattern.search(m.question or "")
        ]
        if not cids:
            return False
        count = session.exec(
            select(HarvestOrder).where(
                HarvestOrder.condition_id.in_(cids),  # type: ignore[arg-type]
                HarvestOrder.status.in_(["pending", "filled", "won", "lost", "stopped_out"]),  # type: ignore[arg-type]
            )
        ).first()
        return count is not None


def _market_group(condition_id: str, settings: Settings) -> str | None:
    """Return a group key (event_id + market type) for one-per-group enforcement."""
    import re

    from sqlmodel import Session

    from poly_bot.persistence.db import get_engine
    from poly_bot.persistence.schema import Market

    _TYPE_PATTERNS = [
        (re.compile(r"^Exact Score", re.I), "exact_score"),
        (re.compile(r"^Over/Under|^O/U", re.I), "over_under"),
        (re.compile(r"^Both Teams", re.I), "btts"),
        (re.compile(r"^(Will .* win|Match (Winner|Result))", re.I), "result"),
    ]

    with Session(get_engine(settings)) as session:
        market = session.get(Market, condition_id)
        if market is None or market.event_id is None:
            return None
        mtype = "other"
        for pattern, label in _TYPE_PATTERNS:
            if pattern.search(market.question or ""):
                mtype = label
                break
        return f"{market.event_id}:{mtype}"


def _persist_close(order_id: str, exit_price: float, pnl: float, settings: Settings) -> None:
    from sqlmodel import Session

    from poly_bot.persistence.db import get_engine
    from poly_bot.persistence.schema import HarvestOrder

    with Session(get_engine(settings)) as session:
        order = session.get(HarvestOrder, order_id)
        if order is None:
            return
        order.status = "stopped_out"
        order.pnl_usd = pnl
        order.resolved_at = datetime.now(UTC)
        session.add(order)
        session.commit()


def _count_session_chains(settings: Settings) -> int:
    """Return number of distinct chain_ids in the current (unarchived) session."""
    from sqlalchemy import text

    from poly_bot.persistence.db import get_engine

    with get_engine(settings).connect() as conn:
        return int(conn.execute(text(
            "SELECT COUNT(DISTINCT chain_id) FROM harvest_orders"
            " WHERE chain_id IS NOT NULL AND session_id IS NULL"
        )).scalar() or 0)


def _mark_ticket_consumed(ticket_id: str, order_id: str, settings: Settings) -> None:
    from sqlmodel import Session

    from poly_bot.persistence.db import get_engine
    from poly_bot.persistence.schema import ChainTicket

    with Session(get_engine(settings)) as session:
        ticket = session.get(ChainTicket, ticket_id)
        if ticket is None:
            return
        ticket.status = "consumed"
        ticket.consumed_by_order_id = order_id
        session.add(ticket)
        session.commit()


async def _persist(order: HarvestOrder) -> None:
    """Write order to DB (non-blocking via thread pool)."""

    def _write() -> None:
        from sqlmodel import Session

        from poly_bot.persistence.db import get_engine
        from poly_bot.settings import Settings

        with Session(get_engine(Settings()), expire_on_commit=False) as session:
            session.add(order)
            session.commit()

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _write)
