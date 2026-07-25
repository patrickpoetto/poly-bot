"""Background reconciler — resolves filled paper orders against actual market outcomes.

Runs every 60s. For each filled-but-unresolved order whose market end_ts has passed
(plus 10min grace for settlement), reads the resolution from the DB itself:

  raw_payload['outcomePrices'] → ["0","1"] or ["1","0"]
  Token.outcome_index → maps index to token_id
  winning token = where outcomePrices[i] == "1"

No external API calls; uses data already written by the discovery/reconcile background tasks.

Outcome:
  - our token won  → status='won',  pnl_usd = size_usd * net_yield_pct  (confirmed)
  - our token lost → status='lost', pnl_usd = -size_usd
  - market not resolved yet → skip, retry next cycle
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from poly_bot.harvest.engine import decision_log as dlog
from poly_bot.harvest.engine.state import ChainTicketData
from poly_bot.settings import Settings

log = logging.getLogger(__name__)

_RECONCILE_INTERVAL_S = 60.0
_RESOLUTION_GRACE_MINUTES = 10


class OrderReconciler:
    def __init__(
        self,
        settings: Settings,
        on_chain_win: Callable[[ChainTicketData], None] | None = None,
        on_order_resolved: Callable[[str], None] | None = None,
    ) -> None:
        self._settings = settings
        self._on_chain_win = on_chain_win
        self._on_order_resolved = on_order_resolved

    async def run(self) -> None:
        """Runs until cancelled."""
        while True:
            try:
                await self._reconcile_batch()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("reconciler error: %s", exc)
            await asyncio.sleep(_RECONCILE_INTERVAL_S)

    # ------------------------------------------------------------------ private

    async def _reconcile_batch(self) -> None:
        orders = await asyncio.get_running_loop().run_in_executor(None, self._load_pending)
        if not orders:
            return

        by_market: dict[str, list[_PendingRow]] = {}
        for o in orders:
            by_market.setdefault(o.condition_id, []).append(o)

        log.debug("reconciler: %d markets / %d orders to check", len(by_market), len(orders))

        for cid, market_orders in by_market.items():
            winner_token = await asyncio.get_running_loop().run_in_executor(
                None, self._find_winner_in_db, cid
            )
            if winner_token is None:
                log.debug("reconciler: %s not yet resolved in DB", cid[:12])
                continue

            for row in market_orders:
                won = row.token_id == winner_token
                ticket = await asyncio.get_running_loop().run_in_executor(
                    None, self._persist_resolution, row.order_id, won
                )
                log.info(
                    "reconciled %s: %s ask=%.4f yield=%.2f%%",
                    row.order_id[:8],
                    "WON" if won else "LOST",
                    row.price_target,
                    (row.net_yield_pct or 0.0) * 100,
                )
                if self._on_order_resolved is not None:
                    self._on_order_resolved(row.condition_id)
                if ticket is not None and self._on_chain_win is not None:
                    self._on_chain_win(ticket)

    def _load_pending(self) -> list[_PendingRow]:
        """Return filled unresolved orders whose market end_ts + grace has passed."""
        from sqlmodel import Session, select

        from poly_bot.persistence.db import get_engine
        from poly_bot.persistence.schema import HarvestOrder, Market

        engine = get_engine(self._settings)
        cutoff = datetime.now(UTC) - timedelta(minutes=_RESOLUTION_GRACE_MINUTES)

        with Session(engine) as session:
            pending = session.exec(
                select(HarvestOrder).where(
                    HarvestOrder.status == "filled",  # type: ignore[arg-type]
                    HarvestOrder.resolved_at.is_(None),  # type: ignore[attr-defined]
                )
            ).all()

            result: list[_PendingRow] = []
            for o in pending:
                # prefer market's actual end_ts from DB
                market = session.get(Market, o.condition_id)
                if market is not None and market.end_ts is not None:
                    end_ts = market.end_ts
                    if end_ts.tzinfo is None:
                        end_ts = end_ts.replace(tzinfo=UTC)
                    threshold = end_ts
                else:
                    # fallback: created_at + duration_s (or 2h)
                    created = o.created_at
                    if created.tzinfo is None:
                        created = created.replace(tzinfo=UTC)
                    threshold = created + timedelta(seconds=o.duration_s or 7200)

                if threshold < cutoff:
                    result.append(
                        _PendingRow(
                            order_id=o.id,
                            condition_id=o.condition_id,
                            token_id=o.token_id,
                            price_target=o.price_target,
                            net_yield_pct=o.net_yield_pct,
                            size_usd=o.size_usd,
                            chain_id=o.chain_id,
                            chain_level=o.chain_level or 0,
                        )
                    )
        return result

    def _find_winner_in_db(self, condition_id: str) -> str | None:
        """Return winning token_id from DB raw_payload, or None if not yet settled."""
        from sqlmodel import Session, select

        from poly_bot.persistence.db import get_engine
        from poly_bot.persistence.schema import Market, Token

        engine = get_engine(self._settings)
        with Session(engine) as session:
            market = session.get(Market, condition_id)
            if market is None or not market.closed:
                return None

            prices = market.raw_payload.get("outcomePrices")
            if isinstance(prices, str):
                import json as _json
                try:
                    prices = _json.loads(prices)
                except Exception:
                    return None
            if not prices or not isinstance(prices, list):
                return None

            # winning index = where price ≈ 1.0
            winning_index: int | None = None
            for i, p in enumerate(prices):
                try:
                    if float(p) >= 0.99:
                        winning_index = i
                        break
                except (TypeError, ValueError):
                    continue

            if winning_index is None:
                return None  # ambiguous / not yet settled

            tokens = session.exec(
                select(Token).where(Token.condition_id == condition_id)  # type: ignore[arg-type]
            ).all()
            winner = next((t for t in tokens if t.outcome_index == winning_index), None)
            return winner.token_id if winner else None

    def _persist_resolution(self, order_id: str, won: bool) -> ChainTicketData | None:
        from sqlmodel import Session, select

        from poly_bot.persistence.db import get_engine
        from poly_bot.persistence.schema import ChainTicket, HarvestOrder

        engine = get_engine(self._settings)
        with Session(engine) as session:
            order = session.get(HarvestOrder, order_id)
            if order is None:
                return None
            order.status = "won" if won else "lost"
            order.pnl_usd = (
                round(order.size_usd * (order.net_yield_pct or 0.0), 4)
                if won
                else -round(order.size_usd, 4)
            )
            order.resolved_at = datetime.now(UTC)
            session.add(order)

            chain_ticket_data: ChainTicketData | None = None
            if won and self._settings.engine_chain_enabled and order.chain_id:
                # skip if dashboard "close chain" wrote a closing sentinel
                closing = session.exec(
                    select(ChainTicket).where(
                        ChainTicket.chain_id == order.chain_id,  # type: ignore[arg-type]
                        ChainTicket.status == "cancelled",  # type: ignore[arg-type]
                    )
                ).first()
                if closing is None:
                    next_level = (order.chain_level or 0) + 1
                    next_size = round(order.size_usd * (1.0 + (order.net_yield_pct or 0.0)), 4)
                    ticket = ChainTicket(
                        id=str(uuid.uuid4()),
                        chain_id=order.chain_id,
                        chain_level=next_level,
                        size_usd=next_size,
                        parent_order_id=order.id,
                    )
                    session.add(ticket)
                    chain_ticket_data = ChainTicketData(
                        ticket_id=ticket.id,
                        chain_id=ticket.chain_id,
                        chain_level=ticket.chain_level,
                        size_usd=ticket.size_usd,
                        created_at=ticket.created_at,
                    )
                    dlog.append(
                        self._settings, dlog.CHAIN_TICKET_CREATED,
                        chain_id=ticket.chain_id,
                        chain_level=ticket.chain_level,
                        size_usd=next_size,
                        parent_order_id=order.id,
                    )

            session.commit()
            return chain_ticket_data


class _PendingRow:
    __slots__ = (  # noqa: RUF023
        "order_id",
        "condition_id",
        "token_id",
        "price_target",
        "net_yield_pct",
        "size_usd",
        "chain_id",
        "chain_level",
    )

    def __init__(
        self,
        order_id: str,
        condition_id: str,
        token_id: str,
        price_target: float,
        net_yield_pct: float | None,
        size_usd: float,
        chain_id: str | None,
        chain_level: int,
    ) -> None:
        self.order_id = order_id
        self.condition_id = condition_id
        self.token_id = token_id
        self.price_target = price_target
        self.net_yield_pct = net_yield_pct
        self.size_usd = size_usd
        self.chain_id = chain_id
        self.chain_level = chain_level
