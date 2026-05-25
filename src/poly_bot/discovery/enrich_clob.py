"""Backfill tick/fees from CLOB for markets missing Gamma fields."""

from __future__ import annotations

import asyncio

import structlog
from sqlmodel import Session, select

from poly_bot.discovery.fees import clob_market_fee_fields
from poly_bot.exchange.clob_public import ClobPublicClient
from poly_bot.persistence.schema import Market
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)


class ClobMarketEnricher:
    def __init__(self, settings: Settings, session: Session) -> None:
        self._settings = settings
        self._session = session
        self._clob = ClobPublicClient(settings)

    async def enrich_missing_tick_size(
        self,
        *,
        limit: int = 500,
        concurrency: int = 20,
    ) -> int:
        """Fetch CLOB /markets for rows without tick_size (cap at ``limit``)."""
        rows = self._session.exec(
            select(Market)
            .where(Market.active == True)  # noqa: E712
            .where(Market.closed == False)  # noqa: E712
            .where((Market.tick_size == None) | (Market.tick_size == ""))  # noqa: E711
            .limit(limit)
        ).all()
        if not rows:
            return 0

        sem = asyncio.Semaphore(concurrency)
        updated = 0

        async def one(market: Market) -> None:
            nonlocal updated
            async with sem:
                raw = await self._clob.fetch_market(market.condition_id)
            if not raw:
                return
            fee = clob_market_fee_fields(raw)
            changed = False
            if fee.get("tick_size") and not market.tick_size:
                market.tick_size = fee["tick_size"]
                changed = True
            if fee.get("min_order_size") and not market.min_order_size:
                market.min_order_size = fee["min_order_size"]
                changed = True
            if market.maker_base_fee is None and fee.get("maker_base_fee") is not None:
                market.maker_base_fee = fee["maker_base_fee"]
                changed = True
            if market.taker_base_fee is None and fee.get("taker_base_fee") is not None:
                market.taker_base_fee = fee["taker_base_fee"]
                changed = True
            if fee.get("fees_enabled"):
                market.fees_enabled = True
                changed = True
            if changed:
                self._session.add(market)
                updated += 1

        await asyncio.gather(*(one(m) for m in rows))
        self._session.commit()
        logger.info("clob_tick_enrich_done", candidates=len(rows), updated=updated)
        return updated
