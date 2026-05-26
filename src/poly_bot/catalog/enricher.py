"""REST enrichment for expiring markets — quotes + optional OI."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import structlog
from sqlalchemy.exc import OperationalError
from sqlmodel import Session

from poly_bot.catalog.expiring import (
    conditions_needing_oi_refresh,
    conditions_needing_quote_refresh,
    expiring_condition_ids,
    token_ids_for_conditions,
)
from poly_bot.exchange.clob_public import ClobPublicClient
from poly_bot.exchange.data_public import DataPublicClient
from poly_bot.persistence.schema import MarketEnrichment, Token, TokenQuote
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)


class CatalogEnricher:
    def __init__(self, settings: Settings, session: Session) -> None:
        self._settings = settings
        self._session = session
        self._clob = ClobPublicClient(settings)
        self._data = DataPublicClient(settings)

    async def enrich_expiring_window(
        self,
        *,
        hours: float | None = None,
        force: bool = False,
    ) -> dict[str, int]:
        horizon = hours if hours is not None else self._settings.catalog_expiring_hours
        all_conditions = expiring_condition_ids(self._session, hours=horizon)
        if not all_conditions:
            logger.info("enrich_expiring_empty", hours=horizon)
            return {
                "conditions_total": 0,
                "conditions_quotes": 0,
                "conditions_oi": 0,
                "quotes": 0,
                "enrichment": 0,
                "skipped_fresh": 0,
            }

        stale_seconds = self._settings.catalog_enrich_stale_seconds
        only_stale = self._settings.catalog_enrich_only_stale and not force
        if only_stale:
            quote_conditions = conditions_needing_quote_refresh(
                self._session,
                all_conditions,
                stale_seconds=stale_seconds,
            )
            oi_conditions = (
                conditions_needing_oi_refresh(
                    self._session,
                    all_conditions,
                    stale_seconds=stale_seconds,
                )
                if self._settings.catalog_fetch_open_interest
                else []
            )
        else:
            quote_conditions = list(all_conditions)
            oi_conditions = list(all_conditions)

        now = datetime.now(UTC)
        quotes_written = await self._enrich_quotes(quote_conditions, now)
        enrichment_written = await self._enrich_oi_rows(oi_conditions, now)

        needing = set(quote_conditions) | set(oi_conditions)
        skipped = len(all_conditions) - len(needing)

        logger.info(
            "enrich_expiring_done",
            conditions_total=len(all_conditions),
            conditions_quotes=len(quote_conditions),
            conditions_oi=len(oi_conditions),
            quotes=quotes_written,
            enrichment=enrichment_written,
            skipped_fresh=skipped,
            only_stale=only_stale,
        )
        return {
            "conditions_total": len(all_conditions),
            "conditions_quotes": len(quote_conditions),
            "conditions_oi": len(oi_conditions),
            "quotes": quotes_written,
            "enrichment": enrichment_written,
            "skipped_fresh": skipped,
        }

    async def _enrich_quotes(self, condition_ids: list[str], now: datetime) -> int:
        if not condition_ids:
            return 0
        token_ids_list = token_ids_for_conditions(self._session, condition_ids)
        token_map = self._tokens_by_id(token_ids_list)
        if not token_map:
            return 0

        mids = await self._clob.fetch_midpoints(token_ids_list)
        spreads = await self._clob.fetch_spreads(token_ids_list)

        quotes_written = 0
        batch = 0
        for token in token_map:
            mid = mids.get(token.token_id)
            spread = spreads.get(token.token_id)
            if mid is None and spread is None:
                continue
            row = self._session.get(TokenQuote, token.token_id) or TokenQuote(
                token_id=token.token_id,
                condition_id=token.condition_id,
            )
            row.bid = None
            row.ask = None
            row.mid = mid
            row.spread = spread
            if mid is not None and spread is not None:
                row.bid = max(0.0, mid - spread / 2)
                row.ask = min(1.0, mid + spread / 2)
            row.updated_at = now
            self._session.add(row)
            quotes_written += 1
            batch += 1
            if batch >= 500:
                self._session.commit()
                batch = 0
        self._session.commit()
        return quotes_written

    async def _enrich_oi_rows(self, condition_ids: list[str], now: datetime) -> int:
        if not condition_ids:
            return 0
        if self._settings.catalog_fetch_open_interest:
            return await self._enrich_open_interest(condition_ids, now)
        for cid in condition_ids:
            enrich_row = self._session.get(MarketEnrichment, cid)
            if enrich_row is None:
                enrich_row = MarketEnrichment(condition_id=cid)
            enrich_row.enriched_at = now
            self._session.add(enrich_row)
        self._session.commit()
        return len(condition_ids)

    def _tokens_by_id(self, token_ids: list[str]) -> list[Token]:
        return [t for tid in token_ids if (t := self._session.get(Token, tid)) is not None]

    async def _enrich_open_interest(self, condition_ids: list[str], now: datetime) -> int:
        """Fetch OI concurrently, then persist sequentially in deterministic order.

        Sorting by ``condition_id`` before flush eliminates the lock-ordering
        cycle that previously caused ``DeadlockDetected`` against concurrent
        writers (discovery, ws-daemon). Persistence is retried up to three
        times on transient deadlocks.
        """
        sem = asyncio.Semaphore(self._settings.catalog_oi_concurrency)
        fetched: dict[str, float] = {}

        async def one(cid: str) -> None:
            async with sem:
                try:
                    oi = await self._data.fetch_open_interest(cid)
                except Exception:
                    logger.exception("oi_fetch_failed", condition_id=cid)
                    return
            if oi is None:
                return
            fetched[cid] = oi

        await asyncio.gather(*(one(cid) for cid in condition_ids))
        if not fetched:
            return 0

        ordered = sorted(fetched.items())
        for attempt in range(3):
            try:
                with self._session.no_autoflush:
                    for cid, oi in ordered:
                        enrich_row = self._session.get(MarketEnrichment, cid)
                        if enrich_row is None:
                            enrich_row = MarketEnrichment(condition_id=cid)
                        enrich_row.open_interest = oi
                        enrich_row.enriched_at = now
                        self._session.add(enrich_row)
                self._session.commit()
                return len(ordered)
            except OperationalError as exc:
                self._session.rollback()
                if "deadlock" not in str(exc).lower() or attempt == 2:
                    raise
                logger.warning("oi_deadlock_retry", attempt=attempt + 1)
                await asyncio.sleep(0.5 * (attempt + 1))
        return 0
