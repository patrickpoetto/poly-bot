"""Discovery orchestrator — markets, events, series."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

import structlog
from sqlmodel import Session

from poly_bot.discovery.crawler.gamma import GammaClient
from poly_bot.discovery.enrich_clob import ClobMarketEnricher
from poly_bot.discovery.mapper import (
    gamma_event_to_entity,
    gamma_market_enrichment_fields,
    gamma_market_to_entities,
    gamma_series_to_entity,
)
from poly_bot.persistence.catalog_state import (
    KEY_EVENTS_DELTA_WATERMARK,
    KEY_MARKETS_DELTA_WATERMARK,
    CatalogStateStore,
)
from poly_bot.persistence.db import get_engine, init_db
from poly_bot.persistence.repo import CatalogRepository
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)

T = TypeVar("T")


class DiscoveryRunner:
    """Run catalog scans and persist to SQLite."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._gamma = GammaClient(settings)
        self._engine = get_engine(settings)

    def init_database(self) -> None:
        init_db(self._engine)

    async def scan_active(self) -> dict[str, int]:
        """Full crawl of all active (non-closed) markets."""
        stats = await self._scan_markets(
            run_type="full_active",
            source=self._gamma.iter_active_markets(),
        )
        await self._finalize_scan_watermarks()
        return stats

    async def bootstrap_closed(self) -> dict[str, int]:
        """One-shot import of recently closed markets (lookback from settings)."""
        days = self._settings.discovery_closed_lookback_days
        logger.info("bootstrap_closed_start", lookback_days=days)
        return await self._scan_markets(
            run_type="bootstrap_closed",
            source=self._gamma.iter_recently_closed_markets(days),
        )

    async def scan_events(self, *, include_closed: bool = True) -> dict[str, int]:
        """Crawl active events; optionally include closed lookback window."""
        active = await self._scan_records(
            run_type="full_active_events",
            source=self._gamma.iter_active_events(),
            mapper=gamma_event_to_entity,
            upsert=lambda repo, entity: repo.upsert_event(entity),
            entity_label="events",
        )
        if not include_closed:
            active["series_backfilled"] = self._backfill_series_from_events()
            return active

        days = self._settings.discovery_closed_lookback_days
        closed = await self._scan_records(
            run_type="bootstrap_closed_events",
            source=self._gamma.iter_recently_closed_events(days),
            mapper=gamma_event_to_entity,
            upsert=lambda repo, entity: repo.upsert_event(entity),
            entity_label="events",
        )
        merged = _merge_stats(active, closed)
        merged["series_backfilled"] = self._backfill_series_from_events()
        return merged

    async def scan_series(self) -> dict[str, int]:
        """Crawl all series definitions from Gamma."""
        return await self._scan_records(
            run_type="full_series",
            source=self._gamma.iter_series(),
            mapper=gamma_series_to_entity,
            upsert=lambda repo, entity: repo.upsert_series(entity),
            entity_label="series",
            stat_key="series_upserted",
        )

    async def scan_taxonomy(self, *, include_closed_events: bool = True) -> dict[str, int]:
        """Events + series (no markets), then link series_id on markets."""
        series = await self.scan_series()
        events = await self.scan_events(include_closed=include_closed_events)
        return _merge_stats(series, events)

    def _backfill_series_from_events(self) -> int:
        with Session(self._engine) as session:
            n = CatalogRepository(session).backfill_market_series_from_events()
        logger.info("series_backfill_complete", markets_updated=n)
        return n

    async def scan_all(self) -> dict[str, int]:
        """Active markets + closed lookback bootstrap."""
        active_stats = await self.scan_active()
        closed_stats = await self.bootstrap_closed()
        merged = _merge_stats(active_stats, closed_stats)
        await self._finalize_scan_watermarks()
        return merged

    async def scan_delta(self) -> dict[str, int]:
        """Incremental scan: markets/events updated since last watermark."""
        overlap = timedelta(seconds=60)

        with Session(self._engine) as session:
            state = CatalogStateStore(session)
            since_markets = state.get_watermark(KEY_MARKETS_DELTA_WATERMARK) - overlap
            since_events = state.get_watermark(KEY_EVENTS_DELTA_WATERMARK) - overlap

        market_stats = await self._scan_markets(
            run_type="delta_markets",
            source=self._gamma.iter_active_markets_delta(since_markets),
        )
        event_stats = await self._scan_records(
            run_type="delta_events",
            source=self._gamma.iter_active_events_delta(since_events),
            mapper=gamma_event_to_entity,
            upsert=lambda repo, entity: repo.upsert_event(entity),
            entity_label="events",
        )
        merged = _merge_stats(market_stats, event_stats)
        merged["series_backfilled"] = self._backfill_series_from_events()

        with Session(self._engine) as session:
            CatalogRepository(session).rollup_event_fees()
            state = CatalogStateStore(session)
            state.set_watermark_now(KEY_MARKETS_DELTA_WATERMARK)
            state.set_watermark_now(KEY_EVENTS_DELTA_WATERMARK)
            session.commit()

        with Session(self._engine) as session:
            clob_n = await ClobMarketEnricher(self._settings, session).enrich_missing_tick_size(
                limit=200
            )
        merged["clob_tick_enriched"] = clob_n
        return merged

    async def _finalize_scan_watermarks(self, *, session: Session | None = None) -> None:
        if session is not None:
            state = CatalogStateStore(session)
            state.set_watermark_now(KEY_MARKETS_DELTA_WATERMARK)
            state.set_watermark_now(KEY_EVENTS_DELTA_WATERMARK)
            return
        with Session(self._engine) as sess:
            state = CatalogStateStore(sess)
            state.set_watermark_now(KEY_MARKETS_DELTA_WATERMARK)
            state.set_watermark_now(KEY_EVENTS_DELTA_WATERMARK)
            sess.commit()

    async def scan_catalog(
        self,
        *,
        markets: bool = True,
        events: bool = True,
        series: bool = True,
        include_closed: bool = True,
    ) -> dict[str, int]:
        """Full catalog refresh: markets, events, and series."""
        totals: dict[str, int] = {
            "markets_upserted": 0,
            "events_upserted": 0,
            "series_upserted": 0,
            "pages_fetched": 0,
            "errors": 0,
        }
        if series:
            totals = _merge_stats(totals, await self.scan_series())
        if events:
            totals = _merge_stats(
                totals,
                await self.scan_events(include_closed=include_closed),
            )
        if markets:
            market_stats = (
                await self.scan_all() if include_closed else await self.scan_active()
            )
            totals = _merge_stats(totals, market_stats)
        with Session(self._engine) as session:
            CatalogRepository(session).rollup_event_fees()
        return totals

    async def _scan_markets(
        self,
        *,
        run_type: str,
        source: AsyncIterator[dict[str, Any]],
    ) -> dict[str, int]:
        markets_upserted = 0
        pages_fetched = 0
        errors = 0

        with Session(self._engine) as session:
            from poly_bot.catalog.taxonomy import TaxonomyOverrideStore

            repo = CatalogRepository(session)
            run = repo.start_run(run_type)
            overrides = TaxonomyOverrideStore.load(session)

            try:
                async for raw in source:
                    pages_fetched += 1
                    try:
                        condition_id = raw.get("conditionId") or raw.get("condition_id")
                        if not condition_id:
                            continue
                        market, tokens, tags = gamma_market_to_entities(raw, overrides=overrides)
                        repo.upsert_market(market)
                        session.flush()
                        repo.replace_tokens(market.condition_id, tokens)
                        repo.replace_market_tags(market.condition_id, tags)
                        enrich = gamma_market_enrichment_fields(raw)
                        repo.upsert_market_enrichment(
                            market.condition_id,
                            volume_24h=enrich.get("volume_24h"),
                            fee_rate_bps=enrich.get("fee_rate_bps"),
                            maker_base_fee=enrich.get("maker_base_fee"),
                            taker_base_fee=enrich.get("taker_base_fee"),
                            rewards_min_size=enrich.get("rewards_min_size"),
                            rewards_max_spread=enrich.get("rewards_max_spread"),
                        )
                        markets_upserted += 1
                        if markets_upserted % 500 == 0:
                            session.commit()
                            logger.info(
                                "scan_progress",
                                run_type=run_type,
                                markets=markets_upserted,
                            )
                    except Exception:
                        errors += 1
                        logger.exception(
                            "market_upsert_failed",
                            condition_id=raw.get("conditionId"),
                        )

                session.commit()
                repo.finish_run(
                    run,
                    markets_upserted=markets_upserted,
                    pages_fetched=pages_fetched,
                    errors=errors,
                )
            except Exception as exc:
                session.rollback()
                repo.finish_run(
                    run,
                    markets_upserted=markets_upserted,
                    pages_fetched=pages_fetched,
                    errors=errors + 1,
                    notes=str(exc),
                )
                raise

        logger.info(
            "scan_complete",
            run_type=run_type,
            markets=markets_upserted,
            errors=errors,
        )
        return {
            "markets_upserted": markets_upserted,
            "pages_fetched": pages_fetched,
            "errors": errors,
        }

    async def _scan_records(
        self,
        *,
        run_type: str,
        source: AsyncIterator[dict[str, Any]],
        mapper: Callable[[dict[str, Any]], T | None],
        upsert: Callable[[CatalogRepository, T], None],
        entity_label: str,
        stat_key: str | None = None,
    ) -> dict[str, int]:
        stat_key = stat_key or f"{entity_label}_upserted"
        upserted = 0
        pages_fetched = 0
        errors = 0

        with Session(self._engine) as session:
            repo = CatalogRepository(session)
            run = repo.start_run(run_type)

            try:
                async for raw in source:
                    pages_fetched += 1
                    try:
                        entity = mapper(raw)
                        if entity is None:
                            continue
                        upsert(repo, entity)
                        upserted += 1
                        if upserted % 500 == 0:
                            session.commit()
                            logger.info(
                                "scan_progress",
                                run_type=run_type,
                                entity=entity_label,
                                count=upserted,
                            )
                    except Exception:
                        errors += 1
                        logger.exception(
                            f"{entity_label}_upsert_failed",
                            record_id=raw.get("id"),
                        )

                session.commit()
                if stat_key == "series_upserted":
                    repo.finish_run(
                        run,
                        series_upserted=upserted,
                        pages_fetched=pages_fetched,
                        errors=errors,
                    )
                else:
                    repo.finish_run(
                        run,
                        events_upserted=upserted,
                        pages_fetched=pages_fetched,
                        errors=errors,
                    )
            except Exception as exc:
                session.rollback()
                repo.finish_run(
                    run,
                    pages_fetched=pages_fetched,
                    errors=errors + 1,
                    notes=str(exc),
                )
                raise

        logger.info(
            "scan_complete",
            run_type=run_type,
            entity=entity_label,
            upserted=upserted,
            errors=errors,
        )
        result: dict[str, int] = {
            "pages_fetched": pages_fetched,
            "errors": errors,
        }
        result[stat_key] = upserted
        return result

    def stats(self) -> dict[str, int]:
        with Session(self._engine) as session:
            repo = CatalogRepository(session)
            return {
                "total_markets": repo.count_markets(active_only=False),
                "active_markets": repo.count_markets(active_only=True),
                "total_events": repo.count_events(active_only=False),
                "active_events": repo.count_events(active_only=True),
                "total_series": repo.count_series(),
                "markets_with_event_id": repo.count_markets_with_event(),
                "markets_with_series_id": repo.count_markets_with_series(),
                "markets_with_fees": repo.count_markets_with_fees(),
                "events_with_fees": repo.count_events_with_fees(),
            }


def _merge_stats(a: dict[str, int], b: dict[str, int]) -> dict[str, int]:
    merged = dict(a)
    for k, v in b.items():
        merged[k] = merged.get(k, 0) + v
    return merged


def run_scan_sync(settings: Settings, *, bootstrap_closed: bool = True) -> dict[str, int]:
    """Synchronous entry for CLI (markets only)."""
    runner = DiscoveryRunner(settings)
    runner.init_database()
    if bootstrap_closed:
        return asyncio.run(runner.scan_all())
    return asyncio.run(runner.scan_active())
