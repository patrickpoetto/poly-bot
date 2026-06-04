"""Live catalog daemon — link, enrich, stream quotes for expiring window."""

from __future__ import annotations

import asyncio

import structlog
from sqlmodel import Session

from poly_bot.catalog.enricher import CatalogEnricher
from poly_bot.catalog.expiring import subscription_condition_ids, token_ids_for_conditions
from poly_bot.catalog.linker import CatalogLinker
from poly_bot.catalog.live_ws import LiveQuoteStream
from poly_bot.persistence.db import get_engine, init_db
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)


class CatalogWatch:
    """Keep expiring-market slice fresh (REST + optional WS)."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._engine = get_engine(settings)

    def init_database(self) -> None:
        init_db(self._engine)

    async def run_cycle(self, *, hours: float | None = None) -> dict[str, int]:
        horizon = hours if hours is not None else self._settings.catalog_quote_horizon_hours
        stats: dict[str, int] = {}

        with Session(self._engine) as session:
            linker = CatalogLinker(session)
            stats.update(linker.link_all())

            enricher = CatalogEnricher(self._settings, session)
            stats.update(await enricher.enrich_expiring_window(hours=horizon))

            # Persistent WS daemon runs separately when CATALOG_WS_PERSISTENT=true.
            if (
                self._settings.catalog_ws_enabled
                and not self._settings.catalog_ws_persistent
            ):
                cids = subscription_condition_ids(
                    session,
                    self._settings,
                    hours=horizon,
                )
                tids = token_ids_for_conditions(session, cids)
                ws = LiveQuoteStream(self._settings, session)
                ws_seconds = min(
                    self._settings.catalog_enrich_interval_seconds,
                    55.0,
                )
                stats["ws_messages"] = await ws.run(tids, duration_seconds=ws_seconds)

        stats["expiring_count"] = self.count_expiring(hours=horizon)
        return stats

    async def run_forever(self, *, hours: float | None = None) -> None:
        interval = self._settings.catalog_enrich_interval_seconds
        cycle_timeout = max(interval * 5, 300.0)
        effective_hours = hours if hours is not None else self._settings.catalog_quote_horizon_hours
        logger.info(
            "catalog_watch_start",
            interval=interval,
            hours=effective_hours,
            cycle_timeout=cycle_timeout,
        )
        while True:
            try:
                stats = await asyncio.wait_for(
                    self.run_cycle(hours=hours), timeout=cycle_timeout
                )
                logger.info("catalog_watch_cycle", **stats)
            except TimeoutError:
                logger.error("catalog_watch_cycle_timeout", timeout=cycle_timeout)
            except Exception:
                logger.exception("catalog_watch_cycle_failed")
            await asyncio.sleep(interval)

    def count_expiring(self, *, hours: float) -> int:
        from poly_bot.catalog.expiring import count_expiring_markets

        with Session(self._engine) as session:
            return count_expiring_markets(
                session,
                hours=hours,
                sports_in_play_hours=self._settings.catalog_sports_in_play_hours,
            )
