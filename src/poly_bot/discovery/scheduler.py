"""Discovery daemon — full scan on interval + delta between runs."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import structlog
from sqlmodel import Session

from poly_bot.catalog.linker import CatalogLinker
from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.metrics import record_discovery_cycle
from poly_bot.persistence.catalog_state import (
    KEY_LAST_DELTA_SCAN_AT,
    KEY_LAST_FULL_SCAN_AT,
    KEY_LAST_SERIES_SCAN_AT,
    CatalogStateStore,
)
from poly_bot.persistence.db import get_engine, init_db
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)


class DiscoveryScheduler:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._runner = DiscoveryRunner(settings)
        self._engine = get_engine(settings)

    def init_database(self) -> None:
        init_db(self._engine)

    async def run_cycle(self, *, force_full: bool = False) -> dict[str, int | str]:
        with Session(self._engine) as session:
            state = CatalogStateStore(session)
            last_full = state.get_datetime(KEY_LAST_FULL_SCAN_AT)
            full_interval = timedelta(
                minutes=self._settings.discovery_full_scan_interval_minutes
            )
            need_full = force_full or last_full is None or (
                datetime.now(UTC) - last_full >= full_interval
            )

        stats: dict[str, int | str]
        if need_full:
            logger.info("discovery_cycle_full")
            stats = dict(
                await self._runner.scan_catalog(
                    markets=True,
                    events=True,
                    series=True,
                    include_closed=True,
                )
            )
            stats["cycle"] = "full"
            with Session(self._engine) as session:
                state = CatalogStateStore(session)
                now = datetime.now(UTC)
                state.set_datetime(KEY_LAST_FULL_SCAN_AT, now)
                state.set_datetime(KEY_LAST_DELTA_SCAN_AT, now)
                state.set_datetime(KEY_LAST_SERIES_SCAN_AT, now)
                session.commit()
        else:
            logger.info("discovery_cycle_delta")
            stats = dict(await self._runner.scan_delta())
            stats["cycle"] = "delta"
            with Session(self._engine) as session:
                state = CatalogStateStore(session)
                state.set_datetime(KEY_LAST_DELTA_SCAN_AT, datetime.now(UTC))
                session.commit()

            await self._maybe_refresh_series(stats)
            await self._maybe_refresh_series_on_demand(stats)

        with Session(self._engine) as session:
            link_stats = CatalogLinker(session).link_all()
            from poly_bot.persistence.repo import CatalogRepository

            fees_rolled = CatalogRepository(session).rollup_event_fees()

        stats["link_total"] = sum(link_stats.values())
        stats["event_fees_rolled"] = fees_rolled
        record_discovery_cycle(str(stats.get("cycle", "unknown")), stats)
        return stats

    async def _maybe_refresh_series(self, stats: dict[str, int | str]) -> None:
        interval = timedelta(minutes=self._settings.discovery_series_refresh_interval_minutes)
        with Session(self._engine) as session:
            state = CatalogStateStore(session)
            last_series = state.get_datetime(KEY_LAST_SERIES_SCAN_AT)
            if last_series is not None and datetime.now(UTC) - last_series < interval:
                return

        await self._refresh_series_now(stats, reason="interval")

    async def _maybe_refresh_series_on_demand(self, stats: dict[str, int | str]) -> None:
        """Trigger a series scan if the delta brought in events with unknown series.

        Gamma has no delta endpoint for series, but new event payloads carry a
        ``series_id``. Whenever the join yields any orphan, we refresh — bounded
        by a 30s cool-down (``KEY_LAST_SERIES_SCAN_AT``) so a noisy delta does
        not hammer the endpoint.
        """
        cooldown = timedelta(seconds=30)
        with Session(self._engine) as session:
            state = CatalogStateStore(session)
            last_series = state.get_datetime(KEY_LAST_SERIES_SCAN_AT)
            if last_series is not None and datetime.now(UTC) - last_series < cooldown:
                return

        try:
            orphans = self._runner.count_unknown_event_series()
        except Exception:
            logger.exception("series_orphan_count_failed")
            return
        if orphans == 0:
            return

        logger.info("discovery_series_refresh_on_demand", orphans=orphans)
        await self._refresh_series_now(stats, reason="on_demand", orphans=orphans)

    async def _refresh_series_now(
        self, stats: dict[str, int | str], *, reason: str, orphans: int | None = None
    ) -> None:
        logger.info("discovery_series_refresh", reason=reason, orphans=orphans)
        series_stats = await self._runner.scan_series()
        stats["series_upserted"] = stats.get(
            "series_upserted", 0
        ) + series_stats.get("series_upserted", 0)  # type: ignore[operator]
        with Session(self._engine) as session:
            CatalogStateStore(session).set_datetime(
                KEY_LAST_SERIES_SCAN_AT,
                datetime.now(UTC),
            )
            session.commit()

    async def run_forever(self) -> None:
        interval = self._settings.discovery_delta_scan_interval_seconds
        logger.info(
            "discovery_scheduler_start",
            delta_interval=interval,
            full_minutes=self._settings.discovery_full_scan_interval_minutes,
            series_refresh_minutes=self._settings.discovery_series_refresh_interval_minutes,
        )
        while True:
            try:
                stats = await self.run_cycle()
                logger.info("discovery_scheduler_cycle", **stats)
            except Exception:
                logger.exception("discovery_scheduler_cycle_failed")
            await asyncio.sleep(interval)
