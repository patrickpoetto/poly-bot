"""Unified runtime — discovery scheduler + catalog watch + persistent WS."""

from __future__ import annotations

import asyncio

import structlog

from poly_bot.catalog.watch import CatalogWatch
from poly_bot.catalog.ws_daemon import PersistentWSDaemon
from poly_bot.discovery.scheduler import DiscoveryScheduler
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)


async def run_platform(
    settings: Settings,
    *,
    catalog_hours: float | None = None,
) -> None:
    """Run discovery + live catalog + persistent WS loops concurrently."""
    discovery = DiscoveryScheduler(settings)
    watch = CatalogWatch(settings)
    discovery.init_database()
    watch.init_database()

    hours = catalog_hours if catalog_hours is not None else settings.catalog_expiring_hours
    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(discovery.run_forever()),
        asyncio.create_task(watch.run_forever(hours=hours)),
    ]
    if settings.catalog_ws_enabled and settings.catalog_ws_persistent:
        ws_daemon = PersistentWSDaemon(
            settings,
            hours=hours,
            refresh_seconds=settings.catalog_ws_refresh_seconds,
        )
        tasks.append(asyncio.create_task(ws_daemon.run_forever()))

    logger.info(
        "platform_run_start",
        catalog_hours=hours,
        ws_persistent=settings.catalog_ws_persistent and settings.catalog_ws_enabled,
        task_count=len(tasks),
    )
    await asyncio.gather(*tasks)
