"""Periodic backup loop with retention — runs as a sidecar service."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import structlog

from poly_bot.ops.backup import backup_catalog
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)


async def run_backup_loop(
    settings: Settings,
    *,
    interval_seconds: int = 3600,
    retention_days: int = 7,
    dest_dir: Path | None = None,
) -> None:
    out_dir = dest_dir or (settings.poly_bot_data_dir / "backups")
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info(
        "backup_loop_start",
        interval_s=interval_seconds,
        retention_days=retention_days,
        dest=str(out_dir),
        backend="postgres" if settings.uses_postgres else "sqlite",
    )
    while True:
        try:
            path = backup_catalog(settings, dest_dir=out_dir)
            size_kb = path.stat().st_size / 1024
            logger.info("backup_loop_wrote", path=str(path), size_kb=round(size_kb, 1))
            _prune_old_backups(out_dir, retention_days=retention_days)
        except Exception:
            logger.exception("backup_loop_failed")
        await asyncio.sleep(interval_seconds)


def _prune_old_backups(dest_dir: Path, *, retention_days: int) -> int:
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    removed = 0
    for path in dest_dir.glob("catalog_*"):
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        except OSError:
            continue
        if mtime < cutoff:
            try:
                path.unlink()
                removed += 1
            except OSError:
                logger.warning("backup_prune_failed", path=str(path))
    if removed:
        logger.info("backup_pruned", removed=removed, retention_days=retention_days)
    return removed
