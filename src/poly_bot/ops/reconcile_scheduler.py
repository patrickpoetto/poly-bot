"""Periodic reconcile loop — runs as a sidecar service.

Mirrors ``backup_scheduler``: every ``interval_seconds`` it calls
``run_reconciliation`` and persists the result in two places:

* ``catalog_state`` keys ``last_reconcile_at`` / ``last_reconcile_report`` —
  cheap, joins easily on the dashboard.
* Prometheus gauges ``polybot_reconcile_issues{metric=...}`` — surfaces in
  ``/metrics`` so alertmanager / Grafana can page on regressions.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import structlog
from sqlmodel import Session

from poly_bot.metrics import record_reconcile_issues, record_reconcile_run
from poly_bot.ops.reconcile import run_reconciliation
from poly_bot.persistence.catalog_state import (
    KEY_LAST_RECONCILE_AT,
    KEY_LAST_RECONCILE_REPORT,
    CatalogStateStore,
)
from poly_bot.persistence.db import get_engine, init_db
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)


async def run_reconcile_loop(
    settings: Settings,
    *,
    interval_seconds: int = 3600,
) -> None:
    engine = get_engine(settings)
    init_db(engine)
    logger.info("reconcile_loop_start", interval_s=interval_seconds)
    while True:
        try:
            _run_once(engine)
            record_reconcile_run(success=True)
        except Exception:
            logger.exception("reconcile_loop_failed")
            record_reconcile_run(success=False)
        await asyncio.sleep(interval_seconds)


def _run_once(engine: object) -> None:
    now = datetime.now(UTC)
    with Session(engine) as session:  # type: ignore[arg-type]
        report = run_reconciliation(session)
        numeric_issues = {k: v for k, v in report.items() if isinstance(v, int)}
        state = CatalogStateStore(session)
        state.set_datetime(KEY_LAST_RECONCILE_AT, now)
        state.set(KEY_LAST_RECONCILE_REPORT, json.dumps(report, default=str))
        session.commit()
    record_reconcile_issues(numeric_issues)
    logger.info("reconcile_loop_done", **numeric_issues)
