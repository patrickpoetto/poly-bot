"""Prometheus metrics (optional — requires prometheus-client)."""

from __future__ import annotations

from typing import Any

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False
    CONTENT_TYPE_LATEST = "text/plain"

if _AVAILABLE:
    DISCOVERY_CYCLES = Counter(
        "polybot_discovery_cycles_total",
        "Discovery scheduler cycles",
        ["cycle_type"],
    )
    DISCOVERY_UPSERTS = Counter(
        "polybot_discovery_upserts_total",
        "Records upserted by discovery",
        ["entity"],
    )
    CATALOG_ENRICH = Counter(
        "polybot_catalog_enrich_total",
        "Catalog enrichment operations",
        ["kind"],
    )
    MARKETS_ACTIVE = Gauge("polybot_markets_active", "Active markets in catalog")
    MARKETS_WITH_FEES = Gauge("polybot_markets_with_fees", "Markets with fees enabled")
    EXPIRING_6H = Gauge("polybot_expiring_6h", "Markets expiring within 6 hours")
    RECONCILE_ISSUES = Gauge(
        "polybot_reconcile_issues",
        "Outstanding data-quality issues per metric",
        ["metric"],
    )
    RECONCILE_RUNS = Counter(
        "polybot_reconcile_runs_total",
        "Reconcile loop iterations (outcome: ok or failed)",
        ["outcome"],
    )


def metrics_enabled() -> bool:
    return _AVAILABLE


def record_discovery_cycle(cycle_type: str, stats: dict[str, int | str]) -> None:
    if not _AVAILABLE:
        return
    DISCOVERY_CYCLES.labels(cycle_type=cycle_type).inc()
    for key, label in (
        ("markets_upserted", "markets"),
        ("events_upserted", "events"),
        ("series_upserted", "series"),
    ):
        val = stats.get(key)
        if isinstance(val, int) and val:
            DISCOVERY_UPSERTS.labels(entity=label).inc(val)


def record_catalog_enrich(kind: str, count: int = 1) -> None:
    if _AVAILABLE and count:
        CATALOG_ENRICH.labels(kind=kind).inc(count)


def set_catalog_gauges(
    *,
    active_markets: int,
    markets_with_fees: int,
    expiring_6h: int,
) -> None:
    if not _AVAILABLE:
        return
    MARKETS_ACTIVE.set(active_markets)
    MARKETS_WITH_FEES.set(markets_with_fees)
    EXPIRING_6H.set(expiring_6h)


def record_reconcile_issues(issues: dict[str, int]) -> None:
    if not _AVAILABLE:
        return
    for key, value in issues.items():
        RECONCILE_ISSUES.labels(metric=key).set(value)


def record_reconcile_run(success: bool) -> None:
    if not _AVAILABLE:
        return
    RECONCILE_RUNS.labels(outcome="ok" if success else "failed").inc()


def render_metrics() -> tuple[bytes, str]:
    if not _AVAILABLE:
        return b"# prometheus-client not installed\n", "text/plain; charset=utf-8"
    return generate_latest(), CONTENT_TYPE_LATEST
