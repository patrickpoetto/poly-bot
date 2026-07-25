"""CLI entrypoint — ``poly-bot``."""

import asyncio

import structlog
import typer
from structlog.typing import FilteringBoundLogger

from poly_bot.discovery.runner import DiscoveryRunner
from poly_bot.logging_setup import configure_logging
from poly_bot.settings import Settings, get_settings

app = typer.Typer(
    name="poly-bot",
    help="Polymarket trading platform — discovery, strategies, execution.",
    no_args_is_help=True,
)
discovery_app = typer.Typer(help="Market catalog discovery engine.")
catalog_app = typer.Typer(help="Live catalog — expiring window, quotes, dashboard.")
harvest_app = typer.Typer(help="Near-resolution harvest — priced outcomes in expiring window.")
ops_app = typer.Typer(help="Operations — migrate, backup, reconciliation.")
app.add_typer(discovery_app, name="discovery")
app.add_typer(catalog_app, name="catalog")
app.add_typer(harvest_app, name="harvest")
app.add_typer(ops_app, name="ops")


def _bootstrap() -> tuple[Settings, FilteringBoundLogger]:
    settings = get_settings()
    configure_logging(settings)
    return settings, structlog.get_logger()


@discovery_app.command("init-db")
def discovery_init_db() -> None:
    """Create or migrate catalog schema (SQLite create_all or Postgres Alembic)."""
    settings, log = _bootstrap()
    runner = DiscoveryRunner(settings)
    runner.init_database()
    log.info("database_initialized", url=settings.resolved_database_url.split("@")[-1])
    typer.echo(f"Database ready: {settings.resolved_database_url.split('@')[-1]}")


@discovery_app.command("scan")
def discovery_scan(
    skip_closed: bool = typer.Option(
        False,
        "--skip-closed",
        help="Only scan active markets (skip 7-day closed bootstrap).",
    ),
) -> None:
    """Full catalog scan: active markets + optional closed lookback."""
    settings, log = _bootstrap()
    runner = DiscoveryRunner(settings)
    runner.init_database()
    log.info("scan_starting", data_dir=str(settings.poly_bot_data_dir))

    stats = asyncio.run(runner.scan_active()) if skip_closed else asyncio.run(runner.scan_all())

    catalog_stats = runner.stats()
    typer.echo(
        typer.style("Scan complete.", fg=typer.colors.GREEN, bold=True)
        + f"\n  Upserted: {stats['markets_upserted']}"
        f"\n  Errors:   {stats['errors']}"
        f"\n  Active in DB: {catalog_stats['active_markets']}"
        f"\n  Total in DB:  {catalog_stats['total_markets']}"
        f"\n  Database: {settings.catalog_db_path}"
    )


@discovery_app.command("scan-events")
def discovery_scan_events(
    skip_closed: bool = typer.Option(
        False,
        "--skip-closed",
        help="Only active events (skip 7-day closed lookback).",
    ),
) -> None:
    """Crawl Gamma events (keyset pagination) into catalog DB."""
    settings, log = _bootstrap()
    runner = DiscoveryRunner(settings)
    runner.init_database()
    log.info("scan_events_starting")
    stats = asyncio.run(runner.scan_events(include_closed=not skip_closed))
    _echo_taxonomy_result("Events scan complete.", stats, runner.stats(), settings)


@discovery_app.command("scan-series")
def discovery_scan_series() -> None:
    """Crawl Gamma series definitions into catalog DB."""
    settings, log = _bootstrap()
    runner = DiscoveryRunner(settings)
    runner.init_database()
    log.info("scan_series_starting")
    stats = asyncio.run(runner.scan_series())
    _echo_taxonomy_result("Series scan complete.", stats, runner.stats(), settings)


@discovery_app.command("scan-taxonomy")
def discovery_scan_taxonomy(
    skip_closed: bool = typer.Option(
        False,
        "--skip-closed",
        help="Only active events (skip closed lookback). Series always full.",
    ),
) -> None:
    """Crawl events + series (recommended after first market scan)."""
    settings, log = _bootstrap()
    runner = DiscoveryRunner(settings)
    runner.init_database()
    log.info("scan_taxonomy_starting")
    stats = asyncio.run(runner.scan_taxonomy(include_closed_events=not skip_closed))
    _echo_taxonomy_result("Taxonomy scan complete.", stats, runner.stats(), settings)


@discovery_app.command("delta")
def discovery_delta() -> None:
    """Incremental scan: recently updated markets + events (fast)."""
    settings, log = _bootstrap()
    runner = DiscoveryRunner(settings)
    runner.init_database()
    log.info("discovery_delta_starting")
    stats = asyncio.run(runner.scan_delta())
    catalog_stats = runner.stats()
    typer.echo(
        typer.style("Delta scan complete.", fg=typer.colors.GREEN, bold=True)
        + f"\n  Markets upserted: {stats.get('markets_upserted', 0)}"
        + f"\n  Events upserted:   {stats.get('events_upserted', 0)}"
        + f"\n  CLOB tick filled:  {stats.get('clob_tick_enriched', 0)}"
        + f"\n  Errors:            {stats.get('errors', 0)}"
        + f"\n  Markets w/ fees:   {catalog_stats['markets_with_fees']}"
        + f"\n  Events w/ fees:    {catalog_stats['events_with_fees']}"
    )


@discovery_app.command("serve")
def discovery_serve(
    once: bool = typer.Option(False, "--once", help="Single scheduler cycle then exit."),
) -> None:
    """Daemon: delta scans + periodic full catalog refresh."""
    settings, log = _bootstrap()
    from poly_bot.discovery.scheduler import DiscoveryScheduler

    scheduler = DiscoveryScheduler(settings)
    scheduler.init_database()
    log.info("discovery_serve_start", once=once)
    if once:
        stats = asyncio.run(scheduler.run_cycle())
        typer.echo(f"Cycle: {stats}")
    else:
        asyncio.run(scheduler.run_forever())


@discovery_app.command("backfill-fees")
def discovery_backfill_fees(
    all_markets: bool = typer.Option(
        False,
        "--all",
        help="Re-parse all markets (not only missing fee fields).",
    ),
) -> None:
    """Backfill fee columns from stored Gamma raw_payload."""
    settings, log = _bootstrap()
    from sqlmodel import Session

    from poly_bot.discovery.backfill_fees import backfill_fees_from_raw
    from poly_bot.persistence.db import get_engine, init_db

    init_db(get_engine(settings))
    with Session(get_engine(settings)) as session:
        stats = backfill_fees_from_raw(session, only_missing=not all_markets)
    log.info("backfill_fees_done", **stats)
    typer.echo(f"Backfill fees: {stats}")


@discovery_app.command("stats")
def discovery_stats() -> None:
    """Show catalog database statistics."""
    settings, _log = _bootstrap()
    runner = DiscoveryRunner(settings)
    runner.init_database()
    stats = runner.stats()
    typer.echo(
        f"Catalog: {settings.resolved_database_url.split('@')[-1]}\n"
        f"  Markets — total: {stats['total_markets']}, active: {stats['active_markets']}\n"
        f"            linked event_id: {stats['markets_with_event_id']}, "
        f"series_id: {stats['markets_with_series_id']}\n"
        f"            fees_enabled: {stats['markets_with_fees']}\n"
        f"  Events  — total: {stats['total_events']}, active: {stats['active_events']}\n"
        f"            fees_enabled: {stats['events_with_fees']}\n"
        f"  Series  — total: {stats['total_series']}"
    )


def _echo_taxonomy_result(
    title: str,
    run_stats: dict[str, int],
    catalog_stats: dict[str, int],
    settings: Settings,
) -> None:
    typer.echo(
        typer.style(title, fg=typer.colors.GREEN, bold=True)
        + f"\n  Events upserted: {run_stats.get('events_upserted', 0)}"
        + f"\n  Series upserted: {run_stats.get('series_upserted', 0)}"
        + f"\n  Errors:          {run_stats.get('errors', 0)}"
        + f"\n  Events in DB:    {catalog_stats['total_events']} "
        f"(active: {catalog_stats['active_events']})"
        + f"\n  Series in DB:     {catalog_stats['total_series']}"
        + f"\n  Database:        {settings.catalog_db_path}"
    )


@catalog_app.command("link")
def catalog_link() -> None:
    """Link series slugs and sync tags from events."""
    settings, log = _bootstrap()
    from sqlmodel import Session

    from poly_bot.catalog.linker import CatalogLinker
    from poly_bot.persistence.db import get_engine, init_db

    init_db(get_engine(settings))
    with Session(get_engine(settings)) as session:
        stats = CatalogLinker(session).link_all()
    log.info("catalog_link_done", **stats)
    typer.echo(f"Linked: {stats}")


@catalog_app.command("enrich")
def catalog_enrich(
    hours: float = typer.Option(6.0, "--hours", "-h", help="Expiring horizon in hours."),
    force: bool = typer.Option(
        False,
        "--force",
        help="Refresh all markets in the window (ignore stale filter).",
    ),
) -> None:
    """REST refresh: midpoints, spreads, OI for markets expiring within horizon."""
    settings, log = _bootstrap()
    from sqlmodel import Session

    from poly_bot.catalog.enricher import CatalogEnricher
    from poly_bot.catalog.watch import CatalogWatch

    watch = CatalogWatch(settings)
    watch.init_database()
    with Session(watch._engine) as session:
        stats = asyncio.run(
            CatalogEnricher(settings, session).enrich_expiring_window(
                hours=hours,
                force=force,
            )
        )
    log.info("catalog_enrich_done", **stats)
    typer.echo(f"Enrichment: {stats}")


@catalog_app.command("window-stats")
def catalog_window_stats(
    hours: float | None = typer.Option(
        None,
        "--hours",
        "-h",
        help="Catalog window (default: CATALOG_EXPIRING_HOURS).",
    ),
    show_band: bool = typer.Option(
        False,
        "--show-band",
        help="List markets with DB mid in harvest price band.",
    ),
    hot_only: bool = typer.Option(
        False,
        "--hot",
        help="With --show-band: only HOT-tier markets.",
    ),
) -> None:
    """Funnel: DB totals → catalog window → HOT → quoted → harvest band."""
    settings, _log = _bootstrap()
    from rich.console import Console
    from rich.table import Table
    from sqlmodel import Session

    from poly_bot.catalog.time_window import hot_window_label
    from poly_bot.catalog.watch import CatalogWatch
    from poly_bot.catalog.window_stats import compute_window_stats, list_harvest_band_rows

    watch = CatalogWatch(settings)
    watch.init_database()
    with Session(watch._engine) as session:
        stats = compute_window_stats(session, settings, hours=hours)
        band_rows = (
            list_harvest_band_rows(session, settings, hours=hours, hot_only=hot_only)
            if show_band
            else []
        )

    typer.echo("\nCatalog window funnel")
    typer.echo(f"  Total in DB:              {stats.total_markets:,}")
    typer.echo(f"  Active + open:            {stats.active_open:,}")
    typer.echo(f"  Closed / inactive:        {stats.closed_or_inactive:,}")
    typer.echo(f"  Catalog window ≤{stats.horizon_hours:.0f}h:     {stats.in_catalog_window:,}")
    typer.echo(f"  {hot_window_label(settings)}:  {stats.in_hot_window:,}")
    typer.echo(
        f"  With tokens (catalog):    {stats.with_tokens:,}"
        f" ({stats.with_both_tokens:,} with yes+no)"
    )
    typer.echo(
        f"  token_quotes globally:    {stats.global_quotes_total:,}",
    )
    if stats.latest_quote_age_s is not None:
        typer.echo(f"  Latest quote age:         {stats.latest_quote_age_s / 60:.0f}m ago")
    else:
        typer.echo("  Latest quote age:         never")
    typer.echo(f"  With DB quote (catalog):  {stats.with_any_quote:,}")
    typer.echo(
        f"  Mid ≥ 95¢ (catalog win):  {stats.mid_ge_95:,}  (DB mids — harvest scan uses live books)"
    )
    typer.echo(
        f"  Mid in harvest band:      {stats.in_harvest_band:,}  "
        f"({settings.harvest_min_buy_price:.3f}-{settings.harvest_max_buy_price:.3f})"
    )

    if stats.in_catalog_window > 0 and stats.with_any_quote == 0:
        typer.echo("")
        if stats.global_quotes_total == 0:
            typer.echo(
                "  ⚠ No quotes in DB — start catalog pipeline:\n"
                "    docker compose --profile heavy up -d catalog-watch ws-daemon\n"
                "    or once: poly-bot catalog enrich --force"
            )
        elif stats.with_tokens < stats.in_catalog_window:
            typer.echo("  ⚠ Some catalog markets lack tokens — run discovery delta scan")
        else:
            typer.echo(
                "  ⚠ Catalog window has tokens but no mids — enrich/WS not covering "
                "this slice. Run: poly-bot catalog enrich --force"
            )

    if stats.by_domain:
        table = Table(title="By domain (catalog window)")
        table.add_column("Domain")
        table.add_column("Catalog", justify="right")
        table.add_column("HOT", justify="right")
        table.add_column("Live", justify="right")
        table.add_column("Quoted", justify="right")
        table.add_column("≥95¢", justify="right")
        table.add_column("Band", justify="right")
        for d in stats.by_domain:
            table.add_row(
                d.domain,
                str(d.in_catalog),
                str(d.in_hot),
                str(d.live_sports) if d.domain == "sports" else "—",
                str(d.with_quote),
                str(d.mid_ge_95),
                str(d.in_harvest_band),
            )
        Console().print(table)

    if show_band:
        title = "DB mid in harvest band"
        if hot_only:
            title += f" ({hot_window_label(settings)} only)"
        else:
            title += f" (catalog ≤{stats.horizon_hours:.0f}h)"
        band_table = Table(title=title)
        band_table.add_column("HOT")
        band_table.add_column("Left", justify="right")
        band_table.add_column("Side")
        band_table.add_column("Mid", justify="right")
        band_table.add_column("Yes", justify="right")
        band_table.add_column("No", justify="right")
        band_table.add_column("Domain")
        band_table.add_column("Question", max_width=42)
        if not band_rows:
            typer.echo(
                "\nNo rows (DB mid in band). Try without --hot or run harvest scan for live asks."
            )
        for r in band_rows:
            live = "L" if r.is_live else ""
            band_table.add_row(
                "✓" if r.in_hot else "",
                f"{live}{r.hours_left:.1f}h",
                r.band_side[:12],
                f"{r.band_mid:.3f}",
                f"{r.yes_mid:.3f}" if r.yes_mid is not None else "—",
                f"{r.no_mid:.3f}" if r.no_mid is not None else "—",
                r.domain,
                (r.question[:40] + "…") if len(r.question) > 41 else r.question,
            )
        Console().print(band_table)
        typer.echo(
            f"\n{len(band_rows)} side(s) — DB mids only; "
            "use `harvest scan` for live ask + book sanity."
        )


@catalog_app.command("expiring")
def catalog_expiring(
    hours: float = typer.Option(6.0, "--hours", "-h"),
    limit: int = typer.Option(200, "--limit", "-n"),
) -> None:
    """Print table of markets expiring within the next N hours."""
    settings, _log = _bootstrap()
    from sqlmodel import Session

    from poly_bot.catalog.display import print_expiring_table
    from poly_bot.catalog.expiring import list_expiring_markets
    from poly_bot.catalog.watch import CatalogWatch

    watch = CatalogWatch(settings)
    watch.init_database()
    with Session(watch._engine) as session:
        markets = list_expiring_markets(session, hours=hours)
    print_expiring_table(markets[:limit], hours=hours)
    typer.echo(f"\nTotal in window: {len(markets)} (showing {min(limit, len(markets))})")


@catalog_app.command("watch")
def catalog_watch(
    hours: float = typer.Option(6.0, "--hours", "-h"),
    once: bool = typer.Option(False, "--once", help="Single cycle then exit."),
) -> None:
    """Daemon: link + enrich + WS quotes for expiring markets."""
    settings, log = _bootstrap()
    from poly_bot.catalog.watch import CatalogWatch

    watch = CatalogWatch(settings)
    watch.init_database()
    log.info("catalog_watch", hours=hours, once=once)
    if once:
        stats = asyncio.run(watch.run_cycle(hours=hours))
        typer.echo(f"Cycle complete: {stats}")
    else:
        asyncio.run(watch.run_forever(hours=hours))


@catalog_app.command("events")
def catalog_events(
    hours: float = typer.Option(6.0, "--hours", "-h"),
    limit: int = typer.Option(30, "--limit", "-n"),
) -> None:
    """Events with markets expiring in the window (fee summary)."""
    settings, _log = _bootstrap()
    from rich.console import Console
    from rich.table import Table
    from sqlmodel import Session

    from poly_bot.catalog.events_view import group_expiring_by_event
    from poly_bot.catalog.expiring import list_expiring_markets
    from poly_bot.catalog.watch import CatalogWatch

    watch = CatalogWatch(settings)
    watch.init_database()
    with Session(watch._engine) as session:
        markets = list_expiring_markets(session, hours=hours)
    events = group_expiring_by_event(markets)[:limit]

    table = Table(title=f"Events in next {hours:.0f}h")
    table.add_column("Title", max_width=40)
    table.add_column("Domain")
    table.add_column("Mkts", justify="right")
    table.add_column("Fee%", justify="right")
    table.add_column("Left", justify="right")
    for e in events:
        rate = f"{e.fee_rate * 100:.1f}" if e.fee_rate else "—"
        table.add_row(
            (e.title[:37] + "…") if len(e.title) > 38 else e.title,
            e.domain,
            str(e.market_count),
            rate,
            f"{e.min_hours_left:.1f}h",
        )
    Console().print(table)
    typer.echo(f"Total events: {len(group_expiring_by_event(markets))}")


@harvest_app.command("scan")
def harvest_scan(
    hours: float | None = typer.Option(
        None,
        "--hours",
        "-h",
        help="Expiring window (default: CATALOG_EXPIRING_HOURS).",
    ),
    hot_only: bool = typer.Option(
        True,
        "--hot/--all-window",
        help="Only markets within CATALOG_QUOTE_HOT_HOURS of window end.",
    ),
    minutes: float | None = typer.Option(
        None,
        "--minutes",
        "-m",
        help="Tighter time cap (overrides HOT hours when set).",
    ),
    require_fresh: bool = typer.Option(
        False,
        "--require-fresh",
        help="DB-only: reject quotes older than HARVEST_QUOTE_MAX_AGE_SECONDS.",
    ),
    db_only: bool = typer.Option(
        False,
        "--db-only",
        help="Skip live CLOB /book refresh (faster, no depth check).",
    ),
    log: bool = typer.Option(
        False,
        "--log",
        help="Append results to data/harvest_paper.jsonl.",
    ),
    limit: int = typer.Option(50, "--limit", "-n"),
) -> None:
    """Near-expiry outcomes with ask in 98-99c band and net yield >= target."""
    settings, log_ = _bootstrap()
    from sqlmodel import Session

    from poly_bot.catalog.watch import CatalogWatch
    from poly_bot.harvest.display import print_harvest_table
    from poly_bot.harvest.paper import append_paper_log
    from poly_bot.harvest.scan import (
        scan_harvest_opportunities,
        scan_harvest_opportunities_live,
    )

    watch = CatalogWatch(settings)
    watch.init_database()
    with Session(watch._engine) as session:
        if db_only:
            opps = scan_harvest_opportunities(
                session,
                settings,
                hours=hours,
                hot_only=hot_only,
                max_minutes_left=minutes,
                require_fresh_quotes=require_fresh,
            )
        else:
            opps = asyncio.run(
                scan_harvest_opportunities_live(
                    session,
                    settings,
                    hours=hours,
                    hot_only=hot_only,
                    max_minutes_left=minutes,
                    require_fresh_quotes=False,
                )
            )
    shown = opps[:limit]
    print_harvest_table(
        shown,
        hot_hours=settings.catalog_quote_hot_hours,
        min_net_pct=settings.harvest_min_net_yield_pct,
        order_usd=settings.harvest_order_usd,
    )
    if log and opps:
        path = append_paper_log(settings, opps)
        typer.echo(f"Logged {len(opps)} rows → {path}")
    log_.info(
        "harvest_scan_done",
        total=len(opps),
        shown=len(shown),
        hot_only=hot_only,
        live=not db_only,
    )
    typer.echo(
        f"\n{len(opps)} opportunities (sorted by window end)"
        + (f", showing {len(shown)}" if len(shown) < len(opps) else "")
    )


@harvest_app.command("diagnose")
def harvest_diagnose(
    hours: float | None = typer.Option(None, "--hours", "-h"),
    hot_only: bool = typer.Option(True, "--hot/--all-window"),
    db_only: bool = typer.Option(False, "--db-only"),
) -> None:
    """Show why harvest scan finds 0 (or N) opportunities — funnel counts."""
    settings, _log = _bootstrap()
    from rich.console import Console
    from rich.table import Table
    from sqlmodel import Session

    from poly_bot.catalog.watch import CatalogWatch
    from poly_bot.harvest.diagnose import diagnose_harvest

    watch = CatalogWatch(settings)
    watch.init_database()
    with Session(watch._engine) as session:
        diag = asyncio.run(
            diagnose_harvest(
                session,
                settings,
                hours=hours,
                hot_only=hot_only,
                live_books=not db_only,
                require_fresh_quotes=False,
            )
        )

    from poly_bot.catalog.time_window import hot_window_label

    mode = hot_window_label(settings) if hot_only else f"window ≤{diag.horizon_hours:.0f}h"
    typer.echo(f"\nHarvest diagnose ({mode}, {'DB' if db_only else 'live books'})")
    typer.echo(f"  Markets in horizon:     {diag.markets_in_horizon}")
    typer.echo(f"  Markets in time filter: {diag.markets_in_hot}")
    typer.echo(f"  Outcome sides quoted:   {diag.sides_with_quote}")
    typer.echo(f"  Price ≥ 95¢:            {diag.sides_price_ge_95}")
    typer.echo(
        f"  In band {settings.harvest_min_buy_price:.3f}"
        f"-{settings.harvest_max_buy_price:.3f}:  {diag.sides_in_price_band}"
    )
    typer.echo(
        f"  Net >= {settings.harvest_min_net_yield_pct * 100:.1f}%:"
        f"       {diag.sides_pass_net_yield}"
    )
    if not db_only:
        typer.echo(f"  Book sanity:            {diag.sides_pass_book_sanity}")
    typer.echo(f"  Pass liquidity:         {diag.sides_pass_liquidity}")
    typer.echo(f"  → Opportunities:        {diag.opportunities}")
    if diag.stale_skipped:
        typer.echo(f"  (skipped stale quotes:  {diag.stale_skipped})")

    if diag.near_misses:
        table = Table(title="Near misses (price ≥ 90¢ in HOT window)")
        table.add_column("Price")
        table.add_column("Net%")
        table.add_column("Side")
        table.add_column("Left")
        table.add_column("Why not")
        table.add_column("Question", max_width=32)
        for n in diag.near_misses[:10]:
            table.add_row(
                f"{n.price:.3f} ({n.price_source})",
                f"{n.net_yield_pct * 100:.2f}",
                n.side,
                f"{n.hours_left:.1f}h",
                n.reject_reason,
                n.question[:32],
            )
        Console().print(table)
    elif diag.markets_in_hot == 0:
        typer.echo("\nNo markets in time filter — try: harvest diagnose --all-window")
    elif diag.sides_price_ge_95 == 0:
        typer.echo("\nNothing near 98¢ in this window right now (normal off-peak).")
        typer.echo("Try: harvest scan --all-window   or widen HOT hours in .env")


@harvest_app.command("engine")
def harvest_engine(
    paper: bool = typer.Option(
        True,
        "--paper/--live",
        help="Paper mode (default) or live execution.  --live requires CLOB credentials.",
    ),
) -> None:
    """Live execution engine — monitors near-expiry markets and places orders."""
    settings, _log = _bootstrap()
    if not paper and not (
        settings.clob_api_key and settings.clob_secret and settings.clob_passphrase
    ):
        typer.echo(
            "Live mode requires CLOB_API_KEY, CLOB_SECRET, CLOB_PASSPHRASE in .env", err=True
        )
        raise typer.Exit(code=1)

    settings = settings.model_copy(update={"paper_trading": paper})
    mode = "PAPER" if paper else "LIVE"
    typer.echo(f"harvest engine starting [{mode}] — Ctrl+C to stop")
    from poly_bot.harvest.engine import LiveEngine

    engine = LiveEngine(settings)
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        typer.echo("\nEngine stopped.")
    finally:
        s = engine.stats
        typer.echo(
            f"\nStats — orders: {s.orders_placed} (paper: {s.orders_paper}"
            f", failed: {s.orders_failed}) | watchlist peak: {s.watchlist_size}"
        )


@harvest_app.command("paper")
def harvest_paper(limit: int = typer.Option(20, "--limit", "-n")) -> None:
    """Show recent harvest scan log lines."""
    settings, _log = _bootstrap()
    from rich.console import Console
    from rich.table import Table

    from poly_bot.harvest.paper import read_paper_log

    rows = read_paper_log(settings, limit=limit)
    table = Table(title="Harvest paper log (recent)")
    table.add_column("When")
    table.add_column("Net%")
    table.add_column("Ask")
    table.add_column("Side")
    table.add_column("Min")
    table.add_column("Question", max_width=36)
    for r in rows:
        table.add_row(
            str(r.get("scanned_at", ""))[:19],
            str(r.get("net_yield_pct", "")),
            str(r.get("ask", "")),
            str(r.get("side", "")),
            str(r.get("minutes_left", "")),
            str(r.get("question", ""))[:36],
        )
    Console().print(table)
    typer.echo(f"{len(rows)} lines")


@harvest_app.command("clear-paper")
def harvest_clear_paper(
    before: str = typer.Option(
        "",
        "--before",
        help="Delete only paper orders created before this date (YYYY-MM-DD). Default: all.",
    ),
    force: bool = typer.Option(False, "--force", help="Skip confirmation prompt."),
) -> None:
    """Delete paper orders from the DB (test-session cleanup)."""
    settings, _ = _bootstrap()
    from sqlmodel import Session, select

    from poly_bot.persistence.db import get_engine
    from poly_bot.persistence.schema import HarvestOrder

    engine = get_engine(settings)
    cutoff = None
    if before:
        from datetime import datetime
        try:
            cutoff = datetime.fromisoformat(before)
        except ValueError:
            typer.echo(f"Invalid date '{before}' — use YYYY-MM-DD.", err=True)
            raise typer.Exit(code=1) from None
    with Session(engine) as session:
        stmt = select(HarvestOrder).where(HarvestOrder.paper == True)  # noqa: E712
        if cutoff is not None:
            stmt = stmt.where(HarvestOrder.created_at < cutoff)  # type: ignore[operator]
        rows = session.exec(stmt).all()

    count = len(rows)
    if count == 0:
        typer.echo("No paper orders found matching the filter.")
        return

    date_suffix = f" before {before}" if before else ""
    typer.echo(f"Found {count} paper order(s){date_suffix}.")
    if not force:
        typer.confirm("Delete all?", abort=True)

    with Session(engine) as session:
        stmt2 = select(HarvestOrder).where(HarvestOrder.paper == True)  # noqa: E712
        if cutoff is not None:
            stmt2 = stmt2.where(HarvestOrder.created_at < cutoff)  # type: ignore[operator]
        rows2 = session.exec(stmt2).all()
        for r in rows2:
            session.delete(r)
        session.commit()

    typer.echo(f"Deleted {len(rows2)} paper order(s).")


@app.command("run")
def platform_run(
    hours: float = typer.Option(6.0, "--hours", "-h", help="Catalog expiring window."),
) -> None:
    """Run discovery scheduler + catalog watch together."""
    settings, log = _bootstrap()
    from poly_bot.runtime import run_platform

    log.info("platform_run", hours=hours)
    typer.echo(
        f"Starting platform (discovery + catalog watch, {hours:.0f}h window). Ctrl+C to stop."
    )
    try:
        asyncio.run(run_platform(settings, catalog_hours=hours))
    except KeyboardInterrupt:
        typer.echo("\nStopped.")


@catalog_app.command("dashboard")
def catalog_dashboard(
    hours: float = typer.Option(6.0, "--hours", "-h", help="Default API horizon."),
) -> None:
    """Start local web UI (requires pip install -e '.[dashboard]')."""
    settings, _log = _bootstrap()
    from poly_bot.catalog.dashboard import run_dashboard

    typer.echo(
        f"Dashboard http://{settings.catalog_dashboard_host}:"
        f"{settings.catalog_dashboard_port}/?hours={hours}"
    )
    run_dashboard(settings)


@catalog_app.command("ws-daemon")
def catalog_ws_daemon(
    hours: float | None = typer.Option(
        None,
        "--hours",
        "-h",
        help="Quote horizon hours (default: CATALOG_EXPIRING_HOURS).",
    ),
    refresh: float = typer.Option(120.0, "--refresh", help="Resubscribe interval (seconds)."),
) -> None:
    """Persistent CLOB WebSocket — auto-reconnect + rolling subscription."""
    settings, log = _bootstrap()
    from poly_bot.catalog.ws_daemon import PersistentWSDaemon
    from poly_bot.persistence.db import get_engine, init_db

    init_db(get_engine(settings))
    horizon = hours if hours is not None else settings.catalog_quote_horizon_hours
    daemon = PersistentWSDaemon(settings, hours=horizon, refresh_seconds=refresh)
    log.info("ws_daemon_starting", hours=horizon, refresh=refresh)
    try:
        asyncio.run(daemon.run_forever())
    except KeyboardInterrupt:
        typer.echo("\nStopped.")


@catalog_app.command("taxonomy-add")
def catalog_taxonomy_add(
    kind: str = typer.Argument(..., help="tag | condition_id | slug_prefix"),
    key: str = typer.Argument(..., help="tag slug, condition_id, or slug prefix"),
    domain: str = typer.Option("", "--domain", help="Override domain."),
    structure: str = typer.Option("", "--structure", help="Override structure."),
    resolution: str = typer.Option("", "--resolution", help="Override resolution_source."),
    note: str = typer.Option("", "--note"),
) -> None:
    """Add or update a taxonomy override."""
    settings, _log = _bootstrap()
    from sqlmodel import Session

    from poly_bot.catalog.taxonomy import upsert_override
    from poly_bot.persistence.db import get_engine, init_db

    init_db(get_engine(settings))
    with Session(get_engine(settings)) as session:
        row = upsert_override(
            session,
            kind=kind,  # type: ignore[arg-type]
            key=key,
            domain=domain or None,
            structure=structure or None,
            resolution_source=resolution or None,
            note=note,
        )
    typer.echo(
        f"Override #{row.id}: {row.kind}={row.key} → domain={row.domain} structure={row.structure}"
    )


@catalog_app.command("taxonomy-list")
def catalog_taxonomy_list() -> None:
    """List taxonomy overrides."""
    settings, _log = _bootstrap()
    from sqlmodel import Session

    from poly_bot.catalog.taxonomy import list_overrides
    from poly_bot.persistence.db import get_engine, init_db

    init_db(get_engine(settings))
    with Session(get_engine(settings)) as session:
        rows = list_overrides(session)
    if not rows:
        typer.echo("No overrides.")
        return
    for row in rows:
        typer.echo(
            f"  #{row.id:>4} {row.kind:>13}  {row.key[:40]:<40}  "
            f"domain={row.domain or '-'}  struct={row.structure or '-'}"
        )


@catalog_app.command("reclassify")
def catalog_reclassify(
    all_markets: bool = typer.Option(
        False,
        "--all",
        help="Re-classify entire catalog (not only domain=unknown).",
    ),
) -> None:
    """Re-run classifier rules on stored raw_payload."""
    settings, log = _bootstrap()
    from sqlmodel import Session

    from poly_bot.ops.reclassify import reclassify_markets
    from poly_bot.persistence.db import get_engine, init_db

    init_db(get_engine(settings))
    with Session(get_engine(settings)) as session:
        stats = reclassify_markets(session, only_unknown=not all_markets, force=all_markets)
    log.info("reclassify_done", **stats)
    typer.echo(f"Reclassified: {stats}")


@ops_app.command("migrate")
def ops_migrate() -> None:
    """Apply Alembic migrations (Postgres) or SQLite schema bootstrap."""
    settings, log = _bootstrap()
    from poly_bot.persistence.db import get_engine, init_db

    engine = get_engine(settings)
    init_db(engine)
    log.info("migrate_complete", dialect=engine.dialect.name)
    typer.echo(f"Migrations applied ({engine.dialect.name})")


@ops_app.command("reconcile")
def ops_reconcile() -> None:
    """Report catalog data-quality issues."""
    settings, _log = _bootstrap()
    from sqlmodel import Session

    from poly_bot.ops.reconcile import run_reconciliation
    from poly_bot.persistence.db import get_engine, init_db

    init_db(get_engine(settings))
    with Session(get_engine(settings)) as session:
        issues = run_reconciliation(session)
    for key, value in issues.items():
        typer.echo(f"  {key}: {value}")


@ops_app.command("backup")
def ops_backup(
    dest: str = typer.Option("", "--dest", help="Backup directory (default: data/backups)."),
) -> None:
    """Backup SQLite catalog or pg_dump for Postgres."""
    settings, log = _bootstrap()
    from pathlib import Path

    from poly_bot.ops.backup import backup_catalog

    dest_dir = Path(dest) if dest else None
    path = backup_catalog(settings, dest_dir=dest_dir)
    log.info("backup_complete", path=str(path))
    typer.echo(f"Backup written: {path}")


@ops_app.command("backup-loop")
def ops_backup_loop(
    interval: int = typer.Option(3600, "--interval", help="Seconds between backups."),
    retention_days: int = typer.Option(7, "--retention-days"),
    dest: str = typer.Option("", "--dest", help="Backup directory."),
) -> None:
    """Daemon: backup every N seconds, prune older than retention."""
    settings, log = _bootstrap()
    from pathlib import Path

    from poly_bot.ops.backup_scheduler import run_backup_loop

    log.info("backup_loop_starting", interval=interval, retention_days=retention_days)
    try:
        asyncio.run(
            run_backup_loop(
                settings,
                interval_seconds=interval,
                retention_days=retention_days,
                dest_dir=Path(dest) if dest else None,
            )
        )
    except KeyboardInterrupt:
        typer.echo("\nStopped.")


@ops_app.command("reconcile-loop")
def ops_reconcile_loop(
    interval: int = typer.Option(3600, "--interval", help="Seconds between checks."),
) -> None:
    """Daemon: run reconcile every N seconds; write report to catalog_state + metrics."""
    settings, log = _bootstrap()
    from poly_bot.ops.reconcile_scheduler import run_reconcile_loop

    log.info("reconcile_loop_starting", interval=interval)
    try:
        asyncio.run(run_reconcile_loop(settings, interval_seconds=interval))
    except KeyboardInterrupt:
        typer.echo("\nStopped.")


@ops_app.command("alerter-loop")
def ops_alerter_loop() -> None:
    """Daemon: poll health snapshot and page Telegram on state transitions."""
    settings, log = _bootstrap()
    from poly_bot.ops.alerter import run_alerter_loop

    if not settings.alerter_enabled:
        typer.echo("ALERTER_ENABLED is false — not starting.")
        raise typer.Exit(code=0)
    if not (settings.telegram_token and settings.telegram_chat_id):
        typer.echo("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set.", err=True)
        raise typer.Exit(code=1)

    log.info("alerter_loop_starting", interval=settings.alerter_interval_seconds)
    try:
        asyncio.run(run_alerter_loop(settings))
    except KeyboardInterrupt:
        typer.echo("\nStopped.")


@ops_app.command("telegram-test")
def ops_telegram_test(
    message: str = typer.Option(
        "poly-bot alerter test message",
        "--message",
        "-m",
        help="Text to send.",
    ),
) -> None:
    """Send a one-shot Telegram message using configured token/chat_id."""
    settings, _log = _bootstrap()
    if not (settings.telegram_token and settings.telegram_chat_id):
        typer.echo("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set.", err=True)
        raise typer.Exit(code=1)
    from poly_bot.notifier.telegram import TelegramClient

    client = TelegramClient(
        settings.telegram_token.get_secret_value(),
        chat_id=settings.telegram_chat_id,
    )
    ok = asyncio.run(client.send_message(message))
    typer.echo("delivered" if ok else "FAILED")
    raise typer.Exit(code=0 if ok else 1)


@ops_app.command("telegram-chat-id")
def ops_telegram_chat_id() -> None:
    """Print chat ids visible in recent updates — use this once during onboarding.

    First send a message to your bot in the Telegram app, then run this
    command and copy the printed ``chat.id`` into ``TELEGRAM_CHAT_ID``.
    """
    settings, _log = _bootstrap()
    if not settings.telegram_token:
        typer.echo("TELEGRAM_TOKEN must be set.", err=True)
        raise typer.Exit(code=1)
    from poly_bot.notifier.telegram import TelegramClient

    client = TelegramClient(settings.telegram_token.get_secret_value())
    updates = asyncio.run(client.get_updates())
    if not updates:
        typer.echo(
            "No updates yet. Open Telegram, send any message to the bot, then re-run this command."
        )
        raise typer.Exit(code=1)
    seen: set[str] = set()
    for upd in updates:
        msg = upd.get("message") or upd.get("edited_message") or {}
        chat = msg.get("chat") or {}
        cid = chat.get("id")
        if cid is None or str(cid) in seen:
            continue
        seen.add(str(cid))
        title = chat.get("title") or chat.get("username") or chat.get("first_name") or ""
        typer.echo(f"chat.id={cid}  type={chat.get('type')}  title={title}")
    typer.echo(
        "\nSet TELEGRAM_CHAT_ID to one of the values above (use the negative id for groups)."
    )


@ops_app.command("health")
def ops_health(
    service: str = typer.Option(
        "any",
        "--service",
        help="any | discovery | watch | ws | dashboard",
    ),
    max_age: int = typer.Option(
        600,
        "--max-age",
        help="Max staleness for activity checks (seconds).",
    ),
) -> None:
    """Healthcheck — exit 0 if healthy, 1 otherwise. Used by Docker HEALTHCHECK."""
    settings, _log = _bootstrap()
    from poly_bot.ops.health import check_health

    ok, details = check_health(settings, service=service, max_age_seconds=max_age)  # type: ignore[arg-type]
    for k, v in details.items():
        typer.echo(f"  {k}: {v}")
    typer.echo("OK" if ok else "UNHEALTHY")
    raise typer.Exit(code=0 if ok else 1)


@app.command("version")
def version() -> None:
    """Print package version."""
    from poly_bot import __version__

    typer.echo(f"poly-bot {__version__}")
