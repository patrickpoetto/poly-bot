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
weather_app = typer.Typer(help="Daily temperature markets — stations, events, bins.")
app.add_typer(discovery_app, name="discovery")
app.add_typer(catalog_app, name="catalog")
app.add_typer(harvest_app, name="harvest")
app.add_typer(ops_app, name="ops")
app.add_typer(weather_app, name="weather")


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

    stats = (
        asyncio.run(runner.scan_active())
        if skip_closed
        else asyncio.run(runner.scan_all())
    )

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
    from poly_bot.discovery.backfill_fees import backfill_fees_from_raw
    from poly_bot.persistence.db import get_engine, init_db
    from sqlmodel import Session

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
    from poly_bot.catalog.linker import CatalogLinker
    from poly_bot.persistence.db import get_engine, init_db
    from sqlmodel import Session

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
    from poly_bot.catalog.enricher import CatalogEnricher
    from poly_bot.catalog.watch import CatalogWatch
    from sqlmodel import Session

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


@catalog_app.command("expiring")
def catalog_expiring(
    hours: float = typer.Option(6.0, "--hours", "-h"),
    limit: int = typer.Option(200, "--limit", "-n"),
) -> None:
    """Print table of markets expiring within the next N hours."""
    settings, _log = _bootstrap()
    from poly_bot.catalog.display import print_expiring_table
    from poly_bot.catalog.expiring import list_expiring_markets
    from poly_bot.catalog.watch import CatalogWatch
    from sqlmodel import Session

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
    from poly_bot.catalog.events_view import group_expiring_by_event
    from poly_bot.catalog.expiring import list_expiring_markets
    from poly_bot.catalog.watch import CatalogWatch
    from rich.console import Console
    from rich.table import Table
    from sqlmodel import Session

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
    stale_ok: bool = typer.Option(
        False,
        "--stale-quotes",
        help="Include opportunities even if quote age exceeds harvest limit.",
    ),
    limit: int = typer.Option(50, "--limit", "-n"),
) -> None:
    """List Yes/No mids in harvest price band (98–99¢, net yield ≥ target)."""
    settings, log = _bootstrap()
    from poly_bot.catalog.watch import CatalogWatch
    from poly_bot.harvest.display import print_harvest_table
    from poly_bot.harvest.scan import scan_harvest_opportunities
    from sqlmodel import Session

    watch = CatalogWatch(settings)
    watch.init_database()
    with Session(watch._engine) as session:
        opps = scan_harvest_opportunities(
            session,
            settings,
            hours=hours,
            hot_only=hot_only,
            require_fresh_quotes=not stale_ok,
        )
    shown = opps[:limit]
    print_harvest_table(
        shown,
        hot_hours=settings.catalog_quote_hot_hours,
        min_net_pct=settings.harvest_min_net_yield_pct,
    )
    log.info(
        "harvest_scan_done",
        total=len(opps),
        shown=len(shown),
        hot_only=hot_only,
    )
    typer.echo(
        f"\n{len(opps)} opportunities"
        + (f" (showing {len(shown)})" if len(shown) < len(opps) else "")
    )


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
        help="Quote horizon hours (default: max of CATALOG_EXPIRING_HOURS and weather days).",
    ),
    refresh: float = typer.Option(
        120.0, "--refresh", help="Resubscribe interval (seconds)."
    ),
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
    resolution: str = typer.Option(
        "", "--resolution", help="Override resolution_source."
    ),
    note: str = typer.Option("", "--note"),
) -> None:
    """Add or update a taxonomy override."""
    settings, _log = _bootstrap()
    from poly_bot.catalog.taxonomy import upsert_override
    from poly_bot.persistence.db import get_engine, init_db
    from sqlmodel import Session

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
        f"Override #{row.id}: {row.kind}={row.key} "
        f"→ domain={row.domain} structure={row.structure}"
    )


@catalog_app.command("taxonomy-list")
def catalog_taxonomy_list() -> None:
    """List taxonomy overrides."""
    settings, _log = _bootstrap()
    from poly_bot.catalog.taxonomy import list_overrides
    from poly_bot.persistence.db import get_engine, init_db
    from sqlmodel import Session

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
    from poly_bot.ops.reclassify import reclassify_markets
    from poly_bot.persistence.db import get_engine, init_db
    from sqlmodel import Session

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
    from poly_bot.ops.reconcile import run_reconciliation
    from poly_bot.persistence.db import get_engine, init_db
    from sqlmodel import Session

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
            "No updates yet. Open Telegram, send any message to the bot, "
            "then re-run this command."
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
        "\nSet TELEGRAM_CHAT_ID to one of the values above (use the negative id "
        "for groups)."
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


@weather_app.command("sync")
def weather_sync(
    horizon: int | None = typer.Option(
        None,
        "--horizon",
        "-H",
        help="Override WEATHER_FORECAST_HORIZON_DAYS (today through +N).",
    ),
) -> None:
    """Build weather_stations / weather_daily_events / weather_bins from catalog."""
    settings, log = _bootstrap()
    if horizon is not None:
        settings = settings.model_copy(update={"weather_forecast_horizon_days": horizon})
    from poly_bot.weather.sync import WeatherCatalogSync

    stats = WeatherCatalogSync(settings).sync()
    log.info("weather_sync_done", **stats)
    typer.echo(
        typer.style("Weather catalog synced.", fg=typer.colors.GREEN, bold=True)
        + f"\n  Stations:     {stats['stations']}"
        + f"\n  Cities:       {stats['cities']}"
        + f"\n  Daily events: {stats['daily_events']}"
        + f"\n  Bins:         {stats['bins']}"
        + f"\n  Pruned:       {stats['pruned_events']}"
        + f"\n  Skipped:      {stats['skipped_markets']}"
        + f"\n  Horizon:      today .. today+{settings.weather_forecast_horizon_days}"
    )


@weather_app.command("markets-sync")
def weather_markets_sync() -> None:
    """Incremental Polymarket sync focused on weather markets/events only."""
    settings, log = _bootstrap()
    from poly_bot.discovery.runner import DiscoveryRunner

    runner = DiscoveryRunner(settings)
    runner.init_database()
    stats = asyncio.run(runner.scan_weather_delta())
    log.info("weather_markets_sync_done", **stats)
    typer.echo(
        typer.style("Weather markets synced.", fg=typer.colors.GREEN, bold=True)
        + f"\n  Markets upserted: {stats.get('markets_upserted', 0)}"
        + f"\n  Events upserted:  {stats.get('events_upserted', 0)}"
        + f"\n  Errors:           {stats.get('errors', 0)}"
    )


@weather_app.command("serve")
def weather_serve() -> None:
    """Loop: weather markets + catalog + aviation + WU station on intervals."""
    import time

    settings, log = _bootstrap()
    if not settings.weather_market_sync_enabled:
        typer.echo("WEATHER_MARKET_SYNC_ENABLED=false — exiting.")
        raise typer.Exit(1)

    from poly_bot.discovery.runner import DiscoveryRunner
    from poly_bot.persistence.db import get_engine
    from poly_bot.weather.aviation_sync import WeatherAviationSync
    from poly_bot.weather.station_sync import WeatherStationSnapshotSync
    from poly_bot.weather.sync import WeatherCatalogSync
    from sqlmodel import Session

    runner = DiscoveryRunner(settings)
    runner.init_database()
    weather_syncer = WeatherCatalogSync(settings)
    aviation_syncer = WeatherAviationSync(settings)
    wu_syncer = WeatherStationSnapshotSync(settings)
    interval = settings.weather_market_sync_interval_seconds
    wu_interval = settings.weather_wu_sync_interval_seconds
    last_wu = 0.0
    typer.echo(
        f"Weather serve loop every {interval}s · WU station every {wu_interval}s (Ctrl+C to stop)"
    )
    while True:
        try:
            market_stats = asyncio.run(runner.scan_weather_delta())
            catalog_stats = weather_syncer.sync()
            aviation_stats = (
                aviation_syncer.sync() if settings.weather_aviation_sync_enabled else {}
            )
            wu_stats: dict[str, int] = {}
            if settings.weather_wu_sync_enabled and time.time() - last_wu >= wu_interval:
                with Session(get_engine(settings)) as session:
                    wu_stats = wu_syncer.sync_horizon(session)
                last_wu = time.time()
            log.info(
                "weather_serve_cycle",
                **market_stats,
                weather_daily_events=catalog_stats.get("daily_events", 0),
                weather_bins=catalog_stats.get("bins", 0),
                taf_updated=aviation_stats.get("taf_updated", 0),
                metar_new=aviation_stats.get("metar_new", 0),
                wu_ok=wu_stats.get("ok", 0),
                wu_error=wu_stats.get("error", 0),
            )
        except Exception:
            log.exception("weather_serve_cycle_failed")
        time.sleep(interval)


@weather_app.command("status")
def weather_status(
    horizon: int | None = typer.Option(None, "--horizon", "-H"),
) -> None:
    """Summary of synced weather events by forecast day."""
    from datetime import date, timedelta

    from sqlmodel import Session

    settings, _log = _bootstrap()
    if horizon is not None:
        settings = settings.model_copy(update={"weather_forecast_horizon_days": horizon})
    from poly_bot.persistence.db import get_engine
    from poly_bot.weather.display import print_weather_summary
    from poly_bot.weather.repo import WeatherRepository

    ref = date.today()
    max_date = ref + timedelta(days=settings.weather_forecast_horizon_days)
    with Session(get_engine(settings)) as session:
        print_weather_summary(
            WeatherRepository(session),
            min_date=ref,
            max_date=max_date,
        )


@weather_app.command("list")
def weather_list(
    day: str = typer.Option(
        "",
        "--day",
        "-d",
        help="Forecast date YYYY-MM-DD (default: tomorrow UTC).",
    ),
    city: str = typer.Option("", "--city", "-c", help="Filter city_key, e.g. london."),
    limit: int = typer.Option(10, "--limit", "-n", help="Max cities to show."),
) -> None:
    """List bins for a forecast day (station, coordinates, questions)."""
    from datetime import UTC, date, datetime, timedelta

    from sqlmodel import Session

    settings, _log = _bootstrap()
    from poly_bot.persistence.db import get_engine
    from poly_bot.weather.display import print_weather_day_detail
    from poly_bot.weather.repo import WeatherRepository

    if day:
        forecast_date = date.fromisoformat(day)
    else:
        forecast_date = (datetime.now(UTC) + timedelta(days=1)).date()

    with Session(get_engine(settings)) as session:
        print_weather_day_detail(
            WeatherRepository(session),
            forecast_date=forecast_date,
            city_key=city or None,
            limit_cities=limit,
        )


@weather_app.command("export")
def weather_export(
    out: str | None = typer.Option(
        None,
        "--out",
        "-o",
        help="JSON export path (default: POLY_BOT_DATA_DIR/weather_catalog.json).",
    ),
) -> None:
    """Export full weather catalog (stations, events, bins) to JSON."""
    import json
    from datetime import date, timedelta
    from pathlib import Path

    settings, _log = _bootstrap()
    out_path = Path(out) if out else settings.poly_bot_data_dir / "weather_catalog.json"
    from sqlmodel import Session

    from poly_bot.persistence.db import get_engine
    from poly_bot.weather.repo import WeatherRepository
    from poly_bot.weather.schema import WeatherCity

    ref = date.today()
    max_date = ref + timedelta(days=settings.weather_forecast_horizon_days)
    with Session(get_engine(settings)) as session:
        repo = WeatherRepository(session)
        events = repo.list_daily_events(min_date=ref, max_date=max_date)
        payload: dict[str, object] = {
            "reference_date": ref.isoformat(),
            "horizon_days": settings.weather_forecast_horizon_days,
            "stations": [],
            "cities": [],
            "daily_events": [],
            "bins": [],
        }
        station_ids: set[str] = set()
        city_keys: set[str] = set()
        for ev in events:
            if ev.station_id not in station_ids:
                st = repo.get_station(ev.station_id)
                if st:
                    payload["stations"].append(st.model_dump(mode="json"))
                    station_ids.add(ev.station_id)
            if ev.city_key not in city_keys:
                city = session.get(WeatherCity, ev.city_key)
                if city:
                    payload["cities"].append(city.model_dump(mode="json"))
                    city_keys.add(ev.city_key)
            payload["daily_events"].append(ev.model_dump(mode="json"))
            for b in repo.bins_for_event(ev.id):
                payload["bins"].append(b.model_dump(mode="json"))

    path = out_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    typer.echo(f"Exported {len(events)} events, {len(payload['bins'])} bins → {path}")


@weather_app.command("station-sync")
def weather_station_sync(
    day: str | None = typer.Option(None, "--day", help="YYYY-MM-DD or full horizon"),
) -> None:
    """Fetch Wunderground resolution-station temps (api.weather.com) for synced events."""
    from datetime import date

    from sqlmodel import Session

    settings, log = _bootstrap()
    from poly_bot.persistence.db import get_engine
    from poly_bot.weather.station_sync import WeatherStationSnapshotSync

    syncer = WeatherStationSnapshotSync(settings)
    with Session(get_engine(settings)) as session:
        if day:
            stats = syncer.sync_day(session, date.fromisoformat(day))
        else:
            stats = syncer.sync_horizon(session)
    log.info("weather_station_sync_done", **stats)
    typer.echo(
        typer.style("WU station sync done.", fg=typer.colors.GREEN, bold=True)
        + f"\n  Stations: {stats.get('stations', 0)}"
        + f"\n  OK:       {stats.get('ok', 0)}"
        + f"\n  Errors:   {stats.get('error', 0)}"
        + f"\n  Partial:  {stats.get('partial', 0)}"
    )


@weather_app.command("aviation-sync")
def weather_aviation_sync() -> None:
    """Fetch TAF/METAR for all ICAO stations; refresh forecasts and shadow signals."""
    settings, log = _bootstrap()
    from poly_bot.weather.aviation_sync import WeatherAviationSync

    stats = WeatherAviationSync(settings).sync()
    log.info("weather_aviation_sync_cli", **stats)
    typer.echo(
        typer.style("Aviation weather synced.", fg=typer.colors.GREEN, bold=True)
        + f"\n  Stations:     {stats['stations']}"
        + f"\n  METAR new:    {stats['metar_new']}"
        + f"\n  TAF updated:  {stats['taf_updated']}"
        + f"\n  Forecasts:    {stats['forecasts_written']}"
        + f"\n  Signals:      {stats['signals']}"
    )


@weather_app.command("aviation-status")
def weather_aviation_status() -> None:
    """Coverage summary: TAF/METAR ingest and shadow trading signals."""
    from datetime import date, timedelta

    from sqlalchemy import func
    from sqlmodel import Session, col, select

    settings, _log = _bootstrap()
    from poly_bot.persistence.db import get_engine
    from poly_bot.weather.forecast_schema import (
        WeatherDailyForecast,
        WeatherStationForecastConfig,
        WeatherTafReport,
        WeatherTradingSignal,
    )

    ref = date.today()
    with Session(get_engine(settings)) as session:
        taf_n = len(list(session.exec(select(WeatherTafReport)).all()))
        with_tx = len(
            list(
                session.exec(
                    select(WeatherTafReport).where(WeatherTafReport.coverage == "temp_tx")
                ).all()
            )
        )
        trading_on = len(
            list(
                session.exec(
                    select(WeatherStationForecastConfig).where(
                        WeatherStationForecastConfig.trading_enabled.is_(True)
                    )
                ).all()
            )
        )
        fc_n = len(
            list(
                session.exec(
                    select(WeatherDailyForecast).where(
                        WeatherDailyForecast.forecast_date >= ref
                    )
                ).all()
            )
        )
        sig_n = len(list(session.exec(select(WeatherTradingSignal)).all()))
        edges = list(
            session.exec(
                select(WeatherTradingSignal.edge_hint, func.count())
                .group_by(col(WeatherTradingSignal.edge_hint))
            ).all()
        )
    typer.echo(f"Reference date (UTC): {ref}")
    typer.echo(f"TAF reports: {taf_n} ({with_tx} with TX/TN temps)")
    typer.echo(f"Daily forecasts (>= today): {fc_n}")
    typer.echo(f"Shadow signals: {sig_n}")
    typer.echo(f"Trading-enabled stations: {trading_on}")
    if edges:
        typer.echo("Edge hints:")
        for hint, cnt in edges:
            typer.echo(f"  {hint or '—'}: {cnt}")


@weather_app.command("aviation-serve")
def weather_aviation_serve() -> None:
    """Loop: aviation-sync on WEATHER_AVIATION_INTERVAL_SECONDS."""
    import time

    settings, log = _bootstrap()
    if not settings.weather_aviation_sync_enabled:
        typer.echo("WEATHER_AVIATION_SYNC_ENABLED=false — exiting.")
        raise typer.Exit(1)
    from poly_bot.weather.aviation_sync import WeatherAviationSync

    syncer = WeatherAviationSync(settings)
    interval = settings.weather_aviation_interval_seconds
    typer.echo(f"Aviation sync loop every {interval}s (Ctrl+C to stop)")
    while True:
        try:
            stats = syncer.sync()
            log.info("aviation_serve_cycle", **stats)
        except Exception:
            log.exception("aviation_serve_cycle_failed")
        time.sleep(interval)


@app.command("version")
def version() -> None:
    """Print package version."""
    from poly_bot import __version__

    typer.echo(f"poly-bot {__version__}")
