"""FastAPI dashboard for expiring markets and events."""

from __future__ import annotations

import asyncio
import contextlib
import html
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode

import structlog
from fastapi import FastAPI, Form, HTTPException, Query
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlmodel import Session, select

from poly_bot.catalog.events_view import group_expiring_by_event, market_fee_label
from poly_bot.catalog.expiring import count_expiring_markets, list_expiring_markets
from poly_bot.catalog.models import ExpiringMarket
from poly_bot.catalog.taxonomy import delete_override, list_overrides, upsert_override
from poly_bot.core.fees import fee_summary
from poly_bot.metrics import metrics_enabled, render_metrics, set_catalog_gauges
from poly_bot.persistence.db import get_engine, init_db
from poly_bot.persistence.schema import Market
from poly_bot.settings import Settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger()


async def _discovery_background(settings: Settings) -> None:
    """Discovery scheduler — runs forever with configured intervals."""
    from poly_bot.discovery.scheduler import DiscoveryScheduler

    sched = DiscoveryScheduler(settings)
    sched.init_database()
    await sched.run_forever()


async def _enrich_background(settings: Settings) -> None:
    """Enrich quotes for the lookahead window on a fixed interval."""
    from poly_bot.catalog.enricher import CatalogEnricher

    interval = settings.catalog_enrich_interval_seconds
    while True:
        try:
            with Session(get_engine(settings)) as session:
                enricher = CatalogEnricher(settings, session)
                stats = await enricher.enrich_expiring_window(hours=48, force=False)
                logger.debug("dashboard_enrich_done", **stats)
        except Exception:
            logger.exception("dashboard_enrich_failed")
        await asyncio.sleep(interval)


async def _reconcile_background(settings: Settings) -> None:
    """Close expired markets — runs every hour."""
    from poly_bot.ops.reconcile_scheduler import run_reconcile_loop

    await run_reconcile_loop(get_engine(settings), interval_seconds=3600)


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI, settings: Settings):  # type: ignore[misc]
    tasks = [
        asyncio.create_task(_discovery_background(settings), name="discovery"),
        asyncio.create_task(_enrich_background(settings), name="enrich"),
        asyncio.create_task(_reconcile_background(settings), name="reconcile"),
    ]
    logger.info("dashboard_background_started", tasks=[t.get_name() for t in tasks])
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("dashboard_background_stopped")


def create_app(settings: Settings, *, background: bool = False) -> FastAPI:
    init_db(get_engine(settings))
    lifespan = (
        contextlib.asynccontextmanager(
            lambda app: _lifespan(app, settings)  # type: ignore[arg-type]
        )
        if background
        else None
    )
    app = FastAPI(title="poly_bot catalog", version="0.2.0", lifespan=lifespan)

    static_dir = Path(__file__).resolve().parent / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _render_main_page(settings)

    @app.get("/api/markets")
    def api_markets_live(
        limit: int = Query(default=100, ge=1, le=500),
        domain: str = Query(default=""),
        q: str = Query(default=""),
    ) -> list[dict[str, object]]:
        with Session(get_engine(settings)) as session:
            return _query_live_markets(session, limit=limit, domain=domain, q=q)

    @app.get("/api/stream/prices")
    async def api_stream_prices(
        token_ids: str = Query(default=""),
    ) -> StreamingResponse:
        from poly_bot.exchange.clob_public import ClobPublicClient

        tokens = [t.strip() for t in token_ids.split(",") if t.strip()][:40]

        async def generator() -> object:
            clob = ClobPublicClient(settings)
            while True:
                prices: dict[str, float] = {}
                if tokens:
                    try:
                        prices = await clob.fetch_midpoints(tokens)
                    except Exception:
                        pass
                yield f"data: {json.dumps({'prices': prices, 'ts': datetime.now(UTC).isoformat()})}\n\n"
                await asyncio.sleep(3)

        return StreamingResponse(
            generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/events", response_class=HTMLResponse)
    def events_page(
        hours: float = Query(default=6.0, ge=1, le=168),
        domain: str = Query(default=""),
    ) -> str:
        all_markets = _load_markets(settings, hours=hours)
        markets = [m for m in all_markets if not domain or m.domain == domain]
        evs = group_expiring_by_event(markets)
        return _render_page(
            settings=settings,
            hours=hours,
            all_markets=all_markets,
            markets=markets[:200],
            events=evs[:100],
            domain=domain,
            q="",
            fees_only=False,
            tab="events",
        )

    @app.get("/api/expiring")
    def api_expiring(hours: float = Query(default=6.0, ge=1, le=168)) -> list[dict[str, object]]:
        markets = _load_markets(settings, hours=hours)
        return [_market_json(m) for m in markets]

    @app.get("/api/events")
    def api_events(hours: float = Query(default=6.0, ge=1, le=168)) -> list[dict[str, object]]:
        markets = _load_markets(settings, hours=hours)
        return [
            {
                "event_id": e.event_id,
                "title": e.title,
                "domain": e.domain,
                "market_count": e.market_count,
                "min_hours_left": e.min_hours_left,
                "fee_rate": e.fee_rate,
                "fees_enabled": e.fees_enabled,
            }
            for e in group_expiring_by_event(markets)
        ]

    @app.get("/harvest", response_class=HTMLResponse)
    def harvest_page(
        hot_only: bool = Query(default=True),
    ) -> str:
        from poly_bot.harvest.scan import scan_harvest_opportunities

        with Session(get_engine(settings)) as session:
            opps = scan_harvest_opportunities(
                session,
                settings,
                hot_only=hot_only,
                require_fresh_quotes=False,
            )
        return _render_harvest_page(settings, opps[:200], hot_only=hot_only)

    @app.get("/api/harvest")
    def api_harvest(
        hot_only: bool = Query(default=True),
    ) -> list[dict[str, object]]:
        from poly_bot.harvest.scan import scan_harvest_opportunities

        with Session(get_engine(settings)) as session:
            opps = scan_harvest_opportunities(
                session,
                settings,
                hot_only=hot_only,
                require_fresh_quotes=False,
            )
        return [
            {
                "condition_id": o.condition_id,
                "slug": o.slug,
                "question": o.question,
                "domain": o.domain,
                "side": o.side,
                "outcome_label": o.outcome_label,
                "token_id": o.token_id,
                "ask": o.ask,
                "mid": o.mid,
                "net_yield_pct": round(o.net_yield_pct * 100, 3),
                "fillable_usd": round(o.fillable_usd, 2),
                "est_profit_usd": round(o.est_profit_usd, 3),
                "minutes_left": round(o.minutes_left, 1),
                "window_end_ts": o.window_end_ts.isoformat(),
                "hours_left": o.hours_left,
                "is_live": o.is_live,
                "resolution_kind": o.resolution_kind,
            }
            for o in opps
        ]

    @app.get("/health")
    def health() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/metrics")
    def metrics() -> Response:
        if not metrics_enabled():
            return Response(
                content="# METRICS_ENABLED=false or prometheus-client missing\n",
                media_type="text/plain; charset=utf-8",
            )
        _update_gauges(settings, hours=settings.catalog_expiring_hours)
        body, content_type = render_metrics()
        return Response(content=body, media_type=content_type)

    @app.get("/weather", response_class=HTMLResponse)
    def weather_page(
        day: str = Query(default=""),
        city: str = Query(default=""),
    ) -> str:
        from datetime import date

        from poly_bot.weather.dashboard_views import (
            default_forecast_day,
            load_city_rows,
            load_day_summaries,
            render_weather_page,
            weather_horizon,
        )
        from poly_bot.weather.schema import WeatherDailyEvent

        min_d, max_d = weather_horizon(settings)
        import json as json_mod

        from poly_bot.weather.cards_data import build_weather_cards_payload

        with Session(get_engine(settings)) as session:
            summaries = load_day_summaries(session, settings)
            empty = session.exec(select(WeatherDailyEvent).limit(1)).first() is None
            if day:
                selected = date.fromisoformat(day)
            elif summaries:
                picked = default_forecast_day(summaries)
                selected = picked if picked is not None else min_d
            else:
                selected = min_d
            rows = load_city_rows(session, forecast_date=selected, city_q=city)
            payload = build_weather_cards_payload(
                session,
                settings,
                forecast_date=selected,
                city_q=city,
                refresh_quotes=False,
            )
            initial_json = json_mod.dumps(payload)
        return render_weather_page(
            settings=settings,
            summaries=summaries,
            rows=rows,
            selected_day=selected,
            city_q=city,
            empty_catalog=empty,
            initial_cards_json=initial_json,
            poll_seconds=settings.weather_dashboard_poll_seconds,
        )

    @app.get("/weather/globe", response_class=HTMLResponse)
    def weather_globe_page(day: str = Query(default="")) -> str:
        from datetime import date

        from poly_bot.weather.dashboard_views import (
            default_forecast_day,
            load_day_summaries,
            load_globe_points,
            render_weather_globe_page,
            weather_horizon,
        )

        min_d, _max_d = weather_horizon(settings)
        with Session(get_engine(settings)) as session:
            summaries = load_day_summaries(session, settings)
            if day:
                selected = date.fromisoformat(day)
            elif summaries:
                picked = default_forecast_day(summaries)
                selected = picked if picked is not None else min_d
            else:
                selected = min_d
            points = load_globe_points(session, forecast_date=selected)
        import json as json_mod

        return render_weather_globe_page(
            settings=settings,
            summaries=summaries,
            selected_day=selected,
            points_json=json_mod.dumps(points),
            point_count=len(points),
        )

    @app.get("/weather/board", response_class=HTMLResponse)
    def weather_board_page(day: str = Query(default="")) -> str:
        from datetime import date

        from poly_bot.weather.dashboard_views import (
            default_forecast_day,
            load_city_rows,
            load_day_summaries,
            weather_horizon,
        )
        from poly_bot.weather.forecast_views import load_board_data, render_weather_board_page

        min_d, _max_d = weather_horizon(settings)
        with Session(get_engine(settings)) as session:
            summaries = load_day_summaries(session, settings)
            if day:
                selected = date.fromisoformat(day)
            elif summaries:
                picked = default_forecast_day(summaries)
                selected = picked if picked is not None else min_d
            else:
                selected = min_d
            rows = load_city_rows(session, forecast_date=selected)
            source_keys, matrix, signals = load_board_data(
                session,
                forecast_date=selected,
                rows=rows,
            )
        return render_weather_board_page(
            settings=settings,
            forecast_date=selected,
            rows=rows,
            summaries=summaries,
            source_keys=source_keys,
            matrix=matrix,
            signals=signals,
        )

    @app.get("/weather/aviation", response_class=HTMLResponse)
    def weather_aviation_page() -> str:
        from poly_bot.weather.forecast_schema import (
            WeatherForecastSource,
            WeatherStationForecastConfig,
            WeatherTafReport,
        )
        from poly_bot.weather.forecast_views import render_weather_aviation_page
        from poly_bot.weather.schema import WeatherStation

        with Session(get_engine(settings)) as session:
            stations = list(session.exec(select(WeatherStation)).all())
            configs = list(session.exec(select(WeatherStationForecastConfig)).all())
            sources = list(session.exec(select(WeatherForecastSource)).all())
            tafs = list(session.exec(select(WeatherTafReport)).all())
        return render_weather_aviation_page(
            settings=settings,
            stations=stations,
            configs=configs,
            sources=sources,
            taf_rows=tafs,
        )

    @app.get("/api/weather/forecasts")
    def api_weather_forecasts(day: str = Query(...)) -> list[dict[str, object]]:
        from datetime import date

        from poly_bot.weather.dashboard_views import load_city_rows
        from poly_bot.weather.forecast_views import load_forecast_context_for_rows

        forecast_date = date.fromisoformat(day)
        with Session(get_engine(settings)) as session:
            rows = load_city_rows(session, forecast_date=forecast_date)
            ctx = load_forecast_context_for_rows(session, rows, forecast_date=forecast_date)
        out: list[dict[str, object]] = []
        for row in rows:
            s = ctx.get(row.event.station_id)
            out.append(
                {
                    "city_key": row.event.city_key,
                    "station_id": row.event.station_id,
                    "icao": row.station.icao if row.station else None,
                    "taf_tmax_c": s.taf_tmax_c if s else None,
                    "metar_run_c": s.metar_run_c if s else None,
                    "merged_tmax_c": s.merged_tmax_c if s else None,
                    "merged_source": s.merged_source if s else "",
                    "edge_hint": s.edge_hint if s else "",
                    "taf_coverage": s.taf_coverage if s else "",
                }
            )
        return out

    @app.get("/api/weather/globe")
    def api_weather_globe(day: str = Query(default="")) -> list[dict[str, object]]:
        from datetime import date

        from poly_bot.weather.dashboard_views import load_globe_points, weather_horizon

        min_d, _max_d = weather_horizon(settings)
        forecast_date = date.fromisoformat(day) if day else min_d
        with Session(get_engine(settings)) as session:
            return load_globe_points(session, forecast_date=forecast_date)

    @app.get("/weather/event", response_class=HTMLResponse)
    def weather_event_page(
        city: str = Query(...),
        day: str = Query(...),
    ) -> str:
        from datetime import date

        from poly_bot.weather.dashboard_views import (
            load_bin_ticks,
            load_city_row_by_keys,
            render_weather_detail_page,
        )

        forecast_date = date.fromisoformat(day)
        with Session(get_engine(settings)) as session:
            row = load_city_row_by_keys(
                session,
                forecast_date=forecast_date,
                city_key=city.strip().lower(),
            )
            if row is None:
                return HTMLResponse(
                    "<p>Event not found. <a href='/weather'>Back</a></p>",
                    status_code=404,
                )
            ticks = load_bin_ticks(session, row.bins)
            from poly_bot.weather.forecast_repo import WeatherForecastRepository
            from poly_bot.weather.forecast_schema import WeatherTradingSignal

            sig = session.get(WeatherTradingSignal, row.event.id)
            taf = WeatherForecastRepository(session).get_taf(row.event.station_id)
            taf_raw = taf.raw_taf if taf else ""
        return render_weather_detail_page(
            settings=settings,
            row=row,
            ticks=ticks,
            signal=sig,
            taf_raw=taf_raw,
        )

    @app.get("/api/weather/cards")
    def api_weather_cards(
        day: str = Query(default=""),
        city: str = Query(default=""),
        refresh_quotes: bool = Query(default=False),
    ) -> dict[str, object]:
        from datetime import date

        from poly_bot.weather.cards_data import build_weather_cards_payload
        from poly_bot.weather.dashboard_views import weather_horizon

        min_d, _max_d = weather_horizon(settings)
        forecast_date = date.fromisoformat(day) if day else min_d
        with Session(get_engine(settings)) as session:
            return build_weather_cards_payload(
                session,
                settings,
                forecast_date=forecast_date,
                city_q=city,
                refresh_quotes=refresh_quotes,
            )

    @app.get("/api/weather")
    def api_weather(
        day: str = Query(default=""),
    ) -> list[dict[str, object]]:
        from datetime import date

        from poly_bot.weather.dashboard_views import load_city_rows, weather_horizon

        min_d, _max_d = weather_horizon(settings)
        forecast_date = date.fromisoformat(day) if day else min_d
        with Session(get_engine(settings)) as session:
            rows = load_city_rows(session, forecast_date=forecast_date)
        return [
            {
                "event_id": r.event.id,
                "city_key": r.event.city_key,
                "forecast_date": r.event.forecast_date.isoformat(),
                "title": r.event.title,
                "bin_count": len(r.bins),
                "station_icao": r.station.icao if r.station else None,
                "latitude": r.station.latitude if r.station else None,
                "longitude": r.station.longitude if r.station else None,
                "timezone": r.station.timezone if r.station else None,
            }
            for r in rows
        ]

    @app.get("/api/weather/event/{event_id}")
    def api_weather_event(event_id: str) -> dict[str, object]:
        from poly_bot.weather.dashboard_views import (
            load_bin_ticks,
            load_city_row_by_keys,
            weather_event_json,
        )
        from poly_bot.weather.schema import WeatherDailyEvent

        with Session(get_engine(settings)) as session:
            ev = session.get(WeatherDailyEvent, event_id)
            if ev is None:
                raise HTTPException(status_code=404, detail="not found")
            row = load_city_row_by_keys(
                session,
                forecast_date=ev.forecast_date,
                city_key=ev.city_key,
            )
            if row is None:
                raise HTTPException(status_code=404, detail="not found")
            ticks = load_bin_ticks(session, row.bins)
        return weather_event_json(row, ticks)

    @app.get("/api/weather/stream")
    async def api_weather_stream(
        event_id: str = Query(...),
    ) -> StreamingResponse:
        """SSE — REST CLOB refresh + DB quotes for one weather event (5s)."""

        async def event_generator() -> object:
            from poly_bot.weather.dashboard_views import weather_event_live_snapshot
            from poly_bot.weather.quotes import refresh_event_quotes_from_clob

            while True:
                payload: dict[str, object]
                with Session(get_engine(settings)) as session:
                    try:
                        await refresh_event_quotes_from_clob(session, settings, event_id=event_id)
                    except Exception:
                        pass
                    payload = weather_event_live_snapshot(session, event_id=event_id)
                yield f"data: {json.dumps(payload)}\n\n"
                await asyncio.sleep(5)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/taxonomy", response_class=HTMLResponse)
    def taxonomy_page(
        domain: str = Query(default="unknown"),
        q: str = Query(default=""),
        limit: int = Query(default=200, ge=10, le=2000),
    ) -> str:
        with Session(get_engine(settings)) as session:
            overrides = list_overrides(session)
            samples = _sample_markets_for_classification(session, domain=domain, q=q, limit=limit)
        return _render_taxonomy_page(
            settings=settings,
            overrides=overrides,
            samples=samples,
            domain=domain,
            q=q,
        )

    @app.post("/taxonomy/add")
    def taxonomy_add(
        kind: str = Form(...),
        key: str = Form(...),
        domain: str = Form(default=""),
        structure: str = Form(default=""),
        resolution_source: str = Form(default=""),
        note: str = Form(default=""),
    ) -> RedirectResponse:
        with Session(get_engine(settings)) as session:
            upsert_override(
                session,
                kind=kind,  # type: ignore[arg-type]
                key=key.strip(),
                domain=domain.strip() or None,
                structure=structure.strip() or None,
                resolution_source=resolution_source.strip() or None,
                note=note.strip(),
            )
        return RedirectResponse("/taxonomy", status_code=303)

    @app.post("/taxonomy/delete/{override_id}")
    def taxonomy_delete(override_id: int) -> RedirectResponse:
        with Session(get_engine(settings)) as session:
            delete_override(session, override_id)
        return RedirectResponse("/taxonomy", status_code=303)

    @app.get("/api/taxonomy")
    def api_taxonomy() -> list[dict[str, object]]:
        with Session(get_engine(settings)) as session:
            rows = list_overrides(session)
        return [
            {
                "id": r.id,
                "kind": r.kind,
                "key": r.key,
                "domain": r.domain,
                "structure": r.structure,
                "resolution_source": r.resolution_source,
                "note": r.note,
            }
            for r in rows
        ]

    @app.get("/api/health")
    def api_health(hours: float = Query(default=6.0, ge=1, le=168)) -> dict[str, object]:
        return _live_snapshot(settings, hours=hours)

    @app.get("/api/reconcile")
    def api_reconcile() -> dict[str, object]:
        from poly_bot.persistence.catalog_state import (
            KEY_LAST_RECONCILE_AT,
            KEY_LAST_RECONCILE_REPORT,
            CatalogStateStore,
        )

        with Session(get_engine(settings)) as session:
            store = CatalogStateStore(session)
            last_at = store.get_datetime(KEY_LAST_RECONCILE_AT)
            report_raw = store.get(KEY_LAST_RECONCILE_REPORT)
        report: dict[str, object] = {}
        if report_raw:
            try:
                report = json.loads(report_raw)
            except json.JSONDecodeError:
                report = {}
        return {
            "last_at": last_at.isoformat() if last_at else None,
            "age_s": int((datetime.now(UTC) - last_at).total_seconds()) if last_at else None,
            "report": report,
        }

    @app.get("/api/stream")
    async def api_stream(
        hours: float = Query(default=6.0, ge=1, le=168),
        domain: str = Query(default=""),
        q: str = Query(default=""),
        fees_only: bool = Query(default=False),
    ) -> StreamingResponse:
        """SSE stream — health snapshot + live quote ticks every 5s."""

        async def event_generator() -> object:
            while True:
                snapshot = _live_snapshot(
                    settings,
                    hours=hours,
                    domain=domain,
                    q=q,
                    fees_only=fees_only,
                    include_quotes=True,
                )
                yield f"data: {json.dumps(snapshot)}\n\n"
                await asyncio.sleep(5)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    return app


def _update_gauges(settings: Settings, *, hours: float) -> None:
    from poly_bot.discovery.runner import DiscoveryRunner

    runner = DiscoveryRunner(settings)
    stats = runner.stats()
    with Session(get_engine(settings)) as session:
        expiring = count_expiring_markets(
            session,
            hours=hours,
            sports_in_play_hours=settings.catalog_sports_in_play_hours,
        )
    set_catalog_gauges(
        active_markets=stats["active_markets"],
        markets_with_fees=stats["markets_with_fees"],
        expiring_6h=expiring,
    )


def _load_markets(settings: Settings, *, hours: float) -> list[ExpiringMarket]:
    with Session(get_engine(settings)) as session:
        return list_expiring_markets(
            session,
            hours=hours,
            sports_in_play_hours=settings.catalog_sports_in_play_hours,
        )


def _live_snapshot(
    settings: Settings,
    *,
    hours: float,
    domain: str = "",
    q: str = "",
    fees_only: bool = False,
    include_quotes: bool = False,
    quote_limit: int = 500,
) -> dict[str, object]:
    """Stats for SSE/health. Optional ``quotes`` map for visible table rows."""
    from poly_bot.persistence.catalog_state import (
        KEY_LAST_DELTA_SCAN_AT,
        KEY_LAST_DISCOVERY_ACTIVITY_AT,
        KEY_LAST_FULL_SCAN_AT,
        KEY_LAST_RECONCILE_AT,
        CatalogStateStore,
    )

    now = datetime.now(UTC)
    engine = get_engine(settings)

    with engine.connect() as conn:
        quote_row = conn.execute(text("SELECT MAX(updated_at) FROM token_quotes")).one()
        markets_active = int(
            conn.execute(text("SELECT COUNT(*) FROM markets WHERE active = TRUE")).scalar() or 0
        )
        fees_with = int(
            conn.execute(
                text("SELECT COUNT(*) FROM markets WHERE fees_enabled = TRUE AND active = TRUE")
            ).scalar()
            or 0
        )
    quote_last = _coerce_dt(quote_row[0] if quote_row else None)

    with Session(engine) as session:
        in_play = settings.catalog_sports_in_play_hours
        expiring = count_expiring_markets(session, hours=hours, sports_in_play_hours=in_play)
        markets = list_expiring_markets(session, hours=hours, sports_in_play_hours=in_play)
        events = group_expiring_by_event(markets)
        fee_in_window = sum(1 for m in markets if m.fees_enabled or m.event_fees_enabled)
        store = CatalogStateStore(session)
        last_full = store.get_datetime(KEY_LAST_FULL_SCAN_AT)
        last_delta = store.get_datetime(KEY_LAST_DELTA_SCAN_AT)
        last_discovery = store.get_datetime(KEY_LAST_DISCOVERY_ACTIVITY_AT)
        last_reconcile = store.get_datetime(KEY_LAST_RECONCILE_AT)

    snapshot: dict[str, object] = {
        "now": now.isoformat(),
        "hours": hours,
        "quote_age_s": _age_s(now, quote_last),
        "quote_last": quote_last.isoformat() if quote_last else None,
        "delta_age_s": _age_s(now, last_delta),
        "full_age_s": _age_s(now, last_full),
        "discovery_activity_age_s": _age_s(now, last_discovery),
        "reconcile_age_s": _age_s(now, last_reconcile),
        "counts": {
            "markets_active": markets_active,
            "markets_with_fees": fees_with,
            "expiring_window": expiring,
            "events_window": len(events),
            "fees_in_window": fee_in_window,
        },
    }
    if include_quotes:
        visible = _filter_markets(markets, domain=domain, q=q, fees_only=fees_only)[:quote_limit]
        snapshot["quotes"] = _quotes_payload(visible)
    return snapshot


def _quotes_payload(markets: list[ExpiringMarket]) -> dict[str, dict[str, float | None]]:
    """Compact yes/no/spread ticks keyed by condition_id (SSE payload)."""
    out: dict[str, dict[str, float | None]] = {}
    for m in markets:
        spr = m.yes_spread if m.yes_spread is not None else m.no_spread
        out[m.condition_id] = {"y": m.yes_mid, "n": m.no_mid, "s": spr}
    return out


def _coerce_dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    except (ValueError, TypeError):
        return None


def _age_s(now: datetime, dt: datetime | None) -> int | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int((now - dt).total_seconds())


def _filter_markets(
    markets: list[ExpiringMarket],
    *,
    domain: str,
    q: str,
    fees_only: bool,
) -> list[ExpiringMarket]:
    out = markets
    if domain:
        out = [m for m in out if m.domain == domain]
    if fees_only:
        out = [m for m in out if m.fees_enabled or m.event_fees_enabled]
    if q:
        ql = q.lower()
        out = [
            m
            for m in out
            if ql in m.question.lower()
            or ql in m.slug.lower()
            or ql in (m.event_title or "").lower()
        ]
    return out


def _render_page(
    *,
    settings: Settings,
    hours: float,
    all_markets: list[ExpiringMarket],
    markets: list[ExpiringMarket],
    events: list[object],
    domain: str,
    q: str,
    fees_only: bool,
    tab: str,
) -> str:
    domains = sorted({m.domain for m in all_markets})
    fee_count = sum(1 for m in markets if m.fees_enabled)
    market_rows = "\n".join(_market_row(m) for m in markets)
    event_rows = "\n".join(_event_row(e) for e in events)  # type: ignore[arg-type]

    params = {"hours": hours, "domain": domain, "q": q}
    if fees_only:
        params["fees_only"] = "true"
    query = urlencode({k: v for k, v in params.items() if v not in ("", False, None)})
    stream_qs = urlencode(
        {
            "hours": str(hours),
            "domain": domain,
            "q": q,
            **({"fees_only": "true"} if fees_only else {}),
        }
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>poly_bot — expiring {hours:.0f}h</title>
<style>
:root {{
  --bg: #0b0f14;
  --panel: #121a24;
  --border: #243044;
  --text: #e8eef7;
  --muted: #8b9cb3;
  --accent: #3d8bfd;
  --green: #3dd68c;
  --amber: #f5a524;
  --red: #f2555a;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  font-family: "SF Pro Text", system-ui, -apple-system, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.45;
}}
header {{
  padding: 1rem 1.25rem;
  border-bottom: 1px solid var(--border);
  background: linear-gradient(180deg, #141c28 0%, var(--bg) 100%);
  position: sticky;
  top: 0;
  z-index: 10;
}}
h1 {{ margin: 0 0 0.25rem; font-size: 1.35rem; font-weight: 600; }}
.sub {{ color: var(--muted); font-size: 0.85rem; }}
.stats {{
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  margin-top: 0.75rem;
}}
.stat {{
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 0.45rem 0.75rem;
  font-size: 0.8rem;
}}
.stat b {{ color: var(--accent); }}
nav.tabs {{ margin-top: 0.75rem; display: flex; gap: 0.5rem; }}
nav.tabs a {{
  color: var(--muted);
  text-decoration: none;
  padding: 0.35rem 0.75rem;
  border-radius: 6px;
  border: 1px solid transparent;
  font-size: 0.85rem;
}}
nav.tabs a.active {{
  color: var(--text);
  border-color: var(--border);
  background: var(--panel);
}}
.filters {{
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  align-items: center;
  margin-top: 0.75rem;
}}
.filters input, .filters select {{
  background: var(--panel);
  border: 1px solid var(--border);
  color: var(--text);
  border-radius: 6px;
  padding: 0.4rem 0.6rem;
  font-size: 0.85rem;
}}
.filters button {{
  background: var(--accent);
  color: #fff;
  border: none;
  border-radius: 6px;
  padding: 0.45rem 0.9rem;
  cursor: pointer;
  font-weight: 500;
}}
main {{ padding: 1rem 1.25rem 2rem; }}
.table-wrap {{
  overflow: auto;
  border: 1px solid var(--border);
  border-radius: 10px;
  background: var(--panel);
}}
table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
th {{
  text-align: left;
  padding: 0.55rem 0.65rem;
  background: #1a2433;
  color: var(--muted);
  font-weight: 500;
  position: sticky;
  top: 0;
  white-space: nowrap;
}}
td {{ padding: 0.45rem 0.65rem; border-top: 1px solid var(--border); vertical-align: top; }}
tr:hover td {{ background: rgba(61, 139, 253, 0.06); }}
.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.tag {{
  display: inline-block;
  padding: 0.1rem 0.4rem;
  border-radius: 4px;
  font-size: 0.72rem;
  background: #1e2a3d;
  color: var(--muted);
}}
.urgent {{ color: var(--red); font-weight: 600; }}
.soon {{ color: var(--amber); }}
.fee-yes {{ color: var(--amber); }}
.fee-no {{ color: var(--muted); }}
.q {{ max-width: 280px; }}
.hidden {{ display: none; }}
td.tick-flash {{ animation: tickFlash 0.45s ease-out; }}
@keyframes tickFlash {{
  from {{ background: rgba(61, 214, 140, 0.22); }}
  to {{ background: transparent; }}
}}
footer {{ color: var(--muted); font-size: 0.75rem; margin-top: 1rem; }}
.live-dot {{
  display: inline-block;
  width: 0.55rem;
  height: 0.55rem;
  border-radius: 50%;
  background: var(--muted);
  margin-right: 0.35rem;
  vertical-align: 0.05rem;
  box-shadow: 0 0 0 0 currentColor;
}}
.live-dot.ok {{
  background: var(--green);
  animation: pulse 1.6s ease-out infinite;
}}
.live-dot.warn {{ background: var(--amber); }}
.live-dot.err {{ background: var(--red); }}
@keyframes pulse {{
  0% {{ box-shadow: 0 0 0 0 rgba(61, 214, 140, 0.55); }}
  70% {{ box-shadow: 0 0 0 6px rgba(61, 214, 140, 0); }}
  100% {{ box-shadow: 0 0 0 0 rgba(61, 214, 140, 0); }}
}}
</style>
</head>
<body>
<header>
  <h1>poly_bot catalog</h1>
  <div class="sub">
    Window {hours:.0f}h ·
    <span class="live-dot" id="live-dot" title="SSE status"></span>
    <span id="live-status">connecting…</span> ·
    quotes <span id="quote-age">—</span> ·
    delta <span id="delta-age">—</span>
  </div>
  <div class="stats">
    <div class="stat">Markets <b id="stat-markets">{len(markets)}</b></div>
    <div class="stat">Events <b id="stat-events">{len(events)}</b></div>
    <div class="stat">With fees <b id="stat-fees">{fee_count}</b></div>
    <div class="stat">Active catalog <b id="stat-active">—</b></div>
    <div class="stat">Updated <b id="stat-updated">—</b></div>
  </div>
  <nav class="tabs">
    <a href="/?{query}" class="{"active" if tab == "markets" else ""}">Markets</a>
    <a href="/events?hours={hours}" class="{"active" if tab == "events" else ""}">Events & fees</a>
    <a href="/harvest">Harvest</a>
    <a href="/weather">Weather</a>
    <a href="/taxonomy">Taxonomy</a>
  </nav>
  <form class="filters" method="get" action="/">
    <input type="hidden" name="hours" value="{hours}"/>
    <select name="domain">
      <option value="">All domains</option>
      {"".join(f'<option value="{html.escape(d)}" {"selected" if d == domain else ""}>{html.escape(d)}</option>' for d in domains)}
    </select>
    <input type="search" name="q" value="{html.escape(q)}" placeholder="Search question…" size="28"/>
    <label><input type="checkbox" name="fees_only" value="true" {"checked" if fees_only else ""}/> Fees only</label>
    <button type="submit">Apply</button>
  </form>
</header>
<main>
  <div id="panel-markets" class="{"hidden" if tab != "markets" else ""}">
    <div class="table-wrap">
      <table id="markets-table">
        <thead><tr>
          <th>Kickoff (UTC)</th><th>Status</th><th>Domain</th>
          <th class="num">Yes</th><th class="num">No</th><th class="num">Spr</th>
          <th class="num">Vol 24h</th><th class="num">OI</th><th>Fee @ mid</th>
          <th>Question</th>
        </tr></thead>
        <tbody>{market_rows}</tbody>
      </table>
    </div>
  </div>
  <div id="panel-events" class="{"hidden" if tab != "events" else ""}">
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Event</th><th>Domain</th><th class="num">Mkts</th>
          <th class="num">Left</th><th class="num">Vol 24h</th><th>Fee rate</th><th class="num">Mkts w/ fee</th>
        </tr></thead>
        <tbody>{event_rows}</tbody>
      </table>
    </div>
  </div>
  <footer>DB: {html.escape(str(settings.catalog_db_path))}</footer>
</main>
<script>
document.getElementById('markets-table')?.addEventListener('click', (e) => {{
  const th = e.target.closest('th');
  if (!th) return;
  const table = th.closest('table');
  const idx = Array.from(th.parentNode.children).indexOf(th);
  const rows = [...table.querySelectorAll('tbody tr')];
  const asc = th.dataset.sort !== 'asc';
  rows.sort((a,b) => {{
    const av = a.children[idx]?.textContent?.trim() || '';
    const bv = b.children[idx]?.textContent?.trim() || '';
    const an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  }});
  rows.forEach(r => table.querySelector('tbody').appendChild(r));
  table.querySelectorAll('th').forEach(h => delete h.dataset.sort);
  th.dataset.sort = asc ? 'asc' : 'desc';
}});

(() => {{
  const dot = document.getElementById('live-dot');
  const status = document.getElementById('live-status');
  const qAge = document.getElementById('quote-age');
  const dAge = document.getElementById('delta-age');
  const sMk = document.getElementById('stat-markets');
  const sEv = document.getElementById('stat-events');
  const sFe = document.getElementById('stat-fees');
  const sAc = document.getElementById('stat-active');
  const sUp = document.getElementById('stat-updated');
  if (!dot) return;
  const HOURS = {hours};
  const STREAM_QS = {json.dumps(stream_qs)};
  const rowByCid = new Map();
  document.querySelectorAll('#markets-table tbody tr[data-cid]').forEach((tr) => {{
    rowByCid.set(tr.dataset.cid, tr);
  }});
  function fmtNum(v, decimals) {{
    if (v === null || v === undefined) return '—';
    return Number(v).toFixed(decimals);
  }}
  function setCell(td, text, flash) {{
    if (!td || td.textContent === text) return;
    td.textContent = text;
    if (flash) {{
      td.classList.remove('tick-flash');
      void td.offsetWidth;
      td.classList.add('tick-flash');
    }}
  }}
  function applyQuotes(quotes) {{
    if (!quotes) return;
    for (const [cid, q] of Object.entries(quotes)) {{
      const row = rowByCid.get(cid);
      if (!row) continue;
      setCell(row.querySelector('[data-col="yes"]'), fmtNum(q.y, 3), true);
      setCell(row.querySelector('[data-col="no"]'), fmtNum(q.n, 3), true);
      setCell(row.querySelector('[data-col="spr"]'), fmtNum(q.s, 3), true);
    }}
  }}
  function fmtAge(seconds) {{
    if (seconds === null || seconds === undefined) return '—';
    if (seconds < 60) return seconds + 's';
    if (seconds < 3600) return Math.round(seconds / 60) + 'm';
    return (seconds / 3600).toFixed(1) + 'h';
  }}
  function setStatus(cls, label) {{
    dot.classList.remove('ok', 'warn', 'err');
    if (cls) dot.classList.add(cls);
    status.textContent = label;
  }}
  function apply(snap) {{
    applyQuotes(snap.quotes);
    qAge.textContent = fmtAge(snap.quote_age_s);
    dAge.textContent = fmtAge(snap.discovery_activity_age_s ?? snap.delta_age_s);
    sMk.textContent = snap.counts.expiring_window;
    sEv.textContent = snap.counts.events_window;
    sFe.textContent = snap.counts.fees_in_window;
    sAc.textContent = snap.counts.markets_active.toLocaleString();
    sUp.textContent = new Date(snap.now).toLocaleTimeString();
    const q = snap.quote_age_s;
    if (q === null || q === undefined) setStatus('warn', 'no quotes yet');
    else if (q > 600) setStatus('err', 'stale ' + fmtAge(q));
    else if (q > 60) setStatus('warn', 'lagging ' + fmtAge(q));
    else setStatus('ok', 'live');
  }}
  let es = null;
  function connect() {{
    setStatus(null, 'connecting…');
    es = new EventSource('/api/stream?' + STREAM_QS);
    es.onmessage = (e) => {{
      try {{ apply(JSON.parse(e.data)); }} catch (err) {{}}
    }};
    es.onerror = () => {{
      setStatus('err', 'offline (reconnecting…)');
      es.close();
      setTimeout(connect, 3000);
    }};
  }}
  connect();
}})();
</script>
</body>
</html>"""


def _market_row(m: ExpiringMarket) -> str:
    left_cls = "urgent" if m.hours_left < 1 and not m.is_live else ""
    if m.is_live or m.hours_left < 3:
        left_cls = "soon"
    mid = m.yes_mid if m.yes_mid is not None else m.no_mid
    fee_lbl = market_fee_label(m)
    fee_cls = "fee-yes" if m.fees_enabled or m.event_fees_enabled else "fee-no"
    spr = m.yes_spread if m.yes_spread is not None else m.no_spread
    kickoff = m.game_start_ts or m.end_ts
    if m.is_live:
        status = (
            f'<span class="tag" style="color:var(--green)">LIVE</span> {m.hours_left:.1f}h window'
        )
    elif m.resolution_kind == "in_play" and m.hours_to_kickoff is not None:
        status = f"{m.hours_to_kickoff:.1f}h kick · {m.hours_left:.1f}h window"
    elif m.hours_left < 1:
        status = f'<span class="urgent">{m.hours_left:.1f}h</span>'
    else:
        status = f"{m.hours_left:.1f}h"
    cid = html.escape(m.condition_id, quote=True)
    return (
        f'<tr data-cid="{cid}">'
        f"<td>{html.escape(kickoff.strftime('%m-%d %H:%M'))}</td>"
        f'<td class="num {left_cls}">{status}</td>'
        f'<td><span class="tag">{html.escape(m.domain)}</span></td>'
        f'<td class="num" data-col="yes">{_num(m.yes_mid, 3)}</td>'
        f'<td class="num" data-col="no">{_num(m.no_mid, 3)}</td>'
        f'<td class="num" data-col="spr">{_num(spr, 3)}</td>'
        f'<td class="num">{_fmt_vol(m.volume_24h)}</td>'
        f'<td class="num">{_fmt_vol(m.open_interest)}</td>'
        f'<td class="{fee_cls}">{html.escape(fee_lbl)}</td>'
        f'<td class="q">{html.escape(m.question)}</td>'
        "</tr>"
    )


def _event_row(e: object) -> str:
    from poly_bot.catalog.events_view import ExpiringEvent

    assert isinstance(e, ExpiringEvent)
    rate = f"{e.fee_rate * 100:.1f}%" if e.fee_rate is not None else "—"
    left = f"{e.min_hours_left:.1f}–{e.max_hours_left:.1f}h"
    return (
        "<tr>"
        f"<td><strong>{html.escape(e.title[:70])}</strong>"
        f'<br/><span class="tag">{html.escape(e.event_id[:12])}…</span></td>'
        f'<td><span class="tag">{html.escape(e.domain)}</span></td>'
        f'<td class="num">{e.market_count}</td>'
        f'<td class="num">{left}</td>'
        f'<td class="num">{_fmt_vol(e.total_volume_24h)}</td>'
        f"<td>{html.escape(rate)}</td>"
        f'<td class="num">{e.markets_with_fees}/{e.market_count}</td>'
        "</tr>"
    )


def _num(v: float | None, decimals: int) -> str:
    if v is None:
        return "—"
    return f"{v:.{decimals}f}"


def _fmt_vol(v: float | None) -> str:
    if v is None:
        return "—"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v / 1_000:.1f}k"
    return f"{v:.0f}"


def _render_harvest_page(settings: Settings, opps: list, *, hot_only: bool) -> str:
    from poly_bot.harvest.scan import HarvestOpportunity

    hot = settings.catalog_quote_hot_hours
    min_pct = settings.harvest_min_net_yield_pct * 100
    mode = f"HOT ≤{hot:.0f}h" if hot_only else "full window"
    rows = []
    for o in opps:
        assert isinstance(o, HarvestOpportunity)
        live = "LIVE" if o.is_live else ""
        rows.append(
            "<tr>"
            f"<td class='num'>{o.minutes_left:.0f}m</td>"
            f"<td class='num'>{o.net_yield_pct * 100:.2f}%</td>"
            f"<td>{html.escape(o.side.upper())} {html.escape(o.outcome_label[:14])}</td>"
            f"<td class='num'>{o.ask:.3f}</td>"
            f"<td class='num'>{o.fillable_usd:.0f}</td>"
            f"<td class='num'>{o.est_profit_usd:.2f}</td>"
            f"<td>{html.escape(o.domain)}</td>"
            f"<td>{live}</td>"
            f"<td>{html.escape(o.question[:80])}</td>"
            "</tr>"
        )
    body = (
        "\n".join(rows)
        if rows
        else "<tr><td colspan='9'>No priced opportunities in band.</td></tr>"
    )
    alt = f"/harvest?hot_only={'false' if hot_only else 'true'}"
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Harvest · poly_bot</title>
<style>
body {{ font-family: system-ui, sans-serif; background: #0a0e14; color: #e8eef7; margin: 0; padding: 1rem 1.25rem; }}
a {{ color: #6eb6ff; }}
header {{ margin-bottom: 1rem; }}
table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
th, td {{ border-bottom: 1px solid #243044; padding: 0.45rem 0.5rem; text-align: left; }}
th {{ color: #8b9cb3; }}
.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.sub {{ color: #8b9cb3; font-size: 0.9rem; }}
</style></head><body>
<header>
  <h1 style="margin:0">Harvest targets</h1>
  <p class="sub">{mode} · ask {settings.harvest_min_buy_price:.3f}–{settings.harvest_max_buy_price:.3f} · net ≥ {min_pct:.1f}% · ${settings.harvest_order_usd:.0f} · sorted by window end</p>
  <p><a href="/">Catalog</a> · <a href="{alt}">{"Show all window" if hot_only else "HOT only"}</a> · CLI: <code>poly-bot harvest scan --log</code></p>
</header>
<table>
  <thead><tr>
    <th class="num">Left</th><th class="num">Net%</th><th>Side</th><th class="num">Ask</th>
    <th class="num">Fill$</th><th class="num">Est$</th><th>Domain</th><th></th><th>Question</th>
  </tr></thead>
  <tbody>{body}</tbody>
</table>
</body></html>"""


def _market_json(m: ExpiringMarket) -> dict[str, object]:
    mid = m.yes_mid if m.yes_mid is not None else m.no_mid
    summary = fee_summary(
        price=mid,
        fees_enabled=m.fees_enabled or m.event_fees_enabled,
        fee_type=m.fee_type,
        fee_schedule=m.fee_schedule,
        domain=m.domain,
        maker_base_fee=m.maker_base_fee,
        taker_base_fee=m.taker_base_fee or m.event_taker_base_fee,
    )
    return {
        "condition_id": m.condition_id,
        "slug": m.slug,
        "question": m.question,
        "domain": m.domain,
        "end_ts": m.end_ts.isoformat(),
        "window_end_ts": m.window_end_ts.isoformat() if m.window_end_ts else None,
        "resolution_kind": m.resolution_kind,
        "hours_to_kickoff": m.hours_to_kickoff,
        "hours_left": m.hours_left,
        "is_live": m.is_live,
        "yes_mid": m.yes_mid,
        "no_mid": m.no_mid,
        "open_interest": m.open_interest,
        "volume_24h": m.volume_24h,
        "tags": m.tags,
        "fees": summary,
    }


def _sample_markets_for_classification(
    session: Session,
    *,
    domain: str,
    q: str,
    limit: int,
) -> list[Market]:
    from sqlmodel import select

    stmt = select(Market)
    if domain:
        stmt = stmt.where(Market.domain == domain)
    if q:
        like = f"%{q.lower()}%"
        from sqlalchemy import func as sa_func

        stmt = stmt.where(sa_func.lower(Market.question).like(like))
    stmt = stmt.where(Market.active == True).limit(limit)  # noqa: E712
    return list(session.exec(stmt).all())


def _render_taxonomy_page(
    *,
    settings: Settings,
    overrides: list[object],
    samples: list[Market],
    domain: str,
    q: str,
) -> str:
    override_rows = "\n".join(_override_row(o) for o in overrides)  # type: ignore[arg-type]
    sample_rows = "\n".join(_sample_row(m) for m in samples)
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/>
<title>poly_bot — taxonomy</title>
<style>
body {{ background:#0b0f14; color:#e8eef7; font-family:system-ui,sans-serif;
  margin:0; padding:1rem 1.25rem; line-height:1.45; }}
h1 {{ font-size:1.25rem; margin:0 0 .5rem; }}
nav.tabs {{ display:flex; gap:.5rem; margin:.5rem 0 1rem; }}
nav.tabs a {{ color:#8b9cb3; text-decoration:none; padding:.35rem .75rem;
  border-radius:6px; border:1px solid transparent; font-size:.85rem; }}
nav.tabs a.active {{ color:#e8eef7; background:#121a24; border-color:#243044; }}
.section {{ background:#121a24; border:1px solid #243044; border-radius:10px;
  padding:1rem; margin-bottom:1rem; }}
table {{ width:100%; border-collapse:collapse; font-size:.82rem; }}
th,td {{ padding:.45rem .65rem; border-top:1px solid #243044; text-align:left; }}
th {{ background:#1a2433; color:#8b9cb3; font-weight:500; }}
input,select,button {{ background:#0b0f14; color:#e8eef7;
  border:1px solid #243044; border-radius:6px; padding:.4rem .6rem; font-size:.85rem; }}
button {{ background:#3d8bfd; border:none; color:#fff; cursor:pointer; }}
button.danger {{ background:#f2555a; }}
form.inline {{ display:inline; }}
.muted {{ color:#8b9cb3; font-size:.78rem; }}
.tag {{ display:inline-block; padding:.1rem .4rem; border-radius:4px;
  background:#1e2a3d; color:#8b9cb3; font-size:.7rem; }}
</style>
</head><body>
<h1>Taxonomy overrides</h1>
<nav class="tabs">
  <a href="/">Markets</a>
  <a href="/events">Events &amp; fees</a>
  <a href="/weather">Weather</a>
  <a href="/weather/globe">Globe</a>
  <a href="/taxonomy" class="active">Taxonomy</a>
</nav>

<div class="section">
  <h2 style="margin-top:0;font-size:1rem">Add override</h2>
  <form method="post" action="/taxonomy/add" style="display:flex;flex-wrap:wrap;gap:.5rem;align-items:center">
    <select name="kind">
      <option value="tag">tag</option>
      <option value="condition_id">condition_id</option>
      <option value="slug_prefix">slug_prefix</option>
    </select>
    <input name="key" placeholder="key (tag slug / condition_id / slug prefix)" size="40" required/>
    <input name="domain" placeholder="domain (crypto, sports, …)"/>
    <input name="structure" placeholder="structure"/>
    <input name="resolution_source" placeholder="resolution_source"/>
    <input name="note" placeholder="note" size="20"/>
    <button type="submit">Save</button>
  </form>
  <div class="muted" style="margin-top:.5rem">
    Run <code>poly-bot catalog reclassify</code> after adding to apply to existing markets.
  </div>
</div>

<div class="section">
  <h2 style="margin-top:0;font-size:1rem">Active overrides ({len(overrides)})</h2>
  <table>
    <thead><tr><th>Kind</th><th>Key</th><th>Domain</th><th>Struct</th><th>Resolution</th><th>Note</th><th></th></tr></thead>
    <tbody>{override_rows or '<tr><td colspan="7" class="muted">No overrides yet.</td></tr>'}</tbody>
  </table>
</div>

<div class="section">
  <h2 style="margin-top:0;font-size:1rem">Sample markets (filter to find candidates)</h2>
  <form method="get" action="/taxonomy" style="display:flex;gap:.5rem;margin-bottom:.75rem">
    <input name="domain" value="{html.escape(domain)}" placeholder="domain"/>
    <input name="q" value="{html.escape(q)}" placeholder="search question" size="40"/>
    <button type="submit">Filter</button>
  </form>
  <table>
    <thead><tr><th>Domain</th><th>Slug</th><th>Question</th><th>Condition</th><th></th></tr></thead>
    <tbody>{sample_rows or '<tr><td colspan="5" class="muted">No matches.</td></tr>'}</tbody>
  </table>
</div>

</body></html>"""


def _override_row(o: object) -> str:
    from poly_bot.persistence.schema import TaxonomyOverride

    assert isinstance(o, TaxonomyOverride)
    return (
        "<tr>"
        f"<td><span class='tag'>{html.escape(o.kind)}</span></td>"
        f"<td><code>{html.escape(o.key[:50])}</code></td>"
        f"<td>{html.escape(o.domain or '—')}</td>"
        f"<td>{html.escape(o.structure or '—')}</td>"
        f"<td>{html.escape(o.resolution_source or '—')}</td>"
        f"<td class='muted'>{html.escape(o.note or '')}</td>"
        "<td>"
        f"<form class='inline' method='post' action='/taxonomy/delete/{o.id}'>"
        "<button class='danger' type='submit'>Delete</button></form>"
        "</td></tr>"
    )


def _sample_row(m: Market) -> str:
    quick_form = (
        "<form class='inline' method='post' action='/taxonomy/add'>"
        "<input type='hidden' name='kind' value='condition_id'/>"
        f"<input type='hidden' name='key' value='{html.escape(m.condition_id)}'/>"
        "<input name='domain' placeholder='domain' size='10' required/>"
        "<button type='submit'>Override</button></form>"
    )
    return (
        "<tr>"
        f"<td><span class='tag'>{html.escape(m.domain)}</span></td>"
        f"<td><code>{html.escape(m.slug[:30])}</code></td>"
        f"<td>{html.escape(m.question[:80])}</td>"
        f"<td><code>{html.escape(m.condition_id[:14])}…</code></td>"
        f"<td>{quick_form}</td>"
        "</tr>"
    )


def _query_live_markets(
    session: Session,
    *,
    limit: int = 100,
    domain: str = "",
    q: str = "",
) -> list[dict[str, object]]:
    """Top N markets sorted by end_ts ASC — no hour-window restriction."""
    from sqlalchemy import text as sa_text

    from poly_bot.persistence import dialect as sql_dialect

    dialect = session.get_bind().dialect.name  # type: ignore[union-attr]
    active = sql_dialect.active_market_filter(dialect)
    if sql_dialect.is_sqlite(dialect):
        future = "datetime(COALESCE(m.end_ts, e.end_ts)) > datetime('now')"
        order = "datetime(COALESCE(m.end_ts, e.end_ts)) ASC"
    else:
        future = "COALESCE(m.end_ts, e.end_ts) > NOW()"
        order = "COALESCE(m.end_ts, e.end_ts) ASC"

    domain_filter = "AND m.domain = :domain" if domain else ""
    q_filter = "AND (LOWER(m.question) LIKE :q OR LOWER(m.slug) LIKE :q)" if q else ""

    rows = session.execute(
        sa_text(
            f"""
            SELECT
                m.condition_id, m.slug, m.question, m.domain,
                COALESCE(m.end_ts, e.end_ts) AS end_ts,
                m.game_start_ts,
                t0.token_id AS yes_token_id, t1.token_id AS no_token_id,
                q0.mid AS yes_mid, q1.mid AS no_mid,
                en.volume_24h, en.open_interest,
                m.fees_enabled, m.taker_base_fee,
                e.title AS event_title
            FROM markets m
            LEFT JOIN events e ON e.id = m.event_id
            LEFT JOIN tokens t0 ON t0.condition_id = m.condition_id AND t0.outcome_index = 0
            LEFT JOIN tokens t1 ON t1.condition_id = m.condition_id AND t1.outcome_index = 1
            LEFT JOIN token_quotes q0 ON q0.token_id = t0.token_id
            LEFT JOIN token_quotes q1 ON q1.token_id = t1.token_id
            LEFT JOIN market_enrichment en ON en.condition_id = m.condition_id
            WHERE {active}
              AND COALESCE(m.end_ts, e.end_ts) IS NOT NULL
              AND {future}
              {domain_filter}
              {q_filter}
            ORDER BY {order}
            LIMIT :limit
            """
        ),
        {
            "limit": limit,
            **({"domain": domain} if domain else {}),
            **({"q": f"%{q.lower()}%"} if q else {}),
        },
    ).fetchall()

    out: list[dict[str, object]] = []
    for r in rows:
        (
            cid, slug, question, dom, end_ts, game_start_ts,
            yes_tid, no_tid, yes_mid, no_mid,
            vol, oi, fees_enabled, taker_fee, event_title,
        ) = r
        end_dt = _coerce_dt(end_ts)
        out.append(
            {
                "condition_id": cid,
                "slug": slug or "",
                "question": question or "",
                "domain": dom or "unknown",
                "end_ts": end_dt.isoformat() if end_dt else None,
                "game_start_ts": _coerce_dt(game_start_ts).isoformat()
                if game_start_ts
                else None,
                "yes_token_id": yes_tid,
                "no_token_id": no_tid,
                "yes_mid": float(yes_mid) if yes_mid is not None else None,
                "no_mid": float(no_mid) if no_mid is not None else None,
                "volume_24h": float(vol) if vol is not None else None,
                "open_interest": float(oi) if oi is not None else None,
                "event_title": event_title,
            }
        )
    return out


_MAIN_PAGE_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>poly_bot · live markets</title>
<style>
:root {
  --bg: #080c10;
  --surface: #0f1620;
  --surface2: #141e2c;
  --border: #1a2535;
  --border2: #243044;
  --text: #dde6f0;
  --muted: #4d6380;
  --accent: #3b7ef8;
  --green: #22c55e;
  --amber: #f59e0b;
  --red: #ef4444;
  --purple: #a855f7;
  --teal: #14b8a6;
  --pink: #ec4899;
  --font: "SF Pro Text","Inter",system-ui,-apple-system,sans-serif;
  --font-mono: "SF Mono","Fira Code",ui-monospace,monospace;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: var(--font); font-size: 13px; line-height: 1.5; min-height: 100vh; }

/* ─── TOP BAR ─────────────────────────────────────────── */
.topbar {
  position: sticky; top: 0; z-index: 100;
  display: flex; align-items: center; gap: 1.5rem;
  padding: 0 1.25rem; height: 48px;
  background: rgba(8,12,16,.92); backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border);
}
.brand { font-weight: 700; font-size: 15px; letter-spacing: -0.3px; color: var(--text); }
.brand span { color: var(--accent); }
.live-indicator { display: flex; align-items: center; gap: 6px; }
.dot {
  width: 7px; height: 7px; border-radius: 50%;
  background: var(--muted); flex-shrink: 0;
  transition: background 0.3s;
}
.dot.ok { background: var(--green); animation: pulse-dot 2s ease-out infinite; }
.dot.warn { background: var(--amber); }
.dot.err { background: var(--red); }
@keyframes pulse-dot {
  0%,100% { box-shadow: 0 0 0 0 rgba(34,197,94,.5); }
  50% { box-shadow: 0 0 0 5px rgba(34,197,94,0); }
}
.status-text { color: var(--muted); font-size: 12px; }
.topbar-stats { display: flex; gap: 1.25rem; margin-left: auto; }
.stat-pill { font-size: 11.5px; color: var(--muted); }
.stat-pill strong { color: var(--text); font-weight: 600; }
.topbar-nav { display: flex; gap: 2px; }
.topbar-nav a {
  color: var(--muted); text-decoration: none; font-size: 12.5px;
  padding: 4px 10px; border-radius: 6px;
  transition: color .15s, background .15s;
}
.topbar-nav a:hover { color: var(--text); background: var(--surface2); }

/* ─── FILTER BAR ─────────────────────────────────────── */
.filter-bar {
  display: flex; align-items: center; gap: 8px;
  padding: 8px 1.25rem;
  border-bottom: 1px solid var(--border);
  background: var(--surface);
}
.chips { display: flex; gap: 4px; flex-wrap: wrap; }
.chip {
  padding: 3px 10px; border-radius: 20px; font-size: 11.5px; font-weight: 500;
  border: 1px solid var(--border2); background: transparent; color: var(--muted);
  cursor: pointer; transition: all .15s; letter-spacing: .3px;
}
.chip:hover { border-color: var(--accent); color: var(--accent); }
.chip.active { background: var(--accent); border-color: var(--accent); color: #fff; }
.chip.active.d-crypto { background: #1d4ed8; border-color: #1d4ed8; }
.chip.active.d-sports { background: #15803d; border-color: #15803d; }
.chip.active.d-politics { background: #b45309; border-color: #b45309; }
.chip.active.d-finance { background: #0f766e; border-color: #0f766e; }
.chip.active.d-science { background: #7e22ce; border-color: #7e22ce; }
.search-wrap { margin-left: auto; position: relative; }
.search-input {
  background: var(--surface2); border: 1px solid var(--border2); color: var(--text);
  border-radius: 8px; padding: 5px 10px 5px 30px; font-size: 12.5px; width: 220px;
  outline: none; transition: border-color .15s;
}
.search-input:focus { border-color: var(--accent); }
.search-icon { position: absolute; left: 9px; top: 50%; transform: translateY(-50%); color: var(--muted); font-size: 12px; pointer-events: none; }
.refresh-btn {
  background: var(--surface2); border: 1px solid var(--border2); color: var(--muted);
  border-radius: 8px; padding: 5px 10px; cursor: pointer; font-size: 13px;
  transition: all .15s;
}
.refresh-btn:hover { color: var(--text); border-color: var(--accent); }
.count-badge { font-size: 11px; color: var(--muted); white-space: nowrap; }

/* ─── TABLE ───────────────────────────────────────────── */
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; table-layout: fixed; }
col.col-left   { width: 70px; }
col.col-exp    { width: 96px; }
col.col-dom    { width: 88px; }
col.col-price  { width: 60px; }
col.col-juice  { width: 54px; }
col.col-vol    { width: 60px; }
col.col-q      { width: auto; }
thead th {
  position: sticky; top: 48px; z-index: 10;
  background: var(--surface); border-bottom: 1px solid var(--border2);
  padding: 7px 8px; font-size: 10.5px; font-weight: 600;
  letter-spacing: .6px; color: var(--muted); text-transform: uppercase;
  text-align: right; white-space: nowrap; user-select: none; cursor: pointer;
}
thead th:first-child, thead th.th-left { text-align: left; }
thead th.th-q { text-align: left; }
thead th:hover { color: var(--text); }
tbody tr { border-bottom: 1px solid var(--border); transition: background .1s; }
tbody tr:hover { background: rgba(59,126,248,.04); }
td {
  padding: 7px 8px; font-family: var(--font-mono); font-size: 12px;
  text-align: right; vertical-align: middle; white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis;
}
td.td-left { text-align: left; }
td.td-q { font-family: var(--font); font-size: 12px; text-align: left; color: var(--text); max-width: 0; overflow: hidden; text-overflow: ellipsis; }
.loading-row { text-align: center; color: var(--muted); padding: 3rem; font-family: var(--font); }
.empty-row { text-align: center; color: var(--muted); padding: 2rem; font-family: var(--font); }

/* ─── COUNTDOWN ───────────────────────────────────────── */
.cd {
  font-family: var(--font-mono); font-size: 12px; font-weight: 600;
  display: inline-block; min-width: 52px; text-align: right;
  color: var(--muted);
}
.cd.ok    { color: var(--text); }
.cd.warm  { color: #fcd34d; }
.cd.soon  { color: var(--amber); }
.cd.crit  { color: var(--red); }
.cd.urg   { color: var(--red); animation: blink .4s step-end infinite; }
.cd.exp   { color: var(--muted); font-style: italic; }
@keyframes blink { 50% { opacity: 0; } }

/* ─── DOMAIN BADGES ───────────────────────────────────── */
.badge {
  display: inline-block; padding: 2px 7px; border-radius: 4px;
  font-family: var(--font); font-size: 10px; font-weight: 600;
  letter-spacing: .4px; text-transform: uppercase;
}
.b-crypto   { background: rgba(59,130,246,.15); color: #60a5fa; }
.b-sports   { background: rgba(34,197,94,.15);  color: #4ade80; }
.b-politics { background: rgba(245,158,11,.15); color: #fbbf24; }
.b-finance  { background: rgba(20,184,166,.15); color: #2dd4bf; }
.b-science  { background: rgba(168,85,247,.15); color: #c084fc; }
.b-culture  { background: rgba(236,72,153,.15); color: #f472b6; }
.b-unknown  { background: rgba(100,116,139,.1); color: #94a3b8; }

/* ─── PRICE CELLS ─────────────────────────────────────── */
.price-cell { transition: color .2s; }
.price-cell.harvest { color: #22c55e; font-weight: 700; }
.flash-up   { animation: fup .7s ease-out; }
.flash-down { animation: fdn .7s ease-out; }
@keyframes fup { 0% { background: rgba(34,197,94,.35); } 100% { background: transparent; } }
@keyframes fdn { 0% { background: rgba(239,68,68,.35); } 100% { background: transparent; } }

/* ─── JUICE ───────────────────────────────────────────── */
.juice-pos { color: var(--muted); }
.juice-tight { color: var(--green); }
.juice-neg { color: var(--red); }

/* ─── SORT ARROWS ─────────────────────────────────────── */
th[data-sort="asc"]::after  { content: " ↑"; color: var(--accent); }
th[data-sort="desc"]::after { content: " ↓"; color: var(--accent); }
</style>
</head>
<body>

<header class="topbar">
  <span class="brand">poly<span>_bot</span></span>
  <div class="live-indicator">
    <span class="dot" id="dot"></span>
    <span class="status-text" id="status-text">connecting…</span>
  </div>
  <div class="topbar-stats">
    <span class="stat-pill">Active <strong id="s-active">—</strong></span>
    <span class="stat-pill">Quotes <strong id="s-quotes">—</strong></span>
    <span class="stat-pill">Discovery <strong id="s-disco">—</strong></span>
  </div>
  <nav class="topbar-nav">
    <a href="/harvest">Harvest</a>
    <a href="/events">Events</a>
    <a href="/weather">Weather</a>
    <a href="/taxonomy">Taxonomy</a>
  </nav>
</header>

<div class="filter-bar">
  <div class="chips" id="chips">
    <button class="chip active" data-domain="">ALL</button>
    <button class="chip d-crypto" data-domain="crypto">Crypto</button>
    <button class="chip d-sports" data-domain="sports">Sports</button>
    <button class="chip d-politics" data-domain="politics">Politics</button>
    <button class="chip d-finance" data-domain="finance">Finance</button>
    <button class="chip d-science" data-domain="science">Science</button>
  </div>
  <span class="count-badge" id="count-badge"></span>
  <div class="search-wrap">
    <span class="search-icon">🔍</span>
    <input class="search-input" id="search" placeholder="Search markets…" autocomplete="off"/>
  </div>
  <button class="refresh-btn" id="refresh-btn" title="Reload markets">↻</button>
</div>

<div class="table-wrap">
  <table id="markets-table">
    <colgroup>
      <col class="col-left"/><col class="col-exp"/><col class="col-dom"/>
      <col class="col-price"/><col class="col-price"/>
      <col class="col-juice"/><col class="col-vol"/><col class="col-q"/>
    </colgroup>
    <thead>
      <tr>
        <th class="th-left" data-col="left">LEFT</th>
        <th data-col="end_ts">EXPIRES (UTC)</th>
        <th class="th-left" data-col="domain">DOMAIN</th>
        <th data-col="yes_mid">YES</th>
        <th data-col="no_mid">NO</th>
        <th data-col="juice">JUICE</th>
        <th data-col="volume_24h">VOL 24h</th>
        <th class="th-q" data-col="question">QUESTION</th>
      </tr>
    </thead>
    <tbody id="tbody"><tr><td colspan="8" class="loading-row">Loading markets…</td></tr></tbody>
  </table>
</div>

<script>
'use strict';
// ── State ─────────────────────────────────────────────────
let ALL_MARKETS = [];        // raw API response
let PRICES = {};             // token_id → mid (live from CLOB)
let PREV_PRICES = {};        // for flash direction
let ACTIVE_DOMAIN = '';
let SEARCH_Q = '';
let SORT_COL = 'left';
let SORT_ASC = true;
let PRICE_ES = null;         // EventSource for prices

const MIN_HARVEST = 0.983, MAX_HARVEST = 0.995;

// ── Boot ──────────────────────────────────────────────────
async function boot() {
  bindFilters();
  bindHealthStream();
  await loadMarkets();
  setInterval(tickCountdowns, 1000);
}

// ── Data load ─────────────────────────────────────────────
async function loadMarkets() {
  const btn = document.getElementById('refresh-btn');
  btn.textContent = '…';
  try {
    const qs = new URLSearchParams({ limit: 100 });
    if (ACTIVE_DOMAIN) qs.set('domain', ACTIVE_DOMAIN);
    if (SEARCH_Q) qs.set('q', SEARCH_Q);
    const resp = await fetch('/api/markets?' + qs);
    ALL_MARKETS = await resp.json();
    // seed PRICES from DB mids (overwritten by SSE later)
    for (const m of ALL_MARKETS) {
      if (m.yes_token_id && m.yes_mid !== null) PRICES[m.yes_token_id] = m.yes_mid;
      if (m.no_token_id  && m.no_mid  !== null) PRICES[m.no_token_id]  = m.no_mid;
    }
    renderTable();
    reconnectPriceStream();
  } catch(e) {
    document.getElementById('tbody').innerHTML =
      '<tr><td colspan="8" class="empty-row">Failed to load — ' + e.message + '</td></tr>';
  } finally {
    btn.textContent = '↻';
  }
}

// ── Render ────────────────────────────────────────────────
function renderTable() {
  const filtered = applySort(filterMarkets(ALL_MARKETS));
  const badge = document.getElementById('count-badge');
  badge.textContent = filtered.length + ' markets';
  if (!filtered.length) {
    document.getElementById('tbody').innerHTML =
      '<tr><td colspan="8" class="empty-row">No markets match.</td></tr>';
    return;
  }
  const frag = document.createDocumentFragment();
  for (const m of filtered) frag.appendChild(makeRow(m));
  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '';
  tbody.appendChild(frag);
}

function filterMarkets(list) {
  return list.filter(m => {
    if (ACTIVE_DOMAIN && m.domain !== ACTIVE_DOMAIN) return false;
    if (SEARCH_Q) {
      const ql = SEARCH_Q.toLowerCase();
      if (!m.question.toLowerCase().includes(ql) &&
          !m.slug.toLowerCase().includes(ql) &&
          !(m.event_title||'').toLowerCase().includes(ql)) return false;
    }
    return true;
  });
}

function applySort(list) {
  return [...list].sort((a, b) => {
    let av, bv;
    switch(SORT_COL) {
      case 'left':    av = secondsLeft(a.end_ts); bv = secondsLeft(b.end_ts); break;
      case 'end_ts':  av = a.end_ts || ''; bv = b.end_ts || ''; break;
      case 'domain':  av = a.domain; bv = b.domain; break;
      case 'yes_mid': av = livePrice(a.yes_token_id, a.yes_mid) ?? -1;
                      bv = livePrice(b.yes_token_id, b.yes_mid) ?? -1; break;
      case 'no_mid':  av = livePrice(a.no_token_id, a.no_mid) ?? -1;
                      bv = livePrice(b.no_token_id, b.no_mid) ?? -1; break;
      case 'juice':   av = juice(a); bv = juice(b); break;
      case 'volume_24h': av = a.volume_24h ?? -1; bv = b.volume_24h ?? -1; break;
      default:        av = a.end_ts || ''; bv = b.end_ts || '';
    }
    const cmp = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv));
    return SORT_ASC ? cmp : -cmp;
  });
}

function makeRow(m) {
  const tr = document.createElement('tr');
  tr.dataset.cid = m.condition_id;
  tr.dataset.endTs = m.end_ts || '';
  tr.dataset.yTid = m.yes_token_id || '';
  tr.dataset.nTid = m.no_token_id || '';
  tr.dataset.domain = m.domain;

  const endFmt = fmtExpires(m.end_ts);
  const domBadge = `<span class="badge b-${m.domain}">${m.domain}</span>`;
  const yMid = livePrice(m.yes_token_id, m.yes_mid);
  const nMid = livePrice(m.no_token_id, m.no_mid);
  const j = juice(m);

  const yHarvest = yMid !== null && yMid >= MIN_HARVEST && yMid <= MAX_HARVEST;
  const nHarvest = nMid !== null && nMid >= MIN_HARVEST && nMid <= MAX_HARVEST;

  const link = `https://polymarket.com/market/${m.slug}`;

  tr.innerHTML = `
    <td class="td-left"><span class="cd" id="cd-${m.condition_id}">—</span></td>
    <td>${endFmt}</td>
    <td class="td-left">${domBadge}</td>
    <td class="price-cell${yHarvest?' harvest':''}" id="y-${m.yes_token_id||m.condition_id}">${fmtMid(yMid)}</td>
    <td class="price-cell${nHarvest?' harvest':''}" id="n-${m.no_token_id||m.condition_id}">${fmtMid(nMid)}</td>
    <td class="${juiceCls(j)}">${fmtJuice(j)}</td>
    <td>${fmtVol(m.volume_24h)}</td>
    <td class="td-q" title="${esc(m.question)}"><a href="${link}" target="_blank" rel="noopener" style="color:inherit;text-decoration:none">${esc(m.question)}</a></td>
  `;
  return tr;
}

// ── Price stream ──────────────────────────────────────────
function reconnectPriceStream() {
  if (PRICE_ES) { PRICE_ES.close(); PRICE_ES = null; }
  const top20 = ALL_MARKETS.slice(0, 20);
  const tids = top20.flatMap(m => [m.yes_token_id, m.no_token_id]).filter(Boolean);
  if (!tids.length) return;
  PRICE_ES = new EventSource('/api/stream/prices?token_ids=' + tids.join(','));
  PRICE_ES.onmessage = (e) => {
    try { applyPrices(JSON.parse(e.data).prices || {}); } catch(_) {}
  };
}

function applyPrices(newPrices) {
  for (const [tid, mid] of Object.entries(newPrices)) {
    PREV_PRICES[tid] = PRICES[tid];
    PRICES[tid] = mid;
    updatePriceCell(tid);
  }
  // refresh juice and harvest classes for affected rows
  const tids = new Set(Object.keys(newPrices));
  document.querySelectorAll('tr[data-y-tid]').forEach(tr => {
    if (tids.has(tr.dataset.yTid) || tids.has(tr.dataset.nTid)) {
      updateRowDerived(tr);
    }
  });
}

function updatePriceCell(tid) {
  // find cell (could be yes or no)
  const yCell = document.getElementById('y-' + tid);
  const nCell = document.getElementById('n-' + tid);
  const cell = yCell || nCell;
  if (!cell) return;

  const mid = PRICES[tid];
  const prev = PREV_PRICES[tid];
  const text = fmtMid(mid);
  if (cell.textContent === text) return;
  cell.textContent = text;

  const harvest = mid !== null && mid >= MIN_HARVEST && mid <= MAX_HARVEST;
  cell.classList.toggle('harvest', harvest);

  cell.classList.remove('flash-up', 'flash-down');
  void cell.offsetWidth;
  if (prev !== undefined && prev !== null) {
    cell.classList.add(mid > prev ? 'flash-up' : 'flash-down');
  }
}

function updateRowDerived(tr) {
  const yTid = tr.dataset.yTid;
  const nTid = tr.dataset.nTid;
  const cid  = tr.dataset.cid;
  const yMid = PRICES[yTid] ?? null;
  const nMid = PRICES[nTid] ?? null;
  const j = (yMid !== null && nMid !== null) ? (1 - yMid - nMid) : null;
  const jCell = tr.querySelector('.juice-pos, .juice-tight, .juice-neg');
  if (jCell) {
    jCell.textContent = fmtJuice(j);
    jCell.className = juiceCls(j);
  }
}

// ── Countdown ─────────────────────────────────────────────
function tickCountdowns() {
  const now = Date.now();
  document.querySelectorAll('tr[data-end-ts]').forEach(tr => {
    const endTs = tr.dataset.endTs;
    if (!endTs) return;
    const diff = new Date(endTs).getTime() - now;
    const el = document.getElementById('cd-' + tr.dataset.cid);
    if (!el) return;
    if (diff <= 0) { el.textContent = 'expired'; el.className = 'cd exp'; return; }
    const s = Math.floor(diff / 1000);
    let txt, cls;
    if (s < 60)    { txt = '0:' + pad(s);                            cls = 'cd urg'; }
    else if (s < 300)  { txt = Math.floor(s/60) + ':' + pad(s%60);  cls = 'cd crit'; }
    else if (s < 3600) { txt = Math.floor(s/60) + ':' + pad(s%60);  cls = 'cd soon'; }
    else if (s < 7200) { txt = Math.floor(s/3600) + 'h ' + pad(Math.floor((s%3600)/60)) + 'm'; cls = 'cd warm'; }
    else               { txt = Math.floor(s/3600) + 'h ' + pad(Math.floor((s%3600)/60)) + 'm'; cls = 'cd ok'; }
    el.textContent = txt; el.className = cls;
  });
}

// ── Health SSE ────────────────────────────────────────────
function bindHealthStream() {
  const dot  = document.getElementById('dot');
  const stEl = document.getElementById('status-text');
  const sAc  = document.getElementById('s-active');
  const sQu  = document.getElementById('s-quotes');
  const sDi  = document.getElementById('s-disco');

  function connect() {
    const es = new EventSource('/api/stream?hours=168');
    es.onmessage = (e) => {
      try {
        const d = JSON.parse(e.data);
        sAc.textContent = (d.counts?.markets_active || 0).toLocaleString();
        const qa = d.quote_age_s;
        sQu.textContent = qa == null ? '—' : fmtAge(qa);
        const da = d.discovery_activity_age_s ?? d.full_age_s;
        sDi.textContent = da == null ? '—' : fmtAge(da);
        dot.className = 'dot ' + (qa == null ? 'warn' : qa < 60 ? 'ok' : qa < 600 ? 'warn' : 'err');
        stEl.textContent = qa == null ? 'no quotes' : qa < 60 ? 'live' : 'stale ' + fmtAge(qa);
      } catch(_) {}
    };
    es.onerror = () => {
      dot.className = 'dot err'; stEl.textContent = 'offline';
      es.close(); setTimeout(connect, 4000);
    };
  }
  connect();
}

// ── Filters ───────────────────────────────────────────────
function bindFilters() {
  document.getElementById('chips').addEventListener('click', e => {
    const chip = e.target.closest('.chip');
    if (!chip) return;
    document.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
    chip.classList.add('active');
    ACTIVE_DOMAIN = chip.dataset.domain || '';
    renderTable();
  });

  let searchTimer;
  document.getElementById('search').addEventListener('input', e => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      SEARCH_Q = e.target.value.trim();
      renderTable();
    }, 200);
  });

  document.getElementById('refresh-btn').addEventListener('click', loadMarkets);

  document.querySelectorAll('thead th[data-col]').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (SORT_COL === col) { SORT_ASC = !SORT_ASC; }
      else { SORT_COL = col; SORT_ASC = col === 'left' || col === 'end_ts'; }
      document.querySelectorAll('thead th').forEach(h => delete h.dataset.sort);
      th.dataset.sort = SORT_ASC ? 'asc' : 'desc';
      renderTable();
    });
  });
}

// ── Helpers ───────────────────────────────────────────────
function livePrice(tid, fallback) {
  if (tid && PRICES[tid] !== undefined) return PRICES[tid];
  return fallback ?? null;
}

function juice(m) {
  const y = livePrice(m.yes_token_id, m.yes_mid);
  const n = livePrice(m.no_token_id, m.no_mid);
  if (y === null || n === null) return null;
  return 1 - y - n;
}

function juiceCls(j) {
  if (j === null) return 'juice-pos';
  if (j < 0.005) return 'juice-tight';
  if (j < 0) return 'juice-neg';
  return 'juice-pos';
}

function secondsLeft(isoStr) {
  if (!isoStr) return Infinity;
  return Math.max(0, (new Date(isoStr).getTime() - Date.now()) / 1000);
}

function fmtMid(v) {
  if (v === null || v === undefined) return '—';
  return v.toFixed(3);
}

function fmtJuice(j) {
  if (j === null) return '—';
  return (j >= 0 ? '+' : '') + (j * 100).toFixed(1) + '¢';
}

function fmtExpires(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  const now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  const hhmm = d.toISOString().slice(11, 16);
  if (sameDay) return hhmm;
  const mdd = d.toISOString().slice(5, 10).replace('-', '/');
  return mdd + ' ' + hhmm;
}

function fmtVol(v) {
  if (v == null) return '—';
  if (v >= 1e6) return (v/1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v/1e3).toFixed(0) + 'k';
  return v.toFixed(0);
}

function fmtAge(s) {
  if (s === null || s === undefined) return '—';
  if (s < 60) return s + 's';
  if (s < 3600) return Math.round(s/60) + 'm';
  return (s/3600).toFixed(1) + 'h';
}

function pad(n) { return String(n).padStart(2,'0'); }

function esc(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── Go ────────────────────────────────────────────────────
boot();
</script>
</body>
</html>
"""


def _render_main_page(settings: Settings) -> str:
    return _MAIN_PAGE_HTML


def run_dashboard(settings: Settings) -> None:
    try:
        import uvicorn
    except ImportError as exc:
        raise SystemExit("Install dashboard extras: pip install -e '.[dashboard]'") from exc

    app = create_app(settings, background=True)
    uvicorn.run(
        app,
        host=settings.catalog_dashboard_host,
        port=settings.catalog_dashboard_port,
        log_level="warning",
    )
