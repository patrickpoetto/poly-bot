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


def _run_discovery_in_thread(settings: Settings) -> None:
    """Run discovery scheduler in its own thread with a private event loop.

    Keeps all sync SQLAlchemy calls off the uvicorn event loop so HTTP stays
    responsive even when SQLite is under write-lock contention.
    """
    import asyncio as _aio

    from poly_bot.discovery.scheduler import DiscoveryScheduler

    async def _inner() -> None:
        sched = DiscoveryScheduler(settings)
        sched.init_database()
        await sched.run_forever()

    _aio.run(_inner())


def _run_enrich_cycle_in_thread(settings: Settings) -> None:
    """Run one enrich cycle in a thread with a private event loop."""
    import asyncio as _aio

    from poly_bot.catalog.enricher import CatalogEnricher

    async def _inner() -> None:
        with Session(get_engine(settings)) as session:
            enricher = CatalogEnricher(settings, session)
            stats = await enricher.enrich_expiring_window(hours=48, force=False)
            logger.debug("dashboard_enrich_done", **stats)

    _aio.run(_inner())


def _run_reconcile_in_thread(settings: Settings) -> None:
    """Run reconcile loop in a thread with a private event loop."""
    import asyncio as _aio

    from poly_bot.ops.reconcile_scheduler import run_reconcile_loop

    _aio.run(run_reconcile_loop(settings, interval_seconds=3600))


async def _discovery_background(settings: Settings) -> None:
    """Offload discovery to a thread so sync SQLAlchemy never blocks uvicorn."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _run_discovery_in_thread, settings)


async def _enrich_background(settings: Settings) -> None:
    """Enrich quotes on a fixed interval, each cycle in its own thread."""
    await asyncio.sleep(10)  # let discovery write its first heartbeat before competing for the lock
    interval = settings.catalog_enrich_interval_seconds
    loop = asyncio.get_running_loop()
    while True:
        try:
            await loop.run_in_executor(None, _run_enrich_cycle_in_thread, settings)
        except Exception:
            logger.exception("dashboard_enrich_failed")
        await asyncio.sleep(interval)


async def _reconcile_background(settings: Settings) -> None:
    """Offload reconcile to a thread so sync SQLAlchemy never blocks uvicorn."""
    await asyncio.sleep(30)  # don't compete with startup writes
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _run_reconcile_in_thread, settings)


# NearCT engine singleton — shared between the background task and the API routes
_nearct_engine: object = None


async def _nearct_background(settings: Settings) -> None:
    """NearCT sequential compounding engine."""
    global _nearct_engine
    from poly_bot.nearct.engine import NearctEngine

    await asyncio.sleep(5)  # let DB init settle
    engine = NearctEngine(settings)
    _nearct_engine = engine
    while True:
        try:
            await engine.run()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("nearct_engine_crashed — restarting in 60s")
            await asyncio.sleep(60)


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI, settings: Settings):  # type: ignore[misc]
    tasks = [
        asyncio.create_task(_discovery_background(settings), name="discovery"),
        asyncio.create_task(_enrich_background(settings), name="enrich"),
        asyncio.create_task(_reconcile_background(settings), name="reconcile"),
        asyncio.create_task(_nearct_background(settings), name="nearct"),
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
    lifespan = (lambda app: _lifespan(app, settings)) if background else None
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
        tokens = [t.strip() for t in token_ids.split(",") if t.strip()][:40]

        async def generator() -> object:
            import httpx as _httpx
            # dedicated short-timeout client — skip tenacity retries for live stream
            async with _httpx.AsyncClient(
                base_url="https://clob.polymarket.com",
                timeout=_httpx.Timeout(connect=2.0, read=4.0, write=2.0, pool=2.0),
            ) as client:
                last_prices: dict[str, float] = {}
                while True:
                    # yield first so the connection stays alive even when CLOB is slow
                    yield f"data: {json.dumps({'prices': last_prices, 'ts': datetime.now(UTC).isoformat()})}\n\n"
                    if tokens:
                        try:
                            body = [{"token_id": t} for t in tokens]
                            r = await client.post("/midpoints", json=body)
                            r.raise_for_status()
                            data = r.json()
                            if isinstance(data, list):
                                for item in data:
                                    if item.get("mid") is not None:
                                        last_prices[item["token_id"]] = float(item["mid"])
                            elif isinstance(data, dict):
                                last_prices.update({k: float(v) for k, v in data.items() if v is not None})
                        except Exception:
                            pass  # keep stale prices
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

    @app.get("/api/orders")
    def api_orders(
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
        paper: bool | None = Query(default=None),
        status: str = Query(default=""),
        domain: str = Query(default=""),
    ) -> list[dict[str, object]]:
        from sqlalchemy import text as _text2

        from poly_bot.persistence.schema import HarvestOrder

        with Session(get_engine(settings)) as session:
            stmt = select(HarvestOrder).where(HarvestOrder.session_id.is_(None)).order_by(HarvestOrder.created_at.desc()).offset(offset).limit(limit)  # type: ignore[union-attr,arg-type]
            if paper is not None:
                stmt = stmt.where(HarvestOrder.paper == paper)
            if status:
                stmt = stmt.where(HarvestOrder.status == status)
            if domain:
                stmt = stmt.where(HarvestOrder.domain == domain)
            rows = session.exec(stmt).all()
            # fetch end_ts for each order's market in one query
            cids = list({r.condition_id for r in rows})
            end_ts_map: dict[str, str | None] = {}
            if cids:
                placeholders = ",".join(f":c{i}" for i in range(len(cids)))
                params = {f"c{i}": c for i, c in enumerate(cids)}
                res = session.execute(
                    _text2(f"SELECT condition_id, end_ts FROM markets WHERE condition_id IN ({placeholders})"),
                    params,
                ).fetchall()
                end_ts_map = dict(res)
        return [_order_json(r, end_ts=end_ts_map.get(r.condition_id)) for r in rows]

    @app.post("/api/orders/snapshot")
    def snapshot_session(label: str = Form(default="")) -> JSONResponse:
        """Move all unsnapshot paper orders into a named TradingSession.

        Also cancels pending chain tickets that belong to the archived orders,
        so the executor starts clean for the next session.
        """
        import uuid as _uuid

        from sqlalchemy import text as _text
        from sqlmodel import Session as _Session

        from poly_bot.persistence.schema import TradingSession

        engine = get_engine(settings)
        session_id = str(_uuid.uuid4())
        ts_label = label.strip() or datetime.now(UTC).strftime("Session %Y-%m-%d %H:%M")
        with _Session(engine) as session:
            # aggregate in one query, then bulk-update — avoids N row-level locks
            agg = session.execute(_text(
                "SELECT COUNT(*), COALESCE(SUM(pnl_usd), 0.0) "
                "FROM harvest_orders WHERE paper = 1 AND session_id IS NULL"
            )).one()
            count, pnl = int(agg[0]), round(float(agg[1]), 4)
            if count:
                session.execute(_text(
                    "UPDATE harvest_orders SET session_id = :sid "
                    "WHERE paper = 1 AND session_id IS NULL"
                ), {"sid": session_id})
                # Cancel pending chain tickets for archived chains so the executor
                # doesn't carry them over into the next session.
                session.execute(_text(
                    "UPDATE chain_tickets SET status = 'cancelled' "
                    "WHERE status = 'pending' AND chain_id IN ("
                    "  SELECT DISTINCT chain_id FROM harvest_orders"
                    "  WHERE session_id = :sid AND chain_id IS NOT NULL"
                    ")"
                ), {"sid": session_id})
            ts = TradingSession(
                id=session_id,
                label=ts_label,
                order_count=count,
                pnl_usd=pnl,
            )
            session.add(ts)
            session.commit()
        return JSONResponse({"session_id": session_id, "label": ts_label, "orders": count, "pnl_usd": pnl})

    @app.get("/api/sessions")
    def list_sessions() -> JSONResponse:
        """List all trading sessions ordered newest first."""
        from sqlmodel import Session as _Session
        from sqlmodel import select as _select

        from poly_bot.persistence.schema import TradingSession

        engine = get_engine(settings)
        with _Session(engine) as session:
            rows = session.exec(
                _select(TradingSession).order_by(TradingSession.created_at.desc())  # type: ignore[attr-defined]
            ).all()
        return JSONResponse([
            {
                "id": r.id,
                "label": r.label,
                "created_at": r.created_at.isoformat(),
                "order_count": r.order_count,
                "pnl_usd": r.pnl_usd,
            }
            for r in rows
        ])

    @app.get("/orders", response_class=HTMLResponse)
    def orders_page() -> str:
        return _ORDERS_PAGE_HTML

    @app.get("/chains", response_class=HTMLResponse)
    def chains_page() -> str:
        return _CHAINS_PAGE_HTML

    @app.get("/api/chains")
    def api_chains() -> JSONResponse:
        try:
            return JSONResponse(_build_chains_payload(settings))
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.get("/api/stream/chains")
    async def api_stream_chains() -> StreamingResponse:
        """SSE — full chain grid every 5 s."""

        async def generator() -> object:
            while True:
                try:
                    payload = _build_chains_payload(settings)
                    yield f"data: {json.dumps(payload)}\n\n"
                except Exception:
                    yield f"data: {json.dumps({'error': True})}\n\n"
                await asyncio.sleep(5)

        return StreamingResponse(
            generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/api/chains/{chain_id}/close")
    def close_chain(chain_id: str) -> JSONResponse:
        """Mark a chain as closing: cancel pending ticket + create sentinel."""
        import uuid as _uuid2

        from sqlmodel import Session as _Sess
        from sqlmodel import select as _sel

        from poly_bot.persistence.schema import ChainTicket, HarvestOrder

        with _Sess(get_engine(settings)) as session:
            # cancel any pending tickets for this chain
            pending_tickets = session.exec(
                _sel(ChainTicket).where(
                    ChainTicket.chain_id == chain_id,  # type: ignore[arg-type]
                    ChainTicket.status == "pending",  # type: ignore[arg-type]
                )
            ).all()
            for t in pending_tickets:
                t.status = "cancelled"
                session.add(t)

            # if there's an active order, create a closing sentinel so reconciler
            # won't open a new ticket after this order resolves
            active_order = session.exec(
                _sel(HarvestOrder).where(
                    HarvestOrder.chain_id == chain_id,  # type: ignore[arg-type]
                    HarvestOrder.status.in_(["filled", "pending"]),  # type: ignore[arg-type]
                )
            ).first()

            if active_order:
                already = session.exec(
                    _sel(ChainTicket).where(
                        ChainTicket.chain_id == chain_id,  # type: ignore[arg-type]
                        ChainTicket.status == "cancelled",  # type: ignore[arg-type]
                    )
                ).first()
                if not already:
                    sentinel = ChainTicket(
                        id=str(_uuid2.uuid4()),
                        chain_id=chain_id,
                        chain_level=0,
                        size_usd=0.0,
                        parent_order_id=active_order.id,
                        status="cancelled",
                    )
                    session.add(sentinel)

            session.commit()

        return JSONResponse({"ok": True, "chain_id": chain_id})

    @app.get("/api/stream/engine")
    async def api_stream_engine() -> StreamingResponse:
        """SSE — engine stats from DB every 5s."""

        async def generator() -> object:
            from sqlalchemy import text as _text

            from poly_bot.persistence.schema import HarvestOrder

            while True:
                try:
                    now = datetime.now(UTC)
                    eng = get_engine(settings)
                    with eng.connect() as conn:
                        total = int(conn.execute(_text("SELECT COUNT(*) FROM harvest_orders WHERE session_id IS NULL")).scalar() or 0)
                        paper_count = int(conn.execute(_text("SELECT COUNT(*) FROM harvest_orders WHERE paper = 1 AND session_id IS NULL")).scalar() or 0)
                        today_date = now.strftime("%Y-%m-%d")
                        today_count = int(conn.execute(_text(
                            "SELECT COUNT(*) FROM harvest_orders WHERE DATE(created_at) = :d AND session_id IS NULL"
                        ), {"d": today_date}).scalar() or 0)
                        open_count = int(conn.execute(_text(
                            "SELECT COUNT(*) FROM harvest_orders WHERE status = 'filled' AND resolved_at IS NULL AND session_id IS NULL"
                        )).scalar() or 0)
                        pnl_row = conn.execute(_text(
                            "SELECT COALESCE(SUM(pnl_usd), 0) FROM harvest_orders"
                            " WHERE paper = 1 AND status IN ('won','lost') AND session_id IS NULL"
                        )).scalar()
                    with Session(eng) as session:
                        recent_raw = session.exec(
                            select(HarvestOrder)
                            .where(HarvestOrder.session_id.is_(None))  # type: ignore[union-attr]
                            .order_by(HarvestOrder.created_at.desc())  # type: ignore[arg-type]
                            .limit(10)
                        ).all()
                    recent = [_order_json(r) for r in recent_raw]
                    last_at = recent_raw[0].created_at.isoformat() if recent_raw else None
                    payload = {
                        "ts": now.isoformat(),
                        "paper_mode": settings.paper_trading,
                        "orders_total": total,
                        "orders_paper": paper_count,
                        "orders_today": today_count,
                        "orders_open": open_count,
                        "pnl_paper_usd": float(pnl_row or 0),
                        "last_order_at": last_at,
                        "recent_orders": recent,
                    }
                except Exception:
                    payload = {"ts": datetime.now(UTC).isoformat(), "error": True}
                yield f"data: {json.dumps(payload)}\n\n"
                await asyncio.sleep(5)

        return StreamingResponse(
            generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/nearct", response_class=HTMLResponse)
    def nearct_page() -> str:
        return _NEARCT_PAGE_HTML

    @app.get("/api/nearct/status")
    def api_nearct_status() -> JSONResponse:
        from poly_bot.nearct.engine import NearctEngine, NearctStatus
        engine: NearctEngine | None = _nearct_engine  # type: ignore[assignment]
        if engine is None:
            return JSONResponse({"state": "starting", "capital": settings.nearct_start_capital})
        s: NearctStatus = engine.status
        return JSONResponse({
            "state": s.state,
            "capital": round(s.capital, 4),
            "start_capital": s.start_capital,
            "open_order": s.open_order,
            "open_quote": s.open_quote,
            "total_trades": s.total_trades,
            "wins": s.wins,
            "losses": s.losses,
            "stuck": s.stuck,
            "trades_today": s.trades_today,
            "win_rate_pct": round(100.0 * s.wins / max(1, s.wins + s.losses), 2),
            "total_pnl": round(s.total_pnl, 4),
            "last_updated": s.last_updated.isoformat(),
        })

    @app.post("/api/nearct/reset")
    async def api_nearct_reset() -> JSONResponse:
        from poly_bot.nearct.engine import NearctEngine
        engine: NearctEngine | None = _nearct_engine  # type: ignore[assignment]
        if engine is None:
            return JSONResponse({"ok": False, "error": "engine not started"}, status_code=503)
        await asyncio.get_running_loop().run_in_executor(None, engine.reset)
        return JSONResponse({"ok": True})

    @app.get("/api/nearct/scans")
    def api_nearct_scans() -> JSONResponse:
        from poly_bot.nearct.engine import NearctEngine
        engine: NearctEngine | None = _nearct_engine  # type: ignore[assignment]
        if engine is None:
            return JSONResponse([])
        return JSONResponse(engine.scan_log)

    @app.get("/api/nearct/orders")
    def api_nearct_orders(limit: int = Query(default=200, ge=1, le=1000)) -> JSONResponse:
        from sqlmodel import Session, select

        from poly_bot.nearct.schema import NearctOrder, get_nearct_engine

        with Session(get_nearct_engine(settings)) as session:
            orders = session.exec(
                select(NearctOrder)
                .order_by(NearctOrder.created_at.desc())  # type: ignore[attr-defined]
                .limit(limit)
            ).all()
        return JSONResponse([{
            "id": o.id,
            "condition_id": o.condition_id,
            "question": o.question,
            "domain": o.domain,
            "ask": o.ask,
            "net_yield_pct": o.net_yield_pct,
            "size_usd": o.size_usd,
            "status": o.status,
            "pnl_usd": o.pnl_usd,
            "capital_before": o.capital_before,
            "capital_after": o.capital_after,
            "end_ts": o.end_ts.isoformat() if o.end_ts else None,
            "created_at": o.created_at.isoformat() if o.created_at else None,
            "resolved_at": o.resolved_at.isoformat() if o.resolved_at else None,
        } for o in orders])

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


def _build_chains_payload(settings: Settings) -> list[dict[str, object]]:
    """Single-pass chain aggregation — two SQL queries, zero N+1."""
    from collections import defaultdict

    from sqlalchemy import text as _text

    eng = get_engine(settings)
    with eng.connect() as conn:
        order_rows = conn.execute(_text("""
            SELECT chain_id, chain_level, status, size_usd, pnl_usd,
                   question, price_target, created_at, domain, net_yield_pct
            FROM harvest_orders
            WHERE chain_id IS NOT NULL AND session_id IS NULL
            ORDER BY chain_id, chain_level ASC
        """)).fetchall()

        ticket_rows = conn.execute(_text("""
            SELECT ct.chain_id, ct.chain_level, ct.size_usd, ct.status
            FROM chain_tickets ct
            WHERE ct.chain_id IN (
                SELECT DISTINCT chain_id FROM harvest_orders
                WHERE chain_id IS NOT NULL AND session_id IS NULL
            )
        """)).fetchall()

    orders_by_chain: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in order_rows:
        cid, level, status, size, pnl, q, price, created, domain, yield_ = row
        orders_by_chain[cid].append({
            "level": level,
            "status": status,
            "size_usd": size,
            "pnl_usd": pnl,
            "question": q or "",
            "price_target": price,
            "created_at": str(created) if created else None,
            "domain": domain or "",
            "net_yield_pct": yield_,
        })

    pending_by_chain: dict[str, dict[str, object]] = {}
    closing_chains: set[str] = set()
    for row in ticket_rows:
        cid, level, size, status = row
        if status == "cancelled":
            closing_chains.add(cid)
        elif status == "pending" and cid not in pending_by_chain:
            pending_by_chain[cid] = {"level": level, "size_usd": size}

    result: list[dict[str, object]] = []
    for cid, levels in orders_by_chain.items():
        has_active = any(lv["status"] in ("filled", "pending") for lv in levels)
        is_closing = cid in closing_chains
        pending = pending_by_chain.get(cid)

        resolved = [lv for lv in levels if lv["status"] in ("won", "lost", "stopped_out")]
        last_resolved = resolved[-1] if resolved else None

        if has_active and is_closing:
            chain_status: str = "closing"
        elif has_active:
            chain_status = "active"
        elif last_resolved and last_resolved["status"] in ("lost", "stopped_out"):
            chain_status = "dead"  # last outcome was a loss — dead regardless of stale tickets
        elif pending:
            chain_status = "pending"
        elif is_closing:
            chain_status = "closed"
        else:
            chain_status = "idle"

        active_order = next(
            (lv for lv in levels if lv["status"] in ("filled", "pending")), None
        )
        total_pnl = sum(
            float(lv["pnl_usd"] or 0) for lv in levels
            if lv["status"] in ("won", "lost", "stopped_out")
        )

        result.append({
            "chain_id": cid,
            "short_id": cid[:8],
            "status": chain_status,
            "levels": levels,
            "pending_ticket": pending,
            "total_pnl_usd": round(total_pnl, 4),
            "active_order": active_order,
            "max_level": max(int(lv["level"]) for lv in levels),
        })

    _STATUS_PRIORITY = {"active": 0, "closing": 1, "pending": 2, "idle": 3, "dead": 4, "closed": 5}
    result.sort(key=lambda c: (_STATUS_PRIORITY.get(str(c["status"]), 9), str(c["chain_id"])))
    return result


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
  overflow-x: auto;
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
    <a href="/orders">Orders</a>
    <a href="/chains">Chains</a>
    <a href="/nearct">NearCT</a>
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
  <a href="/orders">Orders</a>
  <a href="/chains">Chains</a>
    <a href="/nearct">NearCT</a>
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
    # Use m.end_ts directly (no datetime() wrapper, no COALESCE) so the composite
    # index (active, closed, end_ts) can satisfy both the filter and the ORDER BY
    # without a full table sort. Markets with NULL end_ts are excluded.
    if sql_dialect.is_sqlite(dialect):
        future = "m.end_ts > datetime('now')"
    else:
        future = "m.end_ts > NOW()"

    domain_filter = "AND m.domain = :domain" if domain else ""
    q_filter = "AND (LOWER(m.question) LIKE :q OR LOWER(m.slug) LIKE :q)" if q else ""

    rows = session.execute(
        sa_text(
            f"""
            SELECT
                m.condition_id, m.slug, m.question, m.domain,
                m.end_ts,
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
              AND m.end_ts IS NOT NULL
              AND {future}
              {domain_filter}
              {q_filter}
            ORDER BY m.end_ts ASC
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


def _utc_iso(dt: object) -> str | None:
    """Return ISO string with Z suffix, treating naive datetimes as UTC."""
    from datetime import datetime as _dt
    if not isinstance(dt, _dt):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.isoformat()


def _order_json(r: object, *, end_ts: str | None = None) -> dict[str, object]:
    from poly_bot.persistence.schema import HarvestOrder
    assert isinstance(r, HarvestOrder)
    return {
        "id": r.id,
        "condition_id": r.condition_id,
        "token_id": r.token_id,
        "side": r.side,
        "domain": r.domain,
        "question": r.question,
        "underlying": r.underlying,
        "duration_s": r.duration_s,
        "price_target": r.price_target,
        "size_usd": r.size_usd,
        "size_shares": r.size_shares,
        "paper": r.paper,
        "status": r.status,
        "net_yield_pct": round(r.net_yield_pct * 100, 3) if r.net_yield_pct else None,
        "pnl_usd": r.pnl_usd,
        "chain_id": r.chain_id,
        "chain_level": r.chain_level,
        "minutes_left_at_entry": r.minutes_left_at_entry,
        "midpoint_at_entry": r.midpoint_at_entry,
        "created_at": _utc_iso(r.created_at),
        "filled_at": _utc_iso(r.filled_at),
        "resolved_at": _utc_iso(r.resolved_at),
        "end_ts": end_ts,
    }


_ORDERS_PAGE_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>poly_bot · orders</title>
<style>
:root {
  --bg: #080c10; --surface: #0f1620; --surface2: #141e2c;
  --border: #1a2535; --border2: #243044; --text: #dde6f0;
  --muted: #4d6380; --accent: #3b7ef8; --green: #22c55e;
  --amber: #f59e0b; --red: #ef4444;
  --font: "SF Pro Text","Inter",system-ui,-apple-system,sans-serif;
  --font-mono: "SF Mono","Fira Code",ui-monospace,monospace;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: var(--font); font-size: 13px; line-height: 1.5; min-height: 100vh; }
.topbar {
  position: sticky; top: 0; z-index: 100;
  display: flex; align-items: center; gap: 1.5rem;
  padding: 0 1.25rem; height: 48px;
  background: rgba(8,12,16,.92); backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border);
}
.brand { font-weight: 700; font-size: 15px; letter-spacing: -0.3px; color: var(--text); }
.brand span { color: var(--accent); }
.topbar-nav { display: flex; gap: 2px; margin-left: auto; }
.topbar-nav a {
  color: var(--muted); text-decoration: none; font-size: 12.5px;
  padding: 4px 10px; border-radius: 6px; transition: color .15s, background .15s;
}
.topbar-nav a:hover, .topbar-nav a.active { color: var(--text); background: var(--surface2); }
.filter-bar {
  position: sticky; top: 48px; z-index: 50;
  display: flex; align-items: center; gap: 8px;
  padding: 8px 1.25rem; border-bottom: 1px solid var(--border); background: var(--surface);
}
.chips { display: flex; gap: 4px; flex-wrap: wrap; }
.chip {
  padding: 3px 10px; border-radius: 20px; font-size: 11.5px; font-weight: 500;
  border: 1px solid var(--border2); background: transparent; color: var(--muted);
  cursor: pointer; transition: all .15s; letter-spacing: .3px;
}
.chip:hover { border-color: var(--accent); color: var(--accent); }
.chip.active { background: var(--accent); border-color: var(--accent); color: #fff; }
.chip.s-won  { background: #15803d; border-color: #15803d; color: #fff; }
.chip.s-lost { background: #991b1b; border-color: #991b1b; color: #fff; }
.chip.s-open { background: #b45309; border-color: #b45309; color: #fff; }
.stats-bar {
  display: flex; gap: 1.5rem; padding: 12px 1.25rem;
  border-bottom: 1px solid var(--border); background: var(--surface2);
}
.s-item { display: flex; flex-direction: column; gap: 2px; }
.s-label { font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: .5px; }
.s-val { font-size: 16px; font-weight: 700; color: var(--text); }
.s-val.pos { color: var(--green); }
.s-val.neg { color: var(--red); }
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
thead th {
  position: sticky; top: 0; z-index: 10;
  background: var(--surface); border-bottom: 1px solid var(--border2);
  padding: 7px 8px; font-size: 10.5px; font-weight: 600;
  letter-spacing: .6px; color: var(--muted); text-transform: uppercase;
  text-align: right; white-space: nowrap; user-select: none;
}
thead th:first-child, thead th.th-l { text-align: left; }
tbody td {
  padding: 6px 8px; border-bottom: 1px solid var(--border); font-size: 12px;
  text-align: right; color: var(--text); white-space: nowrap;
}
tbody td.td-l { text-align: left; }
tbody td.td-q { text-align: left; max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
tbody tr:hover td { background: rgba(59,126,248,.04); }
.s-won     { color: var(--green); font-weight: 600; }
.s-lost    { color: var(--red); font-weight: 600; }
.s-stopped { color: #f97316; font-weight: 600; }
.s-open  { color: var(--amber); }
.s-fail  { color: var(--muted); }
.pnl-pos { color: var(--green); }
.pnl-neg { color: var(--red); }
.chain-lvl { color: var(--accent); font-weight: 600; font-size: 11px; }
.mode-paper { color: var(--muted); font-size: 11px; }
.mode-live  { color: var(--accent); font-size: 11px; font-weight: 600; }
.load-more {
  display: block; margin: 1.5rem auto; padding: 8px 24px;
  background: var(--surface2); border: 1px solid var(--border2);
  color: var(--muted); border-radius: 8px; cursor: pointer; font-size: 13px;
  transition: all .15s;
}
.load-more:hover { color: var(--text); border-color: var(--accent); }
.empty { padding: 3rem 1.25rem; text-align: center; color: var(--muted); }
.clear-btn {
  margin-left: auto; padding: 5px 14px;
  background: transparent; border: 1px solid #7f1d1d; color: #f87171;
  border-radius: 7px; cursor: pointer; font-size: 12px; transition: all .15s;
}
.clear-btn:hover { background: rgba(239,68,68,.1); border-color: var(--red); color: var(--red); }
</style>
</head>
<body>
<header class="topbar">
  <span class="brand">poly<span>_bot</span></span>
  <nav class="topbar-nav">
    <a href="/">Markets</a>
    <a href="/harvest">Harvest</a>
    <a href="/events">Events</a>
    <a href="/orders" class="active">Orders</a>
    <a href="/chains">Chains</a>
    <a href="/nearct">NearCT</a>
    <a href="/taxonomy">Taxonomy</a>
  </nav>
</header>

<div class="stats-bar" id="stats-bar">
  <div class="s-item"><span class="s-label">Total</span><span class="s-val" id="st-total">—</span></div>
  <div class="s-item"><span class="s-label">Open</span><span class="s-val s-open" id="st-open">—</span></div>
  <div class="s-item"><span class="s-label">Won</span><span class="s-val s-won" id="st-won">—</span></div>
  <div class="s-item"><span class="s-label">Lost</span><span class="s-val s-lost" id="st-lost">—</span></div>
  <div class="s-item"><span class="s-label">Win rate</span><span class="s-val" id="st-wr">—</span></div>
  <div class="s-item"><span class="s-label">Realized P&amp;L</span><span class="s-val" id="st-pnl">—</span></div>
  <div class="s-item"><span class="s-label">Est. pending</span><span class="s-val" id="st-est">—</span></div>
  <button class="clear-btn" id="snapshot-session-btn" title="Archive current paper orders as a named session">📦 Snapshot session</button>
</div>

<div class="filter-bar">
  <div class="chips" id="domain-chips">
    <button class="chip active" data-domain="">All</button>
    <button class="chip" data-domain="crypto">Crypto</button>
    <button class="chip" data-domain="sports">Sports</button>
  </div>
  <div style="width:1px;background:var(--border2);height:18px;margin:0 4px;flex-shrink:0"></div>
  <div class="chips" id="chips">
    <button class="chip active" data-status="">ALL</button>
    <button class="chip" data-status="filled">Open</button>
    <button class="chip" data-status="won">Won</button>
    <button class="chip" data-status="lost">Lost</button>
    <button class="chip" data-status="failed">Failed</button>
  </div>
</div>

<div class="table-wrap">
  <table id="orders-table">
    <thead>
      <tr>
        <th class="th-l">Time (UTC)</th>
        <th class="th-l">Domain</th>
        <th class="th-l">Side</th>
        <th>Ask</th>
        <th>Yield%</th>
        <th>$Size</th>
        <th>Lvl</th>
        <th>Min@entry</th>
        <th>Expiry (UTC)</th>
        <th>Mode</th>
        <th>Status</th>
        <th>P&amp;L $</th>
        <th class="th-l">Question</th>
      </tr>
    </thead>
    <tbody id="orders-tbody">
      <tr><td colspan="13" class="empty">Loading…</td></tr>
    </tbody>
  </table>
</div>
<button class="load-more" id="load-more" style="display:none">Load more</button>

<script>
'use strict';
(function() {
  const PAGE = 100;
  let currentStatus = '';
  let currentDomain = '';
  let offset = 0;
  let allLoaded = false;

  const tbody = document.getElementById('orders-tbody');
  const loadMore = document.getElementById('load-more');

  // ── stats ────────────────────────────────────────────────
  async function loadStats(domain) {
    try {
      const params = new URLSearchParams({ limit: 1000, paper: 'true' });
      if (domain) params.set('domain', domain);
      const r = await fetch('/api/orders?' + params);
      const rows = await r.json();
      let total = rows.length, won = 0, lost = 0, open = 0;
      let pnl = 0, est = 0;
      for (const o of rows) {
        if (o.status === 'won')         { won++;  pnl += (o.pnl_usd || 0); }
        if (o.status === 'lost')        { lost++; pnl += (o.pnl_usd || 0); }
        if (o.status === 'stopped_out') {         pnl += (o.pnl_usd || 0); }
        if (o.status === 'filled')      { open++; est += (o.pnl_usd || 0); }
      }
      const resolved = won + lost;
      document.getElementById('st-total').textContent = total;
      document.getElementById('st-open').textContent  = open;
      document.getElementById('st-won').textContent   = won;
      document.getElementById('st-lost').textContent  = lost;
      document.getElementById('st-wr').textContent    = resolved ? (won/resolved*100).toFixed(1)+'%' : '—';
      const pnlEl = document.getElementById('st-pnl');
      pnlEl.textContent = (pnl >= 0 ? '+' : '') + pnl.toFixed(2) + ' $';
      pnlEl.className = 's-val ' + (pnl >= 0 ? 'pos' : 'neg');
      const estEl = document.getElementById('st-est');
      estEl.textContent = (est >= 0 ? '+' : '') + est.toFixed(2) + ' $';
      estEl.className = 's-val ' + (est >= 0 ? 'pos' : 'neg');
    } catch(_) {}
  }

  // ── table ─────────────────────────────────────────────────
  async function loadOrders(reset) {
    if (reset) { offset = 0; allLoaded = false; }
    const params = new URLSearchParams({ limit: PAGE, offset, paper: 'true' });
    if (currentStatus) params.set('status', currentStatus);
    if (currentDomain) params.set('domain', currentDomain);
    const r = await fetch('/api/orders?' + params);
    const rows = await r.json();

    if (reset) tbody.innerHTML = '';
    if (!rows.length && reset) {
      tbody.innerHTML = '<tr><td colspan="13" class="empty">No orders found.</td></tr>';
      loadMore.style.display = 'none';
      return;
    }
    if (rows.length < PAGE) { allLoaded = true; loadMore.style.display = 'none'; }
    else loadMore.style.display = 'block';

    const frag = document.createDocumentFragment();
    for (const o of rows) {
      const tr = document.createElement('tr');
      const sc = statusCls(o.status);
      const sl = statusLabel(o.status);
      const yield_ = o.net_yield_pct != null ? o.net_yield_pct.toFixed(2)+'%' : '—';
      const pnl = o.pnl_usd;
      const pnlStr = pnl != null ? (pnl >= 0 ? '+' : '') + pnl.toFixed(2) : '—';
      const pnlCls = pnl != null ? (pnl >= 0 ? 'pnl-pos' : 'pnl-neg') : '';
      const lvl = o.chain_level != null && o.chain_level > 0 ? 'L'+o.chain_level : (o.chain_id ? 'L0' : '—');
      const lvlCls = o.chain_level > 0 ? 'chain-lvl' : '';
      tr.innerHTML =
        '<td class="td-l">' + fmtTime(o.created_at) + '</td>' +
        '<td class="td-l">' + (o.domain || '—') + '</td>' +
        '<td class="td-l">' + (o.underlying ? esc(o.underlying).toUpperCase() + ' ' : '') + (o.side||'').toUpperCase() + '</td>' +
        '<td>' + (o.price_target != null ? o.price_target.toFixed(4) : '—') + '</td>' +
        '<td>' + yield_ + '</td>' +
        '<td>' + (o.size_usd != null ? o.size_usd.toFixed(0) : '—') + '</td>' +
        '<td class="' + lvlCls + '">' + lvl + '</td>' +
        '<td>' + (o.minutes_left_at_entry != null ? o.minutes_left_at_entry.toFixed(1)+'m' : '—') + '</td>' +
        '<td>' + fmtEndTs(o.end_ts) + '</td>' +
        '<td class="' + (o.paper ? 'mode-paper' : 'mode-live') + '">' + (o.paper ? 'paper' : 'live') + '</td>' +
        '<td class="' + sc + '">' + sl + '</td>' +
        '<td class="' + pnlCls + '">' + pnlStr + '</td>' +
        '<td class="td-q" title="' + esc(o.question||'') + '">' + esc(o.question||'') + '</td>';
      frag.appendChild(tr);
    }
    tbody.appendChild(frag);
    offset += rows.length;
  }

  // ── chips ─────────────────────────────────────────────────
  document.getElementById('domain-chips').addEventListener('click', (e) => {
    const btn = e.target.closest('.chip');
    if (!btn) return;
    document.querySelectorAll('#domain-chips .chip').forEach(c => c.classList.remove('active'));
    btn.classList.add('active');
    currentDomain = btn.dataset.domain || '';
    loadOrders(true);
    loadStats(currentDomain);
  });

  document.getElementById('chips').addEventListener('click', (e) => {
    const btn = e.target.closest('.chip');
    if (!btn) return;
    document.querySelectorAll('#chips .chip').forEach(c => c.classList.remove('active','s-won','s-lost','s-open'));
    currentStatus = btn.dataset.status || '';
    const statusMap = { won: 's-won', lost: 's-lost', filled: 's-open' };
    btn.classList.add('active');
    if (statusMap[currentStatus]) btn.classList.add(statusMap[currentStatus]);
    loadOrders(true);
  });

  loadMore.addEventListener('click', () => { if (!allLoaded) loadOrders(false); });

  // ── helpers ───────────────────────────────────────────────
  function fmtTime(iso) {
    if (!iso) return '—';
    return new Date(iso).toISOString().replace('T', ' ').slice(0, 19);
  }
  function fmtEndTs(s) {
    if (!s) return '—';
    // SQLite stores as 'YYYY-MM-DD HH:MM:SS' — replace space with T for Date parsing
    return s.replace(' ', 'T').slice(0, 16).replace('T', ' ');
  }
  function statusCls(s) {
    if (s === 'won')          return 's-won';
    if (s === 'lost')         return 's-lost';
    if (s === 'filled')       return 's-open';
    if (s === 'stopped_out')  return 's-stopped';
    if (s === 'failed')       return 's-fail';
    return '';
  }
  function statusLabel(s) {
    if (s === 'filled')       return 'open';
    if (s === 'stopped_out')  return 'SL';
    return s || '—';
  }
  function esc(s) {
    return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // ── snapshot session ──────────────────────────────────
  document.getElementById('snapshot-session-btn').addEventListener('click', async () => {
    const n = document.getElementById('st-total').textContent;
    if (!n || n === '0') { alert('No unsnapshot paper orders to archive.'); return; }
    const label = prompt(`Archive ${n} order(s) as a session.\nSession label (leave blank for auto):`, '');
    if (label === null) return;
    const fd = new FormData();
    fd.append('label', label);
    const r = await fetch('/api/orders/snapshot', { method: 'POST', body: fd });
    const d = await r.json();
    alert(`Archived ${d.orders} order(s) as "${d.label}"\nP&L: ${d.pnl_usd >= 0 ? '+' : ''}${d.pnl_usd.toFixed(4)} USD`);
    loadStats(currentDomain);
    loadOrders(true);
  });

  loadStats(currentDomain);
  loadOrders(true);
})();
</script>
</body>
</html>
"""


_CHAINS_PAGE_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>poly_bot · chains</title>
<style>
:root {
  --bg: #080c10; --surface: #0f1620; --surface2: #141e2c;
  --border: #1a2535; --border2: #243044; --text: #dde6f0;
  --muted: #4d6380; --accent: #3b7ef8; --green: #22c55e;
  --amber: #f59e0b; --red: #ef4444; --orange: #f97316; --purple: #a855f7;
  --font: "SF Pro Text","Inter",system-ui,-apple-system,sans-serif;
  --font-mono: "SF Mono","Fira Code",ui-monospace,monospace;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: var(--font); font-size: 13px; line-height: 1.5; min-height: 100vh; }
.topbar {
  position: sticky; top: 0; z-index: 100;
  display: flex; align-items: center; gap: 1.5rem;
  padding: 0 1.25rem; height: 48px;
  background: rgba(8,12,16,.92); backdrop-filter: blur(12px);
  border-bottom: 1px solid var(--border);
}
.brand { font-weight: 700; font-size: 15px; letter-spacing: -0.3px; color: var(--text); }
.brand span { color: var(--accent); }
.topbar-nav { display: flex; gap: 2px; margin-left: auto; }
.topbar-nav a {
  color: var(--muted); text-decoration: none; font-size: 12.5px;
  padding: 4px 10px; border-radius: 6px; transition: color .15s, background .15s;
}
.topbar-nav a:hover, .topbar-nav a.active { color: var(--text); background: var(--surface2); }
.summary-bar {
  display: flex; gap: 2rem; align-items: center;
  padding: 10px 1.25rem; border-bottom: 1px solid var(--border);
  background: var(--surface2);
}
.s-item { display: flex; flex-direction: column; gap: 1px; }
.s-label { font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: .5px; }
.s-val { font-size: 17px; font-weight: 700; }
.s-val.pos { color: var(--green); }
.s-val.neg { color: var(--red); }
.s-val.neutral { color: var(--text); }
.conn-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--muted); margin-left: auto; transition: background .3s; }
.conn-dot.live { background: var(--green); }
.conn-dot.err  { background: var(--red); }
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(310px, 1fr));
  gap: 12px;
  padding: 16px 1.25rem;
}
.card {
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: 10px;
  overflow: hidden;
  transition: border-color .2s;
}
.card.st-active  { border-color: #92400e; }
.card.st-closing { border-color: var(--orange); }
.card.st-pending { border-color: #1e3a6e; }
.card.st-dead    { border-color: #4c0519; }
.card.st-closed  { border-color: var(--border); opacity: .7; }
.card.st-idle    { border-color: var(--border); opacity: .6; }
.card-header {
  display: flex; align-items: center; gap: 8px;
  padding: 9px 12px 7px; border-bottom: 1px solid var(--border);
}
.chain-short { font-family: var(--font-mono); font-size: 11px; color: var(--muted); letter-spacing: .5px; }
.dot-track { display: flex; gap: 4px; align-items: center; flex: 1; }
.dot {
  width: 10px; height: 10px; border-radius: 50%;
  flex-shrink: 0;
}
.dot-won    { background: var(--green); }
.dot-active { background: var(--amber); box-shadow: 0 0 0 3px rgba(245,158,11,.2); }
.dot-ticket { background: transparent; border: 1.5px solid var(--accent); }
.dot-lost   { background: var(--red); }
.dot-sl     { background: var(--orange); }
.dot-empty  { background: transparent; border: 1.5px dashed var(--border2); }
.badge {
  font-size: 10px; font-weight: 600; padding: 2px 7px; border-radius: 4px;
  text-transform: uppercase; letter-spacing: .4px; white-space: nowrap;
}
.badge-active  { background: rgba(245,158,11,.15); color: var(--amber); }
.badge-closing { background: rgba(249,115,22,.15); color: var(--orange); }
.badge-pending { background: rgba(59,126,248,.15); color: var(--accent); }
.badge-dead    { background: rgba(239,68,68,.1);   color: var(--red); }
.badge-closed  { background: rgba(77,99,128,.15);  color: var(--muted); }
.badge-idle    { background: rgba(77,99,128,.1);   color: var(--muted); }
.card-body { padding: 10px 12px 8px; }
.question {
  font-size: 12.5px; color: var(--text); font-weight: 500;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  margin-bottom: 5px;
}
.question.muted { color: var(--muted); font-style: italic; }
.meta { font-size: 11.5px; color: var(--muted); display: flex; gap: 6px; flex-wrap: wrap; }
.meta-val { color: var(--text); }
.card-footer {
  display: flex; align-items: center; justify-content: space-between;
  padding: 7px 12px 9px; border-top: 1px solid var(--border); background: var(--surface2);
}
.pnl { font-size: 12px; font-weight: 600; }
.pnl.pos { color: var(--green); }
.pnl.neg { color: var(--red); }
.pnl.zero { color: var(--muted); }
.close-btn {
  font-size: 11px; font-weight: 500;
  padding: 4px 10px; border-radius: 6px;
  background: transparent; border: 1px solid #4c0519;
  color: #f87171; cursor: pointer; transition: all .15s;
}
.close-btn:hover:not(:disabled) { background: rgba(239,68,68,.1); border-color: var(--red); color: var(--red); }
.close-btn:disabled { opacity: .4; cursor: default; }
.empty-state { padding: 4rem 1.25rem; text-align: center; color: var(--muted); font-size: 14px; }
</style>
</head>
<body>
<header class="topbar">
  <span class="brand">poly<span>_bot</span></span>
  <nav class="topbar-nav">
    <a href="/">Markets</a>
    <a href="/harvest">Harvest</a>
    <a href="/events">Events</a>
    <a href="/orders">Orders</a>
    <a href="/chains" class="active">Chains</a>
    <a href="/taxonomy">Taxonomy</a>
  </nav>
</header>

<div class="summary-bar">
  <div class="s-item"><span class="s-label">Active</span><span class="s-val neutral" id="sum-active">—</span></div>
  <div class="s-item"><span class="s-label">Pending ticket</span><span class="s-val neutral" id="sum-pending">—</span></div>
  <div class="s-item"><span class="s-label">Total chains</span><span class="s-val neutral" id="sum-total">—</span></div>
  <div class="s-item"><span class="s-label">Realized P&amp;L</span><span class="s-val" id="sum-pnl">—</span></div>
  <div class="conn-dot" id="conn-dot" title="SSE connection"></div>
</div>

<div class="grid" id="chain-grid">
  <div class="empty-state">Connecting…</div>
</div>

<script>
'use strict';
(function() {
  const grid = document.getElementById('chain-grid');
  const dot  = document.getElementById('conn-dot');
  let es = null;
  let retryMs = 2000;

  function connect() {
    if (es) { es.close(); }
    es = new EventSource('/api/stream/chains');
    es.onopen = () => { dot.className = 'conn-dot live'; retryMs = 2000; };
    es.onerror = () => {
      dot.className = 'conn-dot err';
      es.close();
      setTimeout(connect, retryMs);
      retryMs = Math.min(retryMs * 2, 30000);
    };
    es.onmessage = (e) => {
      let chains;
      try { chains = JSON.parse(e.data); } catch(_) { return; }
      if (chains && chains.error) { grid.innerHTML = '<div class="empty-state">Error loading chain data.</div>'; return; }
      renderGrid(chains);
    };
  }

  function renderGrid(chains) {
    updateSummary(chains);
    if (!chains || !chains.length) {
      grid.innerHTML = '<div class="empty-state">No chains yet. Base orders will start chains automatically.</div>';
      return;
    }
    const frag = document.createDocumentFragment();
    for (const c of chains) frag.appendChild(buildCard(c));
    grid.replaceChildren(frag);
  }

  function updateSummary(chains) {
    const active  = chains.filter(c => c.status === 'active' || c.status === 'closing').length;
    const pending = chains.filter(c => c.status === 'pending').length;
    const total   = chains.length;
    const pnl     = chains.reduce((s, c) => s + (c.total_pnl_usd || 0), 0);
    document.getElementById('sum-active').textContent  = active;
    document.getElementById('sum-pending').textContent = pending;
    document.getElementById('sum-total').textContent   = total;
    const pnlEl = document.getElementById('sum-pnl');
    pnlEl.textContent = (pnl >= 0 ? '+' : '') + pnl.toFixed(4) + ' $';
    pnlEl.className = 's-val ' + (pnl > 0 ? 'pos' : pnl < 0 ? 'neg' : 'zero');
  }

  function buildCard(c) {
    const div = document.createElement('div');
    div.className = 'card st-' + c.status;
    div.innerHTML = cardHeader(c) + cardBody(c) + cardFooter(c);
    const btn = div.querySelector('.close-btn');
    if (btn) {
      btn.addEventListener('click', () => doClose(c.chain_id, btn));
    }
    return div;
  }

  function cardHeader(c) {
    return `<div class="card-header">
      <span class="chain-short">${esc(c.short_id)}</span>
      <div class="dot-track">${buildDots(c)}</div>
      <span class="badge badge-${c.status}">${c.status}</span>
    </div>`;
  }

  function buildDots(c) {
    const dots = [];
    // one dot per known level (from orders)
    for (const lv of (c.levels || [])) {
      const s = lv.status;
      if (s === 'won')                           dots.push('<span class="dot dot-won" title="won L' + lv.level + '"></span>');
      else if (s === 'filled' || s === 'pending') dots.push('<span class="dot dot-active" title="open L' + lv.level + '"></span>');
      else if (s === 'lost')                     dots.push('<span class="dot dot-lost" title="lost L' + lv.level + '"></span>');
      else if (s === 'stopped_out')              dots.push('<span class="dot dot-sl" title="SL L' + lv.level + '"></span>');
      else                                       dots.push('<span class="dot dot-empty"></span>');
    }
    // pending ticket dot
    if (c.pending_ticket) {
      dots.push('<span class="dot dot-ticket" title="next: $' + (c.pending_ticket.size_usd || 0).toFixed(2) + ' ready"></span>');
    }
    return dots.join('');
  }

  function cardBody(c) {
    const ao = c.active_order;
    let qHtml, metaHtml;
    if (ao) {
      qHtml = `<div class="question" title="${esc(ao.question)}">${esc(ao.question)}</div>`;
      const age = ao.created_at ? relTime(ao.created_at) : '—';
      const lvl = 'L' + ao.level;
      const dom = ao.domain || '—';
      const sz  = ao.size_usd != null ? '$' + ao.size_usd.toFixed(2) : '—';
      const ask = ao.price_target != null ? 'ask ' + ao.price_target.toFixed(4) : '—';
      metaHtml = `<div class="meta">
        <span><span class="meta-val">${sz}</span></span>
        <span>·</span>
        <span><span class="meta-val">${ask}</span></span>
        <span>·</span>
        <span>${dom} <span class="meta-val">${lvl}</span></span>
        <span>·</span>
        <span><span class="meta-val">${age}</span></span>
      </div>`;
    } else if (c.pending_ticket) {
      const t = c.pending_ticket;
      const sz = t.size_usd != null ? '$' + t.size_usd.toFixed(4) : '—';
      qHtml = `<div class="question muted">Waiting for next opportunity…</div>`;
      metaHtml = `<div class="meta">Next: <span class="meta-val">${sz}</span> · Level ${t.level}</div>`;
    } else {
      const last = c.levels && c.levels.length ? c.levels[c.levels.length - 1] : null;
      const lastQ = last ? last.question : '';
      qHtml = `<div class="question muted">${esc(lastQ) || 'Chain ended'}</div>`;
      metaHtml = `<div class="meta">No active order or pending ticket</div>`;
    }
    return `<div class="card-body">${qHtml}${metaHtml}</div>`;
  }

  function cardFooter(c) {
    const pnl = c.total_pnl_usd || 0;
    const pnlCls  = pnl > 0 ? 'pos' : pnl < 0 ? 'neg' : 'zero';
    const pnlText = (pnl >= 0 ? '+' : '') + pnl.toFixed(4) + ' $ realized';
    const canClose = c.status === 'active' || c.status === 'pending';
    const btnHtml = canClose
      ? `<button class="close-btn" data-chain="${esc(c.chain_id)}">Close ×</button>`
      : (c.status === 'closing' ? `<button class="close-btn" disabled>closing…</button>` : '');
    return `<div class="card-footer">
      <span class="pnl ${pnlCls}">${pnlText}</span>
      ${btnHtml}
    </div>`;
  }

  async function doClose(chainId, btn) {
    btn.disabled = true;
    btn.textContent = '…';
    try {
      const r = await fetch('/api/chains/' + encodeURIComponent(chainId) + '/close', { method: 'POST' });
      if (!r.ok) { const d = await r.json(); alert('Error: ' + (d.detail || r.status)); btn.disabled = false; btn.textContent = 'Close ×'; }
      // SSE will refresh within 5s — no manual DOM update needed
    } catch(err) {
      alert('Network error: ' + err);
      btn.disabled = false;
      btn.textContent = 'Close ×';
    }
  }

  function relTime(isoOrSqlite) {
    const s = String(isoOrSqlite).replace(' ', 'T');
    const dt = new Date(s.includes('Z') || s.includes('+') ? s : s + 'Z');
    if (isNaN(dt)) return '—';
    const diff = Math.floor((Date.now() - dt) / 1000);
    if (diff < 60)   return diff + 's ago';
    if (diff < 3600) return Math.floor(diff/60) + 'm ago';
    return Math.floor(diff/3600) + 'h ago';
  }

  function esc(s) {
    return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  connect();
})();
</script>
</body>
</html>
"""


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
  position: sticky; top: 48px; z-index: 50;
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
  position: sticky; top: 0; z-index: 10;
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

/* ─── ENGINE PANEL ────────────────────────────────────── */
.engine-panel {
  margin: 0; border-top: 1px solid var(--border);
  background: var(--surface);
}
.engine-header {
  display: flex; align-items: center; gap: 1rem;
  padding: 10px 1.25rem; cursor: pointer;
  border-bottom: 1px solid var(--border);
  user-select: none;
}
.engine-header:hover { background: var(--surface2); }
.engine-title { font-weight: 600; font-size: 12.5px; letter-spacing: .3px; color: var(--text); }
.engine-pills { display: flex; gap: 8px; align-items: center; margin-left: auto; }
.e-pill {
  font-size: 10.5px; font-weight: 700; letter-spacing: .5px;
  padding: 2px 8px; border-radius: 4px; text-transform: uppercase;
}
.e-pill.paper { background: rgba(245,158,11,.15); color: #fbbf24; }
.e-pill.live  { background: rgba(34,197,94,.15);  color: #4ade80; }
.e-pill.off   { background: rgba(100,116,139,.1); color: #94a3b8; }
.e-stat { font-size: 11.5px; color: var(--muted); }
.e-stat strong { color: var(--text); }
.engine-body { padding: 0 1.25rem 1rem; }
.engine-body.collapsed { display: none; }
.engine-table-wrap { overflow-x: auto; margin-top: 10px; }
.engine-table { width: 100%; border-collapse: collapse; font-size: 11.5px; }
.engine-table th {
  padding: 5px 8px; text-align: right; font-size: 10px;
  font-weight: 600; letter-spacing: .5px; text-transform: uppercase;
  color: var(--muted); border-bottom: 1px solid var(--border2);
  white-space: nowrap;
}
.engine-table th.th-l { text-align: left; }
.engine-table td {
  padding: 5px 8px; border-bottom: 1px solid var(--border);
  text-align: right; font-family: var(--font-mono); white-space: nowrap;
  vertical-align: middle;
}
.engine-table td.td-l { text-align: left; font-family: var(--font); }
.engine-table td.td-q { text-align: left; font-family: var(--font); max-width: 280px; overflow: hidden; text-overflow: ellipsis; }
.engine-table tr:hover td { background: rgba(59,126,248,.04); }
.s-paper  { color: #fbbf24; }
.s-live   { color: #4ade80; }
.s-won     { color: var(--green); font-weight: 600; }
.s-lost    { color: var(--red); font-weight: 600; }
.s-filled  { color: var(--amber); }
.s-stopped { color: #f97316; font-weight: 600; }
.s-failed { color: var(--muted); }
.s-pending{ color: var(--muted); }
.toggle-icon { margin-left: auto; color: var(--muted); font-size: 11px; transition: transform .2s; }
.toggle-icon.open { transform: rotate(180deg); }
.no-orders { color: var(--muted); font-size: 12px; padding: 1.5rem 0; text-align: center; }
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
    <span class="stat-pill">Prices <strong id="s-price-tick">—</strong></span>
  </div>
  <nav class="topbar-nav">
    <a href="/harvest">Harvest</a>
    <a href="/events">Events</a>
    <a href="/orders">Orders</a>
    <a href="/chains">Chains</a>
    <a href="/nearct">NearCT</a>
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
  // refresh market list + price stream every 60s (crypto markets expire fast)
  setInterval(() => loadMarkets(), 60_000);
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
  let _priceTick = 0;
  PRICE_ES.onmessage = (e) => {
    try {
      const d = JSON.parse(e.data);
      const prices = d.prices || {};
      const n = Object.keys(prices).length;
      _priceTick++;
      const el = document.getElementById('s-price-tick');
      if (el) el.textContent = '#' + _priceTick + ' (' + n + ')';
      applyPrices(prices);
    } catch(_) {}
  };
  PRICE_ES.onerror = () => {
    // browser auto-reconnects; just log so we know it happened
    console.warn('price SSE error — browser will reconnect');
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
        // dot reflects live SSE price stream, not DB quote age
        // DB quote age shown in Quotes pill separately
        dot.className = 'dot ok';
        stEl.textContent = 'live';
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

<!-- ── ENGINE PANEL ───────────────────────────────────── -->
<section class="engine-panel" id="engine-panel">
  <div class="engine-header" id="engine-header">
    <span class="engine-title">⚡ Harvest Engine</span>
    <div class="engine-pills">
      <span class="e-pill off" id="e-mode">OFF</span>
      <span class="e-stat">Today <strong id="e-today">—</strong></span>
      <span class="e-stat">Open <strong id="e-open">—</strong></span>
      <span class="e-stat">Total <strong id="e-total">—</strong></span>
      <span class="e-stat">P&amp;L realized <strong id="e-pnl">—</strong></span>
      <span class="e-stat">Last <strong id="e-last">—</strong></span>
      <a href="/orders" style="font-size:11.5px;color:var(--accent);text-decoration:none;margin-left:4px">All orders →</a>
    </div>
    <span class="toggle-icon" id="engine-toggle">▾</span>
  </div>
  <div class="engine-body" id="engine-body">
    <div class="engine-table-wrap">
      <table class="engine-table">
        <thead>
          <tr>
            <th class="th-l">Time (UTC)</th>
            <th class="th-l">Domain</th>
            <th class="th-l">Side</th>
            <th>Ask</th>
            <th>Yield%</th>
            <th>$Size</th>
            <th>Min@entry</th>
            <th>Mode</th>
            <th>Status</th>
            <th class="th-l">Question</th>
          </tr>
        </thead>
        <tbody id="engine-tbody">
          <tr><td colspan="10" class="no-orders">Engine not running — start with: <code>poly-bot harvest engine</code></td></tr>
        </tbody>
      </table>
    </div>
  </div>
</section>

<script>
'use strict';
(function enginePanel() {
  // ── toggle collapse ──────────────────────────────────
  const header = document.getElementById('engine-header');
  const body   = document.getElementById('engine-body');
  const icon   = document.getElementById('engine-toggle');
  let collapsed = false;
  header.addEventListener('click', () => {
    collapsed = !collapsed;
    body.classList.toggle('collapsed', collapsed);
    icon.classList.toggle('open', !collapsed);
  });

  // ── SSE stream ───────────────────────────────────────
  const modeEl  = document.getElementById('e-mode');
  const todayEl = document.getElementById('e-today');
  const openEl  = document.getElementById('e-open');
  const totalEl = document.getElementById('e-total');
  const pnlEl   = document.getElementById('e-pnl');
  const lastEl  = document.getElementById('e-last');
  const tbody   = document.getElementById('engine-tbody');

  function fmtTime(iso) {
    if (!iso) return '—';
    return new Date(iso).toISOString().slice(11, 19);
  }

  function fmtAge(iso) {
    if (!iso) return '—';
    const s = Math.round((Date.now() - new Date(iso).getTime()) / 1000);
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.round(s/60) + 'm ago';
    return (s/3600).toFixed(1) + 'h ago';
  }

  function statusCls(s) {
    if (s === 'won')          return 's-won';
    if (s === 'lost')         return 's-lost';
    if (s === 'filled')       return 's-filled';
    if (s === 'stopped_out')  return 's-stopped';
    if (s === 'failed')       return 's-failed';
    return 's-pending';
  }

  function statusLabel(s) {
    if (s === 'filled')       return 'open';
    if (s === 'stopped_out')  return 'SL';
    return s || '—';
  }

  function apply(d) {
    if (d.error) return;

    // mode pill
    if (d.orders_total === 0 && !d.last_order_at) {
      modeEl.textContent = 'OFF'; modeEl.className = 'e-pill off';
    } else if (d.paper_mode) {
      modeEl.textContent = 'PAPER'; modeEl.className = 'e-pill paper';
    } else {
      modeEl.textContent = 'LIVE'; modeEl.className = 'e-pill live';
    }

    todayEl.textContent = d.orders_today ?? '—';
    openEl.textContent  = d.orders_open ?? '—';
    totalEl.textContent = d.orders_total ?? '—';
    const pnl = d.pnl_paper_usd;
    pnlEl.textContent = pnl != null ? (pnl >= 0 ? '+' : '') + pnl.toFixed(2) + '$' : '—';
    lastEl.textContent = fmtAge(d.last_order_at);

    const orders = d.recent_orders || [];
    if (!orders.length) {
      tbody.innerHTML = '<tr><td colspan="10" class="no-orders">No orders yet — start engine: <code>poly-bot harvest engine</code></td></tr>';
      return;
    }
    const frag = document.createDocumentFragment();
    for (const o of orders) {
      const tr = document.createElement('tr');
      const modeCls = o.paper ? 's-paper' : 's-live';
      const modeLabel = o.paper ? 'paper' : 'live';
      const yield_ = o.net_yield_pct != null ? o.net_yield_pct.toFixed(2) + '%' : '—';
      tr.innerHTML =
        '<td class="td-l">' + fmtTime(o.created_at) + '</td>' +
        '<td class="td-l">' + (o.domain || '—') + '</td>' +
        '<td class="td-l">' + (o.underlying ? o.underlying.toUpperCase() + ' ' : '') + (o.side||'').toUpperCase() + '</td>' +
        '<td>' + (o.price_target != null ? o.price_target.toFixed(4) : '—') + '</td>' +
        '<td>' + yield_ + '</td>' +
        '<td>' + (o.size_usd != null ? o.size_usd.toFixed(0) : '—') + '</td>' +
        '<td>' + (o.minutes_left_at_entry != null ? o.minutes_left_at_entry.toFixed(1)+'m' : '—') + '</td>' +
        '<td class="' + modeCls + '">' + modeLabel + '</td>' +
        '<td class="' + statusCls(o.status) + '">' + statusLabel(o.status) + '</td>' +
        '<td class="td-q" title="' + (o.question||'').replace(/"/g,'&quot;') + '">' + (o.question||'') + '</td>';
      frag.appendChild(tr);
    }
    tbody.innerHTML = '';
    tbody.appendChild(frag);
  }

  function connect() {
    const es = new EventSource('/api/stream/engine');
    es.onmessage = (e) => { try { apply(JSON.parse(e.data)); } catch(_) {} };
    es.onerror = () => { es.close(); setTimeout(connect, 6000); };
  }
  connect();
})();
</script>
</body>
</html>
"""


_NEARCT_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>poly_bot – NearCT</title>
<style>
:root{--bg:#0b1017;--surface:#111820;--surface2:#161f2b;--border:#1e2d3d;--border2:#243044;
--text:#e8eef7;--muted:#8b9cb3;--green:#22c55e;--red:#ef4444;--yellow:#f59e0b;--blue:#3b82f6;}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:'SF Mono',ui-monospace,monospace;font-size:13px;}
.topbar{display:flex;align-items:center;gap:12px;padding:10px 20px;
background:var(--surface);border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100;}
.brand{font-size:16px;font-weight:700;letter-spacing:-0.5px;}
.brand span{color:var(--blue);}
.topbar-nav{display:flex;gap:2px;margin-left:auto;}
.topbar-nav a{color:var(--muted);text-decoration:none;padding:.35rem .7rem;border-radius:5px;
border:1px solid transparent;font-size:12px;}
.topbar-nav a:hover,.topbar-nav a.active{color:var(--text);background:var(--surface2);border-color:var(--border2);}
.stats-bar{display:flex;gap:0;border-bottom:1px solid var(--border);flex-wrap:wrap;}
.s-item{padding:10px 20px;border-right:1px solid var(--border);display:flex;flex-direction:column;gap:2px;}
.s-label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.05em;}
.s-val{font-size:18px;font-weight:600;}
.s-won{color:var(--green);} .s-lost{color:var(--red);} .s-hold{color:var(--yellow);}
.s-scanning{color:var(--blue);}
.card{background:var(--surface);border:1px solid var(--border);border-radius:8px;
padding:16px 20px;margin:16px 20px;}
.card-title{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px;}
.card-body{display:flex;gap:24px;flex-wrap:wrap;}
.card-field{display:flex;flex-direction:column;gap:3px;}
.card-field .label{color:var(--muted);font-size:11px;}
.card-field .val{font-size:14px;font-weight:600;}
.countdown{color:var(--yellow);}
.table-wrap{overflow-x:auto;padding:0 20px 40px;}
table{width:100%;border-collapse:collapse;margin-top:4px;}
th{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.05em;
border-bottom:1px solid var(--border2);padding:7px 10px;text-align:right;white-space:nowrap;}
th.th-l{text-align:left;}
td{padding:7px 10px;border-bottom:1px solid var(--border);text-align:right;white-space:nowrap;}
td.td-l{text-align:left;}
td.td-q{text-align:left;max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.won{color:var(--green);} .lost{color:var(--red);} .pending{color:var(--yellow);}
.empty{text-align:center;color:var(--muted);padding:40px;}
.section-label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em;padding:16px 0 6px;}
.act-placed{color:var(--green);} .act-filtered{color:var(--yellow);} .act-error{color:var(--red);}
.act-none{color:var(--muted);}
tr.scan-row{cursor:pointer;} tr.scan-row:hover td{background:var(--surface2);}
tr.detail-row td{padding:0;background:var(--surface2);}
.detail-inner{padding:8px 16px 12px;}
.detail-inner table{width:auto;margin:0;font-size:12px;}
.detail-inner th{font-size:10px;padding:3px 8px;}
.detail-inner td{padding:3px 8px;border-bottom:1px solid var(--border);}
.r-mid_low{color:var(--muted);} .r-depth_low{color:var(--yellow);}
.r-complement_fail{color:var(--red);} .r-no_book{color:var(--red);}
.r-ask_low{color:var(--muted);} .r-bad_ts{color:var(--red);}
</style>
</head>
<body>
<header class="topbar">
  <span class="brand">poly<span>_bot</span></span>
  <nav class="topbar-nav">
    <a href="/">Markets</a>
    <a href="/harvest">Harvest</a>
    <a href="/events">Events</a>
    <a href="/orders">Orders</a>
    <a href="/chains">Chains</a>
    <a href="/nearct" class="active">NearCT</a>
    <a href="/taxonomy">Taxonomy</a>
  </nav>
</header>

<div class="stats-bar">
  <div class="s-item"><span class="s-label">Status</span><span class="s-val" id="st-state">—</span></div>
  <div class="s-item"><span class="s-label">Capital</span><span class="s-val" id="st-capital">—</span></div>
  <div class="s-item"><span class="s-label">Trades</span><span class="s-val" id="st-trades">—</span></div>
  <div class="s-item"><span class="s-label">Today</span><span class="s-val" id="st-today">—</span></div>
  <div class="s-item"><span class="s-label">Wins</span><span class="s-val s-won" id="st-wins">—</span></div>
  <div class="s-item"><span class="s-label">Losses</span><span class="s-val s-lost" id="st-losses">—</span></div>
  <div class="s-item"><span class="s-label">Stuck</span><span class="s-val" id="st-stuck">—</span></div>
  <div class="s-item"><span class="s-label">Win rate</span><span class="s-val" id="st-wr">—</span></div>
  <div class="s-item"><span class="s-label">Total P&amp;L</span><span class="s-val" id="st-pnl">—</span></div>
  <div class="s-item" style="margin-left:auto;align-self:center;padding-right:20px;">
    <button id="btn-reset" onclick="doReset()"
      style="background:transparent;border:1px solid var(--red);color:var(--red);
             padding:.35rem .8rem;border-radius:5px;font-size:12px;cursor:pointer;
             font-family:inherit;opacity:.7;" title="Delete all orders and reset capital to $10">
      Reset session
    </button>
  </div>
</div>

<div class="card" id="open-card" style="display:none">
  <div class="card-title">Open position</div>
  <div class="card-body">
    <div class="card-field"><span class="label">Market</span><span class="val" id="op-question">—</span></div>
    <div class="card-field"><span class="label">Ask</span><span class="val" id="op-ask">—</span></div>
    <div class="card-field"><span class="label">Yield</span><span class="val" id="op-yield">—</span></div>
    <div class="card-field"><span class="label">Size</span><span class="val" id="op-size">—</span></div>
    <div class="card-field"><span class="label">Expires</span><span class="val countdown" id="op-expires">—</span></div>
  </div>
</div>

<div class="table-wrap">
<div class="section-label">Recent scans</div>
<table id="scans-table">
  <thead>
    <tr>
      <th class="th-l">Time (UTC)</th>
      <th>Window</th>
      <th>Mid✓</th>
      <th>Book</th>
      <th>Candidates</th>
      <th>Best ask</th>
      <th>Best yield</th>
      <th>Mins left</th>
      <th class="th-l">Action</th>
    </tr>
  </thead>
  <tbody id="scans-tbody"><tr><td colspan="9" class="empty">Loading…</td></tr></tbody>
</table>
</div>

<div class="table-wrap">
<div class="section-label">Trade history</div>
<table id="orders-table">
  <thead>
    <tr>
      <th class="th-l">Time (UTC)</th>
      <th class="th-l">Domain</th>
      <th>Ask</th>
      <th>Yield%</th>
      <th>Size $</th>
      <th>Capital before</th>
      <th>Capital after</th>
      <th>Status</th>
      <th>P&amp;L $</th>
      <th class="th-l">Market</th>
    </tr>
  </thead>
  <tbody id="orders-tbody"><tr><td colspan="10" class="empty">Loading…</td></tr></tbody>
</table>
</div>

<script>
'use strict';
(function() {
  let _orders = [];

  function fmt$(v) { return v == null ? '—' : '$' + parseFloat(v).toFixed(4); }
  function fmtPct(v) { return v == null ? '—' : (v*100).toFixed(2)+'%'; }
  function fmtDt(iso) {
    if (!iso) return '—';
    return new Date(iso).toISOString().replace('T',' ').substring(0,19);
  }
  function countdown(iso) {
    if (!iso) return '—';
    const diff = Math.round((new Date(iso) - Date.now()) / 1000);
    if (diff <= 0) return 'resolving…';
    const m = Math.floor(diff/60), s = diff%60;
    return m > 0 ? m+'m '+s+'s' : s+'s';
  }

  async function loadStatus() {
    try {
      const r = await fetch('/api/nearct/status');
      const s = await r.json();
      const stateEl = document.getElementById('st-state');
      stateEl.textContent = s.state || '—';
      stateEl.className = 's-val ' + (
        s.state === 'holding' ? 's-hold' :
        s.state === 'quoting' ? 's-hold' :
        s.state === 'scanning' ? 's-scanning' : '');
      document.getElementById('st-capital').textContent = fmt$(s.capital);
      document.getElementById('st-trades').textContent = s.total_trades ?? '—';
      document.getElementById('st-today').textContent = s.trades_today ?? '—';
      document.getElementById('st-wins').textContent = s.wins ?? '—';
      document.getElementById('st-losses').textContent = s.losses ?? '—';
      document.getElementById('st-stuck').textContent = s.stuck ?? '—';
      const wr = s.total_trades ? ((s.wins/s.total_trades)*100).toFixed(1)+'%' : '—';
      document.getElementById('st-wr').textContent = wr;
      const pnlEl = document.getElementById('st-pnl');
      pnlEl.textContent = s.total_pnl != null ? (s.total_pnl >= 0 ? '+' : '') + '$'+s.total_pnl.toFixed(4) : '—';
      pnlEl.className = 's-val ' + (s.total_pnl > 0 ? 's-won' : s.total_pnl < 0 ? 's-lost' : '');

      const card = document.getElementById('open-card');
      if (s.open_order) {
        card.style.display = '';
        const o = s.open_order;
        document.querySelector('#open-card .card-title').textContent = 'Open position';
        document.getElementById('op-question').textContent = o.question || '—';
        document.getElementById('op-ask').textContent = o.ask != null ? o.ask.toFixed(4) : '—';
        document.getElementById('op-yield').textContent = fmtPct(o.net_yield_pct);
        document.getElementById('op-size').textContent = fmt$(o.size_usd);
        document.getElementById('op-expires').textContent = countdown(o.end_ts);
      } else if (s.open_quote) {
        card.style.display = '';
        const q = s.open_quote;
        document.querySelector('#open-card .card-title').textContent =
          'Resting bid (maker) — ' + (q.n_sigma != null ? q.n_sigma + 'σ' : '');
        document.getElementById('op-question').textContent = q.question || '—';
        document.getElementById('op-ask').textContent = q.bid != null ? q.bid.toFixed(4) : '—';
        document.getElementById('op-yield').textContent = fmtPct(q.net_yield_pct);
        document.getElementById('op-size').textContent = '—';
        document.getElementById('op-expires').textContent = countdown(q.end_ts);
      } else {
        card.style.display = 'none';
      }
    } catch(e) { console.warn('status fetch failed', e); }
  }

  function buildDetailRow(rejected, ncols) {
    const dr = document.createElement('tr');
    dr.className = 'detail-row';
    dr.style.display = 'none';
    const td = document.createElement('td');
    td.colSpan = ncols;
    if (!rejected || !rejected.length) {
      td.innerHTML = '<div class="detail-inner"><span style="color:var(--muted)">No rejected markets this scan.</span></div>';
    } else {
      const rows = rejected.map(r => {
        const rCls = 'r-' + r.reason;
        const q = (r.question || '').replace(/</g,'&lt;').replace(/>/g,'&gt;');
        return '<tr>' +
          '<td class="td-q" style="max-width:320px" title="' + q + '">' + (q || '—') + '</td>' +
          '<td>' + (r.domain || '—') + '</td>' +
          '<td>' + r.minutes_left + 'm</td>' +
          '<td>' + (r.mid != null ? r.mid.toFixed(4) : '—') + '</td>' +
          '<td>' + (r.ask != null ? r.ask.toFixed(4) : '—') + '</td>' +
          '<td>' + (r.depth != null ? '$'+r.depth.toFixed(1) : '—') + '</td>' +
          '<td class="' + rCls + '">' + r.reason + '</td>' +
          '</tr>';
      }).join('');
      td.innerHTML = '<div class="detail-inner"><table>' +
        '<thead><tr><th class="th-l">Market</th><th class="th-l">Domain</th>' +
        '<th>Mins</th><th>Mid</th><th>Ask</th><th>Depth</th><th class="th-l">Reason</th></tr></thead>' +
        '<tbody>' + rows + '</tbody></table></div>';
    }
    dr.appendChild(td);
    return dr;
  }

  async function loadScans() {
    try {
      const r = await fetch('/api/nearct/scans');
      const scans = await r.json();
      const tbody = document.getElementById('scans-tbody');
      if (!scans.length) {
        tbody.innerHTML = '<tr><td colspan="9" class="empty">No scans yet</td></tr>';
        return;
      }
      const frag = document.createDocumentFragment();
      for (const s of scans) {
        const tr = document.createElement('tr');
        tr.className = 'scan-row';
        const actCls = s.action === 'placed' ? 'act-placed' :
                       s.action === 'filtered_out' ? 'act-filtered' :
                       s.action === 'error' ? 'act-error' : 'act-none';
        const actLbl = s.action === 'no_markets' ? 'no markets' : s.action;
        const hasDetail = s.rejected && s.rejected.length > 0;
        const expandIcon = hasDetail ? ' ▶' : '';
        tr.innerHTML =
          '<td class="td-l">' + fmtDt(s.ts) + '</td>' +
          '<td>' + s.markets_in_window + '</td>' +
          '<td>' + s.mid_passed + '</td>' +
          '<td>' + s.book_checked + '</td>' +
          '<td>' + s.candidates + '</td>' +
          '<td>' + (s.best_ask != null ? s.best_ask.toFixed(4) : '—') + '</td>' +
          '<td>' + (s.best_yield_pct != null ? (s.best_yield_pct*100).toFixed(2)+'%' : '—') + '</td>' +
          '<td>' + (s.best_minutes_left != null ? s.best_minutes_left+'m' : '—') + '</td>' +
          '<td class="td-l ' + actCls + '">' + actLbl + expandIcon + '</td>';
        const detailRow = buildDetailRow(s.rejected, 9);
        tr.addEventListener('click', () => {
          const vis = detailRow.style.display !== 'none';
          detailRow.style.display = vis ? 'none' : '';
          const last = tr.querySelector('td:last-child');
          if (last) last.textContent = actLbl + (vis ? expandIcon : ' ▼');
        });
        frag.appendChild(tr);
        frag.appendChild(detailRow);
      }
      tbody.innerHTML = '';
      tbody.appendChild(frag);
    } catch(e) { console.warn('scans fetch failed', e); }
  }

  async function loadOrders() {
    try {
      const r = await fetch('/api/nearct/orders?limit=200');
      _orders = await r.json();
      renderOrders();
    } catch(e) { console.warn('orders fetch failed', e); }
  }

  function renderOrders() {
    const tbody = document.getElementById('orders-tbody');
    if (!_orders.length) {
      tbody.innerHTML = '<tr><td colspan="10" class="empty">No trades yet</td></tr>';
      return;
    }
    const frag = document.createDocumentFragment();
    for (const o of _orders) {
      const tr = document.createElement('tr');
      const sc = o.status === 'won' ? 'won' : o.status === 'lost' ? 'lost' : 'pending';
      const pnlFmt = o.pnl_usd != null
        ? (o.pnl_usd >= 0 ? '+' : '') + '$' + o.pnl_usd.toFixed(4) : '—';
      tr.innerHTML =
        '<td class="td-l">' + fmtDt(o.created_at) + '</td>' +
        '<td class="td-l">' + (o.domain||'—') + '</td>' +
        '<td>' + (o.ask != null ? o.ask.toFixed(4) : '—') + '</td>' +
        '<td>' + fmtPct(o.net_yield_pct) + '</td>' +
        '<td>' + fmt$(o.size_usd) + '</td>' +
        '<td>' + fmt$(o.capital_before) + '</td>' +
        '<td>' + (o.capital_after != null ? fmt$(o.capital_after) : '—') + '</td>' +
        '<td class="' + sc + '">' + o.status + '</td>' +
        '<td class="' + sc + '">' + pnlFmt + '</td>' +
        '<td class="td-q" title="' + (o.question||'').replace(/"/g,'&quot;') + '">' + (o.question||'—') + '</td>';
      frag.appendChild(tr);
    }
    tbody.innerHTML = '';
    tbody.appendChild(frag);
  }

  window.doReset = async function() {
    if (!confirm('Reset session?\\n\\nCancella tutti gli ordini e riporta il capitale a $10.\\nAzione irreversibile.')) return;
    try {
      const r = await fetch('/api/nearct/reset', {method:'POST'});
      const d = await r.json();
      if (d.ok) { loadStatus(); loadScans(); loadOrders(); }
      else alert('Reset failed: ' + (d.error || 'unknown error'));
    } catch(e) { alert('Reset failed: ' + e); }
  };

  // initial load
  loadStatus(); loadScans(); loadOrders();
  // poll
  setInterval(loadStatus, 4000);
  setInterval(loadScans, 5000);
  setInterval(loadOrders, 15000);
})();
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
