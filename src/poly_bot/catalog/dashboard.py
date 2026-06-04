"""FastAPI dashboard for expiring markets and events."""

from __future__ import annotations

import asyncio
import html
import json
from datetime import UTC, datetime
from urllib.parse import urlencode

from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
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


def create_app(settings: Settings) -> FastAPI:
    init_db(get_engine(settings))
    app = FastAPI(title="poly_bot catalog", version="0.2.0")

    static_dir = Path(__file__).resolve().parent / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", response_class=HTMLResponse)
    def index(
        hours: float = Query(default=6.0, ge=1, le=168),
        domain: str = Query(default=""),
        q: str = Query(default=""),
        fees_only: bool = Query(default=False),
    ) -> str:
        all_markets = _load_markets(settings, hours=hours)
        markets = _filter_markets(all_markets, domain=domain, q=q, fees_only=fees_only)
        events = group_expiring_by_event(markets)
        return _render_page(
            settings=settings,
            hours=hours,
            all_markets=all_markets,
            markets=markets[:500],
            events=events[:80],
            domain=domain,
            q=q,
            fees_only=fees_only,
            tab="markets",
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
                selected = summaries[0].forecast_date
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
                selected = summaries[0].forecast_date
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
                selected = summaries[0].forecast_date
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
            ctx = load_forecast_context_for_rows(
                session, rows, forecast_date=forecast_date
            )
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
                        await refresh_event_quotes_from_clob(
                            session, settings, event_id=event_id
                        )
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
            conn.execute(text("SELECT COUNT(*) FROM markets WHERE active = TRUE")).scalar()
            or 0
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
        expiring = count_expiring_markets(
            session, hours=hours, sports_in_play_hours=in_play
        )
        markets = list_expiring_markets(
            session, hours=hours, sports_in_play_hours=in_play
        )
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
        visible = _filter_markets(markets, domain=domain, q=q, fees_only=fees_only)[
            :quote_limit
        ]
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
            if ql in m.question.lower() or ql in m.slug.lower() or ql in (m.event_title or "").lower()
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
    <a href="/?{query}" class="{'active' if tab == 'markets' else ''}">Markets</a>
    <a href="/events?hours={hours}" class="{'active' if tab == 'events' else ''}">Events & fees</a>
    <a href="/weather">Weather</a>
    <a href="/taxonomy">Taxonomy</a>
  </nav>
  <form class="filters" method="get" action="/">
    <input type="hidden" name="hours" value="{hours}"/>
    <select name="domain">
      <option value="">All domains</option>
      {''.join(f'<option value="{html.escape(d)}" {"selected" if d == domain else ""}>{html.escape(d)}</option>' for d in domains)}
    </select>
    <input type="search" name="q" value="{html.escape(q)}" placeholder="Search question…" size="28"/>
    <label><input type="checkbox" name="fees_only" value="true" {'checked' if fees_only else ''}/> Fees only</label>
    <button type="submit">Apply</button>
  </form>
</header>
<main>
  <div id="panel-markets" class="{'hidden' if tab != 'markets' else ''}">
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
  <div id="panel-events" class="{'hidden' if tab != 'events' else ''}">
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
    if m.is_live:
        left_cls = "soon"
    elif m.hours_left < 3:
        left_cls = "soon"
    mid = m.yes_mid if m.yes_mid is not None else m.no_mid
    fee_lbl = market_fee_label(m)
    fee_cls = "fee-yes" if m.fees_enabled or m.event_fees_enabled else "fee-no"
    spr = m.yes_spread if m.yes_spread is not None else m.no_spread
    kickoff = m.game_start_ts or m.end_ts
    if m.is_live:
        status = f'<span class="tag" style="color:var(--green)">LIVE</span> {abs(m.hours_left):.1f}h'
    elif m.hours_left < 1:
        status = f'<span class="urgent">{m.hours_left:.1f}h</span>'
    else:
        status = f"{m.hours_left:.1f}h"
    cid = html.escape(m.condition_id, quote=True)
    return (
        f'<tr data-cid="{cid}">'
        f'<td>{html.escape(kickoff.strftime("%m-%d %H:%M"))}</td>'
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
        f'<td>{html.escape(rate)}</td>'
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
        "hours_left": m.hours_left,
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


def run_dashboard(settings: Settings) -> None:
    try:
        import uvicorn
    except ImportError as exc:
        raise SystemExit("Install dashboard extras: pip install -e '.[dashboard]'") from exc

    app = create_app(settings)
    uvicorn.run(
        app,
        host=settings.catalog_dashboard_host,
        port=settings.catalog_dashboard_port,
        log_level="info",
    )
