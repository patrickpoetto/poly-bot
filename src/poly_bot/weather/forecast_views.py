"""Dashboard helpers — aviation forecasts and shadow trading signals."""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlmodel import Session, col, select

from poly_bot.settings import Settings
from poly_bot.weather.forecast_repo import WeatherForecastRepository
from poly_bot.weather.forecast_schema import (
    WeatherDailyForecast,
    WeatherForecastSource,
    WeatherStationForecastConfig,
    WeatherTafReport,
    WeatherTradingSignal,
)
from poly_bot.weather.market_window import format_days_ahead_label
from poly_bot.weather.schema import WeatherStation


@dataclass(frozen=True, slots=True)
class StationForecastSummary:
    station_id: str
    taf_tmax_c: int | None
    taf_coverage: str
    metar_run_c: int | None
    merged_tmax_c: int | None
    merged_source: str
    edge_hint: str
    trading_enabled: bool
    taf_age: str
    raw_taf_preview: str


def load_forecast_summaries_for_day(
    session: Session,
    *,
    forecast_date: date,
    station_ids: list[str],
) -> dict[str, StationForecastSummary]:
    frepo = WeatherForecastRepository(session)
    out: dict[str, StationForecastSummary] = {}
    for sid in station_ids:
        taf = frepo.get_taf(sid)
        taf_row = session.exec(
            select(WeatherDailyForecast).where(
                WeatherDailyForecast.station_id == sid,
                WeatherDailyForecast.forecast_date == forecast_date,
                WeatherDailyForecast.source_key == "aviation_taf",
            )
        ).first()
        metar_row = session.exec(
            select(WeatherDailyForecast).where(
                WeatherDailyForecast.station_id == sid,
                WeatherDailyForecast.forecast_date == forecast_date,
                WeatherDailyForecast.source_key == "aviation_metar_running",
            )
        ).first()
        merged = frepo.get_merged_forecast(sid, forecast_date)
        cfg = session.get(WeatherStationForecastConfig, sid)
        taf_age = "—"
        if taf and taf.issue_time:
            age_h = (datetime.now(UTC) - taf.issue_time).total_seconds() / 3600
            taf_age = f"{age_h:.1f}h"
        out[sid] = StationForecastSummary(
            station_id=sid,
            taf_tmax_c=taf_row.tmax_c if taf_row else None,
            taf_coverage=taf.coverage if taf else "missing",
            metar_run_c=metar_row.tmax_c if metar_row else None,
            merged_tmax_c=merged.tmax_c if merged else None,
            merged_source=str(merged.meta.get("chosen_source", "")) if merged else "",
            edge_hint="",
            trading_enabled=bool(cfg and cfg.trading_enabled),
            taf_age=taf_age,
            raw_taf_preview=(
                (taf.raw_taf[:80] + "…")
                if taf and len(taf.raw_taf) > 80
                else (taf.raw_taf if taf else "")
            ),
        )
    return out


def load_signal_by_event(
    session: Session,
    event_ids: list[str],
) -> dict[str, WeatherTradingSignal]:
    if not event_ids:
        return {}
    rows = list(
        session.exec(
            select(WeatherTradingSignal).where(
                col(WeatherTradingSignal.weather_event_id).in_(event_ids)
            )
        ).all()
    )
    return {r.weather_event_id: r for r in rows}


def attach_edge_to_summaries(
    summaries: dict[str, StationForecastSummary],
    *,
    event_id_by_station: dict[str, str],
    signals: dict[str, WeatherTradingSignal],
) -> dict[str, StationForecastSummary]:
    updated: dict[str, StationForecastSummary] = {}
    for sid, s in summaries.items():
        eid = event_id_by_station.get(sid)
        sig = signals.get(eid) if eid else None
        edge = sig.edge_hint if sig else ""
        updated[sid] = StationForecastSummary(
            station_id=s.station_id,
            taf_tmax_c=s.taf_tmax_c,
            taf_coverage=s.taf_coverage,
            metar_run_c=s.metar_run_c,
            merged_tmax_c=s.merged_tmax_c,
            merged_source=s.merged_source,
            edge_hint=edge,
            trading_enabled=s.trading_enabled,
            taf_age=s.taf_age,
            raw_taf_preview=s.raw_taf_preview,
        )
    return updated


def load_forecast_context_for_rows(
    session: Session,
    rows: list,
    *,
    forecast_date: date,
) -> dict[str, StationForecastSummary]:
    station_ids = [r.event.station_id for r in rows]
    summaries = load_forecast_summaries_for_day(
        session, forecast_date=forecast_date, station_ids=station_ids
    )
    event_by_station = {r.event.station_id: r.event.id for r in rows}
    signals = load_signal_by_event(session, list(event_by_station.values()))
    return attach_edge_to_summaries(
        summaries,
        event_id_by_station=event_by_station,
        signals=signals,
    )


def _source_cell(row: WeatherDailyForecast | None) -> str:
    if row is None:
        return "—"
    if row.tmax_c is not None:
        return str(row.tmax_c)
    if row.coverage == "no_temp":
        return "no TX"
    if row.coverage == "temp_tx":
        return "TX alt"
    if row.coverage == "none":
        return "n/a"
    return "—"


def load_board_data(
    session: Session,
    *,
    forecast_date: date,
    rows: list,
) -> tuple[
    list[str], dict[str, dict[str, WeatherDailyForecast | None]], dict[str, WeatherTradingSignal]
]:
    station_ids = [r.event.station_id for r in rows]
    if not station_ids:
        return [], {}, {}
    fc_rows = list(
        session.exec(
            select(WeatherDailyForecast).where(
                col(WeatherDailyForecast.station_id).in_(station_ids),
                WeatherDailyForecast.forecast_date == forecast_date,
            )
        ).all()
    )
    keys = sorted({r.source_key for r in fc_rows})
    preferred = [
        "aviation_taf",
        "aviation_metar_running",
        "open_meteo_blend",
        "open_meteo_ecmwf",
        "open_meteo_gfs",
        "open_meteo_icon",
        "merged",
    ]
    source_keys = list(preferred) + [k for k in keys if k not in preferred]
    matrix: dict[str, dict[str, WeatherDailyForecast | None]] = {
        sid: dict.fromkeys(source_keys) for sid in station_ids
    }
    for r in fc_rows:
        if r.station_id in matrix and r.source_key in matrix[r.station_id]:
            matrix[r.station_id][r.source_key] = r
    signals = load_signal_by_event(session, [r.event.id for r in rows])
    return source_keys, matrix, signals


def render_weather_board_page(
    *,
    settings: Settings,
    forecast_date: date,
    rows: list,
    summaries: list,
    source_keys: list[str],
    matrix: dict[str, dict[str, WeatherDailyForecast | None]],
    signals: dict[str, WeatherTradingSignal],
) -> str:
    """One-row-per-city board with all forecast sources."""
    from poly_bot.weather.dashboard_views import _nav_tabs, _weather_css

    if not rows:
        return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><title>weather board</title>{_weather_css()}</head>
<body><header><h1>Weather board</h1>{_nav_tabs("board")}</header><main><p class="muted">No rows.</p></main></body></html>"""

    day_opts = (
        "".join(
            f'<option value="{s.forecast_date.isoformat()}" '
            f"{'selected' if s.forecast_date == forecast_date else ''}>"
            f"{s.forecast_date.isoformat()} ({format_days_ahead_label(s.forecast_date)})"
            f"</option>"
            for s in summaries
        )
        or f'<option value="{forecast_date.isoformat()}" selected>{forecast_date}</option>'
    )

    head_cols = "".join(f"<th class='num'>{html.escape(k)}</th>" for k in source_keys)
    body = ""
    for r in rows:
        city = html.escape(r.city.display_name if r.city else r.event.city_key)
        station = html.escape(r.station.station_name if r.station else "—")
        icao = html.escape((r.station.icao if r.station else "—") or "—")
        values = matrix.get(r.event.station_id, {})
        source_cells = "".join(
            f"<td class='num'>{_source_cell(values.get(k))}</td>" for k in source_keys
        )
        sig = signals.get(r.event.id)
        edge = html.escape(sig.edge_hint if sig else "—")
        body += (
            "<tr>"
            f"<td><strong>{city}</strong><br/><span class='tag'>{html.escape(r.event.city_key)}</span></td>"
            f"<td>{station}</td>"
            f"<td><span class='tag'>{icao}</span></td>"
            f"{source_cells}"
            f"<td>{edge}</td>"
            f"<td class='num'>{len(r.bins)}</td>"
            f"<td class='num'>{format_days_ahead_label(r.event.forecast_date)}</td>"
            "</tr>"
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>poly_bot — weather board</title>
{_weather_css()}
</head>
<body>
<header>
  <h1>poly_bot — weather board</h1>
  <div class="sub">Tmax (°C) per il giorno mercato selezionato — giorno locale stazione 00:00–24:00 · {forecast_date.isoformat()}</div>
  {_nav_tabs("board")}
  <form class="filters" method="get" action="/weather/board">
    <select name="day">{day_opts}</select>
    <button type="submit">Apply</button>
  </form>
</header>
<main>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>City</th><th>Station</th><th>ICAO</th>
        {head_cols}
        <th>Edge</th><th class="num">Bins</th><th class="num">+days</th>
      </tr></thead>
      <tbody>{body}</tbody>
    </table>
  </div>
  <footer>
    Giorno {forecast_date} ·
    <a class="link-muted" href="/weather?day={forecast_date.isoformat()}">Table view</a> ·
    <a class="link-muted" href="/api/weather/forecasts?day={forecast_date.isoformat()}">Forecasts JSON</a>
  </footer>
</main>
</body>
</html>"""


def render_weather_aviation_page(
    *,
    settings: Settings,
    stations: list[WeatherStation],
    configs: list[WeatherStationForecastConfig],
    sources: list[WeatherForecastSource],
    taf_rows: list[WeatherTafReport],
) -> str:
    from poly_bot.weather.dashboard_views import _nav_tabs, _weather_css

    cfg_by_id = {c.station_id: c for c in configs}
    taf_by_id = {t.station_id: t for t in taf_rows}
    src_rows = "".join(
        f"<tr><td><code>{html.escape(s.source_key)}</code></td>"
        f"<td>{html.escape(s.display_name)}</td>"
        f"<td>{html.escape(s.provider_kind)}</td>"
        f"<td class='num'>{s.horizon_hours}h</td>"
        f"<td>{'yes' if s.enabled else 'no'}</td></tr>"
        for s in sources
    )
    st_rows = ""
    for st in sorted(stations, key=lambda x: x.city_display or x.id):
        cfg = cfg_by_id.get(st.id)
        taf = taf_by_id.get(st.id)
        primary = cfg.primary_source_key if cfg else "—"
        fallbacks = ", ".join(cfg.fallback_source_keys) if cfg else ""
        trade = "yes" if cfg and cfg.trading_enabled else "no"
        cov = taf.coverage if taf else "missing"
        preview = html.escape(
            (taf.raw_taf[:100] + "…")
            if taf and len(taf.raw_taf) > 100
            else (taf.raw_taf if taf else "")
        )
        st_rows += (
            f"<tr><td><code>{html.escape(st.icao or st.id)}</code></td>"
            f"<td>{html.escape(st.city_display or st.id)}</td>"
            f"<td><code>{html.escape(primary)}</code></td>"
            f"<td class='muted'>{html.escape(fallbacks)}</td>"
            f"<td>{cov}</td><td>{trade}</td>"
            f"<td class='muted' style='max-width:320px'>{preview}</td></tr>"
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>poly_bot — aviation forecasts</title>
{_weather_css()}
</head>
<body>
<header>
  <h1>Aviation forecast sources</h1>
  <div class="sub">TAF/METAR ingest for all ICAO stations · shadow trading prep · interval {settings.weather_aviation_interval_seconds}s</div>
  {_nav_tabs("weather")}
  <p class="sub"><a class="link-muted" href="/weather">← Weather table</a> · run <code>poly-bot weather aviation-sync</code></p>
</header>
<main>
  <h2>Registered sources</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>Key</th><th>Name</th><th>Kind</th><th>Horizon</th><th>Enabled</th></tr></thead>
    <tbody>{src_rows}</tbody>
  </table></div>
  <h2>Stations ({len(stations)})</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>ICAO</th><th>City</th><th>Primary</th><th>Fallbacks</th><th>TAF</th><th>Trade</th><th>Raw TAF</th></tr></thead>
    <tbody>{st_rows}</tbody>
  </table></div>
  <footer class="sub">Per-station overrides: <code>weather_station_forecast_config.extra_sources</code> (future providers).</footer>
</main>
</body>
</html>"""
