"""HTML views for the catalog dashboard — daily temperature markets."""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlencode

from sqlalchemy import func
from sqlmodel import Session, col, select

from poly_bot.persistence.schema import TokenQuote
from poly_bot.settings import Settings
from poly_bot.weather.market_window import (
    days_ahead_of_today,
    format_days_ahead_label,
    weather_horizon_bounds,
    weather_reference_date,
)
from poly_bot.weather.repo import WeatherRepository
from poly_bot.weather.schema import WeatherBin, WeatherCity, WeatherDailyEvent, WeatherStation


@dataclass(frozen=True, slots=True)
class WeatherDaySummary:
    forecast_date: date
    city_count: int
    bin_count: int
    days_ahead: int


@dataclass(frozen=True, slots=True)
class WeatherCityRow:
    event: WeatherDailyEvent
    city: WeatherCity | None
    station: WeatherStation | None
    bins: list[WeatherBin]


@dataclass(frozen=True, slots=True)
class BinTick:
    condition_id: str
    yes_mid: float | None
    no_mid: float | None
    spread: float | None
    volume_24h: float | None
    open_interest: float | None


def weather_horizon(settings: Settings) -> tuple[date, date]:
    return weather_horizon_bounds(horizon_days=settings.weather_forecast_horizon_days)


def load_day_summaries(session: Session, settings: Settings) -> list[WeatherDaySummary]:
    min_d, max_d = weather_horizon(settings)
    rows = session.exec(
        select(
            WeatherDailyEvent.forecast_date,
            func.count(WeatherDailyEvent.id),
            func.sum(WeatherDailyEvent.bin_count),
        )
        .where(WeatherDailyEvent.forecast_date >= min_d)
        .where(WeatherDailyEvent.forecast_date <= max_d)
        .group_by(WeatherDailyEvent.forecast_date)
    ).all()
    by_date = {r[0]: (int(r[1]), int(r[2] or 0)) for r in rows}
    return [
        WeatherDaySummary(
            forecast_date=day,
            city_count=by_date.get(day, (0, 0))[0],
            bin_count=by_date.get(day, (0, 0))[1],
            days_ahead=days_ahead_of_today(day),
        )
        for day in _horizon_dates(min_d, max_d)
    ]


def _horizon_dates(min_d: date, max_d: date) -> list[date]:
    out: list[date] = []
    d = min_d
    while d <= max_d:
        out.append(d)
        d += timedelta(days=1)
    return out


def default_forecast_day(summaries: list[WeatherDaySummary]) -> date | None:
    """Prefer today UTC, else first day with synced cities."""
    if not summaries:
        return None
    ref = weather_reference_date()
    for s in summaries:
        if s.forecast_date == ref and s.city_count > 0:
            return s.forecast_date
    for s in summaries:
        if s.city_count > 0:
            return s.forecast_date
    return summaries[0].forecast_date


def days_ahead_label_for(forecast_date: date) -> str:
    return format_days_ahead_label(forecast_date)


def load_city_rows(
    session: Session,
    *,
    forecast_date: date,
    city_q: str = "",
) -> list[WeatherCityRow]:
    repo = WeatherRepository(session)
    events = repo.list_daily_events(
        min_date=forecast_date,
        max_date=forecast_date,
        city_key=city_q.strip().lower() if city_q else None,
    )
    out: list[WeatherCityRow] = []
    for ev in events:
        if city_q and city_q.lower() not in ev.city_key:
            continue
        city = session.get(WeatherCity, ev.city_key)
        station = repo.get_station(ev.station_id)
        bins = repo.bins_for_event(ev.id)
        out.append(WeatherCityRow(event=ev, city=city, station=station, bins=bins))
    return out


def load_globe_points(
    session: Session,
    *,
    forecast_date: date,
) -> list[dict[str, object]]:
    """Markers for 3D globe — one per city on the selected forecast day."""
    rows = load_city_rows(session, forecast_date=forecast_date)
    points: list[dict[str, object]] = []
    for row in rows:
        st = row.station
        ev = row.event
        if st is None or st.latitude is None or st.longitude is None:
            continue
        display = row.city.display_name if row.city else ev.city_key
        ticks = load_bin_ticks(session, row.bins)
        from poly_bot.weather.forecast_repo import WeatherForecastRepository

        merged = WeatherForecastRepository(session).get_merged_forecast(
            ev.station_id, forecast_date
        )
        if merged and merged.tmax_c is not None:
            src = merged.meta.get("chosen_source", "model")
            lead_label = f"{merged.tmax_c}°C ({src})"
        else:
            lead_label = _lead_temperature_label(row.bins, ticks)
        detail = urlencode({"city": ev.city_key, "day": forecast_date.isoformat()})
        points.append(
            {
                "lat": st.latitude,
                "lng": st.longitude,
                "city_key": ev.city_key,
                "display_name": display,
                "station_name": st.station_name or st.icao or "",
                "icao": st.icao or "",
                "timezone": st.timezone,
                "unit": st.unit,
                "forecast_date": forecast_date.isoformat(),
                "days_ahead": days_ahead_of_today(forecast_date),
                "bin_count": len(row.bins),
                "lead_temp": lead_label,
                "title": ev.title,
                "detail_url": f"/weather/event?{detail}",
                "wu_url": st.wunderground_url,
            }
        )
    return points


def _lead_temperature_label(
    bins: list[WeatherBin],
    ticks: dict[str, BinTick] | None = None,
) -> str:
    """Favorite °C bin by highest Yes mid when quotes exist, else first exact bin."""
    ticks = ticks or {}
    best_mid = -1.0
    best_label = ""
    for b in bins:
        if b.bin_kind != "exact" or b.temp_c is None:
            continue
        mid = ticks.get(b.condition_id)
        yes = mid.yes_mid if mid else None
        if yes is not None and yes > best_mid:
            best_mid = yes
            best_label = f"{b.temp_c}°C ({yes * 100:.0f}¢)"
    if best_label:
        return best_label
    exact = [b for b in bins if b.bin_kind == "exact" and b.temp_c is not None]
    if exact:
        mid = len(exact) // 2
        return f"{exact[mid].temp_c}°C"
    for b in bins:
        if b.bin_kind == "at_or_below" and b.temp_c_upper is not None:
            return f"≤{b.temp_c_upper}°C"
        if b.bin_kind == "at_or_above" and b.temp_c_lower is not None:
            return f"≥{b.temp_c_lower}°C"
    return "—"


def load_city_row_by_keys(
    session: Session,
    *,
    forecast_date: date,
    city_key: str,
) -> WeatherCityRow | None:
    rows = load_city_rows(session, forecast_date=forecast_date, city_q=city_key)
    return rows[0] if rows else None


def load_bin_ticks(session: Session, bins: list[WeatherBin]) -> dict[str, BinTick]:
    """Latest CLOB mids + enrichment volume/OI per weather bin."""
    from poly_bot.persistence.schema import MarketEnrichment

    if not bins:
        return {}
    yes_ids = [b.yes_token_id for b in bins if b.yes_token_id]
    no_ids = [b.no_token_id for b in bins if b.no_token_id]
    token_ids = [*yes_ids, *no_ids]
    quotes_by_token: dict[str, TokenQuote] = {}
    if token_ids:
        for q in session.exec(select(TokenQuote).where(col(TokenQuote.token_id).in_(token_ids))):
            quotes_by_token[q.token_id] = q

    cids = [b.condition_id for b in bins]
    enrich_by_cid: dict[str, MarketEnrichment] = {}
    for en in session.exec(
        select(MarketEnrichment).where(col(MarketEnrichment.condition_id).in_(cids))
    ):
        enrich_by_cid[en.condition_id] = en

    out: dict[str, BinTick] = {}
    for b in bins:
        qy = quotes_by_token.get(b.yes_token_id or "")
        qn = quotes_by_token.get(b.no_token_id or "")
        en = enrich_by_cid.get(b.condition_id)
        spread = qy.spread if qy and qy.spread is not None else None
        out[b.condition_id] = BinTick(
            condition_id=b.condition_id,
            yes_mid=qy.mid if qy else None,
            no_mid=qn.mid if qn else None,
            spread=spread,
            volume_24h=en.volume_24h if en else None,
            open_interest=en.open_interest if en else None,
        )
    return out


def weather_event_live_snapshot(
    session: Session,
    *,
    event_id: str,
) -> dict[str, object]:
    """Payload for SSE — quote ticks for all bins in one daily event."""
    from datetime import datetime

    from poly_bot.weather.repo import WeatherRepository

    ev = session.get(WeatherDailyEvent, event_id)
    if ev is None:
        return {"error": "not found"}
    bins = WeatherRepository(session).bins_for_event(event_id)
    ticks = load_bin_ticks(session, bins)
    quote_row = session.exec(
        select(func.max(TokenQuote.updated_at))
    ).one()
    quote_last = quote_row[0] if quote_row else None
    now = datetime.now(UTC)
    quote_age_s: int | None = None
    if quote_last is not None:
        if quote_last.tzinfo is None:
            quote_last = quote_last.replace(tzinfo=UTC)
        quote_age_s = int((now - quote_last).total_seconds())

    quotes: dict[str, dict[str, float | None]] = {}
    for cid, t in ticks.items():
        quotes[cid] = {
            "y": t.yes_mid,
            "n": t.no_mid,
            "s": t.spread,
            "vol": t.volume_24h,
            "oi": t.open_interest,
        }
    return {
        "event_id": event_id,
        "quote_age_s": quote_age_s,
        "quotes": quotes,
    }


def yes_mid_for_bins(session: Session, bins: list[WeatherBin]) -> dict[str, float | None]:
    return {cid: t.yes_mid for cid, t in load_bin_ticks(session, bins).items()}


def render_weather_page(
    *,
    settings: Settings,
    summaries: list[WeatherDaySummary],
    rows: list[WeatherCityRow],
    selected_day: date,
    city_q: str,
    empty_catalog: bool,
    initial_cards_json: str,
    poll_seconds: int,
) -> str:
    min_d, max_d = weather_horizon(settings)
    day_opts = "".join(
        f'<option value="{s.forecast_date.isoformat()}" '
        f'{"selected" if s.forecast_date == selected_day else ""}>'
        f'{s.forecast_date.isoformat()} ({days_ahead_label_for(s.forecast_date)}, {s.city_count} cities)'
        f"</option>"
        for s in summaries
    ) or f'<option value="{selected_day.isoformat()}" selected>{selected_day}</option>'

    day_pills = "".join(
        f'<a class="day-pill{" active" if s.forecast_date == selected_day else ""}{" empty" if s.city_count == 0 else ""}" '
        f'href="/weather?{urlencode({"day": s.forecast_date.isoformat(), "city": city_q})}" '
        f'title="{s.city_count} città">'
        f"{s.forecast_date.strftime('%d %b')} ({days_ahead_label_for(s.forecast_date)}"
        f'{"" if s.city_count else ", vuoto"})'
        f"</a>"
        for s in summaries
    ) or f'<span class="day-pill active">{selected_day.isoformat()}</span>'

    empty_msg = ""
    if empty_catalog:
        empty_msg = (
            '<p class="empty-catalog">Nessun catalogo meteo — '
            "<code>poly-bot weather sync</code> dopo discovery.</p>"
        )

    qs = urlencode({"day": selected_day.isoformat(), "city": city_q})
    return f"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>poly_bot — weather</title>
{_weather_css()}
{_weather_cards_css()}
</head>
<body>
<header>
  <h1>Weather scan</h1>
  <div class="sub">Stazione di risoluzione · oggi UTC {weather_reference_date().strftime('%d/%m/%Y')} · aggiornamento ogni {poll_seconds // 60} min</div>
  <div class="day-pills">{day_pills}</div>
  <div class="health-bar" id="health-bar">
    <span class="health-item" id="health-wu"><span class="dot"></span> WU station</span>
    <span class="health-item" id="health-quotes"><span class="dot"></span> Prezzi</span>
    <span class="health-item" id="health-poll"><span class="dot ok"></span> <span id="poll-label">—</span></span>
  </div>
  {_nav_tabs("weather")}
  <form class="filters" method="get" action="/weather">
    <select name="day">{day_opts}</select>
    <input type="search" name="city" value="{html.escape(city_q)}" placeholder="Filtra città…" size="18"/>
    <button type="submit">Applica</button>
  </form>
  <div class="stats" id="header-stats">
    <div class="stat">Giorno <b id="stat-day">{selected_day.isoformat()}</b></div>
    <div class="stat">Mercati <b id="stat-cities">{len(rows)}</b></div>
  </div>
</header>
<main>
  {empty_msg}
  <div class="card-grid" id="card-grid"></div>
  <p class="sub empty-day-hint" id="empty-day-hint" style="display:none">
    Nessun mercato per questo giorno — aggiorna catalogo:
    <code>poly-bot weather markets-sync</code> poi <code>poly-bot weather sync</code>
  </p>
  <footer>Horizon {min_d} … {max_d} ·
    <a class="link-muted" href="/weather/board?day={selected_day.isoformat()}">Board</a> ·
    <a class="link-muted" href="/weather/globe?day={selected_day.isoformat()}">Globe</a> ·
    <a class="link-muted" href="/api/weather/cards?{qs}">Cards JSON</a>
  </footer>
</main>
<script>
const INITIAL = {initial_cards_json};
const POLL_MS = {poll_seconds * 1000};
const API_URL = '/api/weather/cards?{html.escape(qs, quote=True)}';

function esc(s) {{
  return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/"/g,'&quot;');
}}

function fmtAge(sec) {{
  if (sec === null || sec === undefined) return '—';
  if (sec < 60) return Math.round(sec) + 's fa';
  if (sec < 3600) return Math.round(sec / 60) + ' min fa';
  return (sec / 3600).toFixed(1) + ' h fa';
}}

function setHealth(id, cls, label) {{
  const el = document.getElementById(id);
  if (!el) return;
  el.innerHTML = '<span class="dot ' + (cls || '') + '"></span> ' + label;
}}

function applyHealth(h) {{
  const wu = h.wu || {{}};
  const q = h.quotes || {{}};
  let wuCls = 'warn';
  if (wu.error > 0 || wu.missing > 0) wuCls = 'err';
  else if (wu.ok > 0 && (!wu.stale_s || wu.stale_s < 600)) wuCls = 'ok';
  setHealth('health-wu', wuCls,
    `WU: ${{wu.ok || 0}} ok · ${{wu.error || 0}} err · ${{fmtAge(wu.stale_s)}}`);
  let qCls = 'warn';
  if (!q.last_at) qCls = 'err';
  else if (q.stale_s !== null && q.stale_s < 600) qCls = 'ok';
  else if (q.stale_s !== null && q.stale_s < 1800) qCls = 'warn';
  else qCls = 'err';
  setHealth('health-quotes', qCls, `Prezzi: ${{fmtAge(q.stale_s)}}`);
}}

function priceBarsHtml(bars, favorite) {{
  if (!bars || !bars.length) return '<div class="muted">Nessun prezzo</div>';
  const maxY = Math.max(...bars.map(b => (b.yes_mid ?? 0)), 0.05);
  return '<div class="spark-bars">' + bars.map(b => {{
    const h = Math.round(((b.yes_mid ?? 0) / maxY) * 100);
    const fav = favorite && favorite.temp_c === b.temp_c ? ' fav' : '';
    const y = b.yes_mid != null ? Math.round(b.yes_mid * 100) + '¢' : '—';
    return `<div class="spark-col${{fav}}" title="${{esc(b.label)}} Yes ${{y}}">
      <div class="spark-fill" style="height:${{h}}%"></div>
      <div class="spark-lbl">${{esc(b.label)}}</div></div>`;
  }}).join('') + '</div>';
}}

function cardHtml(c) {{
  const st = c.station || {{}};
  const wu = c.wu || {{}};
  const coord = (st.lat != null && st.lng != null)
    ? `<a class="coord-link" href="${{esc(st.wunderground_url)}}" target="_blank" rel="noopener">${{st.lat.toFixed(3)}}, ${{st.lng.toFixed(3)}}</a>`
    : (st.icao ? `<a class="coord-link" href="${{esc(st.wunderground_url)}}" target="_blank" rel="noopener">${{esc(st.icao)}}</a>` : '—');
  const wuCls = wu.status === 'ok' ? 'ok' : (wu.status === 'error' ? 'err' : 'warn');
  const hours = (wu.tmax_hours_local || []).filter(Boolean).join(', ') || '—';
  const err = wu.error ? `<div class="card-err">${{esc(wu.error)}}</div>` : '';
  const model = c.model_tmax_c != null ? `<span class="model-tag">${{c.model_tmax_c}}°C model</span>` : '';
  return `<article class="market-card" data-event="${{esc(c.event_id)}}">
    <div class="card-top">
      <h2><a href="${{esc(c.detail_url)}}">${{c.flag ? c.flag + ' ' : ''}}${{esc(c.display_name)}}</a></h2>
      <span class="days-tag">${{esc(c.days_ahead_label || c.days_ahead)}}</span>
    </div>
    <div class="card-station muted">
      ${{esc(st.name || st.icao || st.id)}} ·
      <a href="${{esc(st.wunderground_url)}}" target="_blank" rel="noopener">${{esc(st.icao || 'WU')}}</a>
      · ${{coord}}
    </div>
    <div class="card-temps">
      <div class="temp-block"><span class="lbl">Adesso</span><span class="val">${{esc(wu.current || '—')}}</span></div>
      <div class="temp-block highlight"><span class="lbl">Max giorno</span>
        <span class="val">${{esc(wu.tmax || '—')}}</span>
        <span class="sub-val">ore ${{esc(hours)}} · ${{esc(st.timezone)}}</span>
      </div>
    </div>
    <div class="wu-status ${{wuCls}}">WU <span class="dot"></span> ${{esc(wu.status)}} · ${{wu.fetched_at ? new Date(wu.fetched_at).toLocaleString() : '—'}} ${{model}}</div>
    ${{err}}
    <details class="card-prices">
      <summary>Distribuzione prezzi (${{c.bin_count}} bin)</summary>
      ${{priceBarsHtml(c.price_bars, c.favorite)}}
    </details>
  </article>`;
}}

function renderPayload(data) {{
  const grid = document.getElementById('card-grid');
  const hint = document.getElementById('empty-day-hint');
  if (!data || !data.cards) {{
    grid.innerHTML = '';
    if (hint) hint.style.display = 'block';
    return;
  }}
  if (hint) hint.style.display = data.cards.length ? 'none' : 'block';
  if (!data.cards.length) {{
    grid.innerHTML = '';
    document.getElementById('stat-cities').textContent = '0';
    applyHealth(data.health || {{}});
    return;
  }}
  grid.innerHTML = data.cards.map(cardHtml).join('');
  document.getElementById('stat-cities').textContent = data.cards.length;
  applyHealth(data.health || {{}});
}}

async function refreshCards() {{
  const pollLbl = document.getElementById('poll-label');
  pollLbl.textContent = 'aggiornamento…';
  try {{
    const r = await fetch(API_URL + '&refresh_quotes=1');
    if (!r.ok) throw new Error(r.statusText);
    const data = await r.json();
    renderPayload(data);
    pollLbl.textContent = 'live · ' + new Date().toLocaleTimeString();
    document.querySelector('#health-poll .dot').classList.add('ok');
  }} catch (e) {{
    pollLbl.textContent = 'errore: ' + e.message;
    document.querySelector('#health-poll .dot').classList.remove('ok');
    document.querySelector('#health-poll .dot').classList.add('err');
  }}
}}

renderPayload(INITIAL);
setInterval(refreshCards, POLL_MS);
</script>
</body>
</html>"""


def render_weather_detail_page(
    *,
    settings: Settings,
    row: WeatherCityRow,
    ticks: dict[str, BinTick],
    signal: object | None = None,
    taf_raw: str = "",
) -> str:
    from poly_bot.weather.forecast_schema import WeatherTradingSignal
    ev = row.event
    st = row.station
    station_line = "—"
    if st:
        coord = ""
        if st.latitude is not None and st.longitude is not None:
            coord = f" ({st.latitude:.4f}, {st.longitude:.4f})"
        station_line = (
            f"{html.escape(st.station_name or st.icao or st.id)}{coord} · "
            f'<a class="link-muted" href="{html.escape(st.wunderground_url)}" '
            f'target="_blank" rel="noopener">Wunderground</a>'
        )

    bin_rows = "".join(
        _bin_row(b, ticks.get(b.condition_id)) for b in row.bins
    )
    rules = html.escape(ev.resolution_rules_text[:1200])
    if len(ev.resolution_rules_text) > 1200:
        rules += "…"

    back_qs = urlencode({"day": ev.forecast_date.isoformat()})
    av_block = ""
    if signal and isinstance(signal, WeatherTradingSignal):
        av_block = (
            f'<div class="stats" style="margin-top:0.75rem">'
            f'<div class="stat">Model <b>{html.escape(signal.model_bin_label or "—")}</b></div>'
            f'<div class="stat">Market fav <b>{signal.market_favorite_c or "—"}°C</b></div>'
            f'<div class="stat">Edge <b>{html.escape(signal.edge_hint)}</b></div>'
            f'<div class="stat">Mode <b>{html.escape(signal.trading_mode)}</b></div>'
            f"</div>"
        )
    if taf_raw:
        av_block += f'<p class="rules"><b>TAF</b> {html.escape(taf_raw[:500])}{"…" if len(taf_raw) > 500 else ""}</p>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>{html.escape(ev.city_key)} {ev.forecast_date}</title>
{_weather_css()}
</head>
<body>
<header>
  <h1>{html.escape(row.city.display_name if row.city else ev.city_key)}</h1>
  <div class="sub">{html.escape(ev.title)} · resolves {ev.end_ts.strftime("%Y-%m-%d %H:%M") if ev.end_ts else "—"} UTC</div>
  <div class="sub live-line">
    <span class="live-dot" id="live-dot"></span>
    <span id="live-status">connecting…</span> · quotes <span id="quote-age">—</span>
  </div>
  {_nav_tabs("weather")}
  <p class="sub">Station: {station_line}</p>
  {av_block}
  <p class="rules">{rules}</p>
  <p><a class="back-link" href="/weather?{back_qs}">← All cities</a></p>
</header>
<main>
  <div class="table-wrap">
    <table id="bins-table">
      <thead><tr>
        <th>#</th><th>Bin</th><th>Kind</th><th class="num">°C</th>
        <th class="num">Yes</th><th class="num">No</th><th class="num">Spr</th>
        <th class="num">Vol 24h</th><th class="num">OI</th>
        <th>Question</th>
      </tr></thead>
      <tbody>{bin_rows}</tbody>
    </table>
  </div>
  <footer class="sub">Prezzi: aggiornamento REST ogni 5s da questa pagina. Vol/OI da <code>catalog-watch</code>. ·
    <a class="link-muted" href="/api/weather/event/{html.escape(ev.id, quote=True)}">JSON</a></footer>
</main>
{_weather_live_script(ev.id)}
</body>
</html>"""


def weather_event_json(row: WeatherCityRow, ticks: dict[str, BinTick]) -> dict[str, object]:
    ev = row.event
    st = row.station
    return {
        "event_id": ev.id,
        "event_slug": ev.event_slug,
        "city_key": ev.city_key,
        "forecast_date": ev.forecast_date.isoformat(),
        "days_ahead": days_ahead_of_today(ev.forecast_date),
        "title": ev.title,
        "resolution_source_url": ev.resolution_source_url,
        "end_ts": ev.end_ts.isoformat() if ev.end_ts else None,
        "station": (
            {
                "id": st.id,
                "name": st.station_name,
                "icao": st.icao,
                "latitude": st.latitude,
                "longitude": st.longitude,
                "timezone": st.timezone,
                "wunderground_url": st.wunderground_url,
            }
            if st
            else None
        ),
        "bins": [
            {
                "condition_id": b.condition_id,
                "bin_label": b.bin_label,
                "bin_kind": b.bin_kind,
                "bin_index": b.bin_index,
                "temp_c": b.temp_c,
                "temp_c_upper": b.temp_c_upper,
                "temp_c_lower": b.temp_c_lower,
                "question": b.question,
                "yes_mid": ticks.get(b.condition_id).yes_mid if b.condition_id in ticks else None,
                "no_mid": ticks.get(b.condition_id).no_mid if b.condition_id in ticks else None,
                "spread": ticks.get(b.condition_id).spread if b.condition_id in ticks else None,
                "volume_24h": ticks.get(b.condition_id).volume_24h
                if b.condition_id in ticks
                else None,
                "open_interest": ticks.get(b.condition_id).open_interest
                if b.condition_id in ticks
                else None,
            }
            for b in row.bins
        ],
    }


def _format_taf_cell(fs: object | None) -> str:
    from poly_bot.weather.forecast_views import StationForecastSummary

    if not isinstance(fs, StationForecastSummary):
        return '<span class="muted" title="Run aviation-sync">?</span>'
    if fs.taf_tmax_c is not None:
        return str(fs.taf_tmax_c)
    if fs.taf_coverage == "no_temp":
        tip = html.escape(fs.raw_taf_preview or "TAF received, no TX/TN max in text")
        return f'<span class="tag" title="{tip}">no TX</span>'
    if fs.taf_coverage == "temp_tx":
        tip = html.escape(
            fs.raw_taf_preview or "TAF has TX/TN for another day — not this forecast date"
        )
        return f'<span class="tag" title="{tip}">TX alt</span>'
    if fs.taf_coverage == "missing":
        return '<span class="muted">—</span>'
    return '<span class="muted">—</span>'


def _format_metar_cell(fs: object | None) -> str:
    from poly_bot.weather.forecast_views import StationForecastSummary

    if not isinstance(fs, StationForecastSummary):
        return '<span class="muted">?</span>'
    if fs.metar_run_c is not None:
        return str(fs.metar_run_c)
    return '<span class="tag" title="Nessun METAR nel giorno mercato">n/a</span>'


def _city_table_row(
    row: WeatherCityRow,
    selected_day: date,
    fc: object | None = None,
) -> str:
    from poly_bot.weather.forecast_views import StationForecastSummary

    fs = fc if isinstance(fc, StationForecastSummary) else None
    ev = row.event
    st = row.station
    display = row.city.display_name if row.city else ev.city_key
    icao = st.icao if st else "—"
    coord = "—"
    if st and st.latitude is not None and st.longitude is not None:
        coord = f"{st.latitude:.3f}, {st.longitude:.3f}"
    elif st and st.icao:
        coord = f"ICAO {st.icao} (coords pending)"
    tz = st.timezone if st else "—"
    station = html.escape((st.station_name if st else "") or "—")[:40]
    taf_c = _format_taf_cell(fs)
    metar_c = _format_metar_cell(fs)
    model_c = "—"
    if fs and fs.merged_tmax_c is not None:
        model_c = f"{fs.merged_tmax_c}"
        if fs.merged_source:
            model_c += f' <span class="muted">({html.escape(fs.merged_source[:12])})</span>'
    edge = html.escape(fs.edge_hint if fs and fs.edge_hint else "—")
    edge_cls = ""
    if fs and fs.edge_hint in ("model_higher", "model_lower"):
        edge_cls = ' class="warn"'
    detail = urlencode({"city": ev.city_key, "day": selected_day.isoformat()})
    return (
        "<tr>"
        f'<td><a class="city-link" href="/weather/event?{detail}">'
        f"<strong>{html.escape(display)}</strong></a>"
        f'<br/><span class="tag">{html.escape(ev.city_key)}</span></td>'
        f"<td>{station}</td>"
        f'<td><span class="tag">{html.escape(icao or "—")}</span></td>'
        f'<td class="num">{taf_c}</td>'
        f'<td class="num">{metar_c}</td>'
        f'<td class="num">{model_c}</td>'
        f"<td{edge_cls}>{edge}</td>"
        f'<td class="num">{len(row.bins)}</td>'
        f'<td class="num">{format_days_ahead_label(ev.forecast_date)}</td>'
        "</tr>"
    )


def _bin_row(b: WeatherBin, tick: BinTick | None) -> str:
    temp = "—"
    if b.bin_kind == "exact" and b.temp_c is not None:
        temp = str(b.temp_c)
    elif b.bin_kind == "at_or_below" and b.temp_c_upper is not None:
        temp = f"≤{b.temp_c_upper}"
    elif b.bin_kind == "at_or_above" and b.temp_c_lower is not None:
        temp = f"≥{b.temp_c_lower}"
    cid = html.escape(b.condition_id, quote=True)
    return (
        f'<tr data-cid="{cid}">'
        f"<td class=\"num\">{b.bin_index}</td>"
        f"<td>{html.escape(b.bin_label)}</td>"
        f'<td><span class="tag">{html.escape(b.bin_kind)}</span></td>'
        f'<td class="num">{temp}</td>'
        f'<td class="num" data-col="yes">{_num(tick.yes_mid if tick else None, 3)}</td>'
        f'<td class="num" data-col="no">{_num(tick.no_mid if tick else None, 3)}</td>'
        f'<td class="num" data-col="spr">{_num(tick.spread if tick else None, 3)}</td>'
        f'<td class="num" data-col="vol">{_fmt_vol(tick.volume_24h if tick else None)}</td>'
        f'<td class="num" data-col="oi">{_fmt_vol(tick.open_interest if tick else None)}</td>'
        f'<td class="q">{html.escape(b.question)}</td>'
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


def _weather_live_script(event_id: str) -> str:
    eid = html.escape(event_id, quote=True)
    return f"""<script>
(() => {{
  const dot = document.getElementById('live-dot');
  const status = document.getElementById('live-status');
  const qAge = document.getElementById('quote-age');
  const rows = new Map();
  document.querySelectorAll('#bins-table tbody tr[data-cid]').forEach((tr) => {{
    rows.set(tr.dataset.cid, tr);
  }});
  function fmtNum(v, d) {{
    if (v === null || v === undefined) return '—';
    return Number(v).toFixed(d);
  }}
  function fmtVol(v) {{
    if (v === null || v === undefined) return '—';
    if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
    if (v >= 1e3) return (v / 1e3).toFixed(1) + 'k';
    return String(Math.round(v));
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
      const row = rows.get(cid);
      if (!row) continue;
      setCell(row.querySelector('[data-col="yes"]'), fmtNum(q.y, 3), true);
      setCell(row.querySelector('[data-col="no"]'), fmtNum(q.n, 3), true);
      setCell(row.querySelector('[data-col="spr"]'), fmtNum(q.s, 3), true);
      setCell(row.querySelector('[data-col="vol"]'), fmtVol(q.vol), false);
      setCell(row.querySelector('[data-col="oi"]'), fmtVol(q.oi), false);
    }}
  }}
  function fmtAge(s) {{
    if (s === null || s === undefined) return '—';
    if (s < 60) return s + 's';
    if (s < 3600) return Math.round(s / 60) + 'm';
    return (s / 3600).toFixed(1) + 'h';
  }}
  function setStatus(cls, label) {{
    dot.classList.remove('ok', 'warn', 'err');
    if (cls) dot.classList.add(cls);
    status.textContent = label;
  }}
  function connect() {{
    const es = new EventSource('/api/weather/stream?event_id={eid}');
    es.onopen = () => setStatus('ok', 'live');
    es.onmessage = (ev) => {{
      try {{
        const data = JSON.parse(ev.data);
        applyQuotes(data.quotes);
        const age = data.quote_age_s;
        qAge.textContent = fmtAge(age);
        if (age === null || age === undefined) setStatus('warn', 'no quotes (ws-daemon?)');
        else if (age > 120) setStatus('warn', 'stale');
        else setStatus('ok', 'live');
      }} catch (_) {{}}
    }};
    es.onerror = () => {{
      setStatus('err', 'offline');
      es.close();
      setTimeout(connect, 3000);
    }};
  }}
  connect();
}})();
</script>"""


def _nav_tabs(active: str) -> str:
    return f"""<nav class="tabs">
  <a href="/" class="{'active' if active == 'markets' else ''}">Markets</a>
  <a href="/events" class="{'active' if active == 'events' else ''}">Events</a>
  <a href="/weather" class="{'active' if active == 'weather' else ''}">Weather</a>
  <a href="/weather/board" class="{'active' if active == 'board' else ''}">Board</a>
  <a href="/weather/globe" class="{'active' if active == 'globe' else ''}">Globe</a>
  <a href="/weather/aviation" class="{'active' if active == 'aviation' else ''}">Aviation</a>
  <a href="/taxonomy" class="{'active' if active == 'taxonomy' else ''}">Taxonomy</a>
</nav>"""


def render_weather_globe_page(
    *,
    settings: Settings,
    summaries: list[WeatherDaySummary],
    selected_day: date,
    points_json: str,
    point_count: int,
) -> str:
    day_opts = "".join(
        f'<option value="{s.forecast_date.isoformat()}" '
        f'{"selected" if s.forecast_date == selected_day else ""}>'
        f"{s.forecast_date.isoformat()} ({format_days_ahead_label(s.forecast_date)})"
        f"</option>"
        for s in summaries
    ) or f'<option value="{selected_day.isoformat()}" selected>{selected_day}</option>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>poly_bot — weather globe</title>
<style>
* {{ box-sizing: border-box; margin: 0; }}
html, body {{ height: 100%; overflow: hidden; background: #03060c; color: #e8eef7;
  font-family: "SF Pro Text", system-ui, sans-serif; }}
#globeViz {{ position: fixed; inset: 0; z-index: 1; }}
#globe-error {{
  display: none; position: fixed; inset: 0; z-index: 20; place-content: center;
  padding: 2rem; text-align: center; background: rgba(3, 6, 12, 0.95);
}}
#globe-error.visible {{ display: grid; }}
#globe-error p {{ color: #f5a524; max-width: 28rem; margin: 0 auto 0.5rem; }}
.hud {{
  position: fixed; z-index: 10; pointer-events: none;
  inset: 0; display: flex; flex-direction: column; justify-content: space-between;
}}
.hud-top, .hud-bottom {{ pointer-events: auto; padding: 1rem 1.25rem;
  background: linear-gradient(180deg, rgba(3,6,12,0.92) 0%, transparent 100%); }}
.hud-bottom {{ background: linear-gradient(0deg, rgba(3,6,12,0.92) 0%, transparent 100%); }}
h1 {{ font-size: 1.2rem; font-weight: 600; letter-spacing: 0.04em;
  text-shadow: 0 0 24px rgba(61, 139, 253, 0.5); }}
.sub {{ color: #8b9cb3; font-size: 0.8rem; margin-top: 0.25rem; }}
nav.tabs {{ display: flex; gap: 0.5rem; margin-top: 0.6rem; flex-wrap: wrap; }}
nav.tabs a {{
  color: #8b9cb3; text-decoration: none; padding: 0.3rem 0.65rem; font-size: 0.8rem;
  border: 1px solid transparent; border-radius: 6px;
}}
nav.tabs a.active, nav.tabs a:hover {{ color: #c5dcff; border-color: #3d8bfd;
  background: rgba(61, 139, 253, 0.12); }}
.controls {{ display: flex; gap: 0.5rem; align-items: center; margin-top: 0.5rem; }}
.controls select {{
  background: #121a24; border: 1px solid #3d8bfd; color: #e8eef7;
  border-radius: 6px; padding: 0.35rem 0.6rem; font-size: 0.85rem;
}}
.stat {{ font-size: 0.8rem; color: #3dd68c; }}
.scanline {{
  position: fixed; inset: 0; pointer-events: none; z-index: 5;
  background: repeating-linear-gradient(
    0deg, transparent, transparent 2px, rgba(61, 139, 253, 0.03) 2px, rgba(61, 139, 253, 0.03) 4px);
}}
</style>
</head>
<body>
<div class="scanline"></div>
<div id="globeViz"></div>
<div id="globe-error" role="alert">
  <div>
    <p><strong>Globe failed to load</strong></p>
    <p class="sub" id="globe-error-detail"></p>
    <p class="sub"><a href="/weather?day={selected_day.isoformat()}" style="color:#3d8bfd">table view</a></p>
  </div>
</div>
<div class="hud">
  <div class="hud-top">
    <h1>WEATHER GRID</h1>
    <div class="sub">Polymarket daily high-temperature · resolution stations</div>
    {_nav_tabs("globe")}
    <form class="controls" method="get" action="/weather/globe">
      <label class="sub">Day</label>
      <select name="day" onchange="this.form.submit()">{day_opts}</select>
      <span class="stat">{point_count} cities plotted</span>
    </form>
  </div>
  <div class="hud-bottom sub">
    Drag to rotate · scroll to zoom · click marker → city detail
    · <a href="/weather?day={selected_day.isoformat()}" style="color:#3d8bfd">table view</a>
  </div>
</div>
<script>
const POINTS = {points_json};
const GLOBE_SCRIPTS = [
  '/static/vendor/globe.gl.min.js',
  'https://cdn.jsdelivr.net/npm/globe.gl@2.46.1/dist/globe.gl.min.js',
];
const EARTH_IMG = '/static/vendor/earth-night.jpg';

function showGlobeError(msg) {{
  const el = document.getElementById('globe-error');
  document.getElementById('globe-error-detail').textContent = msg;
  el.classList.add('visible');
}}

function loadScript(src) {{
  return new Promise((resolve, reject) => {{
    const s = document.createElement('script');
    s.src = src;
    s.async = true;
    s.onload = () => resolve(src);
    s.onerror = () => reject(new Error('Failed to load ' + src));
    document.head.appendChild(s);
  }});
}}

async function loadGlobeLib() {{
  if (typeof Globe === 'function') return;
  let lastErr = null;
  for (const src of GLOBE_SCRIPTS) {{
    try {{
      await loadScript(src);
      if (typeof Globe === 'function') return;
    }} catch (e) {{
      lastErr = e;
    }}
  }}
  throw lastErr || new Error('Globe is not defined after loading scripts');
}}

function pointColor(d) {{
  const a = d.days_ahead ?? 0;
  if (a <= 0) return 'rgba(61, 214, 140, 0.95)';
  if (a === 1) return 'rgba(61, 139, 253, 0.95)';
  return 'rgba(245, 165, 36, 0.95)';
}}

function pointLabel(d) {{
  const esc = (s) => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;');
  return `<div style="background:rgba(8,14,24,0.92);border:1px solid #3d8bfd;padding:8px 10px;
    border-radius:8px;font-size:12px;max-width:220px;box-shadow:0 0 20px rgba(61,139,253,0.35);">
    <div style="font-weight:600;color:#3dd68c;margin-bottom:4px">${{esc(d.display_name)}}</div>
    <div style="color:#8b9cb3">${{esc(d.lead_temp)}} · ${{d.bin_count}} bins · +${{d.days_ahead}}d</div>
    <div style="color:#8b9cb3;margin-top:2px">${{esc(d.station_name)}} (${{esc(d.icao)}})</div>
  </div>`;
}}

async function initGlobe() {{
  try {{
    await loadGlobeLib();
    const el = document.getElementById('globeViz');
    const globe = new Globe(el)
      .globeImageUrl(EARTH_IMG)
      .backgroundColor('#03060c')
      .showAtmosphere(true)
      .atmosphereColor('rgba(61, 139, 253, 0.55)')
      .atmosphereAltitude(0.18)
      .pointsData(POINTS)
      .pointLat('lat')
      .pointLng('lng')
      .pointLabel(pointLabel)
      .pointColor(pointColor)
      .pointAltitude(0.03)
      .pointRadius(0.35)
      .onPointClick((d) => {{ window.location.href = d.detail_url; }})
      .width(window.innerWidth)
      .height(window.innerHeight);
    window.addEventListener('resize', () => {{
      globe.width(window.innerWidth).height(window.innerHeight);
    }});
  }} catch (err) {{
    console.error(err);
    showGlobeError(String(err && err.message ? err.message : err));
  }}
}}

initGlobe();
</script>
</body>
</html>"""


def _weather_cards_css() -> str:
    return """<style>
.health-bar {
  display: flex; flex-wrap: wrap; gap: 0.75rem; margin-top: 0.65rem; font-size: 0.8rem;
}
.health-item { display: inline-flex; align-items: center; gap: 0.35rem;
  background: var(--panel); border: 1px solid var(--border); border-radius: 8px;
  padding: 0.35rem 0.65rem;
}
.health-item .dot {
  width: 0.5rem; height: 0.5rem; border-radius: 50%; background: var(--muted);
}
.health-item .dot.ok { background: var(--green); }
.health-item .dot.warn { background: #f5a524; }
.health-item .dot.err { background: #f2555a; }
.card-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 1rem; margin-top: 1rem;
}
.market-card {
  background: var(--panel); border: 1px solid var(--border); border-radius: 12px;
  padding: 1rem 1.1rem; display: flex; flex-direction: column; gap: 0.55rem;
  transition: border-color 0.2s;
}
.market-card:hover { border-color: var(--accent); }
.card-top { display: flex; justify-content: space-between; align-items: baseline; gap: 0.5rem; }
.market-card h2 { margin: 0; font-size: 1.05rem; font-weight: 600; }
.market-card h2 a { color: var(--text); text-decoration: none; }
.market-card h2 a:hover { color: var(--link-hover); }
.days-tag { font-size: 0.72rem; color: var(--muted); border: 1px solid var(--border);
  padding: 0.1rem 0.4rem; border-radius: 4px; }
.card-station { font-size: 0.78rem; line-height: 1.4; }
.coord-link { color: var(--link); text-decoration: none; }
.coord-link:hover { text-decoration: underline; }
.card-temps { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
.temp-block { background: #0e141c; border-radius: 8px; padding: 0.5rem 0.6rem; }
.temp-block.highlight { border: 1px solid rgba(61, 139, 253, 0.35); }
.temp-block .lbl { display: block; font-size: 0.68rem; color: var(--muted); text-transform: uppercase;
  letter-spacing: 0.04em; }
.temp-block .val { font-size: 1.15rem; font-weight: 600; font-variant-numeric: tabular-nums; }
.temp-block .sub-val { display: block; font-size: 0.72rem; color: var(--muted); margin-top: 0.15rem; }
.wu-status { font-size: 0.75rem; display: flex; align-items: center; gap: 0.35rem; }
.wu-status .dot { width: 0.45rem; height: 0.45rem; border-radius: 50%; background: var(--muted); }
.wu-status.ok .dot { background: var(--green); }
.wu-status.warn .dot { background: #f5a524; }
.wu-status.err .dot { background: #f2555a; }
.model-tag { margin-left: 0.35rem; color: var(--muted); }
.card-err { color: #f2555a; font-size: 0.72rem; word-break: break-word; }
.card-prices summary { cursor: pointer; font-size: 0.8rem; color: var(--muted); }
.spark-bars { display: flex; align-items: flex-end; gap: 3px; height: 72px; margin-top: 0.5rem; }
.spark-col { flex: 1; display: flex; flex-direction: column; align-items: center; min-width: 0; }
.spark-col.fav .spark-fill { background: var(--green); }
.spark-fill { width: 100%; max-width: 28px; background: var(--accent); border-radius: 3px 3px 0 0;
  min-height: 2px; }
.spark-lbl { font-size: 0.62rem; color: var(--muted); margin-top: 2px; overflow: hidden;
  text-overflow: ellipsis; max-width: 100%; }
.empty-catalog { color: #f5a524; padding: 1rem 0; }
</style>"""


def _weather_css() -> str:
    return """<style>
:root {
  --bg: #0b0f14; --panel: #121a24; --border: #243044; --text: #e8eef7;
  --muted: #8b9cb3; --accent: #3d8bfd; --green: #3dd68c;
  --link: #c5dcff; --link-hover: #3dd68c;
}
* { box-sizing: border-box; }
body { margin: 0; font-family: system-ui, sans-serif; background: var(--bg);
  color: var(--text); line-height: 1.45; }
header { padding: 1rem 1.25rem; border-bottom: 1px solid var(--border); }
h1 { margin: 0 0 0.25rem; font-size: 1.35rem; }
.sub, .muted { color: var(--muted); font-size: 0.85rem; }
.rules { font-size: 0.78rem; max-width: 900px; white-space: pre-wrap; }
nav.tabs { margin: 0.75rem 0; display: flex; gap: 0.5rem; }
nav.tabs a { color: var(--muted); text-decoration: none; padding: 0.35rem 0.75rem;
  border-radius: 6px; border: 1px solid transparent; font-size: 0.85rem; }
nav.tabs a.active { color: var(--text); border-color: var(--border); background: var(--panel); }
.filters { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 0.5rem; }
.filters input, .filters select, .filters button {
  background: var(--panel); border: 1px solid var(--border); color: var(--text);
  border-radius: 6px; padding: 0.4rem 0.6rem; font-size: 0.85rem; }
.filters button { background: var(--accent); color: #fff; border: none; cursor: pointer; }
.day-pills { display: flex; flex-wrap: wrap; gap: 0.45rem; margin-top: 0.65rem; }
.day-pill {
  text-decoration: none; font-size: 0.82rem; padding: 0.35rem 0.7rem;
  border-radius: 6px; border: 1px solid var(--border); color: var(--muted);
  background: var(--panel);
}
.day-pill:hover { color: var(--text); border-color: var(--accent); }
.day-pill.active { color: var(--text); border-color: var(--accent); background: #1a2a42; }
.day-pill.empty { opacity: 0.55; border-style: dashed; }
.stats { display: flex; gap: 0.75rem; margin-top: 0.75rem; flex-wrap: wrap; }
.stat { background: var(--panel); border: 1px solid var(--border); border-radius: 8px;
  padding: 0.45rem 0.75rem; font-size: 0.8rem; }
.stat b { color: var(--accent); }
main { padding: 1rem 1.25rem 2rem; }
.table-wrap { overflow: auto; border: 1px solid var(--border); border-radius: 10px;
  background: var(--panel); }
table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
th { text-align: left; padding: 0.55rem 0.65rem; background: #1a2433; color: var(--muted);
  position: sticky; top: 0; }
td { padding: 0.45rem 0.65rem; border-top: 1px solid var(--border); vertical-align: top; }
.num { text-align: right; font-variant-numeric: tabular-nums; }
.tag { display: inline-block; padding: 0.1rem 0.4rem; border-radius: 4px;
  font-size: 0.72rem; background: #1e2a3d; color: var(--muted); }
.aviation-hint { margin-top: 0.5rem; max-width: 960px; line-height: 1.5; }
.warn { color: #f5a524; font-weight: 600; }
h2 { font-size: 1rem; margin: 1.25rem 1.25rem 0.5rem; }
.q { max-width: 360px; }
a.city-link, a.back-link {{
  color: var(--link); text-decoration: none; font-weight: 600;
}}
a.city-link:hover, a.back-link:hover {{
  color: var(--link-hover); text-decoration: underline;
}}
a.link-muted {{ color: var(--muted); text-decoration: none; }}
a.link-muted:hover {{ color: var(--link); }}
footer {{ color: var(--muted); font-size: 0.75rem; margin-top: 1rem; }}
.live-line {{ margin-top: 0.35rem; }}
.live-dot {{
  display: inline-block; width: 0.55rem; height: 0.55rem; border-radius: 50%;
  background: var(--muted); margin-right: 0.35rem; vertical-align: 0.05rem;
}}
.live-dot.ok {{ background: var(--green); }}
.live-dot.warn {{ background: #f5a524; }}
.live-dot.err {{ background: #f2555a; }}
td.tick-flash {{ animation: tickFlash 0.45s ease-out; }}
@keyframes tickFlash {{
  from {{ background: rgba(61, 214, 140, 0.22); }}
  to {{ background: transparent; }}
}}
</style>"""
