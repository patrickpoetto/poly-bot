"""PAPER trading only — no real orders, no real money. Ever.

Live-replicates the validated BTC 5m entry rule: watch btc-up-or-down-5m
markets via Polymarket's CLOB WebSocket for fast, free book monitoring,
and when a side's ask reaches >=0.80 with between 30 and 120 seconds left
before close, confirm with a REAL recent trade (REST, gated — see below)
before logging a SIMULATED $50 position. Track it to resolution and
update a local HTML dashboard.

Architecture (why WS + gated REST confirmation, not either alone):
- Pure REST polling (original version) hit two real bugs: a 300s
  Cloudflare edge cache on data-api.polymarket.com/trades (served the
  same snapshot for a whole candle), then after busting that, a
  long-lived httpx connection that kept serving stale bodies despite
  cf-cache-status: MISS.
- Pure WS book data (first WS version) is FASTER but not the same
  signal as the backtest: Polymarket's WS "market" channel only emits
  "book" and "price_change" events — no trade event exists on it. A
  live loss showed the book ask hit 0.84 with NO real trade anywhere
  near that price — the confirming trade only appeared 117s later, at
  0.01-0.06. A resting ask is not a transaction.
- Fix: WS book crossing the threshold is just a cheap trigger. Before
  actually entering, confirm_recent_trade() does ONE gated REST check
  (fresh one-off client, not pooled — sidesteps the same
  stale-connection bug class) for a genuine trade print at/above
  threshold within the last 20s. This matches the backtest's actual
  signal while keeping REST usage rare (gated by WS, not continuous).
- Market discovery (gamma-api /markets) and resolution checks
  (clob.polymarket.com /markets/{id}, cf-cache-status: DYNAMIC) never
  showed caching issues and are unchanged.
- WS pattern modeled on /Users/pat/Desktop/python_proj/polymarket_bot's
  bot1_btc.py / bot2_btc/price_feed.py.

Strategy provenance: btc_5m_multithreshold_scrape.py backtest, 166 days,
17600 entries at threshold 0.80 in the 30-120s window, 24 losses (99.86%
observed, 95% Wilson CI [99.80%, 99.90%]) — EV/trade ~22.7%, frequency
~107/day. Cross-checked without the window restriction
(btc_5m_nowindow_scrape.py): 30-120s is 20-35x safer than the 0-30s zone
at every threshold tested. Fixed $50 stake per trade, NOT reinvested/
compounded.

Run:
    python btc_5m_paper_trader.py
Then open btc_5m_dashboard.html in a browser (auto-refreshes every 5s).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import websockets

GAMMA_HOST = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

STAKE_USD = 50.0
ENTRY_THRESHOLD = 0.80
MIN_SECONDS_LEFT = 30
MAX_SECONDS_LEFT = 120
FEE_RATE = 0.07  # crypto taker fee rate, exponent 1

DISCOVERY_INTERVAL_S = 3.0  # how often to poll Gamma for newly-entering markets
RESOLUTION_INTERVAL_S = 3.0  # how often to poll CLOB for resolution of an open position
ENTRY_CHECK_INTERVAL_S = 1.0  # how often to check for a confirming trade — a REST call
# per in-window token (see _check_entry), not a free in-memory read.
WS_STALE_SECONDS = 5.0  # ignore WS prices older than this — fall back to "no signal"
TRACK_TTL_S = 60  # unsubscribe/stop tracking this long after a market's end_ts.
# WS is only needed for the entry signal (30-120s pre-close); resolution
# is checked separately via CLOB REST regardless of WS subscription. Keep
# this short — see PriceFeed.unsubscribe for why the list must stay lean.

STATE_FILE = Path(__file__).resolve().parent / "btc_5m_paper_state.json"
TRADES_FILE = Path(__file__).resolve().parent / "btc_5m_paper_trades.jsonl"
DASHBOARD_FILE = Path(__file__).resolve().parent / "btc_5m_dashboard.html"


def _net_yield(price: float) -> float:
    # fee = shares * feeRate * p * (1-p), confermato da help.polymarket.com/en/articles/13364478
    # (non feeRate * min(p,1-p) come usato erroneamente per tutta la sessione precedente)
    fee = FEE_RATE * price * (1.0 - price)
    cost = price + fee
    return (1.0 - cost) / cost


@dataclass
class OpenPosition:
    slug: str
    condition_id: str
    side: str
    entry_price: float
    entry_ts: int
    seconds_left_at_entry: int
    stake: float = STAKE_USD
    confirm_size: float = 0.0


@dataclass
class ClosedTrade:
    slug: str
    side: str
    entry_price: float
    seconds_left_at_entry: int
    stake: float
    won: bool
    pnl: float
    closed_at: str
    confirm_size: float = 0.0


@dataclass
class PaperState:
    trades: list[ClosedTrade] = field(default_factory=list)
    open_positions: list[OpenPosition] = field(default_factory=list)
    watching: list[str] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    @property
    def cumulative_pnl(self) -> float:
        return sum(t.pnl for t in self.trades)

    @property
    def wins(self) -> int:
        return sum(1 for t in self.trades if t.won)

    @property
    def losses(self) -> int:
        return sum(1 for t in self.trades if not t.won)


def save_state(state: PaperState) -> None:
    payload = {
        "trades": [asdict(t) for t in state.trades],
        "open_positions": [asdict(p) for p in state.open_positions],
        "started_at": state.started_at,
    }
    STATE_FILE.write_text(json.dumps(payload, indent=2))


def load_state() -> PaperState:
    if not STATE_FILE.exists():
        return PaperState()
    payload = json.loads(STATE_FILE.read_text())
    state = PaperState(started_at=payload.get("started_at", datetime.now(UTC).isoformat()))
    state.trades = [ClosedTrade(**t) for t in payload.get("trades", [])]
    if "open_positions" in payload:
        state.open_positions = [OpenPosition(**p) for p in payload["open_positions"]]
    elif payload.get("open_position"):  # formato precedente, singola posizione
        state.open_positions = [OpenPosition(**payload["open_position"])]
    return state


def append_trade_log(trade: ClosedTrade) -> None:
    with TRADES_FILE.open("a") as f:
        f.write(json.dumps(asdict(trade)) + "\n")


# ── Market discovery (REST, gamma-api — no caching issues observed) ─────────


async def fetch_active_btc5m(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    horizon = now.timestamp() + MAX_SECONDS_LEFT + 60
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    horizon_iso = datetime.fromtimestamp(horizon, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    resp = await client.get(
        f"{GAMMA_HOST}/markets",
        params={
            "active": "true",
            "closed": "false",
            "order": "endDate",
            "ascending": "true",
            "limit": 20,
            "end_date_min": now_iso,
            "end_date_max": horizon_iso,
        },
        timeout=10.0,
    )
    resp.raise_for_status()
    data = resp.json()
    items = data if isinstance(data, list) else data.get("data", [])
    return [m for m in items if str(m.get("slug", "")).startswith("btc-updown-5m-")]


# ── Resolution check (REST, clob — confirmed cf-cache-status: DYNAMIC) ──────


async def check_resolution(client: httpx.AsyncClient, condition_id: str) -> dict[str, bool] | None:
    try:
        r = await client.get(f"{CLOB_HOST}/markets/{condition_id}", timeout=8.0)
        r.raise_for_status()
        data = r.json()
    except httpx.HTTPError:
        return None
    if not data.get("closed"):
        return None
    tokens = data.get("tokens") or []
    return {t["outcome"]: bool(t.get("winner")) for t in tokens if "outcome" in t}


# ── WebSocket price feed ─────────────────────────────────────────────────────


class PriceFeed:
    """Real-time best bid/ask via Polymarket's CLOB market WebSocket.

    Modeled on polymarket_bot/bot2_btc/price_feed.py. Runs as an asyncio
    task alongside the strategy loop (no threading needed — everything
    here is already async).
    """

    def __init__(self, ws_url: str = WS_URL, stale_seconds: float = WS_STALE_SECONDS) -> None:
        self.ws_url = ws_url
        self.stale_seconds = stale_seconds
        self.prices: dict[str, dict[str, float]] = {}
        self.subscribed: set[str] = set()
        self._resub = asyncio.Event()
        self._force_reconnect = asyncio.Event()
        self.connected = False

    def subscribe(self, token_ids: list[str]) -> None:
        new = set(token_ids) - self.subscribed
        if new:
            self.subscribed |= new
            self._resub.set()

    def unsubscribe(self, token_ids: list[str]) -> None:
        """Drop tokens for resolved/expired markets and force a fresh
        reconnect. Confirmed necessary the hard way: sending a large
        subscribe burst (~160+ token_ids, even all valid/just old) leaves
        the CONNECTION itself degraded — it silently stops delivering
        updates for every subscribed token, including brand-new ones,
        even after shrinking the list back down on the same session.
        Only a fresh WS connection recovers it. Reproduced twice."""
        drop = set(token_ids) & self.subscribed
        if drop:
            self.subscribed -= drop
            for tid in drop:
                self.prices.pop(tid, None)
            self._force_reconnect.set()

    def best_ask(self, token_id: str) -> tuple[float | None, bool]:
        """Returns (ask, stale). stale=True if no fresh-enough data."""
        entry = self.prices.get(token_id)
        if not entry:
            return None, True
        stale = time.time() - entry.get("ts", 0.0) > self.stale_seconds
        return entry.get("ask"), stale

    async def run(self) -> None:
        while True:
            try:
                await self._session()
                self.connected = False
                print("[WS] riconnessione forzata (pulizia sottoscrizioni)", flush=True)
            except Exception as exc:
                self.connected = False
                print(f"[WS] errore: {exc} — riconnetto tra 1s", flush=True)
                await asyncio.sleep(1.0)

    async def _session(self) -> None:
        async with websockets.connect(
            self.ws_url, ping_interval=20, ping_timeout=10, close_timeout=5
        ) as ws:
            self.connected = True
            print("[WS] connesso", flush=True)
            self._resub.clear()
            self._force_reconnect.clear()
            await self._send_sub(ws)
            while True:
                if self._force_reconnect.is_set():
                    self._force_reconnect.clear()
                    return
                if self._resub.is_set():
                    self._resub.clear()
                    await self._send_sub(ws)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    self._handle_raw(raw)
                except TimeoutError:
                    continue

    async def _send_sub(self, ws) -> None:
        if self.subscribed:
            await ws.send(json.dumps({"assets_ids": list(self.subscribed), "type": "market"}))
            print(f"[WS] iscritto a {len(self.subscribed)} token", flush=True)

    def _handle_raw(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        items = data if isinstance(data, list) else [data]
        for item in items:
            if isinstance(item, dict):
                self._process_item(item)

    def _process_item(self, item: dict[str, Any]) -> None:
        asset_id = item.get("asset_id") or item.get("market")
        if not asset_id:
            return

        ask: float | None = None
        if "best_ask" in item:
            with contextlib.suppress(TypeError, ValueError):
                ask = float(item["best_ask"])

        asks_arr = item.get("asks", [])
        if asks_arr and ask is None:
            with contextlib.suppress(TypeError, ValueError):
                ask = min(
                    float(a["price"])
                    for a in asks_arr
                    if a.get("price") and float(a.get("size", 1)) > 0
                )

        if ask is not None:
            entry = self.prices.get(asset_id, {})
            entry["ask"] = ask
            entry["ts"] = time.time()
            self.prices[asset_id] = entry


# ── Dashboard ─────────────────────────────────────────────────────────────


def render_dashboard(state: PaperState, ws_connected: bool) -> None:
    row_parts = []
    for t in reversed(state.trades):
        css_class = "win" if t.won else "loss"
        esito = "VINTA" if t.won else "PERSA"
        row_parts.append(
            f"<tr class='{css_class}'>"
            f"<td>{t.closed_at[11:19]}</td><td>{t.slug}</td><td>{t.side}</td>"
            f"<td>{t.entry_price:.3f}</td><td>{t.seconds_left_at_entry}s</td>"
            f"<td>{esito}</td><td>{t.pnl:+.2f}</td></tr>"
        )
    rows = "".join(row_parts)
    open_html = "".join(
        f"<div class='open'>Posizione aperta: {p.slug} — {p.side} @ {p.entry_price:.3f} "
        f"(entrata a {p.seconds_left_at_entry}s dalla chiusura) — ${p.stake:.0f} in gioco</div>"
        for p in state.open_positions
    )
    pnl = state.cumulative_pnl
    pnl_color = "#2f8c77" if pnl >= 0 else "#c0392b"
    ws_html = (
        "<span style='color:#2f8c77'>&#9679; WS connesso</span>"
        if ws_connected
        else "<span style='color:#c0392b'>&#9679; WS disconnesso</span>"
    )
    html = f"""<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>BTC 5m paper trader</title>
<style>
body {{ font-family: -apple-system, sans-serif; background: #111; color: #eee; padding: 24px; }}
h1 {{ font-size: 18px; color: #9AA0A6; }}
.stats {{ display: flex; gap: 24px; margin: 16px 0 24px; align-items: center; }}
.stat {{ background: #1c1c1c; padding: 12px 20px; border-radius: 8px; }}
.stat .label {{ font-size: 12px; color: #9AA0A6; }}
.stat .value {{ font-size: 24px; font-weight: 600; }}
.open {{ background: #2a2a1c; padding: 10px 16px; border-radius: 8px; margin-bottom: 16px; color: #e0c860;}}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ text-align: left; padding: 6px 10px; border-bottom: 1px solid #333; }}
tr.win td:last-child {{ color: #2f8c77; }}
tr.loss {{ background: #2a1414; }}
tr.loss td:last-child {{ color: #c0392b; font-weight: 600; }}
.warn {{ color: #e0c860; font-size: 12px; margin-bottom: 16px; }}
</style></head><body>
<h1>BTC 5m — PAPER TRADER (nessun ordine reale, nessun soldo vero)</h1>
<div class="warn">Avviato: {state.started_at[:19]} UTC · aggiornamento auto ogni 5s · {ws_html}</div>
<div class="stats">
  <div class="stat"><div class="label">P&amp;L cumulato</div><div class="value" style="color:{pnl_color}">${pnl:+.2f}</div></div>
  <div class="stat"><div class="label">Trade chiusi</div><div class="value">{len(state.trades)}</div></div>
  <div class="stat"><div class="label">Vinti / Persi</div><div class="value">{state.wins} / {state.losses}</div></div>
  <div class="stat"><div class="label">Win rate</div><div class="value">{(state.wins/len(state.trades)*100) if state.trades else 0:.2f}%</div></div>
</div>
{open_html}
<table>
<tr><th>Ora</th><th>Mercato</th><th>Lato</th><th>Prezzo</th><th>Secondi rimasti</th><th>Esito</th><th>P&amp;L</th></tr>
{rows}
</table>
</body></html>"""
    DASHBOARD_FILE.write_text(html)


# ── Strategy loop ─────────────────────────────────────────────────────────


@dataclass
class TrackedToken:
    token_id: str
    slug: str
    condition_id: str
    outcome: str
    end_ts: int


async def _discover(client: httpx.AsyncClient, feed: PriceFeed, tracked: dict[str, TrackedToken]) -> None:
    now_ts = int(datetime.now(UTC).timestamp())
    try:
        markets = await fetch_active_btc5m(client)
    except httpx.HTTPError as exc:
        print(f"[{datetime.now(UTC):%H:%M:%S}] discovery error: {exc}", flush=True)
        return

    new_token_ids: list[str] = []
    for m in markets:
        try:
            end_ts = int(datetime.fromisoformat(str(m["endDate"]).replace("Z", "+00:00")).timestamp())
        except (ValueError, TypeError, KeyError):
            continue
        condition_id = m.get("conditionId")
        if not condition_id:
            continue

        token_ids_raw = m.get("clobTokenIds")
        outcomes_raw = m.get("outcomes")
        if isinstance(token_ids_raw, str):
            token_ids_raw = json.loads(token_ids_raw)
        if isinstance(outcomes_raw, str):
            outcomes_raw = json.loads(outcomes_raw)
        if not token_ids_raw or not outcomes_raw:
            continue

        for tid, outcome in zip(token_ids_raw, outcomes_raw, strict=False):
            tid = str(tid)
            if tid not in tracked:
                tracked[tid] = TrackedToken(
                    token_id=tid,
                    slug=m["slug"],
                    condition_id=condition_id,
                    outcome=str(outcome),
                    end_ts=end_ts,
                )
                new_token_ids.append(tid)

    if new_token_ids:
        feed.subscribe(new_token_ids)

    # drop stale entries — must unsubscribe too, not just stop tracking:
    # a bloated subscription list silently kills updates for ALL tokens.
    stale = [tid for tid, t in tracked.items() if now_ts - t.end_ts > TRACK_TTL_S]
    if stale:
        feed.unsubscribe(stale)
        for tid in stale:
            del tracked[tid]


async def _check_resolution(
    client: httpx.AsyncClient, state: PaperState
) -> None:
    """Checks every open position, not just one — positions are independent
    5-min markets, no reason a slow-to-resolve one should block checking the
    rest (see _check_entry docstring: this used to be exactly the reason
    ~4.7x of the theoretical entry rate was being silently skipped)."""
    if not state.open_positions:
        return
    still_open: list[OpenPosition] = []
    closed_any = False
    for p in state.open_positions:
        res = await check_resolution(client, p.condition_id)
        if res is None or p.side not in res:
            still_open.append(p)
            continue
        now = datetime.now(UTC)
        won = res[p.side]
        pnl = STAKE_USD * _net_yield(p.entry_price) if won else -STAKE_USD
        trade = ClosedTrade(
            slug=p.slug,
            side=p.side,
            entry_price=p.entry_price,
            seconds_left_at_entry=p.seconds_left_at_entry,
            stake=p.stake,
            won=won,
            pnl=pnl,
            closed_at=now.isoformat(),
            confirm_size=p.confirm_size,
        )
        state.trades.append(trade)
        append_trade_log(trade)
        closed_any = True
        print(
            f"[{now:%H:%M:%S}] CHIUSA {trade.slug} {trade.side} "
            f"-> {'VINTA' if won else 'PERSA'} pnl={pnl:+.2f} "
            f"(cumulato {state.cumulative_pnl:+.2f})",
            flush=True,
        )
    state.open_positions = still_open
    if closed_any:
        save_state(state)


MAX_PRE_ENTRY_ACTIVITY = 80
# Trade totali (entrambi i lati) tra apertura finestra e trade di conferma.
# Validato due volte, indipendentemente: backtest 2050 mercati storici
# (EV/trade migliore proprio a soglia 80, tra le poche configurazioni
# nettamente positive) e 153 trade paper live reali (perse concentrate
# in modo nettamente bimodale: 8/19 con attivita 2-22, 11/19 con
# attivita 91-411 — filtrando a <=80 il cumulato passa da -$174.53 a
# +$80.47 scartando solo 24 trade). Troppa attivita prima del nostro
# ingresso correla con differenziale BTC reale piu piccolo (mercato
# genuinamente conteso, non deciso) anche quando il prezzo del token
# dice gia >=0.80 — vedi analisi 2026-08-10.


async def confirm_recent_trade(
    condition_id: str, side: str, window_start_ts: float
) -> tuple[dict[str, Any], int] | None:
    """The backtest's signal is the FIRST trade crossing the threshold
    within the 30-120s-before-close window — not "any recent trade", not
    "the most recent trade". Earlier versions used a rolling N-second
    lookback and took the most-recent qualifying trade; verified against
    two live losses (2026-08-07) that this does NOT reproduce a stale/lagged
    signal — in both cases the first in-window trade was already >=0.80 the
    instant the window opened, so a rolling lookback and a from-window-start
    scan pick the same trade. The real reason to fix it: a rolling lookback
    can pick a LATER trade than the true first crossing (e.g. one still in
    flight mid-crash), which is a different price than the backtest ever
    measured. Taking the earliest in-window trade matches the backtest
    exactly and bounds latency to the poll interval instead of the lookback
    window.

    limit=300, not a small number: a market near resolution can print
    30+ trades in a couple of seconds (repeatedly observed all
    session). With a small limit, a real qualifying trade from even 10s
    earlier gets pushed out of the "recent" window by that burst before
    we ever see it — confirmed directly (limit=30 missed a known-good
    trade that limit=300 caught, same market, same query).

    Deliberately uses a fresh one-off client, not a shared/pooled one:
    the same class of stale-connection bug hit both the old REST-polling
    version (long-lived httpx client) and the WS feed (long-lived
    subscription) — a fresh connection per call sidesteps it."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{DATA_API}/trades",
                params={"market": condition_id, "limit": 300, "_": int(time.time() * 1000)},
                timeout=8.0,
            )
            r.raise_for_status()
            trades = r.json()
    except httpx.HTTPError:
        return None

    candidates = [
        t
        for t in trades
        if t.get("outcome") == side
        and float(t.get("price", 0)) >= ENTRY_THRESHOLD
        and float(t.get("timestamp", 0)) >= window_start_ts
    ]
    if not candidates:
        return None
    confirming_trade = min(candidates, key=lambda t: t["timestamp"])
    n_trades_pre_entry = sum(
        1
        for t in trades
        if window_start_ts <= float(t.get("timestamp", 0)) <= confirming_trade["timestamp"]
    )
    return confirming_trade, n_trades_pre_entry


async def _check_entry(
    state: PaperState, tracked: dict[str, TrackedToken], entered_conditions: set[str]
) -> bool:
    """Returns True if a new position was opened. Multiple positions can be
    open at once now — markets are independent 5-min windows, and gating
    entry on "no position open" meant a slow-to-resolve position (resolution
    detection has been observed to lag several minutes) silently blocked
    every market that opened AND closed its own entry window in the
    meantime. Measured directly: median 18.4 min between trades vs a new
    market every 5 min — about 4.7x fewer entries than the signal actually
    offers (2026-08-10 analysis, 2050-market backtest vs 154 live trades)."""
    now = datetime.now(UTC)
    now_ts = int(now.timestamp())

    for t in tracked.values():
        if t.condition_id in entered_conditions:
            continue
        seconds_left = t.end_ts - now_ts
        if not (MIN_SECONDS_LEFT <= seconds_left <= MAX_SECONDS_LEFT):
            continue

        # Check the real trade tape directly for every in-window token —
        # NOT gated on the WS book first. Gating on "WS ask >= threshold"
        # before checking for a confirming trade sounds reasonable but
        # isn't: a real qualifying trade and the CURRENT best ask are not
        # the same thing (the book can revert/differ moments after a
        # print), so requiring both simultaneously silently drops valid
        # backtest-matching signals. Confirmed live: 5398 WS-side
        # crossings, 0 confirmations, over 5.5h — far below the ~22
        # expected from the backtest rate. This matches the backtest's
        # actual signal (a real trade in the window) directly instead.
        window_start_ts = t.end_ts - MAX_SECONDS_LEFT
        result = await confirm_recent_trade(t.condition_id, t.outcome, window_start_ts)
        if result is None:
            continue
        confirming_trade, n_trades_pre_entry = result
        if n_trades_pre_entry > MAX_PRE_ENTRY_ACTIVITY:
            print(
                f"[{now:%H:%M:%S}] SCARTATO {t.slug} {t.outcome} — troppa attivita pre-ingresso "
                f"({n_trades_pre_entry} > {MAX_PRE_ENTRY_ACTIVITY})",
                flush=True,
            )
            entered_conditions.add(t.condition_id)
            continue
        entry_price = float(confirming_trade["price"])
        confirm_size = float(confirming_trade.get("size", 0.0))

        state.open_positions.append(
            OpenPosition(
                slug=t.slug,
                condition_id=t.condition_id,
                side=t.outcome,
                entry_price=entry_price,
                entry_ts=now_ts,
                seconds_left_at_entry=seconds_left,
                confirm_size=confirm_size,
            )
        )
        entered_conditions.add(t.condition_id)
        print(
            f"[{now:%H:%M:%S}] ENTRATA {t.slug} {t.outcome} @ {entry_price:.3f} "
            f"(confermata da trade reale size={confirm_size:.1f}, {seconds_left}s dalla chiusura) — ${STAKE_USD} paper",
            flush=True,
        )
        save_state(state)
        return True
    return False


async def main() -> None:
    state = load_state()
    print(
        f"Paper trader avviato (WebSocket). Stake fisso ${STAKE_USD}/trade. Dashboard: {DASHBOARD_FILE}",
        flush=True,
    )

    feed = PriceFeed()
    ws_task = asyncio.create_task(feed.run())
    render_dashboard(state, feed.connected)

    tracked: dict[str, TrackedToken] = {}
    entered_conditions: set[str] = set()

    async with httpx.AsyncClient() as client:
        last_discovery = 0.0
        last_resolution = 0.0
        last_dashboard = 0.0
        try:
            while True:
                now = time.monotonic()

                if now - last_discovery >= DISCOVERY_INTERVAL_S:
                    await _discover(client, feed, tracked)
                    last_discovery = now

                if state.open_positions and now - last_resolution >= RESOLUTION_INTERVAL_S:
                    await _check_resolution(client, state)
                    last_resolution = now

                entered = await _check_entry(state, tracked, entered_conditions)

                if entered or now - last_dashboard >= 5.0:
                    render_dashboard(state, feed.connected)
                    last_dashboard = now

                await asyncio.sleep(ENTRY_CHECK_INTERVAL_S)
        finally:
            ws_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
