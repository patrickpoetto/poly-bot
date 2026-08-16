"""PAPER trading only — no real orders, no real money. Ever.

Live-replicates the validated BTC 5m entry rule: watch btc-up-or-down-5m
markets via Polymarket's CLOB WebSocket, and when a side's price is
confirmed by a REAL executed trade (WS last_trade_price event, not a
resting book/ask level) at >=0.80 with between 30 and 120 seconds left
before close, log a SIMULATED $50 position. Track it to resolution and
update a local HTML dashboard.

Architecture (why WS-only now, and why not simpler):
- Book/ask alone is not the backtest's signal: Polymarket's WS "market"
  channel's "book"/"price_change" events reflect resting quotes, which
  cost nothing to leave stale — a real loss showed the book ask hit 0.84
  with no real trade anywhere near that price, the confirming trade only
  appeared 117s later at 0.01-0.06. A resting ask is not a transaction;
  the entry signal has to be a genuine trade print.
- REST polling (data-api.polymarket.com/trades) was the first fix for
  that, and worked for a while, but accumulated a long bug history (see
  git log): a 300s Cloudflare edge cache, a long-lived httpx connection
  serving stale bodies despite cf-cache-status: MISS, a limit=30/limit=300
  response-size truncation that silently excluded real qualifying trades
  from busy candles. Each was fixed in turn — then, 2026-08-12, the
  signal became reliably ABSENT from REST during the live 30-120s window
  on 3+ independently-observed markets (full 90s active polling, no
  candidate found) while always present when re-checked minutes later.
  Response latency (~150ms) and Cloudflare caching (MISS throughout) were
  directly ruled out as causes; the exact mechanism (probably some
  REST-side indexing delay) was never isolated.
- Fix: turns out the earlier claim that "no trade event exists on the WS
  channel" was wrong — it does: last_trade_price, a genuine executed
  trade (has size + transaction_hash, real capital risked), pushed by
  the matching engine as it happens. A direct capture showed 138 such
  events in 20s for one market, far more real-time activity than REST
  ever surfaced for the same window. PriceFeed now buffers these
  per-market (see PriceFeed.trades) and confirm_recent_trade_ws reads
  that local buffer directly — no REST call, no polling, no possible
  indexing-delay gap, and the WS connection was already open for book
  data anyway.
- Market discovery (gamma-api /markets) and resolution checks
  (clob.polymarket.com /markets/{id}, cf-cache-status: DYNAMIC) still use
  REST — no caching issues ever observed there, and both are naturally
  low-frequency (discovery every few seconds, resolution only while a
  position is open).
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
MARKET_DURATION_S = 300  # btc-updown-5m: durata fissa di ogni mercato
FEE_RATE = 0.07  # crypto taker fee rate, exponent 1

DISCOVERY_INTERVAL_S = 3.0  # how often to poll Gamma for newly-entering markets
RESOLUTION_INTERVAL_S = 3.0  # how often to poll CLOB for resolution of an open position
ENTRY_CHECK_INTERVAL_S = 1.0  # how often _check_entry scans tracked tokens
# against PriceFeed's local WS trade buffer — an in-memory read now, not a
# network call, but kept at 1s to match the loop's other cadences.
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
    n_trades_pre_entry: int = 0
    balance: float = 1.0


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
    n_trades_pre_entry: int = 0
    balance: float = 1.0


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


async def fetch_active_btc5m() -> list[dict[str, Any]]:
    """limit=100, not 20: Polymarket has since added the same updown-5m/15m
    format for 7 more assets alongside BTC (bnb, doge, eth, hype, sol, xrp,
    zec) — 16 market families now compete for the same end_date_min/max
    window. Found live (2026-08-11, zero trades overnight): with limit=20,
    btc-updown-5m can land past position 20 in the endDate-ascending
    ordering (observed directly at position 23) and silently vanish from
    the discovered list — no error, just an empty result for that poll.

    Fresh one-off client, not the loop's shared/pooled one: found live
    (2026-08-11, second zero-trade stretch same day) that a standalone
    script with a fresh client found valid signals on every one of the
    last 6 closed markets, while the live process — running for over an
    hour on the same shared client — discovered nothing that whole time.
    Same connection-staleness bug class already fixed once for
    confirm_recent_trade's data-api calls; had never been applied here.

    horizon = now + MARKET_DURATION_S, not now + MAX_SECONDS_LEFT + 60:
    widens registration lead time (tokens get registered for WS trade
    tracking, via _discover -> feed.register_token, at/near market open
    instead of ~120s in). Originally made 2026-08-13 on the theory that
    this alone explained a 92.1%-degenerate n_trades_pre_entry (see
    MIN_BALANCE below) — re-derived the timing on 2026-08-15 and that
    theory doesn't hold up (registration at ~120s old was already ~60s
    ahead of window_start_ts=180s old, comfortable margin). The real
    cause turned out to be WS message loss under current trade volume
    (see refine_pre_entry_via_rest). Left in anyway since it's a strict
    improvement with no downside, not because it's confirmed to matter."""
    async with httpx.AsyncClient() as client:
        now = datetime.now(UTC)
        horizon = now.timestamp() + MARKET_DURATION_S + 60
        now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        horizon_iso = datetime.fromtimestamp(horizon, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        resp = await client.get(
            f"{GAMMA_HOST}/markets",
            params={
                "active": "true",
                "closed": "false",
                "order": "endDate",
                "ascending": "true",
                "limit": 100,
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


async def check_resolution(condition_id: str) -> dict[str, bool] | None:
    """Fresh one-off client, not the loop's shared/pooled one — same
    connection-staleness bug class as fetch_active_btc5m/confirm_recent_trade
    (2026-08-11); applied here precautionarily for consistency, not from a
    directly observed failure on this specific call."""
    try:
        async with httpx.AsyncClient() as client:
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
        # asset_id -> (condition_id, outcome), popolato da register_token.
        # Serve per indicizzare i trade ricevuti via WS per mercato (entrambi
        # i lati insieme), non per singolo token — vedi self.trades sotto.
        self.token_meta: dict[str, tuple[str, str]] = {}
        # condition_id -> lista di trade REALI ricevuti via WS (event_type
        # "last_trade_price"), entrambi i lati mescolati e ordinati per
        # arrivo. Sostituisce il polling REST su data-api/trades — vedi
        # confirm_recent_trade_ws più sotto per il motivo (2026-08-12: il
        # REST mostrava un segnale quasi sempre assente in tempo reale ma
        # sempre presente a posteriori, per motivi mai isolati con
        # certezza nonostante ore di test; il canale WS last_trade_price è
        # uno stream diretto, non polling, e nei test manuali mostrava
        # centinaia di eventi/minuto con latenza sub-secondo).
        self.trades: dict[str, list[dict[str, Any]]] = {}

    def register_token(self, token_id: str, condition_id: str, outcome: str) -> None:
        self.token_meta[token_id] = (condition_id, outcome)

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
                meta = self.token_meta.pop(tid, None)
                if meta:
                    self.trades.pop(meta[0], None)
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

        if item.get("event_type") == "last_trade_price":
            self._process_trade(asset_id, item)
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

    def _process_trade(self, asset_id: str, item: dict[str, Any]) -> None:
        """last_trade_price: a genuine executed trade pushed by the
        matching engine, not a resting quote — has size and
        transaction_hash, real capital risked. WS timestamp is
        milliseconds (13-digit string), unlike the REST /trades
        endpoint's seconds — divide by 1000 to stay consistent with
        end_ts/window_start_ts used everywhere else."""
        meta = self.token_meta.get(asset_id)
        if meta is None:
            return
        condition_id, outcome = meta
        try:
            price = float(item["price"])
            size = float(item.get("size", 0.0))
            ts_ms = float(item["timestamp"])
        except (KeyError, TypeError, ValueError):
            return
        self.trades.setdefault(condition_id, []).append(
            {"outcome": outcome, "price": price, "size": size, "timestamp": ts_ms / 1000.0}
        )


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
            f"<td>{t.n_trades_pre_entry}</td><td>{t.balance:.2f}</td>"
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
<tr><th>Ora</th><th>Mercato</th><th>Lato</th><th>Prezzo</th><th>Secondi rimasti</th><th>N pre-entry</th><th>Balance</th><th>Esito</th><th>P&amp;L</th></tr>
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


async def _discover(feed: PriceFeed, tracked: dict[str, TrackedToken]) -> None:
    now_ts = int(datetime.now(UTC).timestamp())
    try:
        markets = await fetch_active_btc5m()
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
                feed.register_token(tid, condition_id, str(outcome))
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


async def _check_resolution(state: PaperState) -> None:
    """Checks every open position, not just one — positions are independent
    5-min markets, no reason a slow-to-resolve one should block checking the
    rest (see _check_entry docstring: this used to be exactly the reason
    ~4.7x of the theoretical entry rate was being silently skipped)."""
    if not state.open_positions:
        return
    still_open: list[OpenPosition] = []
    closed_any = False
    for p in state.open_positions:
        res = await check_resolution(p.condition_id)
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
            n_trades_pre_entry=p.n_trades_pre_entry,
            balance=p.balance,
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

MIN_BALANCE = 0.90
# Quota di volume (size, non conteggio trade) sul lato che stiamo per
# comprare, rispetto al volume totale di ENTRAMBI i lati nella finestra
# pre-ingresso. Idea di Pat (2026-08-13): il rapporto semplice (balance)
# distingue persi/vinti molto piu' debolmente del VOLUME ASSOLUTO
# sull'altro lato (persi: mediana 740.7, vinti: mediana 191.1 — quasi 4x),
# ma bucketizzato per balance il segnale e' comunque forte e piu' semplice
# da usare come singola soglia. Validato su backtest 2050 mercati storici:
# EV/trade -$0.377 (nessun filtro) -> +$0.933 (balance>=0.90, 418 casi,
# 7.2% perse vs 13.5% base, Wilson 95% CI [5.1%,10.1%] esclude nettamente
# il tasso base). Correlazione con MAX_PRE_ENTRY_ACTIVITY quasi nulla
# (-0.09) — non ridondante, ma il filtro balance da solo domina quasi
# completamente quello attivita (416 vs 418 casi in overlap), quindi
# applicato PRIMA: se balance troppo basso, scarta senza nemmeno
# calcolare n_trades_pre_entry.


def confirm_recent_trade_ws(
    feed: PriceFeed, condition_id: str, side: str, window_start_ts: float
) -> tuple[dict[str, Any], int, float] | None:
    """Same signal the backtest measures — the FIRST real trade crossing
    the threshold within the 30-120s window — but read from PriceFeed's
    local buffer of WS last_trade_price events instead of polling REST.

    Replaces the original REST-based confirm_recent_trade (data-api.polymarket.com
    /trades) on 2026-08-12: found live, repeatedly, that a signal present
    in >=0.80 trades was reliably absent from REST during the live 30-120s
    window (confirmed via a standalone script hitting the exact same
    endpoint) yet always present when re-checked minutes later, on 3+
    independent markets with full 90s-window observation. Timing tests
    ruled out response latency (REST answered in ~150ms either way,
    limit=300 vs 2000) and Cloudflare caching (cf-cache-status was MISS
    throughout) as causes — the exact mechanism was never isolated, but a
    direct WS capture showed 138 last_trade_price events in 20s for one
    market, an order of magnitude more real-time activity than REST ever
    showed for the same window. last_trade_price is a genuine executed
    trade (has size + transaction_hash), not a resting quote — same kind
    of signal as before, delivered by push instead of poll.

    No longer async / no network call here: PriceFeed.run() already
    appends every last_trade_price event to self.trades as it arrives on
    the open WS connection; this just reads that local list."""
    trades = feed.trades.get(condition_id, [])
    candidates = [
        t
        for t in trades
        if t["outcome"] == side and t["price"] >= ENTRY_THRESHOLD and t["timestamp"] >= window_start_ts
    ]
    if not candidates:
        return None
    confirming_trade = min(candidates, key=lambda t: t["timestamp"])
    pre_entry_trades = [
        t for t in trades if window_start_ts <= t["timestamp"] <= confirming_trade["timestamp"]
    ]
    n_trades_pre_entry = len(pre_entry_trades)
    vol_side = sum(t["size"] for t in pre_entry_trades if t["outcome"] == side)
    vol_total = sum(t["size"] for t in pre_entry_trades)
    balance = vol_side / vol_total if vol_total > 0 else 1.0
    return confirming_trade, n_trades_pre_entry, balance


async def refine_pre_entry_via_rest(
    condition_id: str, side: str, window_start_ts: float, confirming_ts: float
) -> tuple[int, float] | None:
    """WS last_trade_price only captures a fraction of real trades under
    current volume — confirmed live 2026-08-16: listened directly on one
    market for 187s, WS caught 232 real trades vs 741 shown by REST for the
    exact same window (matched by size, a near-unique fingerprint per trade,
    since WS also truncates price to 2 decimals while REST keeps full
    precision — 40.8% true capture rate). n_trades_pre_entry/balance
    computed from the WS buffer were reading mostly-degenerate as a result
    (92.1% of live entries had n_trades_pre_entry==1, vs 15.3% in the
    historical population MIN_BALANCE was validated on) — not because
    markets are quiet (a fresh REST sample the same day showed a *median*
    of 66 pre-entry trades, far busier than the historical baseline), but
    because the buffer was missing most of the tape.

    WS still does the fast, cheap job of detecting THAT a trade crossed
    ENTRY_THRESHOLD (only needs to see one — confirm_recent_trade_ws stays
    the trigger). This makes ONE targeted REST call, only after a WS
    signal fires, to recompute n_trades_pre_entry/balance from the
    complete tape before the filters run — not the continuous low-latency
    polling that caused the original REST-invisibility issue on
    2026-08-12 (a single call per rare signal, not a tight real-time
    detection loop)."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{DATA_API}/trades", params={"market": condition_id, "limit": 2000}, timeout=8.0
            )
            r.raise_for_status()
            trades = r.json()
    except httpx.HTTPError:
        return None
    if not isinstance(trades, list):
        return None
    pre_entry_trades = [t for t in trades if window_start_ts <= float(t["timestamp"]) <= confirming_ts]
    n_trades_pre_entry = len(pre_entry_trades)
    vol_side = sum(float(t["size"]) for t in pre_entry_trades if t.get("outcome") == side)
    vol_total = sum(float(t["size"]) for t in pre_entry_trades)
    balance = vol_side / vol_total if vol_total > 0 else 1.0
    return n_trades_pre_entry, balance


async def _check_entry(
    state: PaperState, tracked: dict[str, TrackedToken], entered_conditions: set[str], feed: PriceFeed
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

        # Real trade confirmation via WS last_trade_price (see
        # confirm_recent_trade_ws docstring — replaced REST polling on
        # 2026-08-12) — NOT gated on the WS book/ask first. A real
        # qualifying trade and the CURRENT best ask are not the same
        # thing (the book can revert/differ moments after a print), so
        # requiring both simultaneously would silently drop valid
        # backtest-matching signals.
        window_start_ts = t.end_ts - MAX_SECONDS_LEFT
        result = confirm_recent_trade_ws(feed, t.condition_id, t.outcome, window_start_ts)
        if result is None:
            continue
        confirming_trade, _ws_n_trades_pre_entry, _ws_balance = result

        # Rifinisci balance/n_trades_pre_entry via REST (vedi
        # refine_pre_entry_via_rest — il buffer WS perde la maggioranza dei
        # trade reali sotto il volume attuale, 2026-08-16). Una sola
        # chiamata mirata, non polling continuo.
        refined = await refine_pre_entry_via_rest(
            t.condition_id, t.outcome, window_start_ts, confirming_trade["timestamp"]
        )
        if refined is None:
            print(
                f"[{now:%H:%M:%S}] SALTATO {t.slug} {t.outcome} — REST non disponibile per rifinire balance, ritento",
                flush=True,
            )
            continue
        n_trades_pre_entry, balance = refined
        if balance < MIN_BALANCE:
            print(
                f"[{now:%H:%M:%S}] SCARTATO {t.slug} {t.outcome} — balance troppo basso "
                f"({balance:.3f} < {MIN_BALANCE})",
                flush=True,
            )
            entered_conditions.add(t.condition_id)
            continue
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
                n_trades_pre_entry=n_trades_pre_entry,
                balance=balance,
            )
        )
        entered_conditions.add(t.condition_id)
        print(
            f"[{now:%H:%M:%S}] ENTRATA {t.slug} {t.outcome} @ {entry_price:.3f} "
            f"(confermata da trade reale size={confirm_size:.1f}, n_pre_entry={n_trades_pre_entry}, "
            f"balance={balance:.3f}, {seconds_left}s dalla chiusura) — ${STAKE_USD} paper",
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

    last_discovery = 0.0
    last_resolution = 0.0
    last_dashboard = 0.0
    try:
        while True:
            now = time.monotonic()

            if now - last_discovery >= DISCOVERY_INTERVAL_S:
                await _discover(feed, tracked)
                last_discovery = now

            if state.open_positions and now - last_resolution >= RESOLUTION_INTERVAL_S:
                await _check_resolution(state)
                last_resolution = now

            entered = await _check_entry(state, tracked, entered_conditions, feed)

            if entered or now - last_dashboard >= 5.0:
                render_dashboard(state, feed.connected)
                last_dashboard = now

            await asyncio.sleep(ENTRY_CHECK_INTERVAL_S)
    finally:
        ws_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
