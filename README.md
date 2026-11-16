# poly-bot — Polymarket BTC 5-Minute Up/Down Paper Trader

A research and paper-trading bot for Polymarket's **"Bitcoin Up or Down"**
5-minute prediction markets. It watches live markets, backtests an entry
rule against historical trade data, and runs a simulated live trader with
a self-refreshing dashboard — **no real orders are ever placed, and no
real money is ever at risk.**

## What it does

Every 5 minutes, Polymarket opens a new "BTC Up or Down" market that
resolves on whether BTC is above or below its opening price. This project
implements and validates a simple entry rule for that market:

> When a side's price is first confirmed by a **real executed trade**
> (not a resting order-book quote) at **≥0.80**, with **30–120 seconds**
> left before the candle closes, open a fixed $50 simulated position on
> that side.

The "real trade, not book quote" distinction is the core design decision,
and it is the project's most interesting engineering lesson (see
[Signal history](#signal-history-the-interesting-bug) below): a resting
ask in the order book costs nothing to leave stale, so it is a poor proxy
for where the market actually believes the outcome is going. Only a
printed trade — real capital changing hands — is used to trigger entries.

The repo contains two kinds of scripts:

- **Backtests** (`btc_5m_*_scrape.py`) — pull historical market + trade-tape
  data from Polymarket's REST APIs and evaluate the entry rule across
  different thresholds, time windows, and market sets.
- **Live paper trader** (`btc_5m_paper_trader.py`) — connects to
  Polymarket's CLOB WebSocket, watches live markets in real time, logs
  simulated entries/exits to a local JSON/JSONL state file, and renders a
  local auto-refreshing HTML dashboard (`btc_5m_dashboard.html`).
- **Raw WS recorder** (`btc_5m_ws_recorder.py`) — since Polymarket exposes
  no historical order-book data, this script just records raw book/trade
  WebSocket events to partitioned parquet, going forward, for future
  order-book-behavior analysis.

## Signal history (the interesting bug)

The project's commit history documents a real debugging arc, which is
worth reading for anyone evaluating the code rather than just the result:

1. **Book quote ≠ signal.** An early version triggered off the WS
   order-book's best ask. A real simulated loss showed the book ask
   touching 0.84 with no trade anywhere near that price — the confirming
   trade only printed 117 seconds later, at 0.01–0.06. Resting quotes are
   not transactions.
2. **REST trade-tape, and its own bugs.** Switching the confirmation
   signal to `data-api.polymarket.com/trades` (actual trade prints) fixed
   the false signal, but accumulated its own bug history: a ~300s
   Cloudflare edge cache serving stale responses, a long-lived `httpx`
   connection continuing to serve stale bodies even with
   `cf-cache-status: MISS`, and a `limit=300` page-size truncation that
   silently dropped qualifying trades from busy resolution candles
   (candles commonly print hundreds of trades in the last 15–30 seconds).
   Each was root-caused and fixed in turn.
3. **A REST-only gap that couldn't be explained.** After those fixes,
   the REST signal was still observed to be reliably *absent* during the
   live entry window on multiple independently-observed markets — latency
   and caching were ruled out directly, and the exact mechanism (likely
   REST-side indexing delay) was never isolated.
4. **Fix: use the WebSocket trade event directly.** It turned out the
   WS market channel does carry a genuine trade event
   (`last_trade_price`, with size and transaction hash) — a direct capture
   showed 138 such events in 20 seconds for one market, far more coverage
   than REST ever surfaced for the same window. The live trader now reads
   this from a local in-memory buffer with no REST call and no polling
   gap.
5. **A backtest-truncation trap.** Separately, fetching historical trades
   with a small `limit` on an already-resolved market silently truncated
   the 30–120s entry window for busy candles, which made an early backtest
   look roughly 100x better than reality. Backtests now require
   `limit>=2000` and explicitly flag residual truncation
   (`still_truncated` in `btc_5m_multithreshold_v2_scrape.py`) before any
   result is trusted.

Full detail lives in the module docstrings (`btc_5m_paper_trader.py` especially)
and in `git log`.

## Strategy provenance

Backtested over 166 days / ~17,600 entries at the 0.80 threshold in the
30–120s window: 24 losses observed (99.86% win rate, 95% Wilson CI
[99.80%, 99.90%]), ~22.7% EV/trade, ~107 entries/day. Cross-checked without
the time-window restriction, the 30–120s zone is 20–35x safer than the
0–30s zone at every threshold tested. Stake is a fixed $50/trade, not
compounded.

**This is a backtest + paper-trading result, not a live-money track
record.** High win rates on short-horizon binary markets are consistent
with picking up small, frequent edges that carry tail risk the backtest
window may not fully capture — treat the numbers as a research finding,
not a guarantee.

## Tech stack

- **httpx** (HTTP/2) for REST calls to Polymarket's Gamma, CLOB, and Data
  APIs
- **websockets** for the live CLOB market-data WebSocket feed
- **pandas** / **pyarrow** for backtest data wrangling and parquet storage
- **ruff** + **mypy** + **pre-commit** (ruff-format, trailing-whitespace,
  large-file guard, private-key detection) for linting, formatting, and
  type-checking
- Plain scripts, no framework/DB/package — each file is self-contained;
  see `CLAUDE.md` for the rationale

## Running it

```bash
# Requires Python 3.13+
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Lint / format / type-check
ruff check *.py
ruff format *.py
mypy *.py

# Live paper trader — PAPER ONLY, no real orders, no real money
python btc_5m_paper_trader.py
# then open btc_5m_dashboard.html in a browser (auto-refreshes every 5s)

# Historical backtests (trade tape, not book/ask)
python btc_5m_multithreshold_v2_scrape.py   # full history, limit=2000, truncation-flagged
python btc_5m_recent2000_scrape.py          # most recent ~2000 markets (regime check)

# Raw WS book/trade recorder (going forward only — no historical book data exists)
python btc_5m_ws_recorder.py
```

No API keys, wallet, or `.env` file are required for any of the above —
every script here only reads public Polymarket market data. `.env.example`
is a template left over from an earlier, broader version of this project
and is unused by the current scripts.

## Status

Research / paper-trading project. No live order execution, no wallet
integration, no real funds — by design. This is a signal-validation and
infrastructure exercise, not a production trading system.
