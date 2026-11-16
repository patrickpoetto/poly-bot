# poly_bot — Claude session primer

Standalone research + paper-trading scripts for a Polymarket BTC 5m
("Bitcoin Up or Down") strategy. No package, no DB, no dashboard server —
everything is a self-contained script in the repo root, data lives in
gitignored parquet/JSON/JSONL files next to it.

## Strategy (current, live-validated as of 2026-08)

Watch `btc-updown-5m` markets. When a side's price is first confirmed by a
**real trade** (not just a resting book quote) at >=0.80, with 30-120s left
before close, enter a fixed $50 paper position on that side. Confirmation
must be a genuine trade print (`data-api.polymarket.com/trades`), not a WS
book/ask level — a resting quote costs nothing to leave stale; a trade
risks real capital. See `btc_5m_paper_trader.py` module docstring for the
full bug history (Cloudflare cache, stale connections, WS book != trade
signal, `limit=300` trade-fetch truncation).

**Known backtest trap**: fetching trades with a small `limit` on an
already-resolved market silently truncates the 30-120s entry window for
busy candles (resolution bursts commonly print hundreds of trades in
15-30s) — this made an early backtest look ~100x better than reality.
Always use `limit>=2000` and check for residual truncation
(`still_truncated` in `btc_5m_multithreshold_v2_scrape.py`) before trusting
a historical loss-rate number.

## Quick commands

```bash
source .venv/bin/activate

# Lint / format / type-check
ruff check *.py
ruff format *.py
mypy *.py

# Unit tests (pure logic only: _net_yield, confirm_recent_trade_ws)
pytest -v

# Live paper trader (PAPER ONLY — no real orders, no real money)
python btc_5m_paper_trader.py
# then open btc_5m_dashboard.html in a browser (auto-refreshes every 5s)
# run detached so it survives the session:
#   nohup python btc_5m_paper_trader.py > btc_5m_paper_trader.log 2>&1 & disown

# Historical backtests (data-api.polymarket.com trade tape, not book/ask)
python btc_5m_multithreshold_v2_scrape.py   # full-history, limit=2000, truncation-flagged
python btc_5m_recent2000_scrape.py          # most recent ~2000 markets (regime check)

# WS book/price_change recorder (raw, going forward — no historical book data exists)
python btc_5m_ws_recorder.py
#   nohup python btc_5m_ws_recorder.py > btc_5m_ws_recorder.log 2>&1 & disown
# ~2.4GB/day zstd-compressed (measured, one market tracked at a time) — watch disk.
```

## Data files (gitignored, not in git)

- `btc_5m_paper_state.json` / `btc_5m_paper_trades.jsonl` — live paper trader state/log
- `btc_5m_dashboard.html` — self-refreshing local dashboard, written by the paper trader
- `btc_5m_*.parquet` — cached historical crawls (events + trade tapes)
