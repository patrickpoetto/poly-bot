"""Crawl the most recent ~2000 closed btc-updown-5m markets (up to today,
not the stale Jul 27 cache) and save their full trade tapes (both sides,
from window_start=end_ts-120 onward) for two purposes:

1. Regime check: recompute the threshold=0.80/30-120s-window loss rate on
   just this recent slice and compare to (a) the full 8-month corrected
   backtest (~14.5-20.9% loss rate, negative EV) and (b) live paper results
   (11/105 losses, +31.91 PnL) — is the discrepancy explained by a
   recent-vs-historical regime shift, or does the recent slice still look
   like the bad 8-month number?

2. Hedge simulation input: full post-entry price paths on BOTH sides let us
   simulate "buy the opposite side when it moves to trigger X, sell at TP Y"
   across a parameter grid without re-fetching per parameter combo.

Uses limit=2000 per market (not 300) per the truncation bug found in
btc_5m_multithreshold_scrape.py — 54% of markets had the 30-120s window
completely outside a 300-trade fetch.

Run:
    python btc_5m_recent2000_scrape.py
Output:
    btc_5m_recent2000_events.parquet
    btc_5m_recent2000_trades.parquet
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"
DATA_API = "https://data-api.polymarket.com"
SERIES_ID = 10684
N_MARKETS = 2000
CONCURRENCY = 10
TRADES_LIMIT = 2000
MIN_SECONDS_LEFT = 30
MAX_SECONDS_LEFT = 120

EVENTS_OUT = Path(__file__).resolve().parent / "btc_5m_recent2000_events.parquet"
TRADES_OUT = Path(__file__).resolve().parent / "btc_5m_recent2000_trades.parquet"


def parse_event(e: dict[str, Any]) -> dict[str, Any] | None:
    slug = e.get("slug", "")
    markets = e.get("markets") or []
    if not markets:
        return None
    m = markets[0]
    prices = m.get("outcomePrices")
    if isinstance(prices, str):
        prices = json.loads(prices)
    if not prices or len(prices) != 2:
        return None
    up_price = float(prices[0])
    outcome = "Up" if up_price == 1.0 else "Down" if up_price == 0.0 else None
    try:
        candle_ts = int(slug.rsplit("-", 1)[-1])
    except ValueError:
        candle_ts = None
    return {
        "candle_ts": candle_ts,
        "end_date": m.get("endDate"),
        "slug": slug,
        "outcome": outcome,
        "condition_id": m.get("conditionId"),
    }


async def crawl_recent_events(client: httpx.AsyncClient, n: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    cursor: str | None = None
    page = 0
    while len(rows) < n:
        params: dict[str, Any] = {
            "series_id": SERIES_ID,
            "closed": "true",
            "limit": 100,
            "order": "endDate",
            "ascending": "false",
        }
        if cursor:
            params["end_date_max"] = cursor
        resp = await client.get(GAMMA_EVENTS, params=params, timeout=30)
        resp.raise_for_status()
        events = resp.json()
        new = [e for e in events if e.get("slug") not in seen]
        if not new:
            break
        for e in new:
            seen.add(e["slug"])
            if row := parse_event(e):
                rows.append(row)
        end_dates = [e["endDate"] for e in new if e.get("endDate")]
        if not end_dates:
            break
        cursor = min(end_dates)
        page += 1
        if page % 10 == 0:
            print(f"  events page {page}, {len(rows)} rows so far", flush=True)
        await asyncio.sleep(0.15)
    return pd.DataFrame(rows[:n])


async def fetch_trades(
    client: httpx.AsyncClient, sem: asyncio.Semaphore, condition_id: str, end_ts: int
) -> list[dict[str, Any]]:
    async with sem:
        for attempt in range(3):
            try:
                r = await client.get(
                    f"{DATA_API}/trades",
                    params={"market": condition_id, "limit": TRADES_LIMIT},
                    timeout=25,
                )
                r.raise_for_status()
                trades = r.json()
                break
            except httpx.HTTPError:
                if attempt == 2:
                    return []
                await asyncio.sleep(0.5 * (attempt + 1))

    window_start = end_ts - MAX_SECONDS_LEFT
    trades = [
        t
        for t in trades
        if t.get("timestamp", 0) <= end_ts + 15 and t.get("outcome") in ("Up", "Down")
    ]
    # keep everything from window_start onward — covers both the entry
    # crossing detection and the post-entry path needed for hedge sim
    relevant = [t for t in trades if t.get("timestamp", 0) >= window_start]
    return relevant


async def main() -> None:
    async with httpx.AsyncClient(headers={"User-Agent": "poly-bot-research/0.1"}) as client:
        print(f"Crawling most recent {N_MARKETS} closed btc-updown-5m events...")
        events_df = await crawl_recent_events(client, N_MARKETS)
        events_df = events_df.dropna(subset=["condition_id", "outcome", "candle_ts"])
        events_df.to_parquet(EVENTS_OUT)
        print(f"  saved {len(events_df)} events -> {EVENTS_OUT}")
        print(
            f"  date range: {pd.to_datetime(events_df['end_date']).min()} -> "
            f"{pd.to_datetime(events_df['end_date']).max()}"
        )

        print(f"Fetching trade tapes (limit={TRADES_LIMIT}) for {len(events_df)} markets...")
        sem = asyncio.Semaphore(CONCURRENCY)
        all_rows: list[dict[str, Any]] = []
        start = time.time()

        async def one(row: dict[str, Any]) -> None:
            end_ts = int(row["candle_ts"]) + 300
            trades = await fetch_trades(client, sem, row["condition_id"], end_ts)
            for t in trades:
                all_rows.append(
                    {
                        "slug": row["slug"],
                        "condition_id": row["condition_id"],
                        "winning_outcome": row["outcome"],
                        "end_ts": end_ts,
                        "outcome": t["outcome"],
                        "timestamp": t["timestamp"],
                        "price": float(t["price"]),
                        "size": float(t.get("size", 0.0)),
                    }
                )

        tasks = [one(row) for row in events_df.to_dict("records")]
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            await coro
            if i % 500 == 0:
                elapsed = time.time() - start
                rate = i / elapsed
                eta_min = (len(tasks) - i) / rate / 60
                print(f"  {i}/{len(tasks)} done, {rate:.0f}/s, ETA {eta_min:.1f}min", flush=True)

        trades_df = pd.DataFrame(all_rows)
        trades_df.to_parquet(TRADES_OUT)
        print(f"Saved {len(trades_df)} trade rows -> {TRADES_OUT}")


if __name__ == "__main__":
    asyncio.run(main())
