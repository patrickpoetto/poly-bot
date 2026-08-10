"""One-off: for every closed btc-updown-5m market, find the first moment EACH
side's price crossed into "near-certain" territory (>=0.99, matching the
NearCT entry rule), how much time was left at that point, and whether that
specific side went on to actually win — the real adverse-selection question,
not "what was the winner's price one second before close" (tautological:
by then the outcome is already locked in).

Two phases:
  1. Re-crawl Gamma /events for series_id=10684 (btc-up-or-down-5m), this time
     capturing conditionId + clobTokenIds (missing from the existing
     btc_updown_5m.parquet, which only has the final outcome).
  2. For each closed market, pull recent trades from data-api, and for BOTH
     outcomes separately, find the first trade at price >= ENTRY_THRESHOLD —
     that's the moment a NearCT-style entry rule would have bought — then
     check whether that side actually won.

Run:
    python btc_5m_flip_scrape.py
Output:
    btc_5m_entry_history.parquet — one row per side that crossed the
    threshold: slug, side, entry_price, seconds_before_close_at_entry, won
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
EVENTS_OUT = Path("btc_5m_events_enriched.parquet")
ENTRIES_OUT = Path("btc_5m_entry_history.parquet")
CONCURRENCY = 20
TRADES_LIMIT = 300
ENTRY_THRESHOLD = 0.99


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

    token_ids = m.get("clobTokenIds")
    if isinstance(token_ids, str):
        token_ids = json.loads(token_ids)
    outcomes = m.get("outcomes")
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    return {
        "candle_ts": candle_ts,
        "end_date": m.get("endDate"),
        "slug": slug,
        "outcome": outcome,
        "up": outcome == "Up" if outcome else None,
        "volume": float(m.get("volume") or 0),
        "condition_id": m.get("conditionId"),
        "outcomes": outcomes,
        "token_ids": token_ids,
    }


async def crawl_events(client: httpx.AsyncClient) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    cursor: str | None = None
    page = 0
    while True:
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
        if page % 20 == 0:
            print(f"  events page {page}, {len(rows)} rows so far", flush=True)
        await asyncio.sleep(0.15)
    return pd.DataFrame(rows)


async def first_crossings(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    condition_id: str,
    winning_label: str,
    end_ts: int,
) -> list[dict[str, Any]]:
    """First trade on EACH side that crossed ENTRY_THRESHOLD, and whether that side won."""
    async with sem:
        for attempt in range(3):
            try:
                r = await client.get(
                    f"{DATA_API}/trades",
                    params={"market": condition_id, "limit": TRADES_LIMIT},
                    timeout=15,
                )
                r.raise_for_status()
                trades = r.json()
                break
            except httpx.HTTPError:
                if attempt == 2:
                    return []
                await asyncio.sleep(0.5 * (attempt + 1))

    trades = [t for t in trades if t.get("timestamp", 0) <= end_ts]
    trades.sort(key=lambda t: t["timestamp"])  # ascending — earliest first

    out: list[dict[str, Any]] = []
    for label in {t.get("outcome") for t in trades if t.get("outcome")}:
        side_trades = [t for t in trades if t.get("outcome") == label]
        crossing = next((t for t in side_trades if float(t["price"]) >= ENTRY_THRESHOLD), None)
        if crossing is None:
            continue
        out.append(
            {
                "side": label,
                "entry_price": float(crossing["price"]),
                "entry_ts": crossing["timestamp"],
                "seconds_before_close": end_ts - crossing["timestamp"],
                "won": label == winning_label,
                "n_trades_seen": len(trades),
            }
        )
    return out


async def main() -> None:
    async with httpx.AsyncClient(headers={"User-Agent": "poly-bot-research/0.1"}) as client:
        if EVENTS_OUT.exists():
            print(f"Loading cached events from {EVENTS_OUT}")
            events_df = pd.read_parquet(EVENTS_OUT)
        else:
            print("Phase 1: crawling events with conditionId/tokens...")
            events_df = await crawl_events(client)
            events_df.to_parquet(EVENTS_OUT)
            print(f"  saved {len(events_df)} events -> {EVENTS_OUT}")

        events_df = events_df.dropna(subset=["condition_id", "outcome", "candle_ts"])
        print(f"Phase 2: fetching trades for {len(events_df)} markets (both sides)...")

        sem = asyncio.Semaphore(CONCURRENCY)
        results: list[dict[str, Any]] = []
        start = time.time()

        async def one(row: dict[str, Any]) -> None:
            end_ts = int(row["candle_ts"]) + 300
            crossings = await first_crossings(
                client, sem, row["condition_id"], row["outcome"], end_ts
            )
            for c in crossings:
                results.append({"slug": row["slug"], "condition_id": row["condition_id"], **c})

        tasks = [one(row) for row in events_df.to_dict("records")]
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            await coro
            if i % 1000 == 0:
                elapsed = time.time() - start
                rate = i / elapsed
                eta_min = (len(tasks) - i) / rate / 60
                print(f"  {i}/{len(tasks)} done, {rate:.0f}/s, ETA {eta_min:.1f}min", flush=True)

        df = pd.DataFrame(results)
        df.to_parquet(ENTRIES_OUT)
        print(f"Saved {len(df)} entry-crossing rows -> {ENTRIES_OUT}")
        print(f"Markets with >=1 crossing: {df['slug'].nunique()} / {len(events_df)}")


if __name__ == "__main__":
    asyncio.run(main())
