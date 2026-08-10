"""Sanity check requested directly: does the low loss-rate at threshold 0.80
(and 0.99) hold ONLY inside the 30-120s window, or does it look the same
across the WHOLE candle (0-300s)? No window filter here — record the first
crossing of each threshold per side wherever it falls, then bucket by
seconds_left in analysis. If 0-30s and 120-300s show similar (low) loss
rates to 30-120s, something is wrong with the earlier methodology. If they
show worse loss rates (as the original 0.99-only breakdown suggested),
that's consistency, not a bug.

Reuses cached events (btc_5m_events_enriched.parquet) — no re-crawl.

Run:
    python btc_5m_nowindow_scrape.py
Output:
    btc_5m_nowindow_history.parquet
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

DATA_API = "https://data-api.polymarket.com"
EVENTS_CACHE = Path(__file__).resolve().parent / "btc_5m_events_enriched.parquet"
OUT = Path(__file__).resolve().parent / "btc_5m_nowindow_history.parquet"
CONCURRENCY = 20
TRADES_LIMIT = 300
THRESHOLDS = [0.99, 0.95, 0.90, 0.80]


async def first_crossings_no_window(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    condition_id: str,
    winning_label: str,
    end_ts: int,
) -> list[dict[str, Any]]:
    """First crossing of each threshold per side, ANYWHERE in the candle —
    no 30-120s restriction. seconds_left recorded as-is for bucketing later."""
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
    trades.sort(key=lambda t: t["timestamp"])

    out: list[dict[str, Any]] = []
    labels = {t.get("outcome") for t in trades if t.get("outcome")}
    for label in labels:
        side_trades = [t for t in trades if t.get("outcome") == label]
        for threshold in THRESHOLDS:
            crossing = next(
                (t for t in side_trades if float(t["price"]) >= threshold), None
            )
            if crossing is None:
                continue
            seconds_left = end_ts - crossing["timestamp"]
            if seconds_left < 0 or seconds_left > 300:
                continue  # data artifact (same anomaly filtered before)
            out.append(
                {
                    "side": label,
                    "threshold": threshold,
                    "entry_price": float(crossing["price"]),
                    "seconds_before_close": seconds_left,
                    "won": label == winning_label,
                }
            )
    return out


async def main() -> None:
    events_df = pd.read_parquet(EVENTS_CACHE)
    events_df = events_df.dropna(subset=["condition_id", "outcome", "candle_ts"])
    print(f"Scanning {len(events_df)} markets, no window filter, thresholds {THRESHOLDS}...")

    sem = asyncio.Semaphore(CONCURRENCY)
    results: list[dict[str, Any]] = []
    start = time.time()

    async with httpx.AsyncClient(headers={"User-Agent": "poly-bot-research/0.1"}) as client:

        async def one(row: dict[str, Any]) -> None:
            end_ts = int(row["candle_ts"]) + 300
            crossings = await first_crossings_no_window(
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
    df.to_parquet(OUT)
    print(f"Saved {len(df)} rows -> {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
