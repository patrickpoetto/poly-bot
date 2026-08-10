"""Corrected re-run of btc_5m_multithreshold_scrape.py.

The original script fetched trades with limit=300 PER MARKET, AFTER the
market had already resolved — i.e. after all of its trading volume,
including any late resolution burst, already existed. Sampled on 250
historical markets: for 54% of them, the entire 30-120s-before-close
window was completely outside the 300 most-recent trades (the earliest of
the 300 fetched was already newer than the window's end), and 96.5% had
the window at least partially cut off. Busy/volatile candles — exactly the
ones most likely to have a late reversal — are the ones that generate
300+ trades and get silently dropped from the original sample. The
original "17600 trades, 24 losses, 0.14%" headline number is therefore
measured on a survivorship-biased subsample skewed toward quiet markets.

Fix: limit=2000 instead of 300. Also records whether the window was still
fully covered even at that limit (rare, but the earlier forensic dive
needed 2000 to reach back far enough for one especially busy candle) so we
can see how much residual truncation remains instead of assuming zero.

Run:
    python btc_5m_multithreshold_v2_scrape.py
Output:
    btc_5m_multithreshold_v2_history.parquet
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
OUT = Path(__file__).resolve().parent / "btc_5m_multithreshold_v2_history.parquet"
CONCURRENCY = 10
TRADES_LIMIT = 2000
MIN_SECONDS_LEFT = 30
MAX_SECONDS_LEFT = 120
THRESHOLDS = [0.99, 0.98, 0.97, 0.96, 0.95, 0.94, 0.93, 0.92, 0.91, 0.90, 0.88, 0.85, 0.80]


async def crossings_all_thresholds(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    condition_id: str,
    winning_label: str,
    end_ts: int,
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

    trades = [t for t in trades if t.get("timestamp", 0) <= end_ts]
    trades.sort(key=lambda t: t["timestamp"])

    window_start = end_ts - MAX_SECONDS_LEFT
    window_end = end_ts - MIN_SECONDS_LEFT
    earliest_fetched = trades[0]["timestamp"] if trades else None
    still_truncated = earliest_fetched is not None and earliest_fetched > window_start

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
            if not (MIN_SECONDS_LEFT <= seconds_left <= MAX_SECONDS_LEFT):
                continue
            out.append(
                {
                    "side": label,
                    "threshold": threshold,
                    "entry_price": float(crossing["price"]),
                    "seconds_before_close": seconds_left,
                    "won": label == winning_label,
                    "confirm_size": float(crossing.get("size", 0.0)),
                    "still_truncated": still_truncated,
                }
            )
    if not out and still_truncated:
        # market silently dropped even at limit=2000 — record it so we can
        # measure residual bias instead of pretending it's zero
        out.append(
            {
                "side": None,
                "threshold": None,
                "entry_price": None,
                "seconds_before_close": None,
                "won": None,
                "confirm_size": None,
                "still_truncated": True,
            }
        )
    return out


async def main() -> None:
    events_df = pd.read_parquet(EVENTS_CACHE)
    events_df = events_df.dropna(subset=["condition_id", "outcome", "candle_ts"])
    print(f"Scanning {len(events_df)} markets (limit={TRADES_LIMIT}) for thresholds {THRESHOLDS}...")

    sem = asyncio.Semaphore(CONCURRENCY)
    results: list[dict[str, Any]] = []
    start = time.time()

    async with httpx.AsyncClient(headers={"User-Agent": "poly-bot-research/0.1"}) as client:

        async def one(row: dict[str, Any]) -> None:
            end_ts = int(row["candle_ts"]) + 300
            crossings = await crossings_all_thresholds(
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
    n_truncated = df["still_truncated"].sum() if "still_truncated" in df.columns else 0
    print(f"Saved {len(df)} rows -> {OUT}")
    print(f"Righe ancora troncate anche a limit={TRADES_LIMIT}: {n_truncated}")


if __name__ == "__main__":
    asyncio.run(main())
