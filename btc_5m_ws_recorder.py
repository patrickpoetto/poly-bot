"""Records raw WebSocket book/price_change events for every btc-updown-5m
market, going forward — the one data source we've never had. All of this
session's backtesting relies on the REST trade tape (data-api.polymarket.com
/trades), because Polymarket keeps no historical order-book snapshots. That
was enough to validate the entry rule, but it means every question about
resting-quote behavior (how fast does the book actually reprice? does an ask
sit stale for real, or does it track price continuously between the trades
we see? how deep is the book when the hedge would need to execute?) has been
answered from a handful of hand-pulled examples, not real data.

This script only records — no trading, no strategy logic. Reuses the exact
WS connection/discovery pattern from btc_5m_paper_trader.py (proven over
multiple days live: reconnect-on-force pattern for the subscription-bloat
bug, TRACK_TTL cleanup, Gamma discovery with end_date bounding) rather than
importing it, to avoid coupling a live trading process's restart to a
recorder's changes — see CLAUDE.md on the "standalone scripts" layout.

Storage: buffers raw messages in memory, flushes to partitioned parquet
(ws_data/date=YYYY-MM-DD/part-<epoch_ms>.parquet) every FLUSH_INTERVAL_S or
FLUSH_MAX_ROWS, whichever comes first. Schema is deliberately raw
(recv_ts, event_type, asset_id, market, raw_json) instead of pre-flattened
into bid/ask columns — book messages carry full depth (many price levels)
that a flattened schema would silently drop, and we don't yet know which
fields future analysis will need. Downstream code parses raw_json.

Run:
    python btc_5m_ws_recorder.py
    nohup python btc_5m_ws_recorder.py > btc_5m_ws_recorder.log 2>&1 & disown
Output:
    ws_data/date=YYYY-MM-DD/part-*.parquet
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import websockets
from websockets.asyncio.client import ClientConnection

GAMMA_HOST = "https://gamma-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
OUT_DIR = Path(__file__).resolve().parent / "ws_data"

DISCOVERY_INTERVAL_S = 5.0
DISCOVERY_HORIZON_S = 310  # a bit over 5 min — catch markets as soon as they open
TRACK_TTL_S = 60  # keep recording this long after a market's end_ts, same as the trader
FLUSH_INTERVAL_S = 30.0
FLUSH_MAX_ROWS = 20_000


async def fetch_active_btc5m(client: httpx.AsyncClient) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    horizon = now.timestamp() + DISCOVERY_HORIZON_S
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    horizon_iso = datetime.fromtimestamp(horizon, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    resp = await client.get(
        f"{GAMMA_HOST}/markets",
        params={
            "active": "true",
            "closed": "false",
            "order": "endDate",
            "ascending": "true",
            "limit": 30,
            "end_date_min": now_iso,
            "end_date_max": horizon_iso,
        },
        timeout=10.0,
    )
    resp.raise_for_status()
    data = resp.json()
    items = data if isinstance(data, list) else data.get("data", [])
    return [m for m in items if str(m.get("slug", "")).startswith("btc-updown-5m-")]


@dataclass
class TrackedToken:
    token_id: str
    slug: str
    condition_id: str
    outcome: str
    end_ts: int


class Recorder:
    """Same reconnect/subscription-bloat-fix pattern as PriceFeed in
    btc_5m_paper_trader.py, but records every raw message instead of
    tracking only best_ask."""

    def __init__(self) -> None:
        self.subscribed: set[str] = set()
        self._resub = asyncio.Event()
        self._force_reconnect = asyncio.Event()
        self.connected = False
        self.buffer: list[dict[str, Any]] = []
        self.total_recorded = 0

    def subscribe(self, token_ids: list[str]) -> None:
        new = set(token_ids) - self.subscribed
        if new:
            self.subscribed |= new
            self._resub.set()

    def unsubscribe(self, token_ids: list[str]) -> None:
        drop = set(token_ids) & self.subscribed
        if drop:
            self.subscribed -= drop
            self._force_reconnect.set()

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
            WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5
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

    async def _send_sub(self, ws: ClientConnection) -> None:
        if self.subscribed:
            await ws.send(json.dumps({"assets_ids": list(self.subscribed), "type": "market"}))
            print(f"[WS] iscritto a {len(self.subscribed)} token", flush=True)

    def _handle_raw(self, raw: str | bytes) -> None:
        recv_ts = time.time()
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            asset_id = item.get("asset_id") or ""
            market = item.get("market") or ""
            event_type = item.get("event_type") or item.get("type") or "unknown"
            self.buffer.append(
                {
                    "recv_ts": recv_ts,
                    "event_type": str(event_type),
                    "asset_id": str(asset_id),
                    "market": str(market),
                    "raw_json": raw if len(items) == 1 else json.dumps(item),
                }
            )
        self.total_recorded += len(items)


def flush(buffer: list[dict[str, Any]]) -> None:
    if not buffer:
        return
    df = pd.DataFrame(buffer)
    date_str = datetime.now(UTC).strftime("%Y-%m-%d")
    out_dir = OUT_DIR / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    part_path = out_dir / f"part-{int(time.time() * 1000)}.parquet"
    df.to_parquet(part_path, index=False, compression="zstd")
    rel_path = part_path.relative_to(OUT_DIR.parent)
    print(
        f"[{datetime.now(UTC):%H:%M:%S}] flush: {len(buffer)} righe -> {rel_path}",
        flush=True,
    )


async def discover(
    client: httpx.AsyncClient, recorder: Recorder, tracked: dict[str, TrackedToken]
) -> None:
    now_ts = int(datetime.now(UTC).timestamp())
    try:
        markets = await fetch_active_btc5m(client)
    except httpx.HTTPError as exc:
        print(f"[{datetime.now(UTC):%H:%M:%S}] discovery error: {exc}", flush=True)
        return

    new_token_ids: list[str] = []
    for m in markets:
        try:
            end_dt = datetime.fromisoformat(str(m["endDate"]).replace("Z", "+00:00"))
            end_ts = int(end_dt.timestamp())
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
        recorder.subscribe(new_token_ids)
        print(
            f"[{datetime.now(UTC):%H:%M:%S}] +{len(new_token_ids)} nuovi token "
            f"(totale tracciati: {len(tracked)})",
            flush=True,
        )

    stale = [tid for tid, t in tracked.items() if now_ts - t.end_ts > TRACK_TTL_S]
    if stale:
        recorder.unsubscribe(stale)
        for tid in stale:
            del tracked[tid]


async def main() -> None:
    OUT_DIR.mkdir(exist_ok=True)
    print(f"WS recorder avviato. Output: {OUT_DIR}", flush=True)

    recorder = Recorder()
    tracked: dict[str, TrackedToken] = {}
    ws_task = asyncio.create_task(recorder.run())

    last_flush = time.time()
    async with httpx.AsyncClient(headers={"User-Agent": "poly-bot-research/0.1"}) as client:
        try:
            while True:
                await discover(client, recorder, tracked)
                now = time.time()
                if len(recorder.buffer) >= FLUSH_MAX_ROWS or (now - last_flush) >= FLUSH_INTERVAL_S:
                    to_flush, recorder.buffer = recorder.buffer, []
                    flush(to_flush)
                    last_flush = now
                await asyncio.sleep(DISCOVERY_INTERVAL_S)
        finally:
            flush(recorder.buffer)
            ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ws_task


if __name__ == "__main__":
    asyncio.run(main())
