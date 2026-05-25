"""WebSocket price stream → token_quotes table."""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime

import structlog
import websockets
from sqlalchemy import text
from sqlmodel import Session

from poly_bot.persistence.schema import TokenQuote
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)


class LiveQuoteStream:
    """Subscribe to CLOB market WS and persist best bid/ask."""

    def __init__(self, settings: Settings, session: Session) -> None:
        self._settings = settings
        self._session = session
        self._ws_url = settings.polymarket_ws_market_url
        self._pending: dict[str, dict[str, float | None]] = {}
        self._token_to_condition = _load_token_conditions(session)

    async def run(self, token_ids: list[str], *, duration_seconds: float) -> int:
        if not token_ids or not self._settings.catalog_ws_enabled:
            return 0
        messages = 0
        deadline = asyncio.get_event_loop().time() + duration_seconds
        try:
            async with websockets.connect(
                self._ws_url,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                await ws.send(json.dumps({"assets_ids": token_ids, "type": "market"}))
                flush_task = asyncio.create_task(self._periodic_flush())
                try:
                    while asyncio.get_event_loop().time() < deadline:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                            self._handle_message(raw)
                            messages += 1
                        except asyncio.TimeoutError:
                            continue
                finally:
                    flush_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await flush_task
        except Exception as exc:
            logger.warning("live_ws_error", error=str(exc))
        self._flush()
        return messages

    async def _periodic_flush(self) -> None:
        while True:
            await asyncio.sleep(2.0)
            self._flush()

    def _handle_message(self, raw: str | bytes) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            asset_id = item.get("asset_id") or item.get("market")
            if not asset_id:
                continue
            bid = _to_float(item.get("best_bid"))
            ask = _to_float(item.get("best_ask"))
            if bid is None and ask is None:
                continue
            mid = None
            if bid is not None and ask is not None:
                mid = (bid + ask) / 2.0
            elif bid is not None:
                mid = bid
            elif ask is not None:
                mid = ask
            spread = (ask - bid) if bid is not None and ask is not None else None
            self._pending[str(asset_id)] = {"bid": bid, "ask": ask, "mid": mid, "spread": spread}

    def _flush(self) -> None:
        if not self._pending:
            return
        now = datetime.now(UTC)
        for token_id, q in self._pending.items():
            condition_id = self._token_to_condition.get(token_id)
            if not condition_id:
                continue
            row = self._session.get(TokenQuote, token_id) or TokenQuote(
                token_id=token_id,
                condition_id=condition_id,
            )
            row.bid = q.get("bid")
            row.ask = q.get("ask")
            row.mid = q.get("mid")
            row.spread = q.get("spread")
            row.updated_at = now
            self._session.add(row)
        self._session.commit()
        self._pending.clear()


def _load_token_conditions(session: Session) -> dict[str, str]:
    rows = session.execute(text("SELECT token_id, condition_id FROM tokens")).fetchall()
    return {str(r[0]): str(r[1]) for r in rows}


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None
