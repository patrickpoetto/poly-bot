"""Persistent WebSocket daemon — long-lived CLOB market subscription.

Replaces the per-cycle burst pattern in ``CatalogWatch``: keeps one connection
open, auto-reconnects with backoff, and refreshes the subscription set every
``refresh_seconds`` as the expiring window shifts.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
from collections.abc import Iterable
from datetime import UTC, datetime

import structlog
import websockets
from sqlalchemy import text
from websockets.asyncio.client import ClientConnection
from sqlmodel import Session

from poly_bot.catalog.expiring import expiring_condition_ids, token_ids_for_conditions
from poly_bot.metrics import record_catalog_enrich
from poly_bot.persistence.db import get_engine
from poly_bot.persistence.schema import TokenQuote
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)

_RECONNECT_MIN = 1.0
_RECONNECT_MAX = 30.0
_FLUSH_INTERVAL = 2.0


class PersistentWSDaemon:
    """Maintain a CLOB market WebSocket for the expiring slice — 24/7."""

    def __init__(
        self,
        settings: Settings,
        *,
        hours: float | None = None,
        refresh_seconds: float = 120.0,
    ) -> None:
        self._settings = settings
        self._engine = get_engine(settings)
        self._hours = hours if hours is not None else settings.catalog_expiring_hours
        self._refresh_seconds = refresh_seconds
        self._url = settings.polymarket_ws_market_url
        self._subscribed: set[str] = set()
        self._pending: dict[str, dict[str, float | None]] = {}
        self._token_to_condition: dict[str, str] = {}
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        if not self._settings.catalog_ws_enabled:
            logger.info("ws_daemon_disabled")
            return
        backoff = _RECONNECT_MIN
        logger.info("ws_daemon_start", url=self._url, refresh_s=self._refresh_seconds)
        while not self._stop.is_set():
            try:
                await self._connect_and_consume()
                backoff = _RECONNECT_MIN
            except Exception as exc:
                logger.warning("ws_daemon_reconnect", error=str(exc), backoff=backoff)
                await asyncio.sleep(backoff + random.random())  # noqa: S311
                backoff = min(backoff * 2, _RECONNECT_MAX)

    async def _connect_and_consume(self) -> None:
        token_ids = self._refresh_subscription_set()
        if not token_ids:
            logger.info("ws_daemon_no_tokens", hours=self._hours)
            await asyncio.sleep(min(self._refresh_seconds, 30.0))
            return

        async with websockets.connect(
            self._url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            await self._subscribe(ws, token_ids)
            flush_task = asyncio.create_task(self._flush_loop())
            refresh_task = asyncio.create_task(self._refresh_loop(ws))
            try:
                async for raw in ws:
                    if self._stop.is_set():
                        break
                    self._handle_message(raw)
            finally:
                flush_task.cancel()
                refresh_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await flush_task
                with contextlib.suppress(asyncio.CancelledError):
                    await refresh_task
                self._flush()

    async def _subscribe(self, ws: ClientConnection, token_ids: Iterable[str]) -> None:
        ids = list(token_ids)
        if not ids:
            return
        await ws.send(json.dumps({"assets_ids": ids, "type": "market"}))
        self._subscribed = set(ids)
        logger.info("ws_daemon_subscribed", count=len(ids))

    def _refresh_subscription_set(self) -> list[str]:
        with Session(self._engine) as session:
            cids = expiring_condition_ids(session, hours=self._hours)
            tids = token_ids_for_conditions(session, cids)
            self._token_to_condition = _load_token_conditions(session)
        return tids

    async def _refresh_loop(self, ws: ClientConnection) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self._refresh_seconds)
            try:
                token_ids = self._refresh_subscription_set()
            except Exception:
                logger.exception("ws_daemon_refresh_failed")
                continue
            new_set = set(token_ids)
            added = new_set - self._subscribed
            removed = self._subscribed - new_set
            if not added and not removed:
                continue
            # Polymarket CLOB WS has no incremental subscribe — reconnect
            # by closing; outer loop reopens with refreshed set.
            logger.info(
                "ws_daemon_subscription_drift",
                added=len(added),
                removed=len(removed),
            )
            await ws.close(code=1000, reason="subscription refresh")
            return

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
            mid = (bid + ask) / 2.0 if (bid is not None and ask is not None) else (bid or ask)
            spread = (ask - bid) if (bid is not None and ask is not None) else None
            self._pending[str(asset_id)] = {
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread": spread,
            }

    async def _flush_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(_FLUSH_INTERVAL)
            try:
                self._flush()
            except Exception:
                logger.exception("ws_daemon_flush_failed")

    def _flush(self) -> None:
        if not self._pending:
            return
        snapshot = self._pending
        self._pending = {}
        now = datetime.now(UTC)
        with Session(self._engine) as session:
            n = 0
            for token_id, q in snapshot.items():
                condition_id = self._token_to_condition.get(token_id)
                if not condition_id:
                    continue
                row = session.get(TokenQuote, token_id) or TokenQuote(
                    token_id=token_id,
                    condition_id=condition_id,
                )
                row.bid = q.get("bid")
                row.ask = q.get("ask")
                row.mid = q.get("mid")
                row.spread = q.get("spread")
                row.updated_at = now
                session.add(row)
                n += 1
            session.commit()
        record_catalog_enrich("ws_quote", n)


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
