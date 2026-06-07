"""Persistent WebSocket daemon — sharded CLOB market subscriptions.

Each shard owns one socket and at most ``catalog_ws_shard_size`` assets,
mirroring the limit Polymarket's gateway tolerates without dropping us.
The coordinator (``PersistentWSDaemon``) sizes the shard pool from the
current expiring slice and rebalances on subscription drift.
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
from sqlalchemy.exc import OperationalError
from sqlmodel import Session
from websockets.asyncio.client import ClientConnection

from poly_bot.catalog.expiring import subscription_condition_ids, token_ids_for_conditions
from poly_bot.metrics import record_catalog_enrich
from poly_bot.persistence.db import get_engine
from poly_bot.persistence.schema import TokenQuote
from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)

_RECONNECT_MIN = 1.0
_RECONNECT_MAX = 30.0
_FLUSH_INTERVAL = 2.0


class _ShardConnection:
    """One websocket carrying a slice of the asset universe."""

    def __init__(
        self,
        shard_id: int,
        settings: Settings,
        engine: object,
        token_to_condition: dict[str, str],
        token_ids: list[str],
        stop: asyncio.Event,
    ) -> None:
        self._id = shard_id
        self._settings = settings
        self._engine = engine
        self._token_to_condition = token_to_condition
        self._token_ids = list(token_ids)
        self._url = settings.polymarket_ws_market_url
        self._pending: dict[str, dict[str, float | None]] = {}
        self._stop = stop
        self._target_change = asyncio.Event()

    def set_targets(self, token_ids: list[str]) -> bool:
        """Update the asset list. Returns True if the live socket must reset."""
        new_set = sorted(token_ids)
        old_set = sorted(self._token_ids)
        if new_set == old_set:
            return False
        self._token_ids = list(token_ids)
        self._target_change.set()
        return True

    def update_token_map(self, token_to_condition: dict[str, str]) -> None:
        self._token_to_condition = token_to_condition

    async def run(self) -> None:
        backoff = _RECONNECT_MIN
        while not self._stop.is_set():
            try:
                await self._connect_and_consume()
                backoff = _RECONNECT_MIN
            except Exception as exc:
                logger.warning(
                    "ws_shard_reconnect",
                    shard=self._id,
                    error=str(exc),
                    backoff=backoff,
                )
                await asyncio.sleep(backoff + random.random())  # noqa: S311
                backoff = min(backoff * 2, _RECONNECT_MAX)

    async def _connect_and_consume(self) -> None:
        if not self._token_ids:
            await asyncio.sleep(min(self._settings.catalog_ws_refresh_seconds, 30.0))
            return
        async with websockets.connect(
            self._url,
            ping_interval=30,
            ping_timeout=30,
            close_timeout=5,
            max_size=2**22,
        ) as ws:
            await self._subscribe(ws, self._token_ids)
            flush_task = asyncio.create_task(self._flush_loop())
            drift_task = asyncio.create_task(self._drift_watcher(ws))
            try:
                async for raw in ws:
                    if self._stop.is_set():
                        break
                    self._handle_message(raw)
            finally:
                flush_task.cancel()
                drift_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await flush_task
                with contextlib.suppress(asyncio.CancelledError):
                    await drift_task
                self._flush()

    async def _subscribe(self, ws: ClientConnection, token_ids: Iterable[str]) -> None:
        ids = list(token_ids)
        if not ids:
            return
        await ws.send(json.dumps({"assets_ids": ids, "type": "market"}))
        logger.info("ws_shard_subscribed", shard=self._id, count=len(ids))

    async def _drift_watcher(self, ws: ClientConnection) -> None:
        await self._target_change.wait()
        self._target_change.clear()
        logger.info("ws_shard_resubscribe", shard=self._id, count=len(self._token_ids))
        with contextlib.suppress(Exception):
            await ws.close(code=1000, reason="shard resync")

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
                logger.exception("ws_shard_flush_failed", shard=self._id)

    def _flush(self) -> None:
        if not self._pending:
            return
        snapshot = self._pending
        self._pending = {}
        now = datetime.now(UTC)
        ordered = sorted(snapshot.items())

        with Session(self._engine) as session:  # type: ignore[arg-type]
            for attempt in range(3):
                n = 0
                try:
                    with session.no_autoflush:
                        for token_id, q in ordered:
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
                    return
                except OperationalError as exc:
                    session.rollback()
                    if "deadlock" not in str(exc).lower() or attempt == 2:
                        raise
                    logger.warning(
                        "ws_shard_flush_deadlock_retry",
                        shard=self._id,
                        attempt=attempt + 1,
                    )


class PersistentWSDaemon:
    """Coordinator: split the expiring slice across N shards, refresh on drift."""

    def __init__(
        self,
        settings: Settings,
        *,
        hours: float | None = None,
        refresh_seconds: float | None = None,
    ) -> None:
        self._settings = settings
        self._engine = get_engine(settings)
        self._hours = hours if hours is not None else settings.catalog_quote_horizon_hours
        self._refresh_seconds = (
            refresh_seconds if refresh_seconds is not None else settings.catalog_ws_refresh_seconds
        )
        self._stop = asyncio.Event()
        self._shards: list[_ShardConnection] = []
        self._shard_tasks: list[asyncio.Task[None]] = []

    def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        if not self._settings.catalog_ws_enabled:
            logger.info("ws_daemon_disabled")
            return
        logger.info(
            "ws_daemon_start",
            shard_size=self._settings.catalog_ws_shard_size,
            max_shards=self._settings.catalog_ws_max_shards,
            refresh_s=self._refresh_seconds,
            hours=self._hours,
        )
        self._provision_shards()
        try:
            await self._refresh_loop()
        finally:
            await self._cancel_shards()

    def _provision_shards(self) -> None:
        token_ids, token_map = self._load_subscription_state()
        groups = _partition(
            token_ids,
            self._settings.catalog_ws_shard_size,
            self._settings.catalog_ws_max_shards,
        )
        for idx, group in enumerate(groups):
            shard = _ShardConnection(
                shard_id=idx,
                settings=self._settings,
                engine=self._engine,
                token_to_condition=token_map,
                token_ids=group,
                stop=self._stop,
            )
            self._shards.append(shard)
            self._shard_tasks.append(asyncio.create_task(shard.run()))
        logger.info(
            "ws_daemon_shards_ready",
            shards=len(self._shards),
            total_tokens=len(token_ids),
        )

    async def _refresh_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._refresh_seconds)
                return
            except TimeoutError:
                pass
            try:
                self._rebalance()
            except Exception:
                logger.exception("ws_daemon_refresh_failed")

    def _rebalance(self) -> None:
        token_ids, token_map = self._load_subscription_state()
        groups = _partition(
            token_ids,
            self._settings.catalog_ws_shard_size,
            self._settings.catalog_ws_max_shards,
        )
        # ensure shard count matches
        while len(self._shards) < len(groups):
            idx = len(self._shards)
            shard = _ShardConnection(
                shard_id=idx,
                settings=self._settings,
                engine=self._engine,
                token_to_condition=token_map,
                token_ids=groups[idx],
                stop=self._stop,
            )
            self._shards.append(shard)
            self._shard_tasks.append(asyncio.create_task(shard.run()))
        # tear down extras
        while len(self._shards) > len(groups):
            shard = self._shards.pop()
            task = self._shard_tasks.pop()
            shard.set_targets([])
            task.cancel()
        changed = 0
        for shard, group in zip(self._shards, groups, strict=True):
            shard.update_token_map(token_map)
            if shard.set_targets(group):
                changed += 1
        if changed:
            logger.info(
                "ws_daemon_rebalanced",
                shards=len(self._shards),
                resynced=changed,
                total_tokens=len(token_ids),
            )

    def _load_subscription_state(self) -> tuple[list[str], dict[str, str]]:
        with Session(self._engine) as session:
            cids = subscription_condition_ids(
                session,
                self._settings,
                hours=self._hours,
            )
            tids = token_ids_for_conditions(session, cids)
            token_map = _load_token_conditions(session)
        return tids, token_map

    async def _cancel_shards(self) -> None:
        for task in self._shard_tasks:
            task.cancel()
        for task in self._shard_tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._shard_tasks.clear()
        self._shards.clear()


def _partition(token_ids: list[str], shard_size: int, max_shards: int) -> list[list[str]]:
    """Split into exactly ``n_shards`` groups, sizes differing by at most 1.

    ``n_shards`` = ceil(len / shard_size) capped at ``max_shards``. Distributing
    the remainder across the first few shards keeps later refreshes stable
    (no shard is chronically smaller than the others).
    """
    if not token_ids:
        return []
    ordered = sorted(token_ids)
    needed = max(1, (len(ordered) + shard_size - 1) // shard_size)
    n_shards = min(needed, max_shards)
    base, remainder = divmod(len(ordered), n_shards)
    groups: list[list[str]] = []
    start = 0
    for i in range(n_shards):
        size = base + (1 if i < remainder else 0)
        groups.append(ordered[start : start + size])
        start += size
    return groups


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
