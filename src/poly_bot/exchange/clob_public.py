"""Public CLOB REST — prices, spreads (no auth)."""

from __future__ import annotations

import contextlib
from typing import Any

import httpx
import structlog
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)

_CHUNK = 80
_USER_AGENT = "poly-bot/0.1"


class ClobPublicClient:
    def __init__(self, settings: Settings) -> None:
        self._base = settings.polymarket_clob_host.rstrip("/")
        self._timeout = settings.discovery_request_timeout_seconds
        self._headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._base,
            timeout=self._timeout,
            headers=self._headers,
        )

    @retry(
        retry=retry_if_exception(
            lambda exc: (
                (isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429)
                or isinstance(exc, httpx.TransportError)
            )
        ),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=20),
    )
    async def _post_json(self, path: str, body: list[dict[str, Any]]) -> Any:
        async with self._client() as client:
            response = await client.post(path, json=body)
            if response.status_code == 429:
                response.raise_for_status()
            response.raise_for_status()
            return response.json()

    async def fetch_midpoints(self, token_ids: list[str]) -> dict[str, float]:
        """Batch midpoint prices keyed by token_id."""
        result: dict[str, float] = {}
        for i in range(0, len(token_ids), _CHUNK):
            chunk = token_ids[i : i + _CHUNK]
            body = [{"token_id": tid} for tid in chunk]
            try:
                data = await self._post_json("/midpoints", body)
            except httpx.HTTPError as exc:
                logger.warning("clob_midpoints_failed", error=str(exc), chunk=len(chunk))
                continue
            self._merge_price_map(data, result, value_key="mid")
        return result

    async def fetch_market(self, condition_id: str) -> dict[str, Any] | None:
        """Single market metadata (tick size, fees, rewards)."""
        async with self._client() as client:
            try:
                response = await client.get(f"/markets/{condition_id}")
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                data = response.json()
                return data if isinstance(data, dict) else None
            except httpx.HTTPError as exc:
                logger.debug("clob_market_miss", condition_id=condition_id[:16], error=str(exc))
        return None

    async def fetch_spreads(self, token_ids: list[str]) -> dict[str, float]:
        result: dict[str, float] = {}
        for i in range(0, len(token_ids), _CHUNK):
            chunk = token_ids[i : i + _CHUNK]
            body = [{"token_id": tid} for tid in chunk]
            try:
                data = await self._post_json("/spreads", body)
            except httpx.HTTPError as exc:
                logger.warning("clob_spreads_failed", error=str(exc), chunk=len(chunk))
                continue
            self._merge_price_map(data, result, value_key="spread")
        return result

    async def fetch_book(self, token_id: str) -> dict[str, Any] | None:
        """Full order book for one token."""
        async with self._client() as client:
            try:
                response = await client.get("/book", params={"token_id": token_id})
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                data = response.json()
                return data if isinstance(data, dict) else None
            except httpx.HTTPError as exc:
                logger.debug("clob_book_miss", token_id=token_id[:16], error=str(exc))
        return None

    async def fetch_books_best(
        self, token_ids: list[str]
    ) -> dict[str, tuple[float | None, float | None]]:
        """Best bid/ask per token via /book."""
        out: dict[str, tuple[float | None, float | None]] = {}
        for tid in token_ids:
            data = await self.fetch_book(tid)
            if not data:
                continue
            bids = self._book_levels(data.get("bids"))
            asks = self._book_levels(data.get("asks"))
            best_bid = bids[-1][0] if bids else None
            best_ask = asks[0][0] if asks else None
            out[tid] = (best_bid, best_ask)
        return out

    @staticmethod
    def _book_levels(raw: object) -> list[tuple[float, float]]:
        if not isinstance(raw, list):
            return []
        out: list[tuple[float, float]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                price = float(item.get("price") or 0)
                size = float(item.get("size") or item.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            if price > 0 and size > 0:
                out.append((price, size))
        out.sort(key=lambda x: x[0])
        return out

    @staticmethod
    def _merge_price_map(data: Any, result: dict[str, float], *, value_key: str) -> None:
        if isinstance(data, dict):
            for tid, val in data.items():
                if isinstance(val, dict):
                    raw = val.get(value_key) or val.get("price")
                else:
                    raw = val
                if raw is not None:
                    with contextlib.suppress(TypeError, ValueError):
                        result[str(tid)] = float(raw)
            return
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                tid = str(item.get("token_id") or item.get("asset_id") or "")
                raw = item.get(value_key) or item.get("price") or item.get("mid")
                if tid and raw is not None:
                    with contextlib.suppress(TypeError, ValueError):
                        result[tid] = float(raw)
