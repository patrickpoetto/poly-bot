"""Async Gamma API client with keyset pagination."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import structlog
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from poly_bot.settings import Settings

_MAX_OFFSET = 9_900

logger = structlog.get_logger(__name__)

_DEFAULT_LIMIT = 100
_USER_AGENT = "poly-bot/0.1 (+https://github.com/local/poly_bot)"


class GammaClient:
    """Fetch markets, events, and series from gamma-api.polymarket.com."""

    def __init__(self, settings: Settings) -> None:
        self._base = settings.polymarket_gamma_host.rstrip("/")
        self._timeout = settings.discovery_request_timeout_seconds
        self._headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._base,
            timeout=self._timeout,
            headers=self._headers,
            follow_redirects=True,
        )

    @retry(
        retry=retry_if_exception(
            lambda exc: (
                (isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 429)
                or isinstance(exc, httpx.TransportError)
            )
        ),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=30),
    )
    async def _get_page(
        self,
        client: httpx.AsyncClient,
        path: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        response = await client.get(path, params=params)
        if response.status_code == 429:
            logger.warning("gamma_rate_limited", path=path)
            response.raise_for_status()
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return {"items": data, "next_cursor": None}
        if isinstance(data, dict):
            return data
        return {"items": [], "next_cursor": None}

    async def iter_keyset_pages(
        self,
        path: str,
        items_key: str,
        *,
        closed: bool | None = None,
        limit: int = _DEFAULT_LIMIT,
        extra_params: dict[str, Any] | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """Yield (page_items, next_cursor) from a Gamma keyset endpoint."""
        params: dict[str, Any] = {"limit": limit}
        if closed is not None:
            params["closed"] = str(closed).lower()
        if extra_params:
            params.update(extra_params)

        async with self._client() as client:
            cursor: str | None = None
            page_num = 0
            while True:
                req_params = dict(params)
                if cursor:
                    req_params["after_cursor"] = cursor

                payload = await self._get_page(client, path, req_params)
                items = payload.get(items_key) or payload.get("data") or payload.get("items")
                if items is None and isinstance(payload, list):
                    items = payload
                if not isinstance(items, list):
                    items = []

                next_cursor = (
                    payload.get("next_cursor") or payload.get("nextCursor") or payload.get("cursor")
                )
                page_num += 1
                logger.debug(
                    "gamma_keyset_page",
                    path=path,
                    page=page_num,
                    count=len(items),
                    has_next=bool(next_cursor),
                )
                yield items, next_cursor if isinstance(next_cursor, str) else None

                if not next_cursor or not items:
                    break
                cursor = next_cursor

    async def iter_keyset(
        self,
        path: str,
        items_key: str,
        *,
        closed: bool | None = None,
        limit: int = _DEFAULT_LIMIT,
        extra_params: dict[str, Any] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield individual records from a keyset endpoint."""
        async for page, _ in self.iter_keyset_pages(
            path,
            items_key,
            closed=closed,
            limit=limit,
            extra_params=extra_params,
        ):
            for item in page:
                if isinstance(item, dict):
                    yield item

    # ── Markets ─────────────────────────────────────────────────────────────

    async def iter_markets(
        self,
        *,
        closed: bool | None = False,
        limit: int = _DEFAULT_LIMIT,
        extra_params: dict[str, Any] | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None]]:
        """Yield (page_items, next_cursor) for /markets/keyset."""
        async for page in self.iter_keyset_pages(
            "/markets/keyset",
            "markets",
            closed=closed,
            limit=limit,
            extra_params=extra_params,
        ):
            yield page

    async def iter_active_markets(self) -> AsyncIterator[dict[str, Any]]:
        async for item in self.iter_keyset("/markets/keyset", "markets", closed=False):
            yield item

    async def iter_active_markets_delta(
        self,
        since: datetime,
        *,
        limit: int = _DEFAULT_LIMIT,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield active markets updated after ``since`` (newest first, early stop).

        Uses offset ``/markets`` ordered by ``updatedAt`` desc — stops when a full page
        is older than the watermark (Gamma keyset ignores ``updated_after``).
        """
        since_utc = since.astimezone(UTC)
        offset = 0

        async with self._client() as client:
            while offset <= _MAX_OFFSET:
                try:
                    payload = await self._get_page(
                        client,
                        "/markets",
                        {
                            "limit": limit,
                            "offset": offset,
                            "active": "true",
                            "closed": "false",
                            "order": "updatedAt",
                            "ascending": "false",
                        },
                    )
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 422:
                        logger.warning("gamma_delta_offset_limit", offset=offset)
                        break
                    raise
                items = payload.get("items") or payload.get("data") or []
                if not isinstance(items, list) or not items:
                    break

                page_had_recent = False
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    updated = _parse_gamma_datetime(item.get("updatedAt") or item.get("updated_at"))
                    if updated is not None and updated <= since_utc:
                        return
                    page_had_recent = True
                    yield item

                if not page_had_recent:
                    return
                if len(items) < limit:
                    break
                offset += limit

    async def iter_active_events_delta(
        self,
        since: datetime,
        *,
        limit: int = _DEFAULT_LIMIT,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield active events updated after ``since`` (newest first, early stop)."""
        since_utc = since.astimezone(UTC)
        offset = 0

        async with self._client() as client:
            while offset <= _MAX_OFFSET:
                try:
                    payload = await self._get_page(
                        client,
                        "/events",
                        {
                            "limit": limit,
                            "offset": offset,
                            "active": "true",
                            "closed": "false",
                            "order": "updatedAt",
                            "ascending": "false",
                        },
                    )
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 422:
                        logger.warning("gamma_events_delta_offset_limit", offset=offset)
                        break
                    raise
                items = payload.get("items") or payload.get("data") or []
                if not isinstance(items, list) or not items:
                    break

                page_had_recent = False
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    updated = _parse_gamma_datetime(item.get("updatedAt") or item.get("updated_at"))
                    if updated is not None and updated <= since_utc:
                        return
                    page_had_recent = True
                    yield item

                if not page_had_recent:
                    return
                if len(items) < limit:
                    break
                offset += limit

    async def iter_recently_closed_markets(
        self, lookback_days: int
    ) -> AsyncIterator[dict[str, Any]]:
        cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
        extra: dict[str, Any] = {"end_date_min": cutoff.strftime("%Y-%m-%d")}

        async for item in self.iter_keyset(
            "/markets/keyset",
            "markets",
            closed=True,
            extra_params=extra,
        ):
            end_raw = item.get("endDate") or item.get("end_date")
            if end_raw:
                try:
                    end_dt = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
                    if end_dt < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass
            yield item

    # ── Events ──────────────────────────────────────────────────────────────

    async def iter_active_events(self) -> AsyncIterator[dict[str, Any]]:
        async for item in self.iter_keyset("/events/keyset", "events", closed=False):
            yield item

    async def iter_recently_closed_events(
        self, lookback_days: int
    ) -> AsyncIterator[dict[str, Any]]:
        cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
        extra: dict[str, Any] = {"end_date_min": cutoff.strftime("%Y-%m-%d")}

        async for item in self.iter_keyset(
            "/events/keyset",
            "events",
            closed=True,
            extra_params=extra,
        ):
            end_raw = item.get("endDate") or item.get("end_date")
            if end_raw:
                try:
                    end_dt = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
                    if end_dt < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass
            yield item

    # ── Series ──────────────────────────────────────────────────────────────

    async def iter_series(self) -> AsyncIterator[dict[str, Any]]:
        """All series from Gamma via /series (offset pagination; no keyset API)."""
        offset = 0
        limit = _DEFAULT_LIMIT
        async with self._client() as client:
            while True:
                payload = await self._get_page(
                    client,
                    "/series",
                    {"limit": limit, "offset": offset},
                )
                items = payload.get("series") or payload.get("items") or payload.get("data")
                if isinstance(payload, list):
                    items = payload
                if not isinstance(items, list) or not items:
                    break
                for item in items:
                    if isinstance(item, dict):
                        yield item
                if len(items) < limit:
                    break
                offset += limit


def _parse_gamma_datetime(value: object) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except (ValueError, TypeError):
        return None
