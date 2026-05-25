"""Public Data API — open interest, volume."""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from poly_bot.settings import Settings

logger = structlog.get_logger(__name__)
_USER_AGENT = "poly-bot/0.1"


class DataPublicClient:
    def __init__(self, settings: Settings) -> None:
        self._base = settings.polymarket_data_host.rstrip("/")
        self._timeout = settings.discovery_request_timeout_seconds
        self._headers = {"User-Agent": _USER_AGENT, "Accept": "application/json"}

    async def fetch_open_interest(self, condition_id: str) -> float | None:
        """Open interest for a market (condition id)."""
        async with httpx.AsyncClient(
            base_url=self._base,
            timeout=self._timeout,
            headers=self._headers,
        ) as client:
            try:
                response = await client.get("/oi", params={"market": condition_id})
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                return _parse_oi_payload(response.json())
            except httpx.HTTPError as exc:
                logger.debug("data_oi_miss", condition_id=condition_id[:16], error=str(exc))
        return None


def _parse_oi_payload(data: Any) -> float | None:
    """Parse /oi response: list[{market, value}] or scalar/dict."""
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                raw = item.get("value") or item.get("openInterest") or item.get("oi")
                if raw is not None:
                    return float(raw)
        return None
    if isinstance(data, (int, float)):
        return float(data)
    if isinstance(data, dict):
        for key in ("value", "openInterest", "open_interest", "oi"):
            if key in data and data[key] is not None:
                return float(data[key])
    return None
