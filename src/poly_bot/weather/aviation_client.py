"""NOAA Aviation Weather API — METAR and TAF bulk fetch."""

from __future__ import annotations

from typing import Any

import httpx
import structlog

logger = structlog.get_logger(__name__)

DEFAULT_BASE = "https://aviationweather.gov/api/data"


class AviationWeatherClient:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE,
        timeout: float = 30.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    def fetch_metar(self, icao_ids: list[str]) -> list[dict[str, Any]]:
        return self._fetch("metar", icao_ids)

    def fetch_taf(self, icao_ids: list[str]) -> list[dict[str, Any]]:
        return self._fetch("taf", icao_ids)

    def _fetch(self, product: str, icao_ids: list[str]) -> list[dict[str, Any]]:
        if not icao_ids:
            return []
        ids = ",".join(sorted({i.upper() for i in icao_ids if i}))
        url = f"{self._base}/{product}"
        with httpx.Client(timeout=self._timeout) as client:
            resp = client.get(url, params={"ids": ids, "format": "json"})
            resp.raise_for_status()
            data = resp.json()
        if isinstance(data, list):
            return data
        logger.warning("aviation_unexpected_payload", product=product, type=type(data).__name__)
        return []
