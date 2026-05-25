"""Deterministic market classification from Gamma metadata."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from poly_bot.catalog.taxonomy import TaxonomyOverrideStore

# Recurring slug patterns (domain, horizon, cadence_seconds)
_RECURRING_PATTERNS: list[tuple[re.Pattern[str], str, str, int | None]] = [
    (re.compile(r"^btc-updown-5m-\d+$"), "crypto", "ultra_short", 300),
    (re.compile(r"^btc-updown-15m-\d+$"), "crypto", "ultra_short", 900),
    (re.compile(r"^eth-updown-5m-\d+$"), "crypto", "ultra_short", 300),
    (re.compile(r"^eth-updown-15m-\d+$"), "crypto", "ultra_short", 900),
    (re.compile(r"^sol-updown-5m-\d+$"), "crypto", "ultra_short", 300),
    (re.compile(r"^sol-updown-15m-\d+$"), "crypto", "ultra_short", 900),
    (re.compile(r"^xrp-updown-5m-\d+$"), "crypto", "ultra_short", 300),
    (re.compile(r"^xrp-updown-15m-\d+$"), "crypto", "ultra_short", 900),
    (re.compile(r"^[a-z]+-updown-5m-\d+$"), "crypto", "ultra_short", 300),
    (re.compile(r"^[a-z]+-updown-15m-\d+$"), "crypto", "ultra_short", 900),
    (re.compile(r"^[a-z]+-updown-1h-\d+$"), "crypto", "short", 3600),
    (re.compile(r"^[a-z]+-updown-4h-\d+$"), "crypto", "short", 14400),
]

_DOMAIN_TAG_MAP: dict[str, str] = {
    "crypto": "crypto",
    "bitcoin": "crypto",
    "ethereum": "crypto",
    "solana": "crypto",
    "sports": "sports",
    "nba": "sports",
    "nfl": "sports",
    "mlb": "sports",
    "nhl": "sports",
    "soccer": "sports",
    "politics": "politics",
    "elections": "politics",
    "finance": "finance",
    "science": "science",
    "pop-culture": "culture",
    "entertainment": "culture",
}


def _tags_from_payload(raw: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    for key in ("tags", "events"):
        val = raw.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str):
                    tags.append(item.lower())
                elif isinstance(item, dict):
                    slug = item.get("slug") or item.get("label") or ""
                    if slug:
                        tags.append(str(slug).lower())
    return tags


def classify_domain_from_tags(tags: list[str]) -> str:
    """Map tag slugs to a single domain label."""
    for tag in tags:
        tag_l = tag.lower()
        if tag_l in _DOMAIN_TAG_MAP:
            return _DOMAIN_TAG_MAP[tag_l]
        for key, mapped in _DOMAIN_TAG_MAP.items():
            if key in tag_l:
                return mapped
    return "unknown"


def classify_horizon_from_dates(
    start_ts: datetime | None,
    end_ts: datetime | None,
) -> str:
    if start_ts is None or end_ts is None:
        return "unknown"
    if start_ts.tzinfo is None:
        start_ts = start_ts.replace(tzinfo=UTC)
    if end_ts.tzinfo is None:
        end_ts = end_ts.replace(tzinfo=UTC)
    hours = (end_ts - start_ts).total_seconds() / 3600.0
    if hours <= 6:
        return "ultra_short"
    if hours <= 48:
        return "short"
    if hours <= 24 * 14:
        return "medium"
    return "long"


def classify_liquidity_tier(
    raw: dict[str, Any],
    *,
    volume_24h: float | None = None,
    liquidity_usd: float | None = None,
) -> str:
    vol = volume_24h
    if vol is None:
        for key in ("volume24hr", "volume24hrClob", "volumeNum", "volume"):
            vol = _float_or_none(raw.get(key))
            if vol is not None:
                break
    liq = liquidity_usd
    if liq is None:
        liq = _float_or_none(raw.get("liquidityNum") or raw.get("liquidityClob") or raw.get("liquidity"))
    score = max(vol or 0.0, liq or 0.0)
    if score >= 1_000_000:
        return "high"
    if score >= 100_000:
        return "medium"
    if score >= 5_000:
        return "low"
    if score > 0:
        return "thin"
    return "unknown"


def classify_market(
    raw: dict[str, Any],
    *,
    slug: str,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    volume_24h: float | None = None,
    liquidity_usd: float | None = None,
    overrides: TaxonomyOverrideStore | None = None,
) -> dict[str, str]:
    """Return classification fields for a Market row."""
    tags = _tags_from_payload(raw)
    domain = classify_domain_from_tags(tags)

    structure = "binary"
    outcomes = raw.get("outcomes") or []
    if isinstance(outcomes, str):
        import json

        try:
            outcomes = json.loads(outcomes)
        except json.JSONDecodeError:
            outcomes = []
    if isinstance(outcomes, list) and len(outcomes) > 2:
        structure = "multi_outcome"
    if raw.get("negRisk") or raw.get("neg_risk"):
        structure = "neg_risk_basket"

    horizon = classify_horizon_from_dates(start_ts, end_ts)
    recurrence = "one_off"
    slug_lower = slug.lower()
    for pattern, pat_domain, pat_horizon, _cadence in _RECURRING_PATTERNS:
        if pattern.match(slug_lower):
            domain = pat_domain
            horizon = pat_horizon
            recurrence = "recurring"
            break

    resolution_source = "unknown"
    if domain == "crypto" or "chainlink" in slug_lower:
        resolution_source = "chainlink"
    elif domain == "sports":
        resolution_source = "sports_api"
    elif domain == "politics":
        resolution_source = "uma"

    if overrides is not None:
        cid = (
            raw.get("conditionId")
            or raw.get("condition_id")
            or raw.get("id")
        )
        hit = overrides.lookup(
            condition_id=str(cid) if cid else None,
            slug=slug,
            tags=tags,
        )
        if hit is not None:
            if hit.domain:
                domain = hit.domain
            if hit.structure:
                structure = hit.structure
            if hit.resolution_source:
                resolution_source = hit.resolution_source

    return {
        "structure": structure,
        "domain": domain,
        "horizon": horizon,
        "recurrence": recurrence,
        "resolution_source": resolution_source,
        "liquidity_tier": classify_liquidity_tier(
            raw,
            volume_24h=volume_24h,
            liquidity_usd=liquidity_usd,
        ),
    }


def _float_or_none(value: object) -> float | None:
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
