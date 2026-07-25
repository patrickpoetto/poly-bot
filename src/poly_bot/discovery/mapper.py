"""Map Gamma API JSON payloads to SQLModel entities."""

import contextlib
import json
from datetime import datetime
from typing import Any

from poly_bot.catalog.taxonomy import TaxonomyOverrideStore
from poly_bot.discovery.classifier.rules import classify_market
from poly_bot.discovery.fees import gamma_market_fee_fields, resolve_fee_type
from poly_bot.persistence.schema import Event, Market, Series, Token


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _parse_game_start_ts(raw: dict[str, Any], *, end_ts: datetime | None) -> datetime | None:
    """Gamma sports: ``gameStartTime`` is kickoff; ``endDate`` often matches it."""
    kickoff = _parse_iso_datetime(
        raw.get("gameStartTime") or raw.get("game_start_time") or raw.get("gameStart")
    )
    if kickoff is not None:
        return kickoff
    return end_ts


def gamma_market_to_entities(
    raw: dict[str, Any],
    *,
    overrides: TaxonomyOverrideStore | None = None,
) -> tuple[Market, list[Token], list[str]]:
    """Convert a Gamma /markets item into Market + Tokens + tag slugs."""
    condition_id = raw.get("conditionId") or raw.get("condition_id") or ""
    slug = raw.get("slug") or ""
    question = raw.get("question") or raw.get("title") or ""

    fee = gamma_market_fee_fields(raw)
    start_ts = _parse_iso_datetime(raw.get("startDate") or raw.get("start_date"))
    end_ts = _parse_iso_datetime(raw.get("endDate") or raw.get("end_date"))

    classification = classify_market(
        raw,
        slug=slug,
        start_ts=start_ts,
        end_ts=end_ts,
        volume_24h=fee.get("volume_24h"),
        liquidity_usd=fee.get("liquidity_usd"),
        overrides=overrides,
    )
    game_start_ts = (
        _parse_game_start_ts(raw, end_ts=end_ts) if classification["domain"] == "sports" else None
    )

    event_id = _extract_event_id(raw)
    series_id = _extract_series_id(raw) or _series_id_from_nested_event(raw)

    market = Market(
        condition_id=condition_id,
        question_id=str(raw.get("questionID") or raw.get("question_id") or "") or None,
        slug=slug,
        question=question,
        event_id=event_id,
        series_id=series_id,
        structure=classification["structure"],
        domain=classification["domain"],
        horizon=classification["horizon"],
        recurrence=classification["recurrence"],
        resolution_source=classification["resolution_source"],
        liquidity_tier=classification["liquidity_tier"],
        neg_risk=bool(raw.get("negRisk") or raw.get("neg_risk")),
        neg_risk_market_id=str(raw.get("negRiskMarketID") or "") or None,
        active=bool(raw.get("active", True)),
        closed=bool(raw.get("closed", False)),
        accepting_orders=bool(raw.get("acceptingOrders", raw.get("accepting_orders", True))),
        enable_order_book=bool(raw.get("enableOrderBook", raw.get("enable_order_book", True))),
        start_ts=start_ts,
        end_ts=end_ts,
        game_start_ts=game_start_ts,
        tick_size=fee.get("tick_size"),
        min_order_size=fee.get("min_order_size"),
        rewards_eligible=bool(fee.get("rewards_eligible")),
        fees_enabled=bool(fee.get("fees_enabled")),
        fee_type=resolve_fee_type(fee),
        maker_base_fee=fee.get("maker_base_fee"),
        taker_base_fee=fee.get("taker_base_fee"),
        fee_schedule=fee.get("fee_schedule"),
        raw_payload=raw,
    )

    outcomes = _parse_json_list(raw.get("outcomes"))
    token_ids = _parse_json_list(raw.get("clobTokenIds") or raw.get("clob_token_ids"))
    tokens: list[Token] = []
    for i, tid in enumerate(token_ids):
        label = str(outcomes[i]) if i < len(outcomes) else f"outcome_{i}"
        tokens.append(
            Token(
                token_id=str(tid),
                condition_id=condition_id,
                outcome_index=i,
                outcome_label=label,
            )
        )

    tag_slugs: list[str] = []
    for tag in _parse_json_list(raw.get("tags")):
        if isinstance(tag, dict):
            tag_slugs.append(str(tag.get("slug") or tag.get("label") or ""))
        elif isinstance(tag, str):
            tag_slugs.append(tag)

    return market, tokens, [t for t in tag_slugs if t]


def gamma_market_enrichment_fields(raw: dict[str, Any]) -> dict[str, Any]:
    """Fields for market_enrichment row from Gamma (no extra HTTP)."""
    fee = gamma_market_fee_fields(raw)
    schedule = fee.get("fee_schedule")
    fee_rate_bps: int | None = None
    if isinstance(schedule, dict) and schedule.get("rate") is not None:
        with contextlib.suppress(TypeError, ValueError):
            fee_rate_bps = int(float(schedule["rate"]) * 10_000)

    return {
        "volume_24h": fee.get("volume_24h"),
        "fee_rate_bps": fee_rate_bps,
        "maker_base_fee": fee.get("maker_base_fee"),
        "taker_base_fee": fee.get("taker_base_fee"),
        "rewards_min_size": fee.get("rewards_min_size"),
        "rewards_max_spread": fee.get("rewards_max_spread"),
    }


def _extract_event_id(raw: dict[str, Any]) -> str | None:
    event_raw = raw.get("events")
    if isinstance(event_raw, list) and event_raw:
        first = event_raw[0]
        if isinstance(first, dict):
            return str(first.get("id") or first.get("eventId") or "") or None
        if isinstance(first, str):
            return first
    for key in ("eventId", "event_id"):
        val = raw.get(key)
        if val:
            return str(val)
    return None


def _series_id_from_nested_event(raw: dict[str, Any]) -> str | None:
    event_raw = raw.get("events")
    if isinstance(event_raw, list) and event_raw:
        first = event_raw[0]
        if isinstance(first, dict):
            return _extract_series_id(first)
    return None


def _extract_tag_slugs(raw: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    for tag in _parse_json_list(raw.get("tags")):
        if isinstance(tag, dict):
            slug = str(tag.get("slug") or tag.get("label") or "").strip()
            if slug:
                tags.append(slug.lower())
        elif isinstance(tag, str) and tag.strip():
            tags.append(tag.strip().lower())
    return tags


def _extract_series_id(raw: dict[str, Any]) -> str | None:
    for key in ("seriesId", "series_id", "seriesID"):
        val = raw.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    series_obj = raw.get("series")
    if isinstance(series_obj, dict):
        sid = series_obj.get("id")
        if sid is not None:
            return str(sid).strip()
    if isinstance(series_obj, list) and series_obj:
        first = series_obj[0]
        if isinstance(first, dict) and first.get("id") is not None:
            return str(first["id"]).strip()
    return None


def gamma_event_to_entity(raw: dict[str, Any]) -> Event | None:
    """Convert a Gamma /events item into Event."""
    event_id = str(raw.get("id") or raw.get("eventId") or "").strip()
    if not event_id:
        return None

    title = raw.get("title") or raw.get("name") or raw.get("question") or raw.get("subtitle") or ""

    ev_start = _parse_iso_datetime(
        raw.get("startDate") or raw.get("start_date") or raw.get("startTime")
    )
    ev_end = _parse_iso_datetime(raw.get("endDate") or raw.get("end_date") or raw.get("endTime"))
    return Event(
        id=event_id,
        slug=str(raw.get("slug") or ""),
        title=str(title),
        series_id=_extract_series_id(raw),
        start_ts=ev_start,
        end_ts=ev_end,
        game_start_ts=_parse_game_start_ts(raw, end_ts=ev_end),
        active=bool(raw.get("active", True)),
        closed=bool(raw.get("closed", False)),
        tags_json=_extract_tag_slugs(raw),
        raw_payload=raw,
    )


def _parse_recurrence_seconds(raw: dict[str, Any]) -> int | None:
    for key in (
        "recurrenceSeconds",
        "recurrence_seconds",
        "intervalSeconds",
        "interval_seconds",
        "cadenceSeconds",
    ):
        val = raw.get(key)
        if val is not None:
            try:
                return int(val)
            except (TypeError, ValueError):
                pass
    recurrence = raw.get("recurrence")
    if isinstance(recurrence, str):
        mapping = {
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "4h": 14400,
            "daily": 86400,
        }
        return mapping.get(recurrence.lower())
    return None


def gamma_series_to_entity(raw: dict[str, Any]) -> Series | None:
    """Convert a Gamma /series item into Series."""
    series_id = str(raw.get("id") or raw.get("seriesId") or "").strip()
    if not series_id:
        return None

    title = raw.get("title") or raw.get("name") or raw.get("label") or ""

    return Series(
        id=series_id,
        slug=str(raw.get("slug") or ""),
        title=str(title),
        recurrence_seconds=_parse_recurrence_seconds(raw),
        raw_payload=raw,
    )
