"""Parse fee, tick, and reward fields from Gamma / CLOB payloads."""

from __future__ import annotations

from typing import Any


def parse_fee_schedule(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(raw, dict) or not raw:
        return None
    return {
        "exponent": raw.get("exponent"),
        "rate": raw.get("rate"),
        "takerOnly": raw.get("takerOnly"),
        "rebateRate": raw.get("rebateRate"),
    }


def resolve_fee_type(fee: dict[str, Any]) -> str:
    """Stable fee_type for DB — never leave NULL after ingest/backfill."""
    raw_type = fee.get("fee_type")
    if raw_type:
        return str(raw_type)
    if fee.get("fees_enabled"):
        return "general_fees"
    return "none"


def gamma_market_fee_fields(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract fee/trading params from a Gamma market record."""
    tick = raw.get("orderPriceMinTickSize") or raw.get("minimum_tick_size")
    min_size = raw.get("orderMinSize") or raw.get("minimum_order_size") or raw.get("min_order_size")
    rewards_min = raw.get("rewardsMinSize") or raw.get("rewards_min_size")
    rewards_max = raw.get("rewardsMaxSpread") or raw.get("rewards_max_spread")

    fee_schedule = raw.get("feeSchedule") or raw.get("fee_schedule")
    if isinstance(fee_schedule, str):
        fee_schedule = None

    return {
        "maker_base_fee": _int_or_none(raw.get("makerBaseFee") or raw.get("maker_base_fee")),
        "taker_base_fee": _int_or_none(raw.get("takerBaseFee") or raw.get("taker_base_fee")),
        "fee_type": _str_or_none(raw.get("feeType") or raw.get("fee_type")),
        "fees_enabled": bool(raw.get("feesEnabled", raw.get("fees_enabled", False))),
        "fee_schedule": parse_fee_schedule(
            fee_schedule if isinstance(fee_schedule, dict) else None
        ),
        "tick_size": _str_or_none(tick),
        "min_order_size": _str_or_none(min_size),
        "rewards_min_size": _float_or_none(rewards_min),
        "rewards_max_spread": _float_or_none(rewards_max),
        "rewards_eligible": bool(
            rewards_min or rewards_max or raw.get("clobRewards") or raw.get("rewardsMinSize")
        ),
        "volume_24h": _float_or_none(
            raw.get("volume24hr") or raw.get("volume24hrClob") or raw.get("volume_24h")
        ),
        "liquidity_usd": _float_or_none(
            raw.get("liquidityNum") or raw.get("liquidityClob") or raw.get("liquidity")
        ),
    }


def clob_market_fee_fields(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract fee/trading params from CLOB GET /markets/{condition_id}."""
    rewards = raw.get("rewards")
    rewards_min = rewards_max = None
    if isinstance(rewards, dict):
        rewards_min = rewards.get("min_size")
        rewards_max = rewards.get("max_spread")

    return {
        "maker_base_fee": _int_or_none(raw.get("maker_base_fee")),
        "taker_base_fee": _int_or_none(raw.get("taker_base_fee")),
        "fee_type": None,
        "fees_enabled": bool(
            raw.get("maker_base_fee") is not None or raw.get("taker_base_fee") is not None
        ),
        "fee_schedule": None,
        "tick_size": _str_or_none(raw.get("minimum_tick_size")),
        "min_order_size": _str_or_none(raw.get("minimum_order_size")),
        "rewards_min_size": _float_or_none(rewards_min),
        "rewards_max_spread": _float_or_none(rewards_max),
        "rewards_eligible": bool(rewards_min or rewards_max),
        "volume_24h": None,
        "liquidity_usd": None,
    }


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


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


def _str_or_none(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None
