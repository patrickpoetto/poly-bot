"""Polymarket fee model — per-share costs for trading PnL."""

from __future__ import annotations

from typing import Any

# Protocol taker fee rates by market category (docs.polymarket.com/trading/fees)
_FEE_TYPE_RATES: dict[str, float] = {
    "crypto": 0.07,
    "sports": 0.03,
    "finance": 0.04,
    "politics": 0.04,
    "economics": 0.05,
    "culture": 0.05,
    "general_fees": 0.05,
    "mentions": 0.04,
    "tech": 0.04,
}

_DEFAULT_MAKER_REBATE_FRAC = 0.25


def effective_fee_rate(
    *,
    fee_type: str | None,
    fee_schedule: dict[str, Any] | None,
    domain: str | None = None,
) -> float | None:
    """Curve fee rate (Θ) for fee = C × rate × p × (1-p)."""
    if isinstance(fee_schedule, dict):
        raw = fee_schedule.get("rate")
        if raw is not None:
            try:
                return float(raw)
            except (TypeError, ValueError):
                pass
    if fee_type:
        key = fee_type.lower().strip()
        if key in _FEE_TYPE_RATES:
            return _FEE_TYPE_RATES[key]
    if domain and domain in _FEE_TYPE_RATES:
        return _FEE_TYPE_RATES[domain]
    return None


def taker_fee_per_share(price: float, fee_rate: float) -> float:
    """Taker fee in USDC per share at price p."""
    p = max(0.0, min(1.0, price))
    return fee_rate * p * (1.0 - p)


def maker_rebate_per_share(
    price: float,
    fee_rate: float,
    *,
    rebate_frac: float | None = None,
    fee_schedule: dict[str, Any] | None = None,
) -> float:
    """Maker rebate credit per share (positive = you receive)."""
    frac = rebate_frac
    if frac is None and isinstance(fee_schedule, dict):
        raw = fee_schedule.get("rebateRate")
        if raw is not None:
            try:
                frac = float(raw)
            except (TypeError, ValueError):
                pass
    if frac is None:
        frac = _DEFAULT_MAKER_REBATE_FRAC
    return taker_fee_per_share(price, fee_rate) * frac


def fee_summary(
    *,
    price: float | None,
    fees_enabled: bool,
    fee_type: str | None,
    fee_schedule: dict[str, Any] | None,
    domain: str | None = None,
    maker_base_fee: int | None = None,
    taker_base_fee: int | None = None,
) -> dict[str, Any]:
    """Human-readable fee breakdown for UI / CLI."""
    rate = effective_fee_rate(
        fee_type=fee_type,
        fee_schedule=fee_schedule,
        domain=domain,
    )
    taker_ps: float | None = None
    maker_rebate_ps: float | None = None
    if price is not None and rate is not None and fees_enabled:
        taker_ps = taker_fee_per_share(price, rate)
        maker_rebate_ps = maker_rebate_per_share(price, rate, fee_schedule=fee_schedule)

    return {
        "fees_enabled": fees_enabled,
        "fee_type": fee_type,
        "fee_rate": rate,
        "taker_per_share": taker_ps,
        "taker_per_100_shares": (taker_ps * 100) if taker_ps is not None else None,
        "maker_rebate_per_share": maker_rebate_ps,
        "maker_base_fee_bps": maker_base_fee,
        "taker_base_fee_bps": taker_base_fee,
        "fee_schedule": fee_schedule,
    }


def format_taker_fee(price: float | None, summary: dict[str, Any]) -> str:
    """Short label for tables, e.g. '$0.12 @0.50' or '—'."""
    if not summary.get("fees_enabled"):
        return "no fee"
    taker = summary.get("taker_per_share")
    if taker is None or price is None:
        rate = summary.get("fee_rate")
        if rate is not None:
            return f"{rate * 100:.1f}% curve"
        return "?"
    return f"${taker:.3f}@{price:.2f}"
