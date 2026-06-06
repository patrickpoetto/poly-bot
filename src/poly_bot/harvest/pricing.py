"""Net yield after taker fees for high-probability buys."""

from __future__ import annotations

from poly_bot.core.fees import effective_fee_rate, taker_fee_per_share


def buy_yield_pct(
    price: float,
    *,
    fees_enabled: bool,
    fee_type: str | None,
    fee_schedule: dict[str, object] | None,
    domain: str | None,
    event_fees_enabled: bool = False,
    event_fee_type: str | None = None,
    event_fee_schedule: dict[str, object] | None = None,
) -> tuple[float, float, float]:
    """Return (gross_yield_pct, net_yield_pct, taker_fee_per_share).

    Models buying one share at ``price`` (mid/ask proxy), paying taker fee, resolving to $1.
    """
    if price <= 0.0 or price >= 1.0:
        return 0.0, 0.0, 0.0

    gross = (1.0 - price) / price
    fee = 0.0
    enabled = fees_enabled or event_fees_enabled
    if enabled:
        rate = effective_fee_rate(
            fee_type=fee_type or event_fee_type,
            fee_schedule=fee_schedule or event_fee_schedule,
            domain=domain,
        )
        if rate is not None:
            fee = taker_fee_per_share(price, rate)

    cost = price + fee
    net = (1.0 - cost) / cost if cost > 0 else 0.0
    return gross, net, fee


def passes_harvest_price_band(
    price: float,
    *,
    min_buy_price: float,
    max_buy_price: float,
) -> bool:
    return min_buy_price <= price <= max_buy_price
