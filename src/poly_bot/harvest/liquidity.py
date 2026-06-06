"""Order-book depth helpers for harvest sizing."""

from __future__ import annotations


def parse_book_levels(raw: list[object] | None) -> list[tuple[float, float]]:
    """CLOB book levels → (price, size_shares) sorted by price ascending."""
    if not raw:
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


def fillable_usd_at_ask(
    asks: list[tuple[float, float]],
    *,
    order_usd: float,
    max_price: float,
) -> tuple[float, float | None]:
    """How much notional (USD) fillable at ask ≤ max_price; VWAP ask for that fill."""
    if order_usd <= 0 or not asks:
        return 0.0, None
    budget = order_usd
    spent = 0.0
    shares = 0.0
    for price, size in asks:
        if price > max_price:
            break
        level_usd = price * size
        take_usd = min(level_usd, budget)
        if take_usd <= 0:
            continue
        take_shares = take_usd / price
        spent += take_usd
        shares += take_shares
        budget -= take_usd
        if budget <= 1e-9:
            break
    if shares <= 0:
        return 0.0, None
    return spent, spent / shares
