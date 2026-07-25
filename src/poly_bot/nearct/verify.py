"""Pre-entry verification against the underlying price feed.

Buying the 0.99 ask blindly is adverse selection: near expiry the ask is often
stale while the underlying has drifted toward the strike. This module checks the
actual distance between spot and strike before every entry.

Resolution sources per market type:
- "X above K":   Binance 1h candle close (exact — description names Binance)
- "X Up or Down": Chainlink data streams (Binance spot is a tight proxy;
                  the distance threshold absorbs the basis)

A candidate passes only if the outcome is already "in the money" by a margin
that scales with the volatility class and the time left (sigma ~ sqrt(t)).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

log = logging.getLogger(__name__)

_BINANCE = "https://data-api.binance.vision"

# Question prefix → Binance symbol. HYPE is not listed on Binance:
# verification fails closed and the candidate is skipped.
_SYMBOLS: dict[str, str] = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "solana": "SOLUSDT",
    "xrp": "XRPUSDT",
    "dogecoin": "DOGEUSDT",
    "bnb": "BNBUSDT",
    "hyperliquid": "HYPEUSDT",
}

_MAJORS = {"BTCUSDT", "ETHUSDT"}

_RE_UPDOWN = re.compile(r"^(\w+)\s+up or down\b", re.IGNORECASE)
_RE_ABOVE = re.compile(r"^(\w+)\s+above\s+([\d,]+(?:\.\d+)?)", re.IGNORECASE)


@dataclass
class VerifyResult:
    ok: bool
    reason: str
    distance_pct: float | None = None  # signed, positive = in our favor
    required_pct: float | None = None


def _required_pct(symbol: str, minutes_left: float, base_major: float, base_alt: float) -> float:
    base = base_major if symbol in _MAJORS else base_alt
    result: float = base * float(max(0.25, minutes_left)) ** 0.5
    return result


async def _spot(client: httpx.AsyncClient, symbol: str) -> float:
    r = await client.get(f"{_BINANCE}/api/v3/ticker/price", params={"symbol": symbol})
    r.raise_for_status()
    price: float = float(r.json()["price"])
    return price


async def _window_open(client: httpx.AsyncClient, symbol: str, start_iso: str) -> float:
    """Open of the 1m candle at the window start = start price of the range."""
    ts = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    ms = int(ts.astimezone(UTC).timestamp() * 1000)
    r = await client.get(
        f"{_BINANCE}/api/v3/klines",
        params={"symbol": symbol, "interval": "1m", "startTime": ms, "limit": 1},
    )
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"no kline for {symbol} at {start_iso}")
    return float(data[0][1])


async def verify_crypto_entry(
    *,
    question: str,
    outcome_label: str,
    event_start_iso: str | None,
    minutes_left: float,
    base_major_pct: float,
    base_alt_pct: float,
) -> VerifyResult:
    """Fail-closed: any parse/fetch problem rejects the candidate."""
    q = question.strip()
    label = outcome_label.strip().lower()

    m_ud = _RE_UPDOWN.match(q)
    m_ab = _RE_ABOVE.match(q)

    if m_ud is None and m_ab is None:
        return VerifyResult(False, "unrecognized_market_type")

    name = (m_ud or m_ab).group(1).lower()  # type: ignore[union-attr]
    symbol = _SYMBOLS.get(name)
    if symbol is None:
        return VerifyResult(False, f"unknown_symbol:{name}")

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(6.0)) as client:
            spot = await _spot(client, symbol)

            if m_ab is not None:
                # "X above K?" — [Yes] needs spot > K, [No] needs spot < K
                strike = float(m_ab.group(2).replace(",", ""))
                signed = (spot - strike) / strike
                if label in ("yes", ""):
                    dist = signed
                elif label == "no":
                    dist = -signed
                else:
                    return VerifyResult(False, f"bad_label_above:{label}")
            else:
                # "X Up or Down" — end price vs start price; ties resolve Up
                if not event_start_iso:
                    return VerifyResult(False, "missing_event_start")
                open_ = await _window_open(client, symbol, event_start_iso)
                signed = (spot - open_) / open_
                if label == "up":
                    dist = signed
                elif label == "down":
                    dist = -signed
                else:
                    return VerifyResult(False, f"bad_label_updown:{label}")

    except Exception as exc:
        log.debug("verify_fetch_error %s: %s", symbol, exc)
        return VerifyResult(False, f"fetch_error:{type(exc).__name__}")

    required = _required_pct(symbol, minutes_left, base_major_pct, base_alt_pct)
    ok = dist >= required
    if not ok:
        log.info(
            "nearct_verify_reject: %s [%s] dist=%.3f%% < required=%.3f%% (%.1f min left)",
            q[:40],
            label,
            dist * 100,
            required * 100,
            minutes_left,
        )
    return VerifyResult(
        ok, "ok" if ok else "distance_too_small", distance_pct=dist, required_pct=required
    )
