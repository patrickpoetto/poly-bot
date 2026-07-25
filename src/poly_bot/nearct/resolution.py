"""Onchain resolution checker — ConditionalTokens payoutDenominator on Polygon.

The CTF contract is the source of truth for market resolution: every other feed
(Gamma `closed`, CLOB `winner`, our DB) derives from it with 30-50 min of lag.
Measured onchain landing time for crypto 5m/15m markets: ~60-90 s after end_ts.

payoutDenominator(conditionId) > 0  →  market resolved
payoutNumerators(conditionId, i)    →  payout share for outcome i
"""

from __future__ import annotations

import logging

import httpx

log = logging.getLogger(__name__)

CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# keccak4("payoutDenominator(bytes32)") / keccak4("payoutNumerators(bytes32,uint256)")
_SEL_DENOMINATOR = "dd34de67"
_SEL_NUMERATORS = "0504c814"

# Public Polygon RPCs, tried in order after the configured one.
# polygon-rpc.com returns 403 "tenant disabled" as of 2026-07 — do not rely on it.
_FALLBACK_RPCS = (
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
    "https://polygon.drpc.org",
)


def _encode_call(selector: str, condition_id: str, index: int | None = None) -> str:
    data = "0x" + selector + condition_id.removeprefix("0x").rjust(64, "0")
    if index is not None:
        data += hex(index)[2:].rjust(64, "0")
    return data


class OnchainResolver:
    """Checks market resolution directly on the Polygon CTF contract.

    Rotates across multiple RPCs; remembers the last one that worked.
    """

    def __init__(self, rpc_url: str | None = None) -> None:
        rpcs = [rpc_url] if rpc_url else []
        rpcs += [r for r in _FALLBACK_RPCS if r not in rpcs]
        self._rpcs: list[str] = rpcs
        self._active: int = 0

    async def _eth_call(self, client: httpx.AsyncClient, data: str) -> int | None:
        """Return the uint256 result of an eth_call, or None if all RPCs fail."""
        n = len(self._rpcs)
        for attempt in range(n):
            rpc = self._rpcs[(self._active + attempt) % n]
            try:
                r = await client.post(
                    rpc,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_call",
                        "params": [{"to": CTF_ADDRESS, "data": data}, "latest"],
                    },
                )
                r.raise_for_status()
                body = r.json()
                if "result" in body:
                    self._active = (self._active + attempt) % n
                    return int(body["result"], 16)
                log.debug("onchain_rpc_error %s: %s", rpc, body.get("error"))
            except Exception as exc:
                log.debug("onchain_rpc_fail %s: %s", rpc, exc)
        return None

    async def check(self, condition_id: str, outcome_index: int) -> str | None:
        """Return 'won'/'lost' if resolved onchain, None if pending or RPCs down.

        'won' requires our outcome to hold the majority payout — a 50/50 split
        (possible on ties) counts as 'lost', which is the conservative reading
        for a strategy that models wins as full $1 payouts.
        """
        async with httpx.AsyncClient(timeout=httpx.Timeout(6.0)) as client:
            denom = await self._eth_call(client, _encode_call(_SEL_DENOMINATOR, condition_id))
            if not denom:  # 0 = unresolved, None = RPC failure
                return None
            num = await self._eth_call(
                client, _encode_call(_SEL_NUMERATORS, condition_id, outcome_index)
            )
            if num is None:
                return None
            return "won" if num * 2 > denom else "lost"

    def check_sync(self, condition_id: str, outcome_index: int) -> str | None:
        """Blocking variant for thread-pool callers (stuck-order reaper)."""
        n = len(self._rpcs)

        def call(data: str) -> int | None:
            for attempt in range(n):
                rpc = self._rpcs[(self._active + attempt) % n]
                try:
                    r = httpx.post(
                        rpc,
                        json={
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_call",
                            "params": [{"to": CTF_ADDRESS, "data": data}, "latest"],
                        },
                        timeout=6.0,
                    )
                    r.raise_for_status()
                    body = r.json()
                    if "result" in body:
                        self._active = (self._active + attempt) % n
                        return int(body["result"], 16)
                except Exception as exc:
                    log.debug("onchain_rpc_fail %s: %s", rpc, exc)
            return None

        denom = call(_encode_call(_SEL_DENOMINATOR, condition_id))
        if not denom:
            return None
        num = call(_encode_call(_SEL_NUMERATORS, condition_id, outcome_index))
        if num is None:
            return None
        return "won" if num * 2 > denom else "lost"
