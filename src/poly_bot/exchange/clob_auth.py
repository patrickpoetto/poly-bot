"""Authenticated CLOB operations via py-clob-client.

Requires the [trading] extra:  pip install -e ".[trading]"
Falls back gracefully (raises ImportError) when not installed.
"""

from __future__ import annotations

import asyncio
import logging

from poly_bot.settings import Settings

log = logging.getLogger(__name__)


def _build_client(settings: Settings) -> object:
    from py_clob_client.client import ClobClient  # type: ignore[import-untyped]
    from py_clob_client.clob_types import ApiCreds  # type: ignore[import-untyped]

    if not (settings.clob_api_key and settings.clob_secret and settings.clob_passphrase):
        msg = "CLOB credentials not configured (CLOB_API_KEY / CLOB_SECRET / CLOB_PASSPHRASE)"
        raise RuntimeError(msg)

    creds = ApiCreds(
        api_key=settings.clob_api_key,
        api_secret=settings.clob_secret,
        api_passphrase=settings.clob_passphrase,
    )
    pk = settings.private_key.get_secret_value() if settings.private_key else ""
    return ClobClient(
        host="https://clob.polymarket.com",
        key=pk,
        chain_id=137,
        creds=creds,
    )


async def place_market_buy(
    token_id: str,
    price: float,
    size: float,
    settings: Settings | None = None,
) -> str:
    """Place a GTC limit order at `price` for `size` shares.

    Returns the CLOB order ID string.
    Raises on failure (caller logs and records reject_reason).
    """
    if settings is None:
        settings = Settings()  # type: ignore[call-arg]

    def _sync() -> str:
        from py_clob_client.order_builder.constants import BUY  # type: ignore[import-untyped]

        client = _build_client(settings)
        order_args = {
            "token_id": token_id,
            "price": price,
            "size": size,
            "side": BUY,
        }
        resp = client.create_and_post_order(order_args)
        order_id: str = resp.get("orderID") or resp.get("order_id") or ""
        if not order_id:
            raise RuntimeError(f"CLOB returned no order_id: {resp}")
        return order_id

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _sync)
