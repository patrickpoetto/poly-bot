"""Tests for Gamma/CLOB fee parsing."""

from poly_bot.discovery.fees import gamma_market_fee_fields


def test_gamma_fee_fields_from_market_payload() -> None:
    raw = {
        "makerBaseFee": 1000,
        "takerBaseFee": 1000,
        "feesEnabled": True,
        "feeType": "general_fees",
        "feeSchedule": {"rate": 0.05, "takerOnly": True, "rebateRate": 0.25},
        "orderPriceMinTickSize": 0.01,
        "orderMinSize": 5,
        "volume24hr": 4146.56,
        "liquidityNum": 18985.88,
        "rewardsMinSize": 10,
        "rewardsMaxSpread": 3,
    }
    fee = gamma_market_fee_fields(raw)
    assert fee["maker_base_fee"] == 1000
    assert fee["taker_base_fee"] == 1000
    assert fee["fees_enabled"] is True
    assert fee["fee_type"] == "general_fees"
    assert fee["tick_size"] == "0.01"
    assert fee["min_order_size"] == "5"
    assert fee["volume_24h"] == 4146.56
    assert fee["fee_schedule"]["rate"] == 0.05
