"""Fee calculator tests."""

import pytest

from poly_bot.core.fees import (
    effective_fee_rate,
    fee_summary,
    maker_rebate_per_share,
    taker_fee_per_share,
)


def test_taker_fee_symmetric() -> None:
    assert taker_fee_per_share(0.5, 0.07) == 0.0175
    assert taker_fee_per_share(0.3, 0.07) == pytest.approx(taker_fee_per_share(0.7, 0.07))


def test_effective_rate_from_schedule() -> None:
    rate = effective_fee_rate(
        fee_type="general_fees",
        fee_schedule={"rate": 0.05, "rebateRate": 0.25},
    )
    assert rate == 0.05


def test_fee_summary() -> None:
    s = fee_summary(
        price=0.5,
        fees_enabled=True,
        fee_type="crypto",
        fee_schedule=None,
        domain="crypto",
    )
    assert s["fee_rate"] == 0.07
    assert s["taker_per_share"] == 0.0175
    assert s["taker_per_100_shares"] == pytest.approx(1.75)


def test_maker_rebate() -> None:
    rebate = maker_rebate_per_share(0.5, 0.05, fee_schedule={"rebateRate": 0.25})
    assert rebate == taker_fee_per_share(0.5, 0.05) * 0.25
