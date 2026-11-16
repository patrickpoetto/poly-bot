"""Unit tests for the pure, bug-prone logic in btc_5m_paper_trader.py.

Covers the two pieces of core strategy logic that have actually broken in
this project's history (see module docstrings / git log for the full
story):

- `_net_yield`: the fee/EV formula. An earlier version of this exact
  function used `feeRate * min(p, 1-p)` instead of the correct
  `feeRate * p * (1-p)` for an entire session before being caught — see
  the Italian comment in `_net_yield` itself.
- `confirm_recent_trade_ws`: the WS-trade-tape confirmation signal (the
  project's central "trade-tape, not book-quote" design decision), plus
  the pre-entry activity/balance filters built on top of it.

Run:
    pip install -e ".[dev]"
    pytest test_paper_trader.py -v
"""

from __future__ import annotations

import pytest

from btc_5m_paper_trader import PriceFeed, _net_yield, confirm_recent_trade_ws


class TestNetYield:
    def test_known_fee_formula(self) -> None:
        # fee = feeRate * p * (1-p); cost = p + fee; yield = (1-cost)/cost
        price = 0.80
        fee_rate = 0.07
        expected_fee = fee_rate * price * (1 - price)
        expected_cost = price + expected_fee
        expected_yield = (1 - expected_cost) / expected_cost
        assert _net_yield(price) == pytest.approx(expected_yield)

    def test_not_the_old_buggy_formula(self) -> None:
        # Regression guard: the old (wrong) formula was
        # feeRate * min(p, 1-p), not feeRate * p * (1-p). At p=0.80 these
        # formulas diverge (0.07*0.16=0.0112 vs 0.07*0.20=0.014) — make
        # sure we never silently regress back to it.
        price = 0.80
        fee_rate = 0.07
        buggy_fee = fee_rate * min(price, 1 - price)
        buggy_cost = price + buggy_fee
        buggy_yield = (1 - buggy_cost) / buggy_cost
        assert _net_yield(price) != pytest.approx(buggy_yield)

    def test_higher_price_gives_lower_yield(self) -> None:
        # Monotonicity sanity check: paying more for the same $1 payout
        # should never be better.
        assert _net_yield(0.95) < _net_yield(0.80)

    def test_yield_is_positive_for_reasonable_entry_price(self) -> None:
        assert _net_yield(0.80) > 0


class TestConfirmRecentTradeWs:
    def _feed_with_trades(self, condition_id: str, trades: list[dict]) -> PriceFeed:
        feed = PriceFeed()
        feed.trades[condition_id] = trades
        return feed

    def test_no_trades_returns_none(self) -> None:
        feed = PriceFeed()
        assert confirm_recent_trade_ws(feed, "cond1", "Up", window_start_ts=1000.0) is None

    def test_trade_below_threshold_is_ignored(self) -> None:
        feed = self._feed_with_trades(
            "cond1", [{"outcome": "Up", "price": 0.5, "size": 10.0, "timestamp": 1010.0}]
        )
        assert confirm_recent_trade_ws(feed, "cond1", "Up", window_start_ts=1000.0) is None

    def test_trade_before_window_is_ignored(self) -> None:
        feed = self._feed_with_trades(
            "cond1", [{"outcome": "Up", "price": 0.85, "size": 10.0, "timestamp": 990.0}]
        )
        assert confirm_recent_trade_ws(feed, "cond1", "Up", window_start_ts=1000.0) is None

    def test_trade_on_other_side_is_ignored(self) -> None:
        feed = self._feed_with_trades(
            "cond1", [{"outcome": "Down", "price": 0.85, "size": 10.0, "timestamp": 1010.0}]
        )
        assert confirm_recent_trade_ws(feed, "cond1", "Up", window_start_ts=1000.0) is None

    def test_picks_earliest_qualifying_trade(self) -> None:
        feed = self._feed_with_trades(
            "cond1",
            [
                {"outcome": "Up", "price": 0.90, "size": 5.0, "timestamp": 1020.0},
                {"outcome": "Up", "price": 0.85, "size": 7.0, "timestamp": 1005.0},  # earliest
                {"outcome": "Up", "price": 0.82, "size": 3.0, "timestamp": 1030.0},
            ],
        )
        result = confirm_recent_trade_ws(feed, "cond1", "Up", window_start_ts=1000.0)
        assert result is not None
        confirming_trade, _n_trades_pre_entry, _balance = result
        assert confirming_trade["timestamp"] == 1005.0
        assert confirming_trade["price"] == 0.85

    def test_balance_and_count_only_cover_pre_confirmation_window(self) -> None:
        feed = self._feed_with_trades(
            "cond1",
            [
                {"outcome": "Up", "price": 0.60, "size": 10.0, "timestamp": 1002.0},
                {"outcome": "Down", "price": 0.40, "size": 30.0, "timestamp": 1004.0},
                {"outcome": "Up", "price": 0.85, "size": 5.0, "timestamp": 1006.0},  # confirms
                {"outcome": "Up", "price": 0.95, "size": 999.0, "timestamp": 1050.0},  # after
            ],
        )
        result = confirm_recent_trade_ws(feed, "cond1", "Up", window_start_ts=1000.0)
        assert result is not None
        _, n_trades_pre_entry, balance = result
        # Only the first three trades fall within [window_start, confirm_ts]
        assert n_trades_pre_entry == 3
        # balance = Up volume / total volume within that pre-entry window
        assert balance == pytest.approx((10.0 + 5.0) / (10.0 + 30.0 + 5.0))

    def test_balance_defaults_to_one_when_no_pre_entry_volume(self) -> None:
        # Degenerate case: the confirming trade IS the window start, so
        # there is no distinct pre-entry volume to divide by.
        feed = self._feed_with_trades(
            "cond1", [{"outcome": "Up", "price": 0.85, "size": 5.0, "timestamp": 1000.0}]
        )
        result = confirm_recent_trade_ws(feed, "cond1", "Up", window_start_ts=1000.0)
        assert result is not None
        _, _, balance = result
        assert balance == 1.0
