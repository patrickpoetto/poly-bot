"""Tests for Gamma → entity mapping."""

from poly_bot.discovery.mapper import gamma_market_to_entities


def test_gamma_market_mapping() -> None:
    raw = {
        "conditionId": "0xabc",
        "slug": "btc-updown-5m-1710000000",
        "question": "Bitcoin Up or Down?",
        "outcomes": '["Up","Down"]',
        "clobTokenIds": '["111","222"]',
        "active": True,
        "closed": False,
        "negRisk": False,
    }
    market, tokens, _tags = gamma_market_to_entities(raw)
    assert market.condition_id == "0xabc"
    assert market.domain == "crypto"
    assert len(tokens) == 2
    assert tokens[0].outcome_label == "Up"
