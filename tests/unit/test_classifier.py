"""Tests for market classification rules."""

from poly_bot.discovery.classifier.rules import classify_domain_from_tags, classify_market


def test_btc_5m_recurring_slug() -> None:
    raw = {"outcomes": '["Up","Down"]', "tags": [{"slug": "crypto"}]}
    result = classify_market(raw, slug="btc-updown-5m-1710000000")
    assert result["domain"] == "crypto"
    assert result["horizon"] == "ultra_short"
    assert result["recurrence"] == "recurring"


def test_politics_from_tag() -> None:
    raw = {"outcomes": '["Yes","No"]', "tags": [{"slug": "politics"}]}
    result = classify_market(raw, slug="will-x-happen")
    assert result["domain"] == "politics"
    assert result["resolution_source"] == "uma"


def test_classify_domain_from_tags() -> None:
    assert classify_domain_from_tags(["nba", "sports"]) == "sports"
    assert classify_domain_from_tags(["unknown-tag"]) == "unknown"


def test_liquidity_tier_from_volume() -> None:
    from poly_bot.discovery.classifier.rules import classify_liquidity_tier

    assert classify_liquidity_tier({}, volume_24h=2_000_000) == "high"
    assert classify_liquidity_tier({}, volume_24h=50_000) == "low"


def test_sol_recurring_slug() -> None:
    raw = {"outcomes": '["Up","Down"]'}
    result = classify_market(raw, slug="sol-updown-5m-1710000000")
    assert result["domain"] == "crypto"
    assert result["recurrence"] == "recurring"
