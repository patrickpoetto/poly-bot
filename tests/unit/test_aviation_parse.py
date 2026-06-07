"""Tests for TAF TX/TN parsing."""

from __future__ import annotations

from datetime import UTC, datetime

from poly_bot.weather.aviation_parse import (
    epoch_to_utc,
    metar_temp_values,
    parse_tx_tn_groups,
    taf_tx_temp_unit,
    temp_to_celsius,
    tx_tmax_for_local_date,
    tx_tmax_for_utc_window,
)


def test_metar_api_always_celsius() -> None:
    temp_c, temp_f = metar_temp_values(13, resolution_unit="F")
    assert temp_c == 13
    assert temp_f == 55.4


def test_epoch_to_utc_iso_string() -> None:
    dt = epoch_to_utc("2026-06-01T21:08:00.000Z")
    assert dt is not None
    assert dt.year == 2026 and dt.month == 6 and dt.hour == 21


def test_epoch_to_utc_unix() -> None:
    dt = epoch_to_utc(1780272000)
    assert dt is not None
    assert dt.tzinfo is not None


def test_parse_tx_beijing_celsius() -> None:
    raw = "TAF ZBAA 312305Z 0100/0206 10006KT 9999 FEW030 TX36/0106Z TX37/0206Z TN21/0121Z"
    issue = datetime(2026, 5, 31, 23, 5, tzinfo=UTC)
    groups = parse_tx_tn_groups(raw, issue_time=issue, unit="C")
    assert len(groups) >= 2
    tmax, meta = tx_tmax_for_local_date(
        groups,
        forecast_date=datetime(2026, 6, 1).date(),
        timezone="Asia/Shanghai",
        unit="C",
    )
    assert tmax == 36
    assert meta.get("tmax_raw") == 36


def test_temp_to_celsius_fahrenheit() -> None:
    assert temp_to_celsius(86, "F") == 30
    assert temp_to_celsius(20, "C") == 20


def test_taf_tx_unit_us_vs_argentina() -> None:
    assert taf_tx_temp_unit("KLGA") == "F"
    assert taf_tx_temp_unit("SAEZ") == "C"


def test_tx_us_fahrenheit_station() -> None:
    raw = "TAF KLGA 312330Z 0100/0206 32013KT TX95/0118Z TN72/0106Z"
    issue = datetime(2026, 5, 31, 23, 30, tzinfo=UTC)
    groups = parse_tx_tn_groups(raw, issue_time=issue, unit="F")
    tmax, _ = tx_tmax_for_local_date(
        groups,
        forecast_date=datetime(2026, 6, 1).date(),
        timezone="America/New_York",
        unit="F",
    )
    assert tmax == temp_to_celsius(95, "F")


def test_tx_window_alignment_utc_bounds() -> None:
    raw = "TAF KLGA 312330Z 0100/0206 32013KT TX95/0118Z TX88/0123Z TN72/0106Z"
    issue = datetime(2026, 5, 31, 23, 30, tzinfo=UTC)
    groups = parse_tx_tn_groups(raw, issue_time=issue, unit="F")
    # Keep only the first TX inside [18:00, 22:00) UTC.
    tmax, meta = tx_tmax_for_utc_window(
        groups,
        window_start_utc=datetime(2026, 6, 1, 18, 0, tzinfo=UTC),
        window_end_utc=datetime(2026, 6, 1, 22, 0, tzinfo=UTC),
        unit="F",
    )
    assert tmax == temp_to_celsius(95, "F")
    tx_groups = meta.get("tx_groups", [])
    assert isinstance(tx_groups, list)
    assert len(tx_groups) == 1
