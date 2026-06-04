"""Parse aviation TAF/METAR for daily max temperature."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

_TX_TN_RE = re.compile(
    r"\bT([XN])(\d{2,3})/(\d{2})(\d{2,4})Z",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ParsedTxTn:
    kind: str
    value: int
    day: int
    hour: int
    minute: int
    valid_at: datetime


def parse_utc_datetime(value: int | float | str | None) -> datetime | None:
    """Aviation Weather API times: Unix epoch or ISO-8601 (e.g. 2026-06-01T21:08:00.000Z)."""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if isinstance(value, float):
        return datetime.fromtimestamp(int(value), tz=UTC)
    return datetime.fromtimestamp(int(value), tz=UTC)


def epoch_to_utc(ts: int | float | str | None) -> datetime | None:
    """Alias for API payloads that may use epoch or ISO strings."""
    return parse_utc_datetime(ts)


def taf_tx_temp_unit(icao: str | None) -> str:
    """ICAO TAF TX/TN units: Fahrenheit in US (Kxxx), Celsius elsewhere."""
    if icao and len(icao) == 4 and icao[0] == "K" and icao[1:].isalpha():
        return "F"
    return "C"


def temp_to_celsius(value: float, unit: str) -> int:
    if unit.upper() == "F":
        return round((value - 32) * 5 / 9)
    return round(value)


def celsius_to_station_unit(temp_c: int, unit: str) -> float:
    if unit.upper() == "F":
        return temp_c * 9 / 5 + 32
    return float(temp_c)


def parse_tx_tn_groups(
    raw_taf: str,
    *,
    issue_time: datetime | None,
    unit: str,
) -> list[ParsedTxTn]:
    """Extract TX/TN groups; values use station resolution unit (F US, C elsewhere)."""
    if not raw_taf or issue_time is None:
        return []
    base = issue_time.astimezone(UTC)
    out: list[ParsedTxTn] = []
    for m in _TX_TN_RE.finditer(raw_taf):
        kind = m.group(1).upper()
        value = int(m.group(2))
        day = int(m.group(3))
        hhmm = m.group(4)
        hour = int(hhmm[:2])
        minute = int(hhmm[2:4]) if len(hhmm) >= 4 else 0
        valid_at = _resolve_taf_day(base, day, hour, minute)
        out.append(
            ParsedTxTn(
                kind=kind,
                value=value,
                day=day,
                hour=hour,
                minute=minute,
                valid_at=valid_at,
            )
        )
    return out


def _resolve_taf_day(issue: datetime, day: int, hour: int, minute: int) -> datetime:
    """Map TAF DDhh to UTC datetime using bulletin issue month/year."""
    year = issue.year
    month = issue.month
    try:
        candidate = datetime(year, month, day, hour, minute, tzinfo=UTC)
    except ValueError:
        if month == 12:
            candidate = datetime(year + 1, 1, day, hour, minute, tzinfo=UTC)
        else:
            candidate = datetime(year, month + 1, day, hour, minute, tzinfo=UTC)
    if candidate < issue - timedelta(days=3):
        if month == 12:
            candidate = datetime(year + 1, 1, day, hour, minute, tzinfo=UTC)
        else:
            candidate = datetime(year, month + 1, day, hour, minute, tzinfo=UTC)
    return candidate


def tx_tmax_for_local_date(
    groups: list[ParsedTxTn],
    *,
    forecast_date: date,
    timezone: str,
    unit: str,
) -> tuple[int | None, dict[str, object]]:
    """Pick TX (max) for the station's local calendar day."""
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        tz = UTC
    tx_vals: list[int] = []
    matched: list[dict[str, object]] = []
    for g in groups:
        if g.kind != "X":
            continue
        local_d = g.valid_at.astimezone(tz).date()
        if local_d != forecast_date:
            continue
        tx_vals.append(g.value)
        matched.append(
            {
                "value": g.value,
                "unit": unit,
                "valid_at": g.valid_at.isoformat(),
            }
        )
    if not tx_vals:
        return None, {"tx_groups": matched}
    tmax_raw = max(tx_vals)
    tmax_c = temp_to_celsius(float(tmax_raw), unit)
    return tmax_c, {"tx_groups": matched, "tmax_raw": tmax_raw, "unit": unit}


def tx_tmax_for_utc_window(
    groups: list[ParsedTxTn],
    *,
    window_start_utc: datetime,
    window_end_utc: datetime,
    unit: str,
) -> tuple[int | None, dict[str, object]]:
    """Pick TX (max) for an explicit UTC window [start, end)."""
    tx_vals: list[int] = []
    matched: list[dict[str, object]] = []
    for g in groups:
        if g.kind != "X":
            continue
        if not (window_start_utc <= g.valid_at < window_end_utc):
            continue
        tx_vals.append(g.value)
        matched.append(
            {
                "value": g.value,
                "unit": unit,
                "valid_at": g.valid_at.isoformat(),
            }
        )
    if not tx_vals:
        return None, {"tx_groups": matched}
    tmax_raw = max(tx_vals)
    tmax_c = temp_to_celsius(float(tmax_raw), unit)
    return tmax_c, {"tx_groups": matched, "tmax_raw": tmax_raw, "unit": unit}


def parse_metar_temp_from_raw(raw_ob: str) -> int | None:
    """Temperature from METAR body (always °C), e.g. ``13/10`` or ``M05/M03``."""
    m = re.search(r"\s(M?\d{1,2})/(\d{1,2})\s", raw_ob)
    if not m:
        return None
    t = m.group(1)
    if t.startswith("M"):
        return -int(t[1:])
    return int(t)


def metar_temp_values(
    temp: float | int | None,
    *,
    resolution_unit: str,
) -> tuple[float | None, float | None]:
    """Aviation Weather API METAR temp is always °C; resolution_unit is for display only."""
    if temp is None:
        return None, None
    temp_c = float(temp)
    if resolution_unit.upper() == "F":
        temp_f = celsius_to_station_unit(round(temp_c), "F")
    else:
        temp_f = temp_c
    return temp_c, temp_f


def local_date_for_obs(obs_time: datetime, timezone: str) -> date:
    try:
        return obs_time.astimezone(ZoneInfo(timezone)).date()
    except Exception:
        return obs_time.astimezone(UTC).date()
