"""Parse Polymarket daily temperature slugs, questions, and resolution text."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

_EVENT_SLUG_RE = re.compile(
    r"^highest-temperature-in-(?P<city>[a-z0-9-]+)-on-(?P<month>[a-z]+)-(?P<day>\d{1,2})-(?P<year>\d{4})$",
    re.IGNORECASE,
)

_BIN_SUFFIX_BELOW_RE = re.compile(r"^(?P<temp>\d+)corbelow$", re.IGNORECASE)
_BIN_SUFFIX_ABOVE_RE = re.compile(r"^(?P<temp>\d+)cor(?:above|higher)$", re.IGNORECASE)
_BIN_SUFFIX_EXACT_RE = re.compile(r"^(?P<temp>\d+)c$", re.IGNORECASE)

_QUESTION_BIN_RE = re.compile(
    r"be\s+(?P<label>\d+\s*°?\s*[CFcf](?:\s+or\s+(?:below|above))?)\s+on\s+",
    re.IGNORECASE,
)

_STATION_NAME_RE = re.compile(
    r"at\s+the\s+(.+?)\s+Station\s+in\s+degrees",
    re.IGNORECASE | re.DOTALL,
)

_UNIT_C_RE = re.compile(r"degrees\s+Celsius|°\s*C\b", re.IGNORECASE)
_UNIT_F_RE = re.compile(r"degrees\s+Fahrenheit|°\s*F\b", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class ParsedDailyEventSlug:
    city_key: str
    forecast_date: date
    display_city: str


@dataclass(frozen=True, slots=True)
class ParsedBin:
    bin_kind: str
    temp_c: int | None
    temp_c_upper: int | None
    temp_c_lower: int | None
    bin_label: str
    suffix: str


@dataclass(frozen=True, slots=True)
class ParsedWunderground:
    wunderground_url: str
    wunderground_path: str
    country_code: str
    region: str
    icao: str | None
    station_id: str


def city_key_to_display(city_key: str) -> str:
    return " ".join(part.capitalize() for part in city_key.split("-"))


def parse_daily_event_slug(slug: str) -> ParsedDailyEventSlug | None:
    m = _EVENT_SLUG_RE.match(slug.strip().lower())
    if not m:
        return None
    month = _MONTHS.get(m.group("month").lower())
    if not month:
        return None
    city_key = m.group("city").lower()
    try:
        forecast_date = date(int(m.group("year")), month, int(m.group("day")))
    except ValueError:
        return None
    return ParsedDailyEventSlug(
        city_key=city_key,
        forecast_date=forecast_date,
        display_city=city_key_to_display(city_key),
    )


def parse_forecast_date_from_payload(raw: dict[str, Any]) -> date | None:
    for key in ("eventDate", "endDateIso", "endDate"):
        val = raw.get(key)
        if not val:
            continue
        if isinstance(val, str) and len(val) >= 10:
            try:
                return date.fromisoformat(val[:10])
            except ValueError:
                pass
        if isinstance(val, str):
            dt = _parse_iso(val)
            if dt:
                return dt.date()
    slug = raw.get("slug") or raw.get("ticker") or ""
    parsed = parse_daily_event_slug(str(slug))
    return parsed.forecast_date if parsed else None


def _parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def event_slug_prefix(event_slug: str) -> str:
    return event_slug.strip().lower()


def parse_bin_from_market(
    *,
    event_slug: str,
    market_slug: str,
    question: str,
    group_item_title: str | None = None,
) -> ParsedBin | None:
    prefix = event_slug_prefix(event_slug) + "-"
    slug_l = market_slug.strip().lower()
    if not slug_l.startswith(prefix):
        return None
    suffix = slug_l[len(prefix) :]
    label = (group_item_title or "").strip() or _label_from_question(question)

    m = _BIN_SUFFIX_BELOW_RE.match(suffix)
    if m:
        t = int(m.group("temp"))
        return ParsedBin(
            bin_kind="at_or_below",
            temp_c=None,
            temp_c_upper=t,
            temp_c_lower=None,
            bin_label=label or f"{t}°C or below",
            suffix=suffix,
        )
    m = _BIN_SUFFIX_ABOVE_RE.match(suffix)
    if m:
        t = int(m.group("temp"))
        return ParsedBin(
            bin_kind="at_or_above",
            temp_c=None,
            temp_c_upper=None,
            temp_c_lower=t,
            bin_label=label or f"{t}°C or above",
            suffix=suffix,
        )
    m = _BIN_SUFFIX_EXACT_RE.match(suffix)
    if m:
        t = int(m.group("temp"))
        return ParsedBin(
            bin_kind="exact",
            temp_c=t,
            temp_c_upper=None,
            temp_c_lower=None,
            bin_label=label or f"{t}°C",
            suffix=suffix,
        )
    return ParsedBin(
        bin_kind="unknown",
        temp_c=None,
        temp_c_upper=None,
        temp_c_lower=None,
        bin_label=label,
        suffix=suffix,
    )


def _label_from_question(question: str) -> str:
    m = _QUESTION_BIN_RE.search(question)
    if not m:
        return ""
    return m.group("label").replace(" ", "")


def parse_station_name_from_description(description: str) -> str:
    m = _STATION_NAME_RE.search(description)
    return m.group(1).strip() if m else ""


def parse_resolution_unit(description: str) -> str:
    """Resolution unit from market rules — not the WU UI toggle boilerplate."""
    text = description or ""
    if re.search(
        r"whole degrees Celsius|in degrees Celsius|recorded in degrees Celsius",
        text,
        re.IGNORECASE,
    ):
        return "C"
    if re.search(
        r"whole degrees Fahrenheit|in degrees Fahrenheit|recorded in degrees Fahrenheit",
        text,
        re.IGNORECASE,
    ):
        return "F"
    if _UNIT_C_RE.search(text):
        return "C"
    if _UNIT_F_RE.search(text) and not re.search(
        r"between\s+°?\s*F\s+and\s+°?\s*C",
        text,
        re.IGNORECASE,
    ):
        return "F"
    return "C"


def parse_wunderground_url(url: str) -> ParsedWunderground | None:
    if not url or "wunderground.com" not in url:
        return None
    parsed = urlparse(url.strip())
    path = parsed.path
    marker = "/history/daily/"
    if marker not in path:
        return None
    wu_path = path.split(marker, 1)[1].strip("/")
    if not wu_path:
        return None
    parts = wu_path.split("/")
    icao: str | None = None
    country_code = ""
    region = ""
    if len(parts) >= 3 and len(parts[-1]) in (3, 4) and parts[-1].isalnum():
        icao = parts[-1].upper()
        country_code = parts[0].lower()
        region = "/".join(parts[1:-1])
    station_id = icao or wu_path.replace("/", "_")
    return ParsedWunderground(
        wunderground_url=url.strip(),
        wunderground_path=wu_path,
        country_code=country_code,
        region=region,
        icao=icao,
        station_id=station_id,
    )


def resolution_source_from_payload(raw: dict[str, Any]) -> str:
    return str(raw.get("resolutionSource") or raw.get("resolution_source") or "").strip()


def is_daily_temperature_event_slug(slug: str) -> bool:
    return slug.strip().lower().startswith("highest-temperature-in-") and "-on-" in slug


_MARKET_BIN_SUFFIX_RE = re.compile(r"-\d+c(?:orbelow|orabove|orhigher)?$", re.IGNORECASE)


def infer_event_slug_from_market_slug(market_slug: str) -> str | None:
    """Strip the bin suffix from a market slug to get the parent event slug.

    Returns None if the stripped slug is not a valid daily temperature event slug.
    """
    slug = market_slug.strip().lower()
    stripped = _MARKET_BIN_SUFFIX_RE.sub("", slug)
    if is_daily_temperature_event_slug(stripped):
        return stripped
    return None
