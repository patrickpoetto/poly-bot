"""ICAO / station coordinates for forecast APIs (bundled + overrides)."""

from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Common Polymarket daily-temperature stations (ICAO → lat, lon, IANA tz).
# Extend as new cities appear on Gamma.
ICAO_COORDINATES: dict[str, tuple[float, float, str]] = {
    "EGLC": (51.5053, 0.0553, "Europe/London"),
    "EGLL": (51.4700, -0.4543, "Europe/London"),
    "LFPG": (49.0097, 2.5479, "Europe/Paris"),
    "LFPO": (48.7233, 2.3794, "Europe/Paris"),
    "EDDF": (50.0379, 8.5622, "Europe/Berlin"),
    "EDDM": (48.3538, 11.7861, "Europe/Berlin"),
    "LEMD": (40.4983, -3.5676, "Europe/Madrid"),
    "LIRF": (41.8003, 12.2389, "Europe/Rome"),
    "EHAM": (52.3105, 4.7683, "Europe/Amsterdam"),
    "LOWW": (48.1103, 16.5697, "Europe/Vienna"),
    "LSZH": (47.4581, 8.5481, "Europe/Zurich"),
    "EKCH": (55.6180, 12.6508, "Europe/Copenhagen"),
    "ESSA": (59.6519, 17.9186, "Europe/Stockholm"),
    "ENGM": (60.1939, 11.1004, "Europe/Oslo"),
    "EFHK": (60.3172, 24.9633, "Europe/Helsinki"),
    "EPWA": (52.1657, 20.9671, "Europe/Warsaw"),
    "LKPR": (50.1008, 14.2600, "Europe/Prague"),
    "LHBP": (47.4394, 19.2618, "Europe/Budapest"),
    "LTFM": (41.2753, 28.7519, "Europe/Istanbul"),
    "LTAC": (40.1281, 32.9951, "Europe/Istanbul"),
    "UUEE": (55.9726, 37.4146, "Europe/Moscow"),
    "OMDB": (25.2532, 55.3657, "Asia/Dubai"),
    "VIDP": (28.5562, 77.1000, "Asia/Kolkata"),
    "VABB": (19.0887, 72.8679, "Asia/Kolkata"),
    "WSSS": (1.3644, 103.9915, "Asia/Singapore"),
    "VTBS": (13.6900, 100.7501, "Asia/Bangkok"),
    "VHHH": (22.3080, 113.9185, "Asia/Hong_Kong"),
    "ZGGG": (23.3924, 113.2988, "Asia/Shanghai"),
    "ZSPD": (31.1434, 121.8052, "Asia/Shanghai"),
    "RJTT": (35.5533, 139.7811, "Asia/Tokyo"),
    "RKSI": (37.4602, 126.4407, "Asia/Seoul"),
    "RKPK": (35.1795, 128.9382, "Asia/Seoul"),
    "YSSY": (-33.9461, 151.1772, "Australia/Sydney"),
    "YMML": (-37.6733, 144.8433, "Australia/Melbourne"),
    "NZAA": (-37.0081, 174.7850, "Pacific/Auckland"),
    "KLAX": (33.9425, -118.4081, "America/Los_Angeles"),
    "KSFO": (37.6213, -122.3790, "America/Los_Angeles"),
    "KSEA": (47.4502, -122.3088, "America/Los_Angeles"),
    "KPDX": (45.5897, -122.5951, "America/Los_Angeles"),
    "KLAS": (36.0840, -115.1537, "America/Los_Angeles"),
    "KPHX": (33.4373, -112.0078, "America/Phoenix"),
    "KDEN": (39.8561, -104.6737, "America/Denver"),
    "KDFW": (32.8998, -97.0403, "America/Chicago"),
    "KORD": (41.9742, -87.9073, "America/Chicago"),
    "KIAH": (29.9902, -95.3368, "America/Chicago"),
    "KATL": (33.6407, -84.4277, "America/New_York"),
    "KMIA": (25.7959, -80.2870, "America/New_York"),
    "KJFK": (40.6413, -73.7781, "America/New_York"),
    "KEWR": (40.6895, -74.1745, "America/New_York"),
    "KBOS": (42.3656, -71.0096, "America/New_York"),
    "KDCA": (38.8512, -77.0402, "America/New_York"),
    "CYYZ": (43.6777, -79.6248, "America/Toronto"),
    "CYVR": (49.1967, -123.1815, "America/Vancouver"),
    "MMMX": (19.4363, -99.0721, "America/Mexico_City"),
    "SBGR": (-23.4356, -46.4731, "America/Sao_Paulo"),
    "SCEL": (-33.3930, -70.7858, "America/Santiago"),
    "SAEZ": (-34.8222, -58.5358, "America/Argentina/Buenos_Aires"),
    "FACT": (-33.9648, 18.6017, "Africa/Johannesburg"),
    "HECA": (30.1219, 31.4056, "Africa/Cairo"),
    "OMAR": (25.2731, 51.6081, "Asia/Qatar"),
    "ZBAA": (40.0799, 116.6031, "Asia/Shanghai"),
    "ZUTF": (30.5785, 103.9471, "Asia/Shanghai"),
    "RJCC": (43.0642, 141.3469, "Asia/Tokyo"),
    "RJFF": (33.5859, 130.4511, "Asia/Tokyo"),
    "VTCC": (18.7669, 98.9626, "Asia/Bangkok"),
    "WIII": (-6.1256, 106.6559, "Asia/Jakarta"),
    "RPLL": (14.5086, 121.0194, "Asia/Manila"),
    "VOMM": (12.9941, 80.1805, "Asia/Kolkata"),
    "VOBL": (13.1986, 77.7066, "Asia/Kolkata"),
    "EGKK": (51.1481, -0.1903, "Europe/London"),
    "EGCC": (53.3537, -2.2750, "Europe/London"),
    "EGPH": (55.9500, -3.3725, "Europe/London"),
    "KMSY": (29.9934, -90.2580, "America/Chicago"),
    "KAUS": (30.1975, -97.6664, "America/Chicago"),
    "KMSN": (43.1399, -89.3375, "America/Chicago"),
    "KOKC": (35.3931, -97.6007, "America/Chicago"),
    "KMEM": (35.0424, -89.9767, "America/Chicago"),
    "KSTL": (38.7487, -90.3700, "America/Chicago"),
    "KDTW": (42.2124, -83.3534, "America/New_York"),
    "KPHL": (39.8719, -75.2411, "America/New_York"),
    "KPIT": (40.4915, -80.2329, "America/New_York"),
    "KCLT": (35.2140, -80.9431, "America/New_York"),
    "KBNA": (36.1245, -86.6782, "America/Chicago"),
    "KRDU": (35.8776, -78.7875, "America/New_York"),
    "KMCO": (28.4312, -81.3081, "America/New_York"),
    "KTPA": (27.9755, -82.5332, "America/New_York"),
    # --- Polymarket daily-temperature stations (added as cities appeared) ---
    "KLGA": (40.7772, -73.8726, "America/New_York"),
    "KDAL": (32.8471, -96.8518, "America/Chicago"),
    "KHOU": (29.6454, -95.2789, "America/Chicago"),
    "KBKF": (39.7017, -104.7510, "America/Denver"),
    "LFPB": (48.9694, 2.4414, "Europe/Paris"),
    "LIMC": (45.6306, 8.7281, "Europe/Rome"),
    "RCSS": (25.0697, 121.5520, "Asia/Taipei"),
    "WMKK": (2.7456, 101.7099, "Asia/Kuala_Lumpur"),
    "ZUUU": (30.5785, 103.9471, "Asia/Shanghai"),
    "ZUCK": (29.7192, 106.6417, "Asia/Shanghai"),
    "ZGSZ": (22.6393, 113.8107, "Asia/Shanghai"),
    "ZSQD": (36.2661, 120.3740, "Asia/Shanghai"),
    "ZHHH": (30.7838, 114.2081, "Asia/Shanghai"),
    "VILK": (26.7606, 80.8893, "Asia/Kolkata"),
    "OEJN": (21.6796, 39.1565, "Asia/Riyadh"),
    "OPKC": (24.9065, 67.1608, "Asia/Karachi"),
    "MPMG": (8.9733, -79.5556, "America/Panama"),
    "NZWN": (-41.3272, 174.8053, "Pacific/Auckland"),
}


def lookup_coordinates(
    *,
    icao: str | None,
    country_code: str = "",
    city_display: str = "",
) -> tuple[float | None, float | None, str]:
    """Return (lat, lon, timezone) for a station."""
    if icao:
        key = icao.upper()
        if key in ICAO_COORDINATES:
            lat, lon, tz = ICAO_COORDINATES[key]
            return lat, lon, _valid_tz(tz)
    tz = _guess_timezone(country_code, city_display)
    return None, None, tz


def _valid_tz(name: str) -> str:
    try:
        ZoneInfo(name)
        return name
    except ZoneInfoNotFoundError:
        return "UTC"


def _guess_timezone(country_code: str, city_display: str) -> str:
    cc = country_code.lower()
    mapping = {
        "gb": "Europe/London",
        "us": "America/New_York",
        "ca": "America/Toronto",
        "au": "Australia/Sydney",
        "nz": "Pacific/Auckland",
        "jp": "Asia/Tokyo",
        "kr": "Asia/Seoul",
        "cn": "Asia/Shanghai",
        "in": "Asia/Kolkata",
        "de": "Europe/Berlin",
        "fr": "Europe/Paris",
        "it": "Europe/Rome",
        "es": "Europe/Madrid",
        "br": "America/Sao_Paulo",
        "ar": "America/Argentina/Buenos_Aires",
        "mx": "America/Mexico_City",
        "ae": "Asia/Dubai",
        "tr": "Europe/Istanbul",
        "za": "Africa/Johannesburg",
        "eg": "Africa/Cairo",
        "sg": "Asia/Singapore",
        "th": "Asia/Bangkok",
        "ph": "Asia/Manila",
        "id": "Asia/Jakarta",
    }
    if cc in mapping:
        return mapping[cc]
    _ = city_display
    return "UTC"
