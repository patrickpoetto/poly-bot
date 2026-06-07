"""CLI tables for weather catalog."""

from __future__ import annotations

from datetime import date

from rich.console import Console
from rich.table import Table

from poly_bot.weather.market_window import days_ahead_of_today, format_days_ahead_label
from poly_bot.weather.schema import WeatherBin, WeatherDailyEvent


def print_weather_summary(
    repo: WeatherRepository,
    *,
    min_date: date,
    max_date: date,
) -> None:
    events = repo.list_daily_events(min_date=min_date, max_date=max_date)
    console = Console()
    by_day: dict[date, list[WeatherDailyEvent]] = {}
    for ev in events:
        by_day.setdefault(ev.forecast_date, []).append(ev)

    table = Table(title="Weather daily events (by forecast day)")
    table.add_column("Date")
    table.add_column("Cities")
    table.add_column("Bins (total)")
    table.add_column("Days ahead")

    for day in sorted(by_day):
        evs = by_day[day]
        bins = sum(e.bin_count for e in evs)
        ahead = days_ahead_of_today(day)
        table.add_row(
            day.isoformat(),
            str(len(evs)),
            str(bins),
            format_days_ahead_label(day),
        )
    console.print(table)
    console.print(f"Total cities×days: {len(events)}")


def print_weather_day_detail(
    repo: WeatherRepository,
    *,
    forecast_date: date,
    city_key: str | None = None,
    limit_cities: int = 10,
) -> None:
    events = repo.list_daily_events(
        min_date=forecast_date,
        max_date=forecast_date,
        city_key=city_key,
    )
    console = Console()
    shown = 0
    for ev in events[:limit_cities] if not city_key else events:
        station = repo.get_station(ev.station_id)
        bins = repo.bins_for_event(ev.id)
        console.print(
            f"\n[bold]{ev.city_key}[/bold] — {ev.title} "
            f"({format_days_ahead_label(ev.forecast_date)}, {len(bins)} bins)"
        )
        if station:
            coord = ""
            if station.latitude is not None and station.longitude is not None:
                coord = f" @ {station.latitude:.3f},{station.longitude:.3f}"
            console.print(
                f"  Station: {station.station_name or station.icao} "
                f"({station.icao}){coord} tz={station.timezone}"
            )
            console.print(f"  Source: {station.wunderground_url}")
        t = Table(show_header=True)
        t.add_column("#")
        t.add_column("Bin")
        t.add_column("Kind")
        t.add_column("°C")
        t.add_column("condition_id", overflow="fold")
        for b in bins[:15]:
            t.add_row(
                str(b.bin_index),
                b.bin_label,
                b.bin_kind,
                _temp_cell(b),
                b.condition_id[:18] + "…",
            )
        if len(bins) > 15:
            t.add_row("…", f"+{len(bins) - 15} more", "", "", "")
        console.print(t)
        shown += 1
    if not city_key and len(events) > limit_cities:
        console.print(f"\n… and {len(events) - limit_cities} more cities (use --city)")


def _temp_cell(b: WeatherBin) -> str:
    if b.bin_kind == "exact" and b.temp_c is not None:
        return str(b.temp_c)
    if b.bin_kind == "at_or_below" and b.temp_c_upper is not None:
        return f"≤{b.temp_c_upper}"
    if b.bin_kind == "at_or_above" and b.temp_c_lower is not None:
        return f"≥{b.temp_c_lower}"
    return "—"
