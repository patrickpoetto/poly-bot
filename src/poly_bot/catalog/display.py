"""Rich console display for expiring markets."""

from __future__ import annotations

from datetime import UTC, datetime

from rich.console import Console
from rich.table import Table

from poly_bot.catalog.events_view import market_fee_label
from poly_bot.catalog.models import ExpiringMarket


def print_expiring_table(markets: list[ExpiringMarket], *, hours: float) -> None:
    console = Console()
    table = Table(title=f"Markets expiring in next {hours:.0f}h ({len(markets)} total)")
    table.add_column("Ends (UTC)", style="cyan")
    table.add_column("Left", justify="right")
    table.add_column("Domain")
    table.add_column("Yes", justify="right")
    table.add_column("No", justify="right")
    table.add_column("OI", justify="right")
    table.add_column("Fee@mid", justify="right")
    table.add_column("Question", max_width=42)

    now = datetime.now(UTC)
    for m in markets[:200]:
        if m.quote_updated_at:
            (now - m.quote_updated_at).total_seconds()
        table.add_row(
            m.end_ts.strftime("%m-%d %H:%M"),
            f"{m.hours_left:.1f}h",
            m.domain,
            _fmt_price(m.yes_mid),
            _fmt_price(m.no_mid),
            _fmt_oi(m.open_interest),
            market_fee_label(m),
            (m.question[:42] + "…") if len(m.question) > 43 else m.question,
        )
    console.print(table)
    if len(markets) > 200:
        console.print(f"[dim]… and {len(markets) - 200} more (use dashboard or export)[/dim]")


def _fmt_price(v: float | None) -> str:
    return f"{v:.3f}" if v is not None else "—"


def _fmt_oi(v: float | None) -> str:
    if v is None:
        return "—"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v / 1_000:.1f}k"
    return f"{v:.0f}"
