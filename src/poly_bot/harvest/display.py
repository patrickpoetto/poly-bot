"""CLI tables for harvest opportunities."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from poly_bot.harvest.scan import HarvestOpportunity


def print_harvest_table(
    opportunities: list[HarvestOpportunity],
    *,
    hot_hours: float,
    min_net_pct: float,
    order_usd: float,
) -> None:
    table = Table(
        title=(
            f"Harvest · HOT ≤{hot_hours:.0f}h · net ≥{min_net_pct * 100:.1f}% · "
            f"${order_usd:.0f} size"
        )
    )
    table.add_column("Ends", justify="right")
    table.add_column("Net%", justify="right")
    table.add_column("Side")
    table.add_column("Ask", justify="right")
    table.add_column("Fill$", justify="right")
    table.add_column("Est$", justify="right")
    table.add_column("Domain")
    table.add_column("Question", max_width=40)

    for o in opportunities:
        ends = f"{o.minutes_left:.0f}m"
        if o.is_live:
            ends = f"L{ends}"
        table.add_row(
            ends,
            f"{o.net_yield_pct * 100:.2f}",
            f"{o.side.upper()} {o.outcome_label[:10]}",
            f"{o.ask:.3f}",
            f"{o.fillable_usd:.0f}",
            f"{o.est_profit_usd:.2f}",
            o.domain,
            (o.question[:38] + "…") if len(o.question) > 39 else o.question,
        )

    Console().print(table)
