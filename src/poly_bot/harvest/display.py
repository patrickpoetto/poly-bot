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
) -> None:
    table = Table(
        title=f"Harvest targets (HOT ≤{hot_hours:.0f}h · net ≥{min_net_pct * 100:.1f}%)"
    )
    table.add_column("Net%", justify="right")
    table.add_column("Side")
    table.add_column("Price", justify="right")
    table.add_column("Window", justify="right")
    table.add_column("Domain")
    table.add_column("Live")
    table.add_column("Question", max_width=48)

    for o in opportunities:
        live = "●" if o.is_live else ""
        window = f"{o.hours_left:.1f}h"
        if o.resolution_kind == "in_play" and o.is_live:
            window = f"L {o.hours_left:.1f}h"
        table.add_row(
            f"{o.net_yield_pct * 100:.2f}",
            f"{o.side.upper()} {o.outcome_label[:12]}",
            f"{o.price:.3f}",
            window,
            o.domain,
            live,
            (o.question[:45] + "…") if len(o.question) > 46 else o.question,
        )

    Console().print(table)
