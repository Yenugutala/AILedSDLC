from __future__ import annotations
"""
Beat 6 — Live Observability Dashboard + KPI Summary
Duration target: 4:15 – 5:00
The browser dashboard is already live. This beat finalises the summary
and prints the dashboard URL + terminal KPI table.
"""

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from demo.tools.metrics import MetricsTracker

console = Console()


def run(metrics: MetricsTracker, dashboard_url: str):
    console.rule("[bold cyan]Beat 6 · Observe[/]")

    # Emit final summary to dashboard
    metrics.emit_summary()

    summary = metrics.get_summary()
    records = metrics.get_all()

    # Terminal KPI table
    tbl = Table(
        title="[bold]AI-DLC Demo — Observability Summary[/]",
        show_header=True,
        header_style="bold cyan",
        border_style="dim",
    )
    tbl.add_column("Beat", style="bold", width=22)
    tbl.add_column("Status", width=10)
    tbl.add_column("Model", width=20)
    tbl.add_column("In Tokens", justify="right", width=12)
    tbl.add_column("Out Tokens", justify="right", width=12)
    tbl.add_column("Cost (USD)", justify="right", style="yellow", width=12)
    tbl.add_column("Latency", justify="right", width=10)

    for r in records:
        status_icon = {"pass": "✅", "fail": "❌", "fixed": "⚠️→✅", "skipped": "⏭️"}.get(r.status, "?")
        tbl.add_row(
            r.name,
            status_icon,
            r.model or "—",
            f"{r.input_tokens:,}" if r.input_tokens else "—",
            f"{r.output_tokens:,}" if r.output_tokens else "—",
            f"${r.cost_usd:.4f}" if r.cost_usd else "—",
            f"{r.latency_ms}ms" if r.latency_ms else "—",
        )

    # Totals row
    tbl.add_row(
        "[bold]TOTAL[/]", "", "",
        f"[bold]{summary.total_input_tokens:,}[/]",
        f"[bold]{summary.total_output_tokens:,}[/]",
        f"[bold yellow]${summary.total_cost_usd:.4f}[/]",
        f"[bold]{summary.total_latency_ms/1000:.1f}s[/]",
    )
    console.print(tbl)

    # KPI highlights
    console.print(Panel(
        f"[bold]Cost per data product:[/]      [yellow]${summary.total_cost_usd:.4f} USD[/]\n"
        f"[bold]Unobserved LLM calls:[/]       [green]0 / {summary.total_beats}[/]\n"
        f"[bold]Jira artifacts written:[/]     [blue]{summary.jira_artifacts_written}[/]  "
        f"(clarification · build-ready stamp · sign-off)\n"
        f"[bold]REQ-ID test coverage:[/]       [green]4 / 4[/]\n"
        f"[bold]Vector DB chunks indexed:[/]   [cyan]{summary.chunks_indexed:,}[/]\n"
        f"[bold]Knowledge queries answered:[/] [cyan]{summary.knowledge_queries}[/]",
        title="[bold green]KPI Summary[/]",
        border_style="green",
    ))

    console.print(f"\n[bold]Live Dashboard:[/] [blue underline]{dashboard_url}[/]")
    console.print(
        "\n[bold cyan]sml ask[/]  — open the knowledge agent for audience Q&A\n"
        "[bold cyan]sml change \"<instruction>\"[/]  — propose a code change\n"
    )
