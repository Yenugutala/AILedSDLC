from __future__ import annotations
"""
Beat 5 — Databricks Genie: Business question in plain English
Duration target: 3:30 – 4:15
"""

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from demo.tools.genie_client import GenieClient, GenieResult
from demo.tools.metrics import MetricsTracker, Timer

console = Console()

_QUESTION = (
    "What is the total net settlement amount by security type for bonds? "
    "Show top 10 rows ordered by total descending."
)


def run(genie: GenieClient, metrics: MetricsTracker) -> GenieResult:
    console.rule("[bold cyan]Beat 5 · Genie[/]")
    console.print(Panel(
        "[bold]🤖 Agent:[/] [bold cyan]Databricks Genie[/]\n"
        "[dim]Role:[/]   Natural language → SQL analytics over the gold layer\n"
        "[dim]Tools:[/]  Databricks Genie REST API · auto-generates and executes SQL\n"
        "[dim]No LLM tokens billed — runs entirely inside Databricks[/]",
        border_style="blue",
    ))
    console.print(Panel(
        f"[bold yellow]{_QUESTION}[/]",
        title="[bold]Business Question (plain English)[/]",
        border_style="yellow",
    ))

    metrics.emit_log("beat5", "Sending question to Databricks Genie...")

    try:
        with Timer() as t:
            result = genie.query(_QUESTION)

        metrics.record(
            beat_id="beat5",
            name="Genie Query",
            model=None,
            input_tokens=0,
            output_tokens=0,
            latency_ms=t.elapsed_ms,
            status="pass",
        )
        metrics.emit_log("beat5", f"Genie responded in {t.elapsed_ms}ms")

        # Display result
        if result.answer_text:
            console.print(Panel(
                result.answer_text,
                title="[bold green]Genie Answer[/]",
                border_style="green",
            ))
        if result.sql_query:
            console.print(f"[dim]SQL: {result.sql_query[:200]}...[/]")
        if result.row_count:
            console.print(f"[dim]Rows returned: {result.row_count}[/]")

        return result

    except Exception as e:
        console.print(Panel(
            f"[yellow]Genie returned an error: {e}[/]\n\n"
            "[dim]This is expected if the gold table has not yet been deployed "
            "to the Databricks workspace. The query would work after "
            "`sml deploy` and `sml run --job gold_mart_job`.[/]",
            title="[yellow]Beat 5 · Genie (table not yet deployed)[/]",
            border_style="yellow",
        ))
        metrics.record(
            beat_id="beat5", name="Genie Query", model=None,
            input_tokens=0, output_tokens=0, latency_ms=0,
            status="skipped",
        )
        # Return a placeholder so the demo can continue
        from demo.tools.genie_client import GenieResult
        return GenieResult(question=_QUESTION, answer_text="(table not yet deployed)", sql_query=None, row_count=0)
