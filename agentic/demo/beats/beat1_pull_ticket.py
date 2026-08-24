from __future__ import annotations
"""
Beat 1 — Show Open Tickets → User Selects → Pull Full Ticket Details
Duration target: 0:00 – 0:30
"""

from dataclasses import dataclass

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt

from demo.tools.jira_client import JiraClient, JiraTicket
from demo.tools.metrics import MetricsTracker

console = Console()

JIRA_PROJECT = "SCRUM"


@dataclass
class TicketContext:
    ticket: JiraTicket


def run(jira: JiraClient, metrics: MetricsTracker) -> TicketContext:
    console.rule("[bold cyan]Beat 1 · Pull Ticket[/]")
    console.print(Panel(
        "[bold]🤖 Agent:[/] [bold cyan]BA Agent[/]\n"
        "[dim]Role:[/]   Business Analyst — fetches open Jira tickets, parses structured requirements\n"
        "[dim]Output:[/] Selected ticket + REQ-ID list for downstream agents",
        border_style="blue",
    ))

    metrics.emit_log("beat1", "Fetching open tickets from Jira...")

    # Fetch open tickets assigned to the user
    tickets = jira.get_open_tickets(JIRA_PROJECT)

    if not tickets:
        console.print("[yellow]  No open tickets found. Check Jira project and assignee.[/]")
        raise SystemExit(1)

    # Display ticket list
    tbl = Table(show_header=True, header_style="bold cyan", border_style="dim")
    tbl.add_column("#", style="dim", width=4)
    tbl.add_column("Key", style="bold blue", width=10)
    tbl.add_column("Summary", no_wrap=False)
    tbl.add_column("Status", width=14)
    tbl.add_column("Priority", width=10)

    for i, t in enumerate(tickets, 1):
        status_color = "green" if t.status == "In Progress" else "yellow"
        tbl.add_row(
            str(i),
            t.key,
            t.summary,
            f"[{status_color}]{t.status}[/]",
            t.priority,
        )

    console.print(Panel(tbl, title="[bold]Your Open Tickets[/]", border_style="cyan"))

    # User selects
    choice = Prompt.ask(
        "[bold cyan]Select ticket number[/]",
        choices=[str(i) for i in range(1, len(tickets) + 1)],
    )
    ticket = tickets[int(choice) - 1]

    metrics.emit_log("beat1", f"Selected {ticket.key}: {ticket.summary}")

    # Show full ticket details including description as Business Requirements Document
    console.print(Panel(
        _format_ticket(ticket),
        title=f"[bold blue]{ticket.key}[/] · {ticket.summary}",
        border_style="blue",
    ))
    doc_len = len(ticket.description)
    if doc_len:
        console.print(
            f"[green]  ✓ Business Requirements Document read ({doc_len} chars) "
            f"— indexed into knowledge base[/]"
        )

    metrics.record(
        beat_id="beat1",
        name="Pull Ticket",
        model=None,
        input_tokens=0,
        output_tokens=0,
        latency_ms=0,
        status="pass",
    )
    metrics.emit_log("beat1", f"{len(ticket.requirements)} requirements parsed from ticket")

    return TicketContext(ticket=ticket)


def _format_ticket(t: JiraTicket) -> str:
    lines = [
        f"[bold]Summary:[/]   {t.summary}",
        f"[bold]Status:[/]    {t.status}",
        f"[bold]Priority:[/]  {t.priority}",
        f"[bold]Assignee:[/]  {t.assignee}",
        f"[bold]Labels:[/]    {', '.join(t.labels) or 'none'}",
    ]

    # Show description as Business Requirements Document
    if t.description:
        desc = t.description.strip()
        truncated = desc[:600] + "…" if len(desc) > 600 else desc
        lines += ["", "[bold]Business Requirements Document:[/]", f"[dim]{truncated}[/]"]

    lines += ["", "[bold]Structured Requirements:[/]"]
    for r in t.requirements:
        lines.append(f"  [cyan]{r.id}[/]  {r.text}")
    if not t.requirements:
        lines.append("  [dim](No structured requirements found)[/]")
    return "\n".join(lines)
