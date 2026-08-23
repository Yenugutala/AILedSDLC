from __future__ import annotations
"""
Beat 2 — Codebase-Grounded Clarification + Jira Write-back
Duration target: 0:30 – 1:30

BA Agent reads: ticket + vector DB codebase context → generates ONE grounded question.
Engineer answers. Answer posted to Jira attributed to them.
"""

import sys
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from demo.beats.beat1_pull_ticket import TicketContext
from demo.knowledge.retriever import Retriever
from demo.tools.jira_client import JiraClient
from demo.tools.metrics import MetricsTracker

console = Console()


@dataclass
class ClarificationContext:
    question: str
    answer: str
    jira_comment_id: str


def run(
    ctx: TicketContext,
    jira: JiraClient,
    retriever: Retriever,
    metrics: MetricsTracker,
) -> ClarificationContext:
    console.rule("[bold cyan]Beat 2 · Clarify[/]")
    console.print(Panel(
        "[bold]🤖 Agent:[/] [bold cyan]BA Agent[/]\n"
        "[dim]Role:[/]   Reads ticket + vector DB codebase context → generates ONE grounded question\n"
        "[dim]Tools:[/]  ChromaDB semantic search · Jira comment write-back\n"
        "[dim]Model:[/]  claude-sonnet-4-6",
        border_style="blue",
    ))
    ticket = ctx.ticket

    # Ensure agentic/ is on path so agents/ is importable
    repo_root = Path(__file__).parent.parent.parent.parent
    agents_dir = str(repo_root / "agentic")
    if agents_dir not in sys.path:
        sys.path.insert(0, agents_dir)

    from agents import clarify_agent  # agents/clarify_agent.py

    # Retrieve relevant codebase context from vector DB
    metrics.emit_log("beat2", "Searching codebase for relevant context...")
    query = f"{ticket.summary} {' '.join(r.text for r in ticket.requirements)}"
    chunks = retriever.search(query, n=5)
    codebase_context = retriever.format_context(chunks)
    metrics.emit_log("beat2", f"Retrieved {len(chunks)} relevant chunks from vector DB")

    # Call BA Agent (clarify_agent) to generate the grounded question
    metrics.emit_log("beat2", "BA Agent: generating codebase-grounded clarification question...")
    requirements = [{"id": r.id, "text": r.text} for r in ticket.requirements]
    question, input_tokens, output_tokens, latency_ms = clarify_agent.run(
        ticket_summary=ticket.summary,
        ticket_key=ticket.key,
        requirements=requirements,
        codebase_context=codebase_context,
    )
    metrics.emit_log("beat2", f"Question generated ({input_tokens} input tokens, {latency_ms}ms)")

    # Display question
    console.print(Panel(
        f"[bold yellow]{question}[/]",
        title="[bold]Clarification Question[/] [dim](grounded in your codebase)[/]",
        border_style="yellow",
    ))

    # Engineer types their answer live
    answer = Prompt.ask("[bold cyan]Your answer[/]")

    # Post to Jira as a formatted ADF comment
    comment_id = jira.post_clarification(ticket.key, question, answer)
    metrics.emit_jira_write("beat2", "comment", ticket.key, comment_id)
    metrics.emit_log("beat2", f"Clarification posted to Jira {ticket.key} (comment {comment_id})", "jira")

    console.print(f"[green]  ✓ Answer posted to {ticket.key} as comment {comment_id}[/]")

    metrics.record(
        beat_id="beat2",
        name="BA Agent (Clarify)",
        model="claude-sonnet-4-6",
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        latency_ms=latency_ms,
        status="pass",
        jira_artifact=f"comment:{comment_id}",
    )

    return ClarificationContext(question=question, answer=answer, jira_comment_id=comment_id)
