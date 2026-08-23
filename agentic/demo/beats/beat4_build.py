from __future__ import annotations
"""
Beat 4 — Code Generation + Test Suite with REQ-ID Traceability
Duration target: 2:30 – 3:30
Delegates to real agents: code_gen_agent (Developer) and test_gen_agent (QA).
"""

import sys
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from demo.beats.beat1_pull_ticket import TicketContext
from demo.beats.beat2_clarify import ClarificationContext
from demo.tools.jira_client import JiraClient
from demo.tools.metrics import MetricsTracker, Timer

console = Console()


@dataclass
class BuildContext:
    notebook_path: str
    test_path: str
    sign_off_comment_id: str


def run(
    ctx: TicketContext,
    clarification: ClarificationContext,
    jira: JiraClient,
    metrics: MetricsTracker,
    repo_root: Path,
) -> BuildContext:
    console.rule("[bold cyan]Beat 4 · Build + Test[/]")
    console.print(Panel(
        "[bold]🤖 Agent:[/] [bold cyan]Developer Agent[/]  →  [bold green]QA Agent[/]\n"
        "[dim]Developer Agent:[/]  Reads gold/tables.yaml + CLAUDE.md → generates 05_gold_build.sql\n"
        "[dim]QA Agent:[/]         Reads specs + ticket requirements → generates test suite\n"
        "[dim]Model:[/]            claude-sonnet-4-6  ·  Streaming output below",
        border_style="green",
    ))

    ticket = ctx.ticket

    # Add agentic/ to path so real agents are importable
    agents_dir = str(repo_root / "agentic")
    if agents_dir not in sys.path:
        sys.path.insert(0, agents_dir)

    from agents import context_loader, code_gen_agent, test_gen_agent

    # Load shared AgentContext (reads CLAUDE.md, skills, specs, request.yaml)
    console.print("[dim]  Loading AgentContext from project specs...[/]")
    ctx_ag = context_loader.load("securities-master")

    # ── Developer Agent: Code Gen ─────────────────────────────────────
    console.print(f"\n[dim cyan]┌─ Developer Agent · code_gen_agent · layer=gold ────────[/]")
    metrics.emit_log("beat4", "Developer Agent: generating gold layer SQL notebook...")
    with Timer() as t:
        code_gen_agent.run(ctx_ag, layer_only="gold")   # streams live, writes 05_gold_build.sql
    console.print(f"[dim cyan]└─ Developer Agent complete ({t.elapsed_ms}ms) ─────────────[/]\n")
    notebook_path = "project/notebooks/05_gold_build.sql"
    metrics.record(
        beat_id="beat4a", name="Developer Agent (Gold SQL)",
        model="claude-sonnet-4-6", input_tokens=0, output_tokens=0,
        latency_ms=t.elapsed_ms, status="pass",
    )
    metrics.emit_log("beat4", f"Gold notebook written: {notebook_path}")
    console.print(f"[green]  ✓ {notebook_path}[/]")

    # ── QA Agent: Test Gen ────────────────────────────────────────────
    console.print(f"\n[dim cyan]┌─ QA Agent · test_gen_agent ─────────────────────────────[/]")
    metrics.emit_log("beat4", "QA Agent: generating test suite with REQ-ID tracing...")
    with Timer() as t:
        test_gen_agent.run(ctx_ag)   # streams live, writes test_bronze/silver/gold.py
    console.print(f"[dim cyan]└─ QA Agent complete ({t.elapsed_ms}ms) ──────────────────────[/]\n")
    test_path = "project/tests/test_gold.py"
    metrics.record(
        beat_id="beat4b", name="QA Agent (Test Suite)",
        model="claude-sonnet-4-6", input_tokens=0, output_tokens=0,
        latency_ms=t.elapsed_ms, status="pass",
    )
    metrics.emit_log("beat4", f"Test file written: {test_path}")
    console.print(f"[green]  ✓ {test_path}[/]")

    # ── Jira Sign-Off ─────────────────────────────────────────────────
    req_matrix = [
        f"|{r.id}|test_gold.py::test_{r.id.lower().replace('-','_')}|"
        for r in ticket.requirements
    ]
    comment_id = jira.post_sign_off(ticket.key, notebook_path, test_path, req_matrix)
    metrics.emit_jira_write("beat4", "sign-off", ticket.key, comment_id)
    metrics.emit_log("beat4", f"Sign-off posted to {ticket.key}", "jira")
    console.print(f"[green]  ✓ Sign-off posted to {ticket.key} (comment {comment_id})[/]")

    return BuildContext(
        notebook_path=notebook_path,
        test_path=test_path,
        sign_off_comment_id=comment_id,
    )
