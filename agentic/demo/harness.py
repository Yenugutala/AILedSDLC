from __future__ import annotations
"""
demo/harness.py
Main 7-beat demo orchestrator.
Wires up all tools, starts the dashboard, and runs beats sequentially.

Beat flow:
  Beat 1   BA Agent         → pull ticket from Jira
  Beat 2   BA Agent         → codebase-grounded clarification
  Beat 3   Verify Agents    → req clarity + arch conformance + gold spec (dynamic)
  Beat 4   Developer Agent  → generate notebooks + tests
  Beat 4b  Deploy Agent     → git commit + PR + Databricks bundle deploy + job run
  Beat 5   Genie            → natural-language query returns live data
  Beat 6   Observe          → KPI dashboard
"""

import uuid

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from demo import config as cfg_module
from demo.tools.jira_client import JiraClient
from demo.tools.codebase import CodebaseRetriever
from demo.tools.genie_client import GenieClient
from demo.tools.metrics import MetricsTracker
from demo.tools.schema_catalog import SchemaCatalog
from demo.knowledge.retriever import Retriever
from demo.knowledge.indexer import run as index_run
from demo.knowledge import knowledge_agent as ka
from demo.dashboard import server as dashboard_server

console = Console()


class DemoHarness:
    def __init__(self):
        self.config = cfg_module.load()
        self.session_id = f"demo-{uuid.uuid4().hex[:8]}"
        self.metrics = MetricsTracker()
        self.repo_root = self.config.repo_root

        # Tools
        self.jira = JiraClient(
            url=self.config.jira_url,
            username=self.config.jira_username,
            api_token=self.config.jira_api_token,
        )
        self.codebase = CodebaseRetriever(self.repo_root)
        self.genie = GenieClient(
            host=self.config.databricks_host,
            token=self.config.databricks_token,
            space_id=self.config.genie_space_id,
        )
        # Live Databricks schema catalog — built by `sml schema`
        self.schema_catalog = SchemaCatalog(self.config.chroma_path)

    def run(self):
        # ── Start live dashboard ──────────────────────────────────────
        dashboard_url = dashboard_server.start(open_browser=True)
        self.metrics.set_emit(dashboard_server.emit)
        console.print(f"[dim]  Dashboard: {dashboard_url}[/]")

        # Warn if schema catalog not indexed
        if not self.schema_catalog.available():
            console.print(Panel(
                "[yellow]Schema catalog not indexed.[/]\n\n"
                "[dim]Beat 3 Check 3 will run in greenfield mode "
                "(no existing column context from Databricks).[/]\n\n"
                "[dim]To index the live Databricks schema, run:[/]  sml schema",
                title="[yellow]⚠ Schema Catalog Missing[/]",
                border_style="yellow",
            ))

        console.print(Panel(
            f"[bold cyan]AI-DLC Demo[/]  ·  Session: [dim]{self.session_id}[/]\n"
            "[dim]Ticket → Clarify → Verify → Build → Deploy → Genie → Observe[/]",
            border_style="cyan",
        ))

        # ── Beat 1: Show tickets → user selects ───────────────────────
        from demo.beats import beat1_pull_ticket
        dashboard_server.emit("beat_start", {
            "beat_id": "beat1", "name": "Pull Ticket",
            "timestamp": _ts(), "session_id": self.session_id,
        })
        ctx = beat1_pull_ticket.run(self.jira, self.metrics)
        _human_approval(console, "Beat 1 · BA Agent (Pull Ticket)", "Beat 2 · BA Agent: Clarification")

        # ── Auto-index selected ticket into ChromaDB (silently) ───────
        console.print("[dim]  Indexing knowledge base...[/]")
        ticket_text = (
            f"{ctx.ticket.summary}\n{ctx.ticket.description}\n"
            + "\n".join(f"{r.id}: {r.text}" for r in ctx.ticket.requirements)
        )
        total_chunks = index_run(
            repo_root=self.repo_root,
            chroma_path=self.config.chroma_path,
            ticket_key=ctx.ticket.key,
            ticket_text=ticket_text,
        )
        self.metrics.set_chunks_indexed(total_chunks)
        self.metrics.emit_log("beat1", f"Knowledge base ready: {total_chunks} chunks indexed")

        # Retriever (uses just-built index)
        retriever = Retriever(self.config.chroma_path)

        # ── Beat 2: Clarify ───────────────────────────────────────────
        from demo.beats import beat2_clarify
        dashboard_server.emit("beat_start", {
            "beat_id": "beat2", "name": "Clarify",
            "timestamp": _ts(), "ticket_key": ctx.ticket.key,
        })
        clarification = beat2_clarify.run(ctx, self.jira, retriever, self.metrics)
        _human_approval(console, "Beat 2 · BA Agent (Clarify)", "Beat 3 · Architect + Validate Spec Agent: Verify")

        # ── Beat 3: Verify ────────────────────────────────────────────
        from demo.beats import beat3_verify
        dashboard_server.emit("beat_start", {"beat_id": "beat3", "name": "Verify", "timestamp": _ts()})
        beat3_verify.run(
            ctx, clarification, self.jira, self.codebase,
            self.schema_catalog, self.metrics, self.session_id,
        )
        _human_approval(console, "Beat 3 · Verify (all checks passed, build-ready)", "Beat 4 · Developer + QA Agent: Build")

        # ── Beat 4: Build + Test ──────────────────────────────────────
        from demo.beats import beat4_build
        dashboard_server.emit("beat_start", {"beat_id": "beat4", "name": "Build + Test", "timestamp": _ts()})
        beat4_build.run(ctx, clarification, self.jira, self.metrics, self.repo_root)
        _human_approval(console, "Beat 4 · Developer + QA Agent (Build)", "Beat 4b · Deploy Agent: Git + Databricks")

        # ── Beat 4b: Deploy ───────────────────────────────────────────
        from demo.beats import beat4b_deploy
        dashboard_server.emit("beat_start", {"beat_id": "beat4b", "name": "Deploy", "timestamp": _ts()})
        beat4b_deploy.run(ctx, self.metrics, self.repo_root)
        _human_approval(console, "Beat 4b · Deploy Agent (Git + Bundle + Job)", "Beat 5 · Genie: Live Data Query")

        # ── Beat 5: Genie ─────────────────────────────────────────────
        from demo.beats import beat5_genie
        dashboard_server.emit("beat_start", {"beat_id": "beat5", "name": "Genie", "timestamp": _ts()})
        beat5_genie.run(self.genie, self.metrics)

        # ── Beat 6: Observe ───────────────────────────────────────────
        from demo.beats import beat6_observe
        dashboard_server.emit("beat_start", {"beat_id": "beat6", "name": "Observe", "timestamp": _ts()})
        beat6_observe.run(self.metrics, dashboard_url)

        # ── Launch knowledge agent REPL ────────────────────────────────
        agent = ka.KnowledgeAgent(retriever, self.repo_root, self.metrics)
        ka.run_repl(agent)


def _human_approval(con: Console, after: str, next_step: str) -> None:
    """Pause for explicit human approval before the next agent runs."""
    con.print()
    con.print(Panel(
        f"[bold]{after}[/] complete.\n\n"
        f"[dim]Type [bold green]approve[/] to hand off to [cyan]{next_step}[/]\n"
        f"Type [bold red]reject: <reason>[/] to stop.[/]",
        title="[bold yellow]⏸  Human-in-the-Loop Approval[/]",
        border_style="yellow",
    ))
    while True:
        response = Prompt.ask("[bold yellow]Your decision[/]").strip().lower()
        if response == "approve":
            con.print(f"[green]  ✓ Approved — handing off to {next_step}[/]\n")
            return
        elif response.startswith("reject"):
            reason = response[6:].lstrip(": ").strip() or "No reason given"
            con.print(f"[red]  ✗ Rejected: {reason}[/]")
            raise SystemExit(0)
        else:
            con.print("[dim]  Please type 'approve' or 'reject: <reason>'[/]")


def _ts() -> str:
    import time
    return time.strftime("%H:%M:%S")
