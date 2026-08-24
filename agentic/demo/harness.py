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
from typing import Callable, Any

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from demo import config as cfg_module
from demo.tools.jira_client import JiraClient
from demo.tools.codebase import CodebaseRetriever
from demo.tools.genie_client import GenieClient
from demo.tools.metrics import MetricsTracker
from demo.tools.schema_catalog import SchemaCatalog
from demo.tools.data_catalog import DataCatalog
from demo.tools.langfuse_tracker import LangfuseTracker
from demo.tools.demo_state import DemoState
from demo.knowledge.retriever import Retriever
from demo.knowledge.indexer import get_collection, index_jira_ticket
from demo.knowledge.index_state import IndexState
from demo.knowledge import knowledge_agent as ka
from demo.dashboard import server as dashboard_server

console = Console()

MAX_BEAT_RETRIES = 3


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
        # AI-profiled data catalog — built by `sml profile` (preferred over schema_catalog)
        self.data_catalog = DataCatalog(self.config.chroma_path)
        # Langfuse observability — optional, gracefully disabled if keys not set
        self.langfuse = LangfuseTracker(
            public_key=self.config.langfuse_public_key,
            secret_key=self.config.langfuse_secret_key,
            host=self.config.langfuse_host,
        )

    def run(self):
        # ── Start live dashboard ──────────────────────────────────────
        dashboard_url = dashboard_server.start(open_browser=True)
        self.metrics.set_emit(dashboard_server.emit)
        console.print(f"[dim]  Dashboard: {dashboard_url}[/]")

        # Warn if neither catalog is indexed
        if not self.data_catalog.available() and not self.schema_catalog.available():
            console.print(Panel(
                "[yellow]No data catalog indexed.[/]\n\n"
                "[dim]Beat 3 Check 3 will run in greenfield mode "
                "(no existing column context from Databricks).[/]\n\n"
                "[dim]For best results, run:[/]  sml profile\n"
                "[dim](profiles tables + generates AI column descriptions + join map)\n"
                "Or for schema only:[/]  sml schema",
                title="[yellow]⚠ Data Catalog Missing[/]",
                border_style="yellow",
            ))
        elif self.data_catalog.available():
            console.print("[dim]  ✓ AI data catalog ready (profiled descriptions + join map)[/]")
        else:
            console.print("[dim]  ✓ Schema catalog ready (run sml profile for richer context)[/]")

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
        # Start Langfuse trace now that we have the ticket key
        self.langfuse.start_trace(self.session_id, ctx.ticket.key)
        _human_approval(console, "Beat 1 · BA Agent (Pull Ticket)", "Beat 2 · BA Agent: Clarification")

        # ── Check for existing demo state (resume after crash) ─────────
        demo_state = DemoState.load(self.config.chroma_path, ctx.ticket.key)
        if demo_state and demo_state.completed_beats:
            console.print(Panel(
                f"[bold]Previous session found for {ctx.ticket.key}.[/]\n"
                f"Completed beats: [cyan]{', '.join(demo_state.completed_beats)}[/]\n"
                f"Last updated: [dim]{demo_state.updated_at}[/]\n\n"
                "[dim]Type [bold green]resume[/] to continue from last beat\n"
                "Type [bold yellow]restart[/] to start fresh[/]",
                title="[cyan]↩ Resume Available[/]",
                border_style="cyan",
            ))
            choice = Prompt.ask("[cyan]Resume or restart?[/]").strip().lower()
            if choice != "resume":
                demo_state = DemoState.new(self.config.chroma_path, ctx.ticket.key, self.session_id)
        else:
            demo_state = DemoState.new(self.config.chroma_path, ctx.ticket.key, self.session_id)

        # ── Index Jira ticket into ChromaDB (codebase indexed separately via sml index) ──
        state = IndexState(self.config.chroma_path)
        ticket_text = (
            f"{ctx.ticket.summary}\n{ctx.ticket.description}\n"
            + "\n".join(f"{r.id}: {r.text}" for r in ctx.ticket.requirements)
        )

        if not state.is_codebase_indexed():
            console.print(Panel(
                "[yellow]Codebase not indexed.[/]\n\n"
                "[dim]Run:[/]  [bold]sml index[/]\n\n"
                "[dim]Beat 2–4 agents will have reduced context until you index.[/]",
                title="[yellow]⚠ Run sml index first[/]",
                border_style="yellow",
            ))

        if not state.is_ticket_indexed(ctx.ticket.key):
            console.print("[dim]  Indexing ticket into knowledge base...[/]")
            collection = get_collection(self.config.chroma_path)
            n = index_jira_ticket(collection, ctx.ticket.key, ticket_text)
            state.mark_ticket_indexed(ctx.ticket.key)
            self.metrics.set_chunks_indexed(n)
            self.metrics.emit_log("beat1", f"Ticket {ctx.ticket.key} indexed: {n} chunks")
        else:
            console.print(f"[dim]  ✓ Ticket {ctx.ticket.key} already in knowledge base[/]")
            self.metrics.set_chunks_indexed(0)
            self.metrics.emit_log("beat1", f"Ticket {ctx.ticket.key} already indexed")

        # Retriever (uses knowledge base built by sml index + ticket above)
        retriever = Retriever(self.config.chroma_path)

        # ── Beat 2: Clarify ───────────────────────────────────────────
        from demo.beats import beat2_clarify
        from demo.beats.beat2_clarify import ClarificationContext
        dashboard_server.emit("beat_start", {
            "beat_id": "beat2", "name": "Clarify",
            "timestamp": _ts(), "ticket_key": ctx.ticket.key,
        })

        if demo_state.is_done("beat2"):
            console.print("[dim]  ↩ Resuming — Beat 2 already done[/]")
            clarification = ClarificationContext(
                question=demo_state.get("beat2", "question", ""),
                answer=demo_state.get("beat2", "answer", ""),
                jira_comment_id=demo_state.get("beat2", "jira_comment_id", ""),
            )
        else:
            clarification = _run_beat_with_retry(
                console,
                lambda feedback=None: beat2_clarify.run(
                    ctx, self.jira, retriever, self.metrics, feedback=feedback
                ),
                "Beat 2 · BA Agent (Clarify)",
                "Beat 3 · Architect + Validate Spec Agent: Verify",
            )
            demo_state.mark_done(
                "beat2", self.config.chroma_path,
                question=clarification.question,
                answer=clarification.answer,
                jira_comment_id=clarification.jira_comment_id,
            )

        # ── Beat 3: Verify ────────────────────────────────────────────
        from demo.beats import beat3_verify
        from demo.beats.beat3_verify import VerifyContext
        dashboard_server.emit("beat_start", {"beat_id": "beat3", "name": "Verify", "timestamp": _ts()})

        if demo_state.is_done("beat3"):
            console.print("[dim]  ↩ Resuming — Beat 3 already done[/]")
            verify_ctx = VerifyContext()
            verify_ctx.build_ready_comment_id = demo_state.get("beat3", "build_ready_comment_id", "")
        else:
            verify_ctx = _run_beat_with_retry(
                console,
                lambda feedback=None: beat3_verify.run(
                    ctx, clarification, self.jira, self.codebase,
                    self.schema_catalog, self.metrics, self.session_id,
                    data_catalog=self.data_catalog, feedback=feedback,
                ),
                "Beat 3 · Verify (all checks passed, build-ready)",
                "Beat 4 · Developer + QA Agent: Build",
            )
            demo_state.mark_done(
                "beat3", self.config.chroma_path,
                build_ready_comment_id=verify_ctx.build_ready_comment_id,
            )

        # ── Beat 4: Build + Test ──────────────────────────────────────
        from demo.beats import beat4_build
        from demo.beats.beat4_build import BuildContext
        dashboard_server.emit("beat_start", {"beat_id": "beat4", "name": "Build + Test", "timestamp": _ts()})

        if demo_state.is_done("beat4"):
            console.print("[dim]  ↩ Resuming — Beat 4 already done[/]")
            build_ctx = BuildContext(
                notebook_path=demo_state.get("beat4", "notebook_path", ""),
                test_path=demo_state.get("beat4", "test_path", ""),
                sign_off_comment_id=demo_state.get("beat4", "sign_off_comment_id", ""),
            )
        else:
            build_ctx = _run_beat_with_retry(
                console,
                lambda feedback=None: beat4_build.run(
                    ctx, clarification, self.jira, self.metrics, self.repo_root, feedback=feedback
                ),
                "Beat 4 · Developer + QA Agent (Build)",
                "Beat 4b · Deploy Agent: Git + Databricks",
            )
            demo_state.mark_done(
                "beat4", self.config.chroma_path,
                notebook_path=build_ctx.notebook_path,
                test_path=build_ctx.test_path,
                sign_off_comment_id=build_ctx.sign_off_comment_id,
            )

        # ── Beat 4b: Deploy ───────────────────────────────────────────
        from demo.beats import beat4b_deploy
        from demo.beats.beat4b_deploy import DeployContext
        dashboard_server.emit("beat_start", {"beat_id": "beat4b", "name": "Deploy", "timestamp": _ts()})

        if demo_state.is_done("beat4b"):
            console.print("[dim]  ↩ Resuming — Beat 4b already done[/]")
            deploy_ctx = DeployContext(
                pr_url=demo_state.get("beat4b", "pr_url", ""),
                deployed=demo_state.get("beat4b", "deployed", False),
                job_ran=demo_state.get("beat4b", "job_ran", False),
            )
        else:
            deploy_ctx = _run_beat_with_retry(
                console,
                lambda feedback=None: beat4b_deploy.run(
                    ctx, self.metrics, self.repo_root, feedback=feedback
                ),
                "Beat 4b · Deploy Agent (Git + Bundle + Job)",
                "Beat 5 · Genie: Live Data Query",
            )
            demo_state.mark_done(
                "beat4b", self.config.chroma_path,
                pr_url=deploy_ctx.pr_url,
                deployed=deploy_ctx.deployed,
                job_ran=deploy_ctx.job_ran,
            )

        # ── Beat 5: Genie ─────────────────────────────────────────────
        from demo.beats import beat5_genie
        dashboard_server.emit("beat_start", {"beat_id": "beat5", "name": "Genie", "timestamp": _ts()})
        beat5_genie.run(self.genie, self.metrics)

        # ── Beat 6: Observe ───────────────────────────────────────────
        from demo.beats import beat6_observe
        dashboard_server.emit("beat_start", {"beat_id": "beat6", "name": "Observe", "timestamp": _ts()})
        beat6_observe.run(self.metrics, dashboard_url, self.langfuse)

        # ── Launch knowledge agent REPL ────────────────────────────────
        agent = ka.KnowledgeAgent(retriever, self.repo_root, self.metrics)
        ka.run_repl(agent)


def _run_beat_with_retry(
    con: Console,
    beat_fn: Callable,
    after: str,
    next_step: str,
) -> Any:
    """
    Run beat_fn(), show HITL approval.
    On reject: pass rejection reason as feedback to the next attempt (up to MAX_BEAT_RETRIES).
    After MAX_BEAT_RETRIES rejections: show a helpful exit panel and stop.

    beat_fn signature: beat_fn(feedback: str | None = None) -> result
    """
    feedback = None
    for attempt in range(1, MAX_BEAT_RETRIES + 1):
        result = beat_fn(feedback=feedback)
        approved, reason = _human_approval(
            con, after, next_step, attempt=attempt, max_retries=MAX_BEAT_RETRIES
        )
        if approved:
            return result
        # Rejected — use reason as feedback for next attempt
        feedback = reason if reason and reason != "No reason given" else None
        if attempt < MAX_BEAT_RETRIES:
            if feedback:
                con.print(f"[yellow]  ↩ Re-running agent with your feedback: \"{feedback}\"[/]")
            else:
                con.print(f"[yellow]  ↩ Re-running agent (attempt {attempt + 1}/{MAX_BEAT_RETRIES})...[/]")
        else:
            con.print(Panel(
                f"[bold]Agent could not produce an acceptable output after {MAX_BEAT_RETRIES} attempts.[/]\n\n"
                f"[dim]Last feedback:[/] {reason}\n\n"
                "[dim]Suggested next steps:\n"
                "  • Update the Jira ticket with clearer requirements\n"
                "  • Run [bold]sml index[/] to refresh the knowledge base\n"
                "  • Restart [bold]sml demo[/] with the same ticket — progress is saved and will resume[/]",
                title="[red]⛔ Maximum Retries Reached[/]",
                border_style="red",
            ))
            raise SystemExit(0)


def _human_approval(
    con: Console,
    after: str,
    next_step: str,
    attempt: int = 1,
    max_retries: int = 1,
) -> tuple[bool, str]:
    """Pause for explicit human approval. Returns (approved, reason)."""
    con.print()
    retry_msg = (
        f"\n[dim]Attempt {attempt}/{max_retries}. "
        "Rejecting will re-run the agent with your feedback.[/]"
        if max_retries > 1
        else ""
    )
    con.print(Panel(
        f"[bold]{after}[/] complete.{retry_msg}\n\n"
        f"[dim]Type [bold green]approve[/] to hand off to [cyan]{next_step}[/]\n"
        f"Type [bold red]reject: <reason>[/] to re-run the agent.[/]",
        title="[bold yellow]⏸  Human-in-the-Loop Approval[/]",
        border_style="yellow",
    ))
    while True:
        response = Prompt.ask("[bold yellow]Your decision[/]").strip().lower()
        if response == "approve":
            con.print(f"[green]  ✓ Approved — handing off to {next_step}[/]\n")
            return True, ""
        elif response.startswith("reject"):
            reason = response[6:].lstrip(": ").strip() or "No reason given"
            con.print(f"[yellow]  ↩ Rejected: {reason}[/]")
            return False, reason
        else:
            con.print("[dim]  Please type 'approve' or 'reject: <reason>'[/]")


def _ts() -> str:
    import time
    return time.strftime("%H:%M:%S")
