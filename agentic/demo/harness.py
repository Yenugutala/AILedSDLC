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

HITL accepts any natural language at approval prompts:
  "approve" / "yes" / "go ahead"        → proceed to next beat
  describe what's wrong                  → re-run current agent with your feedback (up to 3 times)
  "run deployment agent" / "skip to X"  → jump directly to any beat
  ambiguous input                        → Claude asks a clarifying question before acting
"""

import json as _json
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

_HITL_CLASSIFIER_SYSTEM = """You are the HITL (Human-in-the-Loop) interpreter for an AI-led SDLC demo.
The demo runs AI agents called beats:
  beat2  - BA Agent: Clarification (understands business requirements)
  beat3  - Verify Agent: validates specs and architecture
  beat4  - Developer Agent + QA Agent: generates notebooks and tests
  beat4b - Deploy Agent: commits to Git, deploys Databricks bundle, runs gold job
  beat5  - Genie: natural-language query on live data
  beat6  - Observe: KPI dashboard

Classify the user's message into one action. Return ONLY valid JSON (no markdown, no code fences):
{
  "action": "approve" or "reject" or "jump" or "question",
  "beat_id": "beat2" or "beat3" or "beat4" or "beat4b" or "beat5" or "beat6" or null,
  "reason": "rejection reason or null",
  "clarifying_question": "question to ask user or null"
}

Examples:
  "yes / go ahead / looks good / approve" -> {"action": "approve", "beat_id": null, "reason": null, "clarifying_question": null}
  "the column name is wrong"              -> {"action": "reject", "beat_id": null, "reason": "the column name is wrong", "clarifying_question": null}
  "retry / try again / please try again"  -> {"action": "reject", "beat_id": null, "reason": "retry", "clarifying_question": null}
  "run the deployment agent"              -> {"action": "jump", "beat_id": "beat4b", "reason": null, "clarifying_question": null}
  "skip to genie"                         -> {"action": "jump", "beat_id": "beat5", "reason": null, "clarifying_question": null}
  "run the next step"                     -> {"action": "question", "beat_id": null, "reason": null, "clarifying_question": "Which agent? Options: Verify (beat3), Build (beat4), Deploy (beat4b), Genie (beat5)"}
"""


class JumpToBeat(Exception):
    """Raised when the user requests a jump to a specific beat at the HITL prompt."""
    def __init__(self, beat_id: str):
        self.beat_id = beat_id


def _classify_hitl_intent(user_input: str, con: Console) -> tuple[str, str]:
    """
    Call Claude Haiku to classify a free-form HITL response.
    Handles clarifying questions internally (up to 3 rounds).
    Returns: ("approve",""), ("reject", reason), ("jump", beat_id), or ("unknown","").
    """
    import anthropic
    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": user_input}]

    for _ in range(3):
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system=_HITL_CLASSIFIER_SYSTEM,
            messages=messages,
        )
        raw = resp.content[0].text.strip()
        # Strip markdown code fences if Haiku wraps the JSON despite instructions
        if "```" in raw:
            import re as _re
            m = _re.search(r"```(?:json)?\s*([\s\S]*?)```", raw)
            if m:
                raw = m.group(1).strip()
        try:
            data = _json.loads(raw)
        except _json.JSONDecodeError:
            con.print(f"[dim red]  (classifier returned malformed JSON — retrying)[/]")
            return "unknown", ""

        action = data.get("action", "")
        if action == "approve":
            return "approve", ""
        if action == "reject":
            return "reject", data.get("reason") or "No reason given"
        if action == "jump" and data.get("beat_id"):
            return "jump", data["beat_id"]

        # action == "question" — ask for clarification then loop
        question = data.get("clarifying_question") or "Could you clarify what you'd like to do?"
        con.print(f"\n[cyan]  🤖 {question}[/]")
        follow_up = Prompt.ask("[bold yellow]Your response[/]").strip()
        messages.append({"role": "assistant", "content": raw})
        messages.append({"role": "user", "content": follow_up})

    return "unknown", ""


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
        self.schema_catalog = SchemaCatalog(self.config.chroma_path)
        self.data_catalog = DataCatalog(self.config.chroma_path)
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
        self.langfuse.start_trace(self.session_id, ctx.ticket.key)

        # Beat 1 HITL — also handles jump commands
        beat1_jump = _human_approval(
            console, "Beat 1 · BA Agent (Pull Ticket)", "Beat 2 · BA Agent: Clarification"
        )
        jump_target: str | None = beat1_jump if beat1_jump else None

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

        # ── Index Jira ticket into ChromaDB ──────────────────────────
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

        retriever = Retriever(self.config.chroma_path)

        # Import context types for resume reconstruction
        from demo.beats import beat2_clarify, beat3_verify, beat4_build, beat4b_deploy
        from demo.beats.beat2_clarify import ClarificationContext
        from demo.beats.beat3_verify import VerifyContext
        from demo.beats.beat4_build import BuildContext
        from demo.beats.beat4b_deploy import DeployContext

        # ── Beat 2: Clarify ───────────────────────────────────────────
        dashboard_server.emit("beat_start", {
            "beat_id": "beat2", "name": "Clarify",
            "timestamp": _ts(), "ticket_key": ctx.ticket.key,
        })

        if jump_target and jump_target != "beat2":
            # Jumping past beat2 — use saved state or empty defaults
            console.print(f"[cyan]  ↷ Jumping past Beat 2 → {jump_target}[/]")
            clarification = ClarificationContext(
                question=demo_state.get("beat2", "question", ""),
                answer=demo_state.get("beat2", "answer", ""),
                jira_comment_id=demo_state.get("beat2", "jira_comment_id", ""),
            )
        elif demo_state.is_done("beat2"):
            console.print("[dim]  ↩ Resuming — Beat 2 already done[/]")
            clarification = ClarificationContext(
                question=demo_state.get("beat2", "question", ""),
                answer=demo_state.get("beat2", "answer", ""),
                jira_comment_id=demo_state.get("beat2", "jira_comment_id", ""),
            )
        else:
            if jump_target == "beat2":
                jump_target = None  # arrived at target
            try:
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
            except JumpToBeat as e:
                jump_target = e.beat_id
                clarification = ClarificationContext(
                    question=demo_state.get("beat2", "question", ""),
                    answer=demo_state.get("beat2", "answer", ""),
                    jira_comment_id=demo_state.get("beat2", "jira_comment_id", ""),
                )

        # ── Beat 3: Verify ────────────────────────────────────────────
        dashboard_server.emit("beat_start", {"beat_id": "beat3", "name": "Verify", "timestamp": _ts()})

        if jump_target and jump_target != "beat3":
            console.print(f"[cyan]  ↷ Jumping past Beat 3 → {jump_target}[/]")
            verify_ctx = VerifyContext()
            verify_ctx.build_ready_comment_id = demo_state.get("beat3", "build_ready_comment_id", "")
        elif demo_state.is_done("beat3"):
            console.print("[dim]  ↩ Resuming — Beat 3 already done[/]")
            verify_ctx = VerifyContext()
            verify_ctx.build_ready_comment_id = demo_state.get("beat3", "build_ready_comment_id", "")
        else:
            if jump_target == "beat3":
                jump_target = None
            try:
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
            except JumpToBeat as e:
                jump_target = e.beat_id
                verify_ctx = VerifyContext()
                verify_ctx.build_ready_comment_id = demo_state.get("beat3", "build_ready_comment_id", "")

        # ── Beat 4: Build + Test ──────────────────────────────────────
        dashboard_server.emit("beat_start", {"beat_id": "beat4", "name": "Build + Test", "timestamp": _ts()})

        if jump_target and jump_target != "beat4":
            console.print(f"[cyan]  ↷ Jumping past Beat 4 → {jump_target}[/]")
            build_ctx = BuildContext(
                notebook_path=demo_state.get("beat4", "notebook_path", ""),
                test_path=demo_state.get("beat4", "test_path", ""),
                sign_off_comment_id=demo_state.get("beat4", "sign_off_comment_id", ""),
            )
        elif demo_state.is_done("beat4"):
            console.print("[dim]  ↩ Resuming — Beat 4 already done[/]")
            build_ctx = BuildContext(
                notebook_path=demo_state.get("beat4", "notebook_path", ""),
                test_path=demo_state.get("beat4", "test_path", ""),
                sign_off_comment_id=demo_state.get("beat4", "sign_off_comment_id", ""),
            )
        else:
            if jump_target == "beat4":
                jump_target = None
            try:
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
            except JumpToBeat as e:
                jump_target = e.beat_id
                build_ctx = BuildContext(
                    notebook_path=demo_state.get("beat4", "notebook_path", ""),
                    test_path=demo_state.get("beat4", "test_path", ""),
                    sign_off_comment_id=demo_state.get("beat4", "sign_off_comment_id", ""),
                )

        # ── Beat 4b: Deploy ───────────────────────────────────────────
        dashboard_server.emit("beat_start", {"beat_id": "beat4b", "name": "Deploy", "timestamp": _ts()})

        if jump_target == "beat4b":
            jump_target = None  # arrived at target — run it
            console.print(Panel(
                "[cyan]Jumped to Deploy Agent as requested.[/]\n"
                "[dim]Running Beat 4b directly...[/]",
                title="[cyan]↷ Jump Executed[/]",
                border_style="cyan",
            ))

        if demo_state.is_done("beat4b") and jump_target is None:
            console.print("[dim]  ↩ Resuming — Beat 4b already done[/]")
            deploy_ctx = DeployContext(
                pr_url=demo_state.get("beat4b", "pr_url", ""),
                deployed=demo_state.get("beat4b", "deployed", False),
                job_ran=demo_state.get("beat4b", "job_ran", False),
            )
        else:
            try:
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
            except JumpToBeat as e:
                jump_target = e.beat_id
                deploy_ctx = DeployContext()

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
    On reject: re-run with feedback (up to MAX_BEAT_RETRIES).
    On jump command: raise JumpToBeat(beat_id).
    After MAX_BEAT_RETRIES rejections: show helpful exit panel and stop.
    """
    feedback = None
    for attempt in range(1, MAX_BEAT_RETRIES + 1):
        result = beat_fn(feedback=feedback)
        action, payload = _human_approval(
            con, after, next_step, attempt=attempt, max_retries=MAX_BEAT_RETRIES
        )
        if action == "approve":
            return result
        if action == "jump":
            raise JumpToBeat(payload)
        # action == "reject"
        reason = payload
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
) -> tuple[str, str]:
    """
    Pause for explicit human approval.
    Returns ("approve", ""), ("reject", reason), or ("jump", beat_id).

    Free-form natural language is routed through Claude Haiku for intent classification.
    Ambiguous input triggers a clarifying question before acting.
    """
    con.print()
    retry_msg = (
        f"\n[dim]Attempt {attempt}/{max_retries}. "
        "Rejecting will re-run the agent with your feedback.[/]"
        if max_retries > 1
        else ""
    )
    con.print(Panel(
        f"[bold]{after}[/] complete.{retry_msg}\n\n"
        f"[dim]Type [bold green]approve[/] (or 'yes', 'go ahead') to proceed to [cyan]{next_step}[/]\n"
        f"Describe what's wrong to re-run the agent with your feedback.\n"
        f"Ask to run any agent by name (e.g. [italic]run deployment agent[/], "
        f"[italic]skip to Genie[/], [italic]go back to build[/]).[/]",
        title="[bold yellow]⏸  Human-in-the-Loop Approval[/]",
        border_style="yellow",
    ))
    while True:
        response = Prompt.ask("[bold yellow]Your decision[/]").strip()
        if response.lower() == "approve":
            con.print(f"[green]  ✓ Approved — handing off to {next_step}[/]\n")
            return "approve", ""
        # Route all other input through LLM intent classifier
        action, payload = _classify_hitl_intent(response, con)
        if action == "approve":
            con.print(f"[green]  ✓ Understood — proceeding to {next_step}[/]\n")
            return "approve", ""
        elif action == "reject":
            con.print(f"[yellow]  ↩ Re-running agent: {payload}[/]")
            return "reject", payload
        elif action == "jump":
            con.print(f"[cyan]  ↷ Jumping to {payload}...[/]")
            return "jump", payload
        con.print("[dim]  I didn't quite understand — please try again, or type 'approve' to proceed.[/]")


def _ts() -> str:
    import time
    return time.strftime("%H:%M:%S")
