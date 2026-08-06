from __future__ import annotations
"""
pipeline.py
Core BMAD agent orchestrator — state machine that drives agents through approval gates.
Reads/writes use-cases/<name>/agent_state.yaml to track progress across sessions.

Stage order:
  ba_review → architect_review → code_gen → qa → doc → deploy → done
"""

import sys
import yaml
from pathlib import Path
from datetime import datetime, timezone

import anthropic
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

from agents import context_loader, ba_agent, architect_agent, code_gen_agent
from agents import test_gen_agent, doc_gen_agent, deploy_agent

console = Console()
from agents.context_loader import PROJECT_ROOT
REPO_ROOT = PROJECT_ROOT

STAGE_ORDER = [
    "ba_review",
    "architect_review",
    "code_gen",
    "qa",
    "doc",
    "deploy",
    "done",
]

STAGE_LABELS = {
    "ba_review":       "BA Agent",
    "architect_review":"Architect Agent",
    "code_gen":        "Developer Agent",
    "qa":              "QA Agent",
    "doc":             "Doc Agent",
    "deploy":          "Deploy Agent",
}


def run(use_case_name: str):
    """Main entry point. Reads agent_state.yaml, resumes from current stage."""
    console.print(f"\n[bold cyan][SML][/] Loading context for [bold]{use_case_name}[/]...")

    ctx = _load_context(use_case_name)
    state = _load_state(use_case_name)

    _print_context_summary(ctx)

    current_stage = state.get("current_stage", "ba_review")

    while current_stage != "done":
        console.print(f"\n[bold yellow][{STAGE_LABELS.get(current_stage, current_stage).upper()}][/] Starting...")

        output = _run_stage(current_stage, ctx, state)

        _show_output(output, STAGE_LABELS.get(current_stage, current_stage))

        # Human approval gate
        approved, feedback = _wait_for_approval(current_stage)

        _record_iteration(state, current_stage, output, feedback)
        _save_state(use_case_name, state)

        if approved:
            current_stage = _advance_stage(state, current_stage)
            # Reload context after spec files may have been written
            ctx = _load_context(use_case_name)
        else:
            # Re-run same stage with feedback
            console.print(f"\n[yellow]Feedback recorded. Re-running {STAGE_LABELS[current_stage]}...[/]")

    console.print("\n[bold green][SML] All stages complete![/]")
    console.print("Merge the PR, then run:")
    console.print("  [bold]sml deploy --use-case <name>[/]")
    console.print("  [bold]sml run    --use-case <name> --job bronze_ingest_job[/]")


def _run_stage(stage: str, ctx, state: dict) -> str:
    if stage == "deploy":
        pr_url = deploy_agent.create_pr(ctx.use_case_name)
        return pr_url or "Branch pushed. Create PR manually if gh CLI is unavailable."

    agent_map = {
        "ba_review":       ba_agent.run,
        "architect_review":architect_agent.run,
        "code_gen":        code_gen_agent.run,
        "qa":              test_gen_agent.run,
        "doc":             doc_gen_agent.run,
    }
    fn = agent_map.get(stage)
    if fn is None:
        return f"Unknown stage: {stage}"
    return fn(ctx)


def _wait_for_approval(stage: str) -> tuple[bool, str]:
    gate_num = STAGE_ORDER.index(stage) + 1
    console.print(
        f"\n[bold]\[GATE {gate_num}][/] Review the output above.\n"
        "  Type [green]approve[/] to continue\n"
        "  Type [red]reject: <your feedback>[/] to revise"
    )
    while True:
        response = Prompt.ask(">").strip()
        if response.lower() == "approve":
            return True, ""
        elif response.lower().startswith("reject:"):
            feedback = response[len("reject:"):].strip()
            if feedback:
                return False, feedback
            console.print("[red]Please provide feedback after 'reject:'[/]")
        else:
            console.print("[yellow]Please type 'approve' or 'reject: <feedback>'[/]")


def _advance_stage(state: dict, current_stage: str) -> str:
    idx = STAGE_ORDER.index(current_stage)
    next_stage = STAGE_ORDER[idx + 1]
    state["current_stage"] = next_stage
    state["stages"][current_stage]["status"] = "approved"
    return next_stage


def _record_iteration(state: dict, stage: str, output: str, feedback: str):
    stage_state = state["stages"].setdefault(stage, {"status": "pending", "history": []})
    stage_state.setdefault("history", []).append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "output": output,
        "feedback": feedback,
    })
    stage_state["status"] = "approved" if not feedback else "rejected"
    stage_state["iterations"] = len(stage_state["history"])


def _load_context(use_case_name: str):
    return context_loader.load(use_case_name)


def _load_state(use_case_name: str) -> dict:
    # State is stored AT THE USE-CASE LEVEL: project/use-cases/<name>/agent_state.yaml
    # It is gitignored (see project/.gitignore) — never committed, persists locally
    # Delete this file to restart the pipeline from scratch for a use case
    state_path = REPO_ROOT / "use-cases" / use_case_name / "agent_state.yaml"
    if state_path.exists():
        with open(state_path) as f:
            return yaml.safe_load(f) or {}
    return {
        "use_case_name": use_case_name,
        "current_stage": "ba_review",
        "stages": {},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _save_state(use_case_name: str, state: dict):
    state_path = REPO_ROOT / "use-cases" / use_case_name / "agent_state.yaml"
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(state_path, "w") as f:
        yaml.dump(state, f, default_flow_style=False)


def _print_context_summary(ctx):
    console.print(f"  [green]✓[/] CLAUDE.md loaded")
    console.print(f"  [green]✓[/] skills/ loaded ({len(ctx.skills)} files)")
    console.print(f"  [green]✓[/] request.yaml loaded")
    known_issues_size = len(ctx.global_known_issues) + len(ctx.use_case_known_issues)
    if known_issues_size > 0:
        console.print(f"  [green]✓[/] known_issues.md loaded")


def _show_output(output: str, agent_name: str):
    console.print(Panel(output, title=f"[bold]{agent_name.upper()} OUTPUT[/]", expand=True))
