from __future__ import annotations
"""
Beat 4b — Deploy Agent
Duration target: between Beat 4 (code gen) and Beat 5 (Genie)

Sub-steps:
  A: git commit + push + GitHub PR     (deploy_agent.create_pr)
  B: databricks bundle validate+deploy  (deploy_agent.deploy)
  C: trigger gold_mart_job + stream     (deploy_agent.trigger_job)

Gracefully skips steps B and C with a yellow panel if Databricks CLI is not configured.
"""

import sys
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from demo.beats.beat1_pull_ticket import TicketContext
from demo.tools.metrics import MetricsTracker

console = Console()


@dataclass
class DeployContext:
    pr_url: str = ""
    deployed: bool = False
    job_ran: bool = False


def run(
    ctx: TicketContext,
    metrics: MetricsTracker,
    repo_root: Path,
) -> DeployContext:
    console.rule("[bold cyan]Beat 4b · Deploy[/]")
    console.print(Panel(
        "[bold]🤖 Agent:[/] [bold cyan]Deploy Agent[/]\n"
        "[dim]Role:[/]   Publishes generated code to Git, deploys Databricks bundle, runs gold job\n"
        "[dim]Tools:[/]  GitPython · gh CLI · Databricks CLI (bundle run)\n"
        "[dim]Steps:[/]  git commit → push → PR → bundle deploy → trigger gold_mart_job → data is live",
        border_style="magenta",
    ))

    # Ensure agentic/ is on path so agents/ is importable
    agents_dir = str(repo_root / "agentic")
    if agents_dir not in sys.path:
        sys.path.insert(0, agents_dir)

    from agents import deploy_agent

    result = DeployContext()

    # ── Sub-step A: Git commit + PR ────────────────────────────────────
    console.print("[bold]Step A:[/] Publishing generated code to Git...")
    metrics.emit_log("beat4b", "Deploy Agent: committing and pushing generated files...")
    try:
        pr_url = deploy_agent.create_pr("securities-master")
        result.pr_url = pr_url
        if pr_url:
            metrics.emit_log("beat4b", f"PR created: {pr_url}", "git")
            console.print(Panel(
                f"[bold green]Pull Request:[/] {pr_url}",
                title="[dim]Git · PR Created[/]",
                border_style="green",
            ))
        else:
            metrics.emit_log("beat4b", "No new files to commit (or PR already exists)", "git")
            console.print("[dim]  Nothing new to commit — PR skipped.[/]")
    except Exception as e:
        console.print(f"[yellow]  ⚠ Git step skipped: {e}[/]")
        metrics.emit_log("beat4b", f"Git skipped: {e}", "warn")

    # ── Sub-step B: Databricks bundle deploy ───────────────────────────
    console.print("[bold]Step B:[/] Deploying Databricks bundle...")
    metrics.emit_log("beat4b", "Deploy Agent: databricks bundle validate + deploy...")
    try:
        deploy_agent.deploy("securities-master")
        result.deployed = True
        metrics.emit_log("beat4b", "Bundle deployed successfully", "deploy")
        console.print("[green]  ✓ Bundle deployed[/]")
    except Exception as e:
        console.print(Panel(
            "[yellow]Databricks bundle deploy skipped.[/]\n\n"
            f"[dim]Reason: {e}[/]\n\n"
            "[dim]To deploy manually run:[/]  databricks bundle deploy",
            title="[yellow]⚠ Databricks CLI not configured[/]",
            border_style="yellow",
        ))
        metrics.emit_log("beat4b", f"Deploy skipped: {e}", "warn")

    # ── Sub-step C: Trigger gold_mart_job ──────────────────────────────
    console.print("[bold]Step C:[/] Triggering gold_mart_job — streaming live output...")
    metrics.emit_log("beat4b", "Deploy Agent: triggering gold_mart_job...")
    try:
        deploy_agent.trigger_job("gold_mart_job")
        result.job_ran = True
        metrics.emit_log("beat4b", "gold_mart_job completed — data is live", "deploy")
        console.print(Panel(
            "[bold green]Data is live![/]\n"
            "[dim]statestreet.g_statestreet.securities_master[/] is populated and queryable via Genie.",
            title="[green]✓ gold_mart_job Succeeded[/]",
            border_style="green",
        ))
    except Exception as e:
        console.print(Panel(
            "[yellow]Job trigger skipped.[/]\n\n"
            f"[dim]Reason: {e}[/]\n\n"
            "[dim]To run manually:[/]  databricks bundle run gold_mart_job",
            title="[yellow]⚠ Job trigger skipped[/]",
            border_style="yellow",
        ))
        metrics.emit_log("beat4b", f"Job skipped: {e}", "warn")

    metrics.record(
        beat_id="beat4b",
        name="Deploy Agent",
        model=None,
        input_tokens=0,
        output_tokens=0,
        latency_ms=0,
        status="pass",
        jira_artifact=f"pr:{result.pr_url}" if result.pr_url else "",
    )

    return result
