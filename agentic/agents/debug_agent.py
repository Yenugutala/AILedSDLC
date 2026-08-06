from __future__ import annotations
"""
debug_agent.py
Debug Agent — fetches Databricks job logs via MCP, classifies failures, generates fixes.

Triggered via: sml debug --use-case <name> --job <job_name>

Flow:
  1. Fetch logs from Databricks (via Databricks SDK / MCP)
  2. Load spec files + generated notebook that failed
  3. Classify failure type
  4. Generate fix (spec update or code patch)
  5. Human approval gate
  6. Apply fix → create PR → monitor re-run
"""

import json
from pathlib import Path
from typing import Literal

import anthropic
from rich.console import Console
from rich.prompt import Prompt
from rich.panel import Panel

from agents import context_loader, code_gen_agent

from agents.context_loader import PROJECT_ROOT
REPO_ROOT = PROJECT_ROOT
console = Console()

FailureType = Literal[
    "schema_mismatch",
    "dq_sql_error",
    "python_bug",
    "sql_logic_error",
    "known_issue",
]


def run(use_case_name: str, job_name: str):
    """Entry point for sml debug."""
    console.print(f"\n[bold cyan][DEBUG AGENT][/] Diagnosing: [bold]{job_name}[/]")

    ctx = context_loader.load(use_case_name)

    # 1. Fetch logs
    error_info = _fetch_job_logs(job_name)
    if not error_info:
        console.print("[red]Could not fetch job logs. Check Databricks MCP config.[/]")
        return

    console.print(f"[dim]  Error: {error_info['error'][:200]}...[/]")

    # 2. Classify + generate fix
    fix = _classify_and_fix(ctx, job_name, error_info)

    # 3. Show to human
    console.print(Panel(
        f"[bold]Root Cause:[/] {fix['description']}\n\n"
        f"[bold]Fix Type:[/] {fix['fix_type']}\n\n"
        f"[bold]Files to change:[/] {', '.join(fix.get('patch_files', []))}",
        title="[bold red]DEBUG AGENT — Proposed Fix[/]",
    ))

    # 4. Human approval
    console.print("\nDoes this fix look correct?")
    response = Prompt.ask("> (approve / reject: <feedback>)").strip()
    if not response.lower().startswith("approve"):
        console.print("[yellow]Fix rejected. Please re-run sml debug after investigating.[/]")
        return

    # 5. Apply fix
    _apply_fix(ctx, fix)

    # 6. Update known issues if new pattern
    if fix.get("known_issue_entry"):
        _update_known_issues(ctx, fix["known_issue_entry"])

    console.print("\n[bold green]Fix applied![/] Raise a PR and merge to redeploy.")
    console.print(f"After merge, re-run the job: [cyan]sml run --use-case {use_case_name} --job {job_name}[/]")


def _fetch_job_logs(job_name: str) -> dict | None:
    """
    Fetch logs from Databricks via SDK or MCP.
    Returns dict with keys: error, task_name, run_id, stack_trace.
    """
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        runs = list(w.jobs.list_runs(job_name_contains=job_name, limit=1))
        if not runs:
            console.print(f"[red]No runs found for job: {job_name}[/]")
            return None
        run_id = runs[0].run_id
        run_output = w.jobs.get_run_output(run_id=run_id)
        return {
            "run_id": run_id,
            "task_name": getattr(runs[0], "run_name", job_name),
            "error": getattr(run_output, "error", "Unknown error"),
            "stack_trace": getattr(run_output, "error_trace", ""),
        }
    except Exception as e:
        console.print(f"[yellow]Databricks SDK not configured: {e}[/]")
        console.print("[yellow]Falling back to manual error entry.[/]")
        error = Prompt.ask("Paste the error message from Databricks")
        return {
            "run_id": "manual",
            "task_name": job_name,
            "error": error,
            "stack_trace": "",
        }


def _classify_and_fix(ctx, job_name: str, error_info: dict) -> dict:
    """Use Claude to classify the failure and propose a fix."""
    client = anthropic.Anthropic()

    layer = _detect_layer(job_name)
    specs_dir = REPO_ROOT / "use-cases" / ctx.use_case_name / "specs" / layer
    tables_yaml = (specs_dir / "tables.yaml").read_text() if (specs_dir / "tables.yaml").exists() else "N/A"
    rules_yaml  = (specs_dir / "rules.yaml").read_text()  if (specs_dir / "rules.yaml").exists() else "N/A"

    prompt = f"""
You are a Debug Agent for a Databricks Securities Master Data lakehouse.

## Failing Job
Job: {job_name}
Layer: {layer}
Error: {error_info['error']}
Stack trace: {error_info['stack_trace'][:2000]}

## Spec Files for {layer} layer
tables.yaml:
{tables_yaml}

rules.yaml:
{rules_yaml}

## Known Issues
{ctx.global_known_issues}
{ctx.use_case_known_issues}

## Task
1. Classify the failure as one of:
   - schema_mismatch     (CSV column doesn't match spec)
   - dq_sql_error        (DQ rule SQL has a syntax or logic bug)
   - python_bug          (Bronze PySpark code bug)
   - sql_logic_error     (Silver/Gold SQL logic bug)
   - known_issue         (matches a pattern in known_issues.md)

2. Propose a specific fix. Output JSON:
{{
  "fix_type": "<one of the 5 types>",
  "description": "<1-2 sentence root cause>",
  "patch_files": ["<list of files to change>"],
  "spec_updates": {{"<file_path>": "<new content or null>"}},
  "code_patch": "<unified diff or null>",
  "known_issue_entry": "<new known_issues.md entry or null>"
}}
"""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    # Extract JSON from response
    text = message.content[0].text
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        return json.loads(text[start:end])
    except (ValueError, json.JSONDecodeError):
        return {
            "fix_type": "unknown",
            "description": text[:500],
            "patch_files": [],
            "spec_updates": {},
            "code_patch": None,
            "known_issue_entry": None,
        }


def _detect_layer(job_name: str) -> str:
    name = job_name.lower()
    if "bronze" in name:
        return "bronze"
    elif "silver" in name:
        return "silver"
    elif "gold" in name:
        return "gold"
    return "silver"  # default


def _apply_fix(ctx, fix: dict):
    """Apply spec updates or code patches."""
    for file_path, new_content in fix.get("spec_updates", {}).items():
        if new_content:
            dest = REPO_ROOT / file_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(new_content, encoding="utf-8")
            console.print(f"  [green]✓[/] Updated spec: {file_path}")

            # If spec changed, regenerate the affected layer's notebook
            layer = _detect_layer(file_path)
            console.print(f"[dim]  Regenerating {layer} notebook after spec change...[/]")
            code_gen_agent.run(ctx, layer_only=layer)


def _update_known_issues(ctx, entry: str):
    known_issues_path = REPO_ROOT / "use-cases" / ctx.use_case_name / "known_issues.md"
    existing = known_issues_path.read_text(encoding="utf-8") if known_issues_path.exists() else ""
    known_issues_path.write_text(existing + f"\n\n{entry}\n", encoding="utf-8")
    console.print("  [green]✓[/] Updated known_issues.md with new pattern")
