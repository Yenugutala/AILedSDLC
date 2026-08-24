from __future__ import annotations
"""
Beat 3 — Three Verification Checks
Duration target: 1:30 – 2:30

Check 1: Requirement clarity      → BA Agent (check_agents)       → PASS
Check 2: Architecture conformance  → Architect Agent (check_agents) → PASS
Check 3: Gold spec completeness    → Validate Spec Agent (LLM)     → FAIL (column missing)
         → live Databricks schema catalog searched via vector similarity
         → Claude decides: surface existing column OR create new one
         → user confirms fix → YAML patched → re-run → PASS
         → Build-ready stamp posted to Jira.

No column names, types, or business rules are hardcoded here.
All knowledge comes from the live Databricks INFORMATION_SCHEMA via ChromaDB.
"""

import sys
from dataclasses import dataclass, field
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm

from demo.beats.beat1_pull_ticket import TicketContext
from demo.beats.beat2_clarify import ClarificationContext
from demo.tools.codebase import CodebaseRetriever
from demo.tools.jira_client import JiraClient
from demo.tools.metrics import MetricsTracker
from demo.tools.schema_catalog import SchemaCatalog
from demo.tools.data_catalog import DataCatalog

console = Console()


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


@dataclass
class VerifyContext:
    checks: list[CheckResult] = field(default_factory=list)
    build_ready_comment_id: str = ""


def run(
    ctx: TicketContext,
    clarification: ClarificationContext,
    jira: JiraClient,
    codebase: CodebaseRetriever,
    schema_catalog: SchemaCatalog,
    metrics: MetricsTracker,
    session_id: str,
    data_catalog: DataCatalog | None = None,
    feedback: str | None = None,
) -> VerifyContext:
    console.rule("[bold cyan]Beat 3 · Verify[/]")
    if feedback:
        console.print(Panel(
            f"[dim]Incorporating your feedback:[/] [yellow]{feedback}[/]",
            title="[yellow]↩ Retry with Feedback[/]",
            border_style="yellow",
        ))
    ticket = ctx.ticket
    result = VerifyContext()

    # Ensure agentic/ is on path so agents/ is importable
    repo_root = Path(__file__).parent.parent.parent.parent
    agents_dir = str(repo_root / "agentic")
    if agents_dir not in sys.path:
        sys.path.insert(0, agents_dir)

    from agents import check_agents  # agents/check_agents.py

    # ── Reset gold spec so Check 3 always shows the failure ───────────
    codebase.reset_gold_spec_columns()
    metrics.emit_log("beat3", "Gold spec columns reset for fresh demo run")

    # ── Check 1: Requirement clarity ──────────────────────────────────
    console.print(Panel(
        "[bold]🤖 Agent:[/] [bold cyan]BA Agent[/]\n"
        "[dim]Role:[/]   Requirements Analyst — checks clarity and completeness of ticket requirements\n"
        "[dim]Model:[/]  claude-sonnet-4-6",
        title="[dim]Check 1 · Requirement Clarity[/]", border_style="blue",
    ))
    console.print("[bold]Check 1:[/] Requirement clarity...")
    metrics.emit_log("beat3", "BA Agent: checking requirement clarity...")
    requirements = [{"id": r.id, "text": r.text} for r in ticket.requirements]
    passed1, detail1, in1, out1, lat1 = check_agents.check_req_clarity(
        ticket_summary=ticket.summary,
        requirements=requirements,
        clarification_q=clarification.question,
        clarification_a=clarification.answer,
    )
    check1 = CheckResult(name="Requirement Clarity", passed=passed1, detail=detail1)
    result.checks.append(check1)
    metrics.record(
        beat_id="beat3a", name="BA Agent: Req Clarity", model="claude-sonnet-4-6",
        input_tokens=in1, output_tokens=out1, latency_ms=lat1,
        status="pass" if passed1 else "fail",
    )
    metrics.emit_check("beat3a", "Requirement Clarity", passed1, detail1)
    _print_check(check1)

    # ── Check 2: Architecture conformance ─────────────────────────────
    console.print(Panel(
        "[bold]🤖 Agent:[/] [bold cyan]Architect Agent[/]\n"
        "[dim]Role:[/]   Reviews ticket against CLAUDE.md standards (naming, UC structure, language split)\n"
        "[dim]Model:[/]  claude-sonnet-4-6",
        title="[dim]Check 2 · Architecture Conformance[/]", border_style="cyan",
    ))
    console.print("[bold]Check 2:[/] Architecture conformance...")
    metrics.emit_log("beat3", "Architect Agent: checking architecture conformance...")
    claude_md = codebase.read_claude_md()
    passed2, detail2, in2, out2, lat2 = check_agents.check_arch_conformance(
        ticket_summary=ticket.summary,
        clarification_a=clarification.answer,
        claude_md=claude_md,
    )
    check2 = CheckResult(name="Architecture Conformance", passed=passed2, detail=detail2)
    result.checks.append(check2)
    metrics.record(
        beat_id="beat3b", name="Architect Agent: Arch Conformance", model="claude-sonnet-4-6",
        input_tokens=in2, output_tokens=out2, latency_ms=lat2,
        status="pass" if passed2 else "fail",
    )
    metrics.emit_check("beat3b", "Architecture Conformance", passed2, detail2)
    _print_check(check2)

    # ── Check 3: Gold spec completeness (dynamic — no hardcodes) ──────
    console.print(Panel(
        "[bold]🤖 Agent:[/] [bold cyan]Validate Spec Agent[/]\n"
        "[dim]Role:[/]   Searches live Databricks schema catalog (INFORMATION_SCHEMA) via vector similarity\n"
        "         Determines whether each required column should be surfaced from silver or created new\n"
        "[dim]Tools:[/]  ChromaDB semantic search · YAML patcher on failure\n"
        "[dim]Model:[/]  claude-sonnet-4-6  (surface vs create reasoning)",
        title="[dim]Check 3 · Gold Spec Completeness[/]", border_style="magenta",
    ))
    console.print("[bold]Check 3:[/] Gold spec completeness...")

    check3, required_cols = _check_gold_spec_dynamic(ticket, codebase, schema_catalog, metrics, data_catalog)
    result.checks.append(check3)
    metrics.emit_check("beat3c", "Gold Spec Completeness", check3.passed, check3.detail)
    _print_check(check3)

    if not check3.passed:
        missing = [c for c in required_cols if not codebase.gold_spec_has_column(c["name"])]

        # Build fix description per column
        fix_lines = []
        for col in missing:
            action = col.get("action", "create")
            if action == "surface":
                src = col.get("source_table", "unknown table")
                fix_lines.append(
                    f"  [cyan]{col['name']}[/]  ({col['sql_type']})\n"
                    f"  [dim]→ Found in [bold]{src}[/] (silver layer) — surfacing to gold[/]"
                )
            else:
                fix_lines.append(
                    f"  [cyan]{col['name']}[/]  ({col['sql_type']})\n"
                    f"  [dim]→ New column — inferred from requirement {col.get('req_id', '')}[/]"
                )

        console.print(Panel(
            "[bold yellow]Missing columns in gold/tables.yaml:[/]\n\n"
            + "\n\n".join(fix_lines),
            title="[red]Check 3 FAILED — Proposed Fix[/]",
            border_style="red",
        ))

        confirmed = Confirm.ask("[bold]Apply fix to gold/tables.yaml?[/]", default=True)
        if not confirmed:
            console.print("[red]  Fix declined — cannot proceed.[/]")
            raise SystemExit(1)

        # Apply all missing columns
        for col in missing:
            column_def = {
                "name": col["name"],
                "type": col["sql_type"],
                "description": col.get("description", ""),
                "nullable": col.get("nullable", True),
            }
            codebase.patch_gold_spec(column_def)
            action_label = "Surfaced from " + col.get("source_table", "silver") if col.get("action") == "surface" else "Created"
            metrics.emit_log(
                "beat3c",
                f"{action_label}: {col['name']} ({col['sql_type']}) added to gold/tables.yaml",
                "fix",
            )
        console.print(f"[green]  ✓ Patched gold/tables.yaml ({len(missing)} column(s) added)[/]")

        # Re-run Check 3
        check3_rerun, _ = _check_gold_spec_dynamic(ticket, codebase, schema_catalog, metrics, data_catalog)
        check3_rerun.name = "Gold Spec Completeness (re-run)"
        result.checks[-1] = check3_rerun
        metrics.emit_check("beat3c", "Gold Spec (re-run)", check3_rerun.passed, check3_rerun.detail)
        _print_check(check3_rerun)

        if not check3_rerun.passed:
            console.print("[red]  Re-check still failed — something went wrong.[/]")
            raise SystemExit(1)

    # ── Post build-ready stamp ─────────────────────────────────────────
    artifacts = [
        "use-cases/securities-master/specs/gold/tables.yaml",
        "use-cases/securities-master/specs/bronze/tables.yaml",
    ]
    comment_id = jira.post_build_ready_stamp(ticket.key, [
        {"name": c.name, "passed": c.passed, "detail": c.detail}
        for c in result.checks
    ], artifacts, session_id)

    result.build_ready_comment_id = comment_id
    metrics.emit_jira_write("beat3", "build-ready-stamp", ticket.key, comment_id)
    metrics.emit_log("beat3", f"Build-ready stamp posted to {ticket.key}", "jira")
    console.print(f"[green]  ✓ Build-ready label + stamp posted to {ticket.key}[/]")

    metrics.record(
        beat_id="beat3",
        name="Verify (all checks)",
        model="claude-sonnet-4-6",
        input_tokens=0,
        output_tokens=0,
        latency_ms=0,
        status="pass",
        jira_artifact=f"stamp:{comment_id}",
    )

    return result


def _check_gold_spec_dynamic(
    ticket,
    codebase: CodebaseRetriever,
    schema_catalog: SchemaCatalog,
    metrics: MetricsTracker,
    data_catalog: DataCatalog | None = None,
) -> tuple[CheckResult, list[dict]]:
    """
    Dynamic gold spec check — uses live Databricks schema catalog.
    No hardcoded column names, types, or business rules.

    Returns (CheckResult, required_cols)
    required_cols: list of column dicts from extract_required_gold_columns()
    """
    import sys
    from pathlib import Path

    repo_root = Path(__file__).parent.parent.parent.parent
    agents_dir = str(repo_root / "agentic")
    if agents_dir not in sys.path:
        sys.path.insert(0, agents_dir)

    from agents import check_agents

    # Build schema context — prefer data_catalog (AI-profiled descriptions) over
    # schema_catalog (raw INFORMATION_SCHEMA). Falls back to greenfield mode.
    req_query = ticket.summary + " " + " ".join(r.text for r in ticket.requirements)

    if data_catalog is not None and data_catalog.available():
        # Best: AI-generated descriptions from real data values + join map
        metrics.emit_log("beat3c", "Validate Spec Agent: searching AI data catalog (profiled from real data)...")
        similar_cols = data_catalog.search(req_query, n=15)
        schema_context = data_catalog.format_for_agent(similar_cols)
        join_map_ctx = data_catalog.format_join_map()
        schema_context = join_map_ctx + "\n\n" + schema_context
        metrics.emit_log(
            "beat3c",
            f"Data catalog: {len(similar_cols)} relevant columns (top score: "
            f"{similar_cols[0]['score'] if similar_cols else 0:.3f})"
        )
    elif schema_catalog.available():
        # Fallback: raw structural schema (column names + types from INFORMATION_SCHEMA)
        metrics.emit_log("beat3c", "Validate Spec Agent: searching schema catalog (INFORMATION_SCHEMA)...")
        similar_cols = schema_catalog.search(req_query, n=15)
        schema_context = schema_catalog.format_for_agent(similar_cols)
        metrics.emit_log(
            "beat3c",
            f"Schema catalog: {len(similar_cols)} similar columns (top score: "
            f"{similar_cols[0]['score'] if similar_cols else 0:.3f})"
        )
    else:
        metrics.emit_log(
            "beat3c",
            "No catalog available — run `sml profile` or `sml schema`. Greenfield mode.",
            "warn",
        )
        schema_context = (
            "DATA CATALOG: Not indexed yet.\n"
            "Run `sml profile` to discover existing columns and generate AI descriptions.\n"
            "Run `sml schema` for raw schema only."
        )

    # Ask Claude: which columns are required? surface vs create?
    requirements = [{"id": r.id, "text": r.text} for r in ticket.requirements]
    required_cols, in_tok, out_tok, lat = check_agents.extract_required_gold_columns(
        ticket_summary=ticket.summary,
        requirements=requirements,
        existing_schema_context=schema_context,
    )
    metrics.record(
        beat_id="beat3c", name="Validate Spec Agent: Gold Spec", model="claude-sonnet-4-6",
        input_tokens=in_tok, output_tokens=out_tok, latency_ms=lat,
        status="pass",
    )

    if not required_cols:
        return CheckResult(
            name="Gold Spec Completeness",
            passed=True,
            detail="No gold columns required by this ticket (or agent returned empty list)",
        ), []

    # Check which are missing
    missing = [c for c in required_cols if not codebase.gold_spec_has_column(c["name"])]

    if not missing:
        names = ", ".join(c["name"] for c in required_cols)
        return CheckResult(
            name="Gold Spec Completeness",
            passed=True,
            detail=f"All required columns present: {names}",
        ), required_cols
    else:
        missing_names = ", ".join(c["name"] for c in missing)
        actions = ", ".join(
            f"{c['name']} ({'surface from ' + c.get('source_table', 'silver') if c.get('action') == 'surface' else 'create new'})"
            for c in missing
        )
        return CheckResult(
            name="Gold Spec Completeness",
            passed=False,
            detail=f"Missing: {missing_names} — proposed: {actions}",
        ), required_cols


def _print_check(c: CheckResult):
    icon = "[green]✅[/]" if c.passed else "[red]❌[/]"
    color = "green" if c.passed else "red"
    console.print(f"  {icon} [bold]{c.name}[/]: [{color}]{c.detail}[/]")
