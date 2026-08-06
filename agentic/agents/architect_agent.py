from __future__ import annotations
"""
architect_agent.py
Architect Agent — reviews and finalizes all 6 spec files produced by BA Agent.
Adds: partitioning strategy, SCD2 configuration, FK validation chain, Iceberg config.
Does NOT generate code — only refines YAML specs.
"""

from pathlib import Path

import anthropic
from rich.console import Console

from agents import context_loader
from agents.context_loader import AgentContext

from agents.context_loader import PROJECT_ROOT
REPO_ROOT = PROJECT_ROOT
console = Console()
PROMPT_FILE = Path(__file__).parent / "prompts" / "architect_agent.md"


def run(ctx: AgentContext) -> str:
    client = anthropic.Anthropic()

    system_prompt = context_loader.build_system_prompt(ctx, "architect_agent")
    system_prompt += f"\n\n{PROMPT_FILE.read_text()}"

    specs_summary = _summarize_specs(ctx)

    user_prompt = context_loader.build_user_prompt(
        ctx,
        "architect_agent",
        extra=f"""
## Current Spec Files (BA Agent Output)
{specs_summary}

## Your Task
Review the spec files above. For each layer (bronze, silver, gold):
1. Confirm or correct partitioning strategy
2. Confirm or correct SCD2 configuration (which tables get SCD2)
3. Confirm or correct FK validation chain (product_id as the join key)
4. Confirm Iceberg UniForm is enabled on all tables
5. Flag any missing columns, wrong types, or architectural issues

Output revised spec blocks using the same ### FILE: labels.
If a spec file is correct, output it unchanged.
After all specs, write a short ARCHITECTURAL DECISIONS summary.
""",
    )

    console.print("[dim]  Calling Claude API (Architect Agent)...[/]")
    message = anthropic.Anthropic().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        messages=[{"role": "user", "content": user_prompt}],
        system=system_prompt,
    )

    output = message.content[0].text

    # Update spec files with architect's revisions
    from agents.ba_agent import _write_spec_files
    _write_spec_files(ctx, output)

    return output


def _summarize_specs(ctx: AgentContext) -> str:
    """Read all 6 spec files and return them as a formatted string."""
    specs_dir = REPO_ROOT / "use-cases" / ctx.use_case_name / "specs"
    parts = []
    for layer in ("bronze", "silver", "gold"):
        for fname in ("tables.yaml", "rules.yaml"):
            fpath = specs_dir / layer / fname
            if fpath.exists():
                content = fpath.read_text(encoding="utf-8")
                parts.append(f"### FILE: specs/{layer}/{fname}\n```yaml\n{content}\n```")
    return "\n\n".join(parts) if parts else "(No spec files yet — BA Agent has not run)"
