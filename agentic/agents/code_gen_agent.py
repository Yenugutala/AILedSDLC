from __future__ import annotations
"""
code_gen_agent.py
Developer Agent — generates Databricks notebooks from approved spec files.

Language split (per CLAUDE.md):
  Bronze  → Python/PySpark notebook (03_bronze_ingest.py)
  Silver  → SQL notebook            (04_silver_conform.sql)
  Gold    → SQL notebook            (05_gold_build.sql)

Generated files are written to generated/notebooks/ (gitignored).
A GitHub PR is created at the end.
"""

import os
from pathlib import Path

import anthropic
from rich.console import Console

from agents import context_loader
from agents.context_loader import AgentContext

from agents.context_loader import PROJECT_ROOT
REPO_ROOT     = PROJECT_ROOT
GENERATED_DIR = PROJECT_ROOT / "notebooks"   # project/ output folder
console = Console()
PROMPT_FILE = Path(__file__).parent / "prompts" / "developer_agent.md"


def run(ctx: AgentContext, layer_only: str | None = None) -> str:
    """
    Generate notebooks for all layers (or a single layer if layer_only is set).
    Used by Debug Agent for partial regeneration after a fix.
    """
    client = anthropic.Anthropic()
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)

    system_prompt = context_loader.build_system_prompt(ctx, "code_gen_agent")
    system_prompt += f"\n\n{PROMPT_FILE.read_text()}"

    layers = [layer_only] if layer_only else ["bronze", "silver", "gold"]
    outputs = []

    for layer in layers:
        console.print(f"[dim]  Generating {layer} notebook...[/]")
        output = _generate_layer(client, ctx, system_prompt, layer)
        outputs.append(output)
        _write_notebook(ctx, layer, output)

    return "\n\n---\n\n".join(outputs)


def _generate_layer(client, ctx: AgentContext, system_prompt: str, layer: str) -> str:
    specs_dir = REPO_ROOT / "use-cases" / ctx.use_case_name / "specs" / layer
    tables_yaml = (specs_dir / "tables.yaml").read_text() if (specs_dir / "tables.yaml").exists() else ""
    rules_yaml  = (specs_dir / "rules.yaml").read_text()  if (specs_dir / "rules.yaml").exists() else ""

    lang = "Python/PySpark" if layer == "bronze" else "Databricks SQL"
    user_prompt = f"""
# Generate {layer.title()} Notebook ({lang})

## {layer.title()} tables.yaml
```yaml
{tables_yaml}
```

## {layer.title()} rules.yaml
```yaml
{rules_yaml}
```

## Task
Generate a complete Databricks notebook for the {layer} layer.
- Language: {lang}
- All code must be Databricks Runtime compatible
- Bronze: use MERGE INTO for idempotent loads; add _ingestion_ts, _source_file, _batch_id, _row_hash
- Silver: apply all DQ rules; write rejects; apply SCD2; set _dq_rule_version
- Gold: build dimensional marts; add COMMENT ON TABLE/COLUMN for Genie
- Output the notebook as a single code block prefixed: ### NOTEBOOK: {layer}
"""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        messages=[{"role": "user", "content": user_prompt}],
        system=system_prompt,
    )
    return message.content[0].text


def _write_notebook(ctx: AgentContext, layer: str, output: str):
    """Extract notebook code from agent output and write to generated/notebooks/."""
    ext_map = {"bronze": "py", "silver": "sql", "gold": "sql"}
    num_map = {"bronze": "03", "silver": "04", "gold": "05"}
    ext = ext_map[layer]
    num = num_map[layer]
    dest = GENERATED_DIR / f"{num}_{layer}_{'ingest' if layer == 'bronze' else 'conform' if layer == 'silver' else 'build'}.{ext}"

    # Extract content between ### NOTEBOOK: <layer> marker and closing ```
    content = _extract_code_block(output, f"### NOTEBOOK: {layer}")
    if content:
        dest.write_text(content, encoding="utf-8")
        console.print(f"  [green]✓[/] Written: {dest.relative_to(PROJECT_ROOT)}")
    else:
        # Fallback: write full output
        dest.write_text(output, encoding="utf-8")
        console.print(f"  [yellow]⚠[/] Written (raw): {dest.relative_to(PROJECT_ROOT)}")


def _extract_code_block(text: str, marker: str) -> str:
    lines = text.split("\n")
    inside = False
    code_lines = []
    for line in lines:
        if marker in line:
            inside = True
            continue
        if inside:
            if line.strip().startswith("```") and not code_lines:
                continue  # opening fence
            if line.strip() == "```" and code_lines:
                break       # closing fence
            code_lines.append(line)
    return "\n".join(code_lines).strip()
