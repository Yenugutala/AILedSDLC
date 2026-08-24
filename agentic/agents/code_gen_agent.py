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


def run(ctx: AgentContext, layer_only: str | None = None, feedback: str | None = None) -> str:
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
        output = _generate_layer(client, ctx, system_prompt, layer, feedback=feedback)
        outputs.append(output)
        _write_notebook(layer, output)

    return "\n\n---\n\n".join(outputs)


def _generate_layer(client, ctx: AgentContext, system_prompt: str, layer: str, feedback: str | None = None) -> str:
    specs_dir = REPO_ROOT / "use-cases" / ctx.use_case_name / "specs" / layer
    tables_yaml = (specs_dir / "tables.yaml").read_text() if (specs_dir / "tables.yaml").exists() else ""
    rules_yaml  = (specs_dir / "rules.yaml").read_text()  if (specs_dir / "rules.yaml").exists() else ""

    lang = "Python/PySpark" if layer == "bronze" else "Databricks SQL"

    # Check if an existing notebook already exists — if so, instruct minimal targeted change
    ext_map  = {"bronze": "py",     "silver": "sql",    "gold": "sql"}
    num_map  = {"bronze": "03",     "silver": "04",     "gold": "05"}
    name_map = {"bronze": "ingest", "silver": "conform", "gold": "build"}
    existing_path = GENERATED_DIR / f"{num_map[layer]}_{layer}_{name_map[layer]}.{ext_map[layer]}"
    existing_content = existing_path.read_text(encoding="utf-8") if existing_path.exists() else ""

    if existing_content:
        task_instruction = (
            f"Make the MINIMAL targeted change to the existing notebook below to satisfy the spec.\n"
            f"Only add or modify what the spec explicitly requires — do not rewrite, restructure, or expand scope.\n\n"
            f"## Existing Notebook\n```{ext_map[layer]}\n{existing_content}\n```"
        )
    else:
        task_instruction = (
            f"Generate a new Databricks notebook for the {layer} layer.\n"
            f"- Language: {lang}\n"
            f"- All code must be Databricks Runtime compatible\n"
            f"- Bronze: use MERGE INTO for idempotent loads; add _ingestion_ts, _source_file, _batch_id, _row_hash\n"
            f"- Silver: apply all DQ rules; write rejects; apply SCD2; set _dq_rule_version\n"
            f"- Gold: build dimensional marts; add COMMENT ON TABLE/COLUMN for Genie"
        )

    feedback_block = (
        f"\n\n## Human Feedback from Previous Attempt\n{feedback}\n"
        "Please address this feedback in your revised output."
    ) if feedback else ""

    user_prompt = f"""
# {layer.title()} Notebook ({lang})

## {layer.title()} tables.yaml
```yaml
{tables_yaml}
```

## {layer.title()} rules.yaml
```yaml
{rules_yaml}
```

## Task
{task_instruction}
{feedback_block}
- Output the notebook as a single code block prefixed: ### NOTEBOOK: {layer}
"""

    console.print(f"[dim]  Writing {layer} notebook ({lang})...[/]")
    output_chunks = []
    with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=8192,
        messages=[{"role": "user", "content": user_prompt}],
        system=system_prompt,
    ) as stream:
        for text in stream.text_stream:
            print(text, end="", flush=True)
            output_chunks.append(text)
    print()
    return "".join(output_chunks)


def _write_notebook(layer: str, output: str):
    """Extract notebook code from agent output and write to generated/notebooks/."""
    ext_map = {"bronze": "py", "silver": "sql", "gold": "sql"}
    num_map = {"bronze": "03", "silver": "04", "gold": "05"}
    ext = ext_map[layer]
    num = num_map[layer]
    dest = GENERATED_DIR / f"{num}_{layer}_{'ingest' if layer == 'bronze' else 'conform' if layer == 'silver' else 'build'}.{ext}"

    # Extract content between ### NOTEBOOK: <layer> marker and closing ```
    content = _extract_code_block(output, f"### NOTEBOOK: {layer}")
    if not content:
        # Fallback: strip any wrapping code fence from the raw output
        content = _strip_code_fence(output)
    dest.write_text(content + "\n", encoding="utf-8")
    console.print(f"  [green]✓[/] Written: {dest.relative_to(PROJECT_ROOT)}")


def _strip_code_fence(text: str) -> str:
    """Remove a wrapping ```...``` or ```sql/```python fence from text."""
    lines = text.strip().split("\n")
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


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
