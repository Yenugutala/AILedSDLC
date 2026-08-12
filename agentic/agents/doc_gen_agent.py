from __future__ import annotations
"""
doc_gen_agent.py
Doc Agent — generates documentation and Genie column comments.

Outputs:
  - SQL COMMENT ON TABLE / COMMENT ON COLUMN statements for Gold tables (for Genie)
  - Updated skills/known_issues.md (appends any new patterns discovered)
  - Lineage doc: generated/docs/lineage.md
"""

from pathlib import Path

import anthropic
from rich.console import Console

from agents import context_loader
from agents.context_loader import AgentContext

from agents.context_loader import PROJECT_ROOT
REPO_ROOT = PROJECT_ROOT
console = Console()
PROMPT_FILE = Path(__file__).parent / "prompts" / "doc_agent.md"


def run(ctx: AgentContext) -> str:
    client = anthropic.Anthropic()
    (PROJECT_ROOT / "docs").mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "demo").mkdir(parents=True, exist_ok=True)

    system_prompt = context_loader.build_system_prompt(ctx, "doc_agent")
    system_prompt += f"\n\n{PROMPT_FILE.read_text()}"

    user_prompt = context_loader.build_user_prompt(ctx, "doc_agent", extra="""
## Your Task
Generate documentation for the Gold layer:

1. Genie comments (SQL): docs/genie_comments.sql
   COMMENT ON TABLE and COMMENT ON COLUMN for all 4 Gold tables:
   - statestreet.g_statestreet.dim_product
   - statestreet.g_statestreet.dim_legal_entity
   - statestreet.g_statestreet.fact_product_rating
   - statestreet.g_statestreet.fact_coupon_schedule
   Comments should be plain English, useful for Genie natural language queries.

2. Lineage doc: docs/lineage.md
   Markdown table showing: Source CSV → Bronze table → Silver table → Gold mart
   One row per table. Include join keys and transformation notes.

3. High-level design: docs/hld.md
   Architecture overview: ingestion → Bronze → Silver → Gold → Genie.
   Include data flow diagram (ASCII), layer responsibilities, and SCD2 strategy.

4. Demo overview: demo/demo-overview.md
   One-page executive summary of the pipeline for a demo audience.
   Cover: problem statement, solution architecture, key outputs, and how to run.

Label each file exactly: ### DOC FILE: docs/genie_comments.sql (etc.)
""")

    console.print("[dim]  Calling Claude API (Doc Agent)...[/]")
    console.print("[dim]  Generating Genie comments, lineage doc, HLD, and demo overview...[/]")
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
    output = "".join(output_chunks)
    _write_doc_files(output)
    return output


def _write_doc_files(output: str):
    file_map = {
        "docs/genie_comments.sql": PROJECT_ROOT / "docs" / "genie_comments.sql",
        "docs/lineage.md":         PROJECT_ROOT / "docs" / "lineage.md",
        "docs/hld.md":             PROJECT_ROOT / "docs" / "hld.md",
        "demo/demo-overview.md":   PROJECT_ROOT / "demo" / "demo-overview.md",
    }

    lines = output.split("\n")
    current_file = None
    current_lines = []

    for line in lines:
        if line.startswith("### DOC FILE:"):
            if current_file and current_lines:
                dest = file_map.get(current_file)
                if dest:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_text("\n".join(current_lines).strip() + "\n", encoding="utf-8")
                    console.print(f"  [green]✓[/] Written: {dest.relative_to(PROJECT_ROOT)}")
            current_file = line.replace("### DOC FILE:", "").strip()
            current_lines = []
        elif current_file:
            stripped = line.strip()
            if stripped in ("```sql", "```markdown", "```"):
                continue
            current_lines.append(line)

    if current_file and current_lines:
        dest = file_map.get(current_file)
        if dest:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text("\n".join(current_lines).strip() + "\n", encoding="utf-8")
            console.print(f"  [green]✓[/] Written: {dest.relative_to(PROJECT_ROOT)}")
