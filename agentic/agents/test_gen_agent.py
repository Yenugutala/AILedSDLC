from __future__ import annotations
"""
test_gen_agent.py
QA Agent — generates tests for all layers.

  Bronze  → pytest (Python) — tests for bronze_loader.py + schema drift
  Silver  → SQL test queries — one SELECT per DQ rule that should return 0 rows in Silver
  Gold    → SQL test queries — row count checks, join integrity, grain checks
"""

from pathlib import Path

import anthropic
from rich.console import Console

from agents import context_loader
from agents.context_loader import AgentContext

from agents.context_loader import PROJECT_ROOT
REPO_ROOT     = PROJECT_ROOT
GENERATED_DIR = PROJECT_ROOT / "tests"   # project/ output folder
console = Console()
PROMPT_FILE = Path(__file__).parent / "prompts" / "qa_agent.md"


def run(ctx: AgentContext) -> str:
    client = anthropic.Anthropic()
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)

    system_prompt = context_loader.build_system_prompt(ctx, "qa_agent")
    system_prompt += f"\n\n{PROMPT_FILE.read_text()}"

    user_prompt = context_loader.build_user_prompt(ctx, "qa_agent", extra="""
## Your Task
Generate pytest files for all three layers. All files must be Python (.py) using pytest.

1. Bronze tests: test_bronze.py
   - Test schema matches spec
   - Test metadata columns are added (_ingestion_ts, _source_file, _batch_id, _row_hash)
   - Test idempotency (run twice → same row count)
   - Test schema drift: new column → auto-merged; breaking change → quarantined

2. Silver tests: test_silver.py
   - For each DQ rule in silver/rules.yaml: assert query returns 0 rows if rule passes
   - Test rejects table has rows that failed DQ
   - Test SCD2: is_current = TRUE has no duplicates per primary key

3. Gold tests: test_gold.py
   - Row count: g_dim_product matches Silver s_product count
   - Grain: g_fact_coupon_schedule has one row per bond per coupon payment date
   - No NULL product_id in any Gold table

Label each file exactly: ### TEST FILE: tests/test_bronze.py  (etc.)
""")

    console.print("[dim]  Calling Claude API (QA Agent)...[/]")
    console.print("[dim]  Generating pytest files for Bronze, Silver, Gold layers...[/]")
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
    _write_test_files(output)
    return output


def _write_test_files(output: str):
    file_map = {
        "tests/test_bronze.py": GENERATED_DIR / "test_bronze.py",
        "tests/test_silver.py": GENERATED_DIR / "test_silver.py",
        "tests/test_gold.py":   GENERATED_DIR / "test_gold.py",
    }

    lines = output.split("\n")
    current_file = None
    current_lines = []

    for line in lines:
        if line.startswith("### TEST FILE:"):
            if current_file and current_lines:
                dest = file_map.get(current_file)
                if dest:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_text("\n".join(current_lines).strip() + "\n", encoding="utf-8")
                    console.print(f"  [green]✓[/] Written: {dest.relative_to(PROJECT_ROOT)}")
            current_file = line.replace("### TEST FILE:", "").strip()
            current_lines = []
        elif current_file:
            stripped = line.strip()
            if stripped in ("```python", "```sql", "```"):
                continue
            current_lines.append(line)

    if current_file and current_lines:
        dest = file_map.get(current_file)
        if dest:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text("\n".join(current_lines).strip() + "\n", encoding="utf-8")
            console.print(f"  [green]✓[/] Written: {dest.relative_to(PROJECT_ROOT)}")
