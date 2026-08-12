from __future__ import annotations
"""
ba_agent.py
Business Analyst Agent — generates 6 spec YAML files from request.yaml.

Output files:
  use-cases/<name>/specs/bronze/tables.yaml
  use-cases/<name>/specs/bronze/rules.yaml
  use-cases/<name>/specs/silver/tables.yaml
  use-cases/<name>/specs/silver/rules.yaml
  use-cases/<name>/specs/gold/tables.yaml
  use-cases/<name>/specs/gold/rules.yaml
"""

import yaml
from pathlib import Path

import anthropic
from rich.console import Console

from agents import context_loader
from agents.context_loader import AgentContext

from agents.context_loader import PROJECT_ROOT
REPO_ROOT        = PROJECT_ROOT   # spec files live in project/, not agentic/
PROJECT_ROOT = PROJECT_ROOT   # outputs also go into project/ folder
console = Console()

PROMPT_FILE = Path(__file__).parent / "prompts" / "ba_agent.md"


BA_SKILLS = {"data_engineering.md", "dq_patterns.md", "ontology.md", "data_dictionary.md", "schema_drift.md"}


def run(ctx: AgentContext) -> str:
    """Run BA Agent. Makes 3 separate API calls (one per layer) to avoid truncation."""
    client = anthropic.Anthropic()

    filtered_ctx = AgentContext(
        use_case_name=ctx.use_case_name,
        claude_md=ctx.claude_md,
        skills={k: v for k, v in ctx.skills.items() if k in BA_SKILLS},
        request=ctx.request,
        global_known_issues=ctx.global_known_issues,
        use_case_known_issues=ctx.use_case_known_issues,
        agent_state=ctx.agent_state,
        specs=ctx.specs,
    )

    system_prompt = context_loader.build_system_prompt(filtered_ctx, "ba_agent")
    system_prompt += f"\n\n{PROMPT_FILE.read_text()}"

    base_user_prompt = context_loader.build_user_prompt(
        filtered_ctx,
        "ba_agent",
        extra=_build_task_instruction(ctx),
    )

    all_outputs = []
    for layer in ("bronze", "silver", "gold"):
        console.print(f"\n[dim]  Calling Claude API (BA Agent — {layer} layer)...[/]")
        layer_prompt = (
            base_user_prompt
            + f"\n\n## This Call: Generate {layer.upper()} tables spec only\n"
            f"Output ONLY this file (no rules.yaml):\n"
            f"  ### FILE: specs/{layer}/tables.yaml\n"
        )
        output_chunks = []
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=16000,
            messages=[{"role": "user", "content": layer_prompt}],
            system=system_prompt,
        ) as stream:
            for text in stream.text_stream:
                print(text, end="", flush=True)
                output_chunks.append(text)
        print()
        layer_output = "".join(output_chunks)
        all_outputs.append(layer_output)
        _write_spec_files(ctx, layer_output)

    output = "\n\n".join(all_outputs)
    _write_docs(ctx)
    _write_spec_markdowns(ctx)
    return output


def _build_task_instruction(ctx: AgentContext) -> str:
    iteration = len(
        ctx.agent_state.get("stages", {}).get("ba_agent", {}).get("history", [])
    )

    if iteration == 0:
        return """
## Your Task
Analyze the use case request and generate the tables.yaml spec for the assigned layer.
Output a clearly labeled YAML code block with the header:
  ### FILE: specs/<layer>/tables.yaml

Do NOT generate rules.yaml — only tables.yaml.
After the file, briefly explain the key decisions made.

Follow CLAUDE.md strictly:
- Bronze schema: statestreet.b_statestreet
- Silver schema: statestreet.s_statestreet
- Gold schema: statestreet.g_statestreet
- Table names: keep original CSV file names exactly
- Gold: single wide/flat table, no dim_/fact_ prefix (e.g. securities_master)
"""
    else:
        feedback = ctx.agent_state["stages"]["ba_agent"]["history"][-1].get("feedback", "")
        return f"""
## Your Task (Revision {iteration + 1})
Human feedback on your previous output:
"{feedback}"

Address this feedback and produce revised spec files.
Label each file block exactly as before:
  ### FILE: specs/bronze/tables.yaml  (etc.)
"""


def _write_spec_files(ctx: AgentContext, output: str):
    """Parse YAML blocks from agent output and write spec files."""
    specs_dir = REPO_ROOT / "use-cases" / ctx.use_case_name / "specs"

    file_map = {
        "specs/bronze/tables.yaml": specs_dir / "bronze" / "tables.yaml",
        "specs/bronze/rules.yaml":  specs_dir / "bronze" / "rules.yaml",
        "specs/silver/tables.yaml": specs_dir / "silver" / "tables.yaml",
        "specs/silver/rules.yaml":  specs_dir / "silver" / "rules.yaml",
        "specs/gold/tables.yaml":   specs_dir / "gold"   / "tables.yaml",
        "specs/gold/rules.yaml":    specs_dir / "gold"   / "rules.yaml",
    }

    lines = output.split("\n")
    current_file = None
    current_lines = []
    inside_fence = False

    for line in lines:
        if line.startswith("### FILE:"):
            # Save previous block
            if current_file and current_lines:
                _save_yaml_block(file_map.get(current_file), current_lines)
            current_file = line.replace("### FILE:", "").strip()
            current_lines = []
            inside_fence = False
        elif current_file:
            stripped = line.strip()
            if stripped == "```yaml" and not inside_fence:
                inside_fence = True          # enter fence — don't capture this line
            elif stripped == "```" and inside_fence:
                inside_fence = False         # exit fence — stop capturing
            elif inside_fence:
                current_lines.append(line)   # only capture lines inside the fence

    # Save last block
    if current_file and current_lines:
        _save_yaml_block(file_map.get(current_file), current_lines)


def _save_yaml_block(dest_path: Path | None, lines: list[str]):
    if dest_path is None:
        return
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(lines).strip()
    if not content:
        return

    # Validate the new content parses as YAML before overwriting
    try:
        import yaml as _yaml
        _yaml.safe_load(content)
    except _yaml.YAMLError as e:
        console.print(f"  [red]✗ Skipping {dest_path.name} — agent output is not valid YAML: {e}[/]")
        return

    # If an existing valid file is longer, keep it — the agent output was truncated
    if dest_path.exists():
        existing = dest_path.read_text(encoding="utf-8")
        try:
            _yaml.safe_load(existing)
            if len(content) < len(existing) * 0.9:   # new content is >10% shorter
                console.print(f"  [yellow]~ Keeping existing {dest_path.name} (agent output appears truncated)[/]")
                return
        except _yaml.YAMLError:
            pass  # existing file is broken — overwrite it

    dest_path.write_text(content + "\n", encoding="utf-8")
    console.print(f"  [green]✓[/] Written: {dest_path.relative_to(REPO_ROOT)}")


def _write_docs(ctx: AgentContext):
    """Write business-requirement.md to docs/ and demo/ at the repo root."""
    request_text = yaml.dump(ctx.request, default_flow_style=False)
    use_case = ctx.request.get("use_case", ctx.use_case_name)
    description = ctx.request.get("description", "")
    tables = ctx.request.get("tables", [])
    tables_md = "\n".join(f"- `{t}`" for t in tables) if tables else "_See request.yaml_"

    content = f"""# Business Requirement — {use_case}

## Description
{description}

## Source Tables
{tables_md}

## Full Request
```yaml
{request_text}
```
"""
    for folder in ("docs", "demo"):
        dest = PROJECT_ROOT / folder / "business-requirement.md"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8")
        console.print(f"  [green]✓[/] Written: {dest.relative_to(PROJECT_ROOT)}")


def _write_spec_markdowns(ctx: AgentContext):
    """Write per-layer markdown spec summaries to specs/ at the repo root."""
    specs_dir = REPO_ROOT / "use-cases" / ctx.use_case_name / "specs"
    out_dir = PROJECT_ROOT / "specs"
    out_dir.mkdir(parents=True, exist_ok=True)

    for layer in ("bronze", "silver", "gold"):
        tables_path = specs_dir / layer / "tables.yaml"
        rules_path  = specs_dir / layer / "rules.yaml"
        if not tables_path.exists():
            continue

        tables_text = tables_path.read_text(encoding="utf-8")
        rules_text  = rules_path.read_text(encoding="utf-8") if rules_path.exists() else ""

        content = f"""# {layer.title()} Layer Spec — {ctx.use_case_name}

## tables.yaml
```yaml
{tables_text}
```

## rules.yaml
```yaml
{rules_text}
```
"""
        dest = out_dir / f"{layer}-spec.md"
        dest.write_text(content, encoding="utf-8")
        console.print(f"  [green]✓[/] Written: {dest.relative_to(PROJECT_ROOT)}")
