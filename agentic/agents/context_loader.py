from __future__ import annotations
"""
context_loader.py
Merges all context sources into a single dict passed to every agent:
  - CLAUDE.md         (naming standards, coding rules, agent rules)
  - skills/           (8 knowledge base markdown files)
  - request.yaml      (use-case description + settings)
  - known_issues.md   (global: skills/known_issues.md + use-case specific)
  - agent_state.yaml  (current stage, iteration history, feedback)
"""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


# agentic/ is the parent of agents/; project/ is the sibling folder
AGENTIC_ROOT     = Path(__file__).parent.parent          # .../agentic/
PROJECT_ROOT     = AGENTIC_ROOT.parent / "project"      # .../project/
REPO_ROOT        = PROJECT_ROOT                          # alias used by rest of code
STATESTREET_ROOT = AGENTIC_ROOT.parent                  # .../StateStreetDemo/ (repo root for outputs)

SKILLS_DIR = PROJECT_ROOT / "skills"
CLAUDE_MD  = PROJECT_ROOT / "CLAUDE.md"


@dataclass
class AgentContext:
    use_case_name: str
    claude_md: str                          # Content of CLAUDE.md
    skills: dict[str, str]                  # filename → content for all skills/
    request: dict                           # Parsed request.yaml
    global_known_issues: str                # skills/known_issues.md
    use_case_known_issues: str             # use-cases/<name>/known_issues.md
    agent_state: dict                       # Parsed agent_state.yaml (may be empty)
    specs: dict[str, dict] = field(default_factory=dict)  # layer → {tables, rules}


def load(use_case_name: str) -> AgentContext:
    """Load and merge all context for a use case."""
    use_case_dir = REPO_ROOT / "use-cases" / use_case_name

    # 1. CLAUDE.md
    claude_md = _read_file(CLAUDE_MD)

    # 2. All skills/ files
    skills = {}
    if SKILLS_DIR.exists():
        for skill_file in sorted(SKILLS_DIR.glob("*.md")):
            skills[skill_file.name] = _read_file(skill_file)

    # 3. request.yaml
    request_path = use_case_dir / "request.yaml"
    if not request_path.exists():
        raise FileNotFoundError(
            f"request.yaml not found at {request_path}.\n"
            f"Create it first: cp use-cases/securities-master/request.yaml use-cases/{use_case_name}/request.yaml"
        )
    with open(request_path) as f:
        request = yaml.safe_load(f)

    # 4. Known issues (global + use-case)
    global_known_issues = _read_file(SKILLS_DIR / "known_issues.md")
    use_case_known_issues = _read_file(use_case_dir / "known_issues.md")

    # 5. agent_state.yaml (auto-managed, may not exist on first run)
    state_path = use_case_dir / "agent_state.yaml"
    agent_state = {}
    if state_path.exists():
        with open(state_path) as f:
            agent_state = yaml.safe_load(f) or {}

    # 6. Load specs if they exist (BA Agent output)
    specs = _load_specs(use_case_dir / "specs")

    return AgentContext(
        use_case_name=use_case_name,
        claude_md=claude_md,
        skills=skills,
        request=request,
        global_known_issues=global_known_issues,
        use_case_known_issues=use_case_known_issues,
        agent_state=agent_state,
        specs=specs,
    )


def _load_specs(specs_dir: Path) -> dict:
    specs = {}
    for layer in ("bronze", "silver", "gold"):
        layer_dir = specs_dir / layer
        specs[layer] = {}
        for fname in ("tables.yaml", "rules.yaml"):
            fpath = layer_dir / fname
            if fpath.exists():
                with open(fpath) as f:
                    specs[layer][fname.replace(".yaml", "")] = yaml.safe_load(f)
    return specs


def _read_file(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    return ""


def build_system_prompt(ctx: AgentContext, agent_name: str) -> str:
    """Build the full system prompt for an agent call."""
    skills_text = "\n\n".join(
        f"### {name}\n{content}" for name, content in ctx.skills.items()
    )
    return f"""
# CLAUDE.md (Company Standards)
{ctx.claude_md}

# Skills Knowledge Base
{skills_text}

# Global Known Issues
{ctx.global_known_issues}

# Use-Case Known Issues ({ctx.use_case_name})
{ctx.use_case_known_issues}
""".strip()


def build_user_prompt(ctx: AgentContext, agent_name: str, extra: str = "") -> str:
    """Build the user message for an agent, including request + iteration history."""
    request_text = yaml.dump(ctx.request, default_flow_style=False)
    state = ctx.agent_state.get("stages", {}).get(agent_name, {})
    iteration_history = _format_iteration_history(state)

    return f"""
# Use Case Request
```yaml
{request_text}
```

{iteration_history}

{extra}
""".strip()


def _format_iteration_history(stage_state: dict) -> str:
    if not stage_state:
        return ""
    history = stage_state.get("history", [])
    if not history:
        return ""
    lines = ["# Previous Iterations (most recent last)"]
    for i, entry in enumerate(history, 1):
        lines.append(f"\n## Iteration {i}")
        if entry.get("output"):
            lines.append(f"### Agent Output\n{entry['output']}")
        if entry.get("feedback"):
            lines.append(f"### Human Feedback\n{entry['feedback']}")
    return "\n".join(lines)
