from __future__ import annotations
"""
validate_spec_agent.py
CI validation — checks all spec YAML files against CLAUDE.md naming rules.
Called by: sml validate --use-case <name>
Called by: .github/workflows/validate-specs.yml
Exits with code 1 if any violations found (fails CI).
"""

import sys
import yaml
from pathlib import Path

from rich.console import Console
from rich.table import Table

from agents.context_loader import PROJECT_ROOT
REPO_ROOT = PROJECT_ROOT
console = Console()

VALID_BRONZE_SCHEMA = "b_statestreet"
VALID_SILVER_SCHEMA = "s_statestreet"
VALID_GOLD_SCHEMA   = "g_statestreet"
VALID_CATALOG       = "statestreet"


def run(use_case_name: str):
    specs_dir = REPO_ROOT / "use-cases" / use_case_name / "specs"
    violations = []

    for layer, schema in [("bronze", VALID_BRONZE_SCHEMA), ("silver", VALID_SILVER_SCHEMA), ("gold", VALID_GOLD_SCHEMA)]:
        tables_path = specs_dir / layer / "tables.yaml"
        if not tables_path.exists():
            violations.append(f"{layer}/tables.yaml: FILE MISSING")
            continue

        with open(tables_path) as f:
            spec = yaml.safe_load(f) or {}

        # Check catalog
        if spec.get("catalog") != VALID_CATALOG:
            violations.append(f"{layer}/tables.yaml: catalog should be '{VALID_CATALOG}', got '{spec.get('catalog')}'")

        # Check schema
        if spec.get("schema") != schema:
            violations.append(f"{layer}/tables.yaml: schema should be '{schema}', got '{spec.get('schema')}'")

        # Check Gold naming
        if layer == "gold":
            for mart in spec.get("marts", []):
                name = mart.get("name", "")
                if not (name.startswith("dim_") or name.startswith("fact_")):
                    violations.append(f"gold/tables.yaml: mart '{name}' must start with 'dim_' or 'fact_'")

        # Check Silver rejects table naming
        if layer == "silver":
            for table in spec.get("tables", []):
                rejects = table.get("rejects_table", "")
                if rejects and not rejects.endswith("_rejects"):
                    violations.append(f"silver/tables.yaml: rejects_table '{rejects}' must end with '_rejects'")

    # Report
    if violations:
        console.print(f"\n[bold red]VALIDATION FAILED — {len(violations)} violation(s):[/]")
        for v in violations:
            console.print(f"  [red]✗[/] {v}")
        sys.exit(1)
    else:
        console.print(f"\n[bold green]VALIDATION PASSED[/] — All spec files conform to CLAUDE.md")


def run(use_case_name: str):
    """Public entry point for sml validate and CI."""
    _validate(use_case_name)


def _validate(use_case_name: str):
    specs_dir = REPO_ROOT / "use-cases" / use_case_name / "specs"
    violations = []

    for layer, schema in [("bronze", VALID_BRONZE_SCHEMA), ("silver", VALID_SILVER_SCHEMA), ("gold", VALID_GOLD_SCHEMA)]:
        tables_path = specs_dir / layer / "tables.yaml"
        if not tables_path.exists():
            violations.append(f"{layer}/tables.yaml: FILE MISSING")
            continue

        with open(tables_path) as f:
            spec = yaml.safe_load(f) or {}

        if spec.get("catalog") != VALID_CATALOG:
            violations.append(f"{layer}/tables.yaml: catalog should be '{VALID_CATALOG}', got '{spec.get('catalog')}'")

        if spec.get("schema") != schema:
            violations.append(f"{layer}/tables.yaml: schema should be '{schema}', got '{spec.get('schema')}'")

        if layer == "gold":
            for mart in spec.get("marts", []):
                name = mart.get("name", "")
                if not (name.startswith("dim_") or name.startswith("fact_")):
                    violations.append(f"gold/tables.yaml: mart '{name}' must start with 'dim_' or 'fact_'")

        if layer == "silver":
            for table in spec.get("tables", []):
                rejects = table.get("rejects_table", "")
                if rejects and not rejects.endswith("_rejects"):
                    violations.append(f"silver/tables.yaml: rejects_table '{rejects}' must end with '_rejects'")

    if violations:
        console.print(f"\n[bold red]VALIDATION FAILED — {len(violations)} violation(s):[/]")
        for v in violations:
            console.print(f"  [red]✗[/] {v}")
        sys.exit(1)
    else:
        console.print(f"\n[bold green]VALIDATION PASSED[/]")
