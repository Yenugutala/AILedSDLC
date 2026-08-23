from __future__ import annotations
"""
demo/tools/codebase.py
Codebase read/search/patch utilities used by the demo beats and knowledge agent.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class CodeSnippet:
    path: str       # relative to repo root
    lines: str      # matching content
    context: str    # brief description of what this file is


class CodebaseRetriever:
    def __init__(self, repo_root: Path):
        self.root = repo_root
        self._project = repo_root / "project"
        self._use_case = self._project / "use-cases" / "securities-master"

    def find_column_in_specs(self, column_name: str) -> list[CodeSnippet]:
        """grep for a column name across all spec YAMLs and notebooks."""
        results = []
        for pattern in ["**/tables.yaml", "**/*.py", "**/*.sql"]:
            for fpath in self.root.glob(pattern):
                try:
                    text = fpath.read_text(errors="ignore")
                except OSError:
                    continue
                if column_name in text:
                    lines = [
                        ln for ln in text.splitlines()
                        if column_name in ln
                    ][:5]
                    results.append(CodeSnippet(
                        path=str(fpath.relative_to(self.root)),
                        lines="\n".join(lines),
                        context=f"Found '{column_name}' in {fpath.name}",
                    ))
        return results

    def read_gold_spec(self) -> dict:
        """Load gold/tables.yaml as a dict. Returns empty dict if missing."""
        path = self._use_case / "specs" / "gold" / "tables.yaml"
        if not path.exists():
            return {}
        return yaml.safe_load(path.read_text()) or {}

    def gold_spec_has_column(self, column_name: str) -> bool:
        """Return True if gold/tables.yaml contains the given column name."""
        spec = self.read_gold_spec()
        tables = spec.get("tables", [])
        for table in tables:
            for col in table.get("columns", []):
                if col.get("name") == column_name:
                    return True
        return False

    def patch_gold_spec(self, column_def: dict) -> None:
        """Append a column definition to the first table in gold/tables.yaml."""
        path = self._use_case / "specs" / "gold" / "tables.yaml"
        spec = yaml.safe_load(path.read_text()) if path.exists() else {}
        if not spec:
            spec = {}
        tables = spec.get("tables") or []
        if not tables:
            # Create a skeleton gold table matching CLAUDE.md conventions
            tables = [{
                "name": "securities_master",
                "catalog": "statestreet",
                "schema": "g_statestreet",
                "description": "Gold wide table — Securities Master analytics mart.",
                "columns": [],
            }]
            spec["tables"] = tables
        tables[0].setdefault("columns", []).append(column_def)
        path.write_text(yaml.dump(spec, default_flow_style=False, sort_keys=False))

    def reset_gold_spec_columns(self) -> None:
        """Clear all columns from gold/tables.yaml, keeping the table skeleton.

        Called at the start of each demo run so Check 3 always demonstrates
        the deliberate failure and recovery flow.
        """
        path = self._use_case / "specs" / "gold" / "tables.yaml"
        spec = yaml.safe_load(path.read_text()) if path.exists() else {}
        if not spec:
            spec = {}
        tables = spec.get("tables") or []
        if not tables:
            spec["tables"] = [{
                "name": "securities_master",
                "catalog": "statestreet",
                "schema": "g_statestreet",
                "description": "Gold wide table \u2014 Securities Master analytics mart.",
                "columns": [],
            }]
        else:
            for table in tables:
                table["columns"] = []
        path.write_text(yaml.dump(spec, default_flow_style=False, sort_keys=False))

    def read_claude_md(self) -> str:
        path = self.root / "CLAUDE.md"
        return path.read_text() if path.exists() else ""

    def get_bronze_schema_snippet(self, table: str) -> Optional[str]:
        """Return column block for a named table from bronze/tables.yaml."""
        path = self._use_case / "specs" / "bronze" / "tables.yaml"
        if not path.exists():
            return None
        spec = yaml.safe_load(path.read_text()) or {}
        for t in spec.get("tables", []):
            if t.get("name") == table:
                cols = t.get("columns", [])
                return "\n".join(
                    f"  - {c.get('name')}: {c.get('type', '')}  # {c.get('description', '')}"
                    for c in cols[:20]
                )
        return None

    def list_indexable_files(self) -> list[Path]:
        """Return all files that should be indexed into ChromaDB."""
        patterns = [
            "project/use-cases/**/*.yaml",
            "project/notebooks/*.py",
            "project/notebooks/*.sql",
            "project/tests/*.py",
            "project/docs/*.md",
            "project/skills/*.md",
            "CLAUDE.md",
        ]
        files = []
        for pat in patterns:
            files.extend(self.root.glob(pat))
        return [f for f in files if f.is_file()]
