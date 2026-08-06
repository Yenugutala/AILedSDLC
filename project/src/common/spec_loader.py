"""
spec_loader.py
Reads Bronze/Silver/Gold spec YAML files for use in generated notebooks.
Provides a clean interface so notebooks don't depend on file paths directly.

Usage in generated notebooks:
    from src.common.spec_loader import load_spec, get_table_spec

    spec = load_spec("securities-master", "bronze")
    table = spec.get_table("product")
    columns = table["columns"]
    pk = table["primary_key"]
"""

import yaml
from pathlib import Path
from typing import Optional


# When running in Databricks, repo root is mounted at workspace path.
# When running locally (tests), use the local repo root.
_REPO_ROOT = Path(__file__).parent.parent.parent


class LayerSpec:
    """Loaded spec for one layer (bronze, silver, or gold)."""

    def __init__(self, layer: str, tables_spec: dict, rules_spec: dict):
        self.layer = layer
        self.tables_spec = tables_spec or {}
        self.rules_spec = rules_spec or {}
        self.catalog = tables_spec.get("catalog", "statestreet")
        self.schema = tables_spec.get("schema", f"b_{layer}")

        # Index tables/marts by name
        self._tables = {
            t["name"]: t
            for t in (tables_spec.get("tables") or tables_spec.get("marts") or [])
        }

    def get_table(self, name: str) -> Optional[dict]:
        return self._tables.get(name)

    def list_tables(self) -> list[str]:
        return list(self._tables.keys())

    def get_rules(self, table_name: Optional[str] = None) -> list[dict]:
        rules = self.rules_spec.get("rules", [])
        if table_name:
            return [r for r in rules if r.get("table") == table_name]
        return rules

    def get_schema_drift_policy(self) -> dict:
        return self.rules_spec.get("schema_drift", {})

    @property
    def full_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"


def load_spec(use_case_name: str, layer: str) -> LayerSpec:
    """
    Load tables.yaml + rules.yaml for a given use case and layer.

    Args:
        use_case_name: e.g. "securities-master"
        layer: "bronze", "silver", or "gold"

    Returns:
        LayerSpec object with helper methods
    """
    specs_dir = _REPO_ROOT / "use-cases" / use_case_name / "specs" / layer

    tables_path = specs_dir / "tables.yaml"
    rules_path = specs_dir / "rules.yaml"

    tables_spec = {}
    if tables_path.exists():
        with open(tables_path) as f:
            tables_spec = yaml.safe_load(f) or {}

    rules_spec = {}
    if rules_path.exists():
        with open(rules_path) as f:
            rules_spec = yaml.safe_load(f) or {}

    return LayerSpec(layer=layer, tables_spec=tables_spec, rules_spec=rules_spec)


def get_table_spec(use_case_name: str, layer: str, table_name: str) -> Optional[dict]:
    """Convenience: load spec and return a single table's spec."""
    spec = load_spec(use_case_name, layer)
    return spec.get_table(table_name)
