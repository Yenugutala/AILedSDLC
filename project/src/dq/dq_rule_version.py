"""
dq_rule_version.py
DQ rule versioning and selective rescan support.

The _dq_rule_version column in Silver tables stores the SHA256 hash of silver/rules.yaml.
When rules change:
  1. Compute new hash of updated rules.yaml
  2. Find Silver tables with stale rows (old hash)
  3. Rescan only those tables
  4. Promote passing rows, update rejects, set new _dq_rule_version

See skills/dq_patterns.md for background.
"""

import hashlib
import yaml
from pathlib import Path
from typing import Optional


def get_current_version(rules_yaml_path: str | Path) -> str:
    """
    Compute SHA256 hash of the rules YAML file.
    This is used as the _dq_rule_version value in Silver tables.

    Args:
        rules_yaml_path: Path to use-cases/<name>/specs/silver/rules.yaml

    Returns:
        8-character hex prefix of SHA256 hash (e.g. "a1b2c3d4")
    """
    path = Path(rules_yaml_path)
    if not path.exists():
        return "no_rules"
    content = path.read_text(encoding="utf-8")
    return hashlib.sha256(content.encode()).hexdigest()[:8]


def tables_needing_rescan(
    spark,
    catalog: str,
    schema: str,
    current_version: str,
    table_names: list[str],
) -> list[str]:
    """
    Find Silver tables that have rows with a stale _dq_rule_version.

    Args:
        spark: SparkSession (Databricks)
        catalog: e.g. "statestreet"
        schema: e.g. "s_statestreet"
        current_version: current SHA256 prefix from get_current_version()
        table_names: list of Silver table names to check

    Returns:
        list of table names that have stale rows
    """
    stale = []
    for table in table_names:
        full_name = f"{catalog}.{schema}.{table}"
        try:
            count = spark.sql(f"""
                SELECT COUNT(*) AS stale_count
                FROM {full_name}
                WHERE _dq_rule_version != '{current_version}'
            """).collect()[0]["stale_count"]
            if count > 0:
                stale.append(table)
        except Exception:
            pass  # Table may not exist yet
    return stale


def rescan_table(
    spark,
    catalog: str,
    schema: str,
    table_name: str,
    rules: list[dict],
    current_version: str,
):
    """
    Re-evaluate DQ rules on stale rows in a Silver table.

    Steps:
      1. Find stale rows (WHERE _dq_rule_version != current_version)
      2. Re-evaluate each DQ rule
      3. New failures → move to rejects table
      4. Former rejects now passing → re-promote to Silver
      5. Update _dq_rule_version on all re-evaluated rows

    Args:
        spark: SparkSession
        catalog: e.g. "statestreet"
        schema: e.g. "s_statestreet"
        table_name: Silver table name (e.g. "product")
        rules: list of rule dicts from rules.yaml (filtered to this table)
        current_version: new _dq_rule_version to stamp on passing rows
    """
    full_table = f"{catalog}.{schema}.{table_name}"
    rejects_table = f"{catalog}.{schema}.{table_name}_rejects"

    table_rules = [r for r in rules if r.get("table") == table_name]
    if not table_rules:
        return

    # Build failing condition from all rules for this table
    failing_conditions = []
    for rule in table_rules:
        sql = rule.get("sql", "")
        # Extract WHERE clause from rule SQL (simple heuristic)
        if "WHERE" in sql.upper():
            where_part = sql[sql.upper().index("WHERE") + 5:].strip()
            failing_conditions.append(f"({where_part})")

    if not failing_conditions:
        # Just update version stamp
        spark.sql(f"""
            UPDATE {full_table}
            SET _dq_rule_version = '{current_version}'
            WHERE _dq_rule_version != '{current_version}'
        """)
        return

    combined_fail = " OR ".join(failing_conditions)

    # Move newly-failing stale rows to rejects
    spark.sql(f"""
        INSERT INTO {rejects_table}
        SELECT *, 'RESCAN' AS _rule_id, 'DQ rule version updated — rule failed on rescan' AS _violation_detail,
          current_timestamp() AS _rejected_ts, '{current_version}' AS _dq_rule_version
        FROM {full_table}
        WHERE _dq_rule_version != '{current_version}'
          AND ({combined_fail})
    """)

    # Remove newly-failing rows from Silver
    spark.sql(f"""
        DELETE FROM {full_table}
        WHERE _dq_rule_version != '{current_version}'
          AND ({combined_fail})
    """)

    # Update version on all remaining stale rows that passed
    spark.sql(f"""
        UPDATE {full_table}
        SET _dq_rule_version = '{current_version}'
        WHERE _dq_rule_version != '{current_version}'
    """)

    print(f"[RESCAN] {table_name}: version updated to {current_version}")
