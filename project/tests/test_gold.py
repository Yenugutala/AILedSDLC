# tests/test_gold.py
"""
Gold layer tests — net_settlement_amount column validation.
Validates the new net_settlement_amount column added to
statestreet.g_statestreet.securities_master.

Run against live Databricks:
    pytest tests/test_gold.py -v

Requires either:
  - databricks-connect configured (.databrickscfg or env vars), OR
  - An active Databricks SparkSession (when run as a notebook job)
"""

import pytest
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Session fixture — prefers databricks-connect; falls back to local Spark
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Return a SparkSession connected to Databricks (via databricks-connect)
    or a local session when running inside a Databricks job cluster.
    """
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        # Already running on a Databricks cluster — reuse the active session
        return SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
COLUMN_NAME = "net_settlement_amount"

# Product types for which net_settlement_amount must be populated
APPLICABLE_SUB_TYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1 — Column exists in the table schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark: SparkSession) -> None:
    """
    net_settlement_amount must be present as a column in
    statestreet.g_statestreet.securities_master.

    Failure means the Gold notebook was not regenerated after the
    ticket was applied, or the COMMENT ON COLUMN DDL truncated before
    the column was added to the SELECT list.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert COLUMN_NAME in column_names, (
        f"Column '{COLUMN_NAME}' not found in {GOLD_TABLE}. "
        f"Present columns: {column_names}"
    )


# ---------------------------------------------------------------------------
# Test 2 — Column produces non-null values for BOND and MUNI rows
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_bonds_and_munis(spark: SparkSession) -> None:
    """
    For rows where sub_type IN ('BOND', 'MUNI') and both current_face_value
    and coupon_rate are available, net_settlement_amount must not be NULL.

    A fully-NULL result for applicable rows indicates the proxy formula
    (current_face_value × (1 + coupon_rate)) failed to resolve — most
    likely because the Silver coupon JOIN produced no matches or the
    column expression evaluated to NULL for every row.

    Acceptable: rows with NULL current_face_value or no coupon record
                remain NULL (see GOLD-005 in known_issues.md).
    """
    df = spark.table(GOLD_TABLE)

    # Eligible rows: BOND or MUNI sub_type where both inputs to the formula exist
    eligible = df.filter(
        f"sub_type IN {APPLICABLE_SUB_TYPES} "
        f"AND current_face_value IS NOT NULL "
        f"AND bond_latest_coupon_rate IS NOT NULL"
    )

    eligible_count = eligible.count()

    if eligible_count == 0:
        pytest.skip(
            "No BOND/MUNI rows with both current_face_value and "
            "bond_latest_coupon_rate populated — cannot validate formula output. "
            "Verify Bronze/Silver ingestion completed successfully."
        )

    null_count = eligible.filter(f"{COLUMN_NAME} IS NULL").count()

    assert null_count == 0, (
        f"{null_count} of {eligible_count} eligible BOND/MUNI rows have "
        f"NULL {COLUMN_NAME} despite current_face_value and coupon_rate being present. "
        f"Check the CASE expression in 05_gold_build.sql (see GOLD-005 in known_issues.md)."
    )
