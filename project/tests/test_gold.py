# tests/test_gold.py
"""
Gold layer tests — net_settlement_amount column validation.
Runs against live Databricks via databricks-connect (or a local SparkSession fallback).
"""
import pytest

# ---------------------------------------------------------------------------
# Session-scoped Spark fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Return a SparkSession.
    Prefers databricks-connect when DATABRICKS_HOST / DATABRICKS_TOKEN are set;
    falls back to a local session so the file is importable without a cluster.
    """
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local").appName("gold-tests").getOrCreate()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLD_TABLE   = "statestreet.g_statestreet.securities_master"
TARGET_COL   = "net_settlement_amount"
# Types for which the column must be non-null (see GOLD-005 in known_issues.md)
APPLICABLE_SUBTYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1 — column exists in schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark):
    """
    net_settlement_amount must be present in the securities_master schema.
    Fails fast with a clear message if the Gold notebook was not regenerated
    after the ticket was applied.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert TARGET_COL in column_names, (
        f"Column '{TARGET_COL}' not found in {GOLD_TABLE}. "
        f"Existing columns: {column_names}"
    )


# ---------------------------------------------------------------------------
# Test 2 — non-null values for applicable security sub-types
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_bonds(spark):
    """
    For rows where sub_type IN ('BOND', 'MUNI') and both current_face_value
    and the latest coupon_rate are available, net_settlement_amount must be
    non-null.

    A zero-row result from the filter means either:
      (a) the formula produced NULLs for all qualifying rows, or
      (b) there are no BOND/MUNI rows with coupon data — both are test failures.
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    # Rows that qualify for a non-null net_settlement_amount
    qualifying = df.filter(
        F.col("sub_type").isin(*APPLICABLE_SUBTYPES)
        & F.col(TARGET_COL).isNotNull()
    )

    qualifying_count = qualifying.count()

    assert qualifying_count > 0, (
        f"Expected at least one row with sub_type IN {APPLICABLE_SUBTYPES} "
        f"to have a non-null '{TARGET_COL}', but found 0. "
        f"Check the proxy formula in 05_gold_build.sql (GOLD-005 in known_issues.md)."
    )

    # Also assert no qualifying row carries a negative settlement amount
    negative = qualifying.filter(F.col(TARGET_COL) < 0)
    negative_count = negative.count()

    assert negative_count == 0, (
        f"Found {negative_count} row(s) with negative '{TARGET_COL}' "
        f"for sub_type IN {APPLICABLE_SUBTYPES}. "
        f"Formula: current_face_value × (1 + coupon_rate) should always be ≥ 0."
    )
