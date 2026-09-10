"""
Gold layer tests — net_settlement_amount column validation.
Runs against live Databricks via databricks-connect (or local SparkSession fallback).

Usage:
    pytest tests/test_gold.py -v
"""
import pytest

# ---------------------------------------------------------------------------
# Session-scoped Spark fixture — prefers Databricks Connect, falls back to local
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    try:
        from databricks.connect import DatabricksSession
        session = DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession
        session = (
            SparkSession.builder
            .master("local[2]")
            .appName("gold-tests")
            .getOrCreate()
        )
    yield session
    session.stop()


GOLD_TABLE = "statestreet.g_statestreet.securities_master"

# Security types for which net_settlement_amount must be populated
APPLICABLE_SUB_TYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1 — Column exists in the Gold table schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark):
    """
    net_settlement_amount must be present in the securities_master schema.
    Fails fast with a clear message if the column was never added to the DDL.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert "net_settlement_amount" in column_names, (
        f"Column 'net_settlement_amount' is missing from {GOLD_TABLE}. "
        f"Columns found: {sorted(column_names)}"
    )


# ---------------------------------------------------------------------------
# Test 2 — Column produces non-null values for BOND and MUNI security types
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_bonds_and_munis(spark):
    """
    For all rows where sub_type IN ('BOND', 'MUNI'), net_settlement_amount
    must not be NULL. Non-bond/muni rows are out of scope and may be NULL.
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    applicable_rows = df.filter(F.col("sub_type").isin(list(APPLICABLE_SUB_TYPES)))
    total = applicable_rows.count()

    assert total > 0, (
        f"No rows with sub_type IN {APPLICABLE_SUB_TYPES} found in {GOLD_TABLE}. "
        "Cannot validate net_settlement_amount. Check that Gold was built correctly."
    )

    null_count = applicable_rows.filter(F.col("net_settlement_amount").isNull()).count()

    assert null_count == 0, (
        f"{null_count} of {total} BOND/MUNI rows in {GOLD_TABLE} have "
        f"net_settlement_amount = NULL. "
        f"Expected 0 nulls for sub_type IN {APPLICABLE_SUB_TYPES}. "
        "Check that current_face_value and coupon_rate are populated in Silver "
        "and that the CASE expression in 05_gold_build.sql covers both sub-types."
    )
