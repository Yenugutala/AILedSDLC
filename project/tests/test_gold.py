"""
Gold layer tests — net_settlement_amount column validation.
Runs against live Databricks via databricks-connect (or a local SparkSession fallback).
"""
import pytest

# ---------------------------------------------------------------------------
# Session-scoped Spark fixture — prefers Databricks Connect, falls back to local
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession
        return (
            SparkSession.builder
            .master("local[2]")
            .appName("test-gold-net-settlement-amount")
            .getOrCreate()
        )


GOLD_TABLE = "statestreet.g_statestreet.securities_master"


# ---------------------------------------------------------------------------
# Test 1: column exists in the Gold table schema
# ---------------------------------------------------------------------------
def test_net_settlement_amount_column_exists(spark):
    """net_settlement_amount must be present in the securities_master schema."""
    df = spark.table(GOLD_TABLE)
    assert "net_settlement_amount" in df.columns, (
        f"Column 'net_settlement_amount' not found in {GOLD_TABLE}. "
        f"Actual columns: {df.columns}"
    )


# ---------------------------------------------------------------------------
# Test 2: column is non-null for BOND and MUNI rows that have a coupon record
# ---------------------------------------------------------------------------
def test_net_settlement_amount_non_null_for_bonds_with_coupon(spark):
    """
    net_settlement_amount must be non-null for every BOND or MUNI row
    where both current_face_value and latest_coupon_rate are populated.
    These are the rows for which the proxy formula is computable.
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    # Rows where the formula inputs are available — result must not be null
    computable = df.filter(
        (F.col("type") == "DEBT")
        & (F.col("sub_type").isin("BOND", "MUNI"))
        & F.col("current_face_value").isNotNull()
        & F.col("latest_coupon_rate").isNotNull()
    )

    total = computable.count()
    assert total > 0, (
        "No BOND/MUNI rows with both current_face_value and latest_coupon_rate found — "
        "cannot validate net_settlement_amount; check Silver data."
    )

    null_count = computable.filter(F.col("net_settlement_amount").isNull()).count()
    assert null_count == 0, (
        f"{null_count}/{total} BOND/MUNI rows with computable inputs have "
        f"net_settlement_amount = NULL in {GOLD_TABLE}."
    )
