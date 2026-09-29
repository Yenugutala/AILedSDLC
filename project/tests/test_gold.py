"""
Gold layer tests — net_settlement_amount column validation.
Runs against live Databricks using databricks-connect (or a supplied SparkSession).
"""
import pytest

# ---------------------------------------------------------------------------
# Session-scoped SparkSession fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Return a SparkSession.
    Prefers an already-running session (e.g. inside a Databricks notebook or
    a databricks-connect environment).  Falls back to a plain local session
    so the file can be imported / syntax-checked without credentials.
    """
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local").appName("test-gold").getOrCreate()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
NEW_COLUMN  = "net_settlement_amount"

# Sub-types for which the column must be non-null
# (BOND and MUNI only — as defined in GOLD-005)
APPLICABLE_SUB_TYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1 — column exists in schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark):
    """
    net_settlement_amount must be present in the securities_master schema.

    Checks the DataFrame schema rather than running a full table scan so the
    assertion is near-instant regardless of table size.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert NEW_COLUMN in column_names, (
        f"Column '{NEW_COLUMN}' not found in {GOLD_TABLE}.\n"
        f"Actual columns: {column_names}"
    )


# ---------------------------------------------------------------------------
# Test 2 — non-null values for BOND and MUNI rows
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_bonds_and_munis(spark):
    """
    For rows where sub_type IN ('BOND', 'MUNI'), net_settlement_amount must
    be non-null.

    Acceptable pre-conditions:
      - current_face_value IS NOT NULL
      - A matching coupon row exists (latest_coupon_rate IS NOT NULL)

    Rows that legitimately lack a coupon (zero-coupon bonds with no coupon
    record) are excluded from the assertion via the WHERE filter so the test
    remains deterministic against the sample dataset.
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    # Rows that must have a populated net_settlement_amount
    eligible = df.filter(
        (F.col("sub_type").isin(list(APPLICABLE_SUB_TYPES)))
        & F.col("current_face_value").isNotNull()
        & F.col("latest_coupon_rate").isNotNull()   # proxy column produced by latest_coupon CTE
    )

    total_eligible = eligible.count()
    assert total_eligible > 0, (
        f"No eligible BOND/MUNI rows found in {GOLD_TABLE} with both "
        f"current_face_value and latest_coupon_rate populated. "
        f"Check that Bronze → Silver → Gold pipeline has run successfully."
    )

    null_count = eligible.filter(F.col(NEW_COLUMN).isNull()).count()
    assert null_count == 0, (
        f"{null_count} of {total_eligible} eligible BOND/MUNI rows have "
        f"NULL {NEW_COLUMN} in {GOLD_TABLE}.\n"
        f"Expected 0 nulls where current_face_value and latest_coupon_rate "
        f"are both populated."
    )
