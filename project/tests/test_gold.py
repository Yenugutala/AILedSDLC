# tests/test_gold.py
"""
Gold layer tests for the net_settlement_amount column added to
statestreet.g_statestreet.securities_master.

Runs against live Databricks using databricks-connect (preferred) or a
pre-configured SparkSession (CI / local with cluster proxy).
"""
import pytest

# ---------------------------------------------------------------------------
# Session-scoped Spark fixture
# ---------------------------------------------------------------------------

def _make_spark():
    """Return a SparkSession, preferring databricks-connect over plain Spark."""
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def spark():
    session = _make_spark()
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
NEW_COLUMN = "net_settlement_amount"

# Security sub-types for which the column formula is defined (BOND + MUNI)
APPLICABLE_SUB_TYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1 — Column exists in the Gold table schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark):
    """
    net_settlement_amount must be present in the Gold table schema.

    Rationale: verifies the DDL / CTAS was applied correctly and the column
    was not accidentally dropped in a subsequent schema migration.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert NEW_COLUMN in column_names, (
        f"Column '{NEW_COLUMN}' not found in {GOLD_TABLE}. "
        f"Present columns: {column_names}"
    )


# ---------------------------------------------------------------------------
# Test 2 — Column is non-null for BOND and MUNI security types
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_applicable_types(spark):
    """
    net_settlement_amount must be non-null for every BOND and MUNI row that
    has both current_face_value and a latest coupon_rate populated.

    The formula is:  current_face_value * (1.0 + coupon_rate)
    so any row where both inputs are non-null MUST produce a non-null result.
    Any null value for such a row indicates a computation defect.
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    # Rows where the formula's inputs are available → result must not be null
    eligible = df.filter(
        F.col("sub_type").isin(list(APPLICABLE_SUB_TYPES))
        & F.col("current_face_value").isNotNull()
        & F.col("bond_coupon_rate").isNotNull()          # latest_coupon CTE column
    )

    eligible_count = eligible.count()

    # Guard: if no eligible rows exist the test is inconclusive, not passing
    assert eligible_count > 0, (
        f"No BOND/MUNI rows with non-null current_face_value AND bond_coupon_rate "
        f"found in {GOLD_TABLE}. Cannot validate {NEW_COLUMN}. "
        "Check that Bronze → Silver → Gold pipeline ran successfully."
    )

    null_count = eligible.filter(F.col(NEW_COLUMN).isNull()).count()

    assert null_count == 0, (
        f"{null_count} of {eligible_count} eligible BOND/MUNI rows have a NULL "
        f"'{NEW_COLUMN}' despite non-null inputs. "
        "Expected: current_face_value * (1.0 + bond_coupon_rate) for all such rows."
    )
