"""
Gold layer tests — net_settlement_amount column validation.

Runs against live Databricks via databricks-connect (or a supplied SparkSession).
Execute with:
    pytest tests/test_gold.py -v
"""

import pytest

# ---------------------------------------------------------------------------
# Session-scoped SparkSession fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Return a SparkSession connected to the target Databricks workspace.

    Priority:
      1. databricks.connect  (remote execution via Databricks Connect)
      2. Local SparkSession  (unit-test fallback — table reads will fail unless
                              a Delta catalog is configured locally)
    """
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.appName("test-gold-net-settlement").getOrCreate()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
TARGET_COLUMN = "net_settlement_amount"

# Sub-types for which a non-null value is expected
APPLICABLE_SUB_TYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1: column exists in the Gold table schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark):
    """
    net_settlement_amount must be present in the Gold mart schema.

    Fails immediately if the Developer Agent did not add the column,
    giving a clear signal before any value-level assertions are attempted.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert TARGET_COLUMN in column_names, (
        f"Column '{TARGET_COLUMN}' not found in {GOLD_TABLE}. "
        f"Actual columns: {column_names}"
    )


# ---------------------------------------------------------------------------
# Test 2: column is non-null for BOND and MUNI rows
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_bonds_and_munis(spark):
    """
    net_settlement_amount must be non-null for every BOND and MUNI row
    that has both current_face_value and a coupon record (coupon_rate).

    Rows where current_face_value IS NULL or coupon_rate IS NULL are
    legitimately NULL and are excluded from this assertion.
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    # Rows that CAN produce a net_settlement_amount value
    eligible = df.filter(
        F.col("sub_type").isin(list(APPLICABLE_SUB_TYPES))
        & F.col("current_face_value").isNotNull()
        & F.col("latest_coupon_rate").isNotNull()   # bond_coupon_rate column in dim
    )

    eligible_count = eligible.count()

    # Guard: if there are no eligible rows at all, the test data set is
    # incomplete — fail explicitly rather than pass vacuously.
    assert eligible_count > 0, (
        f"No BOND/MUNI rows with non-null current_face_value and coupon_rate "
        f"found in {GOLD_TABLE}. Verify that Silver bond and coupon data loaded "
        f"correctly before re-running Gold tests."
    )

    null_count = eligible.filter(F.col(TARGET_COLUMN).isNull()).count()

    assert null_count == 0, (
        f"{null_count} of {eligible_count} eligible BOND/MUNI rows have a NULL "
        f"'{TARGET_COLUMN}' in {GOLD_TABLE}. "
        f"Expected formula: current_face_value × (1 + coupon_rate). "
        f"Check the Gold build notebook — column may be mis-joined or the CASE "
        f"predicate may be too restrictive."
    )
