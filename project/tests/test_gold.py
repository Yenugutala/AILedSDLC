"""
Gold layer tests — net_settlement_amount column validation.
Runs against live Databricks via databricks-connect or an active SparkSession.
"""
import pytest

# ---------------------------------------------------------------------------
# Session-scoped Spark fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Return a SparkSession.
    Prefers an already-running Databricks Connect session; falls back to a
    local session (useful for CI with a remote cluster configured via
    ~/.databrickscfg or DATABRICKS_* env vars).
    """
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
TARGET_COLUMN = "net_settlement_amount"
# Types for which the column must produce non-NULL values
APPLICABLE_SUB_TYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1 — column exists in the Gold table schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark):
    """
    net_settlement_amount must be present in securities_master schema.
    Fails fast if the Gold build did not include the column.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert TARGET_COLUMN in column_names, (
        f"Column '{TARGET_COLUMN}' not found in {GOLD_TABLE}. "
        f"Available columns: {sorted(column_names)}"
    )


# ---------------------------------------------------------------------------
# Test 2 — column is non-null for BOND and MUNI sub-types
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_debt_securities(spark):
    """
    Every BOND and MUNI row that has both current_face_value and a coupon
    record must have a non-NULL net_settlement_amount.

    The Gold formula is:
        current_face_value * (1.0 + coupon_rate)   [DECIMAL(18,6)]

    Rows where coupon_rate IS NULL are legitimately NULL and are excluded
    from this check via the filter on coupon_rate.
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    # Rows that MUST have a value: BOND/MUNI with both face value and coupon rate present
    eligible = df.filter(
        (F.col("sub_type").isin(list(APPLICABLE_SUB_TYPES)))
        & F.col("current_face_value").isNotNull()
        & F.col("latest_coupon_rate").isNotNull()
    )

    eligible_count = eligible.count()

    assert eligible_count > 0, (
        f"No eligible BOND/MUNI rows with both current_face_value and "
        f"latest_coupon_rate found in {GOLD_TABLE}. "
        "Check whether Bronze/Silver/Gold pipeline ran successfully."
    )

    null_count = eligible.filter(F.col(TARGET_COLUMN).isNull()).count()

    assert null_count == 0, (
        f"{null_count} of {eligible_count} BOND/MUNI rows with non-null "
        f"current_face_value and latest_coupon_rate still have NULL "
        f"'{TARGET_COLUMN}' in {GOLD_TABLE}. "
        "Verify the Gold formula: current_face_value * (1.0 + coupon_rate)."
    )
