"""
Gold layer tests — net_settlement_amount column validation.
Runs against live Databricks via databricks-connect (or a supplied SparkSession).

Usage:
    pytest tests/test_gold.py -v

Environment (databricks-connect):
    DATABRICKS_HOST  = https://<workspace>.azuredatabricks.net
    DATABRICKS_TOKEN = dapi...
    DATABRICKS_CLUSTER_ID = <cluster-id>   # or serverless
"""

import pytest

# ---------------------------------------------------------------------------
# Session-scoped SparkSession — prefers databricks-connect, falls back to local
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Return a SparkSession connected to Databricks (databricks-connect) if
    available, otherwise start a local session for CI dry-runs.
    """
    try:
        from databricks.connect import DatabricksSession
        session = DatabricksSession.builder.getOrCreate()
    except ImportError:
        from pyspark.sql import SparkSession
        session = (
            SparkSession.builder
            .master("local[2]")
            .appName("test-gold-net-settlement-amount")
            .getOrCreate()
        )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
COLUMN_UNDER_TEST = "net_settlement_amount"
# Security sub-types for which the column must be non-NULL
APPLICABLE_SUB_TYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1 — column exists in the Gold table schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark):
    """
    net_settlement_amount must be present in securities_master schema.

    Checks column presence at the schema level — no full table scan required.
    Fails fast if the Gold build notebook omitted the column entirely.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert COLUMN_UNDER_TEST in column_names, (
        f"Column '{COLUMN_UNDER_TEST}' not found in {GOLD_TABLE}.\n"
        f"Available columns: {sorted(column_names)}"
    )


# ---------------------------------------------------------------------------
# Test 2 — column is non-NULL for BOND and MUNI rows
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_debt_securities(spark):
    """
    net_settlement_amount must be non-NULL for every BOND and MUNI row that
    has both current_face_value and a coupon record (i.e. the proxy formula
    inputs are available).

    Logic mirrors the Gold formula:
        net_settlement_amount = current_face_value × (1 + coupon_rate)

    Rows where current_face_value IS NULL or coupon_rate IS NULL are excluded
    from the assertion — they are legitimately NULL by design (zero-coupon
    bonds, or bonds with no coupon record ingested yet).
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    # Rows where the formula inputs ARE available — result must be non-NULL
    eligible = df.filter(
        F.col("sub_type").isin(*APPLICABLE_SUB_TYPES)
        & F.col("current_face_value").isNotNull()
        & F.col("latest_coupon_rate").isNotNull()
    )

    eligible_count = eligible.count()

    # If there are no eligible rows at all, the test data set may be empty —
    # surface a clear message rather than silently passing.
    assert eligible_count > 0, (
        f"No {'/'.join(APPLICABLE_SUB_TYPES)} rows found in {GOLD_TABLE} "
        f"with both current_face_value and latest_coupon_rate populated. "
        f"Verify that Bronze → Silver → Gold pipeline has been run and "
        f"that coupon data was ingested."
    )

    null_count = eligible.filter(
        F.col(COLUMN_UNDER_TEST).isNull()
    ).count()

    assert null_count == 0, (
        f"{null_count} out of {eligible_count} eligible "
        f"{'/'.join(APPLICABLE_SUB_TYPES)} rows have NULL '{COLUMN_UNDER_TEST}'. "
        f"Expected 0 — check the CASE expression in 05_gold_build.sql "
        f"for sub_type IN {APPLICABLE_SUB_TYPES}."
    )
