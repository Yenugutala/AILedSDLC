# tests/test_gold.py
"""
Gold layer tests — net_settlement_amount column
Table: statestreet.g_statestreet.securities_master
Ticket: validates the new net_settlement_amount column added to the Gold mart.

Run against live Databricks:
    pytest tests/test_gold.py -v

Requirements:
    pip install pytest databricks-connect
    DATABRICKS_HOST and DATABRICKS_TOKEN must be set in the environment.
"""

import pytest

# ---------------------------------------------------------------------------
# SparkSession fixture — databricks-connect with local fallback
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Return a SparkSession connected to Databricks (databricks-connect).
    Falls back to a local session when databricks-connect is unavailable
    (e.g. unit-test runs in CI without a workspace configured).
    """
    try:
        from databricks.connect import DatabricksSession
        session = DatabricksSession.builder.getOrCreate()
    except Exception:                          # not installed or not configured
        from pyspark.sql import SparkSession
        session = (
            SparkSession.builder
            .master("local[2]")
            .appName("test_gold_net_settlement_amount")
            .getOrCreate()
        )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLD_TABLE   = "statestreet.g_statestreet.securities_master"
TARGET_COL   = "net_settlement_amount"
# sub_types for which the formula is defined (BOND and MUNI)
APPLICABLE_SUB_TYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1 — column exists in the schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark):
    """
    net_settlement_amount must be present in the Gold securities_master schema.

    Failure mode: Developer Agent generated the table without the column,
    or a schema migration dropped it.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert TARGET_COL in column_names, (
        f"Column '{TARGET_COL}' is missing from {GOLD_TABLE}.\n"
        f"Actual columns: {column_names}"
    )


# ---------------------------------------------------------------------------
# Test 2 — column is non-null for BOND and MUNI rows that have coupon data
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_bond_muni(spark):
    """
    net_settlement_amount must be non-null for every BOND or MUNI product
    that has both current_face_value and a coupon record (coupon_rate is
    available via the latest_coupon CTE in the Gold build).

    The formula applied in the Gold notebook is:
        current_face_value * (1.0 + coupon_rate)

    A NULL result for a qualifying row indicates either:
      - current_face_value was NULL in Silver/Bronze
      - coupon_rate had no matching coupon record (bond has no coupon rows)
      - a regression introduced NULLs into the computation

    Only rows where BOTH source inputs are expected to be present are checked,
    so we first confirm at least one such row exists before asserting.
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    # Rows that should have a non-null net_settlement_amount:
    # sub_type IN ('BOND','MUNI') AND current_face_value IS NOT NULL
    # AND net_settlement_amount IS NULL  →  these are violations
    qualifying = df.filter(
        F.col("sub_type").isin(*APPLICABLE_SUB_TYPES)
        & F.col("current_face_value").isNotNull()
    )

    qualifying_count = qualifying.count()

    # Guard: if no qualifying rows at all, skip rather than silently pass.
    # This catches the case where Silver is empty or sub_type column is wrong.
    if qualifying_count == 0:
        pytest.skip(
            f"No rows in {GOLD_TABLE} with sub_type IN {APPLICABLE_SUB_TYPES} "
            f"and non-null current_face_value — cannot validate {TARGET_COL}."
        )

    null_violations = qualifying.filter(F.col(TARGET_COL).isNull()).count()

    assert null_violations == 0, (
        f"{null_violations} of {qualifying_count} BOND/MUNI rows with a non-null "
        f"current_face_value have NULL {TARGET_COL} in {GOLD_TABLE}.\n"
        f"Check: (1) coupon rows exist in Silver for these bonds, "
        f"(2) Gold build formula is CURRENT_FACE_VALUE * (1 + COUPON_RATE)."
    )
