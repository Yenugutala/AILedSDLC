"""
Gold layer tests — net_settlement_amount column validation.
Runs against live Databricks via databricks-connect (or a preconfigured SparkSession).

Usage:
    pytest tests/test_gold.py -v
"""

import pytest

# ---------------------------------------------------------------------------
# Session-scoped SparkSession — prefers databricks-connect, falls back to local
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Return a SparkSession connected to Databricks (databricks-connect) when
    available, otherwise fall back to a local session for CI environments that
    mock the table.
    """
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


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
COLUMN_UNDER_TEST = "net_settlement_amount"
# Types for which net_settlement_amount must be populated
APPLICABLE_SUB_TYPES = ("BOND", "MUNI")


# ---------------------------------------------------------------------------
# Test 1 — column exists in the Gold table schema
# ---------------------------------------------------------------------------

def test_net_settlement_amount_column_exists(spark):
    """
    net_settlement_amount must be present in securities_master schema.
    Fails immediately if the Developer Agent omitted the column.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]

    assert COLUMN_UNDER_TEST in column_names, (
        f"Column '{COLUMN_UNDER_TEST}' not found in {GOLD_TABLE}.\n"
        f"Available columns: {sorted(column_names)}"
    )


# ---------------------------------------------------------------------------
# Test 2 — column produces non-null values for BOND and MUNI sub-types
# ---------------------------------------------------------------------------

def test_net_settlement_amount_non_null_for_bonds_and_munis(spark):
    """
    For every row where sub_type IN ('BOND', 'MUNI'), net_settlement_amount
    must be non-null.  NULL values for these sub-types indicate that the
    formula (current_face_value × (1 + coupon_rate)) could not be computed,
    which is a data quality failure in Gold.

    Rows where sub_type is outside APPLICABLE_SUB_TYPES are explicitly
    excluded — NULL is the correct value for equities, funds, etc.
    """
    sub_type_filter = ", ".join(f"'{s}'" for s in APPLICABLE_SUB_TYPES)

    null_count = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM   {GOLD_TABLE}
        WHERE  sub_type IN ({sub_type_filter})
          AND  {COLUMN_UNDER_TEST} IS NULL
    """).first()["cnt"]

    # Confirm at least one BOND/MUNI row exists so the test is meaningful
    total_applicable = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM   {GOLD_TABLE}
        WHERE  sub_type IN ({sub_type_filter})
    """).first()["cnt"]

    assert total_applicable > 0, (
        f"No rows with sub_type IN ({sub_type_filter}) found in {GOLD_TABLE}. "
        "Cannot validate net_settlement_amount — check Bronze/Silver ingestion."
    )

    assert null_count == 0, (
        f"{null_count} of {total_applicable} BOND/MUNI rows have NULL "
        f"'{COLUMN_UNDER_TEST}' in {GOLD_TABLE}.\n"
        "Likely cause: current_face_value or coupon_rate is NULL in Silver "
        "for those products.  Check statestreet.s_statestreet.product and "
        "statestreet.s_statestreet.coupon for missing values."
    )
