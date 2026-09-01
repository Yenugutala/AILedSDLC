"""
Gold layer tests — net_settlement_amount column
Validates the new derived column added to statestreet.g_statestreet.securities_master.

Run against live Databricks:
    pytest tests/test_gold.py -v
"""

import pytest

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ImportError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("gold-tests").getOrCreate()


GOLD_TABLE = "statestreet.g_statestreet.securities_master"
COLUMN_NAME = "net_settlement_amount"


def test_net_settlement_amount_column_exists():
    """
    net_settlement_amount must be present in the securities_master schema.
    Fails immediately if the Developer Agent omitted the column or misspelled it.
    """
    df = spark.table(GOLD_TABLE)
    assert COLUMN_NAME in df.columns, (
        f"Column '{COLUMN_NAME}' not found in {GOLD_TABLE}. "
        f"Actual columns: {df.columns}"
    )


def test_net_settlement_amount_non_null_for_bonds_and_munis():
    """
    net_settlement_amount must be non-null for BOND and MUNI sub_type rows
    that have both current_face_value and a coupon_rate available.
    Expects at least one qualifying row — if zero rows exist, the Gold
    build has a logic error (wrong JOIN or CASE condition).
    """
    qualifying_rows = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM {GOLD_TABLE}
        WHERE sub_type IN ('BOND', 'MUNI')
          AND {COLUMN_NAME} IS NOT NULL
    """).first()["cnt"]

    assert qualifying_rows > 0, (
        f"Expected at least one BOND or MUNI row with a non-null "
        f"'{COLUMN_NAME}' in {GOLD_TABLE}, but found 0. "
        "Check that the CASE expression in 05_gold_build.sql covers "
        "sub_type IN ('BOND','MUNI') and that current_face_value / "
        "coupon_rate are populated in Silver."
    )
