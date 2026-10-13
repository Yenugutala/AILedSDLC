"""
Gold layer tests — net_settlement_amount column validation.
Validates the new net_settlement_amount column added to
statestreet.g_statestreet.securities_master.

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


def test_net_settlement_amount_column_exists():
    """
    net_settlement_amount must be present in the securities_master schema.
    Fails immediately if the column was never added to the Gold DDL.
    """
    df = spark.table(GOLD_TABLE)
    assert "net_settlement_amount" in df.columns, (
        f"Column 'net_settlement_amount' not found in {GOLD_TABLE}. "
        f"Actual columns: {df.columns}"
    )


def test_net_settlement_amount_non_null_for_bonds_and_munis():
    """
    net_settlement_amount must be non-null for BOND and MUNI securities
    that have both current_face_value and a coupon_rate populated.
    At least one such row must exist and all qualifying rows must be non-null.
    """
    qualifying = spark.sql(f"""
        SELECT
            product_id,
            sub_type,
            net_settlement_amount
        FROM {GOLD_TABLE}
        WHERE sub_type IN ('BOND', 'MUNI')
          AND current_face_value IS NOT NULL
          AND latest_coupon_rate IS NOT NULL
    """)

    total = qualifying.count()
    assert total > 0, (
        "No BOND or MUNI rows found with both current_face_value and "
        "latest_coupon_rate populated. Check Silver data or Gold JOIN logic."
    )

    nulls = qualifying.filter("net_settlement_amount IS NULL").count()
    assert nulls == 0, (
        f"{nulls} of {total} qualifying BOND/MUNI rows have NULL "
        "net_settlement_amount. Expected all to be non-null when both "
        "current_face_value and latest_coupon_rate are present."
    )
