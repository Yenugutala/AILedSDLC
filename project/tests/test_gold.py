"""
Gold layer tests — net_settlement_amount column validation.
Runs against live Databricks using databricks-connect (falls back to local Spark).
"""
import pytest

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except Exception:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("gold-tests").getOrCreate()

GOLD_TABLE = "statestreet.g_statestreet.securities_master"


def test_net_settlement_amount_column_exists():
    """net_settlement_amount must be present in the securities_master schema."""
    columns = spark.table(GOLD_TABLE).columns
    assert "net_settlement_amount" in columns, (
        f"Column 'net_settlement_amount' not found in {GOLD_TABLE}. "
        f"Actual columns: {columns}"
    )


def test_net_settlement_amount_non_null_for_bonds_and_munis():
    """
    net_settlement_amount must be non-null for BOND and MUNI products
    that have a current_face_value and a coupon record (latest_coupon_rate IS NOT NULL).
    """
    result = spark.sql(f"""
        SELECT COUNT(*) AS qualifying_rows_with_nulls
        FROM {GOLD_TABLE}
        WHERE sub_type IN ('BOND', 'MUNI')
          AND current_face_value IS NOT NULL
          AND latest_coupon_rate IS NOT NULL
          AND net_settlement_amount IS NULL
    """).first()["qualifying_rows_with_nulls"]

    assert result == 0, (
        f"{result} BOND/MUNI row(s) with current_face_value and latest_coupon_rate "
        f"have net_settlement_amount = NULL — formula not applied correctly."
    )
