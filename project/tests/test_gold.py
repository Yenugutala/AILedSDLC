"""
Gold layer tests — net_settlement_amount column validation.
Runs against live Databricks via databricks-connect (or local SparkSession fallback).
"""
import pytest

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except ImportError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("test_gold").getOrCreate()

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
COLUMN_NAME = "net_settlement_amount"


def test_net_settlement_amount_column_exists():
    """net_settlement_amount must be present in the securities_master schema."""
    columns = [f.name for f in spark.table(GOLD_TABLE).schema.fields]
    assert COLUMN_NAME in columns, (
        f"Column '{COLUMN_NAME}' not found in {GOLD_TABLE}. "
        f"Found columns: {columns}"
    )


def test_net_settlement_amount_non_null_for_bonds_and_munis():
    """
    net_settlement_amount must be non-null for BOND and MUNI products
    that have both a current_face_value and a coupon record.
    At least one qualifying row must exist and have a non-null value.
    """
    result = spark.sql(f"""
        SELECT
            COUNT(*)                                          AS qualifying_rows,
            COUNT({COLUMN_NAME})                             AS non_null_rows,
            SUM(CASE WHEN {COLUMN_NAME} IS NULL THEN 1 ELSE 0 END) AS null_rows
        FROM {GOLD_TABLE}
        WHERE type    = 'DEBT'
          AND sub_type IN ('BOND', 'MUNI')
          AND current_face_value IS NOT NULL
          AND latest_coupon_rate IS NOT NULL
    """).first()

    qualifying_rows = result["qualifying_rows"]
    non_null_rows   = result["non_null_rows"]
    null_rows       = result["null_rows"]

    assert qualifying_rows > 0, (
        f"No BOND/MUNI rows with current_face_value and latest_coupon_rate found in "
        f"{GOLD_TABLE}. Cannot validate {COLUMN_NAME}."
    )
    assert null_rows == 0, (
        f"{null_rows} of {qualifying_rows} qualifying BOND/MUNI rows have NULL "
        f"{COLUMN_NAME}. Expected 0 nulls for rows where current_face_value "
        f"and latest_coupon_rate are both present."
    )
    assert non_null_rows == qualifying_rows, (
        f"Only {non_null_rows}/{qualifying_rows} qualifying rows have a non-null "
        f"{COLUMN_NAME}."
    )
