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
    spark = SparkSession.builder.master("local").appName("gold-tests").getOrCreate()

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
COLUMN_NAME = "net_settlement_amount"
APPLICABLE_TYPES = ("BOND", "MUNI")          # sub_type values expected to produce non-null


def test_net_settlement_amount_column_exists():
    """net_settlement_amount must be present in the securities_master schema."""
    df = spark.table(GOLD_TABLE)
    assert COLUMN_NAME in df.columns, (
        f"Column '{COLUMN_NAME}' not found in {GOLD_TABLE}. "
        f"Actual columns: {df.columns}"
    )


def test_net_settlement_amount_non_null_for_bond_and_muni():
    """
    net_settlement_amount must be non-null for at least one BOND or MUNI row.
    Rows where current_face_value IS NULL or coupon_rate IS NULL are legitimately NULL
    (zero-coupon bonds, missing data), so we assert only that the column resolves a
    non-null value for at least one qualifying security — not that every row is populated.
    """
    from pyspark.sql import functions as F

    non_null_count = (
        spark.table(GOLD_TABLE)
        .filter(F.col("sub_type").isin(*APPLICABLE_TYPES))
        .filter(F.col(COLUMN_NAME).isNotNull())
        .count()
    )
    assert non_null_count > 0, (
        f"Expected at least one non-null '{COLUMN_NAME}' for sub_type in "
        f"{APPLICABLE_TYPES} in {GOLD_TABLE}, but got 0. "
        "Check that current_face_value and coupon_rate are populated in Silver "
        "and that the CASE expression in 05_gold_build.sql is correct."
    )
