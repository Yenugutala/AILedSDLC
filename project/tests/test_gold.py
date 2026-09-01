# tests/test_gold.py
"""
Gold layer tests — net_settlement_amount column validation.

Runs against live Databricks via databricks-connect.
Requires DATABRICKS_HOST and DATABRICKS_TOKEN (or ~/.databrickscfg) to be set.

Usage:
    pytest tests/test_gold.py -v
"""

import pytest

try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
except Exception:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("gold-tests").getOrCreate()

GOLD_TABLE = "statestreet.g_statestreet.securities_master"
DEBT_SUBTYPES = ("BOND", "MUNI")


def test_net_settlement_amount_column_exists():
    """
    net_settlement_amount must be present in the securities_master schema.

    Fails fast if the Developer Agent omitted the column or named it differently.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [field.name for field in df.schema.fields]
    assert "net_settlement_amount" in column_names, (
        f"Column 'net_settlement_amount' not found in {GOLD_TABLE}. "
        f"Actual columns: {column_names}"
    )


def test_net_settlement_amount_non_null_for_bonds_and_munis():
    """
    net_settlement_amount must be non-null for all BOND and MUNI rows that have
    both current_face_value and a latest coupon rate populated.

    Logic: net_settlement_amount = current_face_value × (1 + coupon_rate).
    Rows where either input is NULL are excluded from the assertion (they
    legitimately yield NULL output — see GOLD-005 in known_issues.md).
    """
    qualifying_rows = (
        spark.table(GOLD_TABLE)
        .filter(
            f"sub_type IN {DEBT_SUBTYPES}"
            " AND current_face_value IS NOT NULL"
            " AND latest_coupon_rate IS NOT NULL"
        )
    )

    total = qualifying_rows.count()
    assert total > 0, (
        f"No BOND/MUNI rows with both current_face_value and latest_coupon_rate "
        f"populated in {GOLD_TABLE}. Check Silver ingestion or Gold JOIN logic."
    )

    null_count = qualifying_rows.filter("net_settlement_amount IS NULL").count()
    assert null_count == 0, (
        f"{null_count} of {total} qualifying BOND/MUNI rows have NULL "
        f"net_settlement_amount in {GOLD_TABLE}. "
        f"Expected 0 — all rows with non-null inputs must produce a non-null result."
    )
