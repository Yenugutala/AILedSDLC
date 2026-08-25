"""
Gold layer tests — net_settlement_amount column
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
    spark = SparkSession.builder.getOrCreate()

GOLD_TABLE = "statestreet.g_statestreet.securities_master"


def test_net_settlement_amount_column_exists():
    """
    net_settlement_amount must be present in the Gold table schema.
    Fails immediately if the column was never added or was renamed.
    """
    df = spark.table(GOLD_TABLE)
    column_names = [f.name for f in df.schema.fields]
    assert "net_settlement_amount" in column_names, (
        f"Column 'net_settlement_amount' not found in {GOLD_TABLE}. "
        f"Available columns: {sorted(column_names)}"
    )


def test_net_settlement_amount_non_null_for_bonds_and_munis():
    """
    net_settlement_amount must be non-null for BOND and MUNI securities
    that have both current_face_value and a coupon rate.
    All other product types (EQUITY, FUND, etc.) must have NULL.
    """
    from pyspark.sql import functions as F

    df = spark.table(GOLD_TABLE)

    # ── Positive check: BOND/MUNI with face value + coupon → value expected ──
    bond_muni_with_data = (
        df.filter(
            (F.col("type") == "DEBT")
            & (F.col("sub_type").isin("BOND", "MUNI"))
            & F.col("current_face_value").isNotNull()
            & F.col("latest_coupon_rate").isNotNull()
        )
    )
    total_eligible = bond_muni_with_data.count()
    null_when_should_have_value = (
        bond_muni_with_data
        .filter(F.col("net_settlement_amount").isNull())
        .count()
    )
    assert null_when_should_have_value == 0, (
        f"{null_when_should_have_value}/{total_eligible} BOND/MUNI rows with "
        f"current_face_value + coupon_rate have NULL net_settlement_amount. "
        f"Expected 0 nulls."
    )

    # ── Negative check: non-DEBT rows must always be NULL ───────────────────
    non_debt_with_value = (
        df.filter(
            (F.col("type") != "DEBT")
            & F.col("net_settlement_amount").isNotNull()
        )
        .count()
    )
    assert non_debt_with_value == 0, (
        f"{non_debt_with_value} non-DEBT rows have a non-null net_settlement_amount. "
        f"Expected NULL for all non-DEBT security types (EQUITY, FUND, DERIVATIVE, RIGHT)."
    )
