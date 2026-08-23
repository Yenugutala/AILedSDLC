```python
"""
Test suite for Securities Master Gold Layer - Settlement Analytics
Ticket: Securities Master: Add Settlement Analytics to Gold Layer
Traceability: REQ-01, REQ-02, REQ-03, REQ-04
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DecimalType, FloatType


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GOLD_TABLE = "catalog.securities_master.gold_securities"
SILVER_BOND_TABLE = "catalog.securities_master.silver_bonds"

BOND_PRODUCT_TYPES = ("bond", "muni")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped Spark fixture.
    Uses databricks.connect when available (remote cluster),
    falls back to a local SparkSession for CI environments.
    """
    try:
        from databricks.connect import DatabricksSession  # type: ignore

        session = DatabricksSession.builder.getOrCreate()
    except ImportError:
        session = (
            SparkSession.builder.master("local[*]")
            .appName("test_gold_settlement_analytics")
            .getOrCreate()
        )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def gold_df(spark: SparkSession):
    """
    Loads the gold securities table once per test session.
    """
    return spark.table(GOLD_TABLE)


@pytest.fixture(scope="session")
def gold_current_df(gold_df):
    """
    Subset of gold table where is_current = TRUE (active security versions).
    """
    return gold_df.filter(F.col("is_current") == True)  # noqa: E712


@pytest.fixture(scope="session")
def gold_bonds_df(gold_current_df):
    """
    Subset of current gold rows for bond and muni product types.
    """
    return gold_current_df.filter(F.col("product_type").isin(list(BOND_PRODUCT_TYPES)))


@pytest.fixture(scope="session")
def silver_bonds_df(spark: SparkSession):
    """
    Loads the silver bond table once per test session.
    """
    return spark.table(SILVER_BOND_TABLE)


# ---------------------------------------------------------------------------
# REQ-01: Gold table must include net_settlement_amount
# ---------------------------------------------------------------------------


class TestNetSettlementAmountColumnExists:
    """REQ-01: net_settlement_amount column presence and basic non-null guarantees."""

    def test_net_settlement_amount_column_exists_in_schema(self, gold_df):
        """
        # Validates: REQ-01
        Asserts that the gold table schema contains the net_settlement_amount column.
        """
        column_names = [field.name.lower() for field in gold_df.schema.fields]
        assert "net_settlement_amount" in column_names, (
            "REQ-01 FAILED: 'net_settlement_amount' column is missing from the gold "
            f"table schema. Columns found: {column_names}"
        )

    def test_net_settlement_amount_is_numeric_type(self, gold_df):
        """
        # Validates: REQ-01
        Asserts that net_settlement_amount is stored as a numeric type
        (DoubleType, FloatType, or DecimalType) suitable for financial calculations.
        """
        schema_map = {field.name.lower(): field.dataType for field in gold_df.schema.fields}
        col_type = schema_map.get("net_settlement_amount")

        assert col_type is not None, (
            "REQ-01 FAILED: 'net_settlement_amount' column not found in schema."
        )
        assert isinstance(col_type, (DoubleType, FloatType, DecimalType)), (
            f"REQ-01 FAILED: 'net_settlement_amount' must be a numeric type "
            f"(Double, Float, or Decimal). Found: {col_type}"
        )

    def test_gold_table_row_count_is_positive(self, gold_df):
        """
        # Validates: REQ-01
        Asserts that the gold table contains at least one row, confirming the
        table is populated and queryable.
        """
        row_count = gold_df.count()
        assert row_count > 0, (
            f"REQ-01 FAILED: Gold table '{GOLD_TABLE}' contains 0 rows. "
            "The table must be populated before settlement analytics can be validated."
        )

    def test_net_settlement_amount_not_all_null_for_bonds(self, gold_bonds_df):
        """
        # Validates: REQ-01
        Asserts that net_settlement_amount is NOT entirely null for bond and muni
        security types — at least one non-null value must exist.
        """
        bond_count = gold_bonds_df.count()
        assert bond_count > 0, (
            "REQ-01 PRECONDITION FAILED: No bond/muni rows found in the gold table. "
            "Cannot evaluate net_settlement_amount nullability."
        )

        null_count = gold_bonds_df.filter(
            F.col("net_settlement_amount").isNull()
        ).count()

        assert null_count < bond_count, (
            f"REQ-01 FAILED: 'net_settlement_amount' is NULL for ALL {bond_count} "
            "bond/muni rows in the gold table. At least one non-null value is required."
        )


# ---------------------------------------------------------------------------
# REQ-02: net_settlement_amount derived from principal_amount x (1 + accrued_interest_rate)
# ---------------------------------------------------------------------------


class TestNetSettlementAmountDerivation:
    """REQ-02: Derivation formula validation against silver bond layer."""

    def test_net_settlement_amount_matches_derivation_formula(
        self, spark: SparkSession, gold_bonds_df, silver_bonds_df
    ):
        """
        # Validates: REQ-02
        Joins the gold bond rows with the silver bond source to verify that
        net_settlement_amount equals principal_amount * (1 + accrued_interest_rate)
        within an acceptable floating-point tolerance.

        Assumption: gold and silver bond tables share a common 'product_id' key.
        """
        silver_with_expected = silver_bonds_df.select(
            F.col("product_id"),
            (
                F.col("principal_amount") * (F.lit(1.0) + F.col("accrued_interest_rate"))
            ).alias("expected_net_settlement_amount"),
        )

        joined = gold_bonds_df.alias("gold").join(
            silver_with_expected.alias("silver"),
            on="product_id",
            how="inner",
        )

        join_count = joined.count()
        assert join_count > 0, (
            "REQ-02 FAILED: No rows matched between gold bond records and silver bond "
            "table on 'product_id'. Cannot validate derivation formula."
        )

        tolerance = 0.0001  # acceptable floating-point delta

        mismatched = joined.filter(
            F.abs(
                F.col("gold.net_settlement_amount")
                - F.col("silver.expected_net_settlement_amount")
            )
            > tolerance
        )

        mismatch_count = mismatched.count()
        assert mismatch_count == 0, (
            f"REQ-02 FAILED: {mismatch_count} row(s) have 'net_settlement_amount' values "
            "that do not match the formula principal_amount * (1 + accrued_interest_rate) "
            f"within tolerance {tolerance}. "
            "Sample mismatches:\n"
            + str(
                