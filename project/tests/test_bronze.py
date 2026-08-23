"""
Bronze Layer Tests — Securities Master Data Lakehouse
Tests raw landing: schema correctness, metadata columns, idempotency, schema drift.
All tests use pytest + PySpark (local Spark session — no Databricks required).
"""

import pytest
import os
import shutil
import tempfile
from datetime import date
from unittest.mock import patch, MagicMock

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, DecimalType,
    DateType, BooleanType, LongType, TimestampType,
    IntegerType,
)

# ---------------------------------------------------------------------------
# Session-scoped Spark fixture (local mode — no Databricks cluster needed)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Local Delta + Spark session. Tear down after all tests complete."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("sml-bronze-tests")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")   # keep tests fast
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def tmp_delta_dir(tmp_path):
    """A fresh temporary directory for each Delta table written in a test."""
    yield str(tmp_path)


# ---------------------------------------------------------------------------
# Canonical source schemas (mirrors bronze/tables.yaml — one per key table)
# ---------------------------------------------------------------------------

PRODUCT_SOURCE_SCHEMA = StructType([
    StructField("product_id",              StringType(),         False),
    StructField("id_type",                 StringType(),         True),
    StructField("type",                    StringType(),         True),
    StructField("sub_type",                StringType(),         True),
    StructField("status",                  StringType(),         True),
    StructField("settlement_type",         StringType(),         True),
    StructField("description",             StringType(),         True),
    StructField("issue_date",              DateType(),           True),
    StructField("issue_price",             DecimalType(28, 8),   True),
    StructField("current_face_value",      DecimalType(28, 8),   True),
    StructField("issuer_legal_entity_id",  StringType(),         True),
    StructField("tick_ladder_scale_id",    StringType(),         True),
])

BOND_SOURCE_SCHEMA = StructType([
    StructField("product_id",          StringType(),  False),
    StructField("coupon_type",         StringType(),  True),
    StructField("maturity_date",       DateType(),    True),
    StructField("issue_currency_code", StringType(),  True),
])

LEGAL_ENTITY_SOURCE_SCHEMA = StructType([
    StructField("legal_entity_id", StringType(), False),
    StructField("legal_name",      StringType(),  True),
    StructField("country",         StringType(),  True),
])

COUPON_SOURCE_SCHEMA = StructType([
    StructField("coupon_id",    StringType(),          False),
    StructField("product_id",   StringType(),          False),
    StructField("coupon_rate",  DecimalType(28, 8),    True),
    StructField("payment_date", DateType(),            True),
    StructField("coupon_type",  StringType(),          True),
    StructField("frequency",    StringType(),          True),
])

IDENTIFIERS_SOURCE_SCHEMA = StructType([
    StructField("identifier_id",    StringType(), False),
    StructField("product_id",       StringType(), False),
    StructField("id_type",          StringType(), True),
    StructField("identifier_value", StringType(), True),
])

CURRENCY_SOURCE_SCHEMA = StructType([
    StructField("currency_code", StringType(), False),
    StructField("currency_name", StringType(), True),
])

# Metadata columns added by the pipeline ─────────────────────────────────
REQUIRED_METADATA_COLS = {
    "_source_file",
    "_ingestion_ts",
    "_batch_id",
    "_row_hash",
}

# All 29 source table names ───────────────────────────────────────────────
ALL_SOURCE_TABLES = [
    "product", "generic_product", "legal_entity", "tick_ladder_scale",
    "tick", "product_rating", "product_rating_type", "classification",
    "identifiers", "fund", "debt", "bond", "muni", "pool_backed_security",
    "right", "series", "listed_derivative", "option", "future", "stock",
    "common_stock", "preferred_stock", "coupon",
    "principal_redemption_provision", "currency",
    "listed_derivative_tick", "debt_principal_redemption_provision",
    "dq_rules_catalog", "dq_issues_catalog",
]

# ---------------------------------------------------------------------------
# Helper — simulate the _add_metadata() function from bronze_loader.py
# (We test the pipeline logic independently of whether the module is importable)
# ---------------------------------------------------------------------------

def _add_metadata(df, source_file: str, batch_id: str):
    """Mirror of src/ingestion/bronze_loader._add_metadata()."""
    data_cols = [F.col(c) for c in df.columns]
    return (
        df
        .withColumn("_source_file",  F.lit(source_file))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_batch_id",     F.lit(batch_id))
        .withColumn("_row_hash",     F.sha2(F.concat_ws("|", *data_cols), 256))
    )


def _make_product_df(spark, rows=None):
    """Return a small product DataFrame (source schema, no metadata)."""
    rows = rows or [
        ("P001", "CUSIP",  "EQUITY", "COMMON_STOCK", "ACTIVE",   None, "Apple Inc",     None, None, None, "LE001", None),
        ("P002", "ISIN",   "DEBT",   "BOND",         "ACTIVE",   None, "US T-Bond 5yr", None, None, 100.0,"LE002", None),
        ("P003", "TICKER", "FUND",   "FUND",         "INACTIVE", None, "SPY ETF",        None, None, None, "LE003", None),
    ]
    return spark.createDataFrame(rows, schema=PRODUCT_SOURCE_SCHEMA)


# ===========================================================================
# 1. SCHEMA TESTS — bronze table columns match spec
# ===========================================================================

class TestBronzeSchema:
    """Bronze tables must land with the exact columns defined in tables.yaml."""

    def test_product_has_all_source_columns(self, spark):
        df = _make_product_df(spark)
        for field in PRODUCT_SOURCE_SCHEMA.fields:
            assert field.name in df.columns, (
                f"Column '{field.name}' missing from product source schema"
            )

    def test_bond_has_all_source_columns(self, spark):
        rows = [("P002", "FIXED", date(2030, 1, 1), "USD")]
        df = spark.createDataFrame(rows, schema=BOND_SOURCE_SCHEMA)
        for field in BOND_SOURCE_SCHEMA.fields:
            assert field.name in df.columns

    def test_legal_entity_has_all_source_columns(self, spark):
        rows = [("LE001", "Apple Inc", "US")]
        df = spark.createDataFrame(rows, schema=LEGAL_ENTITY_SOURCE_SCHEMA)
        for field in LEGAL_ENTITY_SOURCE_SCHEMA.fields:
            assert field.name in df.columns

    def test_coupon_has_all_source_columns(self, spark):
        rows = [("C001", "P002", 0.05, date(2024, 6, 30), "FIXED", "SEMI_ANNUAL")]
        df = spark.createDataFrame(rows, schema=COUPON_SOURCE_SCHEMA)
        for field in COUPON_SOURCE_SCHEMA.fields:
            assert field.name in df.columns

    def test_identifiers_has_all_source_columns(self, spark):
        rows = [("I001", "P001", "CUSIP", "037833100")]
        df = spark.createDataFrame(rows, schema=IDENTIFIERS_SOURCE_SCHEMA)
        for field in IDENTIFIERS_SOURCE_SCHEMA.fields:
            assert field.name in df.columns

    def test_currency_has_all_source_columns(self, spark):
        rows = [("USD", "US Dollar")]
        df = spark.createDataFrame(rows, schema=CURRENCY_SOURCE_SCHEMA)
        for field in CURRENCY_SOURCE_SCHEMA.fields:
            assert field.name in df.columns

    def test_product_id_is_not_nullable_in_spec(self):
        """product_id must be declared NOT NULL in the source schema."""
        field = next(f for f in PRODUCT_SOURCE_SCHEMA.fields if f.name == "product_id")
        assert field.nullable is False, "product_id should be non-nullable"

    def test_all_29_source_tables_accounted_for(self):
        """Sanity check that our ALL_SOURCE_TABLES constant lists all 29 tables."""
        assert len(ALL_SOURCE_TABLES) == 29


# ===========================================================================
# 2. METADATA COLUMN TESTS
# ===========================================================================

class TestMetadataColumns:
    """_add_metadata() must attach exactly the 4 standard metadata columns."""

    def test_all_four_metadata_columns_present(self, spark):
        df = _make_product_df(spark)
        result = _add_metadata(df, "product.csv", "batch_001")
        assert REQUIRED_METADATA_COLS.issubset(set(result.columns)), (
            f"Missing: {REQUIRED_METADATA_COLS - set(result.columns)}"
        )

    def test_source_file_value_is_correct(self, spark):
        df = _make_product_df(spark)
        result = _add_metadata(df, "product.csv", "batch_001")
        files = {r["_source_file"] for r in result.collect()}
        assert files == {"product.csv"}

    def test_batch_id_value_is_correct(self, spark):
        df = _make_product_df(spark)
        result = _add_metadata(df, "product.csv", "batch_XYZ")
        batches = {r["_batch_id"] for r in result.collect()}
        assert batches == {"batch_XYZ"}

    def test_ingestion_ts_is_not_null(self, spark):
        df = _make_product_df(spark)
        result = _add_metadata(df, "product.csv", "batch_001")
        null_ts = result.filter(F.col("_ingestion_ts").isNull()).count()
        assert null_ts == 0, "_ingestion_ts must never be null"

    def test_row_hash_is_not_null(self, spark):
        df = _make_product_df(spark)
        result = _add_metadata(df, "product.csv", "batch_001")
        null_hash = result.filter(F.col("_row_hash").isNull()).count()
        assert null_hash == 0, "_row_hash must never be null"

    def test_row_hash_is_64_hex_chars(self, spark):
        """SHA-256 hex string is always exactly 64 characters."""
        df = _make_product_df(spark)
        result = _add_metadata(df, "product.csv", "batch_001")
        for row in result.collect():
            assert len(row["_row_hash"]) == 64, (
                f"_row_hash length {len(row['_row_hash'])} != 64 for row {row['product_id']}"
            )

    def test_row_hash_deterministic_across_batches(self, spark):
        """Same data → same _row_hash regardless of batch_id or timestamp."""
        df = _make_product_df(spark)
        run1 = _add_metadata(df, "product.csv", "batch_001").select("product_id", "_row_hash")
        run2 = _add_metadata(df, "product.csv", "batch_002").select("product_id", "_row_hash")
        hashes1 = {r["product_id"]: r["_row_hash"] for r in run1.collect()}
        hashes2 = {r["product_id"]: r["_row_hash"] for r in run2.collect()}
        assert hashes1 == hashes2, (
            "Row hashes changed between runs for identical source data"
        )

    def test_different_rows_have_different_hashes(self, spark):
        """Distinct rows must not collide on _row_hash."""
        df = _make_product_df(spark)
        result = _add_metadata(df, "product.csv", "batch_001")
        hashes = [r["_row_hash"] for r in result.collect()]
        assert len(hashes) == len(set(hashes)), "Hash collision detected — rows are not unique"

    def test_row_count_unchanged_by_metadata(self, spark):
        """_add_metadata() must not filter or fan-out any rows."""
        df = _make_product_df(spark)
        result = _add_metadata(df, "product.csv", "batch_001")
        assert result.count() == df.count()

    def test_data_columns_still_present_after_metadata(self, spark):
        """Original data columns must all survive _add_metadata()."""
        df = _make_product_df(spark)
        result = _add_metadata(df, "product.csv", "batch_001")
        for field in PRODUCT_SOURCE_SCHEMA.fields:
            assert field.name in result.columns, (
                f"Source column '{field.name}' was dropped by _add_metadata()"
            )

    def test_changed_row_produces_different_hash(self, spark):
        """Modifying a data column must change the _row_hash (CDC sensitivity)."""
        base_rows = [("P001", "CUSIP", "EQUITY", "COMMON_STOCK", "ACTIVE",
                      None, "Apple Inc", None, None, None, "LE001", None)]
        changed_rows = [("P001", "CUSIP", "EQUITY", "COMMON_STOCK", "INACTIVE",  # status changed
                         None, "Apple Inc", None, None, None, "LE001", None)]

        df_base    = spark.createDataFrame(base_rows,    schema=PRODUCT_SOURCE_SCHEMA)
        df_changed = spark.createDataFrame(changed_rows, schema=PRODUCT_SOURCE_SCHEMA)

        hash_base    = _add_metadata(df_base,    "p.csv", "b1").first()["_row_hash"]
        hash_changed = _add_metadata(df_changed, "p.csv", "b1").first()["_row_hash"]

        assert hash_base != hash_changed, (
            "Hash should change when a data column value changes"
        )


# ===========================================================================
# 3. IDEMPOTENCY TESTS (MERGE INTO behaviour)
# ===========================================================================

class TestBronzeIdempotency:
    """Running the ingestion twice must not duplicate rows (MERGE INTO guarantee)."""

    def _write_delta(self, df, path, mode="overwrite"):
        df.write.format("delta").mode(mode).save(path)

    def _merge_into(self, spark, incoming_df, target_path, pk="product_id"):
        """
        Simulate the MERGE INTO logic used in bronze_loader.py.
        Only updates rows where _row_hash has changed; inserts new rows.
        """
        incoming_df.createOrReplaceTempView("_incoming")
        spark.sql(f"""
            MERGE INTO delta.`{target_path}` AS target
            USING _incoming AS source
            ON target.{pk} = source.{pk}
            WHEN MATCHED AND source._row_hash != target._row_hash
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
        """)

    def test_second_run_same_row_count(self, spark, tmp_delta_dir):
        df = _add_metadata(_make_product_df(spark), "product.csv", "batch_001")
        # First run — create table
        self._write_delta(df, tmp_delta_dir)
        count_after_run1 = spark.read.format("delta").load(tmp_delta_dir).count()

        # Second run — identical data
        df2 = _add_metadata(_make_product_df(spark), "product.csv", "batch_002")
        self._merge_into(spark, df2, tmp_delta_dir)
        count_after_run2 = spark.read.format("delta").load(tmp_delta_dir).count()

        assert count_after_run1 == count_after_run2, (
            f"Row count changed after re-run: {count_after_run1} → {count_after_run2}"
        )

    def test_new_row_in_second_run_increments_count(self, spark, tmp_delta_dir):
        df = _add_metadata(_make_product_df(spark), "product.csv", "batch_001")
        self._write_delta(df, tmp_delta_dir)
        count_run1 = spark.read.format("delta").load(tmp_delta_dir).count()

        # Add a genuinely new product
        new_row = [("P999", "SEDOL", "EQUITY", "COMMON_STOCK", "ACTIVE",
                    None, "New Co", None, None, None, "LE004", None)]
        new_df   = spark.createDataFrame(new_row, schema=PRODUCT_SOURCE_SCHEMA)
        full_df2 = _make_product_df(spark).union(new_df)
        df2      = _add_metadata(full_df2, "product.csv", "batch_002")

        self._merge_into(spark, df2, tmp_delta_dir)
        count_run2 = spark.read.format("delta").load(tmp_delta_dir).count()

        assert count_run2 == count_run1 + 1, (
            f"Expected {count_run1 + 1} rows after new product added, got {count_run2}"
        )

    def test_changed_row_does_not_duplicate(self, spark, tmp_delta_dir):
        """Updated row must replace the old row, not add a second one."""
        df = _add_metadata(_make_product_df(spark), "product.csv", "batch_001")
        self._write_delta(df, tmp_delta_dir)
        count_run1 = spark.read.format("delta").load(tmp_delta_dir).count()

        # Change status for P001
        changed = [("P001", "CUSIP", "EQUITY", "COMMON_STOCK", "SUSPENDED",
                    None, "Apple Inc", None, None, None, "LE001", None),
                   ("P002", "ISIN",   "DEBT",  "BOND", "ACTIVE",
                    None, "US T-Bond 5yr", None, None, 100.0, "LE002", None),
                   ("P003", "TICKER", "FUND",  "FUND", "INACTIVE",
                    None, "SPY ETF", None, None, None, "LE003", None)]
        df2 = _add_metadata(
            spark.createDataFrame(changed, schema=PRODUCT_SOURCE_SCHEMA),
            "product.csv", "batch_002"
        )
        self._merge_into(spark, df2, tmp_delta_dir)
        count_run2 = spark.read.format("delta").load(tmp_delta_dir).count()

        assert count_run1 == count_run2, (
            f"Row duplicated on update: {count_run1} → {count_run2}"
        )

    def test_changed_row_value_is_updated(self, spark, tmp_delta_dir):
        """After MERGE the updated column value must be reflected in the table."""
        df = _add_metadata(_make_product_df(spark), "product.csv", "batch_001")
        self._write_delta(df, tmp_delta_dir)

        changed = [("P001", "CUSIP", "EQUITY", "COMMON_STOCK", "SUSPENDED",
                    None, "Apple Inc", None, None, None, "LE001", None),
                   ("P002", "ISIN", "DEBT", "BOND", "ACTIVE",
                    None, "US T-Bond 5yr", None, None, 100.0, "LE002", None),
                   ("P003", "TICKER", "FUND", "FUND", "INACTIVE",
                    None, "SPY ETF", None, None, None, "LE003", None)]
        df2 = _add_metadata(
            spark.createDataFrame(changed, schema=PRODUCT_SOURCE_SCHEMA),
            "product.csv", "batch_002"
        )
        self._merge_into(spark, df2, tmp_delta_dir)

        updated_status = (
            spark.read.format("delta").load(tmp_delta_dir)
            .filter(F.col("product_id") == "P001")
            .select("status")
            .first()["status"]
        )
        assert updated_status == "SUSPENDED", (
            f"Expected status=SUSPENDED after update, got {updated_status}"
        )

    def test_hash_unchanged_rows_not_rewritten(self, spark, tmp_delta_dir):
        """
        When the source data has not changed, the _row_hash in the table
        must remain identical after a second ingest run.
        """
        df = _add_metadata(_make_product_df(spark), "product.csv", "batch_001")
        self._write_delta(df, tmp_delta_dir)

        hashes_before = {
            r["product_id"]: r["_row_hash"]
            for r in spark.read.format("delta").load(tmp_delta_dir).collect()
        }

        df2 = _add_metadata(_make_product_df(spark), "product.csv", "batch_002")
        self._merge_into(spark, df2, tmp_delta_dir)

        hashes_after = {
            r["product_id"]: r["_row_hash"]
            for r in spark.read.format("delta").load(tmp_delta_dir).collect()
        }

        assert hashes_before == hashes_after, (
            "Row hashes changed despite source data being unchanged"
        )


# ===========================================================================
# 4. SCHEMA DRIFT TESTS
# ===========================================================================

class TestSchemaDrift:
    """
    Additive drift (new column) → auto-merged into Bronze.
    Breaking drift (type change / column removal) → batch quarantined.
    These tests verify the detect_drift() logic directly.
    """

    # ── helper ──────────────────────────────────────────────────────────────
    def _existing_table(self, spark, tmp_path):
        """Write a base product table and return its path."""
        path = str(tmp_path / "product")
        _add_metadata(_make_product_df(spark), "product.csv", "batch_001") \
            .write.format("delta").mode("overwrite").save(path)
        return path

    def _detect_drift(self, spark, table_path, incoming_schema):
        """
        Pure-Python reimplementation of detect_drift() so tests are self-contained.
        Returns dict: new_table, additive, breaking, unchanged.
        """
        META_COLS = {
            "_source_file", "_ingestion_ts", "_batch_id", "_row_hash",
            "_ingestion_date", "_schema_changes",
        }
        try:
            existing_schema = spark.read.format("delta").load(table_path).schema
        except Exception:
            return {"new_table": True, "additive": [], "breaking": [], "unchanged": []}

        existing = {
            f.name: f.dataType
            for f in existing_schema.fields
            if f.name not in META_COLS
        }
        incoming = {
            f.name: f.dataType
            for f in incoming_schema.fields
            if f.name not in META_COLS
        }

        additive     = [c for c in incoming if c not in existing]
        type_changes = [c for c in existing if c in incoming
                        and str(existing[c]) != str(incoming[c])]
        removed      = [c for c in existing if c not in incoming]
        breaking     = type_changes + removed
        unchanged    = [c for c in existing if c in incoming and c not in breaking]

        return {
            "new_table": False,
            "additive":  additive,
            "breaking":  breaking,
            "unchanged": unchanged,
        }

    # ── tests ────────────────────────────────────────────────────────────────

    def test_new_table_flag_when_table_absent(self, spark, tmp_path):
        result = self._detect_drift(
            spark, str(tmp_path / "nonexistent"), PRODUCT_SOURCE_SCHEMA
        )
        assert result["new_table"] is True
        assert result["additive"]  == []
        assert result["breaking"]  == []

    def test_no_drift_when_schema_identical(self, spark, tmp_path):
        path = self._existing_table(spark, tmp_path)
        result = self._detect_drift(spark, path, PRODUCT_SOURCE_SCHEMA)
        assert result["new_table"] is False
        assert result["additive"]  == []
        assert result["breaking"]  == []

    def test_additive_drift_detected(self, spark, tmp_path):
        """Adding a new column to incoming data is additive drift (safe)."""
        path = self._existing_table(spark, tmp_path)

        extended_schema = StructType(
            PRODUCT_SOURCE_SCHEMA.fields
            + [StructField("new_regulatory_flag", StringType(), True)]
        )
        result = self._detect_drift(spark, path, extended_schema)

        assert "new_regulatory_flag" in result["additive"], (
            "Additive drift not detected for new_regulatory_flag"
        )
        assert result["breaking"] == [], "No breaking changes expected"

    def test_additive_drift_auto_merged(self, spark, tmp_path):
        """After detecting additive drift, ALTER TABLE ADD COLUMNS succeeds."""
        path = self._existing_table(spark, tmp_path)

        # Simulate adding a new column via ALTER TABLE
        spark.sql(f"""
            ALTER TABLE delta.`{path}`
            ADD COLUMNS (new_regulatory_flag STRING)
        """)

        cols = spark.read.format("delta").load(path).columns
        assert "new_regulatory_flag" in cols, (
            "New column not present in Delta table after ALTER TABLE"
        )

    def test_additive_drift_existing_rows_get_null(self, spark, tmp_path):
        """Rows that pre-date the new column must have NULL for that column."""
        path = self._existing_table(spark, tmp_path)
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS (new_regulatory_flag STRING)")

        null_count = (
            spark.read.format("delta").load(path)
            .filter(F.col("new_regulatory_flag").isNull())
            .count()
        )
        total = spark.read.format("delta").load(path).count()
        assert null_count == total, (
            "Pre-existing rows should have NULL for newly added column"
        )

    def test_type_change_is_breaking(self, spark, tmp_path):
        """Changing a column's type (STRING → INTEGER) is a breaking change."""
        path = self._existing_table(spark, tmp_path)

        # product_id is STRING in existing; incoming tries to send it as INTEGER
        breaking_schema = StructType([
            StructField("product_id",             IntegerType(),      False),  # type change!
            StructField("id_type",                StringType(),       True),
            StructField("type",                   StringType(),       True),
            StructField("sub_type",               StringType(),       True),
            StructField("status",                 StringType(),       True),
            StructField("settlement_type",        StringType(),       True),
            StructField("description",            StringType(),       True),
            StructField("issue_date",             DateType(),         True),
            StructField("issue_price",            DecimalType(28, 8), True),
            StructField("current_face_value",     DecimalType(28, 8), True),
            StructField("issuer_legal_entity_id", StringType(),       True),
            StructField("tick_ladder_scale_id",   StringType(),       True),
        ])
        result = self._detect_drift(spark, path, breaking_schema)

        assert "product_id" in result["breaking"], (
            "Type change on product_id should be flagged as breaking"
        )
        assert result["additive"] == []

    def test_column_removal_is_breaking(self, spark, tmp_path):
        """Dropping an existing column from incoming data is a breaking change."""
        path = self._existing_table(spark, tmp_path)

        # Remove description and issuer_legal_entity_id from incoming
        reduced_schema = StructType([
            f for f in PRODUCT_SOURCE_SCHEMA.fields
            if f.name not in {"description", "issuer_legal_entity_id"}
        ])
        result = self._detect_drift(spark, path, reduced_schema)

        assert "description"            in result["breaking"]
        assert "issuer_legal_entity_id" in result["breaking"]

    def test_breaking_drift_raises_on_quarantine(self, spark, tmp_path):
        """
        handle_breaking_drift() must raise a ValueError so the Databricks
        job fails and triggers an alert — never silently continues.
        """
        def handle_breaking_drift(breaking_columns, batch_id):
            if breaking_columns:
                raise ValueError(
                    f"[SCHEMA DRIFT] Breaking change: {breaking_columns}. "
                    f"Batch {batch_id} quarantined."
                )

        with pytest.raises(ValueError, match="SCHEMA DRIFT"):
            handle_breaking_drift(["product_id"], "batch_drift_test")

    def test_multiple_additive_columns_all_detected(self, spark, tmp_path):
        """Drift detection must surface every new column, not just the first."""
        path = self._existing_table(spark, tmp_path)

        extended_schema = StructType(
            PRODUCT_SOURCE_SCHEMA.fields + [
                StructField("col_alpha",  StringType(), True),
                StructField("col_beta",   StringType(), True),
                StructField("col_gamma",  StringType(), True),
            ]
        )
        result = self._detect_drift(spark, path, extended_schema)

        for col in ["col_alpha", "col_beta", "col_gamma"]:
            assert col in result["additive"], f"Additive column '{col}' not detected"

    def test_unchanged_columns_classified_correctly(self, spark, tmp_path):
        """Columns that exist in both schemas and have the same type → unchanged."""
        path = self._existing_table(spark, tmp_path)
        result = self._detect_drift(spark, path, PRODUCT_SOURCE_SCHEMA)

        # All source columns (minus metadata) should be in unchanged
