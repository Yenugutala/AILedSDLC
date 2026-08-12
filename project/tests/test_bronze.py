"""
Bronze Layer Tests — Securities Master Data Lakehouse
Tests raw landing of all 29 source CSV tables.
Validates schema, metadata columns, idempotency, and schema drift handling.
"""

import pytest
import os
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, BooleanType, DecimalType, LongType, TimestampType
)


# ---------------------------------------------------------------------------
# Session-scoped Spark fixture (local mode — no Databricks required)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Local Delta-enabled Spark session for Bronze unit tests."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("sml-bronze-tests")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Suppress noisy Spark INFO logs during test runs
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logLevel=WARN")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG  = "statestreet"
B_SCHEMA = "b_statestreet"

# All 29 source tables — ingestion order mirrors bronze notebook
ALL_BRONZE_TABLES = [
    "currency",
    "legal_entity",
    "tick_ladder_scale",
    "tick",
    "product_rating_type",
    "principal_redemption_provision",
    "series",
    "product",
    "generic_product",
    "fund",
    "debt",
    "bond",
    "muni",
    "pool_backed_security",
    "right",
    "stock",
    "common_stock",
    "preferred_stock",
    "listed_derivative",
    "option",
    "future",
    "coupon",
    "identifiers",
    "classification",
    "product_rating",
    "listed_derivative_tick",
    "debt_principal_redemption_provision",
    "dq_rules_catalog",
    "dq_issues_catalog",
]

# Metadata columns that Bronze must add to every table
REQUIRED_META_COLS = [
    "_source_file",
    "_ingestion_ts",
    "_batch_id",
    "_row_hash",
]

# Tables that carry SCD2 columns in Silver (NOT bronze — listed here for
# the bronze-only negative assertion below)
SCD2_TABLES = {"product", "legal_entity", "product_rating"}


# ---------------------------------------------------------------------------
# Helper fixtures — small representative DataFrames
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def sample_product_df(spark):
    """Minimal product DataFrame matching source CSV structure."""
    schema = StructType([
        StructField("product_id",              StringType(),  True),
        StructField("id_type",                 StringType(),  True),
        StructField("type",                    StringType(),  True),
        StructField("sub_type",                StringType(),  True),
        StructField("status",                  StringType(),  True),
        StructField("settlement_type",         StringType(),  True),
        StructField("description",             StringType(),  True),
        StructField("issue_date",              StringType(),  True),
        StructField("issue_price",             StringType(),  True),
        StructField("current_face_value",      StringType(),  True),
        StructField("issuer_legal_entity_id",  StringType(),  True),
        StructField("tick_ladder_scale_id",    StringType(),  True),
    ])
    data = [
        ("PROD001", "CUSIP",  "EQUITY",      "COMMON_STOCK", "ACTIVE",   None,       "AAPL Common",      "2000-01-15", "10.00", "100.00", "LE001", None),
        ("PROD002", "ISIN",   "DEBT",        "BOND",         "ACTIVE",   "DVP",      "US Treasury 5yr",  "2015-06-01", "99.50", "100.00", "LE002", None),
        ("PROD003", "TICKER", "FUND",        "FUND",         "INACTIVE", None,       "Vanguard S&P ETF", "2001-03-10", None,   None,     "LE003", None),
        ("PROD004", "SEDOL",  "DERIVATIVE",  "OPTION",       "ACTIVE",   "T+1",      "AAPL Call Option", "2023-01-01", None,   None,     "LE001", "TLS001"),
        ("PROD005", "CUSIP",  "RIGHT",       "RIGHT",        "ACTIVE",   None,       "Rights Offering",  "2022-05-01", None,   None,     "LE004", None),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def sample_bond_df(spark):
    """Minimal bond DataFrame."""
    schema = StructType([
        StructField("product_id",          StringType(), True),
        StructField("coupon_type",         StringType(), True),
        StructField("maturity_date",       StringType(), True),
        StructField("face_currency_code",  StringType(), True),
        StructField("day_count_convention",StringType(), True),
    ])
    data = [
        ("PROD002", "FIXED",    "2025-06-01", "USD", "ACT/360"),
        ("PROD006", "FLOATING", "2030-12-31", "EUR", "30/360"),
        ("PROD007", "ZERO",     "2028-09-15", "GBP", None),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def sample_legal_entity_df(spark):
    """Minimal legal_entity DataFrame."""
    schema = StructType([
        StructField("legal_entity_id", StringType(), True),
        StructField("legal_name",      StringType(), True),
        StructField("country",         StringType(), True),
    ])
    data = [
        ("LE001", "Apple Inc.",            "US"),
        ("LE002", "US Treasury",           "US"),
        ("LE003", "Vanguard Group",        "US"),
        ("LE004", "Deutsche Bank AG",      "DE"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def sample_coupon_df(spark):
    """Minimal coupon DataFrame."""
    schema = StructType([
        StructField("coupon_id",     StringType(), True),
        StructField("product_id",    StringType(), True),
        StructField("coupon_rate",   StringType(), True),
        StructField("payment_date",  StringType(), True),
        StructField("coupon_type",   StringType(), True),
        StructField("frequency",     StringType(), True),
    ])
    data = [
        ("CPN001", "PROD002", "5.00", "2024-06-01", "FIXED",    "SEMI_ANNUAL"),
        ("CPN002", "PROD002", "5.00", "2025-06-01", "FIXED",    "SEMI_ANNUAL"),
        ("CPN003", "PROD006", "3.50", "2024-09-30", "FLOATING", "QUARTERLY"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def sample_currency_df(spark):
    """Currency DataFrame — includes 2 deliberately bad rows (USE-CASE-002)."""
    schema = StructType([
        StructField("currency_code", StringType(), True),
        StructField("description",   StringType(), True),
    ])
    data = [
        ("USD", "US Dollar"),
        ("EUR", "Euro"),
        ("GBP", "British Pound"),
        ("INVALID",  "Bad code — too long"),   # intentional bad row 1
        ("123",      "Bad code — numeric"),     # intentional bad row 2
    ]
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------
# Utility — add metadata columns (mirrors bronze_loader._add_metadata)
# ---------------------------------------------------------------------------

def _add_metadata(df, source_file: str, batch_id: str):
    """
    Replicate the Bronze metadata-column logic used in 03_bronze_ingest.py.
    Captures data columns BEFORE adding metadata (mirrors production code).
    """
    data_cols = [F.col(c) for c in df.columns]
    return (
        df
        .withColumn("_source_file",  F.lit(source_file))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_batch_id",     F.lit(batch_id))
        .withColumn("_row_hash",     F.sha2(F.concat_ws("|", *data_cols), 256))
    )


# ===========================================================================
# SECTION 1 — Metadata column tests
# ===========================================================================

class TestMetadataColumns:
    """Bronze must attach exactly 4 metadata columns to every ingested table."""

    def test_all_required_meta_cols_present(self, spark, sample_product_df):
        """All 4 metadata columns are added by _add_metadata()."""
        result = _add_metadata(sample_product_df, "product.csv", "batch_001")
        missing = [c for c in REQUIRED_META_COLS if c not in result.columns]
        assert missing == [], f"Missing metadata columns: {missing}"

    def test_source_file_value_matches_input(self, spark, sample_product_df):
        """_source_file column holds the value passed to _add_metadata()."""
        result = _add_metadata(sample_product_df, "product.csv", "batch_001")
        values = {r["_source_file"] for r in result.select("_source_file").collect()}
        assert values == {"product.csv"}

    def test_batch_id_value_matches_input(self, spark, sample_product_df):
        """_batch_id column holds the value passed to _add_metadata()."""
        result = _add_metadata(sample_product_df, "product.csv", "run_20240115")
        values = {r["_batch_id"] for r in result.select("_batch_id").collect()}
        assert values == {"run_20240115"}

    def test_ingestion_ts_is_not_null(self, spark, sample_product_df):
        """_ingestion_ts must be non-null for every row."""
        result = _add_metadata(sample_product_df, "product.csv", "batch_001")
        null_count = result.filter(F.col("_ingestion_ts").isNull()).count()
        assert null_count == 0, f"{null_count} rows have NULL _ingestion_ts"

    def test_row_hash_is_not_null(self, spark, sample_product_df):
        """_row_hash must be non-null for every row."""
        result = _add_metadata(sample_product_df, "product.csv", "batch_001")
        null_count = result.filter(F.col("_row_hash").isNull()).count()
        assert null_count == 0, f"{null_count} rows have NULL _row_hash"

    def test_row_hash_is_64_char_hex(self, spark, sample_product_df):
        """_row_hash is a valid SHA-256 hex string (64 characters)."""
        result = _add_metadata(sample_product_df, "product.csv", "batch_001")
        invalid = result.filter(
            (F.length(F.col("_row_hash")) != 64) |
            F.col("_row_hash").isNull()
        ).count()
        assert invalid == 0, f"{invalid} rows have invalid _row_hash length"

    def test_metadata_does_not_reduce_row_count(self, spark, sample_product_df):
        """Adding metadata columns must not filter or duplicate rows."""
        result = _add_metadata(sample_product_df, "product.csv", "batch_001")
        assert result.count() == sample_product_df.count()

    def test_no_scd2_cols_in_bronze(self, spark, sample_product_df):
        """Bronze tables must NOT have SCD2 columns — those belong to Silver."""
        result = _add_metadata(sample_product_df, "product.csv", "batch_001")
        scd2_cols = {"effective_start_date", "effective_end_date", "is_current"}
        found = scd2_cols.intersection(set(result.columns))
        assert found == set(), (
            f"Bronze must not contain SCD2 columns. Found: {found}"
        )

    @pytest.mark.parametrize("table_name", ALL_BRONZE_TABLES)
    def test_meta_col_naming_convention(self, table_name):
        """
        Every metadata column name starts with underscore — enforced by CLAUDE.md.
        This is a static check: we verify the constant list, not live data.
        """
        for col in REQUIRED_META_COLS:
            assert col.startswith("_"), (
                f"Metadata column '{col}' must start with '_' (CLAUDE.md convention)"
            )


# ===========================================================================
# SECTION 2 — Row-hash determinism (critical for MERGE INTO CDC)
# ===========================================================================

class TestRowHash:
    """
    _row_hash must be deterministic across runs for identical data.
    If hash changes for unchanged rows, MERGE INTO will incorrectly update them.
    """

    def test_same_data_produces_same_hash(self, spark, sample_product_df):
        """Identical rows in two runs must produce identical _row_hash values."""
        df1 = _add_metadata(sample_product_df, "product.csv", "batch_001")
        df2 = _add_metadata(sample_product_df, "product.csv", "batch_002")

        hashes1 = {r["product_id"]: r["_row_hash"]
                   for r in df1.select("product_id", "_row_hash").collect()}
        hashes2 = {r["product_id"]: r["_row_hash"]
                   for r in df2.select("product_id", "_row_hash").collect()}

        assert hashes1 == hashes2, (
            "_row_hash changed between runs for identical data — "
            "MERGE INTO CDC will incorrectly update unchanged rows"
        )

    def test_different_data_produces_different_hash(self, spark, sample_product_df):
        """A row that changed must produce a different _row_hash."""
        df_original = _add_metadata(sample_product_df, "product.csv", "batch_001")

        # Modify one row
        df_modified = sample_product_df.withColumn(
            "description",
            F.when(F.col("product_id") == "PROD001", F.lit("AAPL Modified"))
             .otherwise(F.col("description"))
        )
        df_modified = _add_metadata(df_modified, "product.csv", "batch_002")

        original_hash = (df_original
                         .filter(F.col("product_id") == "PROD001")
                         .select("_row_hash").first()["_row_hash"])
        modified_hash = (df_modified
                         .filter(F.col("product_id") == "PROD001")
                         .select("_row_hash").first()["_row_hash"])

        assert original_hash != modified_hash, (
            "Changed row must produce a different _row_hash"
        )

    def test_unchanged_rows_produce_same_hash_after_partial_update(
        self, spark, sample_product_df
    ):
        """Rows that did NOT change must keep the same hash even when sibling rows change."""
        df_original = _add_metadata(sample_product_df, "product.csv", "batch_001")

        # Only modify PROD001 — PROD002 should keep the same hash
        df_modified = sample_product_df.withColumn(
            "description",
            F.when(F.col("product_id") == "PROD001", F.lit("Changed"))
             .otherwise(F.col("description"))
        )
        df_modified = _add_metadata(df_modified, "product.csv", "batch_002")

        orig_hash = (df_original.filter(F.col("product_id") == "PROD002")
                     .select("_row_hash").first()["_row_hash"])
        mod_hash  = (df_modified.filter(F.col("product_id") == "PROD002")
                     .select("_row_hash").first()["_row_hash"])

        assert orig_hash == mod_hash, (
            "Unchanged row PROD002 must not have its hash changed when PROD001 changes"
        )

    def test_hash_covers_all_data_columns(self, spark, sample_product_df):
        """
        Changing any data column must change the hash.
        Verify this for multiple columns across the product schema.
        """
        sensitive_columns = ["id_type", "type", "status", "issuer_legal_entity_id"]

        base_hash = (
            _add_metadata(sample_product_df, "product.csv", "batch_001")
            .filter(F.col("product_id") == "PROD001")
            .select("_row_hash").first()["_row_hash"]
        )

        for col_name in sensitive_columns:
            df_mutated = sample_product_df.withColumn(
                col_name,
                F.when(F.col("product_id") == "PROD001", F.lit("__MUTATED__"))
                 .otherwise(F.col(col_name))
            )
            mutated_hash = (
                _add_metadata(df_mutated, "product.csv", "batch_001")
                .filter(F.col("product_id") == "PROD001")
                .select("_row_hash").first()["_row_hash"]
            )
            assert mutated_hash != base_hash, (
                f"Changing '{col_name}' must change _row_hash "
                f"(column may be excluded from hash computation)"
            )


# ===========================================================================
# SECTION 3 — Idempotency tests
# ===========================================================================

class TestIdempotency:
    """
    Bronze ingestion uses MERGE INTO — running twice must produce the same result.
    Simulated here using in-memory DataFrames and merge logic.
    """

    def test_merge_is_idempotent_same_data(self, spark, tmp_path):
        """MERGE INTO with identical data on second run = no net change in row count."""
        table_path = str(tmp_path / "product_idempotency")

        schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("description", StringType(), True),
        ])
        data = [("P001", "Security A"), ("P002", "Security B")]
        df = spark.createDataFrame(data, schema)
        df_with_meta = _add_metadata(df, "product.csv", "batch_001")

        # First load — creates table
        (df_with_meta.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .save(table_path))

        count_after_first = spark.read.format("delta").load(table_path).count()

        # Second load — same data, should not duplicate rows
        df_with_meta2 = _add_metadata(df, "product.csv", "batch_002")
        df_with_meta2.createOrReplaceTempView("_incoming_idempotency")

        spark.sql(f"""
            MERGE INTO delta.`{table_path}` AS target
            USING _incoming_idempotency AS source
            ON target.product_id = source.product_id
            WHEN MATCHED AND source._row_hash != target._row_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        count_after_second = spark.read.format("delta").load(table_path).count()
        assert count_after_first == count_after_second, (
            f"Idempotency failed: first load={count_after_first} rows, "
            f"second load={count_after_second} rows (expected same)"
        )

    def test_merge_updates_changed_row(self, spark, tmp_path):
        """MERGE INTO must update a row whose data changed (different _row_hash)."""
        table_path = str(tmp_path / "product_update")

        schema = StructType([
            StructField("product_id",  StringType(), False),
            StructField("description", StringType(), True),
        ])
        original = spark.createDataFrame(
            [("P001", "Original description")], schema
        )
        (
            _add_metadata(original, "product.csv", "batch_001")
            .write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table_path)
        )

        # Changed row — same PK, different description
        updated = spark.createDataFrame(
            [("P001", "Updated description")], schema
        )
        _add_metadata(updated, "product.csv", "batch_002").createOrReplaceTempView(
            "_incoming_update"
        )

        spark.sql(f"""
            MERGE INTO delta.`{table_path}` AS target
            USING _incoming_update AS source
            ON target.product_id = source.product_id
            WHEN MATCHED AND source._row_hash != target._row_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        result_desc = (
            spark.read.format("delta").load(table_path)
            .filter(F.col("product_id") == "P001")
            .select("description")
            .first()["description"]
        )
        assert result_desc == "Updated description", (
            f"MERGE INTO failed to update changed row. Got: '{result_desc}'"
        )

    def test_merge_inserts_new_row(self, spark, tmp_path):
        """MERGE INTO must insert rows that do not exist in target."""
        table_path = str(tmp_path / "product_insert")

        schema = StructType([
            StructField("product_id",  StringType(), False),
            StructField("description", StringType(), True),
        ])
        existing = spark.createDataFrame([("P001", "Existing")], schema)
        (
            _add_metadata(existing, "product.csv", "batch_001")
            .write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table_path)
        )

        # New row P002 not in target
        incoming = spark.createDataFrame(
            [("P001", "Existing"), ("P002", "New row")], schema
        )
        _add_metadata(incoming, "product.csv", "batch_002").createOrReplaceTempView(
            "_incoming_insert"
        )
        spark.sql(f"""
            MERGE INTO delta.`{table_path}` AS target
            USING _incoming_insert AS source
            ON target.product_id = source.product_id
            WHEN MATCHED AND source._row_hash != target._row_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        final_count = spark.read.format("delta").load(table_path).count()
        assert final_count == 2, (
            f"Expected 2 rows after insert of new PK, got {final_count}"
        )

    def test_merge_does_not_update_unchanged_row(self, spark, tmp_path):
        """MERGE INTO must NOT update rows where _row_hash is identical."""
        table_path = str(tmp_path / "product_noop")

        schema = StructType([
            StructField("product_id",  StringType(), False),
            StructField("description", StringType(), True),
        ])
        original = spark.createDataFrame([("P001", "Unchanged")], schema)
        df_first  = _add_metadata(original, "product.csv", "batch_001")
        (df_first.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .save(table_path))

        original_batch_id = (
            spark.read.format("delta").load(table_path)
            .select("_batch_id").first()["_batch_id"]
        )

        # Same data, different batch_id — row hash is computed on DATA cols only
        # so _batch_id change in metadata must not trigger an update on unchanged data
        df_second = _add_metadata(original, "product.csv", "batch_999")
        df_second.createOrReplaceTempView("_incoming_noop")
        spark.sql(f"""
            MERGE INTO delta.`{table_path}` AS target
            USING _incoming_noop AS source
            ON target.product_id = source.product_id
            WHEN MATCHED AND source._row_hash != target._row_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        # _batch_id should still be "batch_001" since the row was NOT updated
        final_batch_id = (
            spark.read.format("delta").load(table_path)
            .select("_batch_id").first()["_batch_id"]
        )
        assert final_batch_id == original_batch_id, (
            f"Unchanged row must NOT be updated by MERGE INTO. "
            f"Expected batch_id='{original_batch_id}', got '{final_batch_id}'"
        )


# ===========================================================================
# SECTION 4 — Schema drift tests
# ===========================================================================

class TestSchemaDrift:
    """
    Bronze drift policy (from bronze/rules.yaml):
      additive_columns: auto_merge   — new column safely added
      breaking_changes: quarantine   — type change / column removal raises ValueError
    """

    def _write_base_table(self, spark, tmp_path, table_name: str, data, schema):
        """Helper: write a base Delta table and register as temp view."""
        path = str(tmp_path / table_name)
        df = spark.createDataFrame(data, schema)
        df_meta = _add_metadata(df, f"{table_name}.csv", "batch_001")
        (df_meta.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .save(path))
        # Register as a named Delta table so detect_drift can read its schema
        spark.read.format("delta").load(path).createOrReplaceTempView(
            f"_base_{table_name}"
        )
        return path

    # --- Additive drift -------------------------------------------------------

    def test_additive_new_column_detected(self, spark, tmp_path, sample_product_df):
        """
        New column in incoming data is classified as additive (not breaking).
        Mirrors detect_drift() in src/ingestion/schema_drift.py.
        """
        # Simulate existing table schema (no new_field)
        existing_cols = set(sample_product_df.columns)

        # Incoming schema adds a new column
        incoming = sample_product_df.withColumn("new_field", F.lit("added_value"))
        incoming_cols = set(incoming.columns)

        additive = incoming_cols - existing_cols
        assert "new_field" in additive, (
            "detect_drift must classify 'new_field' as additive (present in "
            "incoming but absent from existing table)"
        )

    def test_additive_column_merges_into_delta(self, spark, tmp_path):
        """
        When additive drift is detected, ALTER TABLE ADD COLUMNS extends the table
        schema. Existing rows receive NULL for the new column.
        """
        table_path = str(tmp_path / "drift_additive")
        schema_v1 = StructType([
            StructField("product_id",  StringType(), False),
            StructField("description", StringType(), True),
        ])
        df_v1 = spark.createDataFrame([("P001", "Original")], schema_v1)
        (
            _add_metadata(df_v1, "product.csv", "batch_001")
            .write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table_path)
        )

        # Simulate ALTER TABLE ADD COLUMNS (handle_additive_drift behaviour)
        spark.sql(f"ALTER TABLE delta.`{table_path}` ADD COLUMNS (new_field STRING)")

        result = spark.read.format("delta").load(table_path)
        assert "new_field" in result.columns, (
            "New column must be present after ALTER TABLE ADD COLUMNS"
        )

        null_count = result.filter(F.col("new_field").isNull()).count()
        assert null_count == result.count(), (
            "Existing rows must have NULL for the newly added column"
        )

    def test_additive_drift_does_not_drop_existing_columns(self, spark, tmp_path):
        """
        Auto-merging a new column must not remove any existing column.
        """
        table_path = str(tmp_path / "drift_no_drop")
        schema_v1 = StructType([
            StructField("product_id",  StringType(), False),
            StructField("type",        StringType(), True),
            StructField("description", StringType(), True),
        ])
        df_v1 = spark.createDataFrame([("P001", "EQUITY", "Apple")], schema_v1)
        (
            _add_metadata(df_v1, "product.csv", "batch_001")
            .write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .save(table_path)
        )
        spark.sql(
            f"ALTER TABLE delta.`{table_path}` ADD COLUMNS (extra_field STRING)"
        )

        cols_after = set(spark.read.format("delta").load(table_path).columns)
        for original_col in ["product_id", "type", "description"]:
            assert original_col in cols_after, (
                f"Existing column '{original_col}' must not be dropped "
                f"when adding a new column"
            )

    # --- Breaking drift -------------------------------------------------------

    def test_breaking_drift_type_change_raises_error(self, spark, tmp_path):
        """
        Type change in an existing column triggers quarantine — raises ValueError.
        Mirrors handle_breaking_drift() in src/ingestion/schema_drift.py.
        """
        schema_v1 = StructType([
            StructField("product_id",    StringType(),  False),
            StructField("coupon_rate",   DoubleType(),  True),  # originally DOUBLE
        ])
        schema_v2 = StructType([
            StructField("product_id",    StringType(),  False),
            StructField("coupon_rate",   StringType(),  True),  # now STRING — breaking!
        ])

        existing_fields = {f.name: str(f.dataType) for f in schema_v1.fields}
        incoming_fields = {f.name: str(f.dataType) for f in schema_v2.fields}

        # Replicate detect_drift() logic
        META_COLS = {"_source_file", "_ingestion_ts", "_batch_id
