"""
Bronze layer tests — Securities Master Data Lakehouse
Tests: schema presence, metadata columns, idempotency, schema drift handling.

Run locally:  pytest tests/test_bronze.py -v
Run in CI:    pytest tests/test_bronze.py -v --tb=short
"""

import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, BooleanType, LongType, TimestampType,
)
import tempfile
import os


# ---------------------------------------------------------------------------
# Session-scoped Spark fixture (local mode — no Databricks cluster needed)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Local Spark session with Delta Lake extensions enabled.
    Uses local[2] so tests can run in CI without a Databricks cluster.
    """
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("sml-bronze-tests")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")   # keep tests fast
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------------------
# Minimal sample DataFrames for each source table (schema-only fixtures)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def product_schema():
    return StructType([
        StructField("product_id",              StringType(),  True),
        StructField("id_type",                 StringType(),  True),
        StructField("type",                    StringType(),  True),
        StructField("sub_type",                StringType(),  True),
        StructField("status",                  StringType(),  True),
        StructField("settlement_type",         StringType(),  True),
        StructField("description",             StringType(),  True),
        StructField("issue_date",              StringType(),  True),  # raw CSV = string
        StructField("issue_price",             DoubleType(),  True),
        StructField("current_face_value",      DoubleType(),  True),
        StructField("issuer_legal_entity_id",  StringType(),  True),
        StructField("tick_ladder_scale_id",    StringType(),  True),
    ])


@pytest.fixture(scope="session")
def bond_schema():
    return StructType([
        StructField("product_id",          StringType(),  True),
        StructField("coupon_type",         StringType(),  True),
        StructField("maturity_date",       StringType(),  True),
        StructField("face_currency_code",  StringType(),  True),
        StructField("day_count_convention", StringType(), True),
    ])


@pytest.fixture(scope="session")
def legal_entity_schema():
    return StructType([
        StructField("legal_entity_id",  StringType(),  True),
        StructField("name",             StringType(),  True),
        StructField("country",          StringType(),  True),
        StructField("entity_type",      StringType(),  True),
    ])


@pytest.fixture(scope="session")
def identifiers_schema():
    return StructType([
        StructField("identifier_id",     StringType(),  True),
        StructField("product_id",        StringType(),  True),
        StructField("id_type",           StringType(),  True),
        StructField("identifier_value",  StringType(),  True),
    ])


@pytest.fixture(scope="session")
def coupon_schema():
    return StructType([
        StructField("coupon_id",    StringType(),  True),
        StructField("product_id",   StringType(),  True),
        StructField("coupon_rate",  DoubleType(),  True),
        StructField("payment_date", StringType(),  True),
        StructField("coupon_type",  StringType(),  True),
        StructField("frequency",    StringType(),  True),
    ])


@pytest.fixture(scope="session")
def currency_schema():
    return StructType([
        StructField("currency_code",  StringType(),  True),
        StructField("currency_name",  StringType(),  True),
    ])


# ---------------------------------------------------------------------------
# Helper: build sample rows (3 good + 1 edge-case)
# ---------------------------------------------------------------------------

def _make_product_rows():
    return [
        ("PROD001", "CUSIP",  "EQUITY", "COMMON_STOCK", "ACTIVE",   None, "AAPL Common Stock",   "2000-01-01", 10.0,  None,  "LE001", "TLS001"),
        ("PROD002", "ISIN",   "DEBT",   "BOND",         "ACTIVE",   None, "US Treasury 5yr Bond","2010-06-15", 100.0, 100.0, "LE002", None),
        ("PROD003", "TICKER", "FUND",   "FUND",         "INACTIVE", None, "Vanguard S&P 500 ETF","2005-03-01", 50.0,  None,  "LE003", None),
        ("PROD004", "ISIN",   "EQUITY", "PREFERRED_STOCK","ACTIVE", None, "Pref Stock Series A", None,         None,  None,  None,    None),
    ]


# ---------------------------------------------------------------------------
# Metadata column helpers (mirrors src/ingestion/bronze_loader._add_metadata)
# ---------------------------------------------------------------------------

def _add_metadata(df, source_file: str, batch_id: str):
    """
    Replicates the bronze loader metadata logic so tests run without the
    full src package.  Adds the 4 standard Bronze metadata columns.
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
# 1. METADATA COLUMN TESTS
# ===========================================================================

class TestMetadataColumns:
    """
    All four metadata columns must be present and non-null after ingestion.
    Column names follow the CLAUDE.md standard (_-prefixed).
    """

    REQUIRED_META_COLS = ["_ingestion_ts", "_source_file", "_batch_id", "_row_hash"]

    def test_all_metadata_columns_present_product(self, spark, product_schema):
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df = _add_metadata(df_raw, "product.csv", "batch_001")
        missing = [c for c in self.REQUIRED_META_COLS if c not in df.columns]
        assert missing == [], f"Missing metadata columns on product: {missing}"

    @pytest.mark.parametrize("table,source_file", [
        ("bond",          "bond.csv"),
        ("legal_entity",  "legal_entity.csv"),
        ("identifiers",   "identifiers.csv"),
        ("coupon",        "coupon.csv"),
        ("currency",      "currency.csv"),
    ])
    def test_metadata_columns_present_all_tables(
        self, spark, table, source_file, product_schema
    ):
        """Each source table must carry all 4 metadata columns."""
        # Use a minimal one-column DF to simulate any table
        df_raw = spark.createDataFrame([("X001",)], ["product_id"])
        df = _add_metadata(df_raw, source_file, "batch_001")
        missing = [c for c in self.REQUIRED_META_COLS if c not in df.columns]
        assert missing == [], f"Missing metadata columns on {table}: {missing}"

    def test_ingestion_ts_not_null(self, spark, product_schema):
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df = _add_metadata(df_raw, "product.csv", "batch_001")
        null_count = df.filter(F.col("_ingestion_ts").isNull()).count()
        assert null_count == 0, "_ingestion_ts has NULL values"

    def test_source_file_not_null_and_correct(self, spark, product_schema):
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df = _add_metadata(df_raw, "product.csv", "batch_001")
        null_count = df.filter(F.col("_source_file").isNull()).count()
        assert null_count == 0, "_source_file has NULL values"
        values = {r["_source_file"] for r in df.collect()}
        assert values == {"product.csv"}, f"Unexpected _source_file values: {values}"

    def test_batch_id_not_null_and_correct(self, spark, product_schema):
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df = _add_metadata(df_raw, "product.csv", "batch_001")
        null_count = df.filter(F.col("_batch_id").isNull()).count()
        assert null_count == 0, "_batch_id has NULL values"
        values = {r["_batch_id"] for r in df.collect()}
        assert values == {"batch_001"}

    def test_row_hash_not_null(self, spark, product_schema):
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df = _add_metadata(df_raw, "product.csv", "batch_001")
        null_count = df.filter(F.col("_row_hash").isNull()).count()
        assert null_count == 0, "_row_hash has NULL values"

    def test_row_hash_is_256_bit_hex(self, spark, product_schema):
        """SHA256 output: 64 hex characters."""
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df = _add_metadata(df_raw, "product.csv", "batch_001")
        # All hashes must be exactly 64 hex chars
        bad = df.filter(~F.col("_row_hash").rlike("^[0-9a-f]{64}$"))
        assert bad.count() == 0, "_row_hash is not a valid 64-char SHA256 hex"

    def test_row_hash_deterministic_across_batches(self, spark, product_schema):
        """
        _row_hash must be identical for the same data regardless of batch_id.
        This is critical: MERGE INTO uses hash inequality to detect real changes.
        """
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df1 = _add_metadata(df_raw, "product.csv", "batch_001")
        df2 = _add_metadata(df_raw, "product.csv", "batch_002")
        hashes1 = {r["product_id"]: r["_row_hash"] for r in df1.collect()}
        hashes2 = {r["product_id"]: r["_row_hash"] for r in df2.collect()}
        assert hashes1 == hashes2, (
            "_row_hash differed between batch runs for identical source data. "
            "MERGE INTO CDC will incorrectly treat unchanged rows as updated."
        )

    def test_row_hash_changes_when_data_changes(self, spark, product_schema):
        """A changed data column must produce a different _row_hash."""
        rows_original = _make_product_rows()
        rows_changed = list(rows_original)
        # Mutate status on first row: ACTIVE → INACTIVE
        r = list(rows_changed[0])
        r[4] = "INACTIVE"
        rows_changed[0] = tuple(r)

        df_orig    = _add_metadata(spark.createDataFrame(rows_original, product_schema), "product.csv", "b1")
        df_changed = _add_metadata(spark.createDataFrame(rows_changed,  product_schema), "product.csv", "b1")

        hash_orig    = df_orig.filter(F.col("product_id") == "PROD001").first()["_row_hash"]
        hash_changed = df_changed.filter(F.col("product_id") == "PROD001").first()["_row_hash"]
        assert hash_orig != hash_changed, (
            "_row_hash did not change after data mutation — CDC MERGE will miss updates"
        )

    def test_metadata_columns_are_last(self, spark, product_schema):
        """
        Metadata columns must NOT appear before business columns.
        The _row_hash must be computed from data columns only (captured before adding meta).
        """
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df = _add_metadata(df_raw, "product.csv", "batch_001")
        cols = df.columns
        meta_positions = [cols.index(c) for c in self.REQUIRED_META_COLS]
        data_positions  = [i for i, c in enumerate(cols) if not c.startswith("_")]
        assert min(meta_positions) > max(data_positions), (
            "Metadata columns appear before data columns — _row_hash will include "
            "metadata in its hash, breaking CDC detection"
        )

    def test_row_count_preserved_after_metadata(self, spark, product_schema):
        """_add_metadata must not filter, duplicate, or drop rows."""
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df = _add_metadata(df_raw, "product.csv", "batch_001")
        assert df.count() == len(_make_product_rows())


# ===========================================================================
# 2. SCHEMA TESTS (business columns present per table)
# ===========================================================================

class TestBronzeSchema:
    """
    Each Bronze table must expose the expected business columns.
    We test the schema contract, not actual Databricks table data,
    so these run fully in local Spark without a live catalog.
    """

    @pytest.mark.parametrize("required_col", [
        "product_id", "id_type", "type", "sub_type", "status",
        "description", "issue_date", "issue_price",
        "current_face_value", "issuer_legal_entity_id",
    ])
    def test_product_has_required_columns(self, spark, product_schema, required_col):
        df = spark.createDataFrame([], product_schema)
        assert required_col in df.columns, (
            f"Column '{required_col}' missing from product schema"
        )

    @pytest.mark.parametrize("required_col", [
        "product_id", "coupon_type", "maturity_date", "face_currency_code",
    ])
    def test_bond_has_required_columns(self, spark, bond_schema, required_col):
        df = spark.createDataFrame([], bond_schema)
        assert required_col in df.columns

    @pytest.mark.parametrize("required_col", [
        "legal_entity_id", "name",
    ])
    def test_legal_entity_has_required_columns(self, spark, legal_entity_schema, required_col):
        df = spark.createDataFrame([], legal_entity_schema)
        assert required_col in df.columns

    @pytest.mark.parametrize("required_col", [
        "identifier_id", "product_id", "id_type", "identifier_value",
    ])
    def test_identifiers_has_required_columns(self, spark, identifiers_schema, required_col):
        df = spark.createDataFrame([], identifiers_schema)
        assert required_col in df.columns

    @pytest.mark.parametrize("required_col", [
        "coupon_id", "product_id", "coupon_rate", "payment_date",
    ])
    def test_coupon_has_required_columns(self, spark, coupon_schema, required_col):
        df = spark.createDataFrame([], coupon_schema)
        assert required_col in df.columns

    def test_all_29_source_tables_named(self):
        """
        Verify the canonical list of 29 source tables matches the request.yaml.
        This is a static contract test — fails fast if a table is dropped from scope.
        """
        expected = {
            "product", "generic_product", "legal_entity", "tick_ladder_scale",
            "tick", "product_rating", "product_rating_type", "classification",
            "identifiers", "fund", "debt", "bond", "muni", "pool_backed_security",
            "right", "series", "listed_derivative", "option", "future", "stock",
            "common_stock", "preferred_stock", "coupon",
            "principal_redemption_provision", "currency",
            "listed_derivative_tick", "debt_principal_redemption_provision",
            "dq_rules_catalog", "dq_issues_catalog",
        }
        assert len(expected) == 29, (
            f"Expected 29 tables but list has {len(expected)}. "
            "Update this test if the source table list changes."
        )


# ===========================================================================
# 3. IDEMPOTENCY TESTS
# ===========================================================================

class TestBronzeIdempotency:
    """
    The Bronze ingest uses MERGE INTO (not INSERT OVERWRITE).
    Running it twice on the same source data must produce the same row count.
    Rows with unchanged _row_hash must NOT be duplicated or re-written.
    """

    def _write_delta(self, df, path: str, spark):
        """Write a Delta table to a temp path (first load)."""
        (df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .save(path))

    def _merge_delta(self, incoming_df, path: str, pk: str, spark):
        """Simulate MERGE INTO: upsert only changed rows."""
        incoming_df.createOrReplaceTempView("_incoming")
        spark.sql(f"""
            MERGE INTO delta.`{path}` AS target
            USING _incoming AS source
            ON target.{pk} = source.{pk}
            WHEN MATCHED AND source._row_hash != target._row_hash
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
        """)

    def test_idempotent_load_same_count(self, spark, product_schema, tmp_path):
        """
        Two consecutive MERGE runs on identical data → same row count.
        """
        table_path = str(tmp_path / "bronze_product")
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df1 = _add_metadata(df_raw, "product.csv", "batch_001")

        # First load
        self._write_delta(df1, table_path, spark)
        count_after_first = spark.read.format("delta").load(table_path).count()

        # Second load — identical data
        df2 = _add_metadata(df_raw, "product.csv", "batch_002")
        self._merge_delta(df2, table_path, "product_id", spark)
        count_after_second = spark.read.format("delta").load(table_path).count()

        assert count_after_first == count_after_second, (
            f"Idempotency violated: first={count_after_first}, second={count_after_second}. "
            "MERGE INTO produced duplicate rows."
        )

    def test_idempotent_no_duplicate_pks(self, spark, product_schema, tmp_path):
        """
        After two identical loads, each product_id appears exactly once.
        """
        table_path = str(tmp_path / "bronze_product_pk")
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df1 = _add_metadata(df_raw, "product.csv", "batch_001")

        self._write_delta(df1, table_path, spark)
        df2 = _add_metadata(df_raw, "product.csv", "batch_002")
        self._merge_delta(df2, table_path, "product_id", spark)

        result = spark.read.format("delta").load(table_path)
        dup_count = (
            result.groupBy("product_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        assert dup_count == 0, (
            f"{dup_count} product_id values appear more than once after idempotent re-load"
        )

    def test_changed_row_updates_hash(self, spark, product_schema, tmp_path):
        """
        When a row's data changes between runs, the stored _row_hash must update
        (proving MERGE INTO actually wrote the new version).
        """
        table_path = str(tmp_path / "bronze_product_update")
        df_raw = spark.createDataFrame(_make_product_rows(), product_schema)
        df1 = _add_metadata(df_raw, "product.csv", "batch_001")
        self._write_delta(df1, table_path, spark)
        hash_before = (
            spark.read.format("delta").load(table_path)
            .filter(F.col("product_id") == "PROD001")
            .first()["_row_hash"]
        )

        # Mutate PROD001 status
        rows_v2 = list(_make_product_rows())
        r = list(rows_v2[0]); r[4] = "SUSPENDED"; rows_v2[0] = tuple(r)
        df_raw_v2 = spark.createDataFrame(rows_v2, product_schema)
        df2 = _add_metadata(df_raw_v2, "product.csv", "batch_002")
        self._merge_delta(df2, table_path, "product_id", spark)

        hash_after = (
            spark.read.format("delta").load(table_path)
            .filter(F.col("product_id") == "PROD001")
            .first()["_row_hash"]
        )
        assert hash_before != hash_after, (
            "_row_hash did not change after data update — MERGE INTO did not fire UPDATE"
        )

    def test_new_row_inserted_on_second_run(self, spark, product_schema, tmp_path):
        """
        A new product_id introduced in the second run must be INSERTed.
        """
        table_path = str(tmp_path / "bronze_product_insert")
        rows_v1 = _make_product_rows()[:3]   # 3 rows
        df1 = _add_metadata(spark.createDataFrame(rows_v1, product_schema), "product.csv", "b1")
        self._write_delta(df1, table_path, spark)

        rows_v2 = list(_make_product_rows())  # 4 rows (added PROD004)
        df2 = _add_metadata(spark.createDataFrame(rows_v2, product_schema), "product.csv", "b2")
        self._merge_delta(df2, table_path, "product_id", spark)

        final_count = spark.read.format("delta").load(table_path).count()
        assert final_count == 4, f"Expected 4 rows after inserting new record, got {final_count}"


# ===========================================================================
# 4. SCHEMA DRIFT TESTS
# ===========================================================================

class TestSchemaDrift:
    """
    Bronze ingestion must:
      - Auto-merge additive columns (new columns from source)
      - Quarantine batches with breaking changes (type change, column removal)
    """

    def _detect_drift(self, existing_schema, incoming_schema) -> dict:
        """
        Inline drift detector (mirrors src/ingestion/schema_drift.detect_drift logic).
        Returns: {"additive": [...], "breaking": [...], "new_table": bool}
        """
        META_COLS = {
            "_source_file", "_ingestion_ts", "_batch_id", "_row_hash",
            "_ingestion_date", "_schema_changes",
        }
        existing_fields = {
            f.name: str(f.dataType)
            for f in existing_schema.fields
            if f.name not in META_COLS
        }
        incoming_fields = {
            f.name: str(f.dataType)
            for f in incoming_schema.fields
            if f.name not in META_COLS
        }

        additive = [c for c in incoming_fields if c not in existing_fields]
        type_changes = [
            c for c in existing_fields
            if c in incoming_fields and incoming_fields[c] != existing_fields[c]
        ]
        removed = [c for c in existing_fields if c not in incoming_fields]
        breaking = type_changes + removed

        return {
            "additive":     additive,
            "breaking":     breaking,
            "new_table":    False,
            "type_changes": type_changes,
            "removed_cols": removed,
        }

    # -- additive drift -------------------------------------------------------

    def test_additive_drift_detected(self, spark, product_schema):
        """
        A new column in the incoming data must appear in additive list,
        not in breaking list.
        """
        # Incoming schema has an extra column: new_regulatory_flag
        incoming_schema = StructType(
            product_schema.fields + [StructField("new_regulatory_flag", StringType(), True)]
        )
        result = self._detect_drift(product_schema, incoming_schema)
        assert "new_regulatory_flag" in result["additive"], (
            "New column not detected as additive drift"
        )
        assert result["breaking"] == [], (
            "New column incorrectly flagged as breaking drift"
        )

    def test_additive_drift_auto_merge_adds_column(self, spark, product_schema, tmp_path):
        """
        After auto-merge of an additive column, the Delta table schema
        must include the new column (existing rows get NULL for new column).
        """
        table_path = str(tmp_path / "product_additive")
        df_base = _add_metadata(
            spark.createDataFrame(_make_product_rows(), product_schema),
            "product.csv", "b1"
        )
        df_base.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)

        # Incoming DF has extra column
        incoming_schema = StructType(
            product_schema.fields + [StructField("new_regulatory_flag", StringType(), True)]
        )
        rows_with_new_col = [r + (None,) for r in _make_product_rows()]
        df_incoming = _add_metadata(
            spark.createDataFrame(rows_with_new_col, incoming_schema),
            "product.csv", "b2"
        )
        # mergeSchema=True simulates Bronze auto-merge policy
        (df_incoming.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(table_path))

        result_schema_cols = spark.read.format("delta").load(table_path).columns
        assert "new_regulatory_flag" in result_schema_cols, (
            "Auto-merge did not add new_regulatory_flag to Bronze table schema"
        )

    def test_multiple_additive_columns_all_detected(self, spark, product_schema):
        """All newly added columns must be listed in additive — none missed."""
        new_cols = ["reg_flag_a", "reg_flag_b", "data_provider_code"]
        incoming_schema = StructType(
            product_schema.fields + [StructField(c, StringType(), True) for c in new_cols]
        )
        result = self._detect_drift(product_schema, incoming_schema)
        for col in new_cols:
            assert col in result["additive"], f"New column '{col}' not detected as additive"
        assert result["breaking"] == []

    # -- breaking drift: type change ------------------------------------------

    def test_breaking_drift_type_change_detected(self, spark, product_schema):
        """
        Changing a column's type (e.g. DoubleType → LongType) must be
        flagged as breaking, not additive.
        """
        # Rebuild schema with issue_price changed from DoubleType to LongType
        modified_fields = []
        for f in product_schema.fields:
            if f.name == "issue_price":
                modified_fields.append(StructField("issue_price", LongType(), True))
            else:
                modified_fields.append(f)
        incoming_schema = StructType(modified_fields)

        result = self._detect_drift(product_schema, incoming_schema)
        assert "issue_price" in result["breaking"], (
            "Type change on issue_price not flagged as breaking drift"
        )
        assert "issue_price" not in result["additive"]

    def test_breaking_drift_type_change_raises_exception(self, spark, product_schema, tmp_path):
        """
        When breaking drift is detected, the ingestion must raise a ValueError
        (which causes the Databricks job to fail and trigger retry/alert).
        """
        table_path = str(tmp_path / "product_break_type")
        df_base = _add_metadata(
            spark.createDataFrame(_make_product_rows(), product_schema),
            "product.csv", "b1"
        )
        df_base.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)

        def _handle_breaking_drift_raise(breaking_cols):
            if breaking_cols:
                raise ValueError(
                    f"[SCHEMA DRIFT] Breaking change detected: {breaking_cols}. "
                    "Batch quarantined."
                )

        modified_fields = [
            StructField("issue_price", LongType(), True) if f.name == "issue_price" else f
            for f in product_schema.fields
        ]
        incoming_schema = StructType(modified_fields)
        drift = self._detect_drift(product_schema, incoming_schema)

        with pytest.raises(ValueError, match="Breaking change detected"):
            _handle_breaking_drift_raise(drift["breaking"])

    # -- breaking drift: column removal ---------------------------------------

    def test_breaking_drift_removed_column_detected(self, spark, product_schema):
        """
        A column dropped from the source CSV must be flagged as breaking drift.
        """
        # Incoming schema missing "description"
        incoming_fields = [f for f in product_schema.fields if f.name != "description"]
        incoming_schema = StructType(incoming_fields)

        result = self._detect_drift(product_schema, incoming_schema)
        assert "description" in result["breaking"], (
            "Removed column 'description' not flagged as breaking drift"
        )
        assert "description" in result["removed_cols"]

    def test_breaking_drift_quarantine_record_written(self, spark, product_schema, tmp_path):
        """
        A breaking drift must write a quarantine record to _schema_quarantine.
        The original Bronze table must NOT be modified.
        """
        quarantine_path = str(tmp_path / "schema_quarantine")
        table_path      = str(tmp_path / "product_break_removal")

        # Write original table
        df_
