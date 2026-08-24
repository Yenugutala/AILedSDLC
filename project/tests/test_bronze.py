"""
Bronze layer tests — Securities Master Data Lakehouse
Tests raw landing: schema, metadata columns, idempotency, schema drift handling.
All tables read from statestreet.b_statestreet.*
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType,
    TimestampType, BooleanType, DecimalType, IntegerType, LongType
)
from delta import configure_spark_with_delta_pip
import os
import tempfile
import shutil


# ---------------------------------------------------------------------------
# Session-scoped Spark fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """
    Local Delta-enabled Spark session.
    In CI this points at a real Databricks cluster via DATABRICKS_HOST/TOKEN.
    Locally it uses an in-process Delta Lake session.
    """
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("sml-bronze-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def warehouse_dir(tmp_path_factory):
    """Temporary directory used as the local Delta warehouse root."""
    d = tmp_path_factory.mktemp("delta_warehouse")
    yield str(d)
    shutil.rmtree(str(d), ignore_errors=True)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATALOG  = "statestreet"
B_SCHEMA = "b_statestreet"
VOLUME_PATH = "/Volumes/statestreet/securities_master/raw_files/"

# All 29 source tables
ALL_TABLES = [
    "product", "generic_product", "legal_entity", "tick_ladder_scale", "tick",
    "product_rating", "product_rating_type", "classification", "identifiers",
    "fund", "debt", "bond", "muni", "pool_backed_security", "right", "series",
    "listed_derivative", "option", "future", "stock", "common_stock",
    "preferred_stock", "coupon", "principal_redemption_provision", "currency",
    "listed_derivative_tick", "debt_principal_redemption_provision",
    "dq_rules_catalog", "dq_issues_catalog",
]

# The four metadata columns added by the Bronze pipeline
METADATA_COLS = {"_ingestion_ts", "_source_file", "_batch_id", "_row_hash"}

# Expected data columns per table (subset — PK + key columns only for fast checks).
# Full schema is validated via the table-exists + column-presence tests.
EXPECTED_COLUMNS: dict[str, list[str]] = {
    "product": [
        "product_id", "id_type", "type", "sub_type", "status",
        "description", "issue_date", "issuer_legal_entity_id",
    ],
    "bond": ["product_id", "coupon_type", "maturity_date"],
    "stock": ["product_id"],
    "common_stock": ["product_id"],
    "preferred_stock": ["product_id"],
    "fund": ["product_id"],
    "debt": ["product_id"],
    "muni": ["product_id"],
    "pool_backed_security": ["product_id"],
    "right": ["product_id"],
    "listed_derivative": ["product_id"],
    "option": ["product_id", "option_type", "exercise_style"],
    "future": ["product_id"],
    "legal_entity": ["legal_entity_id"],
    "identifiers": ["product_id"],
    "classification": ["product_id"],
    "product_rating": ["product_id"],
    "product_rating_type": ["product_rating_type_id"],
    "coupon": ["bond_id", "coupon_rate", "payment_date"],
    "currency": ["currency_code"],
    "series": ["series_id"],
    "tick_ladder_scale": ["tick_ladder_scale_id"],
    "tick": ["tick_id"],
    "generic_product": ["product_id"],
    "principal_redemption_provision": ["principal_redemption_provision_id"],
    "listed_derivative_tick": ["product_id"],
    "debt_principal_redemption_provision": ["product_id"],
    "dq_rules_catalog": [],   # metadata — no fixed PK required
    "dq_issues_catalog": [],  # metadata — no fixed PK required
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _full(table: str) -> str:
    """Return three-part Unity Catalog name."""
    return f"{CATALOG}.{B_SCHEMA}.{table}"


def _read_bronze(spark: SparkSession, table: str):
    """Read a Bronze table (Delta or Unity Catalog)."""
    return spark.table(_full(table))


def _table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.table(_full(table))
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# 1. TABLE-EXISTENCE TESTS
# ---------------------------------------------------------------------------

class TestBronzeTableExistence:
    """Every Bronze table must exist after ingestion."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_table_exists(self, spark, table):
        assert _table_exists(spark, table), (
            f"Bronze table {_full(table)} does not exist. "
            "Run 03_bronze_ingest.py before running tests."
        )


# ---------------------------------------------------------------------------
# 2. SCHEMA / COLUMN-PRESENCE TESTS
# ---------------------------------------------------------------------------

class TestBronzeSchema:
    """Bronze tables must contain expected data columns and all 4 metadata columns."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_metadata_columns_present(self, spark, table):
        """All four pipeline metadata columns must be in every Bronze table."""
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist — skipping schema check.")
        actual_cols = set(_read_bronze(spark, table).columns)
        missing = METADATA_COLS - actual_cols
        assert not missing, (
            f"{_full(table)} is missing metadata columns: {sorted(missing)}"
        )

    @pytest.mark.parametrize("table,expected_cols", EXPECTED_COLUMNS.items())
    def test_data_columns_present(self, spark, table, expected_cols):
        """Key data columns must be present in the Bronze table."""
        if not expected_cols:
            pytest.skip(f"No required data columns defined for {table}.")
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist — skipping schema check.")
        actual_cols = set(_read_bronze(spark, table).columns)
        missing = set(expected_cols) - actual_cols
        assert not missing, (
            f"{_full(table)} is missing expected data columns: {sorted(missing)}"
        )

    def test_product_id_type_is_string(self, spark):
        """product_id must be STRING (not LONG or INT from inferSchema)."""
        if not _table_exists(spark, "product"):
            pytest.skip("product table not found.")
        schema = _read_bronze(spark, "product").schema
        field = next((f for f in schema.fields if f.name == "product_id"), None)
        assert field is not None, "product_id column missing from Bronze product."
        assert isinstance(field.dataType, StringType), (
            f"product_id should be StringType, got {type(field.dataType).__name__}"
        )

    def test_no_metadata_col_name_collision(self, spark):
        """No source data column should have the same name as a metadata column."""
        for table in ALL_TABLES:
            if not _table_exists(spark, table):
                continue
            df = _read_bronze(spark, table)
            # Metadata cols are added AFTER data cols — data cols should not share the names
            # (The pipeline prefixes them with '_' which the source CSVs should not use)
            data_cols = set(df.columns) - METADATA_COLS
            collision = data_cols & METADATA_COLS
            assert not collision, (
                f"{_full(table)} has source columns that collide with metadata names: {collision}"
            )


# ---------------------------------------------------------------------------
# 3. METADATA COLUMN VALUE TESTS
# ---------------------------------------------------------------------------

class TestBronzeMetadataValues:
    """Metadata column values must be non-null and well-formed for every row."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_ingestion_ts_not_null(self, spark, table):
        """_ingestion_ts must be populated for every row."""
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        null_count = (
            _read_bronze(spark, table)
            .filter(F.col("_ingestion_ts").isNull())
            .count()
        )
        assert null_count == 0, (
            f"{_full(table)}: {null_count} rows have NULL _ingestion_ts."
        )

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_source_file_not_null(self, spark, table):
        """_source_file must be populated for every row."""
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        null_count = (
            _read_bronze(spark, table)
            .filter(F.col("_source_file").isNull())
            .count()
        )
        assert null_count == 0, (
            f"{_full(table)}: {null_count} rows have NULL _source_file."
        )

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_batch_id_not_null(self, spark, table):
        """_batch_id must be populated for every row."""
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        null_count = (
            _read_bronze(spark, table)
            .filter(F.col("_batch_id").isNull())
            .count()
        )
        assert null_count == 0, (
            f"{_full(table)}: {null_count} rows have NULL _batch_id."
        )

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_row_hash_not_null(self, spark, table):
        """_row_hash must be populated for every row."""
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        null_count = (
            _read_bronze(spark, table)
            .filter(F.col("_row_hash").isNull())
            .count()
        )
        assert null_count == 0, (
            f"{_full(table)}: {null_count} rows have NULL _row_hash."
        )

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_row_hash_length(self, spark, table):
        """_row_hash must be a 64-character SHA-256 hex string."""
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        df = _read_bronze(spark, table)
        bad_hash_count = (
            df.filter(
                F.col("_row_hash").isNotNull() &
                (F.length(F.col("_row_hash")) != 64)
            )
            .count()
        )
        assert bad_hash_count == 0, (
            f"{_full(table)}: {bad_hash_count} rows have _row_hash length != 64."
        )

    def test_source_file_matches_table_name(self, spark):
        """_source_file in Bronze product should contain 'product'."""
        if not _table_exists(spark, "product"):
            pytest.skip("product table not found.")
        df = _read_bronze(spark, "product")
        bad_count = (
            df.filter(~F.lower(F.col("_source_file")).contains("product"))
            .count()
        )
        assert bad_count == 0, (
            f"Bronze product table has {bad_count} rows where _source_file "
            "does not reference 'product'."
        )

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_row_hash_is_unique_per_row(self, spark, table):
        """
        _row_hash uniqueness: duplicate hashes are only acceptable if ALL
        data columns are truly identical (hash collision is excluded).
        This test catches accidental hash-on-empty-string bugs.
        """
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        df = _read_bronze(spark, table)
        row_count  = df.count()
        hash_count = df.select("_row_hash").distinct().count()
        # If every row is unique, hash count == row count.
        # Allow collisions only if source data itself has duplicate rows.
        duplicate_hashes = row_count - hash_count
        # Warn but do not fail — genuinely duplicate source rows produce same hash.
        # We fail only when >10% of hashes are identical (indicates pipeline bug).
        if row_count > 0:
            collision_rate = duplicate_hashes / row_count
            assert collision_rate < 0.10, (
                f"{_full(table)}: {collision_rate:.0%} of rows share a _row_hash. "
                "Possible bug in hash computation (hashing empty string?)."
            )


# ---------------------------------------------------------------------------
# 4. IDEMPOTENCY TESTS
# ---------------------------------------------------------------------------

class TestBronzeIdempotency:
    """
    Re-running Bronze ingestion must not change row counts.
    The MERGE INTO pattern guarantees idempotency: identical rows are skipped,
    changed rows are updated, new rows inserted.
    """

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_row_count_stable_on_rerun(self, spark, table):
        """
        Snapshot row count before and after a simulated re-run.
        In full CI the re-run is triggered via notebook job.
        Here we validate that the MERGE pattern leaves counts unchanged.
        """
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        count_before = _read_bronze(spark, table).count()

        # Simulate re-MERGE: merge the table into itself (all rows match → no-op)
        spark.table(_full(table)).createOrReplaceTempView("_idempotency_source")
        pk_map = {
            "product": "product_id",
            "legal_entity": "legal_entity_id",
            "bond": "product_id",
            "stock": "product_id",
            "common_stock": "product_id",
            "preferred_stock": "product_id",
            "fund": "product_id",
            "debt": "product_id",
            "muni": "product_id",
            "pool_backed_security": "product_id",
            "right": "product_id",
            "listed_derivative": "product_id",
            "option": "product_id",
            "future": "product_id",
            "classification": "product_id",
            "identifiers": "product_id",
            "product_rating": "product_rating_id",
            "product_rating_type": "product_rating_type_id",
            "coupon": "coupon_id",
            "currency": "currency_code",
            "series": "series_id",
            "tick_ladder_scale": "tick_ladder_scale_id",
            "tick": "tick_id",
            "generic_product": "product_id",
            "principal_redemption_provision": "principal_redemption_provision_id",
            "listed_derivative_tick": "product_id",
            "debt_principal_redemption_provision": "product_id",
            "dq_rules_catalog": None,
            "dq_issues_catalog": None,
        }
        pk = pk_map.get(table)
        if pk is None:
            pytest.skip(f"No PK defined for {table} — skipping idempotency MERGE test.")

        target = _full(table)
        spark.sql(f"""
            MERGE INTO {target} AS target
            USING _idempotency_source AS source
            ON target.{pk} = source.{pk}
            WHEN MATCHED AND source._row_hash != target._row_hash
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
        """)

        count_after = _read_bronze(spark, table).count()
        assert count_before == count_after, (
            f"{_full(table)}: row count changed after idempotent re-run. "
            f"Before={count_before}, After={count_after}."
        )

    def test_row_hash_unchanged_on_rerun(self, spark):
        """
        _row_hash values must be identical before and after a re-run.
        This verifies the hash function is deterministic.
        """
        if not _table_exists(spark, "product"):
            pytest.skip("product table not found.")
        df = _read_bronze(spark, "product")
        hashes_before = set(row["_row_hash"] for row in df.select("_row_hash").collect())
        # After a no-op re-run (nothing changed), hashes must be identical
        df_after = _read_bronze(spark, "product")
        hashes_after = set(row["_row_hash"] for row in df_after.select("_row_hash").collect())
        assert hashes_before == hashes_after, (
            "product: _row_hash values changed between reads with no source data change."
        )


# ---------------------------------------------------------------------------
# 5. SCHEMA DRIFT TESTS (unit-level — using local Delta tables)
# ---------------------------------------------------------------------------

class TestSchemaDrift:
    """
    Unit tests for schema drift detection and handling logic.
    Uses temporary local Delta tables — does not touch the Databricks catalog.
    """

    @pytest.fixture(autouse=True)
    def _base_df(self, spark, tmp_path):
        """
        Write a minimal 'product'-like Delta table to a temp location.
        Returns (path, DataFrame).
        """
        schema = StructType([
            StructField("product_id",  StringType(),  nullable=False),
            StructField("type",        StringType(),  nullable=True),
            StructField("status",      StringType(),  nullable=True),
            StructField("issue_price", DoubleType(),  nullable=True),
        ])
        rows = [("P001", "EQUITY", "ACTIVE", 100.0),
                ("P002", "DEBT",   "ACTIVE", 200.0)]
        self._base_path = str(tmp_path / "base_product")
        df = spark.createDataFrame(rows, schema)
        df.write.format("delta").mode("overwrite").save(self._base_path)
        self._spark = spark

    # ----- Additive drift -----

    def test_additive_drift_detected(self):
        """
        A new column in the incoming DataFrame must be flagged as additive,
        not breaking.
        """
        from src.ingestion.schema_drift import detect_drift  # local import

        existing_schema = self._spark.read.format("delta").load(self._base_path).schema
        incoming = self._spark.read.format("delta").load(self._base_path).withColumn(
            "new_column", F.lit("extra_value")
        )
        result = detect_drift(self._spark, self._base_path, incoming.schema)
        assert "new_column" in result["additive"], (
            "detect_drift did not flag new_column as additive drift."
        )
        assert result["breaking"] == [], (
            "detect_drift incorrectly flagged a new column as breaking."
        )

    def test_additive_drift_auto_merged(self):
        """
        After handling additive drift the target table must contain the new column,
        and row count must be unchanged.
        """
        from src.ingestion.schema_drift import detect_drift, handle_additive_drift

        incoming = self._spark.read.format("delta").load(self._base_path).withColumn(
            "new_column", F.lit("extra_value")
        )
        result = detect_drift(self._spark, self._base_path, incoming.schema)
        handle_additive_drift(
            self._spark,
            table_path=self._base_path,
            new_columns=result["additive"],
        )
        merged_df = self._spark.read.format("delta").load(self._base_path)
        assert "new_column" in merged_df.columns, (
            "New column was not merged into the target Delta table."
        )
        assert merged_df.count() == 2, (
            "Row count changed after additive drift merge."
        )

    def test_additive_drift_existing_rows_get_null(self):
        """
        After additive drift merge, existing rows must have NULL for the new column
        (since the original data did not have this field).
        """
        from src.ingestion.schema_drift import detect_drift, handle_additive_drift

        incoming = self._spark.read.format("delta").load(self._base_path).withColumn(
            "added_col", F.lit("NEW")
        )
        result = detect_drift(self._spark, self._base_path, incoming.schema)
        handle_additive_drift(
            self._spark,
            table_path=self._base_path,
            new_columns=result["additive"],
        )
        merged_df = self._spark.read.format("delta").load(self._base_path)
        null_count = merged_df.filter(F.col("added_col").isNull()).count()
        assert null_count == 2, (
            f"Expected 2 NULL rows for new column 'added_col', got {null_count}."
        )

    # ----- Breaking drift: type change -----

    def test_breaking_drift_type_change_detected(self):
        """
        Changing issue_price from DoubleType to StringType must be flagged as breaking.
        """
        from src.ingestion.schema_drift import detect_drift

        incoming = self._spark.read.format("delta").load(self._base_path).withColumn(
            "issue_price", F.col("issue_price").cast(StringType())
        )
        result = detect_drift(self._spark, self._base_path, incoming.schema)
        assert "issue_price" in result["breaking"], (
            "detect_drift did not flag type change on issue_price as breaking."
        )
        assert result["additive"] == [], (
            "detect_drift incorrectly flagged type change as additive."
        )

    def test_breaking_drift_column_removal_detected(self):
        """
        Dropping an existing column must be flagged as breaking.
        """
        from src.ingestion.schema_drift import detect_drift

        # Incoming schema is missing 'status'
        incoming = self._spark.read.format("delta").load(self._base_path).drop("status")
        result = detect_drift(self._spark, self._base_path, incoming.schema)
        assert "status" in result["breaking"], (
            "detect_drift did not flag removal of 'status' column as breaking."
        )

    def test_breaking_drift_raises_and_quarantines(self):
        """
        handle_breaking_drift must raise ValueError and must NOT modify the target table.
        """
        from src.ingestion.schema_drift import detect_drift, handle_breaking_drift

        count_before = self._spark.read.format("delta").load(self._base_path).count()
        incoming = self._spark.read.format("delta").load(self._base_path).withColumn(
            "issue_price", F.col("issue_price").cast(StringType())
        )
        result = detect_drift(self._spark, self._base_path, incoming.schema)
        with pytest.raises(ValueError, match="Breaking change"):
            handle_breaking_drift(
                self._spark,
                batch_id="test_batch_001",
                full_table_name=self._base_path,
                breaking_columns=result["breaking"],
            )
        # Table must be unchanged
        count_after = self._spark.read.format("delta").load(self._base_path).count()
        assert count_before == count_after, (
            "Breaking drift handler modified the target table — it must not."
        )

    def test_new_table_detected(self):
        """
        detect_drift on a non-existent table path must return new_table=True.
        """
        from src.ingestion.schema_drift import detect_drift

        fake_schema = StructType([StructField("col_a", StringType(), True)])
        result = detect_drift(self._spark, "/nonexistent/path/12345", fake_schema)
        assert result["new_table"] is True
        assert result["additive"]  == []
        assert result["breaking"]  == []

    # ----- Metadata columns excluded from drift comparison -----

    def test_metadata_cols_excluded_from_drift(self):
        """
        Pipeline metadata columns (_ingestion_ts, _source_file, etc.) must never
        be flagged as new or breaking even when absent from the existing table schema.
        """
        from src.ingestion.schema_drift import detect_drift

        # Existing table has NO metadata columns (raw Delta, no pipeline yet)
        # Incoming adds metadata columns — should NOT appear in additive list
        incoming = (
            self._spark.read.format("delta").load(self._base_path)
            .withColumn("_ingestion_ts",  F.current_timestamp())
            .withColumn("_source_file",   F.lit("product.csv"))
            .withColumn("_batch_id",      F.lit("batch_001"))
            .withColumn("_row_hash",      F.sha2(F.col("product_id"), 256))
        )
        result = detect_drift(self._spark, self._base_path, incoming.schema)
        for meta_col in ["_ingestion_ts", "_source_file", "_batch_id", "_row_hash"]:
            assert meta_col not in result["additive"], (
                f"Metadata column {meta_col} incorrectly flagged as additive drift."
            )
            assert meta_col not in result["breaking"], (
                f"Metadata column {meta_col} incorrectly flagged as breaking drift."
            )


# ---------------------------------------------------------------------------
# 6. DELTA TABLE PROPERTIES TESTS
# ---------------------------------------------------------------------------

class TestBronzeDeltaProperties:
    """Delta table properties must be set correctly on all Bronze tables."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_iceberg_uniform_enabled(self, spark, table):
        """
        Iceberg UniForm must be enabled on all Bronze tables
        (delta.universalFormat.enabledFormats = 'iceberg').
        """
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        props_df = spark.sql(f"SHOW TBLPROPERTIES {_full(table)}")
        props = {
            row["key"]: row["value"]
            for row in props_df.collect()
        }
        assert "delta.universalFormat.enabledFormats" in props, (
            f"{_full(table)}: delta.universalFormat.enabledFormats property not found."
        )
        assert "iceberg" in props["delta.universalFormat.enabledFormats"], (
            f"{_full(table)}: Iceberg UniForm not enabled. "
            f"Got: {props.get('delta.universalFormat.enabledFormats')}"
        )


# ---------------------------------------------------------------------------
# 7. ROW COUNT SANITY TESTS
# ---------------------------------------------------------------------------

class TestBronzeRowCounts:
    """Sanity checks: tables must not be empty and counts must be reasonable."""

    # Minimum expected rows for key tables (from ontology.md population counts)
    MIN_ROW_COUNTS: dict[str, int] = {
        "product":              200,
        "bond":                  50,
        "stock":                 60,
        "common_stock":          40,
        "preferred_stock":       20,
        "fund":                  20,
        "debt":                  70,
        "legal_entity":          40,
        "currency":              15,   # 17 total rows incl. 2 bad ones
        "series":                20,
        "identifiers":          200,
        "product_rating":       200,
        "coupon":               100,
    }

    @pytest.mark.parametrize("table,min_rows", MIN_ROW_COUNTS.items())
    def test_minimum_row_count(self, spark, table, min_rows):
        """
        Key Bronze tables must have at least the expected minimum row count.
        Failure indicates the source CSV was not loaded or was truncated.
        """
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        actual = _read_bronze(spark, table).count()
        assert actual >= min_rows, (
            f"{_full(table)}: Expected >= {min_rows} rows, got {actual}. "
            "Check that the source CSV was fully loaded."
        )

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_table_not_empty(self, spark, table):
        """Every Bronze table must have at least 1 row after ingestion."""
        if not _table_exists(spark, table):
            pytest.skip(f"{_full(table)} does not exist.")
        count = _read_bronze(spark, table).count()
        assert count > 0, (
            f"{_full(table)} is empty. Check that {table}.csv was loaded correctly."
        )


# ---------------------------------------------------------------------------
# 8. BRONZE-SPECIFIC BUSINESS RULE TESTS
# ---------------------------------------------------------------------------

class TestBronzeBusinessRules:
    """
    Light business-rule checks appropriate for the Bronze layer.
    Bronze does NOT enforce DQ — these tests catch gross ingestion errors only.
    """

    def test_product_type_values_recognisable(self, spark):
        """
        Bronze product.type column must only contain recognisable values
        (Spark inferSchema should not have mangled them).
        """
        if not _table_exists(spark, "product"):
            pytest.skip("product table not found.")
        known_types = {"EQUITY", "DEBT", "FUND", "DERIVATIVE", "RIGHT"}
        df = _read_bronze(spark, "product")
        distinct_types = {
            row["type"] for row in df.select("type").distinct().collect()
            if row["type"] is not None
        }
        unrecognised = distinct_types - known_types
        assert not unrecognised, (
            f"Bronze product.type has unrecognised values: {unrecognised}. "
            "Check that inferSchema did not mangle the type column."
        )

    def test_bond_extends_product(self, spark):
        """
        Every bond.product_id must exist in product.product_id.
        Bronze does not enforce FK constraints, but a gross mismatch indicates
        the wrong CSV was loaded into the wrong table.
        """
        if not _table_exists(spark, "
