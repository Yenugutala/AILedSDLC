"""
Bronze Layer Tests — Securities Master Data Lakehouse
Tests raw CSV ingestion: schema, metadata columns, idempotency, schema drift.
All tests use a local Spark session — no Databricks connection required.
"""

import pytest
import os
import tempfile
import shutil
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, TimestampType, BooleanType, LongType, DecimalType
)


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Local Delta-enabled Spark session. Shared across all tests in the session."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("sml-bronze-tests")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")   # small local shuffle
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def delta_warehouse(tmp_path_factory):
    """
    Temporary directory that acts as a Delta warehouse for all session tests.
    Cleaned up automatically by pytest after the session ends.
    """
    return str(tmp_path_factory.mktemp("delta_warehouse"))


# ---------------------------------------------------------------------------
# Schema fixtures  (mirror bronze/tables.yaml)
# ---------------------------------------------------------------------------

PRODUCT_SCHEMA = StructType([
    StructField("product_id",               StringType(),        nullable=False),
    StructField("id_type",                  StringType(),        nullable=True),
    StructField("type",                     StringType(),        nullable=True),
    StructField("sub_type",                 StringType(),        nullable=True),
    StructField("status",                   StringType(),        nullable=True),
    StructField("settlement_type",          StringType(),        nullable=True),
    StructField("description",              StringType(),        nullable=True),
    StructField("issue_date",               DateType(),          nullable=True),
    StructField("issue_price",              DoubleType(),        nullable=True),
    StructField("current_face_value",       DoubleType(),        nullable=True),
    StructField("issuer_legal_entity_id",   StringType(),        nullable=True),
    StructField("tick_ladder_scale_id",     StringType(),        nullable=True),
])

BOND_SCHEMA = StructType([
    StructField("product_id",           StringType(),  nullable=False),
    StructField("coupon_type",          StringType(),  nullable=True),
    StructField("maturity_date",        DateType(),    nullable=True),
    StructField("face_currency_code",   StringType(),  nullable=True),
    StructField("day_count_convention", StringType(),  nullable=True),
])

LEGAL_ENTITY_SCHEMA = StructType([
    StructField("legal_entity_id",  StringType(),  nullable=False),
    StructField("legal_name",       StringType(),  nullable=True),
    StructField("country",          StringType(),  nullable=True),
    StructField("entity_type",      StringType(),  nullable=True),
])

IDENTIFIERS_SCHEMA = StructType([
    StructField("identifier_id",    StringType(),  nullable=False),
    StructField("product_id",       StringType(),  nullable=True),
    StructField("id_type",          StringType(),  nullable=True),
    StructField("identifier_value", StringType(),  nullable=True),
])

COUPON_SCHEMA = StructType([
    StructField("coupon_id",    StringType(),  nullable=False),
    StructField("product_id",   StringType(),  nullable=True),
    StructField("coupon_rate",  DoubleType(),  nullable=True),
    StructField("payment_date", DateType(),    nullable=True),
    StructField("coupon_type",  StringType(),  nullable=True),
    StructField("frequency",    StringType(),  nullable=True),
])

PRODUCT_RATING_SCHEMA = StructType([
    StructField("product_rating_id",       StringType(),  nullable=False),
    StructField("product_id",              StringType(),  nullable=True),
    StructField("product_rating_type_id",  StringType(),  nullable=True),
    StructField("rating_value",            StringType(),  nullable=True),
    StructField("effective_from_date",     DateType(),    nullable=True),
    StructField("rating_agency",           StringType(),  nullable=True),
])

CURRENCY_SCHEMA = StructType([
    StructField("currency_code",  StringType(),  nullable=False),
    StructField("currency_name",  StringType(),  nullable=True),
])

STOCK_SCHEMA = StructType([
    StructField("product_id",  StringType(),  nullable=False),
    StructField("series_id",   StringType(),  nullable=True),
])

COMMON_STOCK_SCHEMA = StructType([
    StructField("product_id",     StringType(),   nullable=False),
    StructField("voting_rights",  BooleanType(),  nullable=True),
])

PREFERRED_STOCK_SCHEMA = StructType([
    StructField("product_id",     StringType(),  nullable=False),
    StructField("dividend_right", StringType(),  nullable=True),
])

FUND_SCHEMA = StructType([
    StructField("product_id",       StringType(),  nullable=False),
    StructField("endness_type",     StringType(),  nullable=True),
    StructField("mutual_fund_type", StringType(),  nullable=True),
])

DEBT_SCHEMA = StructType([
    StructField("product_id",            StringType(),  nullable=False),
    StructField("total_amount_issued",   DoubleType(),  nullable=True),
])

MUNI_SCHEMA = StructType([
    StructField("product_id",  StringType(),   nullable=False),
    StructField("tax_exempt",  BooleanType(),  nullable=True),
    StructField("state",       StringType(),   nullable=True),
    StructField("purpose",     StringType(),   nullable=True),
])

POOL_BACKED_SECURITY_SCHEMA = StructType([
    StructField("product_id",   StringType(),  nullable=False),
    StructField("pool_type",    StringType(),  nullable=True),
    StructField("originator",   StringType(),  nullable=True),
])

RIGHT_SCHEMA = StructType([
    StructField("product_id",  StringType(),  nullable=False),
])

SERIES_SCHEMA = StructType([
    StructField("series_id",   StringType(),  nullable=False),
    StructField("series_name", StringType(),  nullable=True),
])

LISTED_DERIVATIVE_SCHEMA = StructType([
    StructField("product_id",            StringType(),  nullable=False),
    StructField("series_id",             StringType(),  nullable=True),
    StructField("underlying_product_id", StringType(),  nullable=True),
])

OPTION_SCHEMA = StructType([
    StructField("product_id",      StringType(),  nullable=False),
    StructField("option_type",     StringType(),  nullable=True),
    StructField("exercise_style",  StringType(),  nullable=True),
    StructField("strike_price",    DoubleType(),  nullable=True),
    StructField("expiry_date",     DateType(),    nullable=True),
])

FUTURE_SCHEMA = StructType([
    StructField("product_id",        StringType(),  nullable=False),
    StructField("delivery_date",     DateType(),    nullable=True),
    StructField("valuation_method",  StringType(),  nullable=True),
])

TICK_LADDER_SCALE_SCHEMA = StructType([
    StructField("tick_ladder_scale_id",  StringType(),  nullable=False),
    StructField("scale_name",            StringType(),  nullable=True),
])

TICK_SCHEMA = StructType([
    StructField("tick_id",              StringType(),  nullable=False),
    StructField("tick_ladder_scale_id", StringType(),  nullable=True),
    StructField("tick_size",            DoubleType(),  nullable=True),
])

CLASSIFICATION_SCHEMA = StructType([
    StructField("classification_id",  StringType(),  nullable=False),
    StructField("product_id",         StringType(),  nullable=True),
    StructField("classification_type",StringType(),  nullable=True),
    StructField("classification_code",StringType(),  nullable=True),
])

PRODUCT_RATING_TYPE_SCHEMA = StructType([
    StructField("product_rating_type_id",  StringType(),  nullable=False),
    StructField("rating_type_code",        StringType(),  nullable=True),
    StructField("rating_scale",            StringType(),  nullable=True),
])

PRINCIPAL_REDEMPTION_PROVISION_SCHEMA = StructType([
    StructField("provision_id",    StringType(),  nullable=False),
    StructField("provision_type",  StringType(),  nullable=True),
])

LISTED_DERIVATIVE_TICK_SCHEMA = StructType([
    StructField("product_id",  StringType(),  nullable=False),
    StructField("tick_id",     StringType(),  nullable=False),
])

DEBT_PRINCIPAL_REDEMPTION_PROVISION_SCHEMA = StructType([
    StructField("product_id",    StringType(),  nullable=False),
    StructField("provision_id",  StringType(),  nullable=False),
])

GENERIC_PRODUCT_SCHEMA = StructType([
    StructField("generic_product_id",  StringType(),  nullable=False),
    StructField("product_id",          StringType(),  nullable=True),
    StructField("generic_field_1",     StringType(),  nullable=True),
])

DQ_RULES_CATALOG_SCHEMA = StructType([
    StructField("rule_id",       StringType(),  nullable=False),
    StructField("table_name",    StringType(),  nullable=True),
    StructField("rule_type",     StringType(),  nullable=True),
    StructField("description",   StringType(),  nullable=True),
])

DQ_ISSUES_CATALOG_SCHEMA = StructType([
    StructField("issue_id",      StringType(),  nullable=False),
    StructField("rule_id",       StringType(),  nullable=True),
    StructField("table_name",    StringType(),  nullable=True),
    StructField("description",   StringType(),  nullable=True),
])

# Map of all 29 source tables to their expected schemas
ALL_TABLE_SCHEMAS = {
    "product":                              PRODUCT_SCHEMA,
    "generic_product":                      GENERIC_PRODUCT_SCHEMA,
    "legal_entity":                         LEGAL_ENTITY_SCHEMA,
    "tick_ladder_scale":                    TICK_LADDER_SCALE_SCHEMA,
    "tick":                                 TICK_SCHEMA,
    "product_rating":                       PRODUCT_RATING_SCHEMA,
    "product_rating_type":                  PRODUCT_RATING_TYPE_SCHEMA,
    "classification":                       CLASSIFICATION_SCHEMA,
    "identifiers":                          IDENTIFIERS_SCHEMA,
    "fund":                                 FUND_SCHEMA,
    "debt":                                 DEBT_SCHEMA,
    "bond":                                 BOND_SCHEMA,
    "muni":                                 MUNI_SCHEMA,
    "pool_backed_security":                 POOL_BACKED_SECURITY_SCHEMA,
    "right":                                RIGHT_SCHEMA,
    "series":                               SERIES_SCHEMA,
    "listed_derivative":                    LISTED_DERIVATIVE_SCHEMA,
    "option":                               OPTION_SCHEMA,
    "future":                               FUTURE_SCHEMA,
    "stock":                                STOCK_SCHEMA,
    "common_stock":                         COMMON_STOCK_SCHEMA,
    "preferred_stock":                      PREFERRED_STOCK_SCHEMA,
    "coupon":                               COUPON_SCHEMA,
    "principal_redemption_provision":       PRINCIPAL_REDEMPTION_PROVISION_SCHEMA,
    "currency":                             CURRENCY_SCHEMA,
    "listed_derivative_tick":               LISTED_DERIVATIVE_TICK_SCHEMA,
    "debt_principal_redemption_provision":  DEBT_PRINCIPAL_REDEMPTION_PROVISION_SCHEMA,
    "dq_rules_catalog":                     DQ_RULES_CATALOG_SCHEMA,
    "dq_issues_catalog":                    DQ_ISSUES_CATALOG_SCHEMA,
}

METADATA_COLUMNS = ["_source_file", "_ingestion_ts", "_batch_id", "_row_hash"]


# ---------------------------------------------------------------------------
# Helpers — build minimal sample DataFrames and write Delta tables
# ---------------------------------------------------------------------------

def _make_product_rows(spark):
    """Three representative product rows covering EQUITY, DEBT, FUND."""
    return spark.createDataFrame(
        [
            ("PROD001", "CUSIP",  "EQUITY", "COMMON_STOCK", "ACTIVE",   None, "AAPL Inc",           None, None,  None,  "LE001", None),
            ("PROD002", "ISIN",   "DEBT",   "BOND",         "ACTIVE",   None, "US 5yr Treasury",    None, None,  100.0, "LE002", None),
            ("PROD003", "TICKER", "FUND",   "FUND",         "INACTIVE", None, "Vanguard S&P ETF",   None, None,  None,  "LE003", None),
        ],
        schema=PRODUCT_SCHEMA,
    )


def _add_metadata(df, source_file: str = "product.csv",
                  source_type: str = "volume", batch_id: str = "batch_001"):
    """Replicate what Bronze notebook does: add 4 metadata columns."""
    data_cols = [F.col(c) for c in df.columns]
    return (
        df
        .withColumn("_source_file",  F.lit(source_file))
        .withColumn("_source_type",  F.lit(source_type))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_batch_id",     F.lit(batch_id))
        .withColumn("_row_hash",     F.sha2(F.concat_ws("|", *data_cols), 256))
    )


def _write_delta(df, path: str, mode: str = "overwrite"):
    """Write DataFrame as Delta at the given path."""
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").save(path)


def _merge_delta(spark, df, path: str, pk_col: str):
    """MERGE INTO Delta table — simulates Bronze idempotent load."""
    df.createOrReplaceTempView("_incoming_tmp")
    spark.sql(f"""
        MERGE INTO delta.`{path}` AS target
        USING _incoming_tmp AS source
        ON target.{pk_col} = source.{pk_col}
        WHEN MATCHED AND source._row_hash != target._row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# ===========================================================================
# 1. SCHEMA TESTS — all 29 tables
# ===========================================================================

class TestBronzeSchema:
    """
    For every source table, verify that after adding metadata columns the
    resulting DataFrame contains exactly the expected data columns plus the
    4 standard metadata columns.
    """

    @pytest.mark.parametrize("table_name,schema", list(ALL_TABLE_SCHEMAS.items()))
    def test_data_columns_present(self, spark, table_name, schema):
        """All data columns from the spec schema are present after ingestion."""
        raw_df = spark.createDataFrame([], schema)
        result = _add_metadata(raw_df, source_file=f"{table_name}.csv")
        expected_data_cols = {f.name for f in schema.fields}
        actual_cols = set(result.columns)
        missing = expected_data_cols - actual_cols
        assert not missing, (
            f"[{table_name}] Missing data columns after ingestion: {sorted(missing)}"
        )

    @pytest.mark.parametrize("table_name,schema", list(ALL_TABLE_SCHEMAS.items()))
    def test_metadata_columns_added(self, spark, table_name, schema):
        """All 4 metadata columns are appended by the Bronze pipeline."""
        raw_df = spark.createDataFrame([], schema)
        result = _add_metadata(raw_df, source_file=f"{table_name}.csv")
        actual_cols = set(result.columns)
        missing_meta = set(METADATA_COLUMNS) - actual_cols
        assert not missing_meta, (
            f"[{table_name}] Missing metadata columns: {sorted(missing_meta)}"
        )

    @pytest.mark.parametrize("table_name,schema", list(ALL_TABLE_SCHEMAS.items()))
    def test_no_extra_metadata_columns(self, spark, table_name, schema):
        """
        The Bronze layer adds EXACTLY _source_file, _ingestion_ts, _batch_id,
        _row_hash (and _source_type internally). No other underscore-prefixed
        columns are silently injected.
        """
        raw_df = spark.createDataFrame([], schema)
        result = _add_metadata(raw_df, source_file=f"{table_name}.csv")
        all_meta = {c for c in result.columns if c.startswith("_")}
        allowed_meta = {"_source_file", "_source_type", "_ingestion_ts",
                        "_batch_id", "_row_hash"}
        unexpected = all_meta - allowed_meta
        assert not unexpected, (
            f"[{table_name}] Unexpected metadata columns: {sorted(unexpected)}"
        )

    def test_product_primary_key_column_exists(self, spark):
        """product_id is always present and is the first column in product."""
        raw_df = spark.createDataFrame([], PRODUCT_SCHEMA)
        result = _add_metadata(raw_df)
        assert "product_id" in result.columns, "product_id column is missing from product"

    def test_bond_has_product_id_fk(self, spark):
        """bond table carries product_id (FK to product)."""
        raw_df = spark.createDataFrame([], BOND_SCHEMA)
        result = _add_metadata(raw_df, source_file="bond.csv")
        assert "product_id" in result.columns

    def test_coupon_has_product_id_fk(self, spark):
        """coupon table carries product_id (FK to bond)."""
        raw_df = spark.createDataFrame([], COUPON_SCHEMA)
        result = _add_metadata(raw_df, source_file="coupon.csv")
        assert "product_id" in result.columns


# ===========================================================================
# 2. METADATA COLUMN TESTS
# ===========================================================================

class TestBronzeMetadataColumns:
    """Verify content and behaviour of the 4 pipeline metadata columns."""

    @pytest.fixture(autouse=True)
    def product_df_with_meta(self, spark):
        """Product DataFrame with metadata columns — reused by every test in the class."""
        raw_df = _make_product_rows(spark)
        self.df = _add_metadata(raw_df, source_file="product.csv",
                                source_type="volume", batch_id="test_batch_001")

    def test_source_file_value(self):
        """_source_file must equal the literal CSV file name passed at ingest time."""
        values = {r["_source_file"] for r in self.df.collect()}
        assert values == {"product.csv"}, f"Unexpected _source_file values: {values}"

    def test_batch_id_value(self):
        """_batch_id must equal the batch identifier passed at ingest time."""
        values = {r["_batch_id"] for r in self.df.collect()}
        assert values == {"test_batch_001"}, f"Unexpected _batch_id values: {values}"

    def test_ingestion_ts_is_not_null(self):
        """_ingestion_ts must never be NULL."""
        null_count = self.df.filter(F.col("_ingestion_ts").isNull()).count()
        assert null_count == 0, f"{null_count} rows have NULL _ingestion_ts"

    def test_row_hash_is_not_null(self):
        """_row_hash must never be NULL (even when data columns are NULL)."""
        null_count = self.df.filter(F.col("_row_hash").isNull()).count()
        assert null_count == 0, f"{null_count} rows have NULL _row_hash"

    def test_row_hash_length_is_64_chars(self):
        """SHA-256 hex digest is exactly 64 characters."""
        bad = self.df.filter(F.length("_row_hash") != 64).count()
        assert bad == 0, f"{bad} rows have _row_hash with wrong length"

    def test_row_hash_unique_per_distinct_row(self):
        """Each distinct data row produces a distinct _row_hash."""
        total     = self.df.count()
        distinct  = self.df.select("_row_hash").distinct().count()
        assert total == distinct, (
            f"Hash collision detected: {total} rows but only {distinct} distinct hashes"
        )

    def test_row_hash_is_deterministic(self, spark):
        """
        Re-computing metadata on the SAME raw data produces the SAME hashes.
        Crucial for MERGE INTO change detection in subsequent pipeline runs.
        """
        raw_df = _make_product_rows(spark)
        df1 = _add_metadata(raw_df, source_file="product.csv", batch_id="run_1")
        df2 = _add_metadata(raw_df, source_file="product.csv", batch_id="run_2")

        hashes1 = {r["product_id"]: r["_row_hash"] for r in df1.collect()}
        hashes2 = {r["product_id"]: r["_row_hash"] for r in df2.collect()}

        assert hashes1 == hashes2, (
            "_row_hash changed between runs for identical data — MERGE INTO will "
            "incorrectly mark all rows as changed"
        )

    def test_row_hash_changes_when_data_changes(self, spark):
        """Changing a data column must produce a different _row_hash."""
        raw_df = _make_product_rows(spark)
        df_before = _add_metadata(raw_df, source_file="product.csv")

        modified = raw_df.withColumn(
            "description",
            F.when(F.col("product_id") == "PROD001", F.lit("Modified Name"))
             .otherwise(F.col("description"))
        )
        df_after = _add_metadata(modified, source_file="product.csv")

        hash_before = {r["product_id"]: r["_row_hash"] for r in df_before.collect()}
        hash_after  = {r["product_id"]: r["_row_hash"] for r in df_after.collect()}

        assert hash_before["PROD001"] != hash_after["PROD001"], (
            "_row_hash did NOT change after data modification — CDC will miss updates"
        )
        # Other rows must be unchanged
        assert hash_before["PROD002"] == hash_after["PROD002"]
        assert hash_before["PROD003"] == hash_after["PROD003"]

    def test_metadata_columns_not_included_in_hash(self, spark):
        """
        _batch_id should not affect _row_hash (only DATA columns are hashed).
        Changing batch_id between runs must NOT change the hash.
        """
        raw_df = _make_product_rows(spark)
        df1 = _add_metadata(raw_df, source_file="product.csv", batch_id="batch_A")
        df2 = _add_metadata(raw_df, source_file="product.csv", batch_id="batch_B")

        hashes1 = {r["product_id"]: r["_row_hash"] for r in df1.collect()}
        hashes2 = {r["product_id"]: r["_row_hash"] for r in df2.collect()}

        assert hashes1 == hashes2, (
            "_row_hash changed when only _batch_id changed — metadata columns "
            "must be excluded from the hash computation"
        )

    @pytest.mark.parametrize("table_name,pk_col,schema", [
        ("bond",         "product_id", BOND_SCHEMA),
        ("legal_entity", "legal_entity_id", LEGAL_ENTITY_SCHEMA),
        ("coupon",       "coupon_id",  COUPON_SCHEMA),
        ("identifiers",  "identifier_id", IDENTIFIERS_SCHEMA),
    ])
    def test_metadata_added_for_other_tables(self, spark, table_name, pk_col, schema):
        """Metadata columns are correctly added to all key Bronze tables."""
        raw_df = spark.createDataFrame([], schema)
        result = _add_metadata(raw_df, source_file=f"{table_name}.csv")
        assert set(METADATA_COLUMNS).issubset(set(result.columns)), (
            f"[{table_name}] One or more metadata columns are missing"
        )


# ===========================================================================
# 3. IDEMPOTENCY TESTS
# ===========================================================================

class TestBronzeIdempotency:
    """
    Verify that running Bronze ingestion twice (with identical source data)
    produces the same row count — i.e., MERGE INTO does not create duplicates.
    """

    def test_product_no_duplicate_on_rerun(self, spark, delta_warehouse):
        """
        After first load + second load of the same data, row count must equal
        the original source row count (not double it).
        """
        path = f"{delta_warehouse}/idempotency/product"
        raw_df = _make_product_rows(spark)

        # First load
        df1 = _add_metadata(raw_df, batch_id="run_001")
        _write_delta(df1, path, mode="overwrite")
        count_after_first_load = spark.read.format("delta").load(path).count()

        # Second load — same source data, different batch_id
        df2 = _add_metadata(raw_df, batch_id="run_002")
        _merge_delta(spark, df2, path, pk_col="product_id")
        count_after_second_load = spark.read.format("delta").load(path).count()

        assert count_after_first_load == count_after_second_load, (
            f"Idempotency failure: first load={count_after_first_load} rows, "
            f"second load={count_after_second_load} rows — MERGE INTO created duplicates"
        )

    def test_product_updated_row_not_duplicated(self, spark, delta_warehouse):
        """
        When ONE row changes between runs, MERGE INTO must UPDATE it in place —
        not INSERT a second copy — so the total count remains unchanged.
        """
        path = f"{delta_warehouse}/idempotency/product_update"
        raw_df = _make_product_rows(spark)

        df1 = _add_metadata(raw_df, batch_id="run_001")
        _write_delta(df1, path, mode="overwrite")
        count_initial = spark.read.format("delta").load(path).count()

        # Modify PROD001's description
        modified = raw_df.withColumn(
            "description",
            F.when(F.col("product_id") == "PROD001", F.lit("Updated Name"))
             .otherwise(F.col("description"))
        )
        df2 = _add_metadata(modified, batch_id="run_002")
        _merge_delta(spark, df2, path, pk_col="product_id")
        count_after_update = spark.read.format("delta").load(path).count()

        assert count_initial == count_after_update, (
            "Row count changed after an UPDATE — MERGE INTO inserted a duplicate instead"
        )

        # Also verify the value was updated
        updated_desc = (
            spark.read.format("delta").load(path)
            .filter(F.col("product_id") == "PROD001")
            .select("description")
            .first()["description"]
        )
        assert updated_desc == "Updated Name", (
            "Description was NOT updated after MERGE INTO — update branch not applied"
        )

    def test_new_row_added_on_rerun(self, spark, delta_warehouse):
        """
        When a new row appears in the source, it must be INSERTed — not skipped.
        """
        path = f"{delta_warehouse}/idempotency/product_insert"
        raw_df = _make_product_rows(spark)   # 3 rows

        df1 = _add_metadata(raw_df, batch_id="run_001")
        _write_delta(df1, path, mode="overwrite")

        # Add a fourth row
        new_row = spark.createDataFrame(
            [("PROD004", "SEDOL", "EQUITY", "PREFERRED_STOCK",
              "ACTIVE", None, "New Security", None, None, None, "LE004", None)],
            schema=PRODUCT_SCHEMA,
        )
        extended = raw_df.union(new_row)
        df2 = _add_metadata(extended, batch_id="run_002")
        _merge_delta(spark, df2, path, pk_col="product_id")

        final_count = spark.read.format("delta").load(path).count()
        assert final_count == 4, (
            f"Expected 4 rows after INSERT of new row, got {final_count}"
        )

    @pytest.mark.parametrize("table_name,pk_col,schema,sample_rows", [
        (
            "legal_entity",
            "legal_entity_id",
            LEGAL_ENTITY_SCHEMA,
            [("LE001", "Goldman Sachs", "US", "BANK"),
             ("LE002", "JP Morgan",     "US", "BANK")],
        ),
        (
            "currency",
            "currency_code",
            CURRENCY_SCHEMA,
            [("USD", "US Dollar"),
             ("EUR", "Euro"),
             ("GBP", "British Pound")],
        ),
    ])
    def test_idempotency_for_other_tables(self, spark, delta_warehouse,
                                          table_name, pk_col, schema, sample_rows):
        """MERGE INTO idempotency holds for legal_entity and currency tables."""
        path = f"{delta_warehouse}/idempotency/{table_name}"
        raw_df = spark.createDataFrame(sample_rows, schema)

        df1 = _add_metadata(raw_df, source_file=f"{table_name}.csv", batch_id="run_001")
        _write_delta(df1, path, mode="overwrite")
        count1 = spark.read.format("delta").load(path).count()

        df2 = _add_metadata(raw_df, source_file=f"{table_name}.csv", batch_id="run_002")
        _merge_delta(spark, df2, path, pk_
