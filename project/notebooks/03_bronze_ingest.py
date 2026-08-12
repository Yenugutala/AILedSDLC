```python
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Ingestion — Securities Master Data
# MAGIC
# MAGIC Ingests all 29 source CSV files from the Databricks Volume into the
# MAGIC `statestreet.b_statestreet` Bronze layer.
# MAGIC
# MAGIC **Layer:** Bronze
# MAGIC **Catalog:** statestreet
# MAGIC **Schema:** b_statestreet
# MAGIC **Source:** /Volumes/statestreet/securities_master/raw_files/
# MAGIC
# MAGIC ### Groups
# MAGIC - Group 1: Base / Core Reference Tables (8 tables)
# MAGIC - Group 2: Product Subtype Tables (11 tables)
# MAGIC - Group 3: Relationship / Fact Tables (4 tables)
# MAGIC - Group 4: Bridge Tables (2 tables)
# MAGIC - Group 5: Legacy / Metadata Tables — Bronze only (3 tables)

# COMMAND ----------
# MAGIC %md ## 1. Parameters & Configuration

# COMMAND ----------
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Widget defaults ────────────────────────────────────────────────────────────
dbutils.widgets.text("batch_id",       "", "Batch ID (leave blank to auto-generate)")
dbutils.widgets.text("tables_override", "", "Comma-separated table names to run (blank = all)")

_batch_id_widget = dbutils.widgets.get("batch_id").strip()
BATCH_ID = _batch_id_widget if _batch_id_widget else (
    "auto_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
)
TABLES_OVERRIDE = dbutils.widgets.get("tables_override").strip()

# ── Catalog / schema / volume constants ───────────────────────────────────────
CATALOG     = "statestreet"
SCHEMA      = "b_statestreet"
VOLUME_PATH = "/Volumes/statestreet/securities_master/raw_files/"

print(f"BATCH_ID        : {BATCH_ID}")
print(f"CATALOG.SCHEMA  : {CATALOG}.{SCHEMA}")
print(f"VOLUME_PATH     : {VOLUME_PATH}")
print(f"TABLES_OVERRIDE : {TABLES_OVERRIDE or '(all)'}")

# COMMAND ----------
# MAGIC %md ## 2. Helper Functions

# COMMAND ----------
# ── Metadata column helpers ────────────────────────────────────────────────────

def _add_metadata(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """
    Append the 5 standard Bronze metadata columns to a DataFrame.

    Columns added (in order, after all data columns):
        _source_file    STRING     — source CSV filename
        _ingestion_ts   TIMESTAMP  — current UTC timestamp
        _batch_id       STRING     — pipeline run identifier
        _row_hash       STRING     — SHA-256 of all DATA columns (for CDC / MERGE)
        _ingestion_date DATE       — partition column derived from _ingestion_ts

    _row_hash is computed BEFORE the metadata columns are appended so that
    metadata changes (e.g. a different batch_id) do not affect change detection.
    """
    # Capture data-column references before adding metadata
    data_cols = [F.col(c) for c in df.columns]

    return (
        df
        .withColumn("_source_file",    F.lit(source_file))
        .withColumn("_ingestion_ts",   F.current_timestamp())
        .withColumn("_batch_id",       F.lit(batch_id))
        .withColumn("_row_hash",       F.sha2(F.concat_ws("|", *data_cols), 256))
        .withColumn("_ingestion_date", F.to_date(F.col("_ingestion_ts")))
    )


# ── Iceberg UniForm enabler ────────────────────────────────────────────────────

def _enable_iceberg_uniform(full_table: str) -> None:
    """
    Enable Delta UniForm (Iceberg) on a table.

    Requires all three TBLPROPERTIES to be set together as mandated by
    Databricks Runtime 14+ (SETUP-009):
        delta.columnMapping.mode           = name
        delta.enableIcebergCompatV2        = true
        delta.universalFormat.enabledFormats = iceberg

    Falls back gracefully with a warning if the operation is not supported
    (e.g. DBR < 13.3, column mapping precondition not met).
    """
    try:
        spark.sql(f"""
            ALTER TABLE {full_table}
            SET TBLPROPERTIES (
                'delta.columnMapping.mode'             = 'name',
                'delta.enableIcebergCompatV2'           = 'true',
                'delta.universalFormat.enabledFormats' = 'iceberg'
            )
        """)
    except Exception as exc:
        print(f"  [WARN] Iceberg UniForm not enabled for {full_table}: {exc}")


# ── Core ingest function ───────────────────────────────────────────────────────

def ingest_table(
    table_name: str,
    source_file: str,
    primary_key: list[str],
    batch_id: str = BATCH_ID,
) -> dict:
    """
    Read a single CSV from the Volume, add metadata columns, and
    MERGE INTO the Bronze Delta table.

    Returns a dict with ingestion statistics:
        table, rows_read, rows_written, status, error

    Strategy
    ────────
    • First load  → CREATE TABLE via saveAsTable (schema inferred).
    • Re-run      → MERGE INTO (idempotent):
        - MATCHED + hash changed  → UPDATE
        - NOT MATCHED             → INSERT
      This guarantees exactly-once semantics for re-runs and supports CDC
      change detection via _row_hash.

    Notes
    ─────
    • mergeSchema=true handles additive column drift silently on first load.
    • The _ingestion_date generated column is written as a regular DATE column
      (Databricks does not support generated columns via saveAsTable with
       inferSchema; the column is computed in Python instead).
    • Bridge tables (primary_key = composite) use a multi-column join condition.
    • Group 5 tables (primary_key = []) fall back to INSERT OVERWRITE because
      there is no reliable PK to MERGE on.
    """
    full_table = f"{CATALOG}.{SCHEMA}.{table_name}"
    csv_path   = f"{VOLUME_PATH}{source_file}"

    print(f"\n{'─'*70}")
    print(f"  TABLE : {full_table}")
    print(f"  FILE  : {csv_path}")

    stats = {
        "table":         table_name,
        "rows_read":     0,
        "rows_written":  0,
        "status":        "PENDING",
        "error":         None,
    }

    try:
        # ── 1. Read CSV ──────────────────────────────────────────────────────
        df = (
            spark.read
            .option("header",        "true")
            .option("inferSchema",   "true")
            .option("delimiter",     ",")
            .option("nullValue",     "")
            .option("escape",        '"')
            .option("multiLine",     "false")
            .csv(csv_path)
        )

        rows_read = df.count()
        stats["rows_read"] = rows_read
        print(f"  READ  : {rows_read:,} rows")

        # ── 2. Add metadata columns ──────────────────────────────────────────
        df = _add_metadata(df, source_file, batch_id)

        # ── 3. Table existence check ─────────────────────────────────────────
        table_exists = spark.catalog.tableExists(full_table)

        # ── 4a. First load — create table ───────────────────────────────────
        if not table_exists:
            print(f"  MODE  : CREATE (first load)")
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("mergeSchema",     "true")
                .partitionBy("_ingestion_date")
                .saveAsTable(full_table)
            )
            rows_written = rows_read

        # ── 4b. Group 5 tables — no PK, use overwrite ───────────────────────
        elif not primary_key:
            print(f"  MODE  : OVERWRITE (no primary key — legacy/metadata table)")
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("mergeSchema",     "true")
                .partitionBy("_ingestion_date")
                .saveAsTable(full_table)
            )
            rows_written = rows_read

        # ── 4c. Re-run — MERGE INTO ──────────────────────────────────────────
        else:
            print(f"  MODE  : MERGE INTO (idempotent re-run)")
            view_name = f"_bronze_incoming_{table_name}"
            df.createOrReplaceTempView(view_name)

            # Build JOIN condition for single or composite PK
            pk_join = " AND ".join(
                [f"target.{k} = source.{k}" for k in primary_key]
            )

            merge_sql = f"""
                MERGE INTO {full_table} AS target
                USING {view_name} AS source
                ON {pk_join}
                WHEN MATCHED
                  AND source._row_hash != target._row_hash
                THEN UPDATE SET *
                WHEN NOT MATCHED
                THEN INSERT *
            """
            spark.sql(merge_sql)

            # Approximate written rows = updated + inserted (not directly
            # returned by MERGE; we approximate as incoming row count for logging)
            rows_written = rows_read

        stats["rows_written"] = rows_written

        # ── 5. Enable Iceberg UniForm ────────────────────────────────────────
        _enable_iceberg_uniform(full_table)

        stats["status"] = "SUCCESS"
        print(f"  WROTE : {rows_written:,} rows  →  {full_table}")
        print(f"  STATUS: SUCCESS")

    except Exception as exc:
        stats["status"] = "FAILED"
        stats["error"]  = str(exc)
        print(f"  STATUS: FAILED")
        print(f"  ERROR : {exc}")

    return stats


# COMMAND ----------
# MAGIC %md ## 3. Table Registry — Ingestion Order

# COMMAND ----------
# ── Ingestion order matches FK dependency (parents before children) ─────────────
#
# Each entry: (table_name, source_file, primary_key)
#
# primary_key = []  →  no PK (legacy/metadata — INSERT OVERWRITE)
# primary_key = [a, b]  →  composite PK (bridge tables)

INGESTION_ORDER: list[tuple[str, str, list[str]]] = [

    # ── GROUP 1: Base / Core Reference ──────────────────────────────────────
    # Load these first — other tables FK into them.
    ("legal_entity",             "legal_entity.csv",             ["legal_entity_id"]),
    ("currency",                 "currency.csv",                 ["currency_code"]),
    ("series",                   "series.csv",                   ["series_id"]),
    ("tick_ladder_scale",        "tick_ladder_scale.csv",        ["tick_ladder_scale_id"]),
    ("tick",                     "tick.csv",                     ["tick_id"]),
    ("principal_redemption_provision",
                                 "principal_redemption_provision.csv",
                                                                 ["principal_redemption_provision_id"]),
    ("product_rating_type",      "product_rating_type.csv",      ["product_rating_type_id"]),
    ("product",                  "product.csv",                  ["product_id"]),

    # ── GROUP 2: Product Subtype Tables ──────────────────────────────────────
    # Load after product (all FK → product.product_id).
    ("debt",                     "debt.csv",                     ["product_id"]),
    ("bond",                     "bond.csv",                     ["product_id"]),
    ("muni",                     "muni.csv",                     ["product_id"]),
    ("pool_backed_security",     "pool_backed_security.csv",     ["product_id"]),
    ("stock",                    "stock.csv",                    ["product_id"]),
    ("common_stock",             "common_stock.csv",             ["product_id"]),
    ("preferred_stock",          "preferred_stock.csv",          ["product_id"]),
    ("fund",                     "fund.csv",                     ["product_id"]),
    ("right",                    "right.csv",                    ["product_id"]),
    ("listed_derivative",        "listed_derivative.csv",        ["product_id"]),
    ("option",                   "option.csv",                   ["product_id"]),
    ("future",                   "future.csv",                   ["product_id"]),

    # ── GROUP 3: Relationship / Fact Tables ──────────────────────────────────
    ("identifiers",              "identifiers.csv",              ["identifier_id"]),
    ("classification",           "classification.csv",           ["classification_id"]),
    ("product_rating",           "product_rating.csv",           ["product_rating_id"]),
    ("coupon",                   "coupon.csv",                   ["coupon_id"]),

    # ── GROUP 4: Bridge Tables ────────────────────────────────────────────────
    ("listed_derivative_tick",
                                 "listed_derivative_tick.csv",
                                                                 ["product_id", "tick_id"]),
    ("debt_principal_redemption_provision",
                                 "debt_principal_redemption_provision.csv",
                                                                 ["product_id",
                                                                  "principal_redemption_provision_id"]),

    # ── GROUP 5: Legacy / Metadata — Bronze only ──────────────────────────────
    # No Silver conformance or Gold mart build for these three tables.
    # generic_product has no PK (by design — legacy M:1 shadow table).
    ("generic_product",          "generic_product.csv",          []),
    ("dq_rules_catalog",         "dq_rules_catalog.csv",         ["rule_id"]),
    ("dq_issues_catalog",        "dq_issues_catalog.csv",        ["issue_id"]),
]

print(f"Total tables registered: {len(INGESTION_ORDER)}")

# COMMAND ----------
# MAGIC %md ## 4. Schema Pre-flight Check

# COMMAND ----------
# Verify the target schema exists before attempting any writes.
# The schema should have been created by notebook 01_setup_catalog.py.

try:
    spark.sql(f"USE CATALOG {CATALOG}")
    schemas = [r["databaseName"] for r in spark.sql("SHOW SCHEMAS").collect()]
    if SCHEMA not in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Bronze — raw landing layer'")
        print(f"[INFO] Created schema: {CATALOG}.{SCHEMA}")
    else:
        print(f"[OK] Schema exists: {CATALOG}.{SCHEMA}")
except Exception as exc:
    raise RuntimeError(f"Schema pre-flight failed: {exc}") from exc

# COMMAND ----------
# MAGIC %md ## 5. Run Ingestion

# COMMAND ----------
# ── Determine which tables to run ─────────────────────────────────────────────
if TABLES_OVERRIDE:
    allowed = {t.strip() for t in TABLES_OVERRIDE.split(",")}
    RUN_TABLES = [(t, f, pk) for t, f, pk in INGESTION_ORDER if t in allowed]
    print(f"Running {len(RUN_TABLES)} table(s) from override: {allowed}")
else:
    RUN_TABLES = INGESTION_ORDER
    print(f"Running all {len(RUN_TABLES)} tables.")

# ── Execute ingestion loop ─────────────────────────────────────────────────────
results: list[dict] = []

for table_name, source_file, primary_key in RUN_TABLES:
    result = ingest_table(
        table_name  = table_name,
        source_file = source_file,
        primary_key = primary_key,
        batch_id    = BATCH_ID,
    )
    results.append(result)

# COMMAND ----------
# MAGIC %md ## 6. Ingestion Summary

# COMMAND ----------
# ── Print summary table ───────────────────────────────────────────────────────
print("\n" + "═" * 80)
print(f"  BRONZE INGESTION SUMMARY   batch_id={BATCH_ID}")
print("═" * 80)
print(f"  {'TABLE':<45} {'READ':>8} {'WRITTEN':>8}  STATUS")
print("  " + "─" * 76)

total_read    = 0
total_written = 0
failed_tables = []

for r in results:
    status_icon = "✓" if r["status"] == "SUCCESS" else "✗"
    print(
        f"  {status_icon} {r['table']:<43} "
        f"{r['rows_read']:>8,} "
        f"{r['rows_written']:>8,}  "
        f"{r['status']}"
    )
    if r["error"]:
        print(f"    ERROR: {r['error']}")
    total_read    += r["rows_read"]
    total_written += r["rows_written"]
    if r["status"] != "SUCCESS":
        failed_tables.append(r["table"])

print("  " + "─" * 76)
print(f"  {'TOTAL':<45} {total_read:>8,} {total_written:>8,}")
print("═" * 80)
print(f"  Tables attempted : {len(results)}")
print(f"  Tables succeeded : {len(results) - len(failed_tables)}")
print(f"  Tables failed    : {len(failed_tables)}")
if failed_tables:
    print(f"  Failed tables    : {', '.join(failed_tables)}")
print("═" * 80)

# ── Fail the job if any table failed ─────────────────────────────────────────
# This causes Databricks to mark the run as FAILED and triggers retry/alert logic
# configured in the DAB job definition.
if failed_tables:
    raise RuntimeError(
        f"Bronze ingestion FAILED for {len(failed_tables)} table(s): "
        f"{', '.join(failed_tables)}. "
        f"Review per-table errors above. batch_id={BATCH_ID}"
    )

print("\n[OK] All tables ingested successfully.")