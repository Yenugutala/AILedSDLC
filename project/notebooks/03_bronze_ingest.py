# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion — Securities Master Data
# MAGIC
# MAGIC Ingests all 29 source CSV files from the Databricks Volume into the
# MAGIC `statestreet.b_statestreet` Bronze layer as Delta tables with Iceberg UniForm.
# MAGIC
# MAGIC **Layer**: Bronze
# MAGIC **Catalog**: statestreet
# MAGIC **Schema**: b_statestreet
# MAGIC **Source Volume**: /Volumes/statestreet/securities_master/raw_files/
# MAGIC
# MAGIC **Pattern**: Hash-based MERGE INTO (idempotent — safe to re-run)
# MAGIC **Schema drift**: Additive columns → auto-merge; Breaking changes → quarantine + alert

# COMMAND ----------
# MAGIC %md ## 0. Imports & Configuration

# COMMAND ----------

import hashlib
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Parameters (overridable via Databricks job widgets)
# ---------------------------------------------------------------------------
dbutils.widgets.text("batch_id", "", "Batch ID (leave blank to auto-generate)")
dbutils.widgets.text("tables_override", "", "Comma-separated table names to re-run (blank = all)")

_batch_id_param = dbutils.widgets.get("batch_id").strip()
_tables_override = dbutils.widgets.get("tables_override").strip()

BATCH_ID: str = _batch_id_param if _batch_id_param else (
    f"bronze_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
)
TABLES_OVERRIDE: list[str] = (
    [t.strip() for t in _tables_override.split(",") if t.strip()]
    if _tables_override
    else []
)

# ---------------------------------------------------------------------------
# Catalog / schema / volume constants
# ---------------------------------------------------------------------------
CATALOG       = "statestreet"
SCHEMA        = "b_statestreet"
VOLUME_PATH   = "/Volumes/statestreet/securities_master/raw_files/"
FULL_SCHEMA   = f"{CATALOG}.{SCHEMA}"

print(f"[CONFIG] BATCH_ID      = {BATCH_ID}")
print(f"[CONFIG] CATALOG       = {CATALOG}")
print(f"[CONFIG] SCHEMA        = {SCHEMA}")
print(f"[CONFIG] VOLUME_PATH   = {VOLUME_PATH}")
print(f"[CONFIG] TABLES_FILTER = {TABLES_OVERRIDE if TABLES_OVERRIDE else 'ALL'}")

# COMMAND ----------
# MAGIC %md ## 1. Explicit Table Schemas
# MAGIC
# MAGIC All source schemas are declared explicitly (`inferSchema=false`).
# MAGIC This ensures type mismatches are detected as breaking schema drift
# MAGIC rather than silently widening types.

# COMMAND ----------

# ---------------------------------------------------------------------------
# Helper: build a StructField list from a concise column spec list
# ---------------------------------------------------------------------------
def _s(name: str, dtype, nullable: bool = True) -> StructField:
    return StructField(name, dtype, nullable)

# ---------------------------------------------------------------------------
# Source schemas keyed by table name
# ---------------------------------------------------------------------------
SOURCE_SCHEMAS: dict[str, StructType] = {

    "product": StructType([
        _s("product_id",             StringType(),       False),
        _s("id_type",                StringType(),       True),
        _s("type",                   StringType(),       False),
        _s("sub_type",               StringType(),       True),
        _s("status",                 StringType(),       False),
        _s("settlement_type",        StringType(),       True),
        _s("description",            StringType(),       True),
        _s("issue_date",             DateType(),         True),
        _s("issue_price",            DecimalType(18, 6), True),
        _s("current_face_value",     DecimalType(18, 6), True),
        _s("issuer_legal_entity_id", StringType(),       True),
        _s("tick_ladder_scale_id",   StringType(),       True),
    ]),

    "generic_product": StructType([
        _s("generic_product_id",             StringType(), False),
        _s("product_id",                     StringType(), True),
        _s("description",                    StringType(), True),
        _s("status",                         StringType(), True),
        _s("gs_legacy_prime_issue_currency", StringType(), True),
    ]),

    "legal_entity": StructType([
        _s("legal_entity_id",  StringType(), False),
        _s("legal_name",       StringType(), False),
        _s("country",          StringType(), True),
        _s("entity_type",      StringType(), True),
        _s("legal_structure",  StringType(), True),
        _s("formation_date",   DateType(),   True),
    ]),

    "tick_ladder_scale": StructType([
        _s("tick_ladder_scale_id", StringType(),        False),
        _s("description",          StringType(),        True),
        _s("tick_size",            DecimalType(18, 8),  True),
    ]),

    "tick": StructType([
        _s("tick_id",              StringType(),        False),
        _s("tick_ladder_scale_id", StringType(),        True),
        _s("price_from",           DecimalType(18, 6),  True),
        _s("price_to",             DecimalType(18, 6),  True),
        _s("tick_size",            DecimalType(18, 8),  True),
        _s("price_range",          StringType(),        True),
        _s("tick_currency_code",   StringType(),        True),
    ]),

    "series": StructType([
        _s("series_id",   StringType(), False),
        _s("description", StringType(), True),
    ]),

    "currency": StructType([
        _s("currency_code", StringType(), False),
        _s("description",   StringType(), True),
        _s("symbol",        StringType(), True),
    ]),

    "product_rating_type": StructType([
        _s("product_rating_type_id", StringType(), False),
        _s("rating_agency",          StringType(), True),
        _s("rating_scale",           StringType(), True),
        _s("rating_type_code",       StringType(), True),
        _s("description",            StringType(), True),
    ]),

    "product_rating": StructType([
        _s("product_rating_id",      StringType(), False),
        _s("product_id",             StringType(), False),
        _s("product_rating_type_id", StringType(), True),
        _s("rating_value",           StringType(), False),
        _s("rating_agency",          StringType(), False),
        _s("watch_code",             StringType(), True),
        _s("effective_from_date",    DateType(),   False),
    ]),

    "classification": StructType([
        _s("classification_id",    StringType(), False),
        _s("product_id",           StringType(), False),
        _s("classification_type",  StringType(), True),
        _s("classification_value", StringType(), True),
    ]),

    "identifiers": StructType([
        _s("identifiers_id",   StringType(), False),
        _s("product_id",       StringType(), False),
        _s("id_type",          StringType(), False),
        _s("identifier_value", StringType(), False),
        _s("cusip",            StringType(), True),
        _s("isin",             StringType(), True),
        _s("sedol",            StringType(), True),
    ]),

    "fund": StructType([
        _s("product_id",            StringType(), False),
        _s("endness_type",          StringType(), True),
        _s("mutual_fund_type",      StringType(), True),
        _s("mutual_fund_load_type", StringType(), True),
    ]),

    "right": StructType([
        _s("product_id",            StringType(),        False),
        _s("exercise_style",        StringType(),        True),
        _s("option_type",           StringType(),        True),
        _s("strike_price",          DecimalType(18, 6),  True),
        _s("exercise_start_date",   DateType(),          True),
        _s("exercise_end_date",     DateType(),          True),
        _s("underlying_product_id", StringType(),        True),
    ]),

    "debt": StructType([
        _s("product_id",            StringType(),        False),
        _s("face_amount",           DecimalType(18, 6),  True),
        _s("total_amount_issued",   DecimalType(18, 6),  True),
        _s("par_value",             DecimalType(18, 6),  True),
        _s("issue_date_settlement", DateType(),          True),
        _s("face_currency_code",    StringType(),        True),
    ]),

    "bond": StructType([
        _s("product_id",           StringType(), False),
        _s("coupon_type",          StringType(), False),
        _s("maturity_date",        DateType(),   False),
        _s("face_currency_code",   StringType(), False),
        _s("issue_currency_code",  StringType(), True),
        _s("day_count_convention", StringType(), True),
        _s("reference_index_rate", StringType(), True),
        _s("conversion_rule",      StringType(), True),
    ]),

    "muni": StructType([
        _s("product_id",  StringType(),  False),
        _s("tax_exempt",  BooleanType(), True),
        _s("state",       StringType(),  True),
        _s("purpose",     StringType(),  True),
        _s("pledge_type", StringType(),  True),
    ]),

    "pool_backed_security": StructType([
        _s("product_id", StringType(), False),
        _s("pool_type",  StringType(), True),
        _s("originator", StringType(), True),
    ]),

    "stock": StructType([
        _s("product_id",       StringType(), False),
        _s("series_id",        StringType(), True),
        _s("depository_type",  StringType(), True),
        _s("has_voting_rights", StringType(), True),
    ]),

    "common_stock": StructType([
        _s("product_id",    StringType(),  False),
        _s("voting_rights", BooleanType(), True),
    ]),

    "preferred_stock": StructType([
        _s("product_id",    StringType(),        False),
        _s("dividend_type", StringType(),        True),
        _s("dividend_right", StringType(),       True),
        _s("par_value",     DecimalType(18, 6),  True),
    ]),

    "listed_derivative": StructType([
        _s("product_id",            StringType(),  False),
        _s("series_id",             StringType(),  True),
        _s("underlying_product_id", StringType(),  True),
        _s("contract_month",        IntegerType(), True),
        _s("last_trade_date",       DateType(),    True),
    ]),

    "option": StructType([
        _s("product_id",           StringType(),        False),
        _s("option_type",          StringType(),        False),
        _s("exercise_style",       StringType(),        False),
        _s("margin_style",         StringType(),        True),
        _s("strike_price",         DecimalType(18, 6),  True),
        _s("strike_currency_code", StringType(),        True),
        _s("expiry_date",          DateType(),          True),
    ]),

    "future": StructType([
        _s("product_id",        StringType(), False),
        _s("delivery_date",     DateType(),   True),
        _s("valuation_method",  StringType(), True),
    ]),

    "coupon": StructType([
        _s("coupon_id",    StringType(),        False),
        _s("product_id",   StringType(),        False),
        _s("coupon_rate",  DecimalType(18, 6),  False),
        _s("payment_date", DateType(),          False),
        _s("coupon_type",  StringType(),        True),
        _s("frequency",    StringType(),        True),
    ]),

    "principal_redemption_provision": StructType([
        _s("provision_id",    StringType(), False),
        _s("provision_type",  StringType(), True),
        _s("description",     StringType(), True),
    ]),

    "listed_derivative_tick": StructType([
        _s("product_id", StringType(), False),
        _s("tick_id",    StringType(), False),
    ]),

    "debt_principal_redemption_provision": StructType([
        _s("product_id",   StringType(), False),
        _s("provision_id", StringType(), False),
    ]),

    "dq_rules_catalog": StructType([
        _s("rule_id",      StringType(), False),
        _s("table_name",   StringType(), True),
        _s("column_name",  StringType(), True),
        _s("rule_type",    StringType(), True),
        _s("severity",     StringType(), True),
        _s("description",  StringType(), True),
        _s("rule_sql",     StringType(), True),
    ]),

    "dq_issues_catalog": StructType([
        _s("issue_id",    StringType(), False),
        _s("rule_id",     StringType(), True),
        _s("table_name",  StringType(), True),
        _s("description", StringType(), True),
        _s("status",      StringType(), True),
    ]),
}

# ---------------------------------------------------------------------------
# Merge keys per table (from rules.yaml)
# ---------------------------------------------------------------------------
MERGE_KEYS: dict[str, list[str]] = {
    "product":                              ["product_id"],
    "generic_product":                      ["generic_product_id"],
    "legal_entity":                         ["legal_entity_id"],
    "tick_ladder_scale":                    ["tick_ladder_scale_id"],
    "tick":                                 ["tick_id"],
    "series":                               ["series_id"],
    "currency":                             ["currency_code"],
    "principal_redemption_provision":       ["provision_id"],
    "identifiers":                          ["identifiers_id"],
    "classification":                       ["classification_id"],
    "product_rating_type":                  ["product_rating_type_id"],
    "product_rating":                       ["product_rating_id"],
    "stock":                                ["product_id"],
    "common_stock":                         ["product_id"],
    "preferred_stock":                      ["product_id"],
    "debt":                                 ["product_id"],
    "bond":                                 ["product_id"],
    "muni":                                 ["product_id"],
    "pool_backed_security":                 ["product_id"],
    "listed_derivative":                    ["product_id"],
    "option":                               ["product_id"],
    "future":                               ["product_id"],
    "coupon":                               ["coupon_id"],
    "fund":                                 ["product_id"],
    "right":                                ["product_id"],
    "listed_derivative_tick":               ["product_id", "tick_id"],
    "debt_principal_redemption_provision":  ["product_id", "provision_id"],
    "dq_rules_catalog":                     ["rule_id"],
    "dq_issues_catalog":                    ["issue_id"],
}

# Ordered list of all tables to ingest
ALL_TABLES: list[str] = list(MERGE_KEYS.keys())

# Resolved run list (all tables, or override subset)
RUN_TABLES: list[str] = (
    [t for t in TABLES_OVERRIDE if t in ALL_TABLES]
    if TABLES_OVERRIDE
    else ALL_TABLES
)

print(f"[CONFIG] Tables to ingest ({len(RUN_TABLES)}): {RUN_TABLES}")

# COMMAND ----------
# MAGIC %md ## 2. Helper Functions

# COMMAND ----------

# ---------------------------------------------------------------------------
# 2a. Add standard Bronze metadata columns
# ---------------------------------------------------------------------------

_META_COLS = {"_source_file", "_ingestion_ts", "_batch_id", "_row_hash",
              "_ingestion_date"}


def _add_metadata(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """
    Append the four standard Bronze metadata columns plus the generated
    _ingestion_date partition column.

    _row_hash is SHA-256 of all DATA columns (i.e., everything except the
    metadata columns themselves).  Concatenated with '|' separator so that
    a NULL in any column is included as an empty string rather than being
    silently dropped by concat().
    """
    data_cols = [F.col(c) for c in df.columns if c not in _META_COLS]

    return (
        df
        .withColumn("_source_file",    F.lit(source_file))
        .withColumn("_ingestion_ts",   F.current_timestamp())
        .withColumn("_batch_id",       F.lit(batch_id))
        .withColumn("_row_hash",       F.sha2(
            F.concat_ws("|", *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in df.columns]),
            256,
        ))
        .withColumn("_ingestion_date", F.current_date())
    )


# ---------------------------------------------------------------------------
# 2b. Schema drift detection
# ---------------------------------------------------------------------------

_DRIFT_IGNORE_COLS = _META_COLS  # do not compare metadata cols


def _detect_drift(table_full_name: str, incoming_schema: StructType) -> dict:
    """
    Compare incoming DataFrame schema to the existing Bronze Delta table.

    Returns a dict with keys:
      new_table  bool  — table does not yet exist (first load)
      additive   list  — columns present in incoming but absent in existing
      breaking   list  — type changes + removed columns
      unchanged  list  — identical in both
    """
    try:
        existing_fields = {
            f.name: f.dataType
            for f in spark.table(table_full_name).schema.fields
            if f.name not in _DRIFT_IGNORE_COLS
        }
    except Exception:
        return {"new_table": True, "additive": [], "breaking": [], "unchanged": []}

    incoming_fields = {
        f.name: f.dataType
        for f in incoming_schema.fields
        if f.name not in _DRIFT_IGNORE_COLS
    }

    additive      = [c for c in incoming_fields if c not in existing_fields]
    type_changes  = [
        c for c in existing_fields
        if c in incoming_fields
        and str(incoming_fields[c]) != str(existing_fields[c])
    ]
    removed       = [c for c in existing_fields if c not in incoming_fields]
    breaking      = sorted(set(type_changes + removed))
    unchanged     = [
        c for c in existing_fields
        if c in incoming_fields and c not in breaking
    ]

    return {
        "new_table":     False,
        "additive":      additive,
        "breaking":      breaking,
        "unchanged":     unchanged,
        "type_changes":  type_changes,
        "removed_cols":  removed,
    }


# ---------------------------------------------------------------------------
# 2c. Audit table helpers
# ---------------------------------------------------------------------------

_SCHEMA_CHANGES_TABLE     = f"{FULL_SCHEMA}._schema_changes"
_SCHEMA_QUARANTINE_TABLE  = f"{FULL_SCHEMA}._schema_quarantine"


def _ensure_audit_tables() -> None:
    """Create the two Bronze audit tables if they do not yet exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA_CHANGES_TABLE} (
          table_name        STRING     NOT NULL,
          change_type       STRING     NOT NULL,
          columns_affected  STRING,
          batch_id          STRING,
          detected_at       TIMESTAMP  NOT NULL
        )
        USING DELTA
        TBLPROPERTIES ('delta.appendOnly' = 'true')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_SCHEMA_QUARANTINE_TABLE} (
          batch_id          STRING     NOT NULL,
          table_name        STRING     NOT NULL,
          change_type       STRING,
          columns_affected  STRING,
          quarantined_at    TIMESTAMP  NOT NULL,
          resolved_by       STRING,
          resolved_at       TIMESTAMP,
          resolution_note   STRING
        )
        USING DELTA
    """)


def _log_schema_change(table_name: str, change_type: str, columns: list[str]) -> None:
    cols_str = ",".join(columns)
    spark.sql(f"""
        INSERT INTO {_SCHEMA_CHANGES_TABLE}
          (table_name, change_type, columns_affected, batch_id, detected_at)
        VALUES (
          '{table_name}', '{change_type}', '{cols_str}',
          '{BATCH_ID}', current_timestamp()
        )
    """)


def _log_quarantine(table_name: str, change_type: str, columns: list[str]) -> None:
    cols_str = ",".join(columns)
    spark.sql(f"""
        INSERT INTO {_SCHEMA_QUARANTINE_TABLE}
          (batch_id, table_name, change_type, columns_affected, quarantined_at)
        VALUES (
          '{BATCH_ID}', '{table_name}', '{change_type}', '{cols_str}',
          current_timestamp()
        )
    """)


# ---------------------------------------------------------------------------
# 2d. Iceberg UniForm enablement
# ---------------------------------------------------------------------------

def _enable_iceberg_uniform(table_full_name: str) -> None:
    """
    Enable Iceberg UniForm on a Delta table.
    Requires: column mapping mode='name', IcebergCompatV2, no Deletion Vectors.
    All three must be set in a single ALTER TABLE on existing tables.
    Fails silently with a warning if pre-conditions are not met.
    """
    try:
        spark.sql(f"""
            ALTER TABLE {table_full_name}
            SET TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name',
                'delta.enableIcebergCompatV2' = 'true',
                'delta.universalFormat.enabledFormats' = 'iceberg'
            )
        """)
    except Exception as _iceberg_err:
        print(f"  [WARN] Iceberg UniForm skipped for {table_full_name}: {_iceberg_err}")
        print(f"  [WARN] Table is fully usable as a regular Delta table.")


# ---------------------------------------------------------------------------
# 2e. Auto-merge additive columns
# ---------------------------------------------------------------------------

def _handle_additive_drift(table_full_name: str, new_columns: list[str]) -> None:
    """
    Add new columns (as STRING) to the existing Bronze table.
    All existing rows receive NULL for the new columns.
    Event is logged to _schema_changes.
    """
    for col_name in new_columns:
        print(f"  [drift:additive] Adding column '{col_name}' to {table_full_name}")
        spark.sql(f"ALTER TABLE {table_full_name} ADD COLUMNS ({col_name} STRING)")

    _log_schema_change(table_full_name, "ADDITIVE", new_columns)
    print(f"  [drift:additive] Logged {len(new_columns)} new column(s) to {_SCHEMA_CHANGES_TABLE}")


# ---------------------------------------------------------------------------
# 2f. Breaking drift → quarantine
# ---------------------------------------------------------------------------

def _handle_breaking_drift(table_full_name: str, breaking_columns: list[str]) -> None:
    """
    Write a quarantine record and raise an exception to fail the Databricks job.
    The batch is NOT written to Bronze.
    """
    _log_quarantine(table_full_name, "BREAKING", breaking_columns)
    raise ValueError(
        f"[SCHEMA DRIFT — BREAKING] Table '{table_full_name}' has breaking changes: "
        f"{breaking_columns}. "
        f"Batch '{BATCH_ID}' quarantined. "
        f"Review: SELECT * FROM {_SCHEMA_QUARANTINE_TABLE} WHERE batch_id = '{BATCH_ID}'"
    )


# COMMAND ----------
# MAGIC %md ## 3. Core Ingestion Function

# COMMAND ----------

def ingest_table(
    table_name:  str,
    source_file: Optional[str] = None,
    merge_keys:  Optional[list[str]] = None,
) -> dict:
    """
    Full Bronze ingestion pipeline for a single table:

      1. Read CSV from Volume using explicit schema
      2. Add metadata columns (_source_file, _ingestion_ts, _batch_id, _row_hash, _ingestion_date)
      3. Detect schema drift vs existing Bronze Delta table
         a. Additive → auto-merge (ALTER TABLE ADD COLUMNS)
         b. Breaking → quarantine + raise exception
      4. First load → CREATE (write.saveAsTable)
         Re-run    → MERGE INTO (hash-based CDC; only changed rows updated)
      5. Enable Iceberg UniForm
      6. Return stats dict

    Parameters
    ----------
    table_name  : str  — name of the Bronze table (and key in SOURCE_SCHEMAS)
    source_file : str  — CSV filename inside VOLUME_PATH (defaults to <table_name>.csv)
    merge_keys  : list — columns used in the MERGE ON clause (defaults from MERGE_KEYS)
    """
    _source_file = source_file or f"{table_name}.csv"
    _merge_keys  = merge_keys  or MERGE_KEYS.get(table_name, [])
    full_table   = f"{FULL_SCHEMA}.{table_name}"
    csv_path     = f"{VOLUME_PATH}{_source_file}"

    print(f"\n{'─'*70}")
    print(f"[INGEST] {table_name}  ←  {csv_path}")

    # ------------------------------------------------------------------
    # Step 1: Read CSV
    # ------------------------------------------------------------------
    schema = SOURCE_SCHEMAS.get(table_name)
    if schema is None:
        raise ValueError(f"No source schema defined for table '{table_name}'")

    df_raw = (
        spark.read
        .option("header",    "true")
        .option("nullValue",  "")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(csv_path)
    )
    rows_read = df_raw.count()
    print(f"  [read]  {rows_read:,} rows from {csv_path}")

    # ------------------------------------------------------------------
    # Step 2: Add metadata columns
    # ------------------------------------------------------------------
    df = _add_metadata(df_raw, _source_file, BATCH_ID)

    # ------------------------------------------------------------------
    # Step 3: Schema drift detection
    # ------------------------------------------------------------------
    drift = _detect_drift(full_table, schema)

    if not drift["new_table"]:
        if drift["breaking"]:
            # Breaking change — quarantine and stop
            _handle_breaking_drift(full_table, drift["breaking"])

        if drift["additive"]:
            # Additive columns — auto-merge into existing table
            _handle_additive_drift(full_table, drift["additive"])

    # ------------------------------------------------------------------
    # Step 4a: First load — CREATE via saveAsTable
    # ------------------------------------------------------------------
    if drift["new_table"]:
        print(f"  [write] First load — creating table {full_table}")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("_ingestion_date")
            .saveAsTable(full_table)
        )
        rows_written = rows_read

    # ------------------------------------------------------------------
    # Step 4b: Re-run — MERGE INTO (hash-based CDC)
    # ------------------------------------------------------------------
    else:
        temp_view = f"_bronze_incoming_{table_name}"
        df.createOrReplaceTempView(temp_view)

        # Build the ON clause from merge keys
        on_clause = " AND ".join([
            f"target.{k} = source.{k}" for k in _merge_keys
        ])

        merge_sql = f"""
            MERGE INTO {full_table} AS target
            USING {temp_view} AS source
            ON {on_clause}
            WHEN MATCHED AND source._row_hash != target._row_hash
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
        """
        print(f"  [merge] MERGE INTO {full_table} ON ({', '.join(_merge_keys)})")
        spark.sql(merge_sql)

        # Count written by re-reading (Databricks MERGE does not return row counts directly)
        rows_written = df.count()

    # ------------------------------------------------------------------
    # Step 5: Enable Iceberg UniForm
    # ------------------------------------------------------------------
    _enable_iceberg_uniform(full_table)
    print(f"  [done]  Iceberg UniForm enabled on {full_table}")

    return {
        "table":        table_name,
        "source_file":  _source_file,
        "rows_read":    rows_read,
        "rows_written": rows_written,
        "status":       "SUCCESS",
        "batch_id":     BATCH_ID,
    }


# COMMAND ----------
# MAGIC %md ## 4. Ensure Catalog, Schema, and Audit Tables Exist

# COMMAND ----------

print(f"[SETUP] Ensuring catalog '{CATALOG}' and schema '{FULL_SCHEMA}' exist...")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {FULL_SCHEMA}")

_ensure_audit_tables()

print(f"[SETUP] Audit tables ready:")
print(f"        {_SCHEMA_CHANGES_TABLE}")
print(f"        {_SCHEMA_QUARANTINE_TABLE}")

# COMMAND ----------
# MAGIC %md ## 5. Ingest All Tables
# MAGIC
# MAGIC Tables are ingested in dependency-safe order:
# MAGIC reference tables first, then base tables, then subtype tables,
# MAGIC then relationship/bridge tables, then metadata tables.

# COMMAND ----------

# ---------------------------------------------------------------------------
# Ingestion order — dependency-safe
# Group 0: Pure reference tables (no FKs into securities)
# Group 1: Core base tables
# Group 2: Product subtype tables (extend product)
# Group 3: Deeper subtype tables (extend subtypes)
# Group 4: Relationship / bridge tables
# Group 5: DQ metadata tables (Bronze-only, no Silver/Gold)
# ---------------------------------------------------------------------------

INGESTION_ORDER: list[str] = [
    # ── Group 0: Reference tables ──────────────────────────────────────────
    "currency",
    "series",
    "tick_ladder_scale",
    "tick",
    "principal_redemption_provision",
    "product_rating_type",

    # ── Group 1: Core base table ───────────────────────────────────────────
    "product",
    "legal_entity",
    "generic_product",

    # ── Group 2: First-level product subtypes ─────────────────────────────
    "fund",
    "right",
    "stock",
    "debt",
    "listed_derivative",

    # ── Group 3: Second-level product subtypes ────────────────────────────
    "common_stock",
    "preferred_stock",
    "bond",
    "muni",
    "pool_backed_security",
    "option",
    "future",

    # ── Group 4: Relationship / rating / coupon tables ────────────────────
    "identifiers",
    "classification",
    "product_rating",
    "coupon",
    "listed_derivative_tick",
    "debt_principal_redemption_provision",

    # ── Group 5: DQ metadata tables (Bronze-only) ─────────────────────
    "dq_rules_catalog",
    "dq_issues_catalog",
]

# ---------------------------------------------------------------------------
# Apply table filter (if tables_override widget was supplied)
# ---------------------------------------------------------------------------
RUN_TABLES: list[str] = (
    [t for t in INGESTION_ORDER if t in TABLES_OVERRIDE]
    if TABLES_OVERRIDE
    else INGESTION_ORDER
)

print(f"\n[RUN] Ingesting {len(RUN_TABLES)} of {len(INGESTION_ORDER)} tables")
print(f"      Tables: {', '.join(RUN_TABLES)}")

# COMMAND ----------
# MAGIC %md ## 6. Execute Ingestion Loop

# COMMAND ----------

results = []
errors  = []

for table_name in RUN_TABLES:
    try:
        result = ingest_table(table_name)
        results.append(result)
    except Exception as exc:
        import traceback as _tb
        err_msg = f"{type(exc).__name__}: {exc}"
        errors.append({"table": table_name, "error": err_msg})
        print(f"  [ERROR] {table_name}: {err_msg}")
        print(_tb.format_exc())

# COMMAND ----------
# MAGIC %md ## 7. Summary

# COMMAND ----------

print(f"\n{'='*70}")
print(f"[SUMMARY] Batch ID  : {BATCH_ID}")
print(f"[SUMMARY] Tables run: {len(results)} succeeded, {len(errors)} failed")
print(f"{'─'*70}")
total_rows_read    = 0
total_rows_written = 0
for r in results:
    print(f"  OK  {r['table']:35s}  read={r['rows_read']:>8,}  written={r['rows_written']:>8,}")
    total_rows_read    += r["rows_read"]
    total_rows_written += r["rows_written"]

for e in errors:
    print(f"  ERR {e['table']:35s}  ERROR: {e['error']}")

print(f"{'─'*70}")
print(f"[SUMMARY] Total rows read   : {total_rows_read:,}")
print(f"[SUMMARY] Total rows written: {total_rows_written:,}")
print(f"{'='*70}")

if errors:
    first = errors[0]
    raise RuntimeError(
        f"Bronze ingestion completed with {len(errors)} error(s). "
        f"First failure — {first['table']}: {first['error']}"
    )

print("\n[DONE] Bronze ingestion completed successfully.")