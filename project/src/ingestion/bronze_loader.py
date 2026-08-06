from __future__ import annotations
"""
bronze_loader.py
Source-agnostic Bronze ingestion engine.

Delegates data reading to source_reader.py based on source.type in request.yaml.
Supported source types: volume, jdbc, api, kafka, delta, s3, adls, gcs.

This module handles only the Bronze layer responsibilities:
  - Add metadata columns (_source_file, _source_type, _ingestion_ts, _batch_id, _row_hash)
  - Detect and handle schema drift (additive=auto-merge, breaking=quarantine)
  - Write to Delta via MERGE INTO (idempotent) or CREATE TABLE (first load)
  - Enable Iceberg UniForm

Usage in generated Bronze notebooks:
    from src.ingestion.bronze_loader import ingest_bronze, ingest_all_bronze
    from src.common.spec_loader import load_spec

    spec = load_spec("securities-master", "bronze")
    source_config = request_config["source"]           # from request.yaml

    # Ingest a single table
    result = ingest_bronze(spark, spec.get_table("product"), source_config,
                           catalog="statestreet", schema="b_statestreet",
                           batch_id=dbutils.widgets.get("batch_id"))

    # Or ingest all tables from spec in one call
    results = ingest_all_bronze(spark, spec, source_config,
                                batch_id=dbutils.widgets.get("batch_id"))
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.ingestion.source_reader import read_source
from src.ingestion.schema_drift import detect_drift, handle_additive_drift, handle_breaking_drift
from src.common.catalog_utils import enable_iceberg_uniform


def ingest_bronze(
    spark: SparkSession,
    table_spec: dict,
    source_config: dict,
    catalog: str,
    schema: str,
    batch_id: str,
    iceberg_uniform: bool = True,
) -> dict:
    """
    Ingest a single table into the Bronze Delta layer from any source.

    The source type (volume / jdbc / api / kafka / delta / s3 / adls / gcs) is
    read from source_config["type"] — this comes directly from request.yaml.

    Args:
        spark:          Active SparkSession
        table_spec:     One table entry from bronze/tables.yaml (dict with name, primary_key, etc.)
        source_config:  The 'source:' block from request.yaml
        catalog:        Unity Catalog name  (e.g. "statestreet")
        schema:         Bronze schema name  (e.g. "b_statestreet")
        batch_id:       Pipeline run identifier (e.g. UUID or timestamp string)
        iceberg_uniform: Enable Delta UniForm for Iceberg compat (default: True)

    Returns:
        dict with keys: table_name, source_type, rows_read, rows_written, drift, status
    """
    table_name       = table_spec["name"]
    primary_key      = table_spec.get("primary_key", [])
    source_type      = source_config.get("type", "volume")
    full_table_name  = f"{catalog}.{schema}.{table_name}"

    # ── 1. Read from source (source_reader routes by source_config["type"]) ──
    df = read_source(spark, table_spec, source_config)
    rows_read = df.count()

    if rows_read == 0:
        return {
            "table_name":   table_name,
            "source_type":  source_type,
            "rows_read":    0,
            "rows_written": 0,
            "drift":        {},
            "status":       "skipped_empty",
        }

    # ── 2. Add Bronze metadata columns ────────────────────────────────────────
    source_label = table_spec.get("source_file", f"{table_name}.{source_config.get('format', source_type)}")
    df = _add_metadata(df, source_label, source_type, batch_id)

    # ── 3. Schema drift detection ─────────────────────────────────────────────
    drift = detect_drift(spark, full_table_name, df.schema)

    if drift.get("breaking"):
        handle_breaking_drift(spark, batch_id, full_table_name, drift["breaking"])
        return {
            "table_name":   table_name,
            "source_type":  source_type,
            "rows_read":    rows_read,
            "rows_written": 0,
            "drift":        drift,
            "status":       "quarantined_breaking_drift",
        }

    if drift.get("additive") and not drift.get("new_table"):
        handle_additive_drift(spark, catalog, schema, full_table_name, drift["additive"])

    # ── 4. Write to Delta ────────────────────────────────────────────────────
    if drift.get("new_table"):
        _write_new_table(df, full_table_name)
    else:
        _merge_into(spark, df, full_table_name, primary_key)

    # ── 5. Enable Iceberg UniForm ────────────────────────────────────────────
    if iceberg_uniform:
        enable_iceberg_uniform(spark, full_table_name)

    rows_written = spark.table(full_table_name).count()

    return {
        "table_name":   table_name,
        "source_type":  source_type,
        "rows_read":    rows_read,
        "rows_written": rows_written,
        "drift":        drift,
        "status":       "ok",
    }


def ingest_all_bronze(
    spark: SparkSession,
    spec: object,
    source_config: dict,
    batch_id: str,
    skip_tables: list[str] | None = None,
    iceberg_uniform: bool = True,
) -> list[dict]:
    """
    Ingest all tables defined in bronze/tables.yaml for a use case.

    Args:
        spark:          Active SparkSession
        spec:           Loaded BronzeSpec object (from spec_loader.load_spec)
        source_config:  The 'source:' block from request.yaml
        batch_id:       Pipeline run identifier
        skip_tables:    Table names to skip (e.g. already loaded in partial run)
        iceberg_uniform: Enable Delta UniForm for all tables

    Returns:
        List of result dicts (one per table) — same shape as ingest_bronze()
    """
    skip = set(skip_tables or [])
    results = []

    for table_spec in spec.tables:
        table_name = table_spec["name"]
        if table_name in skip:
            print(f"  [skip] {table_name}")
            continue

        print(f"  [ingest] {table_name} (source: {source_config.get('type', 'volume')})")
        try:
            result = ingest_bronze(
                spark         = spark,
                table_spec    = table_spec,
                source_config = source_config,
                catalog       = spec.catalog,
                schema        = spec.schema,
                batch_id      = batch_id,
                iceberg_uniform = iceberg_uniform,
            )
            print(f"    ✓ rows_read={result['rows_read']:,}  rows_written={result['rows_written']:,}  status={result['status']}")
        except Exception as e:
            result = {
                "table_name":   table_name,
                "source_type":  source_config.get("type", "volume"),
                "rows_read":    0,
                "rows_written": 0,
                "drift":        {},
                "status":       f"error: {e}",
            }
            print(f"    ✗ {e}")

        results.append(result)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# PRIVATE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _add_metadata(df: DataFrame, source_label: str, source_type: str, batch_id: str) -> DataFrame:
    """
    Append standard Bronze metadata columns to the right of all data columns.

    Columns added:
      _source_file  — filename or endpoint identifier (from table_spec.source_file or table_name.format)
      _source_type  — "volume" | "jdbc" | "api" | "kafka" | "delta" | "s3" | "adls" | "gcs"
      _ingestion_ts — timestamp when row was read (current_timestamp())
      _batch_id     — pipeline run identifier (e.g. UUID passed from orchestrator)
      _row_hash     — SHA256 of all data column values concatenated with "|" separator
                      Used for CDC: UPDATE only when hash differs (MERGE INTO condition)
    """
    data_columns = [F.col(c) for c in df.columns]
    return (df
            .withColumn("_source_file",  F.lit(source_label))
            .withColumn("_source_type",  F.lit(source_type))
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_batch_id",     F.lit(batch_id))
            .withColumn("_row_hash",     F.sha2(F.concat_ws("|", *data_columns), 256)))


def _write_new_table(df: DataFrame, full_table_name: str):
    """
    First load — create the Delta table.
    Uses overwriteSchema=true so re-running is safe if table exists with wrong schema.
    Iceberg UniForm is enabled separately after write.
    """
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_table_name))


def _merge_into(spark: SparkSession, df: DataFrame, full_table_name: str, primary_key: list[str]):
    """
    Idempotent MERGE INTO existing Delta table.

    Merge logic:
      MATCHED + hash changed → UPDATE all columns (avoids rewriting unchanged rows)
      MATCHED + hash same    → no-op
      NOT MATCHED            → INSERT (new record)

    primary_key must be provided (from bronze/tables.yaml → tables[].primary_key).
    If primary_key is empty, falls back to overwrite (not idempotent — log a warning).
    """
    if not primary_key:
        print(f"  [warn] No primary_key defined for {full_table_name}. Using overwrite (not idempotent).")
        _write_new_table(df, full_table_name)
        return

    # Register incoming batch as a temp view (safe name: replace dots with underscores)
    view_name = f"_incoming_{full_table_name.replace('.', '_')}"
    df.createOrReplaceTempView(view_name)

    pk_join = " AND ".join([f"target.{k} = source.{k}" for k in primary_key])

    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING {view_name} AS source
        ON {pk_join}
        WHEN MATCHED AND source._row_hash != target._row_hash THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
    """)
