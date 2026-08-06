"""
schema_drift.py
Schema drift detection and handling for Bronze ingestion.
See skills/schema_drift.md for full documentation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def detect_drift(spark: SparkSession, full_table_name: str, incoming_schema: StructType) -> dict:
    """
    Compare incoming CSV schema to existing Bronze table schema.

    Returns:
        dict with keys:
          - new_table (bool): True if table doesn't exist yet
          - additive (list): column names that are new in CSV (safe to merge)
          - breaking (list): columns that changed type or were removed
          - unchanged (list): columns with same name + type
    """
    try:
        existing = spark.table(full_table_name).schema
    except Exception:
        return {"new_table": True, "additive": [], "breaking": [], "unchanged": []}

    # Filter out metadata columns added by pipeline (not from source)
    meta_cols = {"_source_file", "_ingestion_ts", "_batch_id", "_row_hash"}

    existing_fields = {
        f.name: str(f.dataType)
        for f in existing.fields
        if f.name not in meta_cols
    }
    incoming_fields = {
        f.name: str(f.dataType)
        for f in incoming_schema.fields
        if f.name not in meta_cols
    }

    additive = [c for c in incoming_fields if c not in existing_fields]

    breaking = [
        c for c in existing_fields
        if c in incoming_fields and incoming_fields[c] != existing_fields[c]
    ]
    # Removed columns (existed before, gone now) are also breaking
    removed = [c for c in existing_fields if c not in incoming_fields]
    breaking.extend(removed)

    unchanged = [
        c for c in existing_fields
        if c in incoming_fields and c not in breaking
    ]

    return {
        "new_table": False,
        "additive": additive,
        "breaking": breaking,
        "unchanged": unchanged,
    }


def handle_additive_drift(
    spark: SparkSession,
    catalog: str,
    schema: str,
    full_table_name: str,
    new_columns: list[str],
):
    """
    Log additive drift to _schema_changes table.
    Delta will auto-merge via mergeSchema=True during the write.
    """
    cols_str = ",".join(new_columns)
    spark.sql(f"""
        INSERT INTO {catalog}.{schema}._schema_changes
        VALUES ('{full_table_name}', 'ADDITIVE', '{cols_str}', current_timestamp())
    """)
    print(f"[DRIFT] Additive columns auto-merged: {new_columns}")


def handle_breaking_drift(
    spark: SparkSession,
    batch_id: str,
    full_table_name: str,
    breaking_columns: list[str],
):
    """
    Log breaking drift and stop the batch.
    Raises ValueError to fail the Databricks job task so ops team is alerted.
    """
    # Extract catalog.schema from full_table_name for quarantine table
    parts = full_table_name.split(".")
    catalog, schema = parts[0], parts[1]
    cols_str = ",".join(breaking_columns)

    try:
        spark.sql(f"""
            INSERT INTO {catalog}.{schema}._schema_quarantine
            VALUES ('{batch_id}', '{full_table_name}', 'BREAKING', '{cols_str}', current_timestamp())
        """)
    except Exception:
        print(f"[DRIFT] Warning: could not write to _schema_quarantine")

    raise ValueError(
        f"Breaking schema change detected in {full_table_name}: {breaking_columns}. "
        f"Batch {batch_id} aborted. Manual review required."
    )
