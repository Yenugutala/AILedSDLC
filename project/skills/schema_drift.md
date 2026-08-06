# Schema Drift — Detection and Handling

Complete reference for detecting and responding to schema changes in source data.

---

## 1. What Is Schema Drift?

Schema drift happens when the source CSV/database structure changes between pipeline runs:

| Change Type | Example | Risk Level | Policy |
|-------------|---------|------------|--------|
| **Additive** | New column added to CSV | Low | Auto-merge into Bronze; log to `_schema_changes` |
| **Type change** | `coupon_rate` changes from STRING to DOUBLE | High | Quarantine batch; alert data engineers |
| **Column removal** | Source drops `legacy_field` | High | Quarantine batch; alert data engineers |
| **Column rename** | `product_code` → `product_id` | High | Quarantine batch (rename = remove + add) |
| **New table** | First time loading this source | None | Create Bronze table automatically |

---

## 2. Drift Policy (from bronze/rules.yaml)

```yaml
schema_drift:
  additive_columns: auto_merge      # New column → auto-merge into Bronze table, log event
  breaking_changes: quarantine      # Type change / column drop → stop batch, alert
  quarantine_schema: statestreet.b_statestreet  # Where quarantine records are written
  alert_on_breaking: true           # Send notification (Databricks webhook or email)
```

---

## 3. Detection Logic (Python — src/ingestion/schema_drift.py)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

def detect_drift(spark: SparkSession, table_name: str, incoming_schema: StructType) -> dict:
    """
    Compare incoming data schema to existing Bronze Delta table schema.

    Returns dict with keys:
      new_table  — bool: table doesn't exist yet (first load)
      additive   — list[str]: columns in incoming but NOT in existing (safe to add)
      breaking   — list[str]: type mismatches + removed columns (unsafe — quarantine)
      unchanged  — list[str]: identical columns in both schemas
    """
    # Filter out metadata columns from drift comparison
    META_COLS = {"_source_file", "_source_type", "_ingestion_ts", "_batch_id", "_row_hash",
                 "_ingestion_date", "_schema_changes"}

    try:
        existing = spark.table(table_name).schema
    except Exception:
        # Table doesn't exist — first load
        return {"additive": [], "breaking": [], "unchanged": [], "new_table": True}

    existing_fields = {
        f.name: f.dataType
        for f in existing.fields
        if f.name not in META_COLS
    }
    incoming_fields = {
        f.name: f.dataType
        for f in incoming_schema.fields
        if f.name not in META_COLS
    }

    # Columns in incoming but not in existing → additive (new column)
    additive = [c for c in incoming_fields if c not in existing_fields]

    # Columns in both but with different types → breaking (type change)
    type_changes = [
        c for c in existing_fields
        if c in incoming_fields
        and str(incoming_fields[c]) != str(existing_fields[c])
    ]

    # Columns in existing but not in incoming → breaking (column removal)
    removed = [c for c in existing_fields if c not in incoming_fields]

    breaking  = type_changes + removed
    unchanged = [
        c for c in existing_fields
        if c in incoming_fields and c not in breaking
    ]

    return {
        "additive":  additive,
        "breaking":  breaking,
        "unchanged": unchanged,
        "new_table": False,
        "type_changes": type_changes,
        "removed_cols": removed,
    }
```

---

## 4. Handling Additive Changes (Auto-Merge)

```python
def handle_additive_drift(
    spark: SparkSession,
    catalog: str,
    schema: str,
    full_table_name: str,
    new_columns: list
):
    """
    Auto-merge new columns into existing Bronze Delta table.

    Delta supports adding columns via ALTER TABLE or mergeSchema option.
    All existing rows get NULL for the new column.
    The drift event is logged to _schema_changes for audit.
    """
    # Option 1: ALTER TABLE ADD COLUMNS (cleanest — no data rewrite)
    for col_name in new_columns:
        spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMNS ({col_name} STRING)")
        print(f"  [drift] Added column '{col_name}' to {full_table_name}")

    # Option 2 (alternative): mergeSchema=True on write
    # df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)

    # Log the drift event to audit table
    cols_str = ",".join(new_columns)
    spark.sql(f"""
        INSERT INTO {catalog}.{schema}._schema_changes
        (table_name, change_type, columns_affected, detected_at)
        VALUES ('{full_table_name}', 'ADDITIVE', '{cols_str}', current_timestamp())
    """)
```

---

## 5. Handling Breaking Changes (Quarantine)

```python
def handle_breaking_drift(
    spark: SparkSession,
    batch_id: str,
    full_table_name: str,
    breaking_columns: list
):
    """
    Stop the batch and quarantine metadata.

    Does NOT modify the existing Bronze table.
    Does NOT write the incoming data to Bronze.
    Writes a quarantine record for human review.
    Raises an exception to fail the Databricks job (triggers retry/alert).
    """
    catalog = full_table_name.split(".")[0]
    schema  = full_table_name.split(".")[1]

    cols_str = ",".join(breaking_columns)
    spark.sql(f"""
        INSERT INTO {catalog}.{schema}._schema_quarantine
        (batch_id, table_name, change_type, columns_affected, quarantined_at)
        VALUES ('{batch_id}', '{full_table_name}', 'BREAKING', '{cols_str}', current_timestamp())
    """)

    raise ValueError(
        f"[SCHEMA DRIFT] Breaking change in {full_table_name}: {breaking_columns}. "
        f"Batch {batch_id} quarantined. "
        f"Review: SELECT * FROM {catalog}.{schema}._schema_quarantine WHERE batch_id = '{batch_id}'"
    )
```

---

## 6. Schema Audit Tables (Created in 01_setup_catalog.py)

```sql
-- Tracks all schema drift events (additive and breaking)
CREATE TABLE IF NOT EXISTS statestreet.b_statestreet._schema_changes (
  table_name       STRING     NOT NULL,
  change_type      STRING     NOT NULL,    -- ADDITIVE | BREAKING
  columns_affected STRING,
  detected_at      TIMESTAMP  NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.appendOnly' = 'true');   -- append-only audit log

-- Holds metadata about quarantined batches (does NOT contain the data itself)
CREATE TABLE IF NOT EXISTS statestreet.b_statestreet._schema_quarantine (
  batch_id         STRING     NOT NULL,
  table_name       STRING     NOT NULL,
  change_type      STRING,
  columns_affected STRING,
  quarantined_at   TIMESTAMP  NOT NULL,
  resolved_by      STRING,                -- filled in when manually resolved
  resolved_at      TIMESTAMP,
  resolution_note  STRING
)
USING DELTA;
```

---

## 7. Querying Drift History

```sql
-- All schema changes in the last 30 days
SELECT *
FROM statestreet.b_statestreet._schema_changes
WHERE detected_at > current_timestamp() - INTERVAL 30 DAYS
ORDER BY detected_at DESC;

-- All breaking changes (quarantine events)
SELECT *
FROM statestreet.b_statestreet._schema_quarantine
ORDER BY quarantined_at DESC;

-- Tables that have experienced schema drift
SELECT DISTINCT table_name, change_type, MAX(detected_at) AS last_drift
FROM statestreet.b_statestreet._schema_changes
GROUP BY table_name, change_type
ORDER BY last_drift DESC;
```

---

## 8. Resolving a Quarantine Event

When a breaking change is quarantined, a data engineer must:

1. **Identify the change:** `SELECT * FROM statestreet.b_statestreet._schema_quarantine WHERE batch_id = '<id>'`
2. **Understand the impact:** Compare old and new source schemas
3. **Update the Bronze table spec** in `specs/bronze/tables.yaml` (new type or column name)
4. **Run a migration** if needed (ALTER TABLE, data backfill)
5. **Re-run the failed batch:** `sml run --use-case securities-master --job bronze_ingest_job`
6. **Mark as resolved:**

```sql
UPDATE statestreet.b_statestreet._schema_quarantine
SET resolved_by   = 'your-email@statestreet.com',
    resolved_at   = current_timestamp(),
    resolution_note = 'Updated coupon_rate column type from STRING to DOUBLE in tables.yaml'
WHERE batch_id = '<quarantine_batch_id>';
```

---

## 9. Schema Evolution Best Practices

- **Never change** column names in the source without coordinating with the pipeline team
- **Always add** new source columns rather than renaming existing ones
- **Notify** the pipeline team 1 sprint ahead of any source schema changes
- **Test** schema changes in a lower environment (dev catalog) before production
- **Use `inferSchema=false`** in production Bronze reads (explicit schema = no surprises):

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Explicit schema (from bronze/tables.yaml at runtime)
PRODUCT_SCHEMA = StructType([
    StructField("product_id",   StringType(), nullable=False),
    StructField("id_type",      StringType(), nullable=True),
    StructField("type",         StringType(), nullable=True),
    StructField("coupon_rate",  DoubleType(), nullable=True),
    StructField("issue_date",   DateType(),   nullable=True),
    # ... all columns
])

df = (spark.read
    .schema(PRODUCT_SCHEMA)     # explicit schema — fails fast on type mismatch
    .option("header", "true")
    .csv("/Volumes/statestreet/securities_master/raw_files/product.csv"))
```
