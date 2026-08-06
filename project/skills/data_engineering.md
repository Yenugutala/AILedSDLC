# Data Engineering Patterns — Databricks / Delta Lake

Complete reference for Bronze → Silver → Gold ingestion patterns used in this project.

---

## 1. Source Types & When to Use Each

| Source Type | Use Case | `request.yaml` `source.type` |
|-------------|----------|-------------------------------|
| Databricks Volume | CSV/Parquet files uploaded to workspace | `volume` |
| JDBC | SQL Server, Oracle, PostgreSQL, MySQL | `jdbc` |
| REST API | External web services returning JSON | `api` |
| Kafka | Event streams (securities events, price feeds) | `kafka` |
| Delta table | Read from another catalog/schema | `delta` |
| Cloud storage | Azure ADLS Gen2, AWS S3, GCS | `adls` / `s3` / `gcs` |

### request.yaml Source Config Examples

```yaml
# Volume (CSV) — default for this use case
source:
  type: volume
  path: /Volumes/statestreet/securities_master/raw_files/
  format: csv
  delimiter: ","
  header: true

# JDBC (SQL Server)
source:
  type: jdbc
  jdbc:
    url: jdbc:sqlserver://server.database.windows.net:1433;databaseName=Securities
    driver: com.microsoft.sqlserver.jdbc.SQLServerDriver
    user: svc_databricks
    password_secret: sml-secrets/sql-password   # Databricks secret scope
    dbtable: dbo.product
    fetch_size: 10000
    partition_column: product_id                 # parallel reads
    num_partitions: 8
    lower_bound: 1
    upper_bound: 1000000

# REST API with pagination
source:
  type: api
  api:
    url: https://api.securities.statestreet.com/v1/products
    method: GET
    auth_secret: sml-secrets/api-bearer-token
    pagination:
      type: page
      page_param: page
      page_size_param: limit
      page_size: 500
      max_pages: 200
    response_path: data.items

# Azure ADLS Gen2
source:
  type: adls
  path: abfss://securities@statestreetdl.dfs.core.windows.net/raw/
  format: parquet
  options:
    mergeSchema: "true"
```

---

## 2. Bronze Layer — Python/PySpark

### Core Ingestion Pattern

```python
# Read CSV from Unity Catalog Volume
df = (spark.read
    .option("header",      "true")
    .option("inferSchema", "true")
    .option("delimiter",   ",")
    .option("nullValue",   "")       # treat empty string as null
    .csv("/Volumes/statestreet/securities_master/raw_files/product.csv"))
```

### Add Metadata Columns (Always 4 standard cols)

```python
from pyspark.sql import functions as F

data_cols = [F.col(c) for c in df.columns]   # capture BEFORE adding metadata

df = (df
    .withColumn("_source_file",  F.lit("product.csv"))
    .withColumn("_source_type",  F.lit("volume"))
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_batch_id",     F.lit(batch_id))
    .withColumn("_row_hash",     F.sha2(F.concat_ws("|", *data_cols), 256)))
# _row_hash: SHA256 of all DATA columns — used in MERGE INTO to detect changes
```

### First Load vs Re-run (MERGE INTO for idempotency)

```python
full_table = "statestreet.b_statestreet.product"
pk         = ["product_id"]

# Check if table exists
try:
    spark.table(full_table)
    table_exists = True
except Exception:
    table_exists = False

if not table_exists:
    # First load — create table
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)
else:
    # Re-run — MERGE (only updates rows where data actually changed)
    view = "_incoming_product"
    df.createOrReplaceTempView(view)
    pk_join = " AND ".join([f"target.{k} = source.{k}" for k in pk])
    spark.sql(f"""
        MERGE INTO {full_table} AS target
        USING {view} AS source
        ON {pk_join}
        WHEN MATCHED AND source._row_hash != target._row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

### Enable Iceberg UniForm (Always — required for external readers)

```python
spark.sql(f"""
    ALTER TABLE {full_table}
    SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
""")
```

### Reading Multiple Formats

```python
# Parquet — simplest, no options needed
df = spark.read.parquet("/Volumes/statestreet/securities_master/raw_files/product.parquet")

# JSON (multi-line records)
df = spark.read.option("multiLine", "true").json("/path/to/product.json")

# Delta table (cross-catalog read)
df = spark.table("catalog_a.schema_a.product")

# JDBC with parallel partitioned read
df = (spark.read.format("jdbc")
    .option("url",             "jdbc:sqlserver://...")
    .option("dbtable",         "dbo.product")
    .option("user",            "svc_db")
    .option("password",        dbutils.secrets.get("sml-secrets", "sql-password"))
    .option("partitionColumn", "product_id")
    .option("numPartitions",   "8")
    .option("lowerBound",      "1")
    .option("upperBound",      "1000000")
    .option("fetchsize",       "10000")
    .load())
```

---

## 3. Silver Layer — SQL (Databricks SQL Dialect)

### DQ Rejects → Then Clean Silver (Always in this order)

```sql
-- Step 1: Write FAILING rows to rejects table (with rule metadata)
INSERT INTO statestreet.s_statestreet.product_rejects
SELECT *,
  'RULE0001'                         AS _rule_id,
  CONCAT('Invalid id_type: ', id_type) AS _violation_detail,
  current_timestamp()                AS _rejected_ts,
  '${dq_rule_version}'               AS _dq_rule_version
FROM statestreet.b_statestreet.product
WHERE id_type NOT IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID');

-- Step 2: MERGE passing rows to Silver with SCD2 columns
MERGE INTO statestreet.s_statestreet.product AS target
USING (
  SELECT *,
    current_date()        AS effective_start_date,
    DATE '9999-12-31'     AS effective_end_date,
    TRUE                  AS is_current,
    '${dq_rule_version}'  AS _dq_rule_version
  FROM statestreet.b_statestreet.product
  WHERE id_type IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID')
    AND status  IN ('ACTIVE','INACTIVE','MATURED','SUSPENDED','DELISTED')
) AS source
ON target.product_id = source.product_id AND target.is_current = TRUE
WHEN MATCHED AND source._row_hash != target._row_hash THEN
  UPDATE SET
    target.is_current         = FALSE,
    target.effective_end_date = current_date()
WHEN NOT MATCHED THEN INSERT *;
```

### Creating Silver Tables (First Run)

```sql
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product (
  product_id              STRING    NOT NULL,
  id_type                 STRING,
  type                    STRING,
  sub_type                STRING,
  status                  STRING,
  description             STRING,
  issue_date              DATE,
  effective_start_date    DATE      NOT NULL,
  effective_end_date      DATE      NOT NULL,
  is_current              BOOLEAN   NOT NULL,
  _dq_rule_version        STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING
)
USING DELTA
PARTITIONED BY (type)
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rejects (
  -- Same columns as product PLUS:
  _rule_id          STRING,
  _violation_detail STRING,
  _rejected_ts      TIMESTAMP,
  _dq_rule_version  STRING
)
USING DELTA;
```

---

## 4. Gold Layer — SQL (Dimensional Marts)

```sql
-- Gold: Full flattened dimension (all product subtypes via LEFT JOIN)
CREATE OR REPLACE TABLE statestreet.g_statestreet.dim_product
USING DELTA
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
PARTITIONED BY (type)
AS
SELECT
  p.product_id,
  p.id_type,
  p.type,
  p.sub_type,
  p.status,
  p.description,
  p.issue_date,
  p.issue_price,
  p.current_face_value,
  p.issuer_legal_entity_id,
  p.effective_start_date,
  p.effective_end_date,
  -- Stock-specific (NULL for non-stocks)
  cs.voting_rights,
  ps.dividend_type,
  -- Bond-specific (NULL for non-bonds)
  b.coupon_type,
  b.maturity_date,
  b.face_currency_code,
  mn.tax_exempt,
  -- Fund-specific
  f.endness_type,
  f.mutual_fund_type,
  -- Derivative-specific
  ld.underlying_product_id,
  op.option_type,
  op.exercise_style,
  ft.valuation_method
FROM statestreet.s_statestreet.product p
LEFT JOIN statestreet.s_statestreet.stock           st ON p.product_id = st.product_id
LEFT JOIN statestreet.s_statestreet.common_stock    cs ON p.product_id = cs.product_id
LEFT JOIN statestreet.s_statestreet.preferred_stock ps ON p.product_id = ps.product_id
LEFT JOIN statestreet.s_statestreet.bond             b ON p.product_id = b.product_id
LEFT JOIN statestreet.s_statestreet.muni            mn ON p.product_id = mn.product_id
LEFT JOIN statestreet.s_statestreet.fund             f ON p.product_id = f.product_id
LEFT JOIN statestreet.s_statestreet.listed_derivative ld ON p.product_id = ld.product_id
LEFT JOIN statestreet.s_statestreet.option          op ON p.product_id = op.product_id
LEFT JOIN statestreet.s_statestreet.future          ft ON p.product_id = ft.product_id
WHERE p.is_current = TRUE;
```

---

## 5. Unity Catalog Setup

```sql
-- Run once — notebook 01_setup_catalog.py
CREATE CATALOG IF NOT EXISTS statestreet;
USE CATALOG statestreet;

-- Schemas per layer
CREATE SCHEMA IF NOT EXISTS statestreet.b_statestreet
  COMMENT 'Bronze — raw landing layer';
CREATE SCHEMA IF NOT EXISTS statestreet.s_statestreet
  COMMENT 'Silver — DQ-conformed, SCD2';
CREATE SCHEMA IF NOT EXISTS statestreet.g_statestreet
  COMMENT 'Gold — dimensional marts for analytics';

-- Volume for source files
CREATE SCHEMA IF NOT EXISTS statestreet.securities_master;
CREATE VOLUME  IF NOT EXISTS statestreet.securities_master.raw_files
  COMMENT 'Source CSV files uploaded here before Bronze ingestion';
```

### Granting Permissions (Unity Catalog RBAC)

```sql
-- Data engineers: full write access to all layers
GRANT CREATE, MODIFY ON SCHEMA statestreet.b_statestreet TO `data-engineers`;
GRANT CREATE, MODIFY ON SCHEMA statestreet.s_statestreet TO `data-engineers`;
GRANT CREATE, MODIFY ON SCHEMA statestreet.g_statestreet TO `data-engineers`;

-- Analysts: read Gold only
GRANT SELECT ON SCHEMA statestreet.g_statestreet TO `data-analysts`;

-- Service account for pipelines
GRANT CREATE, MODIFY, SELECT ON SCHEMA statestreet.b_statestreet TO `svc-pipeline@statestreet.com`;

-- Read Volume (for Bronze ingestion)
GRANT READ VOLUME ON VOLUME statestreet.securities_master.raw_files TO `svc-pipeline@statestreet.com`;
```

---

## 6. Databricks SQL Dialect Rules

| Use This | Not This | Notes |
|----------|----------|-------|
| `current_timestamp()` | `NOW()`, `GETDATE()` | Spark function form always |
| `current_date()` | `CURRENT_DATE` | Same — use function form |
| `RLIKE` | `REGEXP_LIKE`, `~` | Databricks regex operator |
| `MERGE INTO` | `UPSERT`, `INSERT OR REPLACE` | Delta Lake standard |
| `TBLPROPERTIES` | N/A | Delta/Databricks extension |
| `DATE '9999-12-31'` | `'9999-12-31'` | Explicit DATE cast for SCD2 sentinel |
| `TRY_CAST(x AS INT)` | `CAST(x AS INT)` | Returns NULL on failure (safe) |
| `DATEADD(day, 1, date)` | `date + 1` | DATEADD works in Spark SQL |
| `DATEDIFF(end, start)` | `end - start` | Returns integer days |
| `%Y-%m-%d` | N/A | `date_format(col, 'yyyy-MM-dd')` |

---

## 7. DAB (Databricks Asset Bundles) — Deploy Pattern

```yaml
# databricks.yml
bundle:
  name: securities-master-lakehouse

variables:
  databricks_host:
    description: Databricks workspace URL
  catalog:
    default: statestreet

targets:
  dev:
    workspace:
      host: ${var.databricks_host}
    default: true
  prod:
    workspace:
      host: ${var.databricks_host}
    run_as:
      service_principal_name: svc-pipeline@statestreet.com

resources:
  jobs:
    orchestrate_pipeline_job:
      name: "[SML] Securities Master — Full Pipeline"
      tasks:
        - task_key: bronze_ingest
          notebook_task:
            notebook_path: notebooks/03_bronze_ingest.py
            base_parameters:
              use_case: securities-master
          job_cluster_key: pipeline_cluster
          timeout_seconds: 3600

        - task_key: silver_conform
          depends_on: [{task_key: bronze_ingest}]
          notebook_task:
            notebook_path: notebooks/04_silver_conform.sql
          job_cluster_key: pipeline_cluster

        - task_key: gold_build
          depends_on: [{task_key: silver_conform}]
          notebook_task:
            notebook_path: notebooks/05_gold_build.sql
          job_cluster_key: pipeline_cluster

  job_clusters:
    - job_cluster_key: pipeline_cluster
      new_cluster:
        spark_version: 15.4.x-scala2.12
        node_type_id: Standard_DS3_v2
        num_workers: 2
        autotermination_minutes: 30
        spark_conf:
          spark.databricks.delta.schema.autoMerge.enabled: "true"
```

```bash
# Deploy commands
databricks bundle validate         # Validate YAML before deploy
databricks bundle deploy           # Deploy to workspace
databricks bundle run orchestrate_pipeline_job  # Trigger a run
databricks bundle run --restart-on-failure orchestrate_pipeline_job
```

---

## 8. Getting Local Files into Databricks

Databricks runs in the cloud — it **cannot** access your local laptop directly.

### Option A — Databricks UI (easiest for demo)
```
Catalog → statestreet → securities_master → raw_files → Upload
```
Drag & drop all 29 CSVs. They land at `/Volumes/statestreet/securities_master/raw_files/`.

### Option B — Databricks CLI
```bash
pip install databricks-cli
databricks configure --token    # enter workspace URL + PAT token

# Upload all CSVs
for f in /local/path/*.csv; do
  databricks fs cp "$f" "dbfs:/Volumes/statestreet/securities_master/raw_files/"
done
```

### Option C — Notebook 02 with ZIP
1. Upload `securities_master_data.zip` via Databricks UI (Files → Upload)
2. Run `02_upload_raw_files.py` with the zip path as a widget parameter

### Option D — Cloud Storage (Production)
Store CSVs in Azure ADLS Gen2 / AWS S3 → configure Unity Catalog External Location → set `source.type: adls` in `request.yaml` → no manual upload needed.

---

## 9. Best Practices Checklist

- [ ] Always add 5 metadata columns: `_source_file`, `_source_type`, `_ingestion_ts`, `_batch_id`, `_row_hash`
- [ ] Use `MERGE INTO` not `INSERT OVERWRITE` — safe to re-run without data loss
- [ ] Enable `mergeSchema=true` on all Bronze writes (handles additive drift silently)
- [ ] Enable Iceberg UniForm on every table (`delta.universalFormat.enabledFormats = iceberg`)
- [ ] Write DQ rejects BEFORE writing clean Silver rows — never the other way around
- [ ] Use 3-part Unity Catalog names everywhere: `catalog.schema.table`
- [ ] Use `DATE '9999-12-31'` as SCD2 open-ended sentinel (never NULL for `effective_end_date`)
- [ ] Store credentials in Databricks Secret Scopes — never hardcode passwords
- [ ] Use job clusters for pipelines (auto-terminate) — not all-purpose clusters
- [ ] Add `OPTIMIZE` + `VACUUM` as a scheduled maintenance job (not inline with ingestion)
