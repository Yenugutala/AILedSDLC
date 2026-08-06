# Streaming & CDC — Real-Time Ingestion Patterns

Reference for when and how to move from batch to streaming/CDC ingestion of securities data.

---

## 1. When to Use Batch vs Streaming vs CDC

| Pattern | Best For | Latency | Complexity |
|---------|----------|---------|------------|
| **Batch (current)** | Overnight file drops, daily CSV refresh | Hours | Low |
| **CDC (Change Data Capture)** | Database sources with change feeds (SQL Server CDC, Oracle LogMiner) | Minutes | Medium |
| **Micro-batch streaming** | Kafka topics, event streams | Seconds | High |
| **Delta Live Tables (DLT)** | Managed streaming pipeline with auto-scaling | Seconds | Low (managed) |

**Current securities-master setup:** Batch (daily CSV → Volume). Switch to CDC when:
- Source database enables CDC/replication feeds
- SLA requires < 1 hour latency
- Data volume grows beyond 1M rows/day (full reload becomes expensive)

---

## 2. CDC Pattern — SQL Server (Most Common for Securities)

SQL Server Change Data Capture (CDC) captures INSERT, UPDATE, DELETE operations as log records.

### Configure CDC on Source (DBA task)

```sql
-- On SQL Server — enable CDC for the securities database
EXEC sys.sp_cdc_enable_db;

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'product',
    @role_name     = N'cdc_reader',
    @captured_column_list = N'product_id, id_type, type, status, description, issue_date';
```

### Read CDC Feed in Databricks (Python/PySpark)

```python
# Read SQL Server CDC change table via JDBC
# CDC tables are named: cdc.<schema>_<table>_CT
cdc_df = (spark.read.format("jdbc")
    .option("url",      "jdbc:sqlserver://server:1433;database=Securities")
    .option("dbtable",  "cdc.dbo_product_CT")    # CDC change table
    .option("user",     "svc_databricks")
    .option("password", dbutils.secrets.get("sml-secrets", "sql-password"))
    .option("partitionColumn", "__$start_lsn")    # CDC sequence number
    .option("numPartitions",   "4")
    .option("lowerBound",      last_lsn)          # read only new changes
    .option("upperBound",      current_lsn)
    .load())

# CDC operation codes
# 1 = DELETE (before image)
# 2 = INSERT
# 3 = UPDATE (before image)
# 4 = UPDATE (after image)
from pyspark.sql import functions as F

inserts = cdc_df.filter(F.col("__$operation") == 2)
updates = cdc_df.filter(F.col("__$operation") == 4)   # after-image only
deletes = cdc_df.filter(F.col("__$operation") == 1)
```

### Apply CDC Changes to Bronze (MERGE INTO)

```python
# Combine inserts + updates → upsert to Bronze
upserts = inserts.union(updates).select(
    "product_id", "id_type", "type", "status", "description", "issue_date"
)
upserts = _add_metadata(upserts, "CDC:dbo.product", "jdbc", batch_id)
upserts.createOrReplaceTempView("cdc_upserts")

spark.sql("""
    MERGE INTO statestreet.b_statestreet.product AS target
    USING cdc_upserts AS source
    ON target.product_id = source.product_id
    WHEN MATCHED AND source._row_hash != target._row_hash THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Handle deletes — soft delete (preserve history)
if deletes.count() > 0:
    deletes.createOrReplaceTempView("cdc_deletes")
    spark.sql("""
        UPDATE statestreet.b_statestreet.product
        SET status = 'DELETED', _batch_id = '{batch_id}'
        WHERE product_id IN (SELECT product_id FROM cdc_deletes)
    """.format(batch_id=batch_id))
```

### Save LSN Watermark (Checkpoint)

```python
# Store the last processed LSN so next run picks up from here
spark.sql(f"""
    MERGE INTO statestreet.b_statestreet._cdc_checkpoint AS target
    USING (SELECT 'dbo.product' AS table_name, '{current_lsn}' AS last_lsn,
                  current_timestamp() AS updated_at) AS source
    ON target.table_name = source.table_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

---

## 3. Kafka Streaming — Securities Price Feeds / Events

For real-time security price updates or corporate actions via Kafka:

### request.yaml for Kafka Source

```yaml
source:
  type: kafka
  kafka:
    bootstrap_servers: broker1.statestreet.com:9092,broker2.statestreet.com:9092
    topic: securities.master.product.updates
    group_id: sml-bronze-reader
    starting_offsets: latest
    value_format: json
    auth_secret: sml-secrets/kafka-sasl-password
    sasl_username: svc_databricks
```

### Batch Kafka Read (current pattern — source_reader.py)

```python
# Read Kafka topic as a batch (micro-batch — reads available messages, then stops)
kafka_df = (spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092")
    .option("subscribe", "securities.master.product.updates")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "100000")    # cap per batch
    .load())

# Deserialize JSON value
from pyspark.sql import functions as F

parsed = kafka_df.select(
    F.col("key").cast("string").alias("_kafka_key"),
    F.col("offset").alias("_kafka_offset"),
    F.col("timestamp").alias("_kafka_timestamp"),
    F.from_json(F.col("value").cast("string"), product_schema).alias("data")
).select("_kafka_key", "_kafka_offset", "_kafka_timestamp", "data.*")
```

### Streaming Kafka Read (Delta Live Tables — for continuous ingestion)

Use Delta Live Tables (DLT) for continuous streaming ingestion into Bronze.
See Databricks documentation for DLT pipeline configuration.

```python
# DLT Bronze streaming table (in a Delta Live Tables pipeline notebook)
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="bronze_product_stream",
    comment="Streaming Bronze from Kafka — securities product updates",
    table_properties={"delta.universalFormat.enabledFormats": "iceberg"}
)
def bronze_product_stream():
    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "securities.master.product.updates")
        .option("startingOffsets", "latest")
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), product_schema).alias("data"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.col("offset").alias("_kafka_offset"),
        )
        .select("data.*", "_kafka_timestamp", "_kafka_offset")
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit("streaming")))
```

---

## 4. _row_hash for CDC Change Detection

The `_row_hash` column (SHA256 of all data columns) enables efficient change detection
without comparing every column individually.

```python
# In Bronze MERGE INTO — only update rows where something actually changed
"""
MERGE INTO statestreet.b_statestreet.product AS target
USING incoming AS source
ON target.product_id = source.product_id
WHEN MATCHED AND source._row_hash != target._row_hash THEN
  UPDATE SET *                    -- only rows that actually changed
WHEN NOT MATCHED THEN
  INSERT *                        -- new rows
"""
# Result: for a 1M row table where only 500 rows changed, only 500 rows are written
# vs INSERT OVERWRITE which rewrites all 1M rows every run
```

---

## 5. Incremental Processing Pattern

When source files are large or arrive frequently, process only new/changed files:

```python
# Use Spark Auto Loader for incremental file discovery (Databricks)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/statestreet/securities_master/_schema/product")
    .option("header", "true")
    .load("/Volumes/statestreet/securities_master/raw_files/product/"))

# Auto Loader tracks which files have been processed — no duplicates
# Works with: Volume, S3, ADLS Gen2, GCS
# Schema evolution: automatically detects new columns (additive drift)
```

---

## 6. Choosing Between Batch and Streaming

Ask these questions:

1. **What is the required data freshness SLA?**
   - Daily: batch (current setup) ✓
   - Hourly: scheduled batch every hour
   - Minutes: Kafka micro-batch or DLT streaming
   - Seconds: DLT streaming with Kafka

2. **Does the source system support CDC?**
   - SQL Server: CDC + JDBC (section 2 above)
   - Oracle: LogMiner or Golden Gate
   - PostgreSQL: pglogical or Debezium → Kafka
   - CSV files: no CDC — use Auto Loader (section 5) for incremental

3. **Is full reload too expensive?**
   - < 1M rows: full reload is fine
   - 1M–10M rows: use `_row_hash` MERGE to skip unchanged rows
   - > 10M rows: switch to CDC or streaming ingestion

4. **Do you need exactly-once semantics?**
   - Batch with MERGE INTO: idempotent (safe to re-run)
   - Streaming: use DLT with checkpointing for exactly-once
   - Kafka: use consumer group offsets to avoid reprocessing

---

## 7. Migrating from Batch to CDC (Migration Path)

When you're ready to move from daily CSV to CDC:

```
Step 1: Enable CDC on source database (DBA)
Step 2: Update request.yaml: source.type: jdbc, add jdbc.cdc settings
Step 3: Run sml generate → agents re-generate Bronze code for CDC pattern
Step 4: Test in dev: verify LSN watermark, verify MERGE INTO correctness
Step 5: Deploy to staging, verify DQ pass rate same as batch
Step 6: Cutover in prod: run one final batch, then switch to CDC job on schedule
Step 7: Keep batch job available as fallback for first 2 weeks
```
