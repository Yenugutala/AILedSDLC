# Performance — Partitioning, Z-ordering, Liquid Clustering, and Optimization

Complete reference for query performance on Databricks Delta Lake tables.

---

## 1. Partitioning Strategy

Partitioning divides table data into directories by column value. Use it when:
- The partition column has **low to medium cardinality** (e.g. type: EQUITY/DEBT/FUND — 5 values)
- Queries **almost always filter** by that column
- Each partition will have **at least 1 GB** of data (avoid tiny partitions)

**Securities Master partitioning plan:**

| Layer | Table | Partition Column | Cardinality | Rationale |
|-------|-------|-----------------|-------------|-----------|
| Bronze | All tables | `_ingestion_date` (generated) | ~365 values/year | Prune by load date; easy partial reprocessing |
| Silver | `product` | `type` | 5 values | 90% of queries filter by product type |
| Silver | `product_rating` | year(`rating_date`) | ~10 | Time-series range queries |
| Silver | `coupon` | year(`payment_date`) | ~10 | Coupon schedule range queries |
| Gold | `dim_product` | `type` | 5 values | Consistent with Silver |
| Gold | `fact_product_rating` | year(`rating_date`) | ~10 | Analyst date-range queries |
| Gold | `fact_coupon_schedule` | year(`payment_date`) | ~10 | Coupon payment queries |

```sql
-- Static partitioning at create time
CREATE OR REPLACE TABLE statestreet.g_statestreet.dim_product
USING DELTA
PARTITIONED BY (type)           -- low cardinality column
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
AS SELECT ...

-- Generated column for date partitioning (avoids storing a duplicate column)
ALTER TABLE statestreet.b_statestreet.product
ADD COLUMNS (_ingestion_date DATE GENERATED ALWAYS AS (CAST(_ingestion_ts AS DATE)));

ALTER TABLE statestreet.b_statestreet.product
CLUSTER BY (_ingestion_date);   -- use liquid clustering after adding generated col
```

### Anti-patterns to avoid
- Partitioning on `product_id` (too many partitions — one per row)
- Partitioning on `_batch_id` (new partition every run — unbounded growth)
- Partitioning on columns never used in WHERE clauses (zero benefit, extra overhead)

---

## 2. Z-Ordering (Within-Partition Co-location)

Z-ordering co-locates related rows within each Parquet file — reduces data scanned per query.
Use for high-cardinality columns that are frequently filtered but **should not be partition columns**.

```sql
-- Z-order Silver product by frequently-filtered columns
OPTIMIZE statestreet.s_statestreet.product
ZORDER BY (status, issuer_legal_entity_id);

-- Z-order fact table for analyst joins
OPTIMIZE statestreet.g_statestreet.fact_product_rating
ZORDER BY (product_id, rating_date);

-- Z-order coupon schedule for date range queries
OPTIMIZE statestreet.g_statestreet.fact_coupon_schedule
ZORDER BY (product_id, payment_date);
```

**Rule:** Z-ORDER only columns you filter on frequently. More than 4–5 columns gives diminishing returns.

---

## 3. Liquid Clustering (Delta 3.0+ — Recommended for New Tables)

Liquid clustering is dynamic — it re-clusters automatically as data changes.
Better than static partitioning when:
- Access patterns evolve over time
- Data is skewed (unequal distribution across partition values)
- You need to cluster on multiple columns simultaneously

```sql
-- Create with liquid clustering (no PARTITIONED BY)
CREATE OR REPLACE TABLE statestreet.g_statestreet.dim_product
USING DELTA
CLUSTER BY (type, status)
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
AS SELECT ...

-- Run clustering (like OPTIMIZE but for liquid)
OPTIMIZE statestreet.g_statestreet.dim_product;

-- Check clustering state
SELECT * FROM (
  DESCRIBE DETAIL statestreet.g_statestreet.dim_product
) -- look at clusteringColumns field
```

**Choose liquid clustering for:** Gold marts, large Silver tables, tables with multi-column filters.
**Choose static partitioning for:** Bronze (by load date), Silver (by type) where cardinality is known and stable.

---

## 4. Delta UniForm Iceberg

Required for Iceberg-native readers (Apache Spark outside Databricks, Trino, Athena, BigQuery Omni).

```sql
-- Enable at table creation
CREATE TABLE statestreet.b_statestreet.product (...)
USING DELTA
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

-- Or enable on existing table
ALTER TABLE statestreet.b_statestreet.product
SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

-- Verify
DESCRIBE DETAIL statestreet.b_statestreet.product;
-- Look for: tableProperties -> delta.universalFormat.enabledFormats = iceberg

-- Check Iceberg metadata sync status
SHOW TBLPROPERTIES statestreet.b_statestreet.product;
```

**Note:** UniForm generates Iceberg metadata in `_delta_log/` — it does NOT copy data.
Both Delta and Iceberg readers see the same underlying Parquet files.

---

## 5. Auto-Optimize (Databricks Managed Compaction)

Small file compaction runs automatically after writes when these properties are set.

```sql
-- Enable on Bronze tables (high write frequency → many small files)
ALTER TABLE statestreet.b_statestreet.product
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',   -- bin-pack on write
  'delta.autoOptimize.autoCompact'   = 'true'    -- compact small files async
);

-- Check file stats
DESCRIBE DETAIL statestreet.b_statestreet.product;
-- numFiles should be low — if > 1000, run manual OPTIMIZE
```

---

## 6. OPTIMIZE and VACUUM (Maintenance Jobs)

Run these on a **schedule** (daily/weekly) — never inline with ingestion.

```sql
-- Compact small files into larger Parquet files
OPTIMIZE statestreet.s_statestreet.product;

-- Compact + Z-order together
OPTIMIZE statestreet.s_statestreet.product
ZORDER BY (status, issuer_legal_entity_id);

-- Remove old versions (default 7-day retention — comply with your data retention policy)
VACUUM statestreet.s_statestreet.product RETAIN 168 HOURS;  -- 7 days

-- Do NOT run VACUUM with 0 hours (disables time travel, causes failures on concurrent reads)
-- VACUUM ... RETAIN 0 HOURS;  -- DANGEROUS — never do this
```

### Maintenance job in DAB

```yaml
# resources/jobs/maintenance_job.yml
resources:
  jobs:
    maintenance_job:
      name: "[SML] Weekly Table Maintenance"
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"  # 2am daily
        timezone_id: "UTC"
      tasks:
        - task_key: optimize_all
          notebook_task:
            notebook_path: notebooks/99_maintenance.sql
```

---

## 7. Delta Time Travel

```sql
-- Query a previous version of a table
SELECT * FROM statestreet.s_statestreet.product VERSION AS OF 10;
SELECT * FROM statestreet.s_statestreet.product TIMESTAMP AS OF '2024-01-15 00:00:00';

-- Restore to a previous version (careful — irreversible without VACUUM)
RESTORE TABLE statestreet.s_statestreet.product TO VERSION AS OF 5;

-- Show change history
DESCRIBE HISTORY statestreet.s_statestreet.product;

-- Show all versions with metadata
SELECT version, timestamp, operation, operationMetrics
FROM (DESCRIBE HISTORY statestreet.s_statestreet.product);
```

**Use cases:** Debugging bad ingestion runs, auditing, point-in-time recovery.
**Constraint:** Versions older than `VACUUM` retention threshold are deleted.

---

## 8. Compute Sizing (Securities Master Sample)

| Job | Cluster Type | Nodes | Notes |
|-----|-------------|-------|-------|
| Bronze ingest (29 CSVs, 200 rows each) | Job cluster | 2 workers | Small dataset — single node would work |
| Silver conform (128 DQ rules) | Job cluster | 2 workers | SQL parallelism helps here |
| Gold mart build (4 LEFT JOINs) | Job cluster | 2 workers | JOIN-heavy — more workers help at scale |
| Genie queries (interactive) | SQL Warehouse (Serverless) | Auto | Use serverless — scales to zero |
| Development / debugging | All-purpose (auto-terminate 10 min) | 1–2 | Never leave running |

```yaml
# Job cluster config in databricks.yml
new_cluster:
  spark_version: 15.4.x-scala2.12    # Use latest LTS
  node_type_id: Standard_DS3_v2       # 14 GB RAM, 4 cores — good all-around
  num_workers: 2
  autotermination_minutes: 30         # Terminate after 30 min idle
  spark_conf:
    spark.databricks.delta.schema.autoMerge.enabled: "true"
    spark.sql.adaptive.enabled: "true"                  # AQE — auto-optimizes joins
    spark.sql.adaptive.coalescePartitions.enabled: "true"
```

---

## 9. Caching (Selective — Use Sparingly)

```python
# Cache a DataFrame that is used multiple times in the same job
df = spark.table("statestreet.s_statestreet.product").filter("is_current = TRUE").cache()
df.count()  # materialize cache

# Use df for multiple downstream operations...

df.unpersist()  # always unpersist when done
```

**When to cache:** Same table used 3+ times in a single job session.
**When NOT to cache:** Large tables (> cluster memory), tables read only once, streaming tables.

---

## 10. Performance Targets (Sample Dataset — 200 securities)

| Operation | Expected Time | Bottleneck |
|-----------|--------------|-----------|
| Bronze ingest (200 rows × 29 tables) | < 2 min | Volume read + Delta write |
| Silver conformance (128 DQ rules) | < 5 min | 128 SQL checks + SCD2 MERGE |
| Gold mart build (4 marts) | < 3 min | LEFT JOINs |
| Full pipeline (01→05 including setup) | < 15 min | Cluster startup dominates |
| Genie query response | < 3 sec | Serverless SQL warehouse |

**At production scale (100K+ securities):**
- Bronze: partition by `_ingestion_date` → skip already-loaded partitions
- Silver: use `num_partitions=8` JDBC reads if source is relational
- Gold: use `OPTIMIZE` weekly + Z-ORDER on `product_id`
- All: enable Adaptive Query Execution (AQE) — already set in cluster config above

---

## 11. Monitoring Query Performance

```python
# In a notebook — show Spark UI for the last operation
spark.sparkContext.uiWebUrl  # → open in browser

# Get execution plan (useful for debugging slow JOINs)
df.explain(mode="formatted")

# Check table stats (used by Spark query optimizer)
ANALYZE TABLE statestreet.s_statestreet.product COMPUTE STATISTICS;
ANALYZE TABLE statestreet.s_statestreet.product COMPUTE STATISTICS FOR COLUMNS product_id, type, status;
```
