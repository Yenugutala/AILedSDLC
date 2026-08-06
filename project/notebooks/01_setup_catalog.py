# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Setup Unity Catalog
# MAGIC Creates: catalog `statestreet`, schemas `b_statestreet` / `s_statestreet` / `g_statestreet`, Volume `raw_files`

# COMMAND ----------
CATALOG = "statestreet"
BRONZE_SCHEMA  = "b_statestreet"
SILVER_SCHEMA  = "s_statestreet"
GOLD_SCHEMA    = "g_statestreet"
VOLUME_SCHEMA  = "securities_master"
VOLUME_NAME    = "raw_files"

# COMMAND ----------
# MAGIC %md ## Create Catalog

# COMMAND ----------
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✓ Catalog: {CATALOG}")

# COMMAND ----------
# MAGIC %md ## Create Schemas

# COMMAND ----------
for schema in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, VOLUME_SCHEMA]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"✓ Schema: {CATALOG}.{schema}")

# COMMAND ----------
# MAGIC %md ## Create Volume (for raw CSV files)

# COMMAND ----------
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME}")
print(f"✓ Volume: /Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}/")

# COMMAND ----------
# MAGIC %md ## Create Schema Drift Audit Tables (Bronze)

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}._schema_changes (
        table_name STRING,
        change_type STRING,
        columns_affected STRING,
        detected_at TIMESTAMP
    )
    USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}._schema_quarantine (
        batch_id STRING,
        table_name STRING,
        change_type STRING,
        columns_affected STRING,
        quarantined_at TIMESTAMP
    )
    USING DELTA
""")
print(f"✓ Schema drift tables created in {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------
# MAGIC %md ## Create Audit Log (Gold)

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.audit_log (
        run_id STRING,
        use_case_name STRING,
        job_name STRING,
        stage STRING,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        rows_read LONG,
        rows_written LONG,
        rows_rejected LONG,
        status STRING,
        triggered_by STRING
    )
    USING DELTA
    TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
""")
print(f"✓ Audit log table: {CATALOG}.{GOLD_SCHEMA}.audit_log")

# COMMAND ----------
print("\n✅ Setup complete!")
print(f"  Catalog:   {CATALOG}")
print(f"  Bronze:    {CATALOG}.{BRONZE_SCHEMA}")
print(f"  Silver:    {CATALOG}.{SILVER_SCHEMA}")
print(f"  Gold:      {CATALOG}.{GOLD_SCHEMA}")
print(f"  Volume:    /Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}/")
print("\nNext: Run notebook 02_upload_raw_files.py")
