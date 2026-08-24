-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Silver Conformance — Securities Master (Pass-Through)
-- MAGIC
-- MAGIC Copies all Bronze tables into the Silver schema.
-- MAGIC SCD2 columns (effective_start_date, effective_end_date, is_current) added for all tables.
-- MAGIC DQ validation skipped — data flows directly from Bronze to Silver.
-- MAGIC
-- MAGIC **Layer:** Silver
-- MAGIC **Source schema:** statestreet.b_statestreet
-- MAGIC **Target schema:** statestreet.s_statestreet

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS statestreet.s_statestreet")
-- MAGIC print("Silver schema ready.")

-- COMMAND ----------

-- MAGIC %md ## Product hierarchy

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.product
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.product;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.bond
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.bond;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.stock
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.stock;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.common_stock
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.common_stock;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.preferred_stock
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.preferred_stock;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.debt
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.debt;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.muni
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.muni;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.pool_backed_security
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.pool_backed_security;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.fund
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.fund;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.`right`
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.`right`;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.listed_derivative
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.listed_derivative;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.option
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.option;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.future
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.future;

-- COMMAND ----------

-- MAGIC %md ## Reference tables

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.legal_entity
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.legal_entity;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.currency
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.currency;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.series
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.series;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.tick_ladder_scale
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.tick_ladder_scale;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.tick
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.tick;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.identifiers
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.identifiers;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.classification
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.classification;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.product_rating
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.product_rating;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.product_rating_type
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.product_rating_type;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.coupon
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.coupon;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.principal_redemption_provision
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.principal_redemption_provision;

-- COMMAND ----------

-- MAGIC %md ## Bridge tables

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.listed_derivative_tick
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.listed_derivative_tick;

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.s_statestreet.debt_principal_redemption_provision
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.debt_principal_redemption_provision;

-- COMMAND ----------

-- MAGIC %md ## Summary

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tables = [
-- MAGIC   "product","bond","stock","common_stock","preferred_stock","debt","muni",
-- MAGIC   "pool_backed_security","fund","right","listed_derivative","option","future",
-- MAGIC   "legal_entity","currency","series","tick_ladder_scale","tick","identifiers",
-- MAGIC   "classification","product_rating","product_rating_type","coupon",
-- MAGIC   "principal_redemption_provision","listed_derivative_tick",
-- MAGIC   "debt_principal_redemption_provision"
-- MAGIC ]
-- MAGIC print(f"\n{'='*60}")
-- MAGIC print(f"  Silver Pass-Through Summary")
-- MAGIC print(f"{'='*60}")
-- MAGIC total, ok = 0, 0
-- MAGIC for t in tables:
-- MAGIC   tname = f"`{t}`" if t == "right" else t
-- MAGIC   try:
-- MAGIC     count = spark.sql(f"SELECT COUNT(*) AS n FROM statestreet.s_statestreet.{tname}").collect()[0]["n"]
-- MAGIC     print(f"  OK  {t:45} {count:>8} rows")
-- MAGIC     total += count
-- MAGIC     ok += 1
-- MAGIC   except Exception as e:
-- MAGIC     print(f"  ERR {t:45} {str(e)[:60]}")
-- MAGIC print(f"{'='*60}")
-- MAGIC print(f"  Tables OK: {ok}/{len(tables)}    Total rows: {total:,}")
-- MAGIC print(f"{'='*60}\n")
