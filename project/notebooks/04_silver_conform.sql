-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer Conformance — Securities Master
-- MAGIC
-- MAGIC **Schema:** `statestreet.s_statestreet`
-- MAGIC
-- MAGIC **Pipeline:**
-- MAGIC 1. Create Silver tables (if not exists)
-- MAGIC 2. Apply DQ rules → route failing rows to `_rejects` tables
-- MAGIC 3. MERGE passing rows into Silver with SCD2 columns
-- MAGIC
-- MAGIC **DQ Rule Version:** Computed as SHA256 of `specs/silver/rules.yaml` at deploy time.
-- MAGIC Set via widget `dq_rule_version` (injected by the orchestrator job).

-- COMMAND ----------
-- MAGIC %md ## 0. Initialise — widgets and helper view

-- COMMAND ----------

-- Widget: DQ rule version (SHA256 of rules.yaml — injected by orchestrator)
-- Default value shown here is a placeholder; the job always passes the real hash.
-- CREATE WIDGET TEXT dq_rule_version DEFAULT 'auto';

-- COMMAND ----------
-- MAGIC %md ## 1. Create Silver Tables (IF NOT EXISTS)
-- MAGIC
-- MAGIC All 27 security tables + their `_rejects` counterparts.
-- MAGIC SCD2 tables: `product`, `legal_entity`, `product_rating`
-- MAGIC SCD1 tables: all others

-- COMMAND ----------
-- ── product ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product (
  product_id                 STRING    NOT NULL,
  id_type                    STRING,
  type                       STRING,
  sub_type                   STRING,
  status                     STRING,
  settlement_type            STRING,
  description                STRING,
  issue_date                 DATE,
  issue_price                DECIMAL(28,8),
  current_face_value         DECIMAL(28,8),
  issuer_legal_entity_id     STRING,
  tick_ladder_scale_id       STRING,
  -- SCD2
  effective_start_date       DATE      NOT NULL,
  effective_end_date         DATE      NOT NULL,
  is_current                 BOOLEAN   NOT NULL,
  -- metadata
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
PARTITIONED BY (type)
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rejects (
  product_id                 STRING,
  id_type                    STRING,
  type                       STRING,
  sub_type                   STRING,
  status                     STRING,
  settlement_type            STRING,
  description                STRING,
  issue_date                 DATE,
  issue_price                DECIMAL(28,8),
  current_face_value         DECIMAL(28,8),
  issuer_legal_entity_id     STRING,
  tick_ladder_scale_id       STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _rule_id                   STRING,
  _violation_detail          STRING,
  _rejected_ts               TIMESTAMP,
  _dq_rule_version           STRING
)
USING DELTA;

-- COMMAND ----------
-- ── bond ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.bond (
  product_id                 STRING    NOT NULL,
  issue_currency_code        STRING,
  coupon_type                STRING,
  maturity_date              DATE,
  reference_index_rate       STRING,
  conversion_rule            STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.bond_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING) AS product_id,
  CAST(NULL AS STRING) AS issue_currency_code,
  CAST(NULL AS STRING) AS coupon_type,
  CAST(NULL AS DATE)   AS maturity_date,
  CAST(NULL AS STRING) AS reference_index_rate,
  CAST(NULL AS STRING) AS conversion_rule,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING) AS _source_file,
  CAST(NULL AS STRING) AS _batch_id,
  CAST(NULL AS STRING) AS _row_hash,
  CAST(NULL AS STRING) AS _rule_id,
  CAST(NULL AS STRING) AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING) AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── stock ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.stock (
  product_id                 STRING    NOT NULL,
  series_id                  STRING,
  has_voting_rights          STRING,
  depository_type            STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.stock_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING) AS product_id,
  CAST(NULL AS STRING) AS series_id,
  CAST(NULL AS STRING) AS has_voting_rights,
  CAST(NULL AS STRING) AS depository_type,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING) AS _source_file,
  CAST(NULL AS STRING) AS _batch_id,
  CAST(NULL AS STRING) AS _row_hash,
  CAST(NULL AS STRING) AS _rule_id,
  CAST(NULL AS STRING) AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING) AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── common_stock ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.common_stock (
  product_id                 STRING    NOT NULL,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.common_stock_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING) AS product_id,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING) AS _source_file,
  CAST(NULL AS STRING) AS _batch_id,
  CAST(NULL AS STRING) AS _row_hash,
  CAST(NULL AS STRING) AS _rule_id,
  CAST(NULL AS STRING) AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING) AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── preferred_stock ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.preferred_stock (
  product_id                 STRING    NOT NULL,
  dividend_right             STRING,
  par_value                  DECIMAL(28,8),
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.preferred_stock_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)     AS product_id,
  CAST(NULL AS STRING)     AS dividend_right,
  CAST(NULL AS DECIMAL(28,8)) AS par_value,
  CAST(NULL AS TIMESTAMP)  AS _ingestion_ts,
  CAST(NULL AS STRING)     AS _source_file,
  CAST(NULL AS STRING)     AS _batch_id,
  CAST(NULL AS STRING)     AS _row_hash,
  CAST(NULL AS STRING)     AS _rule_id,
  CAST(NULL AS STRING)     AS _violation_detail,
  CAST(NULL AS TIMESTAMP)  AS _rejected_ts,
  CAST(NULL AS STRING)     AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── debt ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.debt (
  product_id                 STRING    NOT NULL,
  total_amount_issued        DECIMAL(28,8),
  par_value                  DECIMAL(28,8),
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.debt_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)        AS product_id,
  CAST(NULL AS DECIMAL(28,8)) AS total_amount_issued,
  CAST(NULL AS DECIMAL(28,8)) AS par_value,
  CAST(NULL AS TIMESTAMP)     AS _ingestion_ts,
  CAST(NULL AS STRING)        AS _source_file,
  CAST(NULL AS STRING)        AS _batch_id,
  CAST(NULL AS STRING)        AS _row_hash,
  CAST(NULL AS STRING)        AS _rule_id,
  CAST(NULL AS STRING)        AS _violation_detail,
  CAST(NULL AS TIMESTAMP)     AS _rejected_ts,
  CAST(NULL AS STRING)        AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── muni ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.muni (
  product_id                 STRING    NOT NULL,
  pledge_type                STRING,
  tax_exempt                 BOOLEAN,
  state                      STRING,
  purpose                    STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.muni_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS pledge_type,
  CAST(NULL AS BOOLEAN)   AS tax_exempt,
  CAST(NULL AS STRING)    AS state,
  CAST(NULL AS STRING)    AS purpose,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── pool_backed_security ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.pool_backed_security (
  product_id                 STRING    NOT NULL,
  pool_type                  STRING,
  originator                 STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.pool_backed_security_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS pool_type,
  CAST(NULL AS STRING)    AS originator,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── fund ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.fund (
  product_id                 STRING    NOT NULL,
  endness_type               STRING,
  mutual_fund_type           STRING,
  mutual_fund_load_type      STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.fund_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS endness_type,
  CAST(NULL AS STRING)    AS mutual_fund_type,
  CAST(NULL AS STRING)    AS mutual_fund_load_type,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── right ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.right (
  product_id                 STRING    NOT NULL,
  exercise_style             STRING,
  option_type                STRING,
  strike_price               DECIMAL(28,8),
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.right_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)        AS product_id,
  CAST(NULL AS STRING)        AS exercise_style,
  CAST(NULL AS STRING)        AS option_type,
  CAST(NULL AS DECIMAL(28,8)) AS strike_price,
  CAST(NULL AS TIMESTAMP)     AS _ingestion_ts,
  CAST(NULL AS STRING)        AS _source_file,
  CAST(NULL AS STRING)        AS _batch_id,
  CAST(NULL AS STRING)        AS _row_hash,
  CAST(NULL AS STRING)        AS _rule_id,
  CAST(NULL AS STRING)        AS _violation_detail,
  CAST(NULL AS TIMESTAMP)     AS _rejected_ts,
  CAST(NULL AS STRING)        AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── listed_derivative ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.listed_derivative (
  product_id                 STRING    NOT NULL,
  series_id                  STRING,
  underlying_product_id      STRING,
  contract_month             INT,
  last_trade_date            DATE,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.listed_derivative_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS series_id,
  CAST(NULL AS STRING)    AS underlying_product_id,
  CAST(NULL AS INT)       AS contract_month,
  CAST(NULL AS DATE)      AS last_trade_date,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── option ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.option (
  product_id                 STRING    NOT NULL,
  option_type                STRING,
  exercise_style             STRING,
  margin_style               STRING,
  strike_price               DECIMAL(28,8),
  strike_currency_code       STRING,
  expiry_date                DATE,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.option_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)        AS product_id,
  CAST(NULL AS STRING)        AS option_type,
  CAST(NULL AS STRING)        AS exercise_style,
  CAST(NULL AS STRING)        AS margin_style,
  CAST(NULL AS DECIMAL(28,8)) AS strike_price,
  CAST(NULL AS STRING)        AS strike_currency_code,
  CAST(NULL AS DATE)          AS expiry_date,
  CAST(NULL AS TIMESTAMP)     AS _ingestion_ts,
  CAST(NULL AS STRING)        AS _source_file,
  CAST(NULL AS STRING)        AS _batch_id,
  CAST(NULL AS STRING)        AS _row_hash,
  CAST(NULL AS STRING)        AS _rule_id,
  CAST(NULL AS STRING)        AS _violation_detail,
  CAST(NULL AS TIMESTAMP)     AS _rejected_ts,
  CAST(NULL AS STRING)        AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── future ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.future (
  product_id                      STRING    NOT NULL,
  valuation_method                STRING,
  delivery_date                   DATE,
  first_delivery_datetime_utc     TIMESTAMP,
  last_delivery_datetime_utc      TIMESTAMP,
  _ingestion_ts                   TIMESTAMP,
  _source_file                    STRING,
  _batch_id                       STRING,
  _row_hash                       STRING,
  _dq_rule_version                STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.future_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS valuation_method,
  CAST(NULL AS DATE)      AS delivery_date,
  CAST(NULL AS TIMESTAMP) AS first_delivery_datetime_utc,
  CAST(NULL AS TIMESTAMP) AS last_delivery_datetime_utc,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── legal_entity (SCD2) ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.legal_entity (
  legal_entity_id            STRING    NOT NULL,
  legal_name                 STRING,
  legal_structure            STRING,
  country                    STRING,
  formation_date             DATE,
  -- SCD2
  effective_start_date       DATE      NOT NULL,
  effective_end_date         DATE      NOT NULL,
  is_current                 BOOLEAN   NOT NULL,
  -- metadata
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.legal_entity_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS legal_entity_id,
  CAST(NULL AS STRING)    AS legal_name,
  CAST(NULL AS STRING)    AS legal_structure,
  CAST(NULL AS STRING)    AS country,
  CAST(NULL AS DATE)      AS formation_date,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── currency ─────────────────────────────────────────────────────────────────
-- NOTE: 2 bad rows expected (seeded DQ issues) → they go to currency_rejects
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.currency (
  code                       STRING    NOT NULL,
  name                       STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.currency_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS code,
  CAST(NULL AS STRING)    AS name,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── series ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.series (
  series_id                  STRING    NOT NULL,
  series_name                STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.series_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS series_id,
  CAST(NULL AS STRING)    AS series_name,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── tick_ladder_scale ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.tick_ladder_scale (
  tick_ladder_scale_id       STRING    NOT NULL,
  tick_size                  DECIMAL(28,8),
  description                STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.tick_ladder_scale_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)        AS tick_ladder_scale_id,
  CAST(NULL AS DECIMAL(28,8)) AS tick_size,
  CAST(NULL AS STRING)        AS description,
  CAST(NULL AS TIMESTAMP)     AS _ingestion_ts,
  CAST(NULL AS STRING)        AS _source_file,
  CAST(NULL AS STRING)        AS _batch_id,
  CAST(NULL AS STRING)        AS _row_hash,
  CAST(NULL AS STRING)        AS _rule_id,
  CAST(NULL AS STRING)        AS _violation_detail,
  CAST(NULL AS TIMESTAMP)     AS _rejected_ts,
  CAST(NULL AS STRING)        AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── tick ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.tick (
  tick_id                    STRING    NOT NULL,
  tick_ladder_scale_id       STRING,
  tick_size                  DECIMAL(28,8),
  price_range                STRING,
  tick_currency_code         STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.tick_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)        AS tick_id,
  CAST(NULL AS STRING)        AS tick_ladder_scale_id,
  CAST(NULL AS DECIMAL(28,8)) AS tick_size,
  CAST(NULL AS STRING)        AS price_range,
  CAST(NULL AS STRING)        AS tick_currency_code,
  CAST(NULL AS TIMESTAMP)     AS _ingestion_ts,
  CAST(NULL AS STRING)        AS _source_file,
  CAST(NULL AS STRING)        AS _batch_id,
  CAST(NULL AS STRING)        AS _row_hash,
  CAST(NULL AS STRING)        AS _rule_id,
  CAST(NULL AS STRING)        AS _violation_detail,
  CAST(NULL AS TIMESTAMP)     AS _rejected_ts,
  CAST(NULL AS STRING)        AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── identifiers ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.identifiers (
  identifiers_id             STRING    NOT NULL,
  product_id                 STRING,
  cusip                      STRING,
  isin                       STRING,
  sedol                      STRING,
  ticker                     STRING,
  bloomberg_id               STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
PARTITIONED BY (product_id)
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.identifiers_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS identifiers_id,
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS cusip,
  CAST(NULL AS STRING)    AS isin,
  CAST(NULL AS STRING)    AS sedol,
  CAST(NULL AS STRING)    AS ticker,
  CAST(NULL AS STRING)    AS bloomberg_id,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── classification ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.classification (
  classification_id          STRING    NOT NULL,
  product_id                 STRING,
  classification_type        STRING,
  classification_value       STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.classification_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS classification_id,
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS classification_type,
  CAST(NULL AS STRING)    AS classification_value,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── product_rating (SCD2) ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rating (
  product_rating_id          STRING    NOT NULL,
  product_id                 STRING,
  product_rating_type_id     STRING,
  rating_value               STRING,
  rating_agency              STRING,
  watch_code                 STRING,
  effective_from_date        DATE,
  -- SCD2
  effective_start_date       DATE      NOT NULL,
  effective_end_date         DATE      NOT NULL,
  is_current                 BOOLEAN   NOT NULL,
  -- metadata
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
PARTITIONED BY (effective_from_date)
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rating_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS product_rating_id,
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS product_rating_type_id,
  CAST(NULL AS STRING)    AS rating_value,
  CAST(NULL AS STRING)    AS rating_agency,
  CAST(NULL AS STRING)    AS watch_code,
  CAST(NULL AS DATE)      AS effective_from_date,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── product_rating_type ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rating_type (
  product_rating_type_id     STRING    NOT NULL,
  rating_agency              STRING,
  rating_scale               STRING,
  rating_type_code           STRING,
  description                STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rating_type_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS product_rating_type_id,
  CAST(NULL AS STRING)    AS rating_agency,
  CAST(NULL AS STRING)    AS rating_scale,
  CAST(NULL AS STRING)    AS rating_type_code,
  CAST(NULL AS STRING)    AS description,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── coupon ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.coupon (
  coupon_id                  STRING    NOT NULL,
  product_id                 STRING,
  coupon_rate                DECIMAL(10,6),
  payment_date               DATE,
  coupon_type                STRING,
  frequency                  STRING,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
PARTITIONED BY (payment_date)
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.coupon_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)        AS coupon_id,
  CAST(NULL AS STRING)        AS product_id,
  CAST(NULL AS DECIMAL(10,6)) AS coupon_rate,
  CAST(NULL AS DATE)          AS payment_date,
  CAST(NULL AS STRING)        AS coupon_type,
  CAST(NULL AS STRING)        AS frequency,
  CAST(NULL AS TIMESTAMP)     AS _ingestion_ts,
  CAST(NULL AS STRING)        AS _source_file,
  CAST(NULL AS STRING)        AS _batch_id,
  CAST(NULL AS STRING)        AS _row_hash,
  CAST(NULL AS STRING)        AS _rule_id,
  CAST(NULL AS STRING)        AS _violation_detail,
  CAST(NULL AS TIMESTAMP)     AS _rejected_ts,
  CAST(NULL AS STRING)        AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── principal_redemption_provision ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.principal_redemption_provision (
  principal_redemption_provision_id  STRING    NOT NULL,
  provision_type                     STRING,
  description                        STRING,
  _ingestion_ts                      TIMESTAMP,
  _source_file                       STRING,
  _batch_id                          STRING,
  _row_hash                          STRING,
  _dq_rule_version                   STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.principal_redemption_provision_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS principal_redemption_provision_id,
  CAST(NULL AS STRING)    AS provision_type,
  CAST(NULL AS STRING)    AS description,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── generic_product ───────────────────────────────────────────────────────────
-- NOTE: deprecated legacy table — no PK uniqueness DQ (many rows per product_id by design)
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.generic_product (
  generic_product_id               STRING    NOT NULL,
  product_id                       STRING,
  description                      STRING,
  status                           STRING,
  gs_legacy_prime_issue_currency   STRING,
  _ingestion_ts                    TIMESTAMP,
  _source_file                     STRING,
  _batch_id                        STRING,
  _row_hash                        STRING,
  _dq_rule_version                 STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.generic_product_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS generic_product_id,
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS description,
  CAST(NULL AS STRING)    AS status,
  CAST(NULL AS STRING)    AS gs_legacy_prime_issue_currency,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── listed_derivative_tick ────────────────────────────────────────────────────
-- Bridge M:M between listed_derivative and tick
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.listed_derivative_tick (
  product_id                 STRING    NOT NULL,
  tick_id                    STRING    NOT NULL,
  _ingestion_ts              TIMESTAMP,
  _source_file               STRING,
  _batch_id                  STRING,
  _row_hash                  STRING,
  _dq_rule_version           STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.listed_derivative_tick_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS tick_id,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- ── debt_principal_redemption_provision ──────────────────────────────────────
-- Bridge M:M between debt and principal_redemption_provision
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.debt_principal_redemption_provision (
  product_id                         STRING    NOT NULL,
  principal_redemption_provision_id  STRING    NOT NULL,
  _ingestion_ts                      TIMESTAMP,
  _source_file                       STRING,
  _batch_id                          STRING,
  _row_hash                          STRING,
  _dq_rule_version                   STRING
)
USING DELTA
TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.debt_principal_redemption_provision_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS principal_redemption_provision_id,
  CAST(NULL AS TIMESTAMP) AS _ingestion_ts,
  CAST(NULL AS STRING)    AS _source_file,
  CAST(NULL AS STRING)    AS _batch_id,
  CAST(NULL AS STRING)    AS _row_hash,
  CAST(NULL AS STRING)    AS _rule_id,
  CAST(NULL AS STRING)    AS _violation_detail,
  CAST(NULL AS TIMESTAMP) AS _rejected_ts,
  CAST(NULL AS STRING)    AS _dq_rule_version
WHERE 1 = 0;

-- COMMAND ----------
-- MAGIC %md ## 2. Apply DQ Rules — Route Failing Rows to _rejects
-- MAGIC
-- MAGIC For each table: detect rows failing DQ rules and INSERT into the corresponding _rejects table.
-- MAGIC Rows that pass all checks are merged into Silver in Section 3.

-- COMMAND ----------
-- ── product ───────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.product_rejects
SELECT
  product_id, id_type, type, sub_type, status, settlement_type, description,
  issue_date, issue_price, current_face_value, issuer_legal_entity_id, tick_ladder_scale_id,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL                                                                             THEN 'RULE0030'
    WHEN id_type IS NOT NULL AND id_type NOT IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID')        THEN 'RULE0001'
    WHEN type IS NOT NULL AND type NOT IN ('DEBT','EQUITY','FUND','DERIVATIVE','RIGHT')                 THEN 'RULE0002'
    WHEN status IS NOT NULL AND status NOT IN ('ACTIVE','INACTIVE','MATURED','SUSPENDED','DELISTED')    THEN 'RULE0003'
    WHEN settlement_type IS NOT NULL AND settlement_type NOT IN ('T+1','T+2','T+3','CASH')             THEN 'RULE0004'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL                                                                             THEN 'product_id IS NULL'
    WHEN id_type IS NOT NULL AND id_type NOT IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID')        THEN CONCAT('Invalid id_type: ', COALESCE(id_type,'null'))
    WHEN type IS NOT NULL AND type NOT IN ('DEBT','EQUITY','FUND','DERIVATIVE','RIGHT')                 THEN CONCAT('Invalid type: ', COALESCE(type,'null'))
    WHEN status IS NOT NULL AND status NOT IN ('ACTIVE','INACTIVE','MATURED','SUSPENDED','DELISTED')    THEN CONCAT('Invalid status: ', COALESCE(status,'null'))
    WHEN settlement_type IS NOT NULL AND settlement_type NOT IN ('T+1','T+2','T+3','CASH')             THEN CONCAT('Invalid settlement_type: ', COALESCE(settlement_type,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.product
WHERE product_id IS NULL
   OR (id_type IS NOT NULL AND id_type NOT IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID'))
   OR (type IS NOT NULL AND type NOT IN ('DEBT','EQUITY','FUND','DERIVATIVE','RIGHT'))
   OR (status IS NOT NULL AND status NOT IN ('ACTIVE','INACTIVE','MATURED','SUSPENDED','DELISTED'))
   OR (settlement_type IS NOT NULL AND settlement_type NOT IN ('T+1','T+2','T+3','CASH'));

-- COMMAND ----------
-- ── bond ──────────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.bond_rejects
SELECT
  product_id, issue_currency_code, coupon_type, maturity_date, reference_index_rate, conversion_rule,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL                                                                                              THEN 'RULE0030'
    WHEN reference_index_rate IS NOT NULL AND reference_index_rate NOT IN ('CPI','LIBOR','SOFR','RPI')                   THEN 'RULE0015'
    WHEN conversion_rule IS NOT NULL AND conversion_rule NOT IN ('MANDATORY','OPTIONAL','CONTINGENT','NONE')             THEN 'RULE0016'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL                                                                                              THEN 'product_id IS NULL'
    WHEN reference_index_rate IS NOT NULL AND reference_index_rate NOT IN ('CPI','LIBOR','SOFR','RPI')                   THEN CONCAT('Invalid reference_index_rate: ', COALESCE(reference_index_rate,'null'))
    WHEN conversion_rule IS NOT NULL AND conversion_rule NOT IN ('MANDATORY','OPTIONAL','CONTINGENT','NONE')             THEN CONCAT('Invalid conversion_rule: ', COALESCE(conversion_rule,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.bond
WHERE product_id IS NULL
   OR (reference_index_rate IS NOT NULL AND reference_index_rate NOT IN ('CPI','LIBOR','SOFR','RPI'))
   OR (conversion_rule IS NOT NULL AND conversion_rule NOT IN ('MANDATORY','OPTIONAL','CONTINGENT','NONE'));

-- COMMAND ----------
-- ── stock ─────────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.stock_rejects
SELECT
  product_id, series_id, has_voting_rights, depository_type,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL                                                                                      THEN 'RULE0030'
    WHEN depository_type IS NOT NULL AND depository_type NOT IN ('DTC','EUROCLEAR','CLEARSTREAM','PHYSICAL')     THEN 'RULE0018'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL                                                                                      THEN 'product_id IS NULL'
    WHEN depository_type IS NOT NULL AND depository_type NOT IN ('DTC','EUROCLEAR','CLEARSTREAM','PHYSICAL')     THEN CONCAT('Invalid depository_type: ', COALESCE(depository_type,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.stock
WHERE product_id IS NULL
   OR (depository_type IS NOT NULL AND depository_type NOT IN ('DTC','EUROCLEAR','CLEARSTREAM','PHYSICAL'));

-- COMMAND ----------
-- ── common_stock ──────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.common_stock_rejects
SELECT
  product_id,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0030' AS _rule_id,
  'product_id IS NULL' AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.common_stock
WHERE product_id IS NULL;

-- COMMAND ----------
-- ── preferred_stock ───────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.preferred_stock_rejects
SELECT
  product_id, dividend_right, par_value,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL                                                                                           THEN 'RULE0030'
    WHEN dividend_right IS NOT NULL AND dividend_right NOT IN ('CUMULATIVE','NON_CUMULATIVE','PARTICIPATING')         THEN 'RULE0019'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL                                                                                           THEN 'product_id IS NULL'
    WHEN dividend_right IS NOT NULL AND dividend_right NOT IN ('CUMULATIVE','NON_CUMULATIVE','PARTICIPATING')         THEN CONCAT('Invalid dividend_right: ', COALESCE(dividend_right,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.preferred_stock
WHERE product_id IS NULL
   OR (dividend_right IS NOT NULL AND dividend_right NOT IN ('CUMULATIVE','NON_CUMULATIVE','PARTICIPATING'));

-- COMMAND ----------
-- ── debt ──────────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.debt_rejects
SELECT
  product_id, total_amount_issued, par_value,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL             THEN 'RULE0030'
    WHEN total_amount_issued < 0        THEN 'RULE0076'
    WHEN par_value < 0                  THEN 'RULE0077'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL             THEN 'product_id IS NULL'
    WHEN total_amount_issued < 0        THEN CONCAT('total_amount_issued negative: ', CAST(total_amount_issued AS STRING))
    WHEN par_value < 0                  THEN CONCAT('par_value negative: ', CAST(par_value AS STRING))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.debt
WHERE product_id IS NULL
   OR total_amount_issued < 0
   OR par_value < 0;

-- COMMAND ----------
-- ── muni ──────────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.muni_rejects
SELECT
  product_id, pledge_type, tax_exempt, state, purpose,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL                                                                                            THEN 'RULE0030'
    WHEN pledge_type IS NOT NULL AND pledge_type NOT IN ('GENERAL_OBLIGATION','REVENUE','DOUBLE_BARRELED')             THEN 'RULE0017'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL                                                                                            THEN 'product_id IS NULL'
    WHEN pledge_type IS NOT NULL AND pledge_type NOT IN ('GENERAL_OBLIGATION','REVENUE','DOUBLE_BARRELED')             THEN CONCAT('Invalid pledge_type: ', COALESCE(pledge_type,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.muni
WHERE product_id IS NULL
   OR (pledge_type IS NOT NULL AND pledge_type NOT IN ('GENERAL_OBLIGATION','REVENUE','DOUBLE_BARRELED'));

-- COMMAND ----------
-- ── pool_backed_security ──────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.pool_backed_security_rejects
SELECT
  product_id, pool_type, originator,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0030' AS _rule_id,
  'product_id IS NULL' AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.pool_backed_security
WHERE product_id IS NULL;

-- COMMAND ----------
-- ── fund ──────────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.fund_rejects
SELECT
  product_id, endness_type, mutual_fund_type, mutual_fund_load_type,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL                                                                                                                    THEN 'RULE0030'
    WHEN endness_type IS NOT NULL AND endness_type NOT IN ('OPEN_END','CLOSED_END')                                                            THEN 'RULE0006'
    WHEN mutual_fund_load_type IS NOT NULL AND mutual_fund_load_type NOT IN ('FRONT_LOAD','BACK_LOAD','NO_LOAD','LEVEL_LOAD')                   THEN 'RULE0007'
    WHEN mutual_fund_type IS NOT NULL AND mutual_fund_type NOT IN ('EQUITY','FIXED_INCOME','BALANCED','MONEY_MARKET','ALTERNATIVE')             THEN 'RULE0008'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL                                                                                                                    THEN 'product_id IS NULL'
    WHEN endness_type IS NOT NULL AND endness_type NOT IN ('OPEN_END','CLOSED_END')                                                            THEN CONCAT('Invalid endness_type: ', COALESCE(endness_type,'null'))
    WHEN mutual_fund_load_type IS NOT NULL AND mutual_fund_load_type NOT IN ('FRONT_LOAD','BACK_LOAD','NO_LOAD','LEVEL_LOAD')                   THEN CONCAT('Invalid mutual_fund_load_type: ', COALESCE(mutual_fund_load_type,'null'))
    WHEN mutual_fund_type IS NOT NULL AND mutual_fund_type NOT IN ('EQUITY','FIXED_INCOME','BALANCED','MONEY_MARKET','ALTERNATIVE')             THEN CONCAT('Invalid mutual_fund_type: ', COALESCE(mutual_fund_type,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.fund
WHERE product_id IS NULL
   OR (endness_type IS NOT NULL AND endness_type NOT IN ('OPEN_END','CLOSED_END'))
   OR (mutual_fund_load_type IS NOT NULL AND mutual_fund_load_type NOT IN ('FRONT_LOAD','BACK_LOAD','NO_LOAD','LEVEL_LOAD'))
   OR (mutual_fund_type IS NOT NULL AND mutual_fund_type NOT IN ('EQUITY','FIXED_INCOME','BALANCED','MONEY_MARKET','ALTERNATIVE'));

-- COMMAND ----------
-- ── right ─────────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.right_rejects
SELECT
  product_id, exercise_style, option_type, strike_price,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL                                                                          THEN 'RULE0030'
    WHEN exercise_style IS NOT NULL AND exercise_style NOT IN ('AMERICAN','EUROPEAN','BERMUDAN')     THEN 'RULE0009'
    WHEN option_type IS NOT NULL AND option_type NOT IN ('CALL','PUT')                               THEN 'RULE0010'
    WHEN strike_price IS NOT NULL AND strike_price < 0                                               THEN 'RULE0078'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL                                                                          THEN 'product_id IS NULL'
    WHEN exercise_style IS NOT NULL AND exercise_style NOT IN ('AMERICAN','EUROPEAN','BERMUDAN')     THEN CONCAT('Invalid exercise_style: ', COALESCE(exercise_style,'null'))
    WHEN option_type IS NOT NULL AND option_type NOT IN ('CALL','PUT')                               THEN CONCAT('Invalid option_type: ', COALESCE(option_type,'null'))
    WHEN strike_price IS NOT NULL AND strike_price < 0                                               THEN CONCAT('strike_price negative: ', CAST(strike_price AS STRING))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.right
WHERE product_id IS NULL
   OR (exercise_style IS NOT NULL AND exercise_style NOT IN ('AMERICAN','EUROPEAN','BERMUDAN'))
   OR (option_type IS NOT NULL AND option_type NOT IN ('CALL','PUT'))
   OR (strike_price IS NOT NULL AND strike_price < 0);

-- COMMAND ----------
-- ── listed_derivative ─────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.listed_derivative_rejects
SELECT
  product_id, series_id, underlying_product_id, contract_month, last_trade_date,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL                              THEN 'RULE0030'
    WHEN contract_month IS NOT NULL AND (contract_month < 1 OR contract_month > 12) THEN 'RULE0084'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL                              THEN 'product_id IS NULL'
    WHEN contract_month IS NOT NULL AND (contract_month < 1 OR contract_month > 12) THEN CONCAT('contract_month out of range: ', CAST(contract_month AS STRING))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.listed_derivative
WHERE product_id IS NULL
   OR (contract_month IS NOT NULL AND (contract_month < 1 OR contract_month > 12));

-- COMMAND ----------
-- ── option ────────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.option_rejects
SELECT
  product_id, option_type, exercise_style, margin_style, strike_price, strike_currency_code, expiry_date,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_id IS NULL                                                                      THEN 'RULE0030'
    WHEN option_type NOT IN ('CALL','PUT')                                                        THEN 'RULE0011'
    WHEN exercise_style NOT IN ('AMERICAN','EUROPEAN','BERMUDAN')                                 THEN 'RULE0012'
    WHEN margin_style IS NOT NULL AND margin_style NOT IN ('PREMIUM','FUTURES_STYLE')             THEN 'RULE0013'
    WHEN strike_price IS NOT NULL AND strike_price < 0                                            THEN 'RULE0079'
  END AS _rule_id,
  CASE
    WHEN product_id IS NULL                                                                      THEN 'product_id IS NULL'
    WHEN option_type NOT IN ('CALL','PUT')                                                        THEN CONCAT('Invalid option_type: ', COALESCE(option_type,'null'))
    WHEN exercise_style NOT IN ('AMERICAN','EUROPEAN','BERMUDAN')                                 THEN CONCAT('Invalid exercise_style: ', COALESCE(exercise_style,'null'))
    WHEN margin_style IS NOT NULL AND margin_style NOT IN ('PREMIUM','FUTURES_STYLE')             THEN CONCAT('Invalid margin_style: ', COALESCE(margin_style,'null'))
    WHEN strike_price IS NOT NULL AND strike_price < 0                                            THEN CONCAT('strike_price negative: ', CAST(strike_price AS STRING))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.option
WHERE product_id IS NULL
   OR option_type NOT IN ('CALL','PUT')
   OR exercise_style NOT IN ('AMERICAN','EUROPEAN','BERMUDAN')
   OR (margin_style IS NOT NULL AND margin_style NOT IN ('PREMIUM','FUTURES_STYLE'))
   OR (strike_price IS NOT NULL AND strike_price < 0);

-- COMMAND ----------
-- ── future ────────────────────────────────────────────────────────────────────
-- Note: first_delivery_datetime_utc and last_delivery_datetime_utc may not exist in bronze CSV
INSERT INTO statestreet.s_statestreet.future_rejects
SELECT
  product_id, valuation_method, delivery_date,
  CAST(NULL AS TIMESTAMP) AS first_delivery_datetime_utc,
  CAST(NULL AS TIMESTAMP) AS last_delivery_datetime_utc,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0030' AS _rule_id,
  'product_id IS NULL' AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.future
WHERE product_id IS NULL;

-- COMMAND ----------
-- ── legal_entity ──────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.legal_entity_rejects
SELECT
  legal_entity_id, legal_name, legal_structure, country, formation_date,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN legal_entity_id IS NULL                                                                                 THEN 'RULE0030'
    WHEN legal_name IS NULL                                                                                      THEN 'RULE0031'
    WHEN legal_structure IS NOT NULL AND legal_structure NOT IN ('CORP','LLC','LP','TRUST','GOVERNMENT')         THEN 'RULE0005'
  END AS _rule_id,
  CASE
    WHEN legal_entity_id IS NULL                                                                                 THEN 'legal_entity_id IS NULL'
    WHEN legal_name IS NULL                                                                                      THEN 'legal_name IS NULL'
    WHEN legal_structure IS NOT NULL AND legal_structure NOT IN ('CORP','LLC','LP','TRUST','GOVERNMENT')         THEN CONCAT('Invalid legal_structure: ', COALESCE(legal_structure,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.legal_entity
WHERE legal_entity_id IS NULL
   OR legal_name IS NULL
   OR (legal_structure IS NOT NULL AND legal_structure NOT IN ('CORP','LLC','LP','TRUST','GOVERNMENT'));

-- COMMAND ----------
-- ── currency — 2 bad rows expected (seeded DQ issues, USE-CASE-002) ───────────
-- Note: bronze column names are currency_code, symbol; silver uses code, name
INSERT INTO statestreet.s_statestreet.currency_rejects
SELECT
  currency_code AS code,
  symbol AS name,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0095' AS _rule_id,
  CONCAT('Invalid currency_code format: ', COALESCE(currency_code,'null')) AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.currency
WHERE currency_code IS NULL
   OR LENGTH(currency_code) <> 3
   OR currency_code NOT RLIKE '^[A-Z]{3}$';

-- COMMAND ----------
-- ── series ────────────────────────────────────────────────────────────────────
-- Note: bronze column name is description; silver uses series_name
INSERT INTO statestreet.s_statestreet.series_rejects
SELECT
  series_id,
  description AS series_name,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0030' AS _rule_id,
  'series_id IS NULL' AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.series
WHERE series_id IS NULL;

-- COMMAND ----------
-- ── tick_ladder_scale ─────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.tick_ladder_scale_rejects
SELECT
  tick_ladder_scale_id, tick_size, description,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN tick_ladder_scale_id IS NULL              THEN 'RULE0030'
    WHEN tick_size IS NOT NULL AND tick_size <= 0  THEN 'RULE0082'
  END AS _rule_id,
  CASE
    WHEN tick_ladder_scale_id IS NULL              THEN 'tick_ladder_scale_id IS NULL'
    WHEN tick_size IS NOT NULL AND tick_size <= 0  THEN CONCAT('tick_size not positive: ', CAST(tick_size AS STRING))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.tick_ladder_scale
WHERE tick_ladder_scale_id IS NULL
   OR (tick_size IS NOT NULL AND tick_size <= 0);

-- COMMAND ----------
-- ── tick ──────────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.tick_rejects
SELECT
  tick_id, tick_ladder_scale_id, tick_size, price_range, tick_currency_code,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN tick_id IS NULL                                                                    THEN 'RULE0030'
    WHEN tick_size IS NOT NULL AND tick_size <= 0                                           THEN 'RULE0081'
    WHEN price_range IS NOT NULL AND price_range NOT IN ('TIER1','TIER2','TIER3')           THEN 'RULE0020'
  END AS _rule_id,
  CASE
    WHEN tick_id IS NULL                                                                    THEN 'tick_id IS NULL'
    WHEN tick_size IS NOT NULL AND tick_size <= 0                                           THEN CONCAT('tick_size not positive: ', CAST(tick_size AS STRING))
    WHEN price_range IS NOT NULL AND price_range NOT IN ('TIER1','TIER2','TIER3')           THEN CONCAT('Invalid price_range: ', COALESCE(price_range,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.tick
WHERE tick_id IS NULL
   OR (tick_size IS NOT NULL AND tick_size <= 0)
   OR (price_range IS NOT NULL AND price_range NOT IN ('TIER1','TIER2','TIER3'));

-- COMMAND ----------
-- ── identifiers ───────────────────────────────────────────────────────────────
-- Note: bronze has cusip/isin/sedol as individual columns; ticker & bloomberg_id are NULL in rejects
INSERT INTO statestreet.s_statestreet.identifiers_rejects
SELECT
  identifiers_id, product_id, cusip, isin, sedol,
  NULL AS ticker,
  NULL AS bloomberg_id,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN identifiers_id IS NULL                                         THEN 'RULE0051'
    WHEN cusip IS NOT NULL AND LENGTH(cusip) <> 9                       THEN 'RULE0069'
    WHEN isin IS NOT NULL AND LENGTH(isin) <> 12                        THEN 'RULE0070'
    WHEN sedol IS NOT NULL AND LENGTH(sedol) <> 7                       THEN 'RULE0071'
  END AS _rule_id,
  CASE
    WHEN identifiers_id IS NULL                                         THEN 'identifiers_id IS NULL'
    WHEN cusip IS NOT NULL AND LENGTH(cusip) <> 9                       THEN CONCAT('CUSIP not 9 chars: ', COALESCE(cusip,'null'))
    WHEN isin IS NOT NULL AND LENGTH(isin) <> 12                        THEN CONCAT('ISIN not 12 chars: ', COALESCE(isin,'null'))
    WHEN sedol IS NOT NULL AND LENGTH(sedol) <> 7                       THEN CONCAT('SEDOL not 7 chars: ', COALESCE(sedol,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.identifiers
WHERE identifiers_id IS NULL
   OR (cusip IS NOT NULL AND LENGTH(cusip) <> 9)
   OR (isin IS NOT NULL AND LENGTH(isin) <> 12)
   OR (sedol IS NOT NULL AND LENGTH(sedol) <> 7);

-- COMMAND ----------
-- ── classification ────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.classification_rejects
SELECT
  classification_id, product_id, classification_type, classification_value,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0030' AS _rule_id,
  'classification_id IS NULL' AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.classification
WHERE classification_id IS NULL;

-- COMMAND ----------
-- ── product_rating ────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.product_rating_rejects
SELECT
  product_rating_id, product_id, product_rating_type_id, rating_value, rating_agency, watch_code, effective_from_date,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_rating_id IS NULL                                                                   THEN 'RULE0052'
    WHEN rating_agency NOT IN ('MOODYS','SP','FITCH','DBRS')                                         THEN 'RULE0021'
    WHEN watch_code IS NOT NULL AND watch_code NOT IN ('POSITIVE','NEGATIVE','STABLE','DEVELOPING','NONE') THEN 'RULE0022'
  END AS _rule_id,
  CASE
    WHEN product_rating_id IS NULL                                                                   THEN 'product_rating_id IS NULL'
    WHEN rating_agency NOT IN ('MOODYS','SP','FITCH','DBRS')                                         THEN CONCAT('Invalid rating_agency: ', COALESCE(rating_agency,'null'))
    WHEN watch_code IS NOT NULL AND watch_code NOT IN ('POSITIVE','NEGATIVE','STABLE','DEVELOPING','NONE') THEN CONCAT('Invalid watch_code: ', COALESCE(watch_code,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.product_rating
WHERE product_rating_id IS NULL
   OR rating_agency NOT IN ('MOODYS','SP','FITCH','DBRS')
   OR (watch_code IS NOT NULL AND watch_code NOT IN ('POSITIVE','NEGATIVE','STABLE','DEVELOPING','NONE'));

-- COMMAND ----------
-- ── product_rating_type ───────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.product_rating_type_rejects
SELECT
  product_rating_type_id, rating_agency, rating_scale, rating_type_code, description,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN product_rating_type_id IS NULL                                                                          THEN 'RULE0046'
    WHEN rating_type_code IS NOT NULL AND rating_type_code NOT IN ('ISSUER','ISSUE','SHORT_TERM','LONG_TERM')    THEN 'RULE0023'
  END AS _rule_id,
  CASE
    WHEN product_rating_type_id IS NULL                                                                          THEN 'product_rating_type_id IS NULL'
    WHEN rating_type_code IS NOT NULL AND rating_type_code NOT IN ('ISSUER','ISSUE','SHORT_TERM','LONG_TERM')    THEN CONCAT('Invalid rating_type_code: ', COALESCE(rating_type_code,'null'))
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.product_rating_type
WHERE product_rating_type_id IS NULL
   OR (rating_type_code IS NOT NULL AND rating_type_code NOT IN ('ISSUER','ISSUE','SHORT_TERM','LONG_TERM'));

-- COMMAND ----------
-- ── coupon ────────────────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.coupon_rejects
SELECT
  coupon_id, product_id, coupon_rate, payment_date, coupon_type, frequency,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  CASE
    WHEN coupon_id IS NULL      THEN 'RULE0030'
    WHEN payment_date IS NULL   THEN 'RULE0040'
  END AS _rule_id,
  CASE
    WHEN coupon_id IS NULL      THEN 'coupon_id IS NULL'
    WHEN payment_date IS NULL   THEN 'payment_date IS NULL'
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.coupon
WHERE coupon_id IS NULL
   OR payment_date IS NULL;

-- COMMAND ----------
-- ── principal_redemption_provision ────────────────────────────────────────────
-- Note: bronze PK column is provision_id; silver renames to principal_redemption_provision_id
INSERT INTO statestreet.s_statestreet.principal_redemption_provision_rejects
SELECT
  provision_id AS principal_redemption_provision_id,
  provision_type,
  description,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0030' AS _rule_id,
  'provision_id IS NULL' AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.principal_redemption_provision
WHERE provision_id IS NULL;

-- COMMAND ----------
-- ── generic_product ───────────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.generic_product_rejects
SELECT
  generic_product_id, product_id, description, status, gs_legacy_prime_issue_currency,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0030' AS _rule_id,
  'generic_product_id IS NULL' AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.generic_product
WHERE generic_product_id IS NULL;

-- COMMAND ----------
-- ── listed_derivative_tick ────────────────────────────────────────────────────
INSERT INTO statestreet.s_statestreet.listed_derivative_tick_rejects
SELECT
  product_id, tick_id,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0030' AS _rule_id,
  CASE
    WHEN product_id IS NULL AND tick_id IS NULL  THEN 'product_id IS NULL, tick_id IS NULL'
    WHEN product_id IS NULL                      THEN 'product_id IS NULL'
    ELSE                                              'tick_id IS NULL'
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.listed_derivative_tick
WHERE product_id IS NULL OR tick_id IS NULL;

-- COMMAND ----------
-- ── debt_principal_redemption_provision ──────────────────────────────────────
-- Note: bronze FK column is provision_id; silver renames to principal_redemption_provision_id
INSERT INTO statestreet.s_statestreet.debt_principal_redemption_provision_rejects
SELECT
  product_id,
  provision_id AS principal_redemption_provision_id,
  _ingestion_ts, _source_file, _batch_id, _row_hash,
  'RULE0030' AS _rule_id,
  CASE
    WHEN product_id IS NULL AND provision_id IS NULL  THEN 'product_id IS NULL, provision_id IS NULL'
    WHEN product_id IS NULL                           THEN 'product_id IS NULL'
    ELSE                                                   'provision_id IS NULL'
  END AS _violation_detail,
  current_timestamp() AS _rejected_ts,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.debt_principal_redemption_provision
WHERE product_id IS NULL OR provision_id IS NULL;

-- COMMAND ----------
-- MAGIC %md ## 3. MERGE Passing Rows into Silver
-- MAGIC
-- MAGIC **SCD2 tables** (product, legal_entity, product_rating):
-- MAGIC   1. Expire changed current records (set is_current=FALSE, effective_end_date=yesterday)
-- MAGIC   2. Insert new versions for new/changed records
-- MAGIC
-- MAGIC **SCD1 tables** (all others): Standard MERGE (upsert on PK).

-- COMMAND ----------
-- MAGIC %md ### SCD2 — product

-- COMMAND ----------
-- Step 1: Expire changed product records
MERGE INTO statestreet.s_statestreet.product AS tgt
USING (
  SELECT src.product_id, src._row_hash
  FROM statestreet.b_statestreet.product src
  LEFT ANTI JOIN statestreet.s_statestreet.product_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id AND tgt.is_current = TRUE AND tgt._row_hash <> src._row_hash
WHEN MATCHED THEN UPDATE SET
  tgt.effective_end_date = current_date() - INTERVAL 1 DAY,
  tgt.is_current         = FALSE;

-- COMMAND ----------
-- Step 2: Insert new/changed product versions
INSERT INTO statestreet.s_statestreet.product
SELECT
  src.product_id, src.id_type, src.type, src.sub_type, src.status, src.settlement_type,
  src.description, src.issue_date, src.issue_price, src.current_face_value,
  src.issuer_legal_entity_id, src.tick_ladder_scale_id,
  current_date()       AS effective_start_date,
  DATE '9999-12-31'    AS effective_end_date,
  TRUE                 AS is_current,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.product src
LEFT ANTI JOIN statestreet.s_statestreet.product_rejects rej ON src.product_id = rej.product_id
WHERE NOT EXISTS (
  SELECT 1 FROM statestreet.s_statestreet.product tgt
  WHERE tgt.product_id = src.product_id AND tgt.is_current = TRUE AND tgt._row_hash = src._row_hash
);

-- COMMAND ----------
-- MAGIC %md ### SCD2 — legal_entity

-- COMMAND ----------
-- Step 1: Expire changed legal_entity records
MERGE INTO statestreet.s_statestreet.legal_entity AS tgt
USING (
  SELECT src.legal_entity_id, src._row_hash
  FROM statestreet.b_statestreet.legal_entity src
  LEFT ANTI JOIN statestreet.s_statestreet.legal_entity_rejects rej ON src.legal_entity_id = rej.legal_entity_id
) AS src
ON tgt.legal_entity_id = src.legal_entity_id AND tgt.is_current = TRUE AND tgt._row_hash <> src._row_hash
WHEN MATCHED THEN UPDATE SET
  tgt.effective_end_date = current_date() - INTERVAL 1 DAY,
  tgt.is_current         = FALSE;

-- COMMAND ----------
-- Step 2: Insert new/changed legal_entity versions
INSERT INTO statestreet.s_statestreet.legal_entity
SELECT
  src.legal_entity_id, src.legal_name, src.legal_structure, src.country, src.formation_date,
  current_date()       AS effective_start_date,
  DATE '9999-12-31'    AS effective_end_date,
  TRUE                 AS is_current,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.legal_entity src
LEFT ANTI JOIN statestreet.s_statestreet.legal_entity_rejects rej ON src.legal_entity_id = rej.legal_entity_id
WHERE NOT EXISTS (
  SELECT 1 FROM statestreet.s_statestreet.legal_entity tgt
  WHERE tgt.legal_entity_id = src.legal_entity_id AND tgt.is_current = TRUE AND tgt._row_hash = src._row_hash
);

-- COMMAND ----------
-- MAGIC %md ### SCD2 — product_rating

-- COMMAND ----------
-- Step 1: Expire changed product_rating records
MERGE INTO statestreet.s_statestreet.product_rating AS tgt
USING (
  SELECT src.product_rating_id, src._row_hash
  FROM statestreet.b_statestreet.product_rating src
  LEFT ANTI JOIN statestreet.s_statestreet.product_rating_rejects rej ON src.product_rating_id = rej.product_rating_id
) AS src
ON tgt.product_rating_id = src.product_rating_id AND tgt.is_current = TRUE AND tgt._row_hash <> src._row_hash
WHEN MATCHED THEN UPDATE SET
  tgt.effective_end_date = current_date() - INTERVAL 1 DAY,
  tgt.is_current         = FALSE;

-- COMMAND ----------
-- Step 2: Insert new/changed product_rating versions
INSERT INTO statestreet.s_statestreet.product_rating
SELECT
  src.product_rating_id, src.product_id, src.product_rating_type_id,
  src.rating_value, src.rating_agency, src.watch_code, src.effective_from_date,
  current_date()       AS effective_start_date,
  DATE '9999-12-31'    AS effective_end_date,
  TRUE                 AS is_current,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash,
  '${dq_rule_version}' AS _dq_rule_version
FROM statestreet.b_statestreet.product_rating src
LEFT ANTI JOIN statestreet.s_statestreet.product_rating_rejects rej ON src.product_rating_id = rej.product_rating_id
WHERE NOT EXISTS (
  SELECT 1 FROM statestreet.s_statestreet.product_rating tgt
  WHERE tgt.product_rating_id = src.product_rating_id AND tgt.is_current = TRUE AND tgt._row_hash = src._row_hash
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — bond

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.bond AS tgt
USING (
  SELECT src.product_id, src.issue_currency_code, src.coupon_type, src.maturity_date,
         src.reference_index_rate, src.conversion_rule,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.bond src
  LEFT ANTI JOIN statestreet.s_statestreet.bond_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.issue_currency_code  = src.issue_currency_code,
  tgt.coupon_type          = src.coupon_type,
  tgt.maturity_date        = src.maturity_date,
  tgt.reference_index_rate = src.reference_index_rate,
  tgt.conversion_rule      = src.conversion_rule,
  tgt._ingestion_ts        = src._ingestion_ts,
  tgt._source_file         = src._source_file,
  tgt._batch_id            = src._batch_id,
  tgt._row_hash            = src._row_hash,
  tgt._dq_rule_version     = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, issue_currency_code, coupon_type, maturity_date, reference_index_rate, conversion_rule,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.issue_currency_code, src.coupon_type, src.maturity_date,
  src.reference_index_rate, src.conversion_rule,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — stock

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.stock AS tgt
USING (
  SELECT src.product_id, src.series_id, src.has_voting_rights, src.depository_type,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.stock src
  LEFT ANTI JOIN statestreet.s_statestreet.stock_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.series_id        = src.series_id,
  tgt.has_voting_rights= src.has_voting_rights,
  tgt.depository_type  = src.depository_type,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, series_id, has_voting_rights, depository_type,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.series_id, src.has_voting_rights, src.depository_type,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — common_stock

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.common_stock AS tgt
USING (
  SELECT src.product_id, src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.common_stock src
  LEFT ANTI JOIN statestreet.s_statestreet.common_stock_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — preferred_stock

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.preferred_stock AS tgt
USING (
  SELECT src.product_id, src.dividend_right, src.par_value,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.preferred_stock src
  LEFT ANTI JOIN statestreet.s_statestreet.preferred_stock_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.dividend_right   = src.dividend_right,
  tgt.par_value        = src.par_value,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, dividend_right, par_value, _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.dividend_right, src.par_value,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — debt

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.debt AS tgt
USING (
  SELECT src.product_id, src.total_amount_issued, src.par_value,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.debt src
  LEFT ANTI JOIN statestreet.s_statestreet.debt_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.total_amount_issued = src.total_amount_issued,
  tgt.par_value           = src.par_value,
  tgt._ingestion_ts       = src._ingestion_ts,
  tgt._source_file        = src._source_file,
  tgt._batch_id           = src._batch_id,
  tgt._row_hash           = src._row_hash,
  tgt._dq_rule_version    = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, total_amount_issued, par_value, _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.total_amount_issued, src.par_value,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — muni

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.muni AS tgt
USING (
  SELECT src.product_id, src.pledge_type, src.tax_exempt, src.state, src.purpose,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.muni src
  LEFT ANTI JOIN statestreet.s_statestreet.muni_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.pledge_type      = src.pledge_type,
  tgt.tax_exempt       = src.tax_exempt,
  tgt.state            = src.state,
  tgt.purpose          = src.purpose,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, pledge_type, tax_exempt, state, purpose,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.pledge_type, src.tax_exempt, src.state, src.purpose,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — pool_backed_security

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.pool_backed_security AS tgt
USING (
  SELECT src.product_id, src.pool_type, src.originator,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.pool_backed_security src
  LEFT ANTI JOIN statestreet.s_statestreet.pool_backed_security_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.pool_type        = src.pool_type,
  tgt.originator       = src.originator,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, pool_type, originator, _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.pool_type, src.originator,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — fund

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.fund AS tgt
USING (
  SELECT src.product_id, src.endness_type, src.mutual_fund_type, src.mutual_fund_load_type,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.fund src
  LEFT ANTI JOIN statestreet.s_statestreet.fund_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.endness_type          = src.endness_type,
  tgt.mutual_fund_type      = src.mutual_fund_type,
  tgt.mutual_fund_load_type = src.mutual_fund_load_type,
  tgt._ingestion_ts         = src._ingestion_ts,
  tgt._source_file          = src._source_file,
  tgt._batch_id             = src._batch_id,
  tgt._row_hash             = src._row_hash,
  tgt._dq_rule_version      = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, endness_type, mutual_fund_type, mutual_fund_load_type,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.endness_type, src.mutual_fund_type, src.mutual_fund_load_type,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — right

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.right AS tgt
USING (
  SELECT src.product_id, src.exercise_style, src.option_type, src.strike_price,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.right src
  LEFT ANTI JOIN statestreet.s_statestreet.right_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.exercise_style   = src.exercise_style,
  tgt.option_type      = src.option_type,
  tgt.strike_price     = src.strike_price,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, exercise_style, option_type, strike_price,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.exercise_style, src.option_type, src.strike_price,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — listed_derivative

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.listed_derivative AS tgt
USING (
  SELECT src.product_id, src.series_id, src.underlying_product_id, src.contract_month, src.last_trade_date,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.listed_derivative src
  LEFT ANTI JOIN statestreet.s_statestreet.listed_derivative_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.series_id             = src.series_id,
  tgt.underlying_product_id = src.underlying_product_id,
  tgt.contract_month        = src.contract_month,
  tgt.last_trade_date       = src.last_trade_date,
  tgt._ingestion_ts         = src._ingestion_ts,
  tgt._source_file          = src._source_file,
  tgt._batch_id             = src._batch_id,
  tgt._row_hash             = src._row_hash,
  tgt._dq_rule_version      = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, series_id, underlying_product_id, contract_month, last_trade_date,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.series_id, src.underlying_product_id, src.contract_month, src.last_trade_date,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — option

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.option AS tgt
USING (
  SELECT src.product_id, src.option_type, src.exercise_style, src.margin_style,
         src.strike_price, src.strike_currency_code, src.expiry_date,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.option src
  LEFT ANTI JOIN statestreet.s_statestreet.option_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.option_type          = src.option_type,
  tgt.exercise_style       = src.exercise_style,
  tgt.margin_style         = src.margin_style,
  tgt.strike_price         = src.strike_price,
  tgt.strike_currency_code = src.strike_currency_code,
  tgt.expiry_date          = src.expiry_date,
  tgt._ingestion_ts        = src._ingestion_ts,
  tgt._source_file         = src._source_file,
  tgt._batch_id            = src._batch_id,
  tgt._row_hash            = src._row_hash,
  tgt._dq_rule_version     = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, option_type, exercise_style, margin_style, strike_price, strike_currency_code, expiry_date,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.option_type, src.exercise_style, src.margin_style,
  src.strike_price, src.strike_currency_code, src.expiry_date,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — future

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.future AS tgt
USING (
  SELECT src.product_id, src.valuation_method, src.delivery_date,
         CAST(NULL AS TIMESTAMP) AS first_delivery_datetime_utc,
         CAST(NULL AS TIMESTAMP) AS last_delivery_datetime_utc,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.future src
  LEFT ANTI JOIN statestreet.s_statestreet.future_rejects rej ON src.product_id = rej.product_id
) AS src
ON tgt.product_id = src.product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.valuation_method              = src.valuation_method,
  tgt.delivery_date                 = src.delivery_date,
  tgt.first_delivery_datetime_utc   = src.first_delivery_datetime_utc,
  tgt.last_delivery_datetime_utc    = src.last_delivery_datetime_utc,
  tgt._ingestion_ts                 = src._ingestion_ts,
  tgt._source_file                  = src._source_file,
  tgt._batch_id                     = src._batch_id,
  tgt._row_hash                     = src._row_hash,
  tgt._dq_rule_version              = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, valuation_method, delivery_date, first_delivery_datetime_utc, last_delivery_datetime_utc,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.valuation_method, src.delivery_date,
  src.first_delivery_datetime_utc, src.last_delivery_datetime_utc,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — currency (column rename: currency_code→code, currency_name→name)

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.currency AS tgt
USING (
  SELECT src.currency_code AS code, src.symbol AS name,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.currency src
  LEFT ANTI JOIN statestreet.s_statestreet.currency_rejects rej ON src.currency_code = rej.code
) AS src
ON tgt.code = src.code
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.name             = src.name,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  code, name, _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.code, src.name, src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — series (column rename: description→series_name)

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.series AS tgt
USING (
  SELECT src.series_id, src.description AS series_name,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.series src
  LEFT ANTI JOIN statestreet.s_statestreet.series_rejects rej ON src.series_id = rej.series_id
) AS src
ON tgt.series_id = src.series_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.series_name      = src.series_name,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  series_id, series_name, _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.series_id, src.series_name, src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — tick_ladder_scale

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.tick_ladder_scale AS tgt
USING (
  SELECT src.tick_ladder_scale_id, src.tick_size, src.description,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.tick_ladder_scale src
  LEFT ANTI JOIN statestreet.s_statestreet.tick_ladder_scale_rejects rej
    ON src.tick_ladder_scale_id = rej.tick_ladder_scale_id
) AS src
ON tgt.tick_ladder_scale_id = src.tick_ladder_scale_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.tick_size        = src.tick_size,
  tgt.description      = src.description,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  tick_ladder_scale_id, tick_size, description, _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.tick_ladder_scale_id, src.tick_size, src.description,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — tick

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.tick AS tgt
USING (
  SELECT src.tick_id, src.tick_ladder_scale_id, src.tick_size, src.price_range, src.tick_currency_code,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.tick src
  LEFT ANTI JOIN statestreet.s_statestreet.tick_rejects rej ON src.tick_id = rej.tick_id
) AS src
ON tgt.tick_id = src.tick_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.tick_ladder_scale_id = src.tick_ladder_scale_id,
  tgt.tick_size            = src.tick_size,
  tgt.price_range          = src.price_range,
  tgt.tick_currency_code   = src.tick_currency_code,
  tgt._ingestion_ts        = src._ingestion_ts,
  tgt._source_file         = src._source_file,
  tgt._batch_id            = src._batch_id,
  tgt._row_hash            = src._row_hash,
  tgt._dq_rule_version     = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  tick_id, tick_ladder_scale_id, tick_size, price_range, tick_currency_code,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.tick_id, src.tick_ladder_scale_id, src.tick_size, src.price_range, src.tick_currency_code,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — identifiers (no ticker/bloomberg_id in bronze → NULL in silver)

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.identifiers AS tgt
USING (
  SELECT src.identifiers_id, src.product_id, src.cusip, src.isin, src.sedol,
         NULL AS ticker, NULL AS bloomberg_id,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.identifiers src
  LEFT ANTI JOIN statestreet.s_statestreet.identifiers_rejects rej ON src.identifiers_id = rej.identifiers_id
) AS src
ON tgt.identifiers_id = src.identifiers_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.product_id       = src.product_id,
  tgt.cusip            = src.cusip,
  tgt.isin             = src.isin,
  tgt.sedol            = src.sedol,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  identifiers_id, product_id, cusip, isin, sedol, ticker, bloomberg_id,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.identifiers_id, src.product_id, src.cusip, src.isin, src.sedol, NULL, NULL,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — classification

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.classification AS tgt
USING (
  SELECT src.classification_id, src.product_id, src.classification_type, src.classification_value,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.classification src
  LEFT ANTI JOIN statestreet.s_statestreet.classification_rejects rej
    ON src.classification_id = rej.classification_id
) AS src
ON tgt.classification_id = src.classification_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.product_id           = src.product_id,
  tgt.classification_type  = src.classification_type,
  tgt.classification_value = src.classification_value,
  tgt._ingestion_ts        = src._ingestion_ts,
  tgt._source_file         = src._source_file,
  tgt._batch_id            = src._batch_id,
  tgt._row_hash            = src._row_hash,
  tgt._dq_rule_version     = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  classification_id, product_id, classification_type, classification_value,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.classification_id, src.product_id, src.classification_type, src.classification_value,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — product_rating_type

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.product_rating_type AS tgt
USING (
  SELECT src.product_rating_type_id, src.rating_agency, src.rating_scale, src.rating_type_code, src.description,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.product_rating_type src
  LEFT ANTI JOIN statestreet.s_statestreet.product_rating_type_rejects rej
    ON src.product_rating_type_id = rej.product_rating_type_id
) AS src
ON tgt.product_rating_type_id = src.product_rating_type_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.rating_agency        = src.rating_agency,
  tgt.rating_scale         = src.rating_scale,
  tgt.rating_type_code     = src.rating_type_code,
  tgt.description          = src.description,
  tgt._ingestion_ts        = src._ingestion_ts,
  tgt._source_file         = src._source_file,
  tgt._batch_id            = src._batch_id,
  tgt._row_hash            = src._row_hash,
  tgt._dq_rule_version     = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_rating_type_id, rating_agency, rating_scale, rating_type_code, description,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_rating_type_id, src.rating_agency, src.rating_scale, src.rating_type_code, src.description,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — coupon

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.coupon AS tgt
USING (
  SELECT src.coupon_id, src.product_id, src.coupon_rate, src.payment_date, src.coupon_type, src.frequency,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.coupon src
  LEFT ANTI JOIN statestreet.s_statestreet.coupon_rejects rej ON src.coupon_id = rej.coupon_id
) AS src
ON tgt.coupon_id = src.coupon_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.product_id       = src.product_id,
  tgt.coupon_rate      = src.coupon_rate,
  tgt.payment_date     = src.payment_date,
  tgt.coupon_type      = src.coupon_type,
  tgt.frequency        = src.frequency,
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  coupon_id, product_id, coupon_rate, payment_date, coupon_type, frequency,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.coupon_id, src.product_id, src.coupon_rate, src.payment_date, src.coupon_type, src.frequency,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — principal_redemption_provision (bronze: provision_id → silver: principal_redemption_provision_id)

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.principal_redemption_provision AS tgt
USING (
  SELECT src.provision_id AS principal_redemption_provision_id, src.provision_type, src.description,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.principal_redemption_provision src
  LEFT ANTI JOIN statestreet.s_statestreet.principal_redemption_provision_rejects rej
    ON src.provision_id = rej.principal_redemption_provision_id
) AS src
ON tgt.principal_redemption_provision_id = src.principal_redemption_provision_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.provision_type                    = src.provision_type,
  tgt.description                       = src.description,
  tgt._ingestion_ts                     = src._ingestion_ts,
  tgt._source_file                      = src._source_file,
  tgt._batch_id                         = src._batch_id,
  tgt._row_hash                         = src._row_hash,
  tgt._dq_rule_version                  = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  principal_redemption_provision_id, provision_type, description,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.principal_redemption_provision_id, src.provision_type, src.description,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — generic_product

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.generic_product AS tgt
USING (
  SELECT src.generic_product_id, src.product_id, src.description, src.status,
         src.gs_legacy_prime_issue_currency,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.generic_product src
  LEFT ANTI JOIN statestreet.s_statestreet.generic_product_rejects rej
    ON src.generic_product_id = rej.generic_product_id
) AS src
ON tgt.generic_product_id = src.generic_product_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt.product_id                    = src.product_id,
  tgt.description                   = src.description,
  tgt.status                        = src.status,
  tgt.gs_legacy_prime_issue_currency= src.gs_legacy_prime_issue_currency,
  tgt._ingestion_ts                 = src._ingestion_ts,
  tgt._source_file                  = src._source_file,
  tgt._batch_id                     = src._batch_id,
  tgt._row_hash                     = src._row_hash,
  tgt._dq_rule_version              = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  generic_product_id, product_id, description, status, gs_legacy_prime_issue_currency,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.generic_product_id, src.product_id, src.description, src.status, src.gs_legacy_prime_issue_currency,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — listed_derivative_tick (bridge table, composite PK)

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.listed_derivative_tick AS tgt
USING (
  SELECT src.product_id, src.tick_id,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.listed_derivative_tick src
  LEFT ANTI JOIN statestreet.s_statestreet.listed_derivative_tick_rejects rej
    ON src.product_id = rej.product_id AND src.tick_id = rej.tick_id
) AS src
ON tgt.product_id = src.product_id AND tgt.tick_id = src.tick_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, tick_id, _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.tick_id,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ### SCD1 — debt_principal_redemption_provision (bridge table, composite PK; bronze: provision_id → silver: principal_redemption_provision_id)

-- COMMAND ----------
MERGE INTO statestreet.s_statestreet.debt_principal_redemption_provision AS tgt
USING (
  SELECT src.product_id, src.provision_id AS principal_redemption_provision_id,
         src._ingestion_ts, src._source_file, src._batch_id, src._row_hash
  FROM statestreet.b_statestreet.debt_principal_redemption_provision src
  LEFT ANTI JOIN statestreet.s_statestreet.debt_principal_redemption_provision_rejects rej
    ON src.product_id = rej.product_id AND src.provision_id = rej.principal_redemption_provision_id
) AS src
ON tgt.product_id = src.product_id AND tgt.principal_redemption_provision_id = src.principal_redemption_provision_id
WHEN MATCHED AND tgt._row_hash <> src._row_hash THEN UPDATE SET
  tgt._ingestion_ts    = src._ingestion_ts,
  tgt._source_file     = src._source_file,
  tgt._batch_id        = src._batch_id,
  tgt._row_hash        = src._row_hash,
  tgt._dq_rule_version = '${dq_rule_version}'
WHEN NOT MATCHED THEN INSERT (
  product_id, principal_redemption_provision_id,
  _ingestion_ts, _source_file, _batch_id, _row_hash, _dq_rule_version
) VALUES (
  src.product_id, src.principal_redemption_provision_id,
  src._ingestion_ts, src._source_file, src._batch_id, src._row_hash, '${dq_rule_version}'
);

-- COMMAND ----------
-- MAGIC %md ## Done
-- MAGIC
-- MAGIC Silver conformance complete:
-- MAGIC - **27 tables** created (IF NOT EXISTS) with correct schema + TBLPROPERTIES
-- MAGIC - **27 _rejects tables** populated with DQ-failing rows
-- MAGIC - **27 silver tables** merged with passing rows (SCD2 for product/legal_entity/product_rating; SCD1 for all others)