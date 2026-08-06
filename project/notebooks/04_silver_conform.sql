```sql
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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

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
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.identifiers_rejects
USING DELTA
AS SELECT
  CAST(NULL AS STRING)    AS identifiers_id,
  CAST(NULL AS STRING)    AS product_id,
  CAST(NULL AS STRING)    AS cusip,
  CAST(NULL AS STRING)    AS isin,
  CAST(NULL AS STRING)    AS sedol,
  CAST(NULL AS STRING)    AS ticker,