```sql
-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Silver Conformance — Securities Master
-- MAGIC
-- MAGIC Applies DQ rules, routes rejects, and performs SCD2 upserts for all 27 Silver tables.
-- MAGIC
-- MAGIC **Execution order:**
-- MAGIC 1. Set DQ rule version parameter
-- MAGIC 2. Create Silver tables (idempotent DDL)
-- MAGIC 3. For each table: write rejects → MERGE passing rows
-- MAGIC
-- MAGIC **Tables with SCD2:** product, legal_entity, product_rating
-- MAGIC **DQ-exempt (Bronze-only):** dq_rules_catalog, dq_issues_catalog

-- COMMAND ----------

-- MAGIC %md ## 0. Parameters

-- COMMAND ----------

-- Compute DQ rule version from notebook parameter or use a default sentinel.
-- In production the orchestrator passes the SHA256 of silver/rules.yaml via widget.
-- Usage: dbutils.widgets.text("dq_rule_version", "<sha256>")
-- Here we declare the widget with a safe default so the notebook is runnable standalone.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("dq_rule_version", "dev-snapshot")
-- MAGIC DQ_RULE_VERSION = dbutils.widgets.get("dq_rule_version")
-- MAGIC spark.conf.set("sml.dq_rule_version", DQ_RULE_VERSION)
-- MAGIC print(f"DQ Rule Version: {DQ_RULE_VERSION}")

-- COMMAND ----------

-- MAGIC %md ## 1. Create Silver Tables (Idempotent DDL)

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.01  product
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product (
  product_id              STRING        NOT NULL,
  id_type                 STRING,
  type                    STRING,
  sub_type                STRING,
  status                  STRING,
  settlement_type         STRING,
  description             STRING,
  issue_date              DATE,
  issue_price             DECIMAL(28,8),
  current_face_value      DECIMAL(28,8),
  issuer_legal_entity_id  STRING,
  tick_ladder_scale_id    STRING,
  effective_start_date    DATE          NOT NULL,
  effective_end_date      DATE          NOT NULL,
  is_current              BOOLEAN       NOT NULL,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
PARTITIONED BY (type)
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rejects (
  product_id              STRING,
  id_type                 STRING,
  type                    STRING,
  sub_type                STRING,
  status                  STRING,
  settlement_type         STRING,
  description             STRING,
  issue_date              DATE,
  issue_price             DECIMAL(28,8),
  current_face_value      DECIMAL(28,8),
  issuer_legal_entity_id  STRING,
  tick_ladder_scale_id    STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.02  generic_product
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.generic_product (
  product_id              STRING,
  generic_product_id      STRING,
  id_type                 STRING,
  identifier_value        STRING,
  description             STRING,
  status                  STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.generic_product_rejects (
  product_id              STRING,
  generic_product_id      STRING,
  id_type                 STRING,
  identifier_value        STRING,
  description             STRING,
  status                  STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.03  legal_entity  (SCD2)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.legal_entity (
  legal_entity_id         STRING        NOT NULL,
  legal_name              STRING,
  country                 STRING,
  entity_type             STRING,
  effective_start_date    DATE          NOT NULL,
  effective_end_date      DATE          NOT NULL,
  is_current              BOOLEAN       NOT NULL,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.legal_entity_rejects (
  legal_entity_id         STRING,
  legal_name              STRING,
  country                 STRING,
  entity_type             STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.04  tick_ladder_scale
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.tick_ladder_scale (
  tick_ladder_scale_id    STRING        NOT NULL,
  description             STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.tick_ladder_scale_rejects (
  tick_ladder_scale_id    STRING,
  description             STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.05  tick
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.tick (
  tick_id                 STRING        NOT NULL,
  tick_ladder_scale_id    STRING        NOT NULL,
  price_from              DECIMAL(28,8),
  price_to                DECIMAL(28,8),
  tick_size               DECIMAL(28,8),
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.tick_rejects (
  tick_id                 STRING,
  tick_ladder_scale_id    STRING,
  price_from              DECIMAL(28,8),
  price_to                DECIMAL(28,8),
  tick_size               DECIMAL(28,8),
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.06  product_rating  (SCD2)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rating (
  product_rating_id       STRING        NOT NULL,
  product_id              STRING        NOT NULL,
  product_rating_type_id  STRING,
  rating_value            STRING,
  rating_agency           STRING,
  watch_code              STRING,
  rating_scale            STRING,
  effective_from_date     DATE          NOT NULL,
  effective_start_date    DATE          NOT NULL,
  effective_end_date      DATE          NOT NULL,
  is_current              BOOLEAN       NOT NULL,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
PARTITIONED BY (product_rating_type_id)
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rating_rejects (
  product_rating_id       STRING,
  product_id              STRING,
  product_rating_type_id  STRING,
  rating_value            STRING,
  rating_agency           STRING,
  watch_code              STRING,
  rating_scale            STRING,
  effective_from_date     DATE,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.07  product_rating_type
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rating_type (
  product_rating_type_id  STRING        NOT NULL,
  rating_type_code        STRING,
  description             STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.product_rating_type_rejects (
  product_rating_type_id  STRING,
  rating_type_code        STRING,
  description             STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.08  classification
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.classification (
  classification_id       STRING        NOT NULL,
  product_id              STRING        NOT NULL,
  classification_type     STRING,
  classification_value    STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.classification_rejects (
  classification_id       STRING,
  product_id              STRING,
  classification_type     STRING,
  classification_value    STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.09  identifiers
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.identifiers (
  identifier_id           STRING        NOT NULL,
  product_id              STRING        NOT NULL,
  id_type                 STRING        NOT NULL,
  identifier_value        STRING        NOT NULL,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.identifiers_rejects (
  identifier_id           STRING,
  product_id              STRING,
  id_type                 STRING,
  identifier_value        STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.10  series
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.series (
  series_id               STRING        NOT NULL,
  description             STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.series_rejects (
  series_id               STRING,
  description             STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.11  currency
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.currency (
  currency_code           STRING        NOT NULL,
  currency_name           STRING,
  currency_symbol         STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.currency_rejects (
  currency_code           STRING,
  currency_name           STRING,
  currency_symbol         STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.12  fund
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.fund (
  product_id              STRING        NOT NULL,
  endness_type            STRING,
  mutual_fund_type        STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.fund_rejects (
  product_id              STRING,
  endness_type            STRING,
  mutual_fund_type        STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.13  right
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.right (
  product_id              STRING        NOT NULL,
  subscription_ratio      DECIMAL(28,8),
  expiry_date             DATE,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.right_rejects (
  product_id              STRING,
  subscription_ratio      DECIMAL(28,8),
  expiry_date             DATE,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.14  debt
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.debt (
  product_id              STRING        NOT NULL,
  total_amount_issued     DECIMAL(28,8),
  issue_date_settlement   DATE,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.debt_rejects (
  product_id              STRING,
  total_amount_issued     DECIMAL(28,8),
  issue_date_settlement   DATE,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.15  bond
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.bond (
  product_id              STRING        NOT NULL,
  coupon_type             STRING        NOT NULL,
  maturity_date           DATE          NOT NULL,
  issue_currency_code     STRING        NOT NULL,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.bond_rejects (
  product_id              STRING,
  coupon_type             STRING,
  maturity_date           DATE,
  issue_currency_code     STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.16  muni
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.muni (
  product_id              STRING        NOT NULL,
  tax_exempt              BOOLEAN,
  state                   STRING,
  purpose                 STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.muni_rejects (
  product_id              STRING,
  tax_exempt              BOOLEAN,
  state                   STRING,
  purpose                 STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.17  pool_backed_security
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.pool_backed_security (
  product_id              STRING        NOT NULL,
  pool_type               STRING,
  originator              STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.pool_backed_security_rejects (
  product_id              STRING,
  pool_type               STRING,
  originator              STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.18  stock
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.stock (
  product_id              STRING        NOT NULL,
  series_id               STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.stock_rejects (
  product_id              STRING,
  series_id               STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.19  common_stock
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.common_stock (
  product_id              STRING        NOT NULL,
  voting_rights           BOOLEAN,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.common_stock_rejects (
  product_id              STRING,
  voting_rights           BOOLEAN,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.20  preferred_stock
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.preferred_stock (
  product_id              STRING        NOT NULL,
  dividend_right          STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.preferred_stock_rejects (
  product_id              STRING,
  dividend_right          STRING,
  _source_file            STRING,
  _ingestion_ts           TIMESTAMP,
  _batch_id               STRING,
  _row_hash               STRING,
  _dq_rule_version        STRING,
  _rule_id                STRING,
  _violation_detail       STRING,
  _rejected_ts            TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ─────────────────────────────────────────────
-- 1.21  listed_derivative
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS statestreet.s_statestreet.listed_derivative (
  product_id              STRING        NOT NULL,
  series_id               STRING,
  underlying_product_id   STRING        NOT NULL,
  _source_file            STRING,
  _ingestion_