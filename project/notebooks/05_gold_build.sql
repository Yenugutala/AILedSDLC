```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer — Securities Master Dimensional Marts
-- MAGIC
-- MAGIC Builds 4 dimensional marts in `statestreet.g_statestreet`:
-- MAGIC
-- MAGIC | Mart | Grain | Source Tables |
-- MAGIC |------|-------|---------------|
-- MAGIC | `dim_product` | one row per product_id | 16 Silver tables |
-- MAGIC | `dim_legal_entity` | one row per legal_entity_id | 1 Silver table |
-- MAGIC | `fact_product_rating` | one row per product × rating_date × rating_type | 3 Silver tables |
-- MAGIC | `fact_coupon_schedule` | one row per bond × payment_date | 4 Silver tables |
-- MAGIC
-- MAGIC **Run order:** Bronze → Silver → Gold (this notebook)
-- MAGIC **Language:** Databricks SQL (all cells)

-- COMMAND ----------
-- MAGIC %md ## 0. Pre-flight — verify Silver tables exist

-- COMMAND ----------

SELECT
  'statestreet.s_statestreet.product'              AS table_name, COUNT(*) AS row_count FROM statestreet.s_statestreet.product          WHERE is_current = TRUE
UNION ALL SELECT 'statestreet.s_statestreet.legal_entity',        COUNT(*) FROM statestreet.s_statestreet.legal_entity       WHERE is_current = TRUE
UNION ALL SELECT 'statestreet.s_statestreet.bond',                COUNT(*) FROM statestreet.s_statestreet.bond               WHERE is_current = TRUE
UNION ALL SELECT 'statestreet.s_statestreet.product_rating',      COUNT(*) FROM statestreet.s_statestreet.product_rating     WHERE is_current = TRUE
UNION ALL SELECT 'statestreet.s_statestreet.coupon',              COUNT(*) FROM statestreet.s_statestreet.coupon
ORDER BY table_name;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1. `dim_product`
-- MAGIC **Grain:** one row per `product_id` (current Silver version only)
-- MAGIC
-- MAGIC Wide flattened dimension — all product subtype attributes surfaced as nullable columns
-- MAGIC via LEFT JOIN from Silver.  SCD2 columns are carried through from Silver but Gold itself
-- MAGIC is a current-state snapshot (no SCD2 tracking at Gold layer).

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.g_statestreet.dim_product (
  product_id                  STRING        NOT NULL,
  id_type                     STRING,
  type                        STRING        NOT NULL,
  sub_type                    STRING,
  status                      STRING        NOT NULL,
  settlement_type             STRING,
  description                 STRING,
  issue_date                  DATE,
  issue_price                 DECIMAL(28,8),
  current_face_value          DECIMAL(28,8),
  issuer_legal_entity_id      STRING,
  tick_ladder_scale_id        STRING,
  effective_start_date        DATE          NOT NULL,
  effective_end_date          DATE          NOT NULL,
  -- Denormalised issuer attributes
  issuer_legal_name           STRING,
  issuer_country              STRING,
  -- Tick ladder
  tick_ladder_scale_description STRING,
  -- Series (stock + listed_derivative)
  series_id                   STRING,
  -- Stock-specific
  voting_rights               BOOLEAN,
  dividend_type               STRING,
  -- Debt-specific
  total_amount_issued         DECIMAL(28,8),
  issue_currency_code         STRING,
  -- Bond-specific
  coupon_type                 STRING,
  maturity_date               DATE,
  face_currency_code          STRING,
  day_count_convention        STRING,
  -- Muni-specific
  tax_exempt                  BOOLEAN,
  muni_state                  STRING,
  muni_purpose                STRING,
  -- Pool-backed-specific
  pool_type                   STRING,
  pool_originator             STRING,
  -- Fund-specific
  endness_type                STRING,
  mutual_fund_type            STRING,
  -- Right-specific
  subscription_ratio          DECIMAL(28,8),
  -- Listed derivative-specific
  underlying_product_id       STRING,
  -- Option-specific
  option_type                 STRING,
  exercise_style              STRING,
  strike_price                DECIMAL(28,8),
  expiry_date                 DATE,
  -- Future-specific
  delivery_date               DATE,
  valuation_method            STRING,
  -- Pipeline metadata
  _dq_rule_version            STRING,
  _ingestion_ts               TIMESTAMP,
  _batch_id                   STRING
)
USING DELTA
PARTITIONED BY (type)
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------
-- MAGIC %md ### 1.1 Build `dim_product` — full LEFT JOIN across all Silver subtype tables

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.g_statestreet.dim_product
USING DELTA
PARTITIONED BY (type)
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT
  -- ── Core product attributes ────────────────────────────────────────────────
  p.product_id,
  p.id_type,
  p.type,
  p.sub_type,
  p.status,
  p.settlement_type,
  p.description,
  p.issue_date,
  p.issue_price,
  p.current_face_value,
  p.issuer_legal_entity_id,
  p.tick_ladder_scale_id,
  -- ── SCD2 range carried from Silver (current row only) ─────────────────────
  p.effective_start_date,
  p.effective_end_date,
  -- ── Denormalised issuer attributes (from legal_entity) ───────────────────
  le.legal_name                                       AS issuer_legal_name,
  le.country                                          AS issuer_country,
  -- ── Tick ladder (from tick_ladder_scale) ──────────────────────────────────
  tls.description                                     AS tick_ladder_scale_description,
  -- ── Series (coalesce stock series and listed_derivative series) ───────────
  COALESCE(st.series_id, ld.series_id)               AS series_id,
  -- ── Common stock-specific ──────────────────────────────────────────────────
  CAST(NULL AS BOOLEAN)                               AS voting_rights,
  -- ── Preferred stock-specific ──────────────────────────────────────────────
  ps.dividend_right                                   AS dividend_type,
  -- ── Debt-specific ──────────────────────────────────────────────────────────
  d.total_amount_issued,
  d.issue_currency_code,
  -- ── Bond-specific ──────────────────────────────────────────────────────────
  b.coupon_type,
  b.maturity_date,
  b.issue_currency_code                               AS face_currency_code,
  CAST(NULL AS STRING)                                AS day_count_convention,
  -- ── Muni-specific ──────────────────────────────────────────────────────────
  mn.tax_exempt,
  mn.state                                            AS muni_state,
  mn.purpose                                          AS muni_purpose,
  -- ── Pool-backed-specific ───────────────────────────────────────────────────
  pbs.pool_type,
  pbs.originator                                      AS pool_originator,
  -- ── Fund-specific ──────────────────────────────────────────────────────────
  f.endness_type,
  f.mutual_fund_type,
  -- ── Right-specific ─────────────────────────────────────────────────────────
  CAST(NULL AS DECIMAL(28,8))                         AS subscription_ratio,
  -- ── Listed derivative-specific ─────────────────────────────────────────────
  ld.underlying_product_id,
  -- ── Option-specific ────────────────────────────────────────────────────────
  op.option_type,
  op.exercise_style,
  op.strike_price,
  op.expiry_date,
  -- ── Future-specific ────────────────────────────────────────────────────────
  ft.delivery_date,
  ft.valuation_method,
  -- ── Pipeline metadata (from Silver product row) ───────────────────────────
  p._dq_rule_version,
  p._ingestion_ts,
  p._batch_id

FROM statestreet.s_statestreet.product p

-- Issuing legal entity
LEFT JOIN statestreet.s_statestreet.legal_entity le
  ON p.issuer_legal_entity_id = le.legal_entity_id
  AND le.is_current = TRUE

-- Tick ladder scale
LEFT JOIN statestreet.s_statestreet.tick_ladder_scale tls
  ON p.tick_ladder_scale_id = tls.tick_ladder_scale_id

-- Stock (base)
LEFT JOIN statestreet.s_statestreet.stock st
  ON p.product_id = st.product_id
  AND st.is_current = TRUE

-- Common stock
LEFT JOIN statestreet.s_statestreet.common_stock cs
  ON p.product_id = cs.product_id
  AND cs.is_current = TRUE

-- Preferred stock
LEFT JOIN statestreet.s_statestreet.preferred_stock ps
  ON p.product_id = ps.product_id
  AND ps.is_current = TRUE

-- Debt (base for bond, muni, pool-backed)
LEFT JOIN statestreet.s_statestreet.debt d
  ON p.product_id = d.product_id
  AND d.is_current = TRUE

-- Bond
LEFT JOIN statestreet.s_statestreet.bond b
  ON p.product_id = b.product_id
  AND b.is_current = TRUE

-- Muni (extends bond)
LEFT JOIN statestreet.s_statestreet.muni mn
  ON p.product_id = mn.product_id
  AND mn.is_current = TRUE

-- Pool-backed security (extends debt, not bond)
LEFT JOIN statestreet.s_statestreet.pool_backed_security pbs
  ON p.product_id = pbs.product_id
  AND pbs.is_current = TRUE

-- Fund
LEFT JOIN statestreet.s_statestreet.fund f
  ON p.product_id = f.product_id
  AND f.is_current = TRUE

-- Right
LEFT JOIN statestreet.s_statestreet.right r
  ON p.product_id = r.product_id
  AND r.is_current = TRUE

-- Listed derivative (base for option, future)
LEFT JOIN statestreet.s_statestreet.listed_derivative ld
  ON p.product_id = ld.product_id
  AND ld.is_current = TRUE

-- Option (extends listed_derivative)
LEFT JOIN statestreet.s_statestreet.option op
  ON p.product_id = op.product_id
  AND op.is_current = TRUE

-- Future (extends listed_derivative)
LEFT JOIN statestreet.s_statestreet.future ft
  ON p.product_id = ft.product_id
  AND ft.is_current = TRUE

WHERE p.is_current = TRUE;

-- COMMAND ----------
-- MAGIC %md ### 1.2 DQ grain check — assert one row per product_id

-- COMMAND ----------

SELECT
  'dim_product grain violation' AS check_name,
  COUNT(*) AS violation_count
FROM (
  SELECT product_id, COUNT(*) AS cnt
  FROM statestreet.g_statestreet.dim_product
  GROUP BY product_id
  HAVING COUNT(*) > 1
);

-- COMMAND ----------
-- MAGIC %md ### 1.3 Row count summary — `dim_product`

-- COMMAND ----------

SELECT
  type,
  COUNT(*) AS product_count,
  SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count
FROM statestreet.g_statestreet.dim_product
GROUP BY type
ORDER BY product_count DESC;

-- COMMAND ----------
-- MAGIC %md ### 1.4 Genie comments — `dim_product`

-- COMMAND ----------

COMMENT ON TABLE statestreet.g_statestreet.dim_product IS
  'Flattened security product dimension. One row per active security (is_current = TRUE in Silver). '
  'Covers all product types: EQUITY (CommonStock, PreferredStock), '
  'DEBT (Bond, Muni, PoolBackedSecurity), FUND, DERIVATIVE (Option, Future), and RIGHT. '
  'Subtype-specific attributes are NULL for products of a different type. '
  'Join to fact tables on product_id. Join to dim_legal_entity on issuer_legal_entity_id. '
  'Source: 16 Silver tables joined on product_id. Partitioned by type.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.product_id IS
  'Unique security identifier — primary key of the dimension. '
  'Alphanumeric string. Every security has exactly one row in this dimension.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.id_type IS
  'Primary identifier type for the security. '
  'Values: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.type IS
  'Top-level product category. '
  'Values: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT. '
  'Use this column to filter for a specific asset class.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.sub_type IS
  'Product sub-category providing more granular classification. '
  'Values: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE, FUND, RIGHT.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.status IS
  'Security lifecycle status. '
  'Values: ACTIVE (currently tradeable), INACTIVE, MATURED, SUSPENDED, DELISTED.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.settlement_type IS
  'Settlement method for trades in this security (e.g. DVP, FOP, RVP).';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.description IS
  'Human-readable name or description of the security as provided by the source system.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_date IS
  'Date when the security was originally issued. DATE format (YYYY-MM-DD). '
  'May be NULL for legacy records pre-dating the field.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_price IS
  'Price at which the security was originally issued. '
  'DECIMAL(28,8). NULL if not captured in source.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.current_face_value IS
  'Current face / par value of the security expressed as a percentage or absolute amount. '
  'DECIMAL(28,8). Relevant primarily for debt securities.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issuer_legal_entity_id IS
  'Foreign key to dim_legal_entity identifying the issuing entity. '
  'Join: dim_product.issuer_legal_entity_id = dim_legal_entity.legal_entity_id.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.tick_ladder_scale_id IS
  'Foreign key to the tick_ladder_scale table defining minimum price increment rules.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.effective_start_date IS
  'SCD2 effective start date of this product version in Silver. '
  'Carried through to Gold for reference — Gold itself is a current-state snapshot.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.effective_end_date IS
  'SCD2 effective end date of this product version in Silver (9999-12-31 for current row). '
  'Carried through to Gold for reference.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issuer_legal_name IS
  'Full legal name of the issuing entity, denormalised from dim_legal_entity. '
  'NULL if the issuer_legal_entity_id is not present in the legal_entity Silver table.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issuer_country IS
  'ISO 3166-1 alpha-2 country code of the issuing entity, denormalised from dim_legal_entity. '
  'Example values: US, GB, DE, JP, FR.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.tick_ladder_scale_description IS
  'Human-readable description of the tick ladder / price increment scale.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.series_id IS
  'Series grouping identifier. Applicable to Stock and ListedDerivative product types. '
  'NULL for Bond, Fund, Right, and other types.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.voting_rights IS
  'Whether this common stock carries shareholder voting rights. '
  'BOOLEAN. NULL for non-common-stock products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.dividend_type IS
  'Dividend policy for preferred stock. '
  'Values: CUMULATIVE (unpaid dividends accumulate), NON_CUMULATIVE. '
  'NULL for non-preferred-stock products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.total_amount_issued IS
  'Total face / principal amount issued for debt securities. DECIMAL(28,8). '
  'NULL for non-debt products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_currency_code IS
  'ISO 4217 three-letter currency code for the debt issuance (e.g. USD, EUR, GBP). '
  'NULL for non-debt products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.coupon_type IS
  'Bond coupon payment structure. '
  'Values: FIXED (constant rate), FLOATING (variable rate), ZERO (no periodic payment). '
  'NULL for non-bond products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.maturity_date IS
  'Date on which the bond principal is repaid and the security expires. DATE format. '
  'NULL for equities, funds, perpetual bonds, and non-debt products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.face_currency_code IS
  'ISO 4217 currency code for the bond face value and coupon payments. '
  'Sourced from the bond.issue_currency_code field. NULL for non-bond products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.day_count_convention IS
  'Day count convention used to accrue coupon interest (e.g. ACT/360, 30/360, ACT/ACT). '
  'NULL — not currently captured in source data.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.tax_exempt IS
  'Whether the municipal bond''s interest payments are exempt from federal income tax. '
  'BOOLEAN. NULL for non-municipal-bond products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.muni_state IS
  'US state of issuance for the municipal bond (e.g. CA, NY, TX). '
  'NULL for non-muni products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.muni_purpose IS
  'Purpose or project description funded by the municipal bond issuance. '
  'NULL for non-muni products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.pool_type IS
  'Type of asset pool backing the security (e.g. MBS for mortgage-backed, ABS for asset-backed). '
  'NULL for non-pool-backed products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.pool_originator IS
  'Institution that originated the underlying loan pool. '
  'NULL for non-pool-backed products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.endness_type IS
  'Fund structure type. '
  'Values: OPEN_END (shares created/redeemed on demand), CLOSED_END (fixed share count). '
  'NULL for non-fund products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.mutual_fund_type IS
  'Mutual fund sub-classification (e.g. equity, bond, money market, balanced). '
  'NULL for non-fund products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.subscription_ratio IS
  'Number of new shares entitlement per right held. DECIMAL(28,8). '
  'NULL — not currently captured in source data for rights.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.underlying_product_id IS
  'Foreign key back to dim_product identifying the underlying security for a derivative. '
  'Self-referencing join: dim_product.underlying_product_id = dim_product.product_id. '
  'NULL for non-derivative products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.option_type IS
  'Direction of the option contract. '
  'Values: CALL (right to buy), PUT (right to sell). '
  'NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.exercise_style IS
  'When the option can be exercised. '
  'Values: AMERICAN (any time before expiry), EUROPEAN (only at expiry). '
  'NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.strike_price IS
  'Price at which the option holder can buy (CALL) or sell (PUT) the underlying. '
  'DECIMAL(28,8). NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.expiry_date IS
  'Date after which the option contract becomes void. DATE format. '
  'NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.delivery_date IS
  'Date on which the futures contract is settled / the underlying is delivered. DATE format. '
  'NULL for non-future products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.valuation_method IS
  'Futures contract valuation approach (e.g. MARK_TO_MARKET, MARK_TO_MODEL). '
  'NULL for non-future products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product._dq_rule_version IS
  'SHA256 short-hash of the silver/rules.yaml file version used when the Silver source row was evaluated. '
  'Used to identify rows that need rescan after DQ rule changes.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product._ingestion_ts IS
  'Timestamp (UTC) when the source CSV row was loaded into the Bronze layer.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product._batch_id IS
  'Pipeline batch run identifier linking this row to a specific Bronze ingestion run.';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 2. `dim_legal_entity`
-- MAGIC **Grain:** one row per `legal_entity_id` (current Silver version only)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.g_statestreet.dim_legal_entity (
  legal_entity_id       STRING    NOT NULL,
  legal_name            STRING    NOT NULL,
  country               STRING,
  entity_type           STRING,
  effective_start_date  DATE      NOT NULL,
  effective_end_date    DATE      NOT NULL,
  _dq_rule_version      STRING,
  _ingestion_ts         TIMESTAMP,
  _batch_id             STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------
-- MAGIC %md ### 2.1 Build `dim_legal_entity`

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.g_statestreet.dim_legal_entity
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT
  le.legal_entity_id,
  le.legal_name,
  le.country,
  CAST(NULL AS STRING)          AS entity_type,
  le.effective_start_date,
  le.effective_end_date,
  le._dq_rule_version,
  le._ingestion_ts,
  le._batch_id
FROM statestreet.s_statestreet.legal_entity le
WHERE le.is_current = TRUE;

-- COMMAND ----------
-- MAGIC %md ### 2.2 DQ grain check — assert one row per legal_entity_id

-- COMMAND ----------

SELECT
  'dim_legal_entity grain violation' AS check_name,
  COUNT(*) AS violation_count
FROM (
  SELECT legal_entity_id, COUNT(*) AS cnt
  FROM statestreet.g_statestreet.dim_legal_entity
  GROUP BY legal_entity_id
  HAVING COUNT(*) > 1
);

-- COMMAND ----------
-- MAGIC %md ### 2.3 Row count summary — `dim_legal_entity`

-- COMMAND ----------

SELECT
  COUNT(*)                                                    AS total_entities,
  COUNT(DISTINCT country)                                     AS distinct_countries,
  SUM(CASE WHEN country IS NULL THEN 1 ELSE 0 END)           AS missing_country
FROM statestreet.g_statestreet.dim_legal_entity;

-- COMMAND ----------
-- MAGIC %md ### 2.4 Genie comments — `dim_legal_entity`

-- COMMAND ----------

COMMENT ON TABLE statestreet.g_statestreet.dim_legal_entity IS
  'Legal entity dimension. One row per active legal entity (issuers, counterparties, custodians). '
  'Join to dim_product on dim_product.issuer_legal_entity_id = dim_legal_entity.legal_entity_id '
  'to enrich securities data with issuer details. '
  'Source: Silver legal_entity table (is_current = TRUE rows only).';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.legal_entity_id IS
  'Unique identifier for the legal entity — primary key of the dimension. '
  'Referenced by dim_product.issuer_legal_entity_id.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.legal_name IS
  'Full registered legal name of the entity as provided by the source system.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.country IS
  'ISO 3166-1 alpha-2 country code where the entity is domiciled or incorporated. '
  'Example values: US (United States), GB (United Kingdom), DE (Germany), JP (Japan).';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.entity_type IS
  'Classification of the legal entity by business type. '
  'Example values: BANK, CORPORATE, GOVERNMENT, MUNICIPALITY, SOVEREIGN, SPV. '
  'NULL — not currently captured in source data.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.effective_start_date IS
  'SCD2 effective start date of this legal entity version in Silver. '
  'Carried to Gold for reference. Gold is a current-state snapshot.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.effective_end_date IS
  'SCD2 effective end date of this legal entity version in Silver (9999-12-31 for current). '
  'Carried to Gold for reference.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity._dq_rule_version IS
  'SHA256 short-hash of the silver/rules.yaml version used at DQ evaluation time.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity._ingestion_ts IS
  'Timestamp (UTC) when the source row was loaded into the Bronze layer.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity._batch_id IS
  'Pipeline batch run identifier linking this row to a specific Bronze ingestion run.';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 3. `fact_product_rating`
-- MAGIC **Grain:** one row per `product_id` × `effective_from_date` × `product_rating_type_id`

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS statestreet.g_statestreet.fact_product_rating (
  product_rating_id         STRING    NOT NULL,
  product_id                STRING    NOT NULL,
  product_rating_type_id    STRING,
  rating_value              STRING    NOT NULL,
  effective_from_date       DATE      NOT NULL,
  rating_agency             STRING,
  watch_code                STRING,
  rating_scale              STRING,
  rating_type_code          STRING,
  rating_type_description   STRING,
  rating_category           STRING,
  product_type              STRING,
  product_status            STRING,
  _dq_rule_version          STRING,
  _ingestion_ts             TIMESTAMP,
  _batch_id                 STRING
)
USING DELTA
PARTITIONED BY (product_type)
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- COMMAND ----------
-- MAGIC %md ### 3.1 Build `fact_product_rating`

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.g_statestreet.fact_product_rating
USING DELTA
PARTITIONED BY (product_type)
TBLPROPER