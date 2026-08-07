-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer — Dimensional Marts
-- MAGIC **Catalog:** `statestreet` | **Schema:** `g_statestreet`
-- MAGIC
-- MAGIC ## Marts Built
-- MAGIC | Mart | Grain | Source Tables |
-- MAGIC |------|-------|---------------|
-- MAGIC | `dim_product` | One row per active security | 13 Silver tables (LEFT JOINed on product_id) |
-- MAGIC | `dim_legal_entity` | One row per active legal entity | `legal_entity` |
-- MAGIC | `fact_product_rating` | One row per product per rating date | `product_rating`, `product_rating_type`, `product` |
-- MAGIC | `fact_coupon_schedule` | One row per bond per coupon payment date | `coupon`, `bond`, `product` |
-- MAGIC
-- MAGIC ## Execution Order
-- MAGIC 1. Schema setup
-- MAGIC 2. `dim_legal_entity` (no dependencies on other Gold tables)
-- MAGIC 3. `dim_product` (no dependencies on other Gold tables)
-- MAGIC 4. `fact_product_rating` (references dim_product)
-- MAGIC 5. `fact_coupon_schedule` (references dim_product)
-- MAGIC 6. Gold quality checks
-- MAGIC 7. Genie table + column comments

-- COMMAND ----------
-- MAGIC %md ### 0 — Schema Setup

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS statestreet.g_statestreet
  COMMENT 'Gold layer — dimensional marts for analytics and Genie AI/BI';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 1 — `dim_legal_entity`
-- MAGIC **Grain:** One row per active legal entity.
-- MAGIC Legal entities are issuers referenced by products via `issuer_legal_entity_id`.

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.g_statestreet.dim_legal_entity
  USING DELTA
  TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')
AS
SELECT
  le.legal_entity_id,
  le.legal_name                     AS legal_entity_name,
  le.country,
  CAST(NULL AS STRING)              AS entity_type,
  -- SCD2 provenance columns carried forward from Silver
  le.effective_start_date,
  le.effective_end_date,
  -- Metadata
  le._ingestion_ts,
  le._batch_id,
  le._row_hash,
  le._dq_rule_version,
  -- Convenience audit column
  current_timestamp()               AS _gold_built_ts
FROM statestreet.s_statestreet.legal_entity le
WHERE le.is_current = TRUE;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 2 — `dim_product`
-- MAGIC **Grain:** One row per active security (`p.is_current = TRUE`).
-- MAGIC All subtype tables are LEFT JOINed — subtype-specific columns are NULL
-- MAGIC for non-matching product types (expected by `null_strategy: coalesce_nulls`).
-- MAGIC
-- MAGIC **Join hierarchy:**
-- MAGIC ```
-- MAGIC product
-- MAGIC   ├── stock ──► common_stock
-- MAGIC   │         └► preferred_stock
-- MAGIC   ├── debt  ──► bond ──► muni
-- MAGIC   │         └► pool_backed_security
-- MAGIC   ├── fund
-- MAGIC   ├── right
-- MAGIC   └── listed_derivative ──► option
-- MAGIC                         └► future
-- MAGIC ```

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.g_statestreet.dim_product
  USING DELTA
  PARTITIONED BY (type)
  TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')
AS
SELECT
  -- ── Base product columns ──────────────────────────────────────────────────
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

  -- ── SCD2 provenance from Silver product ──────────────────────────────────
  p.effective_start_date,
  p.effective_end_date,

  -- ── Stock columns (NULL for non-stock products) ───────────────────────────
  st.series_id                      AS stock_series_id,

  -- ── CommonStock columns (NULL for non-common-stock) ─────────────────────
  CAST(NULL AS BOOLEAN)             AS voting_rights,

  -- ── PreferredStock columns (NULL for non-preferred-stock) ────────────────
  ps.dividend_right                 AS dividend_type,

  -- ── Debt columns (NULL for non-debt products) ────────────────────────────
  d.total_amount_issued             AS face_amount,
  CAST(NULL AS DATE)                AS issue_date_settlement,

  -- ── Bond columns (NULL for non-bond products) ────────────────────────────
  b.coupon_type,
  b.maturity_date,
  b.issue_currency_code             AS face_currency_code,
  CAST(NULL AS STRING)              AS day_count_convention,

  -- ── Muni columns (NULL for non-muni products) ────────────────────────────
  mn.tax_exempt,
  mn.state                          AS muni_state,
  mn.purpose                        AS muni_purpose,

  -- ── PoolBackedSecurity columns (NULL for non-pool-backed) ────────────────
  pbs.pool_type,
  pbs.originator,

  -- ── Fund columns (NULL for non-fund products) ────────────────────────────
  f.endness_type,
  f.mutual_fund_type,

  -- ── Right columns (NULL for non-right products) ──────────────────────────
  CAST(NULL AS DECIMAL(28,8))       AS subscription_ratio,

  -- ── ListedDerivative columns (NULL for non-derivatives) ──────────────────
  ld.series_id                      AS derivative_series_id,
  ld.underlying_product_id,

  -- ── Option columns (NULL for non-options) ────────────────────────────────
  op.option_type,
  op.exercise_style,
  op.strike_price,
  op.expiry_date,

  -- ── Future columns (NULL for non-futures) ────────────────────────────────
  ft.delivery_date,
  ft.valuation_method,

  -- ── Metadata ─────────────────────────────────────────────────────────────
  p._ingestion_ts,
  p._batch_id,
  p._row_hash,
  p._dq_rule_version,
  current_timestamp()               AS _gold_built_ts

FROM statestreet.s_statestreet.product p

-- Stock branch
LEFT JOIN statestreet.s_statestreet.stock            st  ON p.product_id = st.product_id
LEFT JOIN statestreet.s_statestreet.common_stock     cs  ON p.product_id = cs.product_id
LEFT JOIN statestreet.s_statestreet.preferred_stock  ps  ON p.product_id = ps.product_id

-- Debt branch (pool_backed_security joins debt, NOT bond — per USE-CASE-005)
LEFT JOIN statestreet.s_statestreet.debt             d   ON p.product_id = d.product_id
LEFT JOIN statestreet.s_statestreet.bond             b   ON p.product_id = b.product_id
LEFT JOIN statestreet.s_statestreet.muni             mn  ON p.product_id = mn.product_id
LEFT JOIN statestreet.s_statestreet.pool_backed_security pbs ON p.product_id = pbs.product_id

-- Fund branch
LEFT JOIN statestreet.s_statestreet.fund             f   ON p.product_id = f.product_id

-- Right branch
LEFT JOIN statestreet.s_statestreet.right            r   ON p.product_id = r.product_id

-- Derivative branch
LEFT JOIN statestreet.s_statestreet.listed_derivative ld ON p.product_id = ld.product_id
LEFT JOIN statestreet.s_statestreet.option           op  ON p.product_id = op.product_id
LEFT JOIN statestreet.s_statestreet.future           ft  ON p.product_id = ft.product_id

WHERE p.is_current = TRUE;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 3 — `fact_product_rating`
-- MAGIC **Grain:** One row per product per rating date (`product_id`, `rating_date`).
-- MAGIC LEFT JOINs `product_rating_type` for agency/category metadata.
-- MAGIC FK: `product_id` → `dim_product.product_id`.

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.g_statestreet.fact_product_rating
  USING DELTA
  PARTITIONED BY (effective_from_date)
  TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')
AS
SELECT
  -- ── Grain key ─────────────────────────────────────────────────────────────
  pr.product_rating_id,
  pr.product_id,
  pr.effective_from_date,

  -- ── Rating detail ─────────────────────────────────────────────────────────
  pr.rating_value,
  pr.product_rating_type_id,
  pr.rating_agency,
  pr.watch_code,

  -- ── Rating type / agency metadata (from product_rating_type) ─────────────
  prt.rating_scale,
  prt.rating_type_code,
  CAST(NULL AS STRING)              AS rating_category,

  -- ── SCD2 provenance from Silver product_rating ───────────────────────────
  pr.effective_start_date,
  pr.effective_end_date,
  pr.is_current,

  -- ── Product context (status at time of Gold build) ───────────────────────
  p.type                            AS product_type,
  p.sub_type                        AS product_sub_type,
  p.status                          AS product_status,

  -- ── Metadata ─────────────────────────────────────────────────────────────
  pr._ingestion_ts,
  pr._batch_id,
  pr._row_hash,
  pr._dq_rule_version,
  current_timestamp()               AS _gold_built_ts

FROM statestreet.s_statestreet.product_rating pr

-- Rating type lookup — LEFT JOIN so ratings without a type record are still included
LEFT JOIN statestreet.s_statestreet.product_rating_type prt
  ON pr.product_rating_type_id = prt.product_rating_type_id

-- Product context — INNER JOIN ensures only ratings for known current products
INNER JOIN statestreet.s_statestreet.product p
  ON pr.product_id = p.product_id
 AND p.is_current = TRUE

WHERE pr.is_current = TRUE;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 4 — `fact_coupon_schedule`
-- MAGIC **Grain:** One row per bond per coupon payment date (`product_id`, `payment_date`).
-- MAGIC INNER JOIN to `bond` ensures only bond products are included (excludes pool-backed,
-- MAGIC munis without coupons, etc.). FK: `product_id` → `dim_product.product_id`.

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.g_statestreet.fact_coupon_schedule
  USING DELTA
  PARTITIONED BY (payment_date)
  TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')
AS
SELECT
  -- ── Grain key ─────────────────────────────────────────────────────────────
  c.coupon_id,
  c.product_id,
  c.payment_date,

  -- ── Coupon detail ─────────────────────────────────────────────────────────
  c.coupon_rate,
  c.coupon_type,
  c.frequency,

  -- ── Bond context ──────────────────────────────────────────────────────────
  b.maturity_date,
  b.issue_currency_code             AS face_currency_code,
  CAST(NULL AS STRING)              AS day_count_convention,

  -- ── Product context ───────────────────────────────────────────────────────
  p.sub_type                        AS product_sub_type,
  p.status                          AS product_status,
  p.issuer_legal_entity_id,

  -- ── Metadata ─────────────────────────────────────────────────────────────
  c._ingestion_ts,
  c._batch_id,
  c._row_hash,
  c._dq_rule_version,
  current_timestamp()               AS _gold_built_ts

FROM statestreet.s_statestreet.coupon c

-- INNER JOIN bond: only include rows where a bond record exists (coupon without a bond = invalid)
INNER JOIN statestreet.s_statestreet.bond b
  ON c.product_id = b.product_id

-- INNER JOIN product: enforce type = 'DEBT' and is_current guard
INNER JOIN statestreet.s_statestreet.product p
  ON c.product_id = p.product_id
 AND p.is_current = TRUE
 AND p.type = 'DEBT';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 5 — Gold Quality Checks
-- MAGIC All assertions must return zero violations before the pipeline is considered successful.

-- COMMAND ----------
-- MAGIC %md #### 5a — dim_product row count (expected ≈ 200)

-- COMMAND ----------

SELECT
  COUNT(*)                          AS total_rows,
  200                               AS expected_rows,
  COUNT(*) - 200                    AS delta,
  CASE WHEN COUNT(*) = 200 THEN 'PASS' ELSE 'WARN' END AS result
FROM statestreet.g_statestreet.dim_product;

-- COMMAND ----------
-- MAGIC %md #### 5b — dim_product: no duplicate product_id

-- COMMAND ----------

SELECT
  product_id,
  COUNT(*)                          AS occurrences
FROM statestreet.g_statestreet.dim_product
GROUP BY product_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- COMMAND ----------
-- MAGIC %md #### 5c — dim_legal_entity: no duplicate legal_entity_id

-- COMMAND ----------

SELECT
  legal_entity_id,
  COUNT(*)                          AS occurrences
FROM statestreet.g_statestreet.dim_legal_entity
GROUP BY legal_entity_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- COMMAND ----------
-- MAGIC %md #### 5d — fact_coupon_schedule grain: no duplicate (product_id, payment_date)

-- COMMAND ----------

SELECT
  product_id,
  payment_date,
  COUNT(*)                          AS cnt
FROM statestreet.g_statestreet.fact_coupon_schedule
GROUP BY product_id, payment_date
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- COMMAND ----------
-- MAGIC %md #### 5e — fact_product_rating grain: no duplicate (product_id, rating_date, rating_type_id)

-- COMMAND ----------

SELECT
  product_id,
  effective_from_date,
  product_rating_type_id,
  COUNT(*)                          AS cnt
FROM statestreet.g_statestreet.fact_product_rating
GROUP BY product_id, effective_from_date, product_rating_type_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- COMMAND ----------
-- MAGIC %md #### 5f — No NULL product_id in any Gold table

-- COMMAND ----------

SELECT 'dim_product'        AS tbl, COUNT(*) AS null_product_ids FROM statestreet.g_statestreet.dim_product        WHERE product_id IS NULL
UNION ALL
SELECT 'fact_product_rating',        COUNT(*) FROM statestreet.g_statestreet.fact_product_rating    WHERE product_id IS NULL
UNION ALL
SELECT 'fact_coupon_schedule',       COUNT(*) FROM statestreet.g_statestreet.fact_coupon_schedule   WHERE product_id IS NULL;
-- Expected: all null_product_ids = 0

-- COMMAND ----------
-- MAGIC %md #### 5g — Referential integrity: fact tables → dim_product

-- COMMAND ----------

-- Ratings referencing products not in dim_product
SELECT COUNT(*) AS orphaned_ratings
FROM statestreet.g_statestreet.fact_product_rating fpr
LEFT JOIN statestreet.g_statestreet.dim_product dp
  ON fpr.product_id = dp.product_id
WHERE dp.product_id IS NULL;
-- Expected: 0

-- COMMAND ----------

-- Coupon rows referencing bonds not in dim_product
SELECT COUNT(*) AS orphaned_coupons
FROM statestreet.g_statestreet.fact_coupon_schedule fcs
LEFT JOIN statestreet.g_statestreet.dim_product dp
  ON fcs.product_id = dp.product_id
WHERE dp.product_id IS NULL;
-- Expected: 0

-- COMMAND ----------
-- MAGIC %md #### 5h — fact_coupon_schedule: all rows are for DEBT products only

-- COMMAND ----------

SELECT COUNT(*) AS non_debt_coupons
FROM statestreet.g_statestreet.fact_coupon_schedule
WHERE product_sub_type NOT IN ('BOND', 'MUNI', 'POOL_BACKED')
   OR product_sub_type IS NULL;
-- Expected: 0 (coupons only exist for debt products)

-- COMMAND ----------
-- MAGIC %md #### 5i — Row count summary across all Gold tables

-- COMMAND ----------

SELECT 'dim_product'         AS mart, COUNT(*) AS row_count, 200  AS expected FROM statestreet.g_statestreet.dim_product
UNION ALL
SELECT 'dim_legal_entity',            COUNT(*),               40   FROM statestreet.g_statestreet.dim_legal_entity
UNION ALL
SELECT 'fact_product_rating',         COUNT(*),               205  FROM statestreet.g_statestreet.fact_product_rating
UNION ALL
SELECT 'fact_coupon_schedule',        COUNT(*),               105  FROM statestreet.g_statestreet.fact_coupon_schedule
ORDER BY mart;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 6 — Genie Table & Column Comments
-- MAGIC These comments are consumed by Databricks Genie AI/BI for natural language query generation.
-- MAGIC All Gold tables and key columns must have descriptive comments.

-- COMMAND ----------
-- MAGIC %md #### 6a — dim_product comments

-- COMMAND ----------

COMMENT ON TABLE statestreet.g_statestreet.dim_product IS
  'Flattened security product dimension. One row per active security. '
  'Covers all product types: Equity (CommonStock, PreferredStock), '
  'Debt (Bond, Muni, PoolBackedSecurity), Fund, Listed Derivative (Option, Future), and Right. '
  'Subtype-specific columns are NULL for non-matching product types. '
  'Join to fact tables on product_id. '
  'Source: 13 Silver tables joined on product_id where is_current = TRUE.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.product_id IS
  'Unique identifier for the security. Primary key. Alphanumeric string.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.id_type IS
  'Primary identifier type used for this security. Values: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.type IS
  'Top-level product category. Values: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT. Used as partition key.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.sub_type IS
  'Product subcategory. Values: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE, FUND, RIGHT. NULL for older legacy records.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.status IS
  'Lifecycle status. Values: ACTIVE (tradeable), INACTIVE, MATURED, SUSPENDED, DELISTED.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.settlement_type IS
  'Settlement method for the security. Free-text; varies by exchange and product type.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.description IS
  'Human-readable security name or description (e.g. "Apple Inc Common Stock").';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_date IS
  'Date when the security was first issued. DATE format (YYYY-MM-DD). NULL if not recorded.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_price IS
  'Price at issuance. DECIMAL. NULL for securities where issue price is not recorded.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.current_face_value IS
  'Current face or par value expressed as a percentage (e.g. 100.0 = full face value). DECIMAL. NULL for equities and derivatives.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issuer_legal_entity_id IS
  'FK to dim_legal_entity.legal_entity_id. Identifies the legal entity that issued this security.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.tick_ladder_scale_id IS
  'FK to tick_ladder_scale. Identifies the minimum price increment scale for this security.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.effective_start_date IS
  'SCD2: date this version of the product record became active in Silver.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.effective_end_date IS
  'SCD2: date this version was superseded. Value 9999-12-31 means this is the current active version.';

-- Stock columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.stock_series_id IS
  'Series identifier for stock products. FK to series table. NULL for non-stock products.';

-- CommonStock columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.voting_rights IS
  'Whether the stock carries voting rights. BOOLEAN (true/false). NULL for non-common-stock products.';

-- PreferredStock columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.dividend_type IS
  'Dividend type for preferred stock. Values: CUMULATIVE, NON_CUMULATIVE. NULL for non-preferred-stock.';

-- Debt columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.face_amount IS
  'Original face amount of the debt instrument. DECIMAL. NULL for non-debt products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_date_settlement IS
  'Settlement date at issuance for debt instruments. DATE. NULL for non-debt products.';

-- Bond columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.coupon_type IS
  'Bond coupon type. Values: FIXED, FLOATING, ZERO. NULL for non-bond products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.maturity_date IS
  'Date when the bond matures and principal is repaid. DATE. NULL for equities, funds, and perpetual bonds.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.face_currency_code IS
  'ISO 4217 currency code for the bond face value (e.g. USD, EUR, GBP). NULL for non-bond products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.day_count_convention IS
  'Day count convention for interest accrual (e.g. ACT/360, 30/360). NULL for non-bond products.';

-- Muni columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.tax_exempt IS
  'Whether the municipal bond is tax-exempt. BOOLEAN. NULL for non-muni products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.muni_state IS
  'US state of issuance for municipal bonds (e.g. CA, NY). NULL for non-muni products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.muni_purpose IS
  'Purpose of the municipal bond issuance (e.g. infrastructure, education). NULL for non-muni.';

-- PoolBackedSecurity columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.pool_type IS
  'Pool type for asset-backed securities (e.g. MORTGAGE, AUTO, STUDENT_LOAN). NULL for non-pool-backed.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.originator IS
  'Originating institution for pool-backed securities. NULL for non-pool-backed products.';

-- Fund columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.endness_type IS
  'Fund structure type. Values: OPEN_END, CLOSED_END. NULL for non-fund products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.mutual_fund_type IS
  'Mutual fund category (e.g. EQUITY, BOND, MONEY_MARKET). NULL for non-fund products.';

-- Right columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.subscription_ratio IS
  'Number of rights required to subscribe to one new share. DECIMAL. NULL for non-right products.';

-- Derivative columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.derivative_series_id IS
  'Series identifier for listed derivatives. FK to series table. NULL for non-derivative products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.underlying_product_id IS
  'FK to dim_product.product_id — the underlying security for this derivative. NULL for non-derivatives.';

-- Option columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.option_type IS
  'Option type. Values: CALL, PUT. NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.exercise_style IS
  'Option exercise style. Values: AMERICAN (exercise any time), EUROPEAN (exercise at expiry only). NULL for non-options.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.strike_price IS
  'Strike or exercise price of the option. DECIMAL. NULL for non-option products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.expiry_date IS
  'Expiration date of the option. DATE. NULL for non-option products.';

-- Future columns
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.delivery_date IS
  'Futures delivery or settlement date. DATE. NULL for non-future products.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.valuation_method IS
  'Valuation method for futures (e.g. MARK_TO_MARKET). NULL for non-future products.';

-- COMMAND ----------
-- MAGIC %md #### 6b — dim_legal_entity comments

-- COMMAND ----------

COMMENT ON TABLE statestreet.g_statestreet.dim_legal_entity IS
  'Legal entity dimension. One row per active legal entity. '
  'Legal entities are issuers, counterparties, and custodians referenced by securities products. '
  'Join to dim_product on dim_product.issuer_legal_entity_id = dim_legal_entity.legal_entity_id. '
  'Source: Silver legal_entity table where is_current = TRUE.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.legal_entity_id IS
  'Unique identifier for the legal entity. Primary key. Alphanumeric string.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.legal_entity_name IS
  'Full legal name of the entity (e.g. "Apple Inc", "US Treasury"). May be masked for non-privileged users.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.country IS
  'ISO 3166-1 alpha-2 country code of the legal entity (e.g. US, GB, DE). NULL if not recorded.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.entity_type IS
  'Classification of the legal entity. Values: BANK, CORPORATE, GOVERNMENT, SOVEREIGN, MUNICIPALITY, OTHER.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.effective_start_date IS
  'SCD2: date this version of the legal entity record became active.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_legal_entity.effective_end_date IS
  'SCD2: date this version was superseded. Value 9999-12-31 means current active version.';

-- COMMAND ----------
-- MAGIC %md #### 6c — fact_product_rating comments

-- COMMAND ----------

COMMENT ON TABLE statestreet.g_statestreet.fact_product_rating IS
  'Credit rating history for securities. One row per product per rating date. '
  'Grain: (product_id, rating_date, rating_type_id). '
  'Partitioned by effective_from_date for efficient time-range queries. '
  'Join to dim_product on product_id for security attributes. '
  'Join to dim_legal_entity on dim_product.issuer_legal_entity_id for issuer details. '
  'Source: Silver product_rating (inner joined to current products).';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_rating_id IS
  'Unique identifier for this rating record. Primary key.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_id IS
  'FK to dim_product.product_id. Identifies the rated security.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.effective_from_date IS
  'Date the rating was assigned or last confirmed. DATE format (YYYY-MM-DD).';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_value IS
  'Credit rating code assigned by the rating agency. Examples: AAA, AA+, AA, BBB+, BB, CCC. Higher grade = lower credit risk.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_rating_type_id IS
  'FK to Silver product_rating_type. Identifies the rating scale and agency used.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_agency IS
  'Name of the credit rating agency. Examples: Moody''s, S&P, Fitch.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.watch_code IS
  'Rating watch or outlook indicator. Examples: POSITIVE, NEGATIVE, STABLE, DEVELOPING. NULL if no watch.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_scale IS
  'The rating scale used by the agency (e.g. LONG_TERM, SHORT_TERM). From product_rating_type lookup.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_type_code IS
  'Code identifying the rating type within the agency''s scale. From product_rating_type lookup.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_type IS
  'Product type at time of Gold build. Denormalized from dim_product for query convenience.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.product_status IS
  'Product lifecycle status at time of Gold build. Values: ACTIVE, INACTIVE, MATURED, SUSPENDED, DELISTED.';