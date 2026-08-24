```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer — Securities Master Analytics Mart
-- MAGIC
-- MAGIC Builds the Gold wide table `statestreet.g_statestreet.securities_master`.
-- MAGIC
-- MAGIC **Grain:** One row per active security version (`is_current = TRUE`).
-- MAGIC
-- MAGIC **Sources (Silver):**
-- MAGIC - `statestreet.s_statestreet.product`
-- MAGIC - `statestreet.s_statestreet.bond`
-- MAGIC - `statestreet.s_statestreet.muni`
-- MAGIC - `statestreet.s_statestreet.debt`
-- MAGIC - `statestreet.s_statestreet.stock`
-- MAGIC - `statestreet.s_statestreet.common_stock`
-- MAGIC - `statestreet.s_statestreet.preferred_stock`
-- MAGIC - `statestreet.s_statestreet.fund`
-- MAGIC - `statestreet.s_statestreet.right`
-- MAGIC - `statestreet.s_statestreet.listed_derivative`
-- MAGIC - `statestreet.s_statestreet.option`
-- MAGIC - `statestreet.s_statestreet.future`
-- MAGIC - `statestreet.s_statestreet.legal_entity`
-- MAGIC - `statestreet.s_statestreet.coupon`
-- MAGIC - `statestreet.s_statestreet.identifiers`
-- MAGIC - `statestreet.s_statestreet.product_rating`
-- MAGIC
-- MAGIC **REQ coverage:** REQ-01 (product base), REQ-02 (bond/muni net settlement),
-- MAGIC REQ-03 (accrued interest calculation), REQ-04 (is_current filter).

-- COMMAND ----------
-- MAGIC %md ## 0 · Ensure Gold schema exists

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS statestreet.g_statestreet
  COMMENT 'Gold layer — dimensional analytics marts for Securities Master';

-- COMMAND ----------
-- MAGIC %md ## 1 · Build `statestreet.g_statestreet.securities_master`
-- MAGIC
-- MAGIC The mart is rebuilt as `CREATE OR REPLACE TABLE … AS SELECT` so every run is
-- MAGIC idempotent.  Delta UniForm (Iceberg) is enabled so external Iceberg readers
-- MAGIC can query the table without a separate copy.
-- MAGIC
-- MAGIC `net_settlement_amount` is populated only for rows where:
-- MAGIC   - `p.type IN ('DEBT')` **and** `p.sub_type IN ('BOND', 'MUNI')`
-- MAGIC   - Both `b.principal_amount` and `b.accrued_interest_rate` are non-NULL
-- MAGIC   - `p.is_current = TRUE`
-- MAGIC
-- MAGIC Formula (REQ-02 / REQ-03):
-- MAGIC ```
-- MAGIC net_settlement_amount = principal_amount × (1 + accrued_interest_rate)
-- MAGIC ```

-- COMMAND ----------

CREATE OR REPLACE TABLE statestreet.g_statestreet.securities_master
  USING DELTA
  TBLPROPERTIES (
    'delta.columnMapping.mode'             = 'name',
    'delta.enableIcebergCompatV2'          = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
  )
  PARTITIONED BY (type)
AS
WITH

-- ── Silver base product (current versions only — REQ-04) ─────────────────────
current_product AS (
  SELECT *
  FROM statestreet.s_statestreet.product
  WHERE is_current = TRUE
),

-- ── Latest coupon per bond (max payment_date row) ────────────────────────────
latest_coupon AS (
  SELECT
    product_id,
    coupon_rate,
    coupon_type,
    frequency,
    payment_date                                     AS latest_payment_date,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY payment_date DESC
    )                                                AS _rn
  FROM statestreet.s_statestreet.coupon
),
latest_coupon_dedup AS (
  SELECT * FROM latest_coupon WHERE _rn = 1
),

-- ── Primary identifier per product (prefer ISIN > CUSIP > SEDOL > TICKER > BLOOMBERG_ID) ──
primary_identifier AS (
  SELECT
    product_id,
    id_type                                          AS primary_id_type,
    identifier_value                                 AS primary_identifier_value,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE id_type
          WHEN 'ISIN'         THEN 1
          WHEN 'CUSIP'        THEN 2
          WHEN 'SEDOL'        THEN 3
          WHEN 'TICKER'       THEN 4
          WHEN 'BLOOMBERG_ID' THEN 5
          ELSE                     6
        END
    )                                                AS _rn
  FROM statestreet.s_statestreet.identifiers
),
primary_identifier_dedup AS (
  SELECT * FROM primary_identifier WHERE _rn = 1
),

-- ── Latest rating per product ────────────────────────────────────────────────
latest_rating AS (
  SELECT
    product_id,
    rating_value,
    effective_from_date                              AS rating_date,
    rating_agency,
    product_rating_type_id,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY effective_from_date DESC
    )                                                AS _rn
  FROM statestreet.s_statestreet.product_rating
  WHERE is_current = TRUE
),
latest_rating_dedup AS (
  SELECT * FROM latest_rating WHERE _rn = 1
)

-- ── Final wide select ────────────────────────────────────────────────────────
SELECT

  -- ── Identity ──────────────────────────────────────────────────────────────
  p.product_id,
  p.id_type,
  p.type,
  p.sub_type,
  p.status,
  p.description,

  -- ── Primary external identifier ───────────────────────────────────────────
  pid.primary_id_type,
  pid.primary_identifier_value,

  -- ── Issuer ────────────────────────────────────────────────────────────────
  p.issuer_legal_entity_id,
  le.legal_name                                      AS issuer_legal_name,
  le.country                                         AS issuer_country,

  -- ── Issue economics ───────────────────────────────────────────────────────
  p.issue_date,
  p.issue_price,
  p.current_face_value,

  -- ── SCD2 version window ───────────────────────────────────────────────────
  p.effective_start_date,
  p.effective_end_date,

  -- ── Stock attributes (NULL for non-equity) ────────────────────────────────
  cs.voting_rights                                   AS common_stock_voting_rights,
  ps.dividend_right                                  AS preferred_stock_dividend_type,
  st.series_id                                       AS stock_series_id,

  -- ── Fund attributes (NULL for non-fund) ──────────────────────────────────
  f.endness_type                                     AS fund_endness_type,
  f.mutual_fund_type                                 AS fund_mutual_fund_type,

  -- ── Debt / Bond attributes (NULL for non-debt) ────────────────────────────
  d.total_amount_issued                              AS debt_face_amount,
  b.coupon_type                                      AS bond_coupon_type,
  b.maturity_date                                    AS bond_maturity_date,
  b.issue_currency_code                              AS bond_face_currency_code,

  -- ── Muni-specific (NULL for non-muni) ────────────────────────────────────
  mn.tax_exempt                                      AS muni_tax_exempt,
  mn.state                                           AS muni_state,
  mn.purpose                                         AS muni_purpose,

  -- ── Pool-backed security (NULL for non-pool) ─────────────────────────────
  pbs.pool_type                                      AS pool_backed_pool_type,
  pbs.originator                                     AS pool_backed_originator,

  -- ── Latest coupon (NULL for non-bond / zero-coupon) ──────────────────────
  lc.coupon_rate                                     AS latest_coupon_rate,
  lc.coupon_type                                     AS latest_coupon_type,
  lc.frequency                                       AS latest_coupon_frequency,
  lc.latest_payment_date                             AS latest_coupon_payment_date,

  -- ── Listed derivative attributes (NULL for non-derivative) ───────────────
  ld.underlying_product_id                           AS derivative_underlying_product_id,
  ld.series_id                                       AS derivative_series_id,
  op.option_type                                     AS option_type,
  op.exercise_style                                  AS option_exercise_style,
  op.strike_price                                    AS option_strike_price,
  op.expiry_date                                     AS option_expiry_date,
  ft.delivery_date                                   AS future_delivery_date,
  ft.valuation_method                                AS future_valuation_method,

  -- ── Latest credit rating (NULL if no rating exists) ──────────────────────
  lr.rating_value                                    AS latest_rating_value,
  lr.rating_date                                     AS latest_rating_date,
  lr.rating_agency                                   AS latest_rating_agency,
  lr.product_rating_type_id                          AS latest_rating_type_id,

  -- ── net_settlement_amount (REQ-01, REQ-02, REQ-03, REQ-04) ───────────────
  -- Populated only for BOND and MUNI sub_types where both inputs are non-NULL.
  -- Formula: principal_amount × (1 + accrued_interest_rate)
  CASE
    WHEN p.type    = 'DEBT'
     AND p.sub_type IN ('BOND', 'MUNI')
     AND b.principal_amount       IS NOT NULL
     AND b.accrued_interest_rate  IS NOT NULL
    THEN CAST(
           b.principal_amount * (1 + b.accrued_interest_rate)
           AS DECIMAL(18,6)
         )
    ELSE CAST(NULL AS DECIMAL(18,6))
  END                                                AS net_settlement_amount,

  -- ── Pipeline provenance ───────────────────────────────────────────────────
  p._dq_rule_version,
  current_timestamp()                                AS _gold_built_at

FROM current_product                                 AS p

-- External identifier
LEFT JOIN primary_identifier_dedup                   AS pid
  ON p.product_id = pid.product_id

-- Issuer legal entity
LEFT JOIN statestreet.s_statestreet.legal_entity     AS le
  ON p.issuer_legal_entity_id = le.legal_entity_id
 AND le.is_current = TRUE

-- Stock hierarchy
LEFT JOIN statestreet.s_statestreet.stock            AS st
  ON p.product_id = st.product_id
 AND st.is_current = TRUE

LEFT JOIN statestreet.s_statestreet.common_stock     AS cs
  ON p.product_id = cs.product_id
 AND cs.is_current = TRUE

LEFT JOIN statestreet.s_statestreet.preferred_stock  AS ps
  ON p.product_id = ps.product_id
 AND ps.is_current = TRUE

-- Fund
LEFT JOIN statestreet.s_statestreet.fund             AS f
  ON p.product_id = f.product_id
 AND f.is_current = TRUE

-- Debt hierarchy
LEFT JOIN statestreet.s_statestreet.debt             AS d
  ON p.product_id = d.product_id
 AND d.is_current = TRUE

LEFT JOIN statestreet.s_statestreet.bond             AS b
  ON p.product_id = b.product_id
 AND b.is_current = TRUE

LEFT JOIN statestreet.s_statestreet.muni             AS mn
  ON p.product_id = mn.product_id
 AND mn.is_current = TRUE

LEFT JOIN statestreet.s_statestreet.pool_backed_security AS pbs
  ON p.product_id = pbs.product_id
 AND pbs.is_current = TRUE

-- Latest coupon
LEFT JOIN latest_coupon_dedup                        AS lc
  ON p.product_id = lc.product_id

-- Listed derivatives
LEFT JOIN statestreet.s_statestreet.listed_derivative AS ld
  ON p.product_id = ld.product_id
 AND ld.is_current = TRUE

LEFT JOIN statestreet.s_statestreet.option           AS op
  ON p.product_id = op.product_id
 AND op.is_current = TRUE

LEFT JOIN statestreet.s_statestreet.future           AS ft
  ON p.product_id = ft.product_id
 AND ft.is_current = TRUE

-- Latest credit rating
LEFT JOIN latest_rating_dedup                        AS lr
  ON p.product_id = lr.product_id;

-- COMMAND ----------
-- MAGIC %md ## 2 · Post-build: OPTIMIZE for query performance

-- COMMAND ----------

OPTIMIZE statestreet.g_statestreet.securities_master
  ZORDER BY (status, type, sub_type);

-- COMMAND ----------
-- MAGIC %md ## 3 · Data Quality checks on Gold output

-- COMMAND ----------

-- DQ-GOLD-01: Grain check — one row per product_id
SELECT
  'DQ-GOLD-01' AS check_id,
  'Grain: one row per product_id'  AS description,
  COUNT(*)                          AS total_rows,
  COUNT(DISTINCT product_id)        AS distinct_products,
  CASE
    WHEN COUNT(*) = COUNT(DISTINCT product_id) THEN 'PASS'
    ELSE 'FAIL'
  END                               AS result
FROM statestreet.g_statestreet.securities_master;

-- COMMAND ----------

-- DQ-GOLD-02: net_settlement_amount is NULL for all non-DEBT/non-BOND/MUNI rows
SELECT
  'DQ-GOLD-02'                                             AS check_id,
  'net_settlement_amount NULL for non-bond/muni rows'      AS description,
  COUNT(*)                                                 AS violation_count
FROM statestreet.g_statestreet.securities_master
WHERE net_settlement_amount IS NOT NULL
  AND NOT (type = 'DEBT' AND sub_type IN ('BOND', 'MUNI'));

-- COMMAND ----------

-- DQ-GOLD-03: net_settlement_amount is non-negative where populated
SELECT
  'DQ-GOLD-03'                                          AS check_id,
  'net_settlement_amount >= 0 where populated'          AS description,
  COUNT(*)                                              AS violation_count
FROM statestreet.g_statestreet.securities_master
WHERE net_settlement_amount IS NOT NULL
  AND net_settlement_amount < 0;

-- COMMAND ----------

-- DQ-GOLD-04: No NULL product_id in the mart
SELECT
  'DQ-GOLD-04'                    AS check_id,
  'No NULL product_id in Gold'    AS description,
  COUNT(*)                        AS violation_count
FROM statestreet.g_statestreet.securities_master
WHERE product_id IS NULL;

-- COMMAND ----------

-- DQ-GOLD-05: type values are within the known domain
SELECT
  'DQ-GOLD-05'                                         AS check_id,
  'type in allowed domain'                             AS description,
  COUNT(*)                                             AS violation_count
FROM statestreet.g_statestreet.securities_master
WHERE type NOT IN ('EQUITY', 'DEBT', 'FUND', 'DERIVATIVE', 'RIGHT');

-- COMMAND ----------

-- DQ-GOLD-06: Row count must be > 0 (mart is not empty)
SELECT
  'DQ-GOLD-06'            AS check_id,
  'Mart is not empty'     AS description,
  COUNT(*)                AS row_count,
  CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS result
FROM statestreet.g_statestreet.securities_master;

-- COMMAND ----------
-- MAGIC %md ## 4 · Genie AI/BI — Table and column comments

-- COMMAND ----------

COMMENT ON TABLE statestreet.g_statestreet.securities_master IS
  'Gold Securities Master analytics mart. '
  'Grain: one row per active security (is_current = TRUE in Silver). '
  'Covers all product types: EQUITY (CommonStock, PreferredStock), '
  'DEBT (Bond, Muni, PoolBackedSecurity), FUND, DERIVATIVE (Option, Future), and RIGHT. '
  'Type-specific columns are NULL when not applicable to the product type. '
  'net_settlement_amount is computed only for BOND and MUNI sub-types. '
  'Source layers: 16 Silver tables joined on product_id. '
  'Partitioned by type for efficient segment-scoped queries.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.product_id IS
  'Unique identifier for the security product. Primary key of this mart. '
  'Alphanumeric string assigned by the source system.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.id_type IS
  'Identifier type stored in the product base record. '
  'Values: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.type IS
  'Top-level product classification. '
  'Values: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT. '
  'This column is the partition key — always include it in WHERE clauses for best performance.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.sub_type IS
  'Secondary product classification within type. '
  'Values: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE, FUND, RIGHT.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.status IS
  'Lifecycle status of the security. '
  'Values: ACTIVE (currently tradeable), INACTIVE, MATURED, SUSPENDED, DELISTED.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.description IS
  'Human-readable security name or description as provided by the source system.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.primary_id_type IS
  'Type of the primary external identifier selected for this product. '
  'Selection priority: ISIN > CUSIP > SEDOL > TICKER > BLOOMBERG_ID.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.primary_identifier_value IS
  'Value of the primary external identifier (e.g. the ISIN or CUSIP string). '
  'Corresponds to primary_id_type.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issuer_legal_entity_id IS
  'Foreign key to dim_legal_entity (and Silver legal_entity). '
  'Identifies the issuing organisation for this security.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issuer_legal_name IS
  'Full legal name of the issuing entity. '
  'Sourced from statestreet.s_statestreet.legal_entity.legal_name. '
  'NULL when the issuer is not present in the legal entity reference table.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issuer_country IS
  'ISO 3166-1 alpha-2 country code of the issuing legal entity (e.g. US, GB, DE). '
  'NULL when the issuer is unknown or not in the reference table.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issue_date IS
  'Date when the security was first issued. DATE (YYYY-MM-DD). '
  'NULL for some derivative and legacy records.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issue_price IS
  'Price at which the security was originally issued. '
  'Currency is implied by bond_face_currency_code for debt securities.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.current_face_value IS
  'Current face/par value of the security as a percentage (0–100). '
  'Relevant primarily for debt instruments.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.effective_start_date IS
  'SCD2 start date — the date from which this version of the product record became active.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.effective_end_date IS
  'SCD2 end date — the date on which this version was superseded. '
  'Value is 9999-12-31 for the current version (which is the only version in this mart).';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.common_stock_voting_rights IS
  'Indicates whether this common stock carries voting rights. '
  'NULL for all product types other than COMMON_STOCK.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.preferred_stock_dividend_type IS
  'Dividend entitlement type for preferred stock. '
  'Values: CUMULATIVE, NON_CUMULATIVE. '
  'NULL for all product types other than PREFERRED_STOCK.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.stock_series_id IS
  'Series grouping identifier for stock securities. '
  'NULL for non-equity product types.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.fund_endness_type IS
  'Open-ended or closed-ended classification for fund products. '
  'Values: OPEN_END, CLOSED_END. '
  'NULL for all product types other than FUND.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.fund_mutual_fund_type IS
  'Sub-classification of mutual fund type. '
  'NULL for all product types other than FUND.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.debt_face_amount IS
  'Total amount issued for debt securities (face/notional). '
  'Sourced from statestreet.s_statestreet.debt.total_amount_issued. '
  'NULL for non-debt product types.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.bond_coupon_type IS
  'Coupon payment structure for bond securities. '
  'Values: FIXED, FLOATING, ZERO. '
  'NULL for non-bond product types.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.bond_maturity_date IS
  'Date on which the bond principal is repaid. DATE (YYYY-MM-DD). '
  'NULL for perpetual bonds and non-debt product types.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.bond_face_currency_code IS
  'ISO 4217 three-letter currency code for the bond face value (e.g. USD, EUR, GBP). '
  'NULL for non-bond product types.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.muni_tax_exempt IS
  'Whether this municipal bond is tax-exempt. BOOLEAN. '
  'NULL for all product types other than MUNI.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.muni_state IS
  'US state of issuance for municipal bonds (e.g. CA, NY, TX). '
  'NULL for all product types other than MUNI.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.muni_purpose IS
  'Stated purpose of the municipal bond issuance (e.g. education, infrastructure). '
  'NULL for all product types other than MUNI.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.pool_backed_pool_type IS
  'Type of asset pool backing the security (e.g. MORTGAGE, AUTO, STUDENT_LOAN). '
  'NULL for all product types other than POOL_BACKED.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.pool_backed_originator IS
  'Name of the institution that originated the underlying asset pool. '
  'NULL for all product types other than POOL_BACKED.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_coupon_rate IS
  'Most recent annual coupon rate as a decimal (e.g. 0.05 = 5.00%). '
  'Taken from the coupon record with the latest payment_date. '
  'NULL for zero-coupon bonds and non-debt product types.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_coupon_type IS
  'Coupon type of the most recent coupon record. '
  'Values: FIXED, FLOATING. '
  'NULL for zero-coupon bonds and non-debt product types.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_coupon_frequency IS
  'Payment frequency of the most recent coupon. '
  'Values: ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY. '
  'NULL when no coupon schedule exists.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_coupon_payment_date IS
  'Date of the most recent (or next scheduled) coupon payment. DATE (YYYY-MM-DD). '
  'NULL when no coupon schedule exists.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.derivative_underlying_product_id IS
  'product_id of the underlying security for listed derivatives (options and futures). '
  'References product_id in this same mart. '
  'NULL for all non-derivative product types.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.derivative_series_id IS
  'Series grouping identifier for listed derivative securities. '
  'NULL for all non-derivative product types.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.option_type IS
  'Direction of the option contract. Values: CALL, PUT. '
  'NULL for all product types other than OPTION.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.option_exercise_style IS
  'Exercise convention for the option. Values: AMERICAN (any time), EUROPEAN (expiry only). '
  'NULL for all product types other than OPTION.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.option_strike_price IS
  'Strike/exercise price of the option contract. '
  'NULL for all product types other than OPTION.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.option_expiry_date IS
  'Date on which the option contract expires. DATE (YYYY-MM-DD). '
  'NULL for all product types other than OPTION.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.future_delivery_date IS
  'Contractual delivery date for the futures contract. DATE (YYYY-MM-DD). '
  'NULL for all product types other than FUTURE.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.future_valuation_method IS
  'Valuation/mark methodology for the futures contract (e.g. MARK_TO_MARKET). '
  'NULL for all product types other than FUTURE.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_rating_value IS
  'Most recent credit rating code assigned to this security. '
  'Example values: AAA, AA+, AA, AA-, A+, A, BBB+, BBB, BBB-, BB, B, CCC, D. '
  'NULL if no rating record exists for the product.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_rating_date IS
  'Effective date of the most recent credit rating. DATE (YYYY-MM-DD). '
  'NULL if no rating exists.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_rating_agency IS
  'Agency that issued the most recent credit rating. '
  'Values: SP (Standard & Poors), Moodys, Fitch. '
  'NULL if no rating exists.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_rating_type_id IS
  'Foreign key to the product_rating_type reference table. '
  'NULL if no rating exists.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.net_settlement_amount IS
  'Net cash amount due at settlement for BOND and MUNI security types. '
  'Formula: principal_amount × (1 + accrued_interest_rate). '
  'Populated only where sub_type IN (''BOND'', ''MUNI'') and both '
  'principal_amount and accrued_interest_rate are non-NULL in the Silver bond table. '
  'NULL for all other product types (REQ-01, REQ-02, REQ-03, REQ-04). '
  'Precision: DECIMAL(18,6). Currency follows bond_face_currency_code.';

-- COMMAND ----------

COMMENT ON COLUMN statestreet.g_statestreet.securities_master._dq_rule_version IS
  'SHA256 version hash of the Silver DQ rules.yaml file that was applied '
  'when the source Silver rows were evaluated. Used for lineage and re-scan detection.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master._gold_built_at IS
  'Timestamp when this Gold mart was last rebuilt. '
  'Set to current_timestamp() at the time the CREATE OR REPLACE TABLE ran.';

-- COMMAND ----------
-- MAGIC %md ## 5 · Row count summary

-- COMMAND ----------

SELECT
  type,
  sub_type,
  COUNT(*)                                                    AS row_count,
  COUNT(net_settlement_amount)                                AS rows_with_net_settlement,
  ROUND(AVG(net_settlement_amount), 4)                        AS avg_net_settlement_amount
FROM statestreet.g_statestreet.securities_master
GROUP BY type, sub_type
ORDER BY
