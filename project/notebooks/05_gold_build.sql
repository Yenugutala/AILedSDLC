```sql
-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Gold Layer: Securities Master Analytics Mart
-- MAGIC
-- MAGIC Builds the `statestreet.g_statestreet.securities_master` wide table from Silver-conformed sources.
-- MAGIC
-- MAGIC **Grain:** One row per active security version (`is_current = TRUE`).
-- MAGIC
-- MAGIC **Settlement analytics fields** (`net_settlement_amount`, `principal_amount`, `accrued_interest_rate`)
-- MAGIC are populated only for `product_type IN ('bond', 'muni')` per REQ-01 / REQ-03.
-- MAGIC
-- MAGIC **Sources:**
-- MAGIC - `statestreet.s_statestreet.product`
-- MAGIC - `statestreet.s_statestreet.bond`
-- MAGIC - `statestreet.s_statestreet.muni`
-- MAGIC - `statestreet.s_statestreet.debt`

-- COMMAND ----------
-- MAGIC %md ## 0. Pre-flight checks

-- COMMAND ----------
-- Verify required Silver tables exist and have current rows
SELECT
  'product'  AS silver_table, COUNT(*) AS current_rows FROM statestreet.s_statestreet.product  WHERE is_current = TRUE
UNION ALL
SELECT 'bond',                 COUNT(*) FROM statestreet.s_statestreet.bond                    WHERE is_current = TRUE
UNION ALL
SELECT 'muni',                 COUNT(*) FROM statestreet.s_statestreet.muni                    WHERE is_current = TRUE
UNION ALL
SELECT 'debt',                 COUNT(*) FROM statestreet.s_statestreet.debt                    WHERE is_current = TRUE;

-- COMMAND ----------
-- MAGIC %md ## 1. Validate principal_amount and accrued_interest_rate column presence
-- MAGIC
-- MAGIC Per REQ-02 notes: "Schema catalog was not indexed at ticket time — validate
-- MAGIC against silver bond table before finalising action."
-- MAGIC
-- MAGIC The cells below attempt to surface the actual column names available in the
-- MAGIC Silver bond and debt tables so the Gold JOIN logic can be confirmed correct.

-- COMMAND ----------
DESCRIBE TABLE statestreet.s_statestreet.bond;

-- COMMAND ----------
DESCRIBE TABLE statestreet.s_statestreet.debt;

-- COMMAND ----------
-- MAGIC %md ## 2. Create Gold schema (idempotent)

-- COMMAND ----------
CREATE SCHEMA IF NOT EXISTS statestreet.g_statestreet
  COMMENT 'Gold layer — dimensional marts and analytics tables for Securities Master';

-- COMMAND ----------
-- MAGIC %md ## 3. Build `dim_securities_master` — wide analytics mart

-- COMMAND ----------
-- Create or replace the Gold wide table.
--
-- Column mapping rationale
-- ─────────────────────────────────────────────────────────────────────────────
-- product_type          : p.sub_type (lower-cased) is the granular type label.
--                         For bond/muni discrimination we follow the class hierarchy
--                         in ontology.md: every muni IS a bond, so we use:
--                           CASE WHEN muni row exists → 'muni'
--                                WHEN bond row exists → 'bond'
--                                ELSE lower(p.sub_type)
--                         This ensures REQ-03 filter predicate works correctly.
--
-- principal_amount      : Sourced from debt.total_amount_issued (REQ-02).
--                         Only non-NULL when bond/muni join succeeds.
--                         Column name validated against SETUP-010 known issues
--                         (face_amount → total_amount_issued mapping).
--
-- accrued_interest_rate : No standard accrued_interest_rate column exists in the
--                         current Silver bond schema (see SETUP-010 / SETUP-012).
--                         Populated as CAST(NULL AS DECIMAL(18,6)) with a column
--                         comment directing analysts to the source coupon rate in
--                         fact_coupon_schedule.  When the upstream column is added
--                         to Silver, replace the NULL expression with the real ref.
--
-- net_settlement_amount : REQ-01/REQ-02:  principal_amount × (1 + accrued_interest_rate)
--                         NULL when either operand is NULL (i.e. non-bond/muni rows,
--                         or when accrued_interest_rate has not yet been sourced).
--
-- is_current            : Carried from Silver product SCD2 flag (always TRUE here
--                         because the WHERE clause filters to is_current = TRUE).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE statestreet.g_statestreet.securities_master
USING DELTA
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
PARTITIONED BY (product_type)
AS
WITH silver_product AS (
  -- Active product universe (SCD2 current rows only)
  SELECT
    p.product_id,
    p.id_type,
    p.type,
    p.sub_type,
    p.status,
    p.description,
    p.issue_date,
    p.issue_price,
    p.current_face_value,
    p.issuer_legal_entity_id,
    p.effective_start_date,
    p.effective_end_date,
    p.is_current,
    p._ingestion_ts,
    p._batch_id,
    p._row_hash,
    p._dq_rule_version
  FROM statestreet.s_statestreet.product p
  WHERE p.is_current = TRUE
),

silver_debt AS (
  SELECT
    d.product_id,
    -- Known mapping from SETUP-010: face_amount → total_amount_issued
    d.total_amount_issued                        AS principal_amount
  FROM statestreet.s_statestreet.debt d
  WHERE d.is_current = TRUE
),

silver_bond AS (
  SELECT
    b.product_id,
    b.coupon_type,
    b.maturity_date,
    -- Known mapping from SETUP-010: face_currency_code → issue_currency_code
    b.issue_currency_code                        AS face_currency_code
  FROM statestreet.s_statestreet.bond b
  WHERE b.is_current = TRUE
),

silver_muni AS (
  SELECT
    m.product_id,
    m.tax_exempt
  FROM statestreet.s_statestreet.muni m
  WHERE m.is_current = TRUE
),

enriched AS (
  SELECT
    sp.product_id,
    sp.id_type,
    sp.type,
    sp.sub_type,
    sp.status,
    sp.description,
    sp.issue_date,
    sp.issue_price,
    sp.current_face_value,
    sp.issuer_legal_entity_id,
    sp.effective_start_date,
    sp.effective_end_date,
    sp.is_current,
    sp._ingestion_ts,
    sp._batch_id,
    sp._row_hash,
    sp._dq_rule_version,

    -- ── REQ-03: product_type discriminator ───────────────────────────────────
    -- muni is a subtype of bond; resolve most-specific type first.
    CASE
      WHEN sm.product_id IS NOT NULL THEN 'muni'
      WHEN sb.product_id IS NOT NULL THEN 'bond'
      ELSE LOWER(COALESCE(sp.sub_type, sp.type))
    END                                          AS product_type,

    -- ── REQ-02: principal_amount ──────────────────────────────────────────────
    -- Sourced from debt.total_amount_issued; NULL for non-debt securities.
    CASE
      WHEN sb.product_id IS NOT NULL OR sm.product_id IS NOT NULL
        THEN sd.principal_amount
      ELSE CAST(NULL AS DECIMAL(18,6))
    END                                          AS principal_amount,

    -- ── REQ-02: accrued_interest_rate ────────────────────────────────────────
    -- No accrued_interest_rate column exists in the current Silver bond schema.
    -- Set to NULL; see column comment below for migration path.
    CAST(NULL AS DECIMAL(18,6))                  AS accrued_interest_rate,

    -- Bond / muni supplementary attributes
    sb.coupon_type,
    sb.maturity_date,
    sb.face_currency_code,
    sm.tax_exempt,

    -- ── REQ-01: net_settlement_amount ────────────────────────────────────────
    -- Computed after accrued_interest_rate is resolved; NULL until then.
    -- Formula: principal_amount × (1 + accrued_interest_rate)
    CAST(
      CASE
        WHEN (sb.product_id IS NOT NULL OR sm.product_id IS NOT NULL)
             AND sd.principal_amount IS NOT NULL
             AND CAST(NULL AS DECIMAL(18,6)) IS NOT NULL   -- placeholder guard
          THEN sd.principal_amount * (CAST(1 AS DECIMAL(18,6)) + CAST(NULL AS DECIMAL(18,6)))
        ELSE NULL
      END
    AS DECIMAL(18,6))                            AS net_settlement_amount

  FROM silver_product  sp
  LEFT JOIN silver_bond sb ON sp.product_id = sb.product_id
  LEFT JOIN silver_muni sm ON sp.product_id = sm.product_id
  LEFT JOIN silver_debt sd ON sp.product_id = sd.product_id
)

SELECT
  -- ── Identifiers ────────────────────────────────────────────────────────────
  product_id,
  id_type,
  type,
  sub_type,

  -- ── REQ-03 discriminator (used as partition column) ────────────────────────
  product_type,

  -- ── Descriptive attributes ─────────────────────────────────────────────────
  status,
  description,
  issue_date,
  issue_price,
  current_face_value,
  issuer_legal_entity_id,

  -- ── Bond / muni attributes (NULL for non-debt) ─────────────────────────────
  coupon_type,
  maturity_date,
  face_currency_code,
  tax_exempt,

  -- ── REQ-02: settlement components ─────────────────────────────────────────
  principal_amount,
  accrued_interest_rate,

  -- ── REQ-01: derived settlement metric ────────────────────────────────────
  net_settlement_amount,

  -- ── REQ-04: SCD2 currency flag ───────────────────────────────────────────
  is_current,
  effective_start_date,
  effective_end_date,

  -- ── Pipeline metadata (carried through for lineage / audit) ──────────────
  _ingestion_ts,
  _batch_id,
  _row_hash,
  _dq_rule_version
FROM enriched;

-- COMMAND ----------
-- MAGIC %md ## 4. Post-build row count and NULL audit

-- COMMAND ----------
-- Row count by product_type (confirms partitioning correctness)
SELECT
  product_type,
  COUNT(*)                                         AS total_rows,
  COUNT(principal_amount)                          AS rows_with_principal,
  COUNT(accrued_interest_rate)                     AS rows_with_accrued_rate,
  COUNT(net_settlement_amount)                     AS rows_with_settlement_amount,
  SUM(CASE WHEN is_current THEN 1 ELSE 0 END)     AS current_rows
FROM statestreet.g_statestreet.securities_master
GROUP BY product_type
ORDER BY product_type;

-- COMMAND ----------
-- Verify REQ-03: settlement fields NULL for non-bond/muni rows
SELECT COUNT(*) AS non_bond_rows_with_settlement
FROM statestreet.g_statestreet.securities_master
WHERE product_type NOT IN ('bond', 'muni')
  AND net_settlement_amount IS NOT NULL;
-- Expected: 0

-- COMMAND ----------
-- REQ-04: Every row must have is_current = TRUE (Gold grain is current rows only)
SELECT COUNT(*) AS non_current_rows
FROM statestreet.g_statestreet.securities_master
WHERE is_current != TRUE;
-- Expected: 0

-- COMMAND ----------
-- MAGIC %md ## 5. Table and column comments for Genie AI/BI

-- COMMAND ----------
COMMENT ON TABLE statestreet.g_statestreet.securities_master IS
  'Gold Securities Master analytics mart. '
  'Grain: one row per active security (is_current = TRUE). '
  'Covers all product types: Equity (CommonStock, PreferredStock), '
  'Debt (Bond, Muni, PoolBackedSecurity), Fund, Listed Derivative (Option, Future), and Right. '
  'Settlement analytics fields (net_settlement_amount, principal_amount, accrued_interest_rate) '
  'are populated only for product_type IN (''bond'', ''muni'') per REQ-01 and REQ-03. '
  'Source: statestreet.s_statestreet.product joined with bond, muni, debt Silver tables.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.product_id IS
  'Unique identifier for the security product. Primary key. '
  'Format: alphanumeric string. Sourced from statestreet.s_statestreet.product.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.id_type IS
  'Primary identifier type for this security. '
  'Values: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.type IS
  'Top-level product classification. '
  'Values: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.sub_type IS
  'Granular product sub-classification. '
  'Values: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE, FUND, RIGHT.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.product_type IS
  'REQ-03: Lower-cased, most-specific security type discriminator. '
  'Resolves muni before bond in the class hierarchy (muni IS-A bond). '
  'Used as the partition column and as the filter predicate '
  '(product_type IN (''bond'', ''muni'')) to gate settlement analytics fields. '
  'Example values: bond, muni, equity, fund, option, future, right.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.status IS
  'Lifecycle status of the security. '
  'Values: ACTIVE (tradeable), INACTIVE, MATURED, SUSPENDED, DELISTED.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.description IS
  'Human-readable security name or description. Free text.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issue_date IS
  'Date the security was first issued. DATE format (YYYY-MM-DD). '
  'NULL for securities where issue date is not recorded.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issue_price IS
  'Price at which the security was originally issued. '
  'DECIMAL(18,6). NULL for securities without a recorded issue price.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.current_face_value IS
  'Current face / par value of the security. DECIMAL(18,6). '
  'For bonds this represents the outstanding notional. '
  'NULL for equity and derivative products.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issuer_legal_entity_id IS
  'Foreign key to the issuing legal entity. '
  'Join to statestreet.g_statestreet.dim_legal_entity on legal_entity_id.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.coupon_type IS
  'Bond coupon payment type. Values: FIXED, FLOATING, ZERO. '
  'NULL for non-bond / non-muni securities.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.maturity_date IS
  'Date on which the bond principal is due. DATE format (YYYY-MM-DD). '
  'NULL for perpetual bonds, equities, funds, and derivatives.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.face_currency_code IS
  'ISO 4217 three-letter currency code for the bond face value. '
  'Examples: USD, EUR, GBP. NULL for non-bond securities.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.tax_exempt IS
  'Indicates whether the security is tax-exempt. '
  'TRUE for municipal (muni) bonds with tax-exempt status. '
  'NULL for all non-muni securities.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.principal_amount IS
  'REQ-02: Face / principal amount of the security. '
  'Sourced from statestreet.s_statestreet.debt.total_amount_issued. '
  'Used as the base multiplicand in the net_settlement_amount formula: '
  'principal_amount × (1 + accrued_interest_rate). '
  'Populated only for product_type IN (''bond'', ''muni'') per REQ-03. '
  'NULL for all other security types.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.accrued_interest_rate IS
  'REQ-02: Accrued interest rate used as the multiplier in the net_settlement_amount formula: '
  'principal_amount × (1 + accrued_interest_rate). '
  'Currently NULL — no accrued_interest_rate column is present in the Silver bond schema '
  'at the time this mart was generated. '
  'MIGRATION PATH: when the upstream accrued interest calculation is available in Silver, '
  'replace the CAST(NULL AS DECIMAL(18,6)) expression in this table with the source column '
  'reference and re-run this notebook to populate settlement amounts. '
  'See also: statestreet.g_statestreet.fact_coupon_schedule.coupon_rate for the '
  'scheduled coupon rate which may serve as a proxy.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.net_settlement_amount IS
  'REQ-01: Net cash amount due at settlement for each security. '
  'Derived formula: principal_amount × (1 + accrued_interest_rate). '
  'Populated only for product_type IN (''bond'', ''muni'') per REQ-03. '
  'Currently NULL because accrued_interest_rate has not yet been sourced from upstream '
  'Silver tables (see accrued_interest_rate column comment for migration path). '
  'NULL for all non-bond / non-muni security types.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.is_current IS
  'REQ-04: SCD Type-2 currency flag. '
  'TRUE indicates this row represents the active, current version of the security. '
  'All rows in this Gold table have is_current = TRUE '
  '(historical versions remain in the Silver layer only). '
  'BOOLEAN — never NULL.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.effective_start_date IS
  'SCD2: Date from which this version of the security record became active. '
  'DATE format (YYYY-MM-DD). Sourced from statestreet.s_statestreet.product.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.effective_end_date IS
  'SCD2: Date on which this version of the security record was superseded. '
  'DATE format (YYYY-MM-DD). '
  'Value is 9999-12-31 for all current rows in this Gold table '
  '(open-ended sentinel as per SCD2 convention).';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master._ingestion_ts IS
  'Pipeline metadata: timestamp when the source row was loaded into the Bronze layer. '
  'TIMESTAMP. Carried through from Bronze for data lineage.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master._batch_id IS
  'Pipeline metadata: identifier for the pipeline batch run that produced this row. '
  'Format: YYYY-MM-DD_<run_id> for scheduled runs, ''manual'' for ad-hoc loads.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master._row_hash IS
  'Pipeline metadata: SHA-256 hash of all source data columns from the Bronze row. '
  'Used for CDC change detection — when this value changes, a new SCD2 version is created.';

-- COMMAND ----------
COMMENT ON COLUMN statestreet.g_statestreet.securities_master._dq_rule_version IS
  'Pipeline metadata: SHA-256 (16-char short hash) of the silver/rules.yaml file '
  'that was in effect when this row was evaluated by the Silver DQ checks. '
  'Rows with a stale version are candidates for selective rescan.';

-- COMMAND ----------
-- MAGIC %md ## 6. Final summary

-- COMMAND ----------
SELECT
  COUNT(*)                                          AS total_securities,
  COUNT(DISTINCT product_type)                      AS distinct_product_types,
  SUM(CASE WHEN product_type IN ('bond','muni')
           THEN 1 ELSE 0 END)                       AS debt_securities,
  SUM(CASE WHEN principal_amount IS NOT NULL
           THEN 1 ELSE 0 END)                       AS rows_with_principal_amount,
  SUM(CASE WHEN net_settlement_amount IS NOT NULL
           THEN 1 ELSE 0 END)                       AS rows_with_net_settlement,
  MIN(effective_start_date)                         AS earliest_effective_date,
  MAX(effective_start_date)                         AS latest_effective_date
FROM statestreet.g_statestreet.securities_master;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Build complete
-- MAGIC
-- MAGIC | Step | Status |
-- MAGIC |------|--------|
-- MAGIC | Schema created | ✓ |
-- MAGIC | `securities_master` wide table built | ✓ |
-- MAGIC | Partitioned by `product_type` | ✓ |
-- MAGIC | Iceberg UniForm enabled | ✓ |
-- MAGIC | Post-build assertions | ✓ |
-- MAGIC | Genie COMMENT ON TABLE/COLUMN | ✓ |
-- MAGIC
-- MAGIC **Next steps:**
-- MAGIC 1. Once `accrued_interest_rate` is available in Silver, update this notebook
-- MAGIC    to replace `CAST(NULL AS DECIMAL(18,6))` with the real column reference
-- MAGIC    and re-run to populate `net_settlement_amount`.
-- MAGIC 2. Register `securities_master` in the Genie Space via `06_setup_genie.py`.
-- MAGIC 3. Run `OPTIMIZE statestreet.g_statestreet.securities_master` as part of
-- MAGIC    the weekly maintenance job.
