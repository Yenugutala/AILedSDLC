# Bronze Layer Spec — securities-master

## tables.yaml
```yaml
layer: bronze
catalog: statestreet
schema: b_statestreet
volume_path: /Volumes/statestreet/securities_master/raw_files/
iceberg_uniform: true

tables:

  # ─────────────────────────────────────────────
  # CORE BASE TABLE
  # ─────────────────────────────────────────────

  - name: product
    description: "Base table for all securities. Every security has exactly one row here regardless of subtype."
    source_file: product.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,              type: STRING,   nullable: false, description: "Unique security identifier (PK)"}
      - {name: id_type,                 type: STRING,   nullable: true,  description: "Primary identifier type: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID"}
      - {name: type,                    type: STRING,   nullable: false, description: "Product category: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT"}
      - {name: sub_type,                type: STRING,   nullable: true,  description: "Subcategory: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE, FUND, RIGHT"}
      - {name: status,                  type: STRING,   nullable: false, description: "Lifecycle: ACTIVE, INACTIVE, MATURED, SUSPENDED, DELISTED"}
      - {name: settlement_type,         type: STRING,   nullable: true,  description: "Settlement method"}
      - {name: description,             type: STRING,   nullable: true,  description: "Human-readable security name"}
      - {name: issue_date,              type: DATE,     nullable: true,  description: "Date the security was issued"}
      - {name: issue_price,             type: DECIMAL,  nullable: true,  description: "Price at issuance"}
      - {name: current_face_value,      type: DECIMAL,  nullable: true,  description: "Current face/par value (0–100)"}
      - {name: issuer_legal_entity_id,  type: STRING,   nullable: true,  description: "FK to legal_entity.legal_entity_id"}
      - {name: tick_ladder_scale_id,    type: STRING,   nullable: true,  description: "FK to tick_ladder_scale.tick_ladder_scale_id"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # LEGACY / SHADOW TABLE
  # ─────────────────────────────────────────────

  - name: generic_product
    description: "Deprecated legacy shadow of product. One product can have many generic_product rows. No uniqueness constraint on product_id."
    source_file: generic_product.csv
    primary_key: []
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    dq_notes: "Uniqueness DQ rule intentionally NOT applied. See USE-CASE-003 in known_issues.md."
    columns:
      - {name: generic_product_id,  type: STRING,   nullable: false, description: "Row identifier (not a true PK — duplicates exist by design)"}
      - {name: product_id,          type: STRING,   nullable: true,  description: "FK to product.product_id (1:many relationship)"}
      - {name: description,         type: STRING,   nullable: true,  description: "Legacy product description"}
      - {name: status,              type: STRING,   nullable: true,  description: "Legacy status field"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # LEGAL ENTITY (SCD2 CANDIDATE)
  # ─────────────────────────────────────────────

  - name: legal_entity
    description: "Issuers and counterparties. Bitemporal — SCD2 applied in Silver."
    source_file: legal_entity.csv
    primary_key: [legal_entity_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: legal_entity_id,  type: STRING,  nullable: false, description: "Unique entity identifier (PK)"}
      - {name: name,             type: STRING,  nullable: false, description: "Legal entity name"}
      - {name: country,          type: STRING,  nullable: true,  description: "ISO 3166-1 alpha-2 country code"}
      - {name: entity_type,      type: STRING,  nullable: true,  description: "BANK, CORPORATE, GOVERNMENT, etc."}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # TICK LADDER REFERENCE TABLES
  # ─────────────────────────────────────────────

  - name: tick_ladder_scale
    description: "Minimum price increment scale definitions."
    source_file: tick_ladder_scale.csv
    primary_key: [tick_ladder_scale_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: tick_ladder_scale_id,  type: STRING,  nullable: false, description: "Unique scale identifier (PK)"}
      - {name: description,           type: STRING,  nullable: true,  description: "Scale description"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: tick
    description: "Individual tick entries within a tick_ladder_scale."
    source_file: tick.csv
    primary_key: [tick_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: tick_id,               type: STRING,   nullable: false, description: "Unique tick identifier (PK)"}
      - {name: tick_ladder_scale_id,  type: STRING,   nullable: true,  description: "FK to tick_ladder_scale.tick_ladder_scale_id"}
      - {name: price_from,            type: DECIMAL,  nullable: true,  description: "Lower bound of price range"}
      - {name: price_to,              type: DECIMAL,  nullable: true,  description: "Upper bound of price range"}
      - {name: tick_size,             type: DECIMAL,  nullable: true,  description: "Minimum price increment for this range"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # PRODUCT RATING (SCD2 CANDIDATE)
  # ─────────────────────────────────────────────

  - name: product_rating
    description: "Credit rating history for securities. SCD2 applied in Silver."
    source_file: product_rating.csv
    primary_key: [rating_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: rating_id,       type: STRING,  nullable: false, description: "Unique rating record identifier (PK)"}
      - {name: product_id,      type: STRING,  nullable: false, description: "FK to product.product_id"}
      - {name: rating_type_id,  type: STRING,  nullable: true,  description: "FK to product_rating_type.rating_type_id"}
      - {name: rating_value,    type: STRING,  nullable: false, description: "Rating code: AAA, BBB-, etc."}
      - {name: rating_date,     type: DATE,    nullable: false, description: "Date rating was assigned"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: product_rating_type
    description: "Reference table for rating type definitions and agencies."
    source_file: product_rating_type.csv
    primary_key: [rating_type_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: rating_type_id,    type: STRING,  nullable: false, description: "Unique rating type identifier (PK)"}
      - {name: rating_agency,     type: STRING,  nullable: true,  description: "SP, Moodys, Fitch"}
      - {name: rating_scale,      type: STRING,  nullable: true,  description: "Short-term or long-term scale"}
      - {name: description,       type: STRING,  nullable: true,  description: "Rating type description"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # CLASSIFICATION & IDENTIFIERS (RELATIONSHIP TABLES)
  # ─────────────────────────────────────────────

  - name: classification
    description: "Product classification tags. Zero or more classifications per product."
    source_file: classification.csv
    primary_key: [classification_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: classification_id,     type: STRING,  nullable: false, description: "Unique classification record identifier (PK)"}
      - {name: product_id,            type: STRING,  nullable: false, description: "FK to product.product_id"}
      - {name: classification_type,   type: STRING,  nullable: true,  description: "Classification scheme name"}
      - {name: classification_value,  type: STRING,  nullable: true,  description: "Classification code or value"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: identifiers
    description: "Alternate identifiers for a product (CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID)."
    source_file: identifiers.csv
    primary_key: [identifier_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: identifier_id,     type: STRING,  nullable: false, description: "Unique identifier record (PK)"}
      - {name: product_id,        type: STRING,  nullable: false, description: "FK to product.product_id"}
      - {name: id_type,           type: STRING,  nullable: false, description: "CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID"}
      - {name: identifier_value,  type: STRING,  nullable: false, description: "The actual identifier string (licensed data)"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # PRODUCT SUBTYPE TABLES — FUND & RIGHT
  # ─────────────────────────────────────────────

  - name: fund
    description: "Fund-specific attributes. Extends product for FUND type securities."
    source_file: fund.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,       type: STRING,  nullable: false, description: "FK to product.product_id (PK of this table)"}
      - {name: endness_type,     type: STRING,  nullable: true,  description: "OPEN_END or CLOSED_END"}
      - {name: mutual_fund_type, type: STRING,  nullable: true,  description: "Fund subclassification"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: right
    description: "Subscription right-specific attributes. Extends product for RIGHT type securities."
    source_file: right.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,              type: STRING,  nullable: false, description: "FK to product.product_id (PK of this table)"}
      - {name: exercise_price,          type: DECIMAL, nullable: true,  description: "Price at which the right can be exercised"}
      - {name: exercise_start_date,     type: DATE,    nullable: true,  description: "Start of exercise window"}
      - {name: exercise_end_date,       type: DATE,    nullable: true,  description: "End of exercise window"}
      - {name: underlying_product_id,   type: STRING,  nullable: true,  description: "FK to product.product_id for the underlying security"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # PRODUCT SUBTYPE TABLES — DEBT HIERARCHY
  # ─────────────────────────────────────────────

  - name: debt
    description: "Debt-specific attributes. Base for Bond, Muni, and PoolBackedSecurity."
    source_file: debt.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,               type: STRING,   nullable: false, description: "FK to product.product_id (PK of this table)"}
      - {name: face_amount,              type: DECIMAL,  nullable: true,  description: "Total face/par amount of the debt instrument"}
      - {name: issue_date_settlement,    type: DATE,     nullable: true,  description: "Settlement date at issuance"}
      - {name: face_currency_code,       type: STRING,   nullable: true,  description: "ISO 4217 currency code for face value"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: bond
    description: "Bond-specific attributes. Extends debt. Includes coupon and maturity details."
    source_file: bond.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,  nullable: false, description: "FK to product.product_id (also FK to debt.product_id)"}
      - {name: coupon_type,            type: STRING,  nullable: false, description: "FIXED, FLOATING, ZERO"}
      - {name: maturity_date,          type: DATE,    nullable: false, description: "When the bond matures"}
      - {name: face_currency_code,     type: STRING,  nullable: false, description: "ISO 4217 currency code for face value"}
      - {name: day_count_convention,   type: STRING,  nullable: true,  description: "ACT/360, 30/360, ACT/ACT, etc."}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: muni
    description: "Municipal bond attributes. Extends bond."
    source_file: muni.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,   type: STRING,   nullable: false, description: "FK to bond.product_id"}
      - {name: tax_exempt,   type: BOOLEAN,  nullable: true,  description: "Whether interest is tax-exempt"}
      - {name: state,        type: STRING,   nullable: true,  description: "US state of issuance (2-letter code)"}
      - {name: purpose,      type: STRING,   nullable: true,  description: "Municipal use of proceeds"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: pool_backed_security
    description: "Pool-backed security attributes. Extends debt (NOT bond — see USE-CASE-005)."
    source_file: pool_backed_security.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,  type: STRING,  nullable: false, description: "FK to debt.product_id"}
      - {name: pool_type,   type: STRING,  nullable: true,  description: "Type of underlying asset pool"}
      - {name: originator,  type: STRING,  nullable: true,  description: "Entity that originated the pool"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # PRODUCT SUBTYPE TABLES — EQUITY HIERARCHY
  # ─────────────────────────────────────────────

  - name: series
    description: "Optional grouping series for Stock and ListedDerivative."
    source_file: series.csv
    primary_key: [series_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: series_id,    type: STRING,  nullable: false, description: "Unique series identifier (PK)"}
      - {name: description,  type: STRING,  nullable: true,  description: "Series description"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: stock
    description: "Stock-specific attributes. Base for CommonStock and PreferredStock."
    source_file: stock.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,  type: STRING,  nullable: false, description: "FK to product.product_id (PK of this table)"}
      - {name: series_id,   type: STRING,  nullable: true,  description: "FK to series.series_id"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: common_stock
    description: "Common stock attributes. Extends stock."
    source_file: common_stock.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,     type: STRING,   nullable: false, description: "FK to stock.product_id"}
      - {name: voting_rights,  type: BOOLEAN,  nullable: true,  description: "Whether stock carries voting rights"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: preferred_stock
    description: "Preferred stock attributes. Extends stock."
    source_file: preferred_stock.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,      type: STRING,  nullable: false, description: "FK to stock.product_id"}
      - {name: dividend_type,   type: STRING,  nullable: true,  description: "CUMULATIVE or NON_CUMULATIVE"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # PRODUCT SUBTYPE TABLES — DERIVATIVE HIERARCHY
  # ─────────────────────────────────────────────

  - name: listed_derivative
    description: "Listed derivative attributes. Base for Option and Future."
    source_file: listed_derivative.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,  nullable: false, description: "FK to product.product_id (PK of this table)"}
      - {name: series_id,              type: STRING,  nullable: true,  description: "FK to series.series_id"}
      - {name: underlying_product_id,  type: STRING,  nullable: true,  description: "FK to product.product_id for the underlying security"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: option
    description: "Option-specific attributes. Extends listed_derivative."
    source_file: option.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,      type: STRING,   nullable: false, description: "FK to listed_derivative.product_id"}
      - {name: option_type,     type: STRING,   nullable: false, description: "CALL or PUT"}
      - {name: exercise_style,  type: STRING,   nullable: false, description: "AMERICAN or EUROPEAN"}
      - {name: strike_price,    type: DECIMAL,  nullable: true,  description: "Strike/exercise price"}
      - {name: expiry_date,     type: DATE,     nullable: true,  description: "Expiration date"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: future
    description: "Futures contract attributes. Extends listed_derivative."
    source_file: future.csv
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,          type: STRING,  nullable: false, description: "FK to listed_derivative.product_id"}
      - {name: delivery_date,       type: DATE,    nullable: true,  description: "Futures delivery/expiry date"}
      - {name: valuation_method,    type: STRING,  nullable: true,  description: "MARK_TO_MARKET or THEORETICAL"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # COUPON SCHEDULE
  # ─────────────────────────────────────────────

  - name: coupon
    description: "Coupon payment schedule for bond securities. One row per bond per payment date."
    source_file: coupon.csv
    primary_key: [coupon_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: coupon_id,    type: STRING,   nullable: false, description: "Unique coupon record identifier (PK)"}
      - {name: product_id,  type: STRING,   nullable: false, description: "FK to bond.product_id"}
      - {name: coupon_rate, type: DECIMAL,  nullable: false, description: "Annual coupon rate as percentage (e.g. 5.0 = 5%)"}
      - {name: payment_date, type: DATE,    nullable: true,  description: "Coupon payment date"}
      - {name: coupon_type, type: STRING,   nullable: true,  description: "FIXED or FLOATING"}
      - {name: frequency,   type: STRING,   nullable: true,  description: "ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # BRIDGE / RELATIONSHIP TABLES
  # ─────────────────────────────────────────────

  - name: principal_redemption_provision
    description: "Reference table for principal redemption provision types."
    source_file: principal_redemption_provision.csv
    primary_key: [principal_redemption_provision_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: principal_redemption_provision_id,  type: STRING,  nullable: false, description: "Unique provision identifier (PK)"}
      - {name: description,                        type: STRING,  nullable: true,  description: "Provision description"}
      - {name: provision_type,                     type: STRING,  nullable: true,  description: "Type of redemption provision"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: listed_derivative_tick
    description: "Bridge table linking listed derivatives to their applicable tick entries (M:M)."
    source_file: listed_derivative_tick.csv
    primary_key: [product_id, tick_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,  type: STRING,  nullable: false, description: "FK to listed_derivative.product_id"}
      - {name: tick_id,     type: STRING,  nullable: false, description: "FK to tick.tick_id"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  - name: debt_principal_redemption_provision
    description: "Bridge table linking debt instruments to redemption provisions (M:M)."
    source_file: debt_principal_redemption_provision.csv
    primary_key: [product_id, principal_redemption_provision_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,                         type: STRING,  nullable: false, description: "FK to debt.product_id"}
      - {name: principal_redemption_provision_id,  type: STRING,  nullable: false, description: "FK to principal_redemption_provision.principal_redemption_provision_id"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}
      - {name: _row_hash,      type: STRING}

  # ─────────────────────────────────────────────
  # CURRENCY REFERENCE
  # ─────────────────────────────────────────────

  - name: currency
    description: "ISO 4217 currency reference table. Contains 2 intentionally bad rows for DQ testing (see USE-CASE-002)."
    source_file: currency.csv
    primary_key: [currency_code]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: currency_code,  type: STRING,  nullable: false, description: "ISO 4217 3-letter currency code (PK). 2 rows are intentionally invalid for DQ testing."}
      - {name: currency_name,  type: STRING,  nullable: true,  description: "Full currency name"}
      - {name: numeric_code,   type: STRING,  nullable: true,  description: "ISO 4217 numeric code"}
    metadata_columns:
      - {name: _source_file,   type: STRING}
      - {name: _ingestion_ts,  type: TIMESTAMP}
      - {name: _batch_id,      type: STRING}

```

## rules.yaml
```yaml
layer: bronze
catalog: statestreet
schema: b_statestreet

# ─────────────────────────────────────────────
# SCHEMA DRIFT POLICY
# ─────────────────────────────────────────────
schema_drift:
  additive_columns: auto_merge
  # New columns in incoming CSV → auto-merge into Bronze Delta table via mergeSchema.
  # All existing rows receive NULL for the new column.
  # Event is logged to statestreet.b_statestreet._schema_changes.
  breaking_changes: quarantine
  # Type changes or column removals → quarantine the batch.
  # Failing batch metadata written to statestreet.b_statestreet._schema_quarantine.
  # Pipeline raises an exception to trigger job-level alert and retry logic.
  quarantine_table: statestreet.b_statestreet._schema_quarantine
  changes_log_table: statestreet.b_statestreet._schema_changes
  alert_on_breaking: true

# ─────────────────────────────────────────────
# INGESTION MODE
# ─────────────────────────────────────────────
ingestion:
  mode: merge
  # MERGE INTO used for idempotency — safe to re-run without duplicating rows.
  # Only rows where _row_hash differs are updated (CDC-style).
  # First load creates the table; subsequent runs MERGE.
  merge_strategy: hash_based
  # Hash-based: WHEN MATCHED AND source._row_hash != target._row_hash THEN UPDATE SET *
  # Avoids updating rows where no data actually changed.

  # Per-table merge keys
  merge_keys:
    product:                           [product_id]
    generic_product:                   [generic_product_id]
    legal_entity:                      [legal_entity_id]
    tick_ladder_scale:                 [tick_ladder_scale_id]
    tick:                              [tick_id]
    series:                            [series_id]
    currency:                          [currency_code]
    principal_redemption_provision:    [provision_id]
    identifiers:                       [identifier_id]
    classification:                    [classification_id]
    product_rating_type:               [rating_type_id]
    product_rating:                    [rating_id]
    stock:                             [product_id]
    common_stock:                      [product_id]
    preferred_stock:                   [product_id]
    debt:                              [product_id]
    bond:                              [product_id]
    muni:                              [product_id]
    pool_backed_security:              [product_id]
    listed_derivative:                 [product_id]
    option:                            [product_id]
    future:                            [product_id]
    coupon:                            [coupon_id]
    fund:                              [product_id]
    right:                             [product_id]
    listed_derivative_tick:            [product_id, tick_id]
    debt_principal_redemption_provision: [product_id, provision_id]
    dq_rules_catalog:                  [rule_id]
    dq_issues_catalog:                 [issue_id]

# ─────────────────────────────────────────────
# AUDIT TABLES (auto-created by Bronze loader)
# ─────────────────────────────────────────────
audit_tables:
  schema_changes:
    full_name: statestreet.b_statestreet._schema_changes
    description: "Append-only log of additive column changes detected during Bronze ingestion."
  schema_quarantine:
    full_name: statestreet.b_statestreet._schema_quarantine
    description: "Holds batches that triggered breaking schema changes (type change or column removal)."

```
