# Silver Layer Spec — securities-master

## tables.yaml
```yaml
layer: silver
catalog: statestreet
schema: s_statestreet

# SCD2 applies to: product, legal_entity, product_rating
# All other tables: latest-version upsert (no SCD2)
# Rejects table auto-created for every silver table as <table>_rejects

tables:

  # ─────────────────────────────────────────────
  # BASE / CORE TABLES
  # ─────────────────────────────────────────────

  - name: product
    source: statestreet.b_statestreet.product
    primary_key: [product_id]
    scd2: true
    scd2_columns:
      effective_start_date: DATE
      effective_end_date: DATE
      is_current: BOOLEAN
    partition_by: [type]
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "Unique security identifier — primary key"}
      - {name: id_type,                type: STRING,         nullable: false, description: "Primary identifier type: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID"}
      - {name: type,                   type: STRING,         nullable: false, description: "Product category: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT"}
      - {name: sub_type,               type: STRING,         nullable: true,  description: "Subcategory: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE, FUND, RIGHT"}
      - {name: status,                 type: STRING,         nullable: false, description: "Lifecycle status: ACTIVE, INACTIVE, MATURED, SUSPENDED, DELISTED"}
      - {name: settlement_type,        type: STRING,         nullable: true,  description: "Settlement method"}
      - {name: description,            type: STRING,         nullable: true,  description: "Human-readable security name"}
      - {name: issue_date,             type: DATE,           nullable: true,  description: "Date the security was issued"}
      - {name: issue_price,            type: DECIMAL(28,8),  nullable: true,  description: "Price at issuance"}
      - {name: current_face_value,     type: DECIMAL(28,8),  nullable: true,  description: "Current face/par value"}
      - {name: issuer_legal_entity_id, type: STRING,         nullable: true,  description: "FK to legal_entity.legal_entity_id"}
      - {name: tick_ladder_scale_id,   type: STRING,         nullable: true,  description: "FK to tick_ladder_scale.tick_ladder_scale_id"}
    scd2_tracking_columns:
      - {name: effective_start_date,   type: DATE,           nullable: false, description: "When this version became active"}
      - {name: effective_end_date,     type: DATE,           nullable: false, description: "When this version was superseded (9999-12-31 = current)"}
      - {name: is_current,             type: BOOLEAN,        nullable: false, description: "TRUE for the active version"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: generic_product
    source: statestreet.b_statestreet.generic_product
    primary_key: []          # deliberately no PK uniqueness — see USE-CASE-003
    scd2: false
    partition_by: []
    iceberg_uniform: true
    notes: "Deprecated legacy shadow table. One product_id maps to many rows by design. No PK uniqueness rule."
    columns:
      - {name: product_id,             type: STRING,         nullable: true,  description: "FK to product.product_id — not unique in this table"}
      - {name: generic_product_id,     type: STRING,         nullable: true,  description: "Legacy generic product identifier"}
      - {name: description,            type: STRING,         nullable: true,  description: "Legacy product description"}
      - {name: type,                   type: STRING,         nullable: true,  description: "Legacy product type code"}
      - {name: status,                 type: STRING,         nullable: true,  description: "Legacy status value"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: legal_entity
    source: statestreet.b_statestreet.legal_entity
    primary_key: [legal_entity_id]
    scd2: true
    scd2_columns:
      effective_start_date: DATE
      effective_end_date: DATE
      is_current: BOOLEAN
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: legal_entity_id,        type: STRING,         nullable: false, description: "Unique entity identifier — primary key"}
      - {name: legal_name,             type: STRING,         nullable: false, description: "Legal entity name"}
      - {name: country,                type: STRING,         nullable: true,  description: "ISO 3166-1 alpha-2 country code"}
      - {name: legal_entity_type,      type: STRING,         nullable: true,  description: "Entity classification: BANK, CORPORATE, GOVERNMENT, etc."}
      - {name: registration_number,    type: STRING,         nullable: true,  description: "Legal registration or incorporation number"}
      - {name: lei_code,               type: STRING,         nullable: true,  description: "Legal Entity Identifier (20-character ISO 17442 code)"}
    scd2_tracking_columns:
      - {name: effective_start_date,   type: DATE,           nullable: false}
      - {name: effective_end_date,     type: DATE,           nullable: false}
      - {name: is_current,             type: BOOLEAN,        nullable: false}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  # ─────────────────────────────────────────────
  # REFERENCE / DIMENSION TABLES
  # ─────────────────────────────────────────────

  - name: currency
    source: statestreet.b_statestreet.currency
    primary_key: [currency_code]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    notes: "Two rows deliberately invalid (USE-CASE-002). Expect 2 rows in currency_rejects."
    columns:
      - {name: currency_code,          type: STRING,         nullable: false, description: "ISO 4217 3-letter currency code — primary key"}
      - {name: currency_name,          type: STRING,         nullable: true,  description: "Full currency name"}
      - {name: numeric_code,           type: STRING,         nullable: true,  description: "ISO 4217 numeric code"}
      - {name: minor_units,            type: INTEGER,        nullable: true,  description: "Number of decimal places (e.g. 2 for USD)"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: series
    source: statestreet.b_statestreet.series
    primary_key: [series_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: series_id,              type: STRING,         nullable: false, description: "Unique series identifier — primary key"}
      - {name: series_name,            type: STRING,         nullable: true,  description: "Series name or description"}
      - {name: series_type,            type: STRING,         nullable: true,  description: "Series classification"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: tick_ladder_scale
    source: statestreet.b_statestreet.tick_ladder_scale
    primary_key: [tick_ladder_scale_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: tick_ladder_scale_id,   type: STRING,         nullable: false, description: "Unique tick ladder scale identifier — primary key"}
      - {name: scale_name,             type: STRING,         nullable: true,  description: "Human-readable scale name"}
      - {name: description,            type: STRING,         nullable: true,  description: "Scale description"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: tick
    source: statestreet.b_statestreet.tick
    primary_key: [tick_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: tick_id,                type: STRING,         nullable: false, description: "Unique tick identifier — primary key"}
      - {name: tick_ladder_scale_id,   type: STRING,         nullable: false, description: "FK to tick_ladder_scale.tick_ladder_scale_id"}
      - {name: price_from,             type: DECIMAL(28,8),  nullable: true,  description: "Lower bound of the price range for this tick"}
      - {name: price_to,               type: DECIMAL(28,8),  nullable: true,  description: "Upper bound of the price range for this tick"}
      - {name: tick_size,              type: DECIMAL(28,8),  nullable: true,  description: "Minimum price increment in this band"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: principal_redemption_provision
    source: statestreet.b_statestreet.principal_redemption_provision
    primary_key: [principal_redemption_provision_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: principal_redemption_provision_id, type: STRING, nullable: false, description: "Unique provision identifier — primary key"}
      - {name: provision_type,         type: STRING,         nullable: true,  description: "Type of redemption provision (e.g. CALLABLE, PUTTABLE, SINKING_FUND)"}
      - {name: description,            type: STRING,         nullable: true,  description: "Human-readable description of the provision"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: product_rating_type
    source: statestreet.b_statestreet.product_rating_type
    primary_key: [product_rating_type_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_rating_type_id, type: STRING,         nullable: false, description: "Unique rating type identifier — primary key"}
      - {name: rating_agency,          type: STRING,         nullable: true,  description: "Rating agency name (e.g. Moodys, SP, Fitch)"}
      - {name: rating_type_code,       type: STRING,         nullable: true,  description: "Short code for the rating type"}
      - {name: rating_type_name,       type: STRING,         nullable: true,  description: "Full name of the rating type"}
      - {name: rating_scale,           type: STRING,         nullable: true,  description: "Scale description (e.g. LONG_TERM, SHORT_TERM)"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  # ─────────────────────────────────────────────
  # PRODUCT SUBTYPE TABLES (extend product)
  # ─────────────────────────────────────────────

  - name: stock
    source: statestreet.b_statestreet.stock
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id — PK in this table"}
      - {name: series_id,              type: STRING,         nullable: true,  description: "FK to series.series_id"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: common_stock
    source: statestreet.b_statestreet.common_stock
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id — PK in this table"}
      - {name: voting_rights,          type: BOOLEAN,        nullable: true,  description: "TRUE if shares carry voting rights"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: preferred_stock
    source: statestreet.b_statestreet.preferred_stock
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id — PK in this table"}
      - {name: dividend_right,         type: STRING,         nullable: true,  description: "Dividend entitlement type: CUMULATIVE, NON_CUMULATIVE"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: fund
    source: statestreet.b_statestreet.fund
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id — PK in this table"}
      - {name: endness_type,           type: STRING,         nullable: true,  description: "OPEN_END or CLOSED_END"}
      - {name: mutual_fund_type,       type: STRING,         nullable: true,  description: "Fund sub-classification (e.g. ETF, MUTUAL_FUND, HEDGE_FUND)"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: right
    source: statestreet.b_statestreet.right
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id — PK in this table"}
      - {name: underlying_product_id,  type: STRING,         nullable: true,  description: "FK to product.product_id for the underlying security"}
      - {name: expiry_date,            type: DATE,           nullable: true,  description: "Date on which the right expires"}
      - {name: subscription_ratio,     type: DECIMAL(28,8),  nullable: true,  description: "Number of rights required to acquire one underlying share"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: debt
    source: statestreet.b_statestreet.debt
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id — PK in this table"}
      - {name: total_amount_issued,    type: DECIMAL(28,8),  nullable: true,  description: "Total notional/face amount issued"}
      - {name: issue_currency_code,    type: STRING,         nullable: true,  description: "ISO 4217 currency of issuance — FK to currency"}
      - {name: settlement_days,        type: INTEGER,        nullable: true,  description: "Standard settlement days (T+N)"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: bond
    source: statestreet.b_statestreet.bond
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id and debt.product_id — PK in this table"}
      - {name: coupon_type,            type: STRING,         nullable: false, description: "Coupon structure: FIXED, FLOATING, ZERO"}
      - {name: maturity_date,          type: DATE,           nullable: false, description: "Date on which the bond matures"}
      - {name: issue_currency_code,    type: STRING,         nullable: false, description: "ISO 4217 currency code for face value — FK to currency"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: muni
    source: statestreet.b_statestreet.muni
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to bond.product_id — PK in this table"}
      - {name: tax_exempt,             type: BOOLEAN,        nullable: true,  description: "TRUE if interest is federally tax-exempt"}
      - {name: state,                  type: STRING,         nullable: true,  description: "US state of issuance (2-letter code)"}
      - {name: purpose,                type: STRING,         nullable: true,  description: "Municipal bond purpose (e.g. GENERAL_OBLIGATION, REVENUE)"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: pool_backed_security
    source: statestreet.b_statestreet.pool_backed_security
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    notes: "Extends debt, NOT bond. Bond-specific columns will be NULL in dim_product for pool-backed securities."
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to debt.product_id — PK in this table"}
      - {name: pool_type,              type: STRING,         nullable: true,  description: "Type of pool backing (e.g. MORTGAGE, AUTO, STUDENT_LOAN)"}
      - {name: originator,             type: STRING,         nullable: true,  description: "Entity that originated the pool"}
      - {name: pass_through_rate,      type: DECIMAL(28,8),  nullable: true,  description: "Coupon pass-through rate as a percentage"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: listed_derivative
    source: statestreet.b_statestreet.listed_derivative
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id — PK in this table"}
      - {name: series_id,              type: STRING,         nullable: true,  description: "FK to series.series_id"}
      - {name: underlying_product_id,  type: STRING,         nullable: true,  description: "FK to product.product_id for the underlying security"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: option
    source: statestreet.b_statestreet.option
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to listed_derivative.product_id — PK in this table"}
      - {name: option_type,            type: STRING,         nullable: false, description: "CALL or PUT"}
      - {name: exercise_style,         type: STRING,         nullable: false, description: "AMERICAN or EUROPEAN"}
      - {name: strike_price,           type: DECIMAL(28,8),  nullable: true,  description: "Strike/exercise price"}
      - {name: expiry_date,            type: DATE,           nullable: true,  description: "Date on which the option expires"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: future
    source: statestreet.b_statestreet.future
    primary_key: [product_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to listed_derivative.product_id — PK in this table"}
      - {name: delivery_date,          type: DATE,           nullable: true,  description: "Futures contract delivery/settlement date"}
      - {name: valuation_method,       type: STRING,         nullable: true,  description: "Valuation approach (e.g. MARK_TO_MARKET)"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  # ─────────────────────────────────────────────
  # RELATIONSHIP / ATTRIBUTE TABLES
  # ─────────────────────────────────────────────

  - name: identifiers
    source: statestreet.b_statestreet.identifiers
    primary_key: [identifier_id]
    scd2: false
    partition_by: [id_type]
    iceberg_uniform: true
    columns:
      - {name: identifier_id,          type: STRING,         nullable: false, description: "Unique identifier record PK"}
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id"}
      - {name: id_type,                type: STRING,         nullable: false, description: "Identifier type: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID"}
      - {name: identifier_value,       type: STRING,         nullable: false, description: "The actual identifier string"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: classification
    source: statestreet.b_statestreet.classification
    primary_key: [classification_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: classification_id,      type: STRING,         nullable: false, description: "Unique classification record PK"}
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id"}
      - {name: classification_type,    type: STRING,         nullable: true,  description: "Classification scheme (e.g. GICS, SIC, NAICS)"}
      - {name: classification_code,    type: STRING,         nullable: true,  description: "Classification code within the scheme"}
      - {name: classification_name,    type: STRING,         nullable: true,  description: "Human-readable classification name"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: product_rating
    source: statestreet.b_statestreet.product_rating
    primary_key: [product_rating_id]
    scd2: true
    scd2_columns:
      effective_start_date: DATE
      effective_end_date: DATE
      is_current: BOOLEAN
    partition_by: []
    iceberg_uniform: true
    notes: "SCD2 tracks rating changes over time. Grain with SCD2: one row per product+rating_type+effective_from_date version."
    columns:
      - {name: product_rating_id,      type: STRING,         nullable: false, description: "Unique rating record PK"}
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to product.product_id"}
      - {name: product_rating_type_id, type: STRING,         nullable: true,  description: "FK to product_rating_type.product_rating_type_id"}
      - {name: rating_value,           type: STRING,         nullable: false, description: "Rating code (e.g. AAA, BBB-, BB+)"}
      - {name: effective_from_date,    type: DATE,           nullable: false, description: "Date the rating was assigned"}
      - {name: rating_agency,          type: STRING,         nullable: true,  description: "Rating agency that issued the rating"}
      - {name: watch_code,             type: STRING,         nullable: true,  description: "Rating watch status (e.g. POSITIVE, NEGATIVE, STABLE)"}
    scd2_tracking_columns:
      - {name: effective_start_date,   type: DATE,           nullable: false}
      - {name: effective_end_date,     type: DATE,           nullable: false}
      - {name: is_current,             type: BOOLEAN,        nullable: false}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: coupon
    source: statestreet.b_statestreet.coupon
    primary_key: [coupon_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: coupon_id,              type: STRING,         nullable: false, description: "Unique coupon schedule record PK"}
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to bond.product_id"}
      - {name: coupon_rate,            type: DECIMAL(28,8),  nullable: false, description: "Annual coupon rate as a percentage (0–100)"}
      - {name: payment_date,           type: DATE,           nullable: false, description: "Scheduled coupon payment date"}
      - {name: coupon_type,            type: STRING,         nullable: true,  description: "FIXED or FLOATING"}
      - {name: frequency,              type: STRING,         nullable: true,  description: "Payment frequency: ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  # ─────────────────────────────────────────────
  # BRIDGE TABLES
  # ─────────────────────────────────────────────

  - name: listed_derivative_tick
    source: statestreet.b_statestreet.listed_derivative_tick
    primary_key: [product_id, tick_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,             type: STRING,         nullable: false, description: "FK to listed_derivative.product_id — part of composite PK"}
      - {name: tick_id,                type: STRING,         nullable: false, description: "FK to tick.tick_id — part of composite PK"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  - name: debt_principal_redemption_provision
    source: statestreet.b_statestreet.debt_principal_redemption_provision
    primary_key: [product_id, principal_redemption_provision_id]
    scd2: false
    partition_by: []
    iceberg_uniform: true
    columns:
      - {name: product_id,                        type: STRING,  nullable: false, description: "FK to debt.product_id — part of composite PK"}
      - {name: principal_redemption_provision_id, type: STRING,  nullable: false, description: "FK to principal_redemption_provision — part of composite PK"}
      - {name: effective_date,                    type: DATE,    nullable: true,  description: "Date from which the provision applies"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}
      - {name: _dq_rule_version,       type: STRING}

  # ─────────────────────────────────────────────
  # METADATA-ONLY TABLES (Bronze-landed, no DQ)
  # ─────────────────────────────────────────────

  - name: dq_rules_catalog
    source: statestreet.b_statestreet.dq_rules_catalog
    primary_key: [rule_id]
    scd2: false
    partition_by: []
    iceberg_uniform: false
    notes: "Reference metadata. Bronze-only ingestion per USE-CASE-001. Passed through to Silver as-is for auditability — no DQ rules applied."
    dq_rules_applied: false
    columns:
      - {name: rule_id,                type: STRING,  nullable: false, description: "DQ rule identifier"}
      - {name: table_name,             type: STRING,  nullable: true,  description: "Target table for the rule"}
      - {name: column_name,            type: STRING,  nullable: true,  description: "Target column (NULL = row-level rule)"}
      - {name: dq_dimension,           type: STRING,  nullable: true,  description: "DQ dimension: Validity, Completeness, Uniqueness, Consistency, Accuracy, Timeliness"}
      - {name: rule_type,              type: STRING,  nullable: true,  description: "Rule pattern type"}
      - {name: severity,               type: STRING,  nullable: true,  description: "HIGH, MEDIUM, or LOW"}
      - {name: description,            type: STRING,  nullable: true,  description: "Human-readable rule description"}
      - {name: sql,                    type: STRING,  nullable: true,  description: "SQL SELECT returning failing rows"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}

  - name: dq_issues_catalog
    source: statestreet.b_statestreet.dq_issues_catalog
    primary_key: [issue_id]
    scd2: false
    partition_by: []
    iceberg_uniform: false
    notes: "Reference metadata. Bronze-only ingestion per USE-CASE-001. Passed through to Silver as-is — no DQ rules applied."
    dq_rules_applied: false
    columns:
      - {name: issue_id,               type: STRING,  nullable: false, description: "DQ issue identifier"}
      - {name: rule_id,                type: STRING,  nullable: true,  description: "FK to dq_rules_catalog.rule_id"}
      - {name: table_name,             type: STRING,  nullable: true,  description: "Table where the issue was detected"}
      - {name: column_name,            type: STRING,  nullable: true,  description: "Column where the issue was detected"}
      - {name: issue_description,      type: STRING,  nullable: true,  description: "Description of the specific issue"}
      - {name: severity,               type: STRING,  nullable: true,  description: "Issue severity level"}
      - {name: status,                 type: STRING,  nullable: true,  description: "Resolution status (OPEN, RESOLVED, ACCEPTED)"}
    metadata_columns:
      - {name: _ingestion_ts,          type: TIMESTAMP}
      - {name: _source_file,           type: STRING}
      - {name: _batch_id,              type: STRING}
      - {name: _row_hash,              type: STRING}

# ─────────────────────────────────────────────
# REJECTS TABLE CONVENTION
# Auto-created alongside every DQ-checked silver table
# ─────────────────────────────────────────────
rejects_convention:
  naming_pattern: "<table_name>_rejects"
  schema: statestreet.s_statestreet
  extra_columns:
    - {name: _rule_id,          type: STRING,    description: "DQ rule that caused rejection"}
    - {name: _violation_detail, type: STRING,    description: "Human-readable description of what failed"}
    - {name: _rejected_ts,      type: TIMESTAMP, description: "Timestamp when row was rejected"}
    - {name: _dq_rule_version,  type: STRING,    description: "SHA256 of silver/rules.yaml at time of DQ check"}
  tables_with_rejects:
    - product
    - legal_entity
    - currency
    - series
    - tick_ladder_scale
    - tick
    - stock
    - common_stock
    - preferred_stock
    - fund
    - right
    - debt
    - bond
    - muni
    - pool_backed_security
    - listed_derivative
    - option
    - future
    - identifiers
    - classification
    - product_rating
    - coupon
    - listed_derivative_tick
    - debt_principal_redemption_provision
    - principal_redemption_provision
    - product_rating_type
  tables_without_rejects:
    - generic_product    # No DQ rules — legacy table
    - dq_rules_catalog   # Metadata pass-through
    - dq_issues_catalog  # Metadata pass-through

```

## rules.yaml
```yaml

```
