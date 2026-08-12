# Bronze Layer Spec — securities-master

## tables.yaml
```yaml
layer: bronze
catalog: statestreet
schema: b_statestreet
source:
  type: volume
  path: /Volumes/statestreet/securities_master/raw_files/
  format: csv
  delimiter: ","
  header: true

tables:

  # ─────────────────────────────────────────────
  # GROUP 1 — Base / Core Reference Tables
  # ─────────────────────────────────────────────

  - name: product
    source_file: product.csv
    description: "Base table for all securities. Every security has exactly one row here. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,              type: STRING,         nullable: false, description: "Unique security identifier (PK)"}
      - {name: id_type,                 type: STRING,         nullable: false, description: "Primary identifier type: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID"}
      - {name: type,                    type: STRING,         nullable: false, description: "Product category: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT"}
      - {name: sub_type,                type: STRING,         nullable: true,  description: "Subcategory: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE"}
      - {name: status,                  type: STRING,         nullable: false, description: "Lifecycle status: ACTIVE, INACTIVE, MATURED, SUSPENDED, DELISTED"}
      - {name: settlement_type,         type: STRING,         nullable: true,  description: "Settlement method"}
      - {name: description,             type: STRING,         nullable: true,  description: "Human-readable security name"}
      - {name: issue_date,              type: DATE,           nullable: true,  description: "Date the security was issued"}
      - {name: issue_price,             type: DECIMAL(28,8),  nullable: true,  description: "Price at issuance"}
      - {name: current_face_value,      type: DECIMAL(28,8),  nullable: true,  description: "Current face / par value"}
      - {name: issuer_legal_entity_id,  type: STRING,         nullable: true,  description: "FK to legal_entity.legal_entity_id"}
      - {name: tick_ladder_scale_id,    type: STRING,         nullable: true,  description: "FK to tick_ladder_scale.tick_ladder_scale_id"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE,   description: "Partition column derived from _ingestion_ts"}

  - name: legal_entity
    source_file: legal_entity.csv
    description: "Legal entities (issuers, counterparties). Grain: one row per legal_entity_id per load."
    primary_key: [legal_entity_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: legal_entity_id,  type: STRING,  nullable: false, description: "Unique entity identifier (PK)"}
      - {name: legal_name,       type: STRING,  nullable: false, description: "Legal entity name"}
      - {name: country,          type: STRING,  nullable: true,  description: "ISO 3166-1 alpha-2 country code"}
      - {name: entity_type,      type: STRING,  nullable: true,  description: "BANK, CORPORATE, GOVERNMENT, etc."}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: currency
    source_file: currency.csv
    description: "ISO 4217 currency reference. Contains 2 deliberately bad rows for DQ testing. Grain: one row per currency_code."
    primary_key: [currency_code]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: currency_code,  type: STRING,  nullable: false, description: "ISO 4217 three-letter currency code (PK)"}
      - {name: currency_name,  type: STRING,  nullable: true,  description: "Full currency name"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: series
    source_file: series.csv
    description: "Optional grouping for Stock and ListedDerivative. Grain: one row per series_id."
    primary_key: [series_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: series_id,    type: STRING,  nullable: false, description: "Unique series identifier (PK)"}
      - {name: series_name,  type: STRING,  nullable: true,  description: "Descriptive series name"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: tick_ladder_scale
    source_file: tick_ladder_scale.csv
    description: "Minimum price increment scales. Grain: one row per tick_ladder_scale_id."
    primary_key: [tick_ladder_scale_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: tick_ladder_scale_id,    type: STRING,  nullable: false, description: "Unique scale identifier (PK)"}
      - {name: tick_ladder_scale_name,  type: STRING,  nullable: true,  description: "Descriptive scale name"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: tick
    source_file: tick.csv
    description: "Individual tick entries within a tick ladder scale. Grain: one row per tick_id."
    primary_key: [tick_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: tick_id,              type: STRING,         nullable: false, description: "Unique tick identifier (PK)"}
      - {name: tick_ladder_scale_id, type: STRING,         nullable: false, description: "FK to tick_ladder_scale.tick_ladder_scale_id"}
      - {name: price_from,           type: DECIMAL(28,8),  nullable: true,  description: "Lower bound of price range for this tick"}
      - {name: price_to,             type: DECIMAL(28,8),  nullable: true,  description: "Upper bound of price range for this tick"}
      - {name: tick_size,            type: DECIMAL(28,8),  nullable: true,  description: "Minimum price increment in this range"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: principal_redemption_provision
    source_file: principal_redemption_provision.csv
    description: "Principal redemption provision definitions. Grain: one row per provision_id."
    primary_key: [principal_redemption_provision_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: principal_redemption_provision_id,   type: STRING,  nullable: false, description: "Unique provision identifier (PK)"}
      - {name: provision_type,                      type: STRING,  nullable: true,  description: "Type of redemption provision (e.g. CALL, PUT, SINKING_FUND)"}
      - {name: description,                         type: STRING,  nullable: true,  description: "Human-readable description of the provision"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: product_rating_type
    source_file: product_rating_type.csv
    description: "Reference for rating type definitions (agency, scale, category). Grain: one row per product_rating_type_id."
    primary_key: [product_rating_type_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_rating_type_id,  type: STRING,  nullable: false, description: "Unique rating type identifier (PK)"}
      - {name: rating_agency,           type: STRING,  nullable: true,  description: "Rating agency name (e.g. MOODYS, SP, FITCH)"}
      - {name: rating_type_code,        type: STRING,  nullable: true,  description: "Short code for the rating type"}
      - {name: rating_scale,            type: STRING,  nullable: true,  description: "Rating scale description"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  # ─────────────────────────────────────────────
  # GROUP 2 — Product Subtype Tables (extend product)
  # ─────────────────────────────────────────────

  - name: fund
    source_file: fund.csv
    description: "Fund-specific attributes. Extends product. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,       type: STRING,  nullable: false, description: "FK to product.product_id (PK)"}
      - {name: endness_type,     type: STRING,  nullable: true,  description: "OPEN_END or CLOSED_END"}
      - {name: mutual_fund_type, type: STRING,  nullable: true,  description: "Mutual fund classification"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: right
    source_file: right.csv
    description: "Subscription rights extending product. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,           type: STRING,         nullable: false, description: "FK to product.product_id (PK)"}
      - {name: subscription_ratio,   type: DECIMAL(28,8),  nullable: true,  description: "Rights subscription ratio"}
      - {name: expiry_date,          type: DATE,           nullable: true,  description: "Rights expiry date"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: debt
    source_file: debt.csv
    description: "Debt-specific attributes. Extends product. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,            type: STRING,         nullable: false, description: "FK to product.product_id (PK)"}
      - {name: total_amount_issued,   type: DECIMAL(28,8),  nullable: true,  description: "Total face amount at issuance"}
      - {name: issue_currency_code,   type: STRING,         nullable: true,  description: "ISO 4217 currency code for issuance amount"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: bond
    source_file: bond.csv
    description: "Bond-specific attributes. Extends debt. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,          type: STRING,  nullable: false, description: "FK to product.product_id and debt.product_id (PK)"}
      - {name: coupon_type,         type: STRING,  nullable: false, description: "FIXED, FLOATING, or ZERO"}
      - {name: maturity_date,       type: DATE,    nullable: false, description: "Date the bond matures"}
      - {name: issue_currency_code, type: STRING,  nullable: false, description: "ISO 4217 currency code for face value"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: muni
    source_file: muni.csv
    description: "Municipal bond-specific attributes. Extends bond. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,   type: STRING,   nullable: false, description: "FK to bond.product_id (PK)"}
      - {name: tax_exempt,   type: BOOLEAN,  nullable: true,  description: "Whether interest is tax-exempt"}
      - {name: state,        type: STRING,   nullable: true,  description: "US state abbreviation of issuing municipality"}
      - {name: purpose,      type: STRING,   nullable: true,  description: "Purpose of the municipal bond issuance"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: pool_backed_security
    source_file: pool_backed_security.csv
    description: "Pool-backed security attributes. Extends debt (not bond). Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,   type: STRING,  nullable: false, description: "FK to debt.product_id (PK)"}
      - {name: pool_type,    type: STRING,  nullable: true,  description: "Pool type (e.g. MBS, ABS, CMO)"}
      - {name: originator,   type: STRING,  nullable: true,  description: "Originating institution name"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: stock
    source_file: stock.csv
    description: "Stock-specific attributes. Extends product. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,  type: STRING,  nullable: false, description: "FK to product.product_id (PK)"}
      - {name: series_id,   type: STRING,  nullable: true,  description: "FK to series.series_id"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: common_stock
    source_file: common_stock.csv
    description: "Common stock attributes. Extends stock. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,     type: STRING,   nullable: false, description: "FK to stock.product_id (PK)"}
      - {name: voting_rights,  type: BOOLEAN,  nullable: true,  description: "Whether shares carry voting rights"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: preferred_stock
    source_file: preferred_stock.csv
    description: "Preferred stock attributes. Extends stock. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,     type: STRING,  nullable: false, description: "FK to stock.product_id (PK)"}
      - {name: dividend_right, type: STRING,  nullable: true,  description: "Dividend entitlement type: CUMULATIVE or NON_CUMULATIVE"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: listed_derivative
    source_file: listed_derivative.csv
    description: "Listed derivative attributes. Extends product. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,            type: STRING,  nullable: false, description: "FK to product.product_id (PK)"}
      - {name: series_id,             type: STRING,  nullable: true,  description: "FK to series.series_id"}
      - {name: underlying_product_id, type: STRING,  nullable: true,  description: "FK to product.product_id — the underlying instrument"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: option
    source_file: option.csv
    description: "Option contract attributes. Extends listed_derivative. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,      type: STRING,         nullable: false, description: "FK to listed_derivative.product_id (PK)"}
      - {name: option_type,     type: STRING,         nullable: false, description: "CALL or PUT"}
      - {name: exercise_style,  type: STRING,         nullable: false, description: "AMERICAN or EUROPEAN"}
      - {name: strike_price,    type: DECIMAL(28,8),  nullable: true,  description: "Strike / exercise price"}
      - {name: expiry_date,     type: DATE,           nullable: true,  description: "Option expiration date"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: future
    source_file: future.csv
    description: "Futures contract attributes. Extends listed_derivative. Grain: one row per product_id."
    primary_key: [product_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,        type: STRING,  nullable: false, description: "FK to listed_derivative.product_id (PK)"}
      - {name: delivery_date,     type: DATE,    nullable: true,  description: "Futures delivery / settlement date"}
      - {name: valuation_method,  type: STRING,  nullable: true,  description: "Valuation method (e.g. MARK_TO_MARKET)"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  # ─────────────────────────────────────────────
  # GROUP 3 — Relationship / Fact Tables
  # ─────────────────────────────────────────────

  - name: identifiers
    source_file: identifiers.csv
    description: "Alternate identifier aliases for a product. Grain: one row per identifier_id."
    primary_key: [identifier_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: identifier_id,     type: STRING,  nullable: false, description: "Unique identifier record PK"}
      - {name: product_id,        type: STRING,  nullable: false, description: "FK to product.product_id"}
      - {name: id_type,           type: STRING,  nullable: false, description: "Identifier type: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID"}
      - {name: identifier_value,  type: STRING,  nullable: false, description: "The actual identifier string"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: classification
    source_file: classification.csv
    description: "Product classification tags (e.g. sector, asset class). Grain: one row per classification_id."
    primary_key: [classification_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: classification_id,     type: STRING,  nullable: false, description: "Unique classification record PK"}
      - {name: product_id,            type: STRING,  nullable: false, description: "FK to product.product_id"}
      - {name: classification_type,   type: STRING,  nullable: true,  description: "Classification scheme name (e.g. GICS, ICB, SIC)"}
      - {name: classification_value,  type: STRING,  nullable: true,  description: "Classification code or value"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: product_rating
    source_file: product_rating.csv
    description: "Credit and other ratings assigned to a product over time. Grain: one row per product_rating_id."
    primary_key: [product_rating_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_rating_id,      type: STRING,  nullable: false, description: "Unique rating record PK"}
      - {name: product_id,             type: STRING,  nullable: false, description: "FK to product.product_id"}
      - {name: product_rating_type_id, type: STRING,  nullable: true,  description: "FK to product_rating_type.product_rating_type_id"}
      - {name: rating_value,           type: STRING,  nullable: false, description: "Rating code (e.g. AAA, BBB-, BB+)"}
      - {name: watch_code,             type: STRING,  nullable: true,  description: "Credit watch indicator (e.g. POSITIVE, NEGATIVE, STABLE)"}
      - {name: rating_agency,          type: STRING,  nullable: true,  description: "Rating agency (e.g. MOODYS, SP, FITCH)"}
      - {name: effective_from_date,    type: DATE,    nullable: false, description: "Date this rating became effective"}
      - {name: effective_to_date,      type: DATE,    nullable: true,  description: "Date this rating was superseded (NULL = still active)"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: coupon
    source_file: coupon.csv
    description: "Coupon payment schedule for bonds. Grain: one row per coupon_id."
    primary_key: [coupon_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: coupon_id,     type: STRING,         nullable: false, description: "Unique coupon record PK"}
      - {name: product_id,    type: STRING,         nullable: false, description: "FK to bond.product_id"}
      - {name: coupon_rate,   type: DECIMAL(18,8),  nullable: false, description: "Annual coupon rate as a percentage"}
      - {name: payment_date,  type: DATE,           nullable: false, description: "Scheduled coupon payment date"}
      - {name: coupon_type,   type: STRING,         nullable: true,  description: "FIXED or FLOATING"}
      - {name: frequency,     type: STRING,         nullable: true,  description: "ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  # ─────────────────────────────────────────────
  # GROUP 4 — Bridge Tables
  # ─────────────────────────────────────────────

  - name: listed_derivative_tick
    source_file: listed_derivative_tick.csv
    description: "Bridge table linking listed derivatives to tick entries. Grain: one row per (product_id, tick_id)."
    primary_key: [product_id, tick_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,  type: STRING,  nullable: false, description: "FK to listed_derivative.product_id"}
      - {name: tick_id,     type: STRING,  nullable: false, description: "FK to tick.tick_id"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: debt_principal_redemption_provision
    source_file: debt_principal_redemption_provision.csv
    description: "Bridge table linking debt instruments to redemption provisions. Grain: one row per (product_id, principal_redemption_provision_id)."
    primary_key: [product_id, principal_redemption_provision_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    columns:
      - {name: product_id,                         type: STRING,  nullable: false, description: "FK to debt.product_id"}
      - {name: principal_redemption_provision_id,  type: STRING,  nullable: false, description: "FK to principal_redemption_provision.principal_redemption_provision_id"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  # ─────────────────────────────────────────────
  # GROUP 5 — Legacy / Metadata Tables
  # (Bronze only — no Silver or Gold treatment)
  # ─────────────────────────────────────────────

  - name: generic_product
    source_file: generic_product.csv
    description: "Deprecated legacy shadow table. One product maps to MANY generic_product rows. No uniqueness DQ rule applied. Bronze landing only."
    primary_key: []
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    notes: "No PK uniqueness check — by design (legacy M:1 relationship). No Silver or Gold build."
    columns:
      - {name: product_id,       type: STRING,  nullable: true, description: "FK to product.product_id (non-unique here by design)"}
      - {name: generic_code,     type: STRING,  nullable: true, description: "Legacy generic product code"}
      - {name: generic_name,     type: STRING,  nullable: true, description: "Legacy generic product name"}
      - {name: generic_type,     type: STRING,  nullable: true, description: "Legacy generic product type classification"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: dq_rules_catalog
    source_file: dq_rules_catalog.csv
    description: "DQ rule metadata definitions. Reference / audit table. Bronze landing only — no Silver DQ rules applied."
    primary_key: [rule_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    notes: "Metadata table. No Silver conformance or Gold mart build."
    columns:
      - {name: rule_id,          type: STRING,  nullable: false, description: "Unique DQ rule identifier (PK)"}
      - {name: table_name,       type: STRING,  nullable: true,  description: "Target table the rule applies to"}
      - {name: column_name,      type: STRING,  nullable: true,  description: "Target column the rule applies to (NULL = table-level rule)"}
      - {name: dq_dimension,     type: STRING,  nullable: true,  description: "DQ dimension: Validity, Completeness, Uniqueness, Consistency, Accuracy, Timeliness"}
      - {name: rule_type,        type: STRING,  nullable: true,  description: "Rule pattern type (ENUM_MEMBERSHIP, NOT_NULL, etc.)"}
      - {name: severity,         type: STRING,  nullable: true,  description: "HIGH, MEDIUM, or LOW"}
      - {name: description,      type: STRING,  nullable: true,  description: "Human-readable rule description"}
      - {name: rule_logic_sql,   type: STRING,  nullable: true,  description: "SQL SELECT that returns failing rows"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

  - name: dq_issues_catalog
    source_file: dq_issues_catalog.csv
    description: "Known DQ issue log from source system. Reference / audit table. Bronze landing only — no Silver DQ rules applied."
    primary_key: [issue_id]
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    notes: "Metadata table. No Silver conformance or Gold mart build."
    columns:
      - {name: issue_id,          type: STRING,  nullable: false, description: "Unique issue identifier (PK)"}
      - {name: rule_id,           type: STRING,  nullable: true,  description: "FK to dq_rules_catalog.rule_id"}
      - {name: table_name,        type: STRING,  nullable: true,  description: "Table in which the issue was detected"}
      - {name: column_name,       type: STRING,  nullable: true,  description: "Column in which the issue was detected"}
      - {name: issue_description, type: STRING,  nullable: true,  description: "Description of the known data quality issue"}
      - {name: workaround,        type: STRING,  nullable: true,  description: "Documented workaround or accepted deviation"}
      - {name: severity,          type: STRING,  nullable: true,  description: "HIGH, MEDIUM, or LOW"}
      - {name: reported_date,     type: DATE,    nullable: true,  description: "Date the issue was first reported"}
      - {name: resolved_date,     type: DATE,    nullable: true,  description: "Date the issue was resolved (NULL = open)"}
    metadata_columns:
      - {name: _source_file,    type: STRING}
      - {name: _ingestion_ts,   type: TIMESTAMP}
      - {name: _batch_id,       type: STRING}
      - {name: _row_hash,       type: STRING}
      - {name: _ingestion_date, type: DATE}

```

## rules.yaml
```yaml

```
