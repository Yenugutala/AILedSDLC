# Bronze Layer Spec — securities-master

## tables.yaml
```yaml
layer: bronze
catalog: statestreet
schema: b_statestreet
description: "Bronze landing layer for Securities Master Data. Raw CSV ingestion from
  Databricks Volume with no transformation. Schema drift handled via auto-merge
  (additive) or quarantine (breaking). All 29 source tables loaded here."

defaults:
  iceberg_uniform: true
  partition_by: [_ingestion_date]
  metadata_columns:
    - {name: _source_file,    type: STRING,    nullable: false, description: "Source CSV filename"}
    - {name: _ingestion_ts,   type: TIMESTAMP, nullable: false, description: "Timestamp when row was loaded to Bronze"}
    - {name: _batch_id,       type: STRING,    nullable: false, description: "Pipeline run identifier"}
    - {name: _row_hash,       type: STRING,    nullable: false, description: "SHA256 of all data columns for CDC / change detection"}
    - {name: _ingestion_date, type: DATE,      nullable: false, description: "Partition column derived from _ingestion_ts"}

tables:

  # ─────────────────────────────────────────────
  # GROUP 1 — Core product base & reference dims
  # ─────────────────────────────────────────────

  - name: product
    description: "Base table for all securities. Every security has exactly one row here. Class-table inheritance root."
    source_file: product.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
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
      - {name: current_face_value,      type: DECIMAL(28,8),  nullable: true,  description: "Current face/par value"}
      - {name: issuer_legal_entity_id,  type: STRING,         nullable: true,  description: "FK to legal_entity.legal_entity_id"}
      - {name: tick_ladder_scale_id,    type: STRING,         nullable: true,  description: "FK to tick_ladder_scale.tick_ladder_scale_id"}
    metadata_columns: ${defaults.metadata_columns}

  - name: legal_entity
    description: "Issuer and counterparty reference data. SCD2 applied in Silver."
    source_file: legal_entity.csv
    primary_key: [legal_entity_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: legal_entity_id,  type: STRING,  nullable: false, description: "Unique entity identifier (PK)"}
      - {name: legal_name,       type: STRING,  nullable: false, description: "Legal entity name"}
      - {name: short_name,       type: STRING,  nullable: true,  description: "Abbreviated entity name"}
      - {name: country,          type: STRING,  nullable: true,  description: "ISO 3166-1 alpha-2 country code"}
      - {name: entity_type,      type: STRING,  nullable: true,  description: "Entity classification: BANK, CORPORATE, GOVERNMENT, etc."}
      - {name: lei_code,         type: STRING,  nullable: true,  description: "Legal Entity Identifier (LEI) — 20-character alphanumeric"}
      - {name: status,           type: STRING,  nullable: true,  description: "Entity status: ACTIVE, INACTIVE"}
    metadata_columns: ${defaults.metadata_columns}

  - name: currency
    description: "ISO 4217 currency reference. Contains 2 deliberately bad rows (seeded for DQ testing — see USE-CASE-002)."
    source_file: currency.csv
    primary_key: [currency_code]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: currency_code,   type: STRING,  nullable: false, description: "ISO 4217 3-letter currency code (PK)"}
      - {name: currency_name,   type: STRING,  nullable: true,  description: "Full currency name"}
      - {name: numeric_code,    type: STRING,  nullable: true,  description: "ISO 4217 numeric code"}
      - {name: minor_units,     type: INTEGER, nullable: true,  description: "Number of decimal places"}
    metadata_columns: ${defaults.metadata_columns}

  - name: series
    description: "Optional grouping dimension for Stock and ListedDerivative securities."
    source_file: series.csv
    primary_key: [series_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: series_id,     type: STRING,  nullable: false, description: "Unique series identifier (PK)"}
      - {name: series_name,   type: STRING,  nullable: true,  description: "Series name or label"}
      - {name: series_type,   type: STRING,  nullable: true,  description: "Classification of the series"}
    metadata_columns: ${defaults.metadata_columns}

  - name: tick_ladder_scale
    description: "Minimum price increment scale reference — groups one or more tick entries."
    source_file: tick_ladder_scale.csv
    primary_key: [tick_ladder_scale_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: tick_ladder_scale_id,   type: STRING,  nullable: false, description: "Unique scale identifier (PK)"}
      - {name: scale_name,             type: STRING,  nullable: true,  description: "Descriptive name for the tick scale"}
      - {name: market,                 type: STRING,  nullable: true,  description: "Market or exchange this scale applies to"}
    metadata_columns: ${defaults.metadata_columns}

  - name: tick
    description: "Individual tick entries within a tick ladder scale."
    source_file: tick.csv
    primary_key: [tick_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: tick_id,               type: STRING,         nullable: false, description: "Unique tick entry identifier (PK)"}
      - {name: tick_ladder_scale_id,  type: STRING,         nullable: false, description: "FK to tick_ladder_scale.tick_ladder_scale_id"}
      - {name: price_from,            type: DECIMAL(28,8),  nullable: true,  description: "Lower bound of price range for this tick size"}
      - {name: price_to,              type: DECIMAL(28,8),  nullable: true,  description: "Upper bound of price range for this tick size"}
      - {name: tick_size,             type: DECIMAL(28,8),  nullable: true,  description: "Minimum price increment within this range"}
    metadata_columns: ${defaults.metadata_columns}

  - name: principal_redemption_provision
    description: "Reference table of principal redemption provision types for debt instruments."
    source_file: principal_redemption_provision.csv
    primary_key: [principal_redemption_provision_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: principal_redemption_provision_id,   type: STRING,  nullable: false, description: "Unique provision identifier (PK)"}
      - {name: provision_type,                      type: STRING,  nullable: true,  description: "Type of redemption provision: CALL, PUT, SINKING_FUND, etc."}
      - {name: description,                         type: STRING,  nullable: true,  description: "Human-readable description of the provision"}
    metadata_columns: ${defaults.metadata_columns}

  # ─────────────────────────────────────────────
  # GROUP 2 — Product subtype tables
  # ─────────────────────────────────────────────

  - name: fund
    description: "Fund subtype — extends product. Adds fund-specific attributes."
    source_file: fund.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,        type: STRING,  nullable: false, description: "FK to product.product_id (PK)"}
      - {name: endness_type,      type: STRING,  nullable: true,  description: "Fund structure: OPEN_END or CLOSED_END"}
      - {name: mutual_fund_type,  type: STRING,  nullable: true,  description: "Fund classification: EQUITY, BOND, BALANCED, MONEY_MARKET, etc."}
      - {name: nav_frequency,     type: STRING,  nullable: true,  description: "Frequency of NAV calculation: DAILY, WEEKLY, MONTHLY"}
    metadata_columns: ${defaults.metadata_columns}

  - name: right
    description: "Right subtype — extends product. Subscription rights for existing shareholders."
    source_file: right.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,          type: STRING,         nullable: false, description: "FK to product.product_id (PK)"}
      - {name: underlying_product_id, type: STRING,       nullable: true,  description: "FK to product.product_id — the underlying equity"}
      - {name: subscription_price,  type: DECIMAL(28,8),  nullable: true,  description: "Price at which right can be exercised"}
      - {name: expiry_date,         type: DATE,           nullable: true,  description: "Date the right expires"}
      - {name: subscription_ratio,  type: DECIMAL(28,8),  nullable: true,  description: "Number of rights needed to subscribe to one share"}
    metadata_columns: ${defaults.metadata_columns}

  - name: stock
    description: "Stock subtype — extends product. Adds series linkage. Parent of common_stock and preferred_stock."
    source_file: stock.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,  type: STRING,  nullable: false, description: "FK to product.product_id (PK)"}
      - {name: series_id,   type: STRING,  nullable: true,  description: "FK to series.series_id"}
    metadata_columns: ${defaults.metadata_columns}

  - name: common_stock
    description: "CommonStock subtype — extends stock. Adds voting rights flag."
    source_file: common_stock.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,     type: STRING,   nullable: false, description: "FK to product.product_id and stock.product_id (PK)"}
      - {name: voting_rights,  type: BOOLEAN,  nullable: true,  description: "TRUE if this share class carries voting rights"}
    metadata_columns: ${defaults.metadata_columns}

  - name: preferred_stock
    description: "PreferredStock subtype — extends stock. Adds dividend preference attributes."
    source_file: preferred_stock.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,     type: STRING,  nullable: false, description: "FK to product.product_id and stock.product_id (PK)"}
      - {name: dividend_right, type: STRING,  nullable: true,  description: "Dividend preference type: CUMULATIVE, NON_CUMULATIVE"}
      - {name: par_value,      type: DECIMAL(28,8), nullable: true, description: "Par value per preferred share"}
    metadata_columns: ${defaults.metadata_columns}

  - name: debt
    description: "Debt subtype — extends product. Parent of bond, muni, pool_backed_security."
    source_file: debt.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,            type: STRING,         nullable: false, description: "FK to product.product_id (PK)"}
      - {name: total_amount_issued,   type: DECIMAL(28,8),  nullable: true,  description: "Total face amount issued"}
      - {name: issue_currency_code,   type: STRING,         nullable: true,  description: "ISO 4217 currency code for issued amount"}
      - {name: seniority,             type: STRING,         nullable: true,  description: "Debt seniority: SENIOR, SUBORDINATED, JUNIOR"}
    metadata_columns: ${defaults.metadata_columns}

  - name: bond
    description: "Bond subtype — extends debt. Adds coupon and maturity attributes."
    source_file: bond.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,          type: STRING,         nullable: false, description: "FK to product.product_id and debt.product_id (PK)"}
      - {name: coupon_type,         type: STRING,         nullable: false, description: "FIXED, FLOATING, or ZERO"}
      - {name: maturity_date,       type: DATE,           nullable: false, description: "Date the bond matures"}
      - {name: issue_currency_code, type: STRING,         nullable: false, description: "ISO 4217 currency code for face value"}
      - {name: coupon_rate,         type: DECIMAL(28,8),  nullable: true,  description: "Annual coupon rate as a percentage"}
      - {name: coupon_frequency,    type: STRING,         nullable: true,  description: "ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY"}
    metadata_columns: ${defaults.metadata_columns}

  - name: muni
    description: "Muni subtype — extends bond. Adds municipal-specific tax and purpose attributes."
    source_file: muni.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,   type: STRING,   nullable: false, description: "FK to product.product_id and bond.product_id (PK)"}
      - {name: tax_exempt,   type: BOOLEAN,  nullable: true,  description: "TRUE if interest is federally tax-exempt"}
      - {name: state,        type: STRING,   nullable: true,  description: "US state of issuance (2-letter abbreviation)"}
      - {name: purpose,      type: STRING,   nullable: true,  description: "Purpose of bond issuance: GENERAL_OBLIGATION, REVENUE, etc."}
    metadata_columns: ${defaults.metadata_columns}

  - name: pool_backed_security
    description: "PoolBackedSecurity subtype — extends debt (NOT bond). Asset-backed / mortgage-backed securities."
    source_file: pool_backed_security.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,   type: STRING,  nullable: false, description: "FK to product.product_id and debt.product_id (PK)"}
      - {name: pool_type,    type: STRING,  nullable: true,  description: "Type of underlying pool: MORTGAGE, AUTO, STUDENT_LOAN, etc."}
      - {name: originator,   type: STRING,  nullable: true,  description: "Originating institution for the underlying pool"}
      - {name: pass_through_rate, type: DECIMAL(28,8), nullable: true, description: "Pass-through interest rate"}
    metadata_columns: ${defaults.metadata_columns}

  - name: listed_derivative
    description: "ListedDerivative subtype — extends product. Parent of option and future."
    source_file: listed_derivative.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,              type: STRING,  nullable: false, description: "FK to product.product_id (PK)"}
      - {name: series_id,               type: STRING,  nullable: true,  description: "FK to series.series_id"}
      - {name: underlying_product_id,   type: STRING,  nullable: true,  description: "FK to product.product_id — the underlying instrument"}
    metadata_columns: ${defaults.metadata_columns}

  - name: option
    description: "Option subtype — extends listed_derivative. Adds option-specific exercise attributes."
    source_file: option.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,      type: STRING,         nullable: false, description: "FK to product.product_id and listed_derivative.product_id (PK)"}
      - {name: option_type,     type: STRING,         nullable: false, description: "CALL or PUT"}
      - {name: exercise_style,  type: STRING,         nullable: false, description: "AMERICAN or EUROPEAN"}
      - {name: strike_price,    type: DECIMAL(28,8),  nullable: true,  description: "Strike / exercise price"}
      - {name: expiry_date,     type: DATE,           nullable: true,  description: "Option expiration date"}
    metadata_columns: ${defaults.metadata_columns}

  - name: future
    description: "Future subtype — extends listed_derivative. Adds delivery and valuation attributes."
    source_file: future.csv
    primary_key: [product_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,         type: STRING,  nullable: false, description: "FK to product.product_id and listed_derivative.product_id (PK)"}
      - {name: delivery_date,      type: DATE,    nullable: true,  description: "Futures delivery / settlement date"}
      - {name: valuation_method,   type: STRING,  nullable: true,  description: "MARK_TO_MARKET or other valuation convention"}
      - {name: contract_size,      type: DECIMAL(28,8), nullable: true, description: "Standard contract size / lot size"}
    metadata_columns: ${defaults.metadata_columns}

  # ─────────────────────────────────────────────
  # GROUP 3 — Relationship and enrichment tables
  # ─────────────────────────────────────────────

  - name: identifiers
    description: "Cross-reference / alias identifiers per product. Multiple identifier types per product_id."
    source_file: identifiers.csv
    primary_key: [identifier_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: identifier_id,     type: STRING,  nullable: false, description: "Unique identifier record PK"}
      - {name: product_id,        type: STRING,  nullable: false, description: "FK to product.product_id"}
      - {name: id_type,           type: STRING,  nullable: false, description: "Identifier type: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID"}
      - {name: identifier_value,  type: STRING,  nullable: false, description: "The actual identifier string value"}
    metadata_columns: ${defaults.metadata_columns}

  - name: classification
    description: "Product classification tags — multiple classification entries per product."
    source_file: classification.csv
    primary_key: [classification_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: classification_id,    type: STRING,  nullable: false, description: "Unique classification record PK"}
      - {name: product_id,           type: STRING,  nullable: false, description: "FK to product.product_id"}
      - {name: classification_type,  type: STRING,  nullable: true,  description: "Classification scheme: GICS, SIC, NAICS, etc."}
      - {name: classification_code,  type: STRING,  nullable: true,  description: "Classification code within the scheme"}
      - {name: classification_name,  type: STRING,  nullable: true,  description: "Human-readable classification name"}
    metadata_columns: ${defaults.metadata_columns}

  - name: product_rating
    description: "Credit / analyst ratings per product. SCD2 applied in Silver. Multiple ratings per product."
    source_file: product_rating.csv
    primary_key: [product_rating_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_rating_id,       type: STRING,  nullable: false, description: "Unique rating record PK"}
      - {name: product_id,              type: STRING,  nullable: false, description: "FK to product.product_id"}
      - {name: product_rating_type_id,  type: STRING,  nullable: true,  description: "FK to product_rating_type.product_rating_type_id"}
      - {name: rating_agency,           type: STRING,  nullable: true,  description: "Rating agency: MOODYS, SP, FITCH, etc."}
      - {name: rating_value,            type: STRING,  nullable: false, description: "Rating code: AAA, AA+, BBB-, etc."}
      - {name: effective_from_date,     type: DATE,    nullable: false, description: "Date this rating became effective"}
      - {name: effective_to_date,       type: DATE,    nullable: true,  description: "Date this rating was superseded (NULL = current)"}
      - {name: watch_code,              type: STRING,  nullable: true,  description: "Outlook / watch flag: POSITIVE, STABLE, NEGATIVE, WATCH"}
      - {name: rating_scale,            type: STRING,  nullable: true,  description: "Rating scale type: LONG_TERM, SHORT_TERM"}
    metadata_columns: ${defaults.metadata_columns}

  - name: product_rating_type
    description: "Reference table for rating type classifications."
    source_file: product_rating_type.csv
    primary_key: [product_rating_type_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_rating_type_id,  type: STRING,  nullable: false, description: "Unique rating type PK"}
      - {name: rating_type_code,        type: STRING,  nullable: true,  description: "Short code for the rating type"}
      - {name: rating_type_description, type: STRING,  nullable: true,  description: "Full description of rating type"}
      - {name: rating_agency,           type: STRING,  nullable: true,  description: "Agency associated with this rating type"}
    metadata_columns: ${defaults.metadata_columns}

  - name: coupon
    description: "Coupon payment schedule for bond instruments. One row per coupon payment date per bond."
    source_file: coupon.csv
    primary_key: [coupon_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: coupon_id,      type: STRING,         nullable: false, description: "Unique coupon record PK"}
      - {name: product_id,     type: STRING,         nullable: false, description: "FK to bond.product_id"}
      - {name: coupon_rate,    type: DECIMAL(28,8),  nullable: false, description: "Annual coupon rate as a percentage"}
      - {name: payment_date,   type: DATE,           nullable: false, description: "Scheduled coupon payment date"}
      - {name: coupon_type,    type: STRING,         nullable: true,  description: "FIXED or FLOATING"}
      - {name: frequency,      type: STRING,         nullable: true,  description: "ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY"}
      - {name: accrual_start,  type: DATE,           nullable: true,  description: "Start of accrual period for this coupon"}
      - {name: accrual_end,    type: DATE,           nullable: true,  description: "End of accrual period for this coupon"}
    metadata_columns: ${defaults.metadata_columns}

  # ─────────────────────────────────────────────
  # GROUP 4 — Bridge / many-to-many tables
  # ─────────────────────────────────────────────

  - name: listed_derivative_tick
    description: "Bridge table — M:M between listed_derivative and tick."
    source_file: listed_derivative_tick.csv
    primary_key: [product_id, tick_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,  type: STRING,  nullable: false, description: "FK to listed_derivative.product_id (composite PK)"}
      - {name: tick_id,     type: STRING,  nullable: false, description: "FK to tick.tick_id (composite PK)"}
    metadata_columns: ${defaults.metadata_columns}

  - name: debt_principal_redemption_provision
    description: "Bridge table — M:M between debt and principal_redemption_provision."
    source_file: debt_principal_redemption_provision.csv
    primary_key: [product_id, principal_redemption_provision_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    columns:
      - {name: product_id,                          type: STRING,  nullable: false, description: "FK to debt.product_id (composite PK)"}
      - {name: principal_redemption_provision_id,   type: STRING,  nullable: false, description: "FK to principal_redemption_provision.principal_redemption_provision_id (composite PK)"}
      - {name: provision_date,                      type: DATE,    nullable: true,  description: "Date the provision takes effect"}
      - {name: provision_price,                     type: DECIMAL(28,8), nullable: true, description: "Price at which provision is exercised"}
    metadata_columns: ${defaults.metadata_columns}

  # ─────────────────────────────────────────────
  # GROUP 5 — Legacy and metadata tables
  # ─────────────────────────────────────────────

  - name: generic_product
    description: "Deprecated legacy shadow table. One product maps to MANY generic_product rows by design.
      No uniqueness DQ rules applied. Bronze-only — no Silver conformance needed. See USE-CASE-003."
    source_file: generic_product.csv
    primary_key: []
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    notes:
      - "No primary key — multiple rows per product_id is intentional (legacy design)"
      - "No Silver or Gold processing — Bronze landing only"
    columns:
      - {name: product_id,           type: STRING,  nullable: true,  description: "FK to product.product_id (NOT unique in this table)"}
      - {name: generic_product_id,   type: STRING,  nullable: true,  description: "Legacy generic product identifier"}
      - {name: generic_type,         type: STRING,  nullable: true,  description: "Legacy product type classification"}
      - {name: generic_description,  type: STRING,  nullable: true,  description: "Legacy free-text description"}
      - {name: source_system,        type: STRING,  nullable: true,  description: "Originating source system for this legacy record"}
    metadata_columns: ${defaults.metadata_columns}

  - name: dq_rules_catalog
    description: "Metadata table — catalogue of all 128 DQ rules. Bronze-only, no Silver/Gold.
      See USE-CASE-001."
    source_file: dq_rules_catalog.csv
    primary_key: [rule_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    notes:
      - "Metadata table — no DQ rules applied to this table itself"
      - "No Silver or Gold processing"
    columns:
      - {name: rule_id,          type: STRING,  nullable: false, description: "Unique DQ rule identifier (PK)"}
      - {name: table_name,       type: STRING,  nullable: true,  description: "Target table the rule applies to"}
      - {name: column_name,      type: STRING,  nullable: true,  description: "Target column (NULL for table-level rules)"}
      - {name: dq_dimension,     type: STRING,  nullable: true,  description: "DQ dimension: Validity, Completeness, Uniqueness, Consistency, Accuracy, Timeliness"}
      - {name: rule_type,        type: STRING,  nullable: true,  description: "Rule pattern: ENUM_MEMBERSHIP, NOT_NULL, RANGE_CHECK, etc."}
      - {name: severity,         type: STRING,  nullable: true,  description: "HIGH, MEDIUM, or LOW"}
      - {name: description,      type: STRING,  nullable: true,  description: "Human-readable rule description"}
      - {name: rule_logic,       type: STRING,  nullable: true,  description: "SQL or pseudocode expressing the rule logic"}
    metadata_columns: ${defaults.metadata_columns}

  - name: dq_issues_catalog
    description: "Metadata table — catalogue of known DQ issues / exceptions. Bronze-only, no Silver/Gold.
      See USE-CASE-001."
    source_file: dq_issues_catalog.csv
    primary_key: [issue_id]
    iceberg_uniform: true
    partition_by: [_ingestion_date]
    notes:
      - "Metadata table — no DQ rules applied to this table itself"
      - "No Silver or Gold processing"
    columns:
      - {name: issue_id,          type: STRING,  nullable: false, description: "Unique issue identifier (PK)"}
      - {name: rule_id,           type: STRING,  nullable: true,  description: "FK to dq_rules_catalog.rule_id"}
      - {name: table_name,        type: STRING,  nullable: true,  description: "Affected table"}
      - {name: column_name,       type: STRING,  nullable: true,  description: "Affected column"}
      - {name: issue_description, type: STRING,  nullable: true,  description: "Description of the known data quality issue"}
      - {name: workaround,        type: STRING,  nullable: true,  description: "Documented workaround or exception handling"}
      - {name: severity,          type: STRING,  nullable: true,  description: "HIGH, MEDIUM, or LOW"}
      - {name: status,            type: STRING,  nullable: true,  description: "OPEN, RESOLVED, ACCEPTED"}
    metadata_columns: ${defaults.metadata_columns}

```

## rules.yaml
```yaml

```
