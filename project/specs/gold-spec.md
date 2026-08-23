# Gold Layer Spec — securities-master

## tables.yaml
```yaml
layer: gold
catalog: statestreet
schema: g_statestreet

marts:
  # ─────────────────────────────────────────────────────────────────────────────
  # dim_product
  # Grain: one row per product_id (current version only, is_current = TRUE)
  # Flattens all subtype tables via LEFT JOIN on product_id.
  # SCD2 pass-through from Silver product (effective_start_date / effective_end_date).
  # ─────────────────────────────────────────────────────────────────────────────
  - name: dim_product
    description: >
      Flattened product dimension. One row per active product_id, with all
      subtype-specific attributes (stock, bond, fund, derivative, right)
      surfaced as nullable columns. Non-applicable attributes are NULL.
      Preserves SCD2 lineage columns from Silver product.
    grain: one_row_per_product
    grain_key: [product_id]
    source_tables:
      - statestreet.s_statestreet.product
      - statestreet.s_statestreet.stock
      - statestreet.s_statestreet.common_stock
      - statestreet.s_statestreet.preferred_stock
      - statestreet.s_statestreet.debt
      - statestreet.s_statestreet.bond
      - statestreet.s_statestreet.muni
      - statestreet.s_statestreet.pool_backed_security
      - statestreet.s_statestreet.fund
      - statestreet.s_statestreet.right
      - statestreet.s_statestreet.listed_derivative
      - statestreet.s_statestreet.option
      - statestreet.s_statestreet.future
      - statestreet.s_statestreet.series
      - statestreet.s_statestreet.legal_entity
    join_key: product_id
    filter: "p.is_current = TRUE"
    partition_by: [type]
    iceberg_uniform: true
    write_mode: create_or_replace
    columns:
      # ── Core product columns ────────────────────────────────────────────────
      - name: product_id
        type: STRING
        nullable: false
        source_table: product
        description: "Unique security identifier (PK). FK root for all subtype tables."

      - name: id_type
        type: STRING
        nullable: true
        source_table: product
        description: "Primary identifier type: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID"

      - name: type
        type: STRING
        nullable: false
        source_table: product
        description: "Product category: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT"

      - name: sub_type
        type: STRING
        nullable: true
        source_table: product
        description: "Subcategory: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE, FUND, RIGHT"

      - name: status
        type: STRING
        nullable: false
        source_table: product
        description: "Lifecycle: ACTIVE, INACTIVE, MATURED, SUSPENDED, DELISTED"

      - name: settlement_type
        type: STRING
        nullable: true
        source_table: product
        description: "Settlement method."

      - name: description
        type: STRING
        nullable: true
        source_table: product
        description: "Human-readable security name."

      - name: issue_date
        type: DATE
        nullable: true
        source_table: product
        description: "Date the security was issued."

      - name: issue_price
        type: DECIMAL(28, 8)
        nullable: true
        source_table: product
        description: "Price at issuance."

      - name: current_face_value
        type: DECIMAL(28, 8)
        nullable: true
        source_table: product
        description: "Current face/par value."

      - name: issuer_legal_entity_id
        type: STRING
        nullable: true
        source_table: product
        description: "FK to legal_entity. Issuing entity for this security."

      - name: tick_ladder_scale_id
        type: STRING
        nullable: true
        source_table: product
        description: "FK to tick_ladder_scale. Minimum price increment scale."

      # ── SCD2 lineage (from Silver product) ─────────────────────────────────
      - name: effective_start_date
        type: DATE
        nullable: false
        source_table: product
        description: "SCD2 effective start date inherited from Silver product."

      - name: effective_end_date
        type: DATE
        nullable: false
        source_table: product
        description: "SCD2 effective end date. 9999-12-31 = current version."

      # ── Issuer legal entity attributes (denormalized) ───────────────────────
      - name: issuer_legal_name
        type: STRING
        nullable: true
        source_table: legal_entity
        source_column: legal_name
        description: "Legal name of the issuing entity."

      - name: issuer_country
        type: STRING
        nullable: true
        source_table: legal_entity
        source_column: country
        description: "ISO 3166-1 alpha-2 country code of the issuing entity."

      - name: issuer_entity_type
        type: STRING
        nullable: true
        source_table: legal_entity
        source_column: entity_type
        description: "Entity type of the issuer: BANK, CORPORATE, GOVERNMENT, etc."

      # ── Series (shared by stock and listed_derivative) ──────────────────────
      - name: series_id
        type: STRING
        nullable: true
        source_table: stock         # also on listed_derivative; COALESCE(st.series_id, ld.series_id)
        description: "Series identifier. Populated for stock and listed derivative products."

      - name: series_description
        type: STRING
        nullable: true
        source_table: series
        source_column: description
        description: "Human-readable description of the series."

      # ── Stock attributes (EQUITY / CommonStock / PreferredStock) ────────────
      - name: voting_rights
        type: BOOLEAN
        nullable: true
        source_table: common_stock
        description: "Has voting rights. Populated for common stock only."

      - name: dividend_right
        type: STRING
        nullable: true
        source_table: preferred_stock
        description: "Dividend right type for preferred stock: CUMULATIVE, NON_CUMULATIVE."

      # ── Debt base attributes ────────────────────────────────────────────────
      - name: total_amount_issued
        type: DECIMAL(28, 8)
        nullable: true
        source_table: debt
        description: "Total face amount issued. Populated for all Debt subtypes."

      - name: issue_currency_code
        type: STRING
        nullable: true
        source_table: debt
        description: "ISO 4217 currency code of issuance. Populated for all Debt subtypes."

      # ── Bond attributes ─────────────────────────────────────────────────────
      - name: coupon_type
        type: STRING
        nullable: true
        source_table: bond
        description: "Coupon type: FIXED, FLOATING, ZERO. Populated for bond products."

      - name: maturity_date
        type: DATE
        nullable: true
        source_table: bond
        description: "Bond maturity date. Populated for bond products."

      - name: face_currency_code
        type: STRING
        nullable: true
        source_table: bond
        source_column: issue_currency_code
        description: "Face value currency code (from bond.issue_currency_code)."

      - name: day_count_convention
        type: STRING
        nullable: true
        source_table: bond
        description: "Day count convention (e.g. ACT/360, 30/360). May be NULL if not recorded."

      # ── Muni bond attributes ────────────────────────────────────────────────
      - name: tax_exempt
        type: BOOLEAN
        nullable: true
        source_table: muni
        description: "True if the municipal bond is tax-exempt. Populated for muni products."

      - name: muni_state
        type: STRING
        nullable: true
        source_table: muni
        source_column: state
        description: "US state of the municipal issuer."

      - name: muni_purpose
        type: STRING
        nullable: true
        source_table: muni
        source_column: purpose
        description: "Purpose of the municipal bond issuance."

      # ── Pool-backed security attributes ─────────────────────────────────────
      - name: pool_type
        type: STRING
        nullable: true
        source_table: pool_backed_security
        description: "Pool type for mortgage/asset-backed securities."

      - name: originator
        type: STRING
        nullable: true
        source_table: pool_backed_security
        description: "Originating institution for pool-backed securities."

      # ── Fund attributes ─────────────────────────────────────────────────────
      - name: endness_type
        type: STRING
        nullable: true
        source_table: fund
        description: "Fund open/close type: OPEN_END, CLOSED_END."

      - name: mutual_fund_type
        type: STRING
        nullable: true
        source_table: fund
        description: "Mutual fund classification."

      # ── Right attributes ────────────────────────────────────────────────────
      - name: subscription_ratio
        type: DECIMAL(28, 8)
        nullable: true
        source_table: right
        description: "Subscription ratio for rights offerings."

      # ── Listed derivative base attributes ───────────────────────────────────
      - name: underlying_product_id
        type: STRING
        nullable: true
        source_table: listed_derivative
        description: "FK to product.product_id — the underlying security for a derivative."

      # ── Option attributes ───────────────────────────────────────────────────
      - name: option_type
        type: STRING
        nullable: true
        source_table: option
        description: "CALL or PUT. Populated for option products."

      - name: exercise_style
        type: STRING
        nullable: true
        source_table: option
        description: "AMERICAN or EUROPEAN. Populated for option products."

      - name: strike_price
        type: DECIMAL(28, 8)
        nullable: true
        source_table: option
        description: "Strike/exercise price for options."

      - name: expiry_date
        type: DATE
        nullable: true
        source_table: option
        description: "Option expiration date."

      # ── Future attributes ───────────────────────────────────────────────────
      - name: delivery_date
        type: DATE
        nullable: true
        source_table: future
        description: "Futures contract delivery date."

      - name: valuation_method
        type: STRING
        nullable: true
        source_table: future
        description: "Valuation method for futures: MARK_TO_MARKET, etc."

      # ── Pipeline metadata ───────────────────────────────────────────────────
      - name: _row_hash
        type: STRING
        nullable: true
        source_table: product
        description: "SHA256 row hash from Silver product — used for CDC lineage."

      - name: _dq_rule_version
        type: STRING
        nullable: true
        source_table: product
        description: "DQ rule version SHA256 applied to this product row in Silver."

      - name: _gold_build_ts
        type: TIMESTAMP
        nullable: false
        expression: "current_timestamp()"
        description: "Timestamp when this Gold row was last materialized."

  # ─────────────────────────────────────────────────────────────────────────────
  # dim_legal_entity
  # Grain: one row per legal_entity_id (current version only, is_current = TRUE)
  # SCD2 pass-through from Silver legal_entity.
  # ─────────────────────────────────────────────────────────────────────────────
  - name: dim_legal_entity
    description: >
      Legal entity dimension. One row per active legal_entity_id.
      Includes all reference attributes needed to qualify issuers and counterparties.
      SCD2 lineage columns from Silver legal_entity are preserved.
    grain: one_row_per_entity
    grain_key: [legal_entity_id]
    source_tables:
      - statestreet.s_statestreet.legal_entity
    join_key: legal_entity_id
    filter: "le.is_current = TRUE"
    partition_by: []
    iceberg_uniform: true
    write_mode: create_or_replace
    columns:
      - name: legal_entity_id
        type: STRING
        nullable: false
        source_table: legal_entity
        description: "Unique legal entity identifier (PK)."

      - name: legal_name
        type: STRING
        nullable: false
        source_table: legal_entity
        description: "Full legal name of the entity."

      - name: country
        type: STRING
        nullable: true
        source_table: legal_entity
        description: "ISO 3166-1 alpha-2 country code of the entity's domicile."

      - name: entity_type
        type: STRING
        nullable: true
        source_table: legal_entity
        description: "Entity classification: BANK, CORPORATE, GOVERNMENT, etc."

      - name: effective_start_date
        type: DATE
        nullable: false
        source_table: legal_entity
        description: "SCD2 effective start date inherited from Silver legal_entity."

      - name: effective_end_date
        type: DATE
        nullable: false
        source_table: legal_entity
        description: "SCD2 effective end date. 9999-12-31 = current version."

      - name: _dq_rule_version
        type: STRING
        nullable: true
        source_table: legal_entity
        description: "DQ rule version SHA256 applied to this row in Silver."

      - name: _gold_build_ts
        type: TIMESTAMP
        nullable: false
        expression: "current_timestamp()"
        description: "Timestamp when this Gold row was last materialized."

  # ─────────────────────────────────────────────────────────────────────────────
  # fact_product_rating
  # Grain: one row per (product_id, effective_from_date, product_rating_type_id)
  # Represents point-in-time credit ratings. SCD2 from Silver product_rating.
  # ─────────────────────────────────────────────────────────────────────────────
  - name: fact_product_rating
    description: >
      Product rating history fact table. One row per product per rating event
      (product_id + effective_from_date + product_rating_type_id).
      Joins to dim_product and dim_legal_entity for conformed dimensional context.
      SCD2 columns from Silver product_rating are preserved for time-travel analysis.
    grain: one_row_per_product_per_rating_date
    grain_key: [product_id, effective_from_date, product_rating_type_id]
    source_tables:
      - statestreet.s_statestreet.product_rating
      - statestreet.s_statestreet.product_rating_type
      - statestreet.s_statestreet.product
    join_key: product_id
    filter: "pr.is_current = TRUE"
    partition_by: [effective_from_date]
    iceberg_uniform: true
    write_mode: create_or_replace
    columns:
      - name: product_rating_id
        type: STRING
        nullable: false
        source_table: product_rating
        description: "Surrogate PK for the rating event row."

      - name: product_id
        type: STRING
        nullable: false
        source_table: product_rating
        description: "FK to dim_product.product_id."

      - name: product_rating_type_id
        type: STRING
        nullable: true
        source_table: product_rating
        description: "FK to product_rating_type. Identifies the rating agency / methodology."

      - name: rating_agency
        type: STRING
        nullable: true
        source_table: product_rating
        description: "Rating agency name (e.g. Moodys, S&P, Fitch)."

      - name: rating_value
        type: STRING
        nullable: false
        source_table: product_rating
        description: "Credit rating code. Examples: AAA, AA+, AA, AA-, A+, A, A-, BBB+, BBB, BBB-, BB+, BB, BB-, B."

      - name: effective_from_date
        type: DATE
        nullable: false
        source_table: product_rating
        description: "Date this rating became effective."

      - name: watch_code
        type: STRING
        nullable: true
        source_table: product_rating
        description: "Rating watch code indicating direction of potential change."

      - name: rating_scale
        type: STRING
        nullable: true
        source_table: product_rating_type
        description: "Scale used for this rating type (e.g. LONG_TERM, SHORT_TERM)."

      - name: rating_type_code
        type: STRING
        nullable: true
        source_table: product_rating_type
        description: "Short code identifying the rating type."

      # ── Denormalized product context ─────────────────────────────────────────
      - name: product_type
        type: STRING
        nullable: true
        source_table: product
        source_column: type
        description: "Product type from Silver product (EQUITY, DEBT, FUND, DERIVATIVE, RIGHT)."

      - name: product_status
        type: STRING
        nullable: true
        source_table: product
        source_column: status
        description: "Product lifecycle status at time of Gold build."

      # ── SCD2 lineage ─────────────────────────────────────────────────────────
      - name: effective_start_date
        type: DATE
        nullable: false
        source_table: product_rating
        description: "SCD2 effective start date from Silver product_rating."

      - name: effective_end_date
        type: DATE
        nullable: false
        source_table: product_rating
        description: "SCD2 effective end date. 9999-12-31 = current."

      - name: _dq_rule_version
        type: STRING
        nullable: true
        source_table: product_rating
        description: "DQ rule version SHA256 applied to this row in Silver."

      - name: _gold_build_ts
        type: TIMESTAMP
        nullable: false
        expression: "current_timestamp()"
        description: "Timestamp when this Gold row was last materialized."

  # ─────────────────────────────────────────────────────────────────────────────
  # fact_coupon_schedule
  # Grain: one row per (product_id, coupon_id)
  # Each row represents a single coupon payment event for a bond.
  # Coupon is an append-only table — no SCD2 applied.
  # ─────────────────────────────────────────────────────────────────────────────
  - name: fact_coupon_schedule
    description: >
      Coupon payment schedule fact table. One row per bond per coupon payment event
      (product_id + coupon_id). Enriched with bond and product context for direct
      analytics without additional joins to Silver. Coupon is an event/append table
      with no SCD2 — the grain is driven by the coupon_id PK.
    grain: one_row_per_bond_per_coupon_payment_date
    grain_key: [product_id, coupon_id]
    source_tables:
      - statestreet.s_statestreet.coupon
      - statestreet.s_statestreet.bond
      - statestreet.s_statestreet.product
    join_key: product_id
    filter: "p.is_current = TRUE"
    partition_by: [payment_date]
    iceberg_uniform: true
    write_mode: create_or_replace
    columns:
      - name: coupon_id
        type: STRING
        nullable: false
        source_table: coupon
        description: "Surrogate PK for the coupon payment event."

      - name: product_id
        type: STRING
        nullable: false
        source_table: coupon
        description: "FK to dim_product.product_id (bond)."

      - name: coupon_rate
        type: DECIMAL(18, 8)
        nullable: false
        source_table: coupon
        description: "Annual coupon rate expressed as a percentage (e.g. 5.25 = 5.25%)."

      - name: payment_date
        type: DATE
        nullable: false
        source_table: coupon
        description: "Scheduled coupon payment date."

      - name: coupon_type
        type: STRING
        nullable: true
        source_table: coupon
        description: "Coupon type for this payment: FIXED or FLOATING."

      - name: frequency
        type: STRING
        nullable: true
        source_table: coupon
        description: "Payment frequency: ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY."

      # ── Bond context (denormalized) ─────────────────────────────────────────
      - name: bond_coupon_type
        type: STRING
        nullable: true
        source_table: bond
        source_column: coupon_type
        description: "Bond-level coupon type from Silver bond (may differ from coupon-row level)."

      - name: maturity_date
        type: DATE
        nullable: true
        source_table: bond
        description: "Bond maturity date. Enables schedule-to-maturity analytics."

      - name: face_currency_code
        type: STRING
        nullable: true
        source_table: bond
        source_column: issue_currency_code
        description: "Face value currency code (from bond.issue_currency_code)."

      - name: day_count_convention
        type: STRING
        nullable: true
        source_table: bond
        description: "Day count convention used for accrual calculations."

      # ── Product context (denormalized) ──────────────────────────────────────
      - name: product_status
        type: STRING
        nullable: true
        source_table: product
        source_column: status
        description: "Product lifecycle status at time of Gold build."

      - name: issuer_legal_entity_id
        type: STRING
        nullable: true
        source_table: product
        description: "FK to dim_legal_entity.legal_entity_id — bond issuer."

      - name: issue_date
        type: DATE
        nullable: true
        source_table: product
        description: "Bond issue date for schedule validation."

      # ── Pipeline metadata ───────────────────────────────────────────────────
      - name: _dq_rule_version
        type: STRING
        nullable: true
        source_table: coupon
        description: "DQ rule version SHA256 from Silver coupon row."

      - name: _gold_build_ts
        type: TIMESTAMP
        nullable: false
        expression: "current_timestamp()"
        description: "Timestamp when this Gold row was last materialized."

```

## rules.yaml
```yaml

```
