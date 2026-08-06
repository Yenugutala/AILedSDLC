# Gold Layer Spec — securities-master

## tables.yaml
```yaml
# Gold Layer — Dimensional marts for Genie / Analytics
# Schema: statestreet.g_statestreet
# 4 marts: dim_product, dim_legal_entity, fact_product_rating, fact_coupon_schedule

layer: gold
catalog: statestreet
schema: g_statestreet

marts:

  - name: dim_product
    grain: one_row_per_product
    description: >
      Flattened product dimension. One row per active security.
      Covers all product types: Equity (CommonStock, PreferredStock), Debt (Bond, Muni,
      PoolBackedSecurity), Fund, Listed Derivative (Option, Future), and Right.
      Subtype-specific columns are NULL for non-matching product types.
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
    join_key: product_id
    join_type: LEFT JOIN               # All subtypes are optional
    filter: "p.is_current = TRUE"
    partition_by: [type]
    iceberg_uniform: true
    expected_row_count: 200            # Matches total product count in sample data

  - name: dim_legal_entity
    grain: one_row_per_entity
    description: >
      Legal entity dimension. One row per active legal entity.
      Legal entities are issuers referenced by products via issuer_legal_entity_id.
    source_tables:
      - statestreet.s_statestreet.legal_entity
    join_key: legal_entity_id
    filter: "is_current = TRUE"
    partition_by: [_ingestion_date]
    iceberg_uniform: true
    expected_row_count: 40             # 40 legal entities in sample data

  - name: fact_product_rating
    grain: one_row_per_product_per_rating_date
    description: >
      Product rating history. One row per product per rating date.
      Joins to dim_product on product_id and to product_rating_type for rating metadata.
    source_tables:
      - statestreet.s_statestreet.product_rating
      - statestreet.s_statestreet.product_rating_type
      - statestreet.s_statestreet.product
    join_key: product_id
    join_type: LEFT JOIN
    filter: "pr.is_current = TRUE"
    partition_by: [effective_from_date]
    iceberg_uniform: true
    expected_row_count: 205            # ~205 rating rows in sample data
    fk_to_dim: [dim_product]          # Joins to dim_product on product_id

  - name: fact_coupon_schedule
    grain: one_row_per_bond_per_coupon_payment_date
    description: >
      Coupon payment schedule. One row per bond per coupon payment date.
      Only includes bonds (product.type = 'DEBT' and bond record exists).
      Joins to dim_product for bond context.
    source_tables:
      - statestreet.s_statestreet.coupon
      - statestreet.s_statestreet.bond
      - statestreet.s_statestreet.product
    join_key: product_id
    join_type: INNER JOIN              # Only bonds have coupons — INNER to exclude non-bonds
    coupon_join: "c.bond_id = b.product_id"
    filter: "p.is_current = TRUE AND p.type = 'DEBT'"
    partition_by: [payment_date]
    iceberg_uniform: true
    expected_row_count: 105            # ~105 coupon rows in sample data
    fk_to_dim: [dim_product]          # Joins to dim_product on product_id

```

## rules.yaml
```yaml
# Gold Layer Rules — Transformation rules, filters, and mart-specific config

layer: gold
catalog: statestreet
schema: g_statestreet

marts:

  - name: dim_product
    filter: "p.is_current = TRUE"
    dedup_strategy: latest_effective_date   # SCD2: use most recent is_current row
    null_strategy: coalesce_nulls           # Subtype cols NULL for non-matching types (expected)
    iceberg_uniform: true

  - name: dim_legal_entity
    filter: "is_current = TRUE"
    dedup_strategy: latest_effective_date
    iceberg_uniform: true

  - name: fact_product_rating
    filter: "pr.is_current = TRUE"
    aggregate: false                        # Grain is already at rating level
    fk_validation:
      - column: product_id
        references: g_statestreet.dim_product.product_id
    iceberg_uniform: true

  - name: fact_coupon_schedule
    filter: "p.is_current = TRUE AND p.type = 'DEBT'"
    aggregate: false
    fk_validation:
      - column: product_id
        references: g_statestreet.dim_product.product_id
    iceberg_uniform: true

genie_config:
  space_title: Securities Master Data
  space_description: >
    Ask questions about securities: products, ratings, coupons, legal entities.
    Covers all product types: Equity, Debt (Bonds, Munis), Fund, Derivative, Right.
  tables:
    - statestreet.g_statestreet.dim_product
    - statestreet.g_statestreet.dim_legal_entity
    - statestreet.g_statestreet.fact_product_rating
    - statestreet.g_statestreet.fact_coupon_schedule
  sample_questions:
    - How many active equity products do we have?
    - Show all bonds with maturity date in 2025
    - Which legal entities have the most products?
    - What is the average coupon rate by product type?
    - Show products with the highest rating from S&P

quality_checks:
  - name: dim_product_row_count
    sql: "SELECT COUNT(*) FROM statestreet.g_statestreet.dim_product"
    expected: 200
    tolerance: 0

  - name: fact_coupon_grain
    sql: >
      SELECT product_id, payment_date, COUNT(*) AS cnt
      FROM statestreet.g_statestreet.fact_coupon_schedule
      GROUP BY product_id, payment_date
      HAVING cnt > 1
    expected_rows: 0
    description: No duplicate (product_id, payment_date) in coupon schedule

  - name: no_null_product_id_gold
    sql: >
      SELECT 'dim_product' AS tbl, COUNT(*) AS null_count FROM statestreet.g_statestreet.dim_product WHERE product_id IS NULL
      UNION ALL
      SELECT 'fact_product_rating', COUNT(*) FROM statestreet.g_statestreet.fact_product_rating WHERE product_id IS NULL
      UNION ALL
      SELECT 'fact_coupon_schedule', COUNT(*) FROM statestreet.g_statestreet.fact_coupon_schedule WHERE product_id IS NULL
    expected_rows: 3
    all_counts_zero: true

```
