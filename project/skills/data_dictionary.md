# Data Dictionary — Securities Master Data

## product (Base Table — All Securities)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `product_id` | STRING | YES | Unique security identifier (PK) |
| `id_type` | STRING | YES | Primary identifier type: CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID |
| `type` | STRING | YES | Product category: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT |
| `sub_type` | STRING | NO | Subcategory: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE |
| `status` | STRING | YES | Lifecycle: ACTIVE, INACTIVE, MATURED, SUSPENDED, DELISTED |
| `settlement_type` | STRING | NO | Settlement method |
| `description` | STRING | NO | Human-readable security name |
| `issue_date` | DATE | NO | Date the security was issued |
| `issue_price` | DECIMAL | NO | Price at issuance |
| `current_face_value` | DECIMAL | NO | Current face/par value |
| `issuer_legal_entity_id` | STRING | NO | FK → legal_entity.legal_entity_id |
| `tick_ladder_scale_id` | STRING | NO | FK → tick_ladder_scale.tick_ladder_scale_id |

## bond (Extends debt, extends product)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `product_id` | STRING | YES | FK → product.product_id |
| `coupon_type` | STRING | YES | FIXED, FLOATING, ZERO |
| `maturity_date` | DATE | YES | When the bond matures |
| `face_currency_code` | STRING | YES | ISO 4217 currency code for face value |
| `day_count_convention` | STRING | NO | e.g. ACT/360, 30/360 |

## stock (Extends product)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `product_id` | STRING | YES | FK → product.product_id |
| `series_id` | STRING | NO | FK → series.series_id |

## common_stock (Extends stock)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `product_id` | STRING | YES | FK → product.product_id |
| `voting_rights` | BOOLEAN | NO | Has voting rights |

## preferred_stock (Extends stock)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `product_id` | STRING | YES | FK → product.product_id |
| `dividend_type` | STRING | NO | CUMULATIVE, NON_CUMULATIVE |

## listed_derivative (Extends product)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `product_id` | STRING | YES | FK → product.product_id |
| `series_id` | STRING | NO | FK → series.series_id |
| `underlying_product_id` | STRING | NO | FK → product.product_id (the underlying) |

## option (Extends listed_derivative)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `product_id` | STRING | YES | FK → product.product_id |
| `option_type` | STRING | YES | CALL or PUT |
| `exercise_style` | STRING | YES | AMERICAN or EUROPEAN |
| `strike_price` | DECIMAL | NO | Strike/exercise price |
| `expiry_date` | DATE | NO | Expiration date |

## future (Extends listed_derivative)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `product_id` | STRING | YES | FK → product.product_id |
| `delivery_date` | DATE | NO | Futures delivery date |
| `valuation_method` | STRING | NO | MARK_TO_MARKET, etc. |

## legal_entity

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `legal_entity_id` | STRING | YES | Unique entity identifier (PK) |
| `name` | STRING | YES | Legal entity name |
| `country` | STRING | NO | ISO 3166-1 alpha-2 country code |
| `entity_type` | STRING | NO | BANK, CORPORATE, GOVERNMENT, etc. |

## identifiers (Aliases for a product)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `identifier_id` | STRING | YES | PK |
| `product_id` | STRING | YES | FK → product.product_id |
| `id_type` | STRING | YES | CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID |
| `identifier_value` | STRING | YES | The actual identifier string |

## product_rating

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `rating_id` | STRING | YES | PK |
| `product_id` | STRING | YES | FK → product.product_id |
| `rating_type_id` | STRING | NO | FK → product_rating_type |
| `rating_value` | STRING | YES | e.g. AAA, BBB-, BB+ |
| `rating_date` | DATE | YES | Date rating was assigned |

## coupon

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `coupon_id` | STRING | YES | PK |
| `product_id` | STRING | YES | FK → bond.product_id |
| `coupon_rate` | DECIMAL | YES | Annual coupon rate (%) |
| `payment_date` | DATE | YES | Coupon payment date |
| `coupon_type` | STRING | NO | FIXED, FLOATING |
| `frequency` | STRING | NO | ANNUAL, SEMI_ANNUAL, QUARTERLY, MONTHLY |

## Metadata Columns (Added by Bronze Pipeline)

| Column | Type | Description |
|--------|------|-------------|
| `_source_file` | STRING | Source CSV filename |
| `_ingestion_ts` | TIMESTAMP | When row was loaded |
| `_batch_id` | STRING | Pipeline run ID |
| `_row_hash` | STRING | SHA256 of all data columns (for CDC) |

## SCD2 Columns (Added by Silver Pipeline)

| Column | Type | Description |
|--------|------|-------------|
| `effective_start_date` | DATE | When this version became active |
| `effective_end_date` | DATE | When this version was superseded (9999-12-31 = current) |
| `is_current` | BOOLEAN | TRUE for the active version |
| `_dq_rule_version` | STRING | SHA256 of silver/rules.yaml at DQ check time |
