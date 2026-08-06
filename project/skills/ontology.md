# Securities Master Data Ontology

## Class Hierarchy (UML → Table Map)

```
Product (base)
├── Fund
├── Right
├── Stock
│   ├── CommonStock
│   └── PreferredStock
├── Debt
│   ├── Bond
│   │   └── Muni
│   └── PoolBackedSecurity
└── ListedDerivative
    ├── Option
    └── Future
```

## Table Mapping

| UML Class | Table Name | Notes |
|-----------|-----------|-------|
| Product | `product` | Base table, PK: `product_id`. Every security has exactly one row here |
| Fund | `fund` | Adds: `endness_type`, `mutual_fund_type` |
| Right | `right` | Subscription rights |
| Stock | `stock` | Adds: `series_id` (FK to `series`) |
| CommonStock | `common_stock` | Adds: `voting_rights` |
| PreferredStock | `preferred_stock` | Adds: `dividend_type` |
| Debt | `debt` | Adds: `face_amount`, `issue_date_settlement` |
| Bond | `bond` | Adds: `coupon_type`, `maturity_date`, `face_currency_code` |
| Muni | `muni` | Adds: `tax_exempt`, `state`, `purpose` |
| PoolBackedSecurity | `pool_backed_security` | Adds: `pool_type`, `originator` |
| ListedDerivative | `listed_derivative` | Adds: `series_id`, `underlying_product_id` |
| Option | `option` | Adds: `option_type` (CALL/PUT), `exercise_style` (AMERICAN/EUROPEAN) |
| Future | `future` | Adds: `delivery_date`, `valuation_method` |

## Supporting Dimension Tables

| Table | Description |
|-------|-------------|
| `legal_entity` | Issuers and counterparties. Bitemporal in model. |
| `currency` | ISO 4217 currency codes (15 real + 2 bad rows seeded) |
| `series` | Optional grouping for Stock and ListedDerivative |
| `tick_ladder_scale` | Minimum price increment scale |
| `tick` | Individual tick entries in a scale |

## Relationship Tables

| Table | Relationship | Linked Tables |
|-------|-------------|--------------|
| `identifiers` | 0..* per product | `product` → `identifiers` on `product_id` |
| `classification` | 0..* per product | `product` → `classification` on `product_id` |
| `product_rating` | 0..* per product | `product` → `product_rating` on `product_id` |
| `coupon` | 0..* per bond | `bond` → `coupon` on `product_id` |
| `generic_product` | Legacy shadow | deprecated, 1 product → many rows |
| `listed_derivative_tick` | Bridge M:M | `listed_derivative` ↔ `tick` |
| `debt_principal_redemption_provision` | Bridge M:M | `debt` ↔ `principal_redemption_provision` |

## Population Counts (Sample Dataset)

| Category | Count | Breakdown |
|----------|-------|-----------|
| Total products | 200 | — |
| Stock | 60 | 40 CommonStock + 20 PreferredStock |
| Debt | 70 | 50 Bond (15 Muni) + 20 PoolBackedSecurity |
| Fund | 20 | — |
| Right | 10 | — |
| ListedDerivative | 40 | 25 Option + 15 Future |
| Legal entities | 40 | — |
| Currencies | 15 real + 2 bad | 17 total rows |
| Series | 20 | — |
| Tick-ladder scales | 10 | — |
| Ticks | 25 | — |
| Principal redemption provisions | 30 | — |
| Rating types | 8 | — |
| Identifiers | ~211 | — |
| Classifications | ~213 | — |
| Product ratings | ~205 | — |
| Coupons | ~105 | — |

## Key Discriminator Columns

| Column | Table | Values |
|--------|-------|--------|
| `type` | `product` | EQUITY, DEBT, FUND, DERIVATIVE, RIGHT |
| `sub_type` | `product` | COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED, OPTION, FUTURE, FUND, RIGHT |
| `id_type` | `identifiers` | CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID |
| `option_type` | `option` | CALL, PUT |
| `exercise_style` | `option` | AMERICAN, EUROPEAN |
| `coupon_type` | `bond` | FIXED, FLOATING, ZERO |
| `status` | `product` | ACTIVE, INACTIVE, MATURED, SUSPENDED, DELISTED |
| `endness_type` | `fund` | OPEN_END, CLOSED_END |
