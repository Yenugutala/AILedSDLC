# Business Requirement — securities-master

## Description
Ingest 29 security CSV files (product, bond, stock, fund, identifiers, etc.)
from Databricks Volume into a Bronze/Silver/Gold medallion lakehouse.

Data model: Class-table inheritance. Every security has one row in 'product'
plus one row in each applicable subtype table (bond, stock, fund, etc.).
The join key across ALL tables is product_id.

Bronze = raw landing, no transformation, schema drift handled.
Silver = DQ-conformed (128 rules from dq_rules_catalog.csv), rejects quarantined,
         SCD2 applied to bitemporal entities (product, legal_entity, product_rating).
Gold = 4 dimensional marts:
  - dim_product (one row per product, all subtype attributes flattened)
  - dim_legal_entity (one row per legal entity)
  - fact_product_rating (one row per product per rating date)
  - fact_coupon_schedule (one row per bond per coupon payment date)


## Source Tables
_See request.yaml_

## Full Request
```yaml
catalog:
  bronze_schema: b_statestreet
  gold_schema: g_statestreet
  name: statestreet
  silver_schema: s_statestreet
code_generation:
  bronze: python
  dq_framework: sql
  gold: sql
  silver: sql
description: "Ingest 29 security CSV files (product, bond, stock, fund, identifiers,\
  \ etc.)\nfrom Databricks Volume into a Bronze/Silver/Gold medallion lakehouse.\n\
  \nData model: Class-table inheritance. Every security has one row in 'product'\n\
  plus one row in each applicable subtype table (bond, stock, fund, etc.).\nThe join\
  \ key across ALL tables is product_id.\n\nBronze = raw landing, no transformation,\
  \ schema drift handled.\nSilver = DQ-conformed (128 rules from dq_rules_catalog.csv),\
  \ rejects quarantined,\n         SCD2 applied to bitemporal entities (product, legal_entity,\
  \ product_rating).\nGold = 4 dimensional marts:\n  - dim_product (one row per product,\
  \ all subtype attributes flattened)\n  - dim_legal_entity (one row per legal entity)\n\
  \  - fact_product_rating (one row per product per rating date)\n  - fact_coupon_schedule\
  \ (one row per bond per coupon payment date)\n"
dq_reference:
  issues_catalog: dq_issues_catalog.csv
  rules_catalog: dq_rules_catalog.csv
  validation_queries: dq_validation_queries.sql
gold_marts:
- description: Flattened product dimension with all subtype attributes
  grain: one_row_per_product
  name: dim_product
- description: Legal entity dimension
  grain: one_row_per_entity
  name: dim_legal_entity
- description: Product rating history fact table
  grain: one_row_per_product_per_rating_date
  name: fact_product_rating
- description: Coupon payment schedule fact table
  grain: one_row_per_bond_per_coupon_payment_date
  name: fact_coupon_schedule
scd2_tables:
- product
- legal_entity
- product_rating
source:
  delimiter: ','
  format: csv
  header: true
  path: /Volumes/statestreet/securities_master/raw_files/
  tables:
  - product
  - generic_product
  - legal_entity
  - tick_ladder_scale
  - tick
  - product_rating
  - product_rating_type
  - classification
  - identifiers
  - fund
  - debt
  - bond
  - muni
  - pool_backed_security
  - right
  - series
  - listed_derivative
  - option
  - future
  - stock
  - common_stock
  - preferred_stock
  - coupon
  - principal_redemption_provision
  - currency
  - listed_derivative_tick
  - debt_principal_redemption_provision
  - dq_rules_catalog
  - dq_issues_catalog
  type: volume
stakeholders:
- name: State Street Data Office
  role: data_owner
- name: Senior Data Architect
  role: architect_approver
use_case_name: securities-master
volume:
  path: /Volumes/statestreet/securities_master/raw_files/

```
