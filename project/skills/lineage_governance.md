# Data Lineage & Governance — Unity Catalog, OpenLineage, Genie

Complete reference for data lineage tracking, governance policies, and Genie AI/BI setup.

---

## 1. Automatic Lineage in Unity Catalog

Unity Catalog captures table-level and column-level lineage automatically when notebooks run in Databricks. No code changes required.

### Viewing Lineage

```sql
-- What tables was dim_product built from? (upstream lineage)
SELECT
  source_table_full_name,
  target_table_full_name,
  entity_type
FROM system.access.table_lineage
WHERE target_table_full_name = 'statestreet.g_statestreet.dim_product'
ORDER BY event_time DESC;

-- What tables read from dim_product? (downstream consumers)
SELECT
  source_table_full_name,
  target_table_full_name
FROM system.access.table_lineage
WHERE source_table_full_name = 'statestreet.g_statestreet.dim_product';

-- Column-level lineage: where does dim_product.product_id come from?
SELECT
  source_table_full_name,
  source_column_name,
  target_table_full_name,
  target_column_name
FROM system.access.column_lineage
WHERE target_table_full_name = 'statestreet.g_statestreet.dim_product'
  AND target_column_name = 'product_id';
```

### Full Lineage Graph (All Layers)

```sql
-- Securities Master end-to-end lineage
WITH lineage AS (
  SELECT
    source_table_full_name AS source,
    target_table_full_name AS target,
    event_time
  FROM system.access.table_lineage
  WHERE source_table_full_name LIKE 'statestreet.%'
    OR target_table_full_name LIKE 'statestreet.%'
)
SELECT DISTINCT source, target
FROM lineage
ORDER BY source, target;
```

---

## 2. OpenLineage (Enterprise Data Catalog Integration)

For integration with external catalogs (Collibra, Alation, DataHub), emit OpenLineage events:

```python
# src/common/lineage_emitter.py
import json
import requests
from datetime import datetime, timezone

def emit_lineage_event(
    job_name: str,
    source_tables: list,
    target_table: str,
    workspace_url: str,
    run_id: str
):
    """
    Emit an OpenLineage RunEvent to an external catalog or Marquez server.

    OpenLineage is a standard for data lineage metadata:
    https://openlineage.io/spec/

    For Databricks → Collibra integration, configure the Collibra OpenLineage plugin.
    """
    event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {
            "runId": run_id,
            "facets": {
                "databricks_run": {
                    "_producer": "https://openlineage.io/spec",
                    "_schemaURL": "https://openlineage.io/spec",
                    "workspace": workspace_url,
                    "jobName": job_name,
                }
            }
        },
        "job": {
            "namespace": "statestreet.securities-master",
            "name": job_name,
        },
        "inputs": [
            {"namespace": "statestreet", "name": t}
            for t in source_tables
        ],
        "outputs": [
            {"namespace": "statestreet", "name": target_table}
        ]
    }

    # Send to OpenLineage server (Marquez, or Collibra OpenLineage endpoint)
    openlineage_url = "http://marquez:5000/api/v1/lineage"
    requests.post(openlineage_url, json=event, timeout=5)
```

---

## 3. Table and Column Comments (for Genie AI/BI)

Genie uses table and column comments as semantic context for natural language queries.
**Always add comments to Gold tables** — this is what makes Genie accurate.

### Gold Table Comments (Added by Doc Agent)

```sql
-- dim_product: add table-level comment
COMMENT ON TABLE statestreet.g_statestreet.dim_product IS
  'Flattened security product dimension. One row per active security. '
  'Covers all product types: Equity (CommonStock, PreferredStock), '
  'Debt (Bond, Muni, PoolBackedSecurity), Fund, Listed Derivative (Option, Future), and Right. '
  'Join to fact tables on product_id. '
  'Source: 10 Silver tables joined on product_id.';

-- Column-level comments
COMMENT ON COLUMN statestreet.g_statestreet.dim_product.product_id IS
  'Unique identifier for the security product. Primary key. Format: alphanumeric string.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.type IS
  'Top-level product type. Values: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.status IS
  'Lifecycle status. Values: ACTIVE (tradeable), INACTIVE, MATURED, SUSPENDED, DELISTED.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.issue_date IS
  'Date when the security was first issued. DATE format (YYYY-MM-DD).';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.coupon_type IS
  'Bond coupon type. Values: FIXED, FLOATING, ZERO_COUPON, STEP_UP. NULL for non-bonds.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.maturity_date IS
  'Date when the bond matures. NULL for equities, funds, and perpetual bonds.';

COMMENT ON COLUMN statestreet.g_statestreet.dim_product.voting_rights IS
  'Whether the stock has voting rights. Values: YES, NO. NULL for non-common-stock.';

-- fact_coupon_schedule comments
COMMENT ON TABLE statestreet.g_statestreet.fact_coupon_schedule IS
  'Coupon payment schedule for bond securities. '
  'Grain: one row per bond per coupon payment date. '
  'Join to dim_product on product_id for bond attributes.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.coupon_rate IS
  'Annual coupon rate as a decimal (e.g. 0.05 = 5%). '
  'For FLOATING rate bonds, this is the rate as of the last reset date.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_coupon_schedule.payment_date IS
  'Date of the coupon payment. DATE format (YYYY-MM-DD).';

-- fact_product_rating comments
COMMENT ON TABLE statestreet.g_statestreet.fact_product_rating IS
  'Credit rating history for securities. '
  'Grain: one row per product per rating agency per rating date. '
  'Join to dim_product on product_id.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_code IS
  'Credit rating code from rating agency. '
  'Example values: AAA, AA+, AA, AA-, A+, A, A-, BBB+, BBB, BBB-, BB, B, CCC, D.';

COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_agency IS
  'Rating agency name. Values: SP (Standard & Poors), Moodys, Fitch.';

-- dim_legal_entity comments
COMMENT ON TABLE statestreet.g_statestreet.dim_legal_entity IS
  'Legal entity dimension. One row per active legal entity (issuer, counterparty, custodian). '
  'Join to dim_product on issuer_legal_entity_id.';
```

---

## 4. Genie Space Configuration

Genie is Databricks AI/BI — users ask questions in natural language; Genie writes and executes SQL.

### Setup via Notebook (06_setup_genie.py)

```python
# Register Gold tables in a Genie Space
import requests

WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl", "")
TOKEN = dbutils.secrets.get(scope="sml-secrets", key="databricks-token")

genie_payload = {
    "title": "Securities Master Data",
    "description": (
        "Ask questions about securities: products, ratings, coupons, legal entities. "
        "Covers all product types: Equity, Debt (Bonds, Munis), Fund, Derivative, Right."
    ),
    "tables": [
        {"catalog_name": "statestreet", "schema_name": "g_statestreet", "table_name": "dim_product"},
        {"catalog_name": "statestreet", "schema_name": "g_statestreet", "table_name": "dim_legal_entity"},
        {"catalog_name": "statestreet", "schema_name": "g_statestreet", "table_name": "fact_product_rating"},
        {"catalog_name": "statestreet", "schema_name": "g_statestreet", "table_name": "fact_coupon_schedule"},
    ],
}

response = requests.post(
    f"https://{WORKSPACE_URL}/api/2.0/genie/spaces",
    headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    json=genie_payload,
    timeout=30,
)
```

### Example Genie Queries

| Natural Language Question | SQL Genie Generates |
|--------------------------|---------------------|
| "How many active equity products?" | `SELECT COUNT(*) FROM dim_product WHERE type='EQUITY' AND status='ACTIVE'` |
| "Show bonds maturing in 2025" | `SELECT * FROM dim_product WHERE type='DEBT' AND YEAR(maturity_date)=2025` |
| "Top 10 products by coupon rate" | `SELECT p.*, f.coupon_rate FROM dim_product p JOIN fact_coupon_schedule f ON p.product_id=f.product_id ORDER BY f.coupon_rate DESC LIMIT 10` |
| "Products rated AAA by S&P" | `SELECT p.* FROM dim_product p JOIN fact_product_rating r ON p.product_id=r.product_id WHERE r.rating_code='AAA' AND r.rating_agency='SP'` |
| "Legal entities with most products" | `SELECT issuer_legal_entity_id, COUNT(*) FROM dim_product GROUP BY 1 ORDER BY 2 DESC` |

### Access Path

```
Databricks workspace
  → SQL → Genie → "Securities Master Data"
  → Ask any question in natural language
```

---

## 5. Data Catalog Best Practices

### What to Document (minimum for compliance)

| Item | Where | Who Updates |
|------|-------|-------------|
| Table purpose + grain | COMMENT ON TABLE | Doc Agent (auto) |
| Column description + valid values | COMMENT ON COLUMN | Doc Agent (auto) |
| Data owner + steward | Unity Catalog tags | Data steward (manual) |
| Data classification (PII, licensed) | Unity Catalog tags | Data steward (manual) |
| Lineage source | Unity Catalog lineage (auto) | N/A — automatic |
| Known data issues | known_issues.md | Debug Agent (auto) + humans |
| DQ rule history | `_dq_rule_version` col | Pipeline (auto) |

### Unity Catalog Tags (Data Classification)

```python
# Add classification tags to tables
spark.sql("""
    ALTER TABLE statestreet.g_statestreet.dim_product
    SET TAGS ('domain' = 'securities', 'classification' = 'internal', 'pii' = 'false')
""")

spark.sql("""
    ALTER TABLE statestreet.g_statestreet.dim_legal_entity
    SET TAGS ('domain' = 'securities', 'classification' = 'confidential', 'pii' = 'true')
""")

# Tag columns with licensed data (CUSIP, ISIN, SEDOL)
spark.sql("""
    ALTER TABLE statestreet.b_statestreet.identifiers
    ALTER COLUMN identifier_value
    SET TAGS ('license' = 'cusip-global-services', 'distribution' = 'restricted')
""")
```

### Viewing Tags

```sql
-- All tables with their classification tags
SELECT table_name, tag_name, tag_value
FROM system.information_schema.table_tags
WHERE catalog_name = 'statestreet'
ORDER BY table_name, tag_name;
```

---

## 6. Data Retention Policy

| Layer | Retention | Mechanism |
|-------|-----------|-----------|
| Bronze | Indefinite (append-only history) | No VACUUM; time travel always available |
| Silver | 2 years active + 5 years archived | VACUUM with 7-day retention; archive old versions to cold storage |
| Gold | 2 years (refreshed each run) | VACUUM after rebuild |
| Rejects | 1 year (regulatory minimum) | Scheduled VACUUM |
| Audit log | 7 years (SOX requirement) | append-only; never VACUUM |

```sql
-- Set retention on Silver (7 days time travel, then garbage collect)
ALTER TABLE statestreet.s_statestreet.product
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days');

VACUUM statestreet.s_statestreet.product RETAIN 168 HOURS;   -- 7 days
```
