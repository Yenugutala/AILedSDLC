# Data Lineage & Governance — Unity Catalog, OpenLineage, Genie

Complete reference for data lineage tracking, governance policies, and Genie AI/BI setup.

---

## 1. Automatic Lineage in Unity Catalog

Unity Catalog captures table-level and column-level lineage automatically when notebooks run in Databricks. No code changes required.

### Viewing Lineage

```sql
-- What tables was securities_master built from? (upstream lineage)
SELECT
  source_table_full_name,
  target_table_full_name,
  entity_type
FROM system.access.table_lineage
WHERE target_table_full_name = 'statestreet.g_statestreet.securities_master'
ORDER BY event_time DESC;

-- What tables read from securities_master? (downstream consumers)
SELECT
  source_table_full_name,
  target_table_full_name
FROM system.access.table_lineage
WHERE source_table_full_name = 'statestreet.g_statestreet.securities_master';

-- Column-level lineage: where does securities_master.type come from?
SELECT
  source_table_full_name,
  source_column_name,
  target_table_full_name,
  target_column_name
FROM system.access.column_lineage
WHERE target_table_full_name = 'statestreet.g_statestreet.securities_master'
  AND target_column_name = 'type';
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

The Gold layer has a single wide/flat table: `statestreet.g_statestreet.securities_master`

```sql
-- securities_master: table-level comment
COMMENT ON TABLE statestreet.g_statestreet.securities_master IS
  'Single wide gold table for Securities Master Data. '
  'One row per active security product. '
  'Covers all product types: Equity (CommonStock, PreferredStock), '
  'Debt (Bond, Muni, PoolBackedSecurity), Fund, Listed Derivative (Option, Future), and Right. '
  'Source: 10+ Silver tables joined on product_id.';

-- Key column-level comments
COMMENT ON COLUMN statestreet.g_statestreet.securities_master.product_id IS
  'Unique identifier for the security product. Primary key. Format: alphanumeric string.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.type IS
  'Top-level security type. Values: EQUITY, DEBT, FUND, DERIVATIVE, RIGHT. '
  'This is the primary dimension for portfolio composition analysis.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.sub_type IS
  'Sub-category: COMMON_STOCK, PREFERRED_STOCK, BOND, MUNI, POOL_BACKED_SECURITY, OPTION, FUTURE.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.status IS
  'Lifecycle status. Values: ACTIVE (tradeable), INACTIVE, MATURED, SUSPENDED, DELISTED.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.primary_id_type IS
  'Type of the primary external identifier. Values: ISIN, CUSIP, SEDOL, BLOOMBERG_ID, TICKER.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.primary_identifier_value IS
  'The value of the primary external identifier (ISIN/CUSIP/SEDOL/etc.).';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issuer_legal_name IS
  'Legal name of the issuing entity.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.issue_date IS
  'Date when the security was first issued. DATE format (YYYY-MM-DD).';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_coupon_rate IS
  'Most recent annual coupon rate as a decimal (e.g. 0.05 = 5%). NULL for non-bonds.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_rating_value IS
  'Most recent credit rating code. Values: AAA, AA+, AA, A+, A, BBB, BB, B, CCC, D. NULL if unrated.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.latest_rating_agency IS
  'Rating agency. Values: SP (Standard & Poors), Moodys, Fitch.';

COMMENT ON COLUMN statestreet.g_statestreet.securities_master.bond_face_currency_code IS
  'ISO currency code of the bond face value (e.g. USD, EUR, GBP). NULL for non-bonds.';
```

---

## 4. Genie Space Configuration

Genie is Databricks AI/BI — users ask questions in natural language; Genie writes and executes SQL.

### Setup via Notebook (06_setup_genie.py)

```python
# Register the Gold table in a Genie Space
import requests

WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl", "")
TOKEN = dbutils.secrets.get(scope="sml-secrets", key="databricks-token")

genie_payload = {
    "title": "Securities Master Data",
    "description": (
        "Ask questions about securities in the portfolio. "
        "Covers all asset types: Equity, Bond, Muni, Fund, Derivative. "
        "Includes ISIN, CUSIP, coupon rate, maturity date, issuer, currency, credit ratings."
    ),
    "tables": [
        {"catalog_name": "statestreet", "schema_name": "g_statestreet", "table_name": "securities_master"},
    ],
}

response = requests.post(
    f"https://{WORKSPACE_URL}/api/2.0/genie/spaces",
    headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    json=genie_payload,
    timeout=30,
)
```

Or use the CLI shortcut: `sml genie --setup`

### Example Genie Queries

| Natural Language Question | SQL Genie Generates |
|--------------------------|---------------------|
| "How many securities by type?" | `SELECT type, COUNT(*) FROM securities_master GROUP BY type ORDER BY 2 DESC` |
| "How many active equity securities?" | `SELECT COUNT(*) FROM securities_master WHERE type='EQUITY' AND status='ACTIVE'` |
| "Show bonds with the highest coupon rate" | `SELECT product_id, description, latest_coupon_rate FROM securities_master WHERE type='DEBT' ORDER BY latest_coupon_rate DESC LIMIT 10` |
| "Securities rated AAA by S&P" | `SELECT product_id, description, type FROM securities_master WHERE latest_rating_value='AAA' AND latest_rating_agency='SP'` |
| "Which issuers have the most securities?" | `SELECT issuer_legal_name, COUNT(*) FROM securities_master GROUP BY issuer_legal_name ORDER BY 2 DESC` |

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
