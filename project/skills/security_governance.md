# Security & Governance — Databricks Unity Catalog

Complete reference for secrets management, access control, data masking, compliance, and audit.

---

## 1. Secrets Management (CRITICAL — Never Skip)

**Rule: Never store credentials in code, notebooks, YAML files, or Git.**

### Layer 1: Local Development — Environment Variables

```bash
# ~/.zshrc or ~/.bashrc (never commit these files)
export DATABRICKS_HOST="https://<workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
export ANTHROPIC_API_KEY="sk-ant-..."
```

```python
import os
host  = os.environ["DATABRICKS_HOST"]
token = os.environ["DATABRICKS_TOKEN"]
```

### Layer 2: Databricks Secret Scopes (Production)

```bash
# Create a secret scope (once per workspace)
databricks secrets create-scope --scope sml-secrets

# Store secrets (interactive — Databricks prompts for value)
databricks secrets put --scope sml-secrets --key databricks-token
databricks secrets put --scope sml-secrets --key anthropic-api-key
databricks secrets put --scope sml-secrets --key sql-server-password
databricks secrets put --scope sml-secrets --key api-bearer-token

# List secrets (shows keys only — never values)
databricks secrets list --scope sml-secrets
```

```python
# In Databricks notebooks — read secrets at runtime
password = dbutils.secrets.get(scope="sml-secrets", key="sql-server-password")
api_token = dbutils.secrets.get(scope="sml-secrets", key="api-bearer-token")
# dbutils.secrets.get() returns the value but it is REDACTED in notebook output
```

### Layer 3: Azure Key Vault (Enterprise Grade)

```bash
# Link Databricks scope to Azure Key Vault
databricks secrets create-scope \
  --scope sml-keyvault \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<vault> \
  --dns-name https://<vault>.vault.azure.net/
```

### Files to NEVER Commit (add to .gitignore)

```gitignore
.env
.env.local
.env.production
*.token
*.key
*.pem
*.pfx
.databrickscfg
.claude/settings.json        # may contain MCP tokens
agent_state.yaml             # may contain feedback with sensitive data
generated/                   # generated notebooks before review
```

---

## 2. Unity Catalog Access Control (RBAC)

Unity Catalog enforces access at catalog, schema, table, and column level.

### Recommended Role Structure

| Role | Access Level | Groups |
|------|-------------|--------|
| Pipeline Service Account | Full write on all layers | `svc-pipeline@statestreet.com` |
| Data Engineers | Full write Bronze + Silver; read Gold | `data-engineers` group |
| Data Analysts | Read Gold only | `data-analysts` group |
| Data Stewards | Read all layers + manage DQ | `data-stewards` group |
| Data Owners | Approve Silver promotion | `data-owners` group |
| External Consumers | Read Gold via Genie | `genie-users` group |

### Granting Permissions

```sql
-- Pipeline service principal: write all layers
GRANT CREATE, MODIFY, SELECT ON SCHEMA statestreet.b_statestreet TO `svc-pipeline@statestreet.com`;
GRANT CREATE, MODIFY, SELECT ON SCHEMA statestreet.s_statestreet TO `svc-pipeline@statestreet.com`;
GRANT CREATE, MODIFY, SELECT ON SCHEMA statestreet.g_statestreet TO `svc-pipeline@statestreet.com`;
GRANT READ VOLUME ON VOLUME statestreet.securities_master.raw_files TO `svc-pipeline@statestreet.com`;

-- Data engineers: write Bronze + Silver; read Gold
GRANT CREATE, MODIFY, SELECT ON SCHEMA statestreet.b_statestreet TO `data-engineers`;
GRANT CREATE, MODIFY, SELECT ON SCHEMA statestreet.s_statestreet TO `data-engineers`;
GRANT SELECT ON SCHEMA statestreet.g_statestreet TO `data-engineers`;

-- Analysts: read Gold only
GRANT SELECT ON SCHEMA statestreet.g_statestreet TO `data-analysts`;

-- Data stewards: read all layers (for DQ investigation)
GRANT SELECT ON SCHEMA statestreet.b_statestreet TO `data-stewards`;
GRANT SELECT ON SCHEMA statestreet.s_statestreet TO `data-stewards`;
GRANT SELECT ON SCHEMA statestreet.g_statestreet TO `data-stewards`;

-- Revoke accidental broad access
REVOKE ALL PRIVILEGES ON SCHEMA statestreet.b_statestreet FROM `account users`;
```

### Row-Level Security (RLS)

```sql
-- Only show securities where issuer region matches user's allowed region
CREATE OR REPLACE FUNCTION statestreet.g_statestreet.rls_product_region(region STRING)
RETURNS BOOLEAN
RETURN IS_MEMBER('global-access')
    OR region = current_user_region();     -- custom function returning user's region

ALTER TABLE statestreet.g_statestreet.dim_product
SET ROW FILTER statestreet.g_statestreet.rls_product_region ON (issuer_region);
```

---

## 3. Column-Level Security (Data Masking)

For PII or sensitive financial fields, apply masking functions.

```sql
-- Mask legal entity name for non-privileged users
CREATE OR REPLACE FUNCTION statestreet.g_statestreet.mask_entity_name(name STRING)
RETURNS STRING
RETURN CASE
  WHEN IS_MEMBER('data-stewards') OR IS_MEMBER('data-engineers') THEN name
  ELSE CONCAT(LEFT(name, 2), '***')
END;

ALTER TABLE statestreet.g_statestreet.dim_legal_entity
ALTER COLUMN legal_entity_name
SET MASK statestreet.g_statestreet.mask_entity_name;

-- Mask coupon rate for external users
CREATE OR REPLACE FUNCTION statestreet.g_statestreet.mask_rate(rate DOUBLE)
RETURNS DOUBLE
RETURN CASE
  WHEN IS_MEMBER('data-analysts') OR IS_MEMBER('data-stewards') THEN rate
  ELSE NULL
END;

ALTER TABLE statestreet.g_statestreet.fact_coupon_schedule
ALTER COLUMN coupon_rate
SET MASK statestreet.g_statestreet.mask_rate;
```

---

## 4. Audit Log Table

Every pipeline run, schema change, and DQ action must be recorded.

```sql
-- Create audit log (append-only — never update or delete)
CREATE TABLE IF NOT EXISTS statestreet.g_statestreet.audit_log (
  run_id           STRING     NOT NULL,
  use_case_name    STRING     NOT NULL,
  job_name         STRING,
  stage            STRING,                -- bronze | silver | gold | dq_rescan
  table_name       STRING,
  started_at       TIMESTAMP  NOT NULL,
  completed_at     TIMESTAMP,
  rows_read        LONG,
  rows_written     LONG,
  rows_rejected    LONG,
  batch_id         STRING,
  status           STRING,                -- SUCCESS | FAILED | PARTIAL | QUARANTINED
  error_message    STRING,
  triggered_by     STRING,               -- user email or service principal
  dq_rule_version  STRING,
  source_type      STRING,               -- volume | jdbc | api | kafka | delta | s3
  notes            STRING
)
USING DELTA
TBLPROPERTIES ('delta.appendOnly' = 'true');   -- no updates/deletes — immutable audit trail
```

```python
# Write audit entry from Bronze/Silver/Gold notebooks
from datetime import datetime, timezone

spark.sql(f"""
    INSERT INTO statestreet.g_statestreet.audit_log
    VALUES (
      '{run_id}', 'securities-master', '{job_name}', 'bronze', '{table_name}',
      '{started_at}', current_timestamp(),
      {rows_read}, {rows_written}, {rows_rejected},
      '{batch_id}', 'SUCCESS', NULL,
      '{triggered_by}', NULL, 'volume', NULL
    )
""")
```

---

## 5. Data Lineage (Unity Catalog)

Unity Catalog automatically captures lineage when tables are read/written.

```sql
-- View table lineage (what tables was this table built from?)
SELECT * FROM system.access.table_lineage
WHERE target_table_full_name = 'statestreet.g_statestreet.dim_product';

-- Column lineage
SELECT * FROM system.access.column_lineage
WHERE target_table_full_name = 'statestreet.g_statestreet.dim_product'
  AND target_column_name = 'product_id';

-- Who read this table (access audit)
SELECT user_identity.email, action_name, request_params, event_time
FROM system.access.audit
WHERE request_params.table_full_name = 'statestreet.g_statestreet.dim_product'
ORDER BY event_time DESC
LIMIT 50;
```

---

## 6. Git Branch Protection

Configure in GitHub repository settings:

```yaml
# Recommended branch protection rules for 'main'
- Require pull request before merging
  - Required approvals: 1
  - Dismiss stale pull request approvals when new commits are pushed: true
- Require status checks to pass before merging:
  - validate-specs (GitHub Action)
  - deploy-bundle-validation (GitHub Action)
- Restrict pushes that create files:
  - Do not allow bypass (even admins)
- Require signed commits (SOX/MiFID II compliance)
- Do not allow force pushes
- Do not allow deletions
```

---

## 7. Sensitive Data Handling

### Data Classification

| Column | Classification | Masking | Access |
|--------|---------------|---------|--------|
| `product_id` | Internal | None | All roles |
| `legal_entity_name` | Confidential | Masked for analysts | Data stewards + |
| `coupon_rate` | Restricted | Masked for external | Analysts + |
| `issuer_country` | Internal | None | All roles |
| CUSIP/ISIN values | Licensed data | None | All roles (licensed) |

### Handling Licensed Data (CUSIP, ISIN, SEDOL)

CUSIP, ISIN, and SEDOL are **licensed identifiers** — distribution restrictions apply.

```sql
-- Do NOT expose identifier values to non-licensed users
-- Apply column masking on identifiers table
CREATE OR REPLACE FUNCTION statestreet.b_statestreet.mask_identifier(val STRING)
RETURNS STRING
RETURN CASE
  WHEN IS_MEMBER('licensed-data-access') THEN val
  ELSE CONCAT('***', RIGHT(val, 2))   -- show only last 2 chars
END;

ALTER TABLE statestreet.b_statestreet.identifiers
ALTER COLUMN identifier_value
SET MASK statestreet.b_statestreet.mask_identifier;
```

---

## 8. Compliance Reference

| Regulation | Requirement | Implementation |
|------------|------------|----------------|
| **SOX** | Immutable audit trail for financial data changes | `audit_log` with `appendOnly=true` |
| **SOX** | Segregation of duties | RBAC: analysts cannot write to Bronze/Silver |
| **MiFID II** | Data retention (5–7 years) | Delta time travel + `VACUUM` retention policy |
| **GDPR/CCPA** | Right to erasure for PII | Column masking; legal_entity PII fields masked |
| **BCBS 239** | Data lineage and accuracy | Unity Catalog lineage + DQ rule versioning |

---

## 9. Security Checklist

- [ ] All credentials in Databricks Secret Scopes (never in code)
- [ ] `.env`, `.databrickscfg`, `settings.json` in `.gitignore`
- [ ] `account users` access revoked from all schemas
- [ ] Pipeline service principal has minimal permissions (no admin)
- [ ] `audit_log` table created with `appendOnly=true`
- [ ] `_schema_changes` and `_schema_quarantine` tables created
- [ ] Column masking applied to `legal_entity_name` and `identifier_value`
- [ ] Branch protection enabled on `main` with 1 required reviewer
- [ ] `VACUUM` retention set to minimum 168 hours (7 days) for time travel compliance
- [ ] Git signed commits required (SOX)
