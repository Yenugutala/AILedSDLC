# Known Issues & Workarounds — Global

This file is auto-updated by the Debug Agent when new recurring patterns are discovered.
Also see use-cases/<name>/known_issues.md for use-case-specific issues.

---

## ISSUE-001: Databricks SDK not configured

**Symptom**: `sml debug` or `sml status` fails with "Could not connect to Databricks workspace"

**Cause**: `DATABRICKS_HOST` or `DATABRICKS_TOKEN` environment variables not set.

**Fix**:
```bash
export DATABRICKS_HOST="https://<workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="<your-pat-token>"
```
Or configure in `.databrickscfg`:
```ini
[DEFAULT]
host = https://<workspace>.azuredatabricks.net
token = <your-pat-token>
```
**Important**: Never commit PAT tokens to git.

---

## ISSUE-002: Delta UniForm requires Databricks Runtime 13.3+

**Symptom**: `SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')` fails

**Cause**: Cluster is running Databricks Runtime < 13.3 LTS

**Fix**: Update cluster to DBR 13.3 LTS or higher in cluster settings.

---

## ISSUE-003: Currency CSV has 2 intentionally bad rows

**Symptom**: Silver currency table rejects 2 rows with invalid currency codes.

**Cause**: Deliberately seeded in `currency.csv` as DQ test data.

**This is expected**. Do not "fix" the source data. The rejects table confirms DQ rules work.

---

## ISSUE-004: `RLIKE` vs `REGEXP_LIKE` across SQL engines

**Symptom**: DQ rule SQL fails on engines other than Databricks/Spark

**Cause**: `dq_validation_queries.sql` uses `RLIKE` (Spark syntax). Standard ANSI SQL uses `REGEXP_LIKE`.

**Fix**: The pipeline uses Databricks only — `RLIKE` is correct. If running outside Databricks, substitute `RLIKE` → `REGEXP_LIKE`.

---

## ISSUE-005: generic_product table — expected to have duplicate-like rows

**Symptom**: `generic_product` table appears to have many rows per product.

**Cause**: By design. `generic_product` is a deprecated legacy shadow table. 1 product → many generic_product rows. The uniqueness DQ rule is intentionally NOT applied to this table.

---

## ISSUE-006: agent_state.yaml accumulates history indefinitely

**Symptom**: Large agent_state.yaml files after many reject/revise cycles.

**Cause**: Every iteration is stored for auditability.

**Fix**: Manually truncate history in `agent_state.yaml` after a use case is complete.
Or delete the file entirely to restart from scratch: `rm use-cases/<name>/agent_state.yaml`

---

## ISSUE-007: pip install fails due to anthropic SDK version conflict

**Symptom**: `pip install -e .` fails with dependency conflict

**Fix**: Create a fresh virtual environment first:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

---

## ISSUE-008: ModuleNotFoundError: No module named 'src'

**Symptom**: `sml setup` fails immediately after the Setup Agent banner:
```
ModuleNotFoundError: No module named 'src'
```

**Cause**: `setup_agent.py` imports `from src.common.setup_utils import SetupManager` but
Python's path does not include `project/` (where the `src` package lives) at runtime.

**Fix (pre-applied)**: `setup_agent.py` inserts `project/` into `sys.path` before the import.
No action needed if running the current codebase.

---

## ISSUE-009: Invalid Databricks access token

**Symptom**: `sml setup` connects but immediately fails:
```
[ERROR] Schema creation failed: Invalid access token.
```

**Cause**: `DATABRICKS_TOKEN` in `.env` is expired, blank, or incorrectly copied.

**Fix**:
1. Databricks UI → profile icon → **Settings** → **Developer** → **Access tokens**
2. Click **Generate new token** → set a name and expiry → copy the value
3. Paste into `.env`: `DATABRICKS_TOKEN=dapi<new-token>`

---

## ISSUE-010: DELTA_UNIVERSAL_FORMAT_VIOLATION during sml setup

**Symptom**: `sml setup` fails when creating the `audit_log` table:
```
[DELTA_UNIVERSAL_FORMAT_VIOLATION] Requires IcebergCompat to be explicitly enabled
in order for Universal Format (Iceberg) to be enabled on an existing table.
```

**Cause**: The `audit_log` DDL included `TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')`
without also enabling `delta.enableIcebergCompatV2`. Databricks enforces this as of DBR 14+.

**Fix (pre-applied)**: `setup_utils.py` audit table DDL now uses plain `USING DELTA` with no
Iceberg UniForm property. No action needed.

---

## ISSUE-011: DATABRICKS_WAREHOUSE_ID set to workspace org ID instead of warehouse ID

**Symptom**: `sml setup` fails to execute any DDL — typically "warehouse not found" or auth errors.

**Cause**: The workspace URL contains `?o=7474660752819507` (org ID). Users sometimes
copy this number as the warehouse ID. They are different values.

**Correct value**: Go to **SQL Warehouses** → click your warehouse → **Connection Details** →
copy the last segment of the **HTTP path**: `/sql/1.0/warehouses/<THIS_IS_THE_ID>`

---

## ISSUE-012: Python 3.9 / macOS LibreSSL warnings on sml commands

**Symptom**: Every `sml` command prints warnings:
```
NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, LibreSSL 2.8.3 found.
FutureWarning: Python 3.9 is past end of life.
```

**Cause**: macOS ships Python 3.9 compiled against LibreSSL. Python 3.9 EOL was Oct 2025.

**Impact**: Warnings only — all functionality works correctly.

**Recommended fix**: Upgrade to Python 3.11+ via homebrew (`brew install python@3.11`)
and recreate the virtual environment.

---

## ISSUE-013: YAML ScannerError in spec files — unquoted colons in description fields

**Symptom**: `sml generate` fails after BA Agent writes spec YAMLs:
```
yaml.scanner.ScannerError: mapping values are not allowed here
  in ".../specs/bronze/tables.yaml", line NNN, column NN
```

**Cause**: YAML treats any bare `word:` pattern as a mapping key. Description strings
containing colons (e.g. `Grain: one row per product_id`) trigger this error when unquoted.

**Fix (pre-applied)**: All `description:` values containing colons in spec files are wrapped
in double quotes or use block scalar syntax (`>`).

**Prevention**: When writing or editing spec YAML files, always quote descriptions that
contain colons:
```yaml
# Wrong:
description: Securities table. Grain: one row per product_id.

# Correct (quoted):
description: "Securities table. Grain: one row per product_id."

# Correct (block scalar):
description: >
  Securities table. Grain: one row per product_id.
```
