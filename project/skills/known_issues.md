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

---

## ISSUE-014: CLI — .env variables not loaded, commands fail with missing credentials

**Symptom**: `sml generate`, `sml deploy`, `sml run` etc. fail with missing
`ANTHROPIC_API_KEY` or `DATABRICKS_TOKEN` even though `.env` is populated.

**Cause**: `cli.py` did not call `load_dotenv()` on startup. Environment variables in
`.env` were never loaded into the process environment.

**Fix (already applied)**: Added `from dotenv import load_dotenv` and `load_dotenv()`
at the top of `cli.py` before any agent imports.

---

## ISSUE-015: CLI — `--use-case` was required on every command, breaking zero-arg usage

**Symptom**: Running `sml generate`, `sml deploy`, `sml run`, `sml status`, `sml debug`,
or `sml validate` without `--use-case` failed with "Missing option '--use-case'".

**Cause**: All six commands had `required=True` on the `--use-case` option.

**Fix (already applied)**: Changed to `default="securities-master", show_default=True`
on all six commands. Users can still override with `--use-case <name>`.

---

## ISSUE-016: deploy_agent — job status/trigger used SDK name lookup instead of bundle CLI

**Symptom**: `sml status --job <name>` and `sml run --job <name>` failed with SDK
errors ("job not found", auth errors) even with valid credentials.

**Cause**: `get_job_status()` and `trigger_job()` used `WorkspaceClient().jobs.list()`
and `jobs.run_now()` to look up jobs by display name. Bundle-deployed jobs use resource
keys, not display names, making the SDK lookup unreliable.

**Fix (already applied)**: Both functions now delegate to `databricks bundle run`
(with `--refresh` for status) which correctly resolves bundle resource keys.

---

## ISSUE-017: deploy_agent — `openpgp: key expired` error during bundle deploy

**Symptom**: `sml deploy` fails with:
```
Error: Failed to install terraform
openpgp: key expired
```

**Cause**: The Databricks CLI tries to download and verify Terraform via a GPG key that
has expired. If a local Terraform binary is present, it should be used directly.

**Fix (already applied)**: `deploy_agent.py` now auto-detects a local `terraform` binary
via `shutil.which("terraform")` and sets `DATABRICKS_TF_EXEC_PATH` to it before running
`databricks bundle validate/deploy`. This bypasses the GPG-verified download.

**If Terraform is not installed locally**:
```bash
brew install terraform   # macOS
```

---

## ISSUE-018: databricks.yml — redundant `databricks_host` variable caused deploy errors

**Symptom**: `databricks bundle validate` warns about `databricks_host` being unused,
or deploy fails because `${var.databricks_host}` resolves to empty string.

**Cause**: `databricks.yml` declared a `databricks_host` variable and referenced it as
`workspace.host: ${var.databricks_host}`. The Databricks CLI already reads the host
from `.databrickscfg` or environment variables — overriding it with an empty default
caused authentication failures.

**Fix (already applied)**: Removed the `databricks_host` variable and all `workspace.host`
references from `databricks.yml`. Let the CLI resolve the workspace from `.databrickscfg`.

---

## ISSUE-019: databricks.yml — `dq_report` task referenced a non-existent notebook

**Symptom**: `databricks bundle validate` fails with notebook path not found, or
the Silver job fails at the `dq_report` task.

**Cause**: `databricks.yml` defined a `dq_report` task pointing to
`notebooks/04b_dq_report.py` which was never created.

**Fix (already applied)**: Removed the `dq_report` task from the Silver job definition
in `databricks.yml`. DQ reporting is handled inline in `04_silver_conform.sql`.

---

## ISSUE-020: databricks.yml — hardcoded `job_clusters` blocks caused serverless conflicts

**Symptom**: Jobs failed to start, or Databricks showed cluster provisioning errors when
serverless compute was expected.

**Cause**: Each job in `databricks.yml` had a `job_clusters` block defining `i3.xlarge`
clusters. When the workspace uses serverless compute by default, specifying `job_cluster_key`
in tasks conflicts with serverless routing.

**Fix (already applied)**: Removed all `job_clusters` blocks from all four jobs
(`bronze_ingest_job`, `silver_conform_job`, `gold_build_job`, `orchestrate_pipeline_job`).
Tasks now use the workspace-default compute (serverless or existing shared cluster).
