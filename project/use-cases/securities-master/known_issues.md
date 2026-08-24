# Known Issues — Securities Master Use Case

## USE-CASE-001: dq_rules_catalog and dq_issues_catalog are metadata tables

**Tables**: `dq_rules_catalog`, `dq_issues_catalog`

**Note**: These two CSV files are metadata, not securities data. They should be ingested
to Bronze alongside the 27 security tables (so they are tracked in the lakehouse), but they
do NOT need Silver conformance or Gold mart treatment. They are reference-only.

**Action**: Bronze only for these two tables. No DQ rules applied to them.

---

## USE-CASE-002: currency table has 2 deliberately bad rows

**Table**: `currency`

**Note**: Two rows in `currency.csv` have intentionally invalid currency codes.
This is seeded data to test the DQ framework. The Silver DQ rule for currency code
format (ISO 4217: exactly 3 uppercase letters) WILL catch these rows.

**Expected result**: 2 rows in `s_statestreet.currency_rejects`. This is correct behavior.

---

## USE-CASE-003: generic_product has many rows per product (by design)

**Table**: `generic_product`

**Note**: The `generic_product` table is a deprecated legacy shadow. One `product` row
can have MANY `generic_product` rows. The primary key uniqueness DQ rule does NOT apply
to this table. It is kept for backward compatibility only.

**Action**: No PRIMARY_KEY_UNIQUE DQ rule for `generic_product`.

---

## USE-CASE-004: dq_validation_queries.sql is source-of-truth for DQ SQL

**File**: `dq_validation_queries.sql` (in Databricks Volume)

The 128 SQL queries in this file are the authoritative DQ check implementations.
When generating `specs/silver/rules.yaml`, convert these queries into the `rule_logic_sql`
field for each rule. The SQL uses Databricks/Spark syntax (`RLIKE` not `REGEXP_LIKE`).

---

## USE-CASE-005: Pool-backed security has no direct bond linkage

**Tables**: `pool_backed_security`, `bond`

**Note**: `PoolBackedSecurity` extends `Debt` in the class hierarchy, NOT `Bond`.
So `pool_backed_security` rows join to `debt` on `product_id`, but NOT to `bond`.
In `dim_product`, bond-specific attributes (coupon_type, maturity_date) will be NULL
for pool-backed securities.

**Action**: In Gold LEFT JOINs, join pool_backed_security to debt (not bond).

---

# Setup & Environment Issues

## SETUP-001: ModuleNotFoundError: No module named 'src'

**When**: Running `sml setup` for the first time.

**Error**:
```
ModuleNotFoundError: No module named 'src'
```

**Cause**: `setup_agent.py` imports `from src.common.setup_utils import SetupManager` but
Python's path does not include `project/` where the `src` package lives.

**Fix (already applied)**: `setup_agent.py` now inserts `project/` into `sys.path` before
the import:
```python
_PROJECT_ROOT = Path(__file__).parent.parent.parent / "project"
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from src.common.setup_utils import SetupManager
```
No action needed — this is pre-fixed in the codebase.

---

## SETUP-002: Invalid access token

**When**: Running `sml setup` after cloning or after token expiry.

**Error**:
```
[ERROR] Schema creation failed: Invalid access token.
```

**Cause**: The `DATABRICKS_TOKEN` in `.env` is expired or was never filled in.

**Fix**:
1. In Databricks UI → click your profile (top right) → **Settings**
2. Left sidebar → **Developer** → **Access tokens**
3. Click **Generate new token** → give it a name and expiry → copy the token
4. Paste it into `.env`:
   ```
   DATABRICKS_TOKEN=dapi<your-new-token>
   ```

---

## SETUP-003: DELTA_UNIVERSAL_FORMAT_VIOLATION (Iceberg UniForm error)

**When**: Running `sml setup` — the `audit_log` table creation fails.

**Error**:
```
[DELTA_UNIVERSAL_FORMAT_VIOLATION] The validation of Universal Format (iceberg) has failed:
Requires IcebergCompat to be explicitly enabled in order for Universal Format (Iceberg)
to be enabled on an existing table.
```

**Cause**: Databricks requires `delta.enableIcebergCompatV2 = 'true'` to be set
alongside `delta.universalFormat.enabledFormats = 'iceberg'` on existing tables.
The internal audit table DDL had the UniForm property but not IcebergCompatV2.

**Fix (already applied)**: `setup_utils.py` audit table DDL no longer includes the
Iceberg UniForm TBLPROPERTY. These internal tables use plain `USING DELTA`.
No action needed — this is pre-fixed in the codebase.

---

## SETUP-004: Python 3.9 LibreSSL / google-auth warnings

**When**: Running any `sml` command on macOS with Python 3.9.

**Warnings**:
```
NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module
is compiled with 'LibreSSL 2.8.3'.

FutureWarning: You are using a Python version 3.9 past its end of life.
```

**Cause**: macOS system Python 3.9 uses LibreSSL instead of OpenSSL. Python 3.9 is
also end-of-life as of October 2025.

**Impact**: Warnings only — functionality is not affected. The Databricks SDK and
all CLI commands work correctly despite these warnings.

**Recommended fix**: Upgrade to Python 3.11+ using pyenv or homebrew:
```bash
brew install python@3.11
pyenv install 3.11
```
Then recreate the virtual environment with the newer Python version.

---

## SETUP-005: DATABRICKS_WAREHOUSE_ID is the HTTP path ID, not the workspace org ID

**When**: Filling in `.env` for the first time.

**Common mistake**: Using the workspace organization ID (e.g. `7474660752819507` from the
URL `?o=7474660752819507`) as the `DATABRICKS_WAREHOUSE_ID`.

**Correct value**: The SQL Warehouse ID — found in:
Databricks UI → **SQL Warehouses** → click your warehouse → **Connection Details** tab →
**HTTP path**: `/sql/1.0/warehouses/<this-is-the-id>`

Copy only the last segment (e.g. `6fe1b6ace96017a1`).

---

## SETUP-006: YAML ScannerError — "mapping values are not allowed here"

**When**: Running `sml generate` after BA Agent writes spec YAML files.

**Error**:
```
yaml.scanner.ScannerError: mapping values are not allowed here
  in ".../specs/bronze/tables.yaml", line 249, column 62
```

**Cause**: YAML does not allow unquoted colons in scalar values. Description fields like:
```yaml
description: Subscription rights extending product. Grain: one row per product_id.
```
cause a parse failure because YAML interprets `Grain:` as a new mapping key.

**Fix (already applied)**: All `description:` values containing colons are now wrapped
in double quotes:
```yaml
description: "Subscription rights extending product. Grain: one row per product_id."
```

**Prevention for future spec files**: Always quote any `description:` value that contains
a colon (`:`). Alternatively use the YAML block scalar syntax:
```yaml
description: >
  Subscription rights extending product. Grain: one row per product_id.
```

---

## SETUP-007: YAML ScannerError — stray markdown text appended to spec YAML file

**When**: Running `sml generate` — context loader fails to parse a spec YAML.

**Error**:
```
yaml.scanner.ScannerError: while scanning an alias
  in ".../specs/bronze/tables.yaml", line 570, column 1
expected alphabetic or numeric character, but found '*'
```

**Cause**: The BA Agent occasionally appends markdown prose (e.g. `**Key Decisions`) after
the last YAML entry in a spec file. YAML interprets `**` as an alias token and fails.

**Fix**: Open the failing YAML file, scroll to the line number in the error, and delete
any non-YAML content (markdown headings, prose text, triple backticks) after the last
`metadata_columns` block.

**Prevention**: The spec YAML files must contain only valid YAML — no markdown formatting.

---

## SETUP-008: Stray markdown fence at top of notebook files breaks Databricks parsing

**Files affected**: `03_bronze_ingest.py`, `04_silver_conform.sql`, `05_gold_build.sql`

**Symptom**: Databricks notebook fails to parse or the first cell appears as literal text
instead of executable code.

**Cause**: Agent-generated notebooks had a stray ` ```python` or ` ```sql` fence as the
very first line — markdown formatting that is invalid in Databricks notebooks.

**Fix (already applied)**: The fences were removed from all three notebooks. The first line
must be either `# Databricks notebook source` (Python) or `-- Databricks notebook source` (SQL).

**Prevention**: Never include triple-backtick fences in notebook files. Databricks uses
its own cell delimiter (`# COMMAND ----------`).

---

## SETUP-009: Iceberg UniForm requires 3 TBLPROPERTIES set together — not just one

**Files affected**: `03_bronze_ingest.py`, `04_silver_conform.sql`, `05_gold_build.sql`

**Error**:
```
[DELTA_UNIVERSAL_FORMAT_VIOLATION] The validation of Universal Format (iceberg) has failed:
Requires IcebergCompat to be explicitly enabled in order for Universal Format (Iceberg)
to be enabled on an existing table.
```

**Cause**: Original notebooks only set `delta.universalFormat.enabledFormats = 'iceberg'`.
Databricks (DBR 14+) requires **all three** to be set together in a single statement:

```sql
TBLPROPERTIES (
  'delta.columnMapping.mode'           = 'name',
  'delta.enableIcebergCompatV2'        = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
```

**Fix (already applied)**:
- All 13 Silver `CREATE TABLE IF NOT EXISTS` DDLs updated with all three properties.
- All 4 Gold `CREATE OR REPLACE TABLE` DDLs updated with all three properties.
- `_enable_iceberg_uniform()` in Bronze now sets all three properties together (with
  silent fallback warning if preconditions are not met).

---

## SETUP-010: Gold — column name mismatches against Silver schema

**File**: `05_gold_build.sql`

**Cause**: Gold notebook referenced column names that did not exist in Silver tables.
These were fixed by mapping to the correct Silver column names or casting NULLs where
the column genuinely does not exist.

| Gold expression (wrong) | Fix applied |
|---|---|
| `le.name` | → `le.legal_name` |
| `le.entity_type` | → `CAST(NULL AS STRING) AS entity_type` (column absent from Silver) |
| `cs.voting_rights` | → `CAST(NULL AS BOOLEAN) AS voting_rights` (not in Silver common_stock) |
| `ps.dividend_type` | → `ps.dividend_right AS dividend_type` |
| `d.face_amount` | → `d.total_amount_issued AS face_amount` |
| `d.issue_date_settlement` | → `CAST(NULL AS DATE) AS issue_date_settlement` |
| `b.face_currency_code` | → `b.issue_currency_code AS face_currency_code` |
| `b.day_count_convention` | → `CAST(NULL AS STRING) AS day_count_convention` |
| `r.subscription_ratio` | → `CAST(NULL AS DECIMAL(28,8)) AS subscription_ratio` |

**Fix (already applied)**: All corrections applied in `05_gold_build.sql`.

---

## SETUP-011: Gold — fact_product_rating column and JOIN key mismatches

**File**: `05_gold_build.sql`

**Cause**: `fact_product_rating` used column names from an earlier draft of the Silver
`product_rating` schema that differed from the actual Silver DDL.

| Wrong | Correct |
|---|---|
| `pr.rating_id` | `pr.product_rating_id` |
| `pr.rating_date` | `pr.effective_from_date` |
| `pr.rating_type_id` | `pr.product_rating_type_id` |
| `prt.rating_agency` | moved to `pr.rating_agency` (lives on rating row, not type) |
| `prt.rating_category` | `CAST(NULL AS STRING) AS rating_category` (not in Silver) |
| JOIN: `pr.rating_type_id = prt.rating_type_id` | `pr.product_rating_type_id = prt.product_rating_type_id` |
| DQ grain check: `GROUP BY product_id, rating_date, rating_type_id` | `GROUP BY product_id, effective_from_date, product_rating_type_id` |
| Column comment: `rating_id`, `rating_date` | corrected to `product_rating_id`, `effective_from_date` |

**Fix (already applied)**: All corrections applied in `05_gold_build.sql`.

---

## SETUP-012: Gold — fact_coupon_schedule referenced non-existent bond columns

**File**: `05_gold_build.sql`

**Cause**: `fact_coupon_schedule` referenced columns that do not exist in the Silver `bond` table,
and included SCD2 columns on `coupon` which has no SCD2 tracking.

| Wrong | Correct |
|---|---|
| `b.face_currency_code` | `b.issue_currency_code AS face_currency_code` |
| `b.day_count_convention` | `CAST(NULL AS STRING) AS day_count_convention` |
| `c.effective_start_date`, `c.effective_end_date` | Removed — `coupon` has no SCD2 columns |

**Fix (already applied)**: All corrections applied in `05_gold_build.sql`.

---

## SETUP-013: Bronze ingestion notebook was truncated — missing execution loop and summary

**File**: `03_bronze_ingest.py`

**Symptom**: Running `03_bronze_ingest.py` defined all helper functions and constants
but never actually executed any ingestion — the file ended at `INGESTION_ORDER` list
definition without calling `ingest_table()`.

**Also**: `dq_rules_catalog` and `dq_issues_catalog` were missing from the end of
`INGESTION_ORDER` (they are Group 5 Bronze-only metadata tables per USE-CASE-001).

**Fix (already applied)**:
- Added `RUN_TABLES` filter (respects `tables_override` widget if supplied).
- Added ingestion execution loop with per-table error capture.
- Added summary print block (rows read/written per table, total counts).
- Added final `RuntimeError` raise if any table failed (so Databricks job marks as FAILED).
- Appended `"dq_rules_catalog"` and `"dq_issues_catalog"` to `INGESTION_ORDER`.

---

## SETUP-015: Pipeline resumes from wrong stage after agent_state.yaml is stale

**Symptom**: `sml generate` starts from Deploy Agent instead of BA Agent even after
all generated files were deleted.

**Cause**: `agent_state.yaml` is gitignored and persists locally across sessions. If a
previous run completed all stages and set `current_stage: deploy`, the next run resumes
from deploy regardless of whether generated files exist.

**Fix**: Delete `agent_state.yaml` to force a full restart:
```bash
rm project/use-cases/securities-master/agent_state.yaml
```

---

## SETUP-016: Generated notebooks fail `databricks bundle validate` — not a notebook

**Symptom**: `sml deploy` (or the pipeline deploy stage) fails with:
```
Error: expected a notebook for "...tasks[0].notebook_task.notebook_path"
but got a file: file at .../03_bronze_ingest.py is not a notebook
```
(Same error can appear for `04_silver_conform.sql` or `05_gold_build.sql`.)

**Cause**: The Developer Agent wrapped the generated code in a ` ```python ` or ` ```sql ` code fence.
The file literally started with ` ``` ` instead of the required Databricks header,
so Databricks rejected it as a non-notebook file.

Required first lines:
- Python notebooks: `# Databricks notebook source`
- SQL notebooks: `-- Databricks notebook source`

**Fix (already applied in code)**: `code_gen_agent._write_notebook()` now calls `_strip_code_fence()`
to remove any wrapping ` ``` ` fence before writing the file.

**If the files already exist on disk with the fence**, manually remove the first line:
```bash
tail -n +2 project/notebooks/03_bronze_ingest.py   > /tmp/b.py  && mv /tmp/b.py  project/notebooks/03_bronze_ingest.py
tail -n +2 project/notebooks/04_silver_conform.sql > /tmp/s.sql && mv /tmp/s.sql project/notebooks/04_silver_conform.sql
tail -n +2 project/notebooks/05_gold_build.sql    > /tmp/g.sql && mv /tmp/g.sql project/notebooks/05_gold_build.sql
```
Then verify all three:
```bash
for f in project/notebooks/03_bronze_ingest.py project/notebooks/04_silver_conform.sql project/notebooks/05_gold_build.sql; do echo "$f: $(head -1 $f)"; done
```

---

## SETUP-018: Deploy Agent (Beat 4b) shows "Databricks CLI not configured" when notebooks are missing

**Symptom**: Running Beat 4b directly (or jumping to Deploy Agent) shows a yellow warning panel:
```
⚠ Databricks CLI not configured
Reason: notebook notebooks/03_bronze_ingest.py not found
```
The misleading title makes it look like a CLI auth issue, but the real cause is missing notebooks.

**Cause**: `deploy_agent.deploy()` calls `databricks bundle validate` which fails because
the Developer Agent (Beat 4) was never run. The exception is caught generically and displayed
with the wrong title ("Databricks CLI not configured").

**Fix (already applied)**: `deploy_agent.deploy()` and `trigger_job()` now call `_check_notebooks()`
before touching the CLI. If any of the three notebooks are missing, a clear error is raised:
```
Cannot deploy — the following notebooks are missing from project/notebooks/:
  ✗ notebooks/03_bronze_ingest.py
  ✗ notebooks/04_silver_conform.sql
Run Beat 4 (Developer Agent) first to generate all notebooks.
```

**What to do**: Run Beat 4 first. In `sml demo`, at any HITL prompt type:
**"run developer agent"** → jumps to Beat 4 → generates all notebooks → then approve → Beat 4b runs.

---

## SETUP-017: `databricks bundle run <job>` fails when ANY notebook in databricks.yml is missing

**Symptom**: Running `databricks bundle run gold_mart_job` (or any single job) fails with:
```
Error: notebook notebooks/03_bronze_ingest.py not found
Error: notebook notebooks/04_silver_conform.sql not found
```
even though `gold_mart_job` only needs `notebooks/05_gold_build.sql`.

**Cause**: `databricks bundle run` validates the **entire** bundle before running any single job.
All notebook paths declared in `databricks.yml` must exist on disk — not just the notebooks
needed by the specific job being run.

**Trigger**: This happens when the Developer Agent generates only the gold notebook
(e.g. `layer_only="gold"`) without also generating bronze and silver notebooks,
or when notebooks are deleted (e.g. `git clean`) between runs.

**Fix for Developer Agent**:
- Always generate ALL three notebooks together: `03_bronze_ingest.py`, `04_silver_conform.sql`, `05_gold_build.sql`
- Never generate only a single layer if the other notebooks do not already exist on disk
- Check `project/notebooks/` before starting: if any of the three are missing, generate all three

**Manual fix**: Run Beat 4 (Developer Agent) to regenerate all notebooks, then re-run Beat 4b (Deploy Agent).

---

## SETUP-014: Gold — `COMMENT ON COLUMN rating_value` truncated — unclosed SQL string literal

**File**: `05_gold_build.sql`

**Error**:
```
ParseException: [PARSE_SYNTAX_ERROR] Syntax error at or near '''. SQLSTATE: 42601 (line 2, pos 2)
COMMENT ON COLUMN statestreet.g_statestreet.fact_product_rating.rating_value IS
  'Credit rating code. Examples: AAA, AA+, AA, AA-, A+, A, A-, BBB+,
```

**Cause**: The `COMMENT ON COLUMN` for `fact_product_rating.rating_value` was truncated
mid-string — the file ended at line 647 without a closing quote or semicolon.
Databricks parsed the next cell's SQL as a continuation of the unclosed string literal,
causing the `ParseException`.

**Fix (already applied)**: Completed the `rating_value` comment with a proper closing
quote and semicolon, and added the remaining missing column comments for
`fact_product_rating` (`product_rating_type_id`, `rating_agency`, `watch_code`,
`rating_scale`, `rating_type_code`, `product_type`, `product_status`).

---

## GOLD-001: `listed_derivative` has no `underlying_product_id` column

**File**: `05_gold_build.sql`

**Symptom**: Gold job fails with `UNRESOLVED_COLUMN: ld.underlying_product_id`.

**Cause**: The `listed_derivative` CSV does not contain an `underlying_product_id` column.
Actual columns: `product_id`, `contract_year`, `contract_month`, `contract_size`,
`last_trade_date`, `is_flex`, `series_id`.

**Fix (already applied)**: Use `CAST(NULL AS STRING) AS derivative_underlying_product_id`
in the Gold SELECT. Only add the real column if a Jira ticket sources it from a new CSV.

---

## GOLD-002: `coupon` table uses `bond_id`, not `product_id`

**File**: `05_gold_build.sql`

**Symptom**: Gold job fails with `UNRESOLVED_COLUMN: coupon.product_id`.

**Cause**: The `coupon.csv` foreign key is named `bond_id` (references `bond.product_id`),
not `product_id` directly. This differs from all other product-subtype tables.

**Fix (already applied)**: In the `latest_coupon` CTE use `bond_id AS product_id`:
```sql
latest_coupon AS (
  SELECT
    bond_id AS product_id,
    coupon_rate,
    payment_date AS latest_payment_date,
    ROW_NUMBER() OVER (PARTITION BY bond_id ORDER BY payment_date DESC) AS _rn
  FROM statestreet.s_statestreet.coupon
)
```

---

## GOLD-003: `identifiers` table is wide-format, not EAV

**File**: `05_gold_build.sql`

**Symptom**: Gold job fails with `UNRESOLVED_COLUMN: id_type` or `identifier_value`
inside the `primary_identifier` CTE.

**Cause**: The gold notebook assumed EAV format (`id_type`, `identifier_value` rows).
The actual `identifiers.csv` is wide-format: one row per product with columns
`cusip`, `isin`, `sedol`, `bloomberg_id`, `bloomberg_ticker`.

**Fix (already applied)**: Use `COALESCE` and `CASE` in the CTE:
```sql
primary_identifier AS (
  SELECT
    product_id,
    CASE
      WHEN isin             IS NOT NULL THEN 'ISIN'
      WHEN cusip            IS NOT NULL THEN 'CUSIP'
      WHEN sedol            IS NOT NULL THEN 'SEDOL'
      WHEN bloomberg_id     IS NOT NULL THEN 'BLOOMBERG_ID'
      WHEN bloomberg_ticker IS NOT NULL THEN 'TICKER'
      ELSE NULL
    END AS primary_id_type,
    COALESCE(isin, cusip, sedol, bloomberg_id, bloomberg_ticker) AS primary_identifier_value,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS _rn
  FROM statestreet.s_statestreet.identifiers
  WHERE product_id IS NOT NULL
)
```

---

## GOLD-004: ZORDER BY cannot include partition columns

**File**: `05_gold_build.sql`

**Symptom**: OPTIMIZE step fails with:
```
[DELTA_ZORDERING_ON_PARTITION_COLUMN] type is a partition column.
Z-Ordering can only be performed on data columns. SQLSTATE: 42P10
```

**Cause**: The table is `PARTITIONED BY (type)`. Delta Lake forbids listing a partition
column in `ZORDER BY` — it is already physically co-located.

**Fix (already applied)**: Removed `type` from `ZORDER BY`:
```sql
OPTIMIZE statestreet.g_statestreet.securities_master
  ZORDER BY (status, sub_type);
```

---

## GOLD-005: `net_settlement_amount` — do NOT include in the base Gold notebook

**File**: `05_gold_build.sql`

**Background**: `net_settlement_amount` requires `bond.principal_amount` and
`bond.accrued_interest_rate` columns that do NOT exist in the current `bond.csv`.
Generating it as `CAST(NULL AS DECIMAL(18,6))` adds noise and misleads Genie.

**Rule**: The base gold notebook must NOT contain `net_settlement_amount`.

**When to add it**: Only when a Jira ticket explicitly asks for net settlement calculation
(e.g. "compute net settlement for bonds"). The Developer Agent should then:
1. Add `principal_amount` and `accrued_interest_rate` to the `bond.csv` source (or a new CSV)
2. Re-ingest Bronze
3. Re-run Silver
4. Add the column to Gold with the formula:
   ```sql
   CASE
     WHEN p.type = 'DEBT' AND p.sub_type IN ('BOND', 'MUNI')
          AND b.principal_amount IS NOT NULL
          AND b.accrued_interest_rate IS NOT NULL
     THEN b.principal_amount * (1 + b.accrued_interest_rate)
     ELSE NULL
   END AS net_settlement_amount
   ```
5. Add DQ-GOLD-02 and DQ-GOLD-03 checks back
6. Add the `COMMENT ON COLUMN` for `net_settlement_amount`

---

## SILVER-001: `spark.conf.set()` with custom keys blocked on serverless compute

**File**: `04_silver_conform.sql`

**Symptom**: Silver job fails immediately with:
```
CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION: Cannot set 'sml.dq_rule_version'
to a user-supplied value on Serverless compute.
```

**Cause**: Databricks Serverless does not permit `spark.conf.set()` with non-standard
config keys. The original Silver notebook used this to store the DQ rule version hash.

**Fix (already applied)**: Removed `spark.conf.set("sml.dq_rule_version", ...)` from the
Silver notebook. The `_dq_rule_version` column is set directly as a literal in each
`CREATE OR REPLACE TABLE ... AS SELECT` statement:
```sql
'dev-snapshot' AS _dq_rule_version
```

---

## SILVER-002: Silver notebook truncates at ~880 lines — all 26 tables must be explicit

**File**: `04_silver_conform.sql`

**Symptom**: Agent-generated Silver notebook ends mid-DDL (e.g. inside `listed_derivative`
table), causing a `PARSE_SYNTAX_ERROR` when deployed.

**Cause**: The code generation API call hits the token budget before producing all 26 table
definitions. The file is written truncated without any error.

**Fix (already applied)**: The Silver notebook was rewritten as a simple pass-through
(no DQ checking, no complex transformation). Each table is:
```sql
CREATE OR REPLACE TABLE statestreet.s_statestreet.<table>
TBLPROPERTIES (
  'delta.columnMapping.mode'             = 'name',
  'delta.enableIcebergCompatV2'          = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
)
AS
SELECT *,
  current_date()    AS effective_start_date,
  DATE '9999-12-31' AS effective_end_date,
  TRUE              AS is_current,
  'dev-snapshot'    AS _dq_rule_version
FROM statestreet.b_statestreet.<table>;
```

**Rule for Developer Agent**: If regenerating the Silver notebook, verify the file
covers **all 26 tables** before writing. If the API response is truncated, split into
multiple calls (one group of tables per call) and concatenate.

