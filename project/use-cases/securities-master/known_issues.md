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
