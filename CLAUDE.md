# Securities Master Data Lakehouse — Coding Standards

## Naming Conventions

### Unity Catalog Structure

| Layer | Catalog | Schema | Full Reference Pattern |
|-------|---------|--------|----------------------|
| Bronze | `statestreet` | `b_statestreet` | `statestreet.b_statestreet.<table>` |
| Silver | `statestreet` | `s_statestreet` | `statestreet.s_statestreet.<table>` |
| Silver Rejects | `statestreet` | `s_statestreet` | `statestreet.s_statestreet.<table>_rejects` |
| Gold | `statestreet` | `g_statestreet` | `statestreet.g_statestreet.<name>` |

### Table Names
- **Bronze and Silver**: keep original CSV file names exactly (e.g. `product`, `bond`, `stock`)
- **Gold**: single wide/flat table per use case — no `dim_` or `fact_` prefix (e.g. `securities_master`)

### Column Names
- All columns: `snake_case`
- Metadata columns (added by pipeline): `_`-prefixed
  - `_ingestion_ts` — timestamp when row was loaded to Bronze
  - `_source_file` — source CSV file name
  - `_batch_id` — pipeline run identifier
  - `_row_hash` — SHA256 of all data columns (for CDC / change detection)
  - `_dq_rule_version` — SHA256 of silver/rules.yaml at time of DQ check

### SCD2 Columns (Silver + Gold where applicable)
- `effective_start_date DATE` — when this version became active
- `effective_end_date DATE` — when this version was superseded (9999-12-31 = current)
- `is_current BOOLEAN` — TRUE for the active version

## Language Split
- **Bronze ingestion**: Python / PySpark notebooks
- **Silver conformance**: SQL notebooks (Databricks SQL dialect)
- **Gold mart build**: SQL notebooks (Databricks SQL dialect)
- **DQ checks**: SQL SELECT statements (Spark-SQL compatible)

## Databricks Compatibility Rules
- Use `MERGE INTO` for idempotent loads (not `INSERT OVERWRITE`)
- Use `current_timestamp()` not `NOW()` or `GETDATE()`
- Use `current_date()` not `CURRENT_DATE` (Spark prefers the function form)
- Use `RLIKE` for regex matching in Spark SQL
- Always add `TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')` for Iceberg UniForm
- Unity Catalog 3-part names: `catalog.schema.table`
- Volume path pattern: `/Volumes/<catalog>/<schema>/<volume>/`

## Agent Rules
- BA Agent generates all 6 spec files (bronze + silver + gold: tables.yaml + rules.yaml each)
- Architect Agent reviews and finalizes specs — does not generate code
- Developer Agent generates Bronze as Python, Silver/Gold as SQL
- QA Agent generates pytest for Bronze Python, SQL test queries for Silver/Gold
- Doc Agent adds COMMENT ON TABLE / COMMENT ON COLUMN for Genie
- Debug Agent uses Databricks MCP to fetch logs — never guess error causes
- All agents read `CLAUDE.md` + `skills/` context before generating output
- Human types only "approve" or "reject: <reason>" — never specifies which agent runs next

## Git Rules
- Never commit directly to `main` — always raise a PR
- Branch naming: `feat/<use-case>-<stage>` (e.g. `feat/securities-master-silver`)
- `agent_state.yaml` is gitignored — do not commit it
- `generated/` folder is gitignored — it appears only as PR diff

## Volume Path
```
/Volumes/statestreet/securities_master/raw_files/
```
29 source CSV files live here. Bronze ingestion reads from this path.

## Spec File Locations
```
use-cases/<name>/specs/bronze/tables.yaml
use-cases/<name>/specs/bronze/rules.yaml
use-cases/<name>/specs/silver/tables.yaml
use-cases/<name>/specs/silver/rules.yaml
use-cases/<name>/specs/gold/tables.yaml
use-cases/<name>/specs/gold/rules.yaml
```
