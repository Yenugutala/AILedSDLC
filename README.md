# Securities Master Data Lakehouse — Agentic Code Generation Pipeline

An AI-powered pipeline that generates production-ready Databricks notebooks, tests, and
documentation from a single YAML specification file. Powered by Claude Sonnet 4.6 and
orchestrated through a 7-stage BMAD agent loop with human approval gates at every stage.

---

## Overview

You describe your data pipeline in `request.yaml`. The agents do the rest:

1. **BA Agent** reads the spec and produces 6 YAML spec files (Bronze, Silver, Gold schemas + DQ rules)
2. **Architect Agent** reviews and finalizes the specs
3. **Developer Agent** writes the Databricks notebooks (PySpark for Bronze, SQL for Silver/Gold)
4. **QA Agent** writes pytest test files for all three layers
5. **Doc Agent** generates data lineage, HLD, and Genie SQL comments
6. **Deploy Agent** commits all outputs and raises a GitHub PR

After merging the PR you run `sml deploy` to push to Databricks and `sml run` to trigger the pipeline.

---

## Architecture

```
request.yaml
     │
     ▼
 BA Agent ──────────────► 6 YAML spec files
     │                    (bronze/silver/gold ×
     │                     tables.yaml + rules.yaml)
     ▼
Architect Agent ─────────► reviewed + finalized specs
     │
     ▼
Developer Agent ──────────► project/notebooks/
     │                       ├── bronze_ingest.py   (PySpark)
     │                       ├── silver_conform.sql (SQL)
     │                       └── gold_build.sql     (SQL)
     ▼
 QA Agent ────────────────► project/tests/
     │                       ├── test_bronze.py
     │                       ├── test_silver.py
     │                       └── test_gold.py
     ▼
Doc Agent ────────────────► project/docs/
     │                       ├── lineage.md
     │                       ├── hld.md
     │                       └── genie_comments.sql
     ▼
Deploy Agent ─────────────► GitHub PR
     │
     ▼  (merge PR)
Databricks ───────────────► Bronze → Silver → Gold
```

Each stage pauses for human approval before advancing. Type `approve` to proceed or
`reject: <reason>` to re-run the stage with feedback.

---

## Folder Structure

```
StateStreetDemo/
├── agentic/                        # Agent orchestration framework
│   ├── agents/
│   │   ├── pipeline.py             # State machine — drives agents through stages
│   │   ├── cli.py                  # `sml` CLI entry point (Click)
│   │   ├── ba_agent.py             # Business Analyst Agent
│   │   ├── architect_agent.py      # Architect Agent
│   │   ├── code_gen_agent.py       # Developer Agent (notebook generation)
│   │   ├── test_gen_agent.py       # QA Agent (pytest generation)
│   │   ├── doc_gen_agent.py        # Doc Agent (lineage, HLD, Genie SQL)
│   │   ├── deploy_agent.py         # Deploy Agent (git branch, commit, PR)
│   │   ├── debug_agent.py          # Debug Agent (uses Databricks MCP logs)
│   │   ├── setup_agent.py          # Setup Agent (provisions Databricks workspace)
│   │   └── validate_spec_agent.py  # Spec validation (CI use)
│   └── pyproject.toml              # Installs the `sml` CLI command
│
├── project/                        # All project-specific content (agents write here)
│   ├── notebooks/                  # Generated Databricks notebooks
│   │   ├── bronze_ingest.py        # Bronze: CSV → Delta (PySpark, MERGE INTO)
│   │   ├── silver_conform.sql      # Silver: DQ checks + SCD2 (Databricks SQL)
│   │   └── gold_build.sql          # Gold: single wide table (all subtypes flattened, Databricks SQL)
│   │
│   ├── tests/                      # Generated pytest test files
│   │   ├── test_bronze.py          # Bronze ingestion tests
│   │   ├── test_silver.py          # Silver DQ + SCD2 tests
│   │   └── test_gold.py            # Gold mart join/aggregate tests
│   │
│   ├── docs/                       # Generated documentation
│   │   ├── lineage.md              # Column-level data lineage map
│   │   ├── hld.md                  # High-Level Design document
│   │   └── genie_comments.sql      # COMMENT ON TABLE/COLUMN for Databricks Genie
│   │
│   ├── demo/                       # Demo materials
│   │   └── demo-overview.md        # End-to-end demo walkthrough
│   │
│   ├── specs/                      # Specification reference templates
│   │   └── SPEC-TEMPLATE.md        # Template showing expected YAML format
│   │
│   ├── src/                        # Python source code (not generated — hand-maintained)
│   │   ├── common/
│   │   │   └── setup_utils.py      # SDK-based SetupManager (local, no Spark required)
│   │   ├── ingestion/
│   │   │   ├── bronze_loader.py    # Bronze ingestion logic
│   │   │   ├── schema_drift.py     # Schema drift detection + rescue column handling
│   │   │   └── source_reader.py    # CSV reader with configurable options
│   │   └── dq/                     # Data quality framework modules
│   │
│   ├── skills/                     # Knowledge base read by agents before generating
│   │   ├── build_pipeline.md       # How to structure notebooks + jobs
│   │   ├── data_dictionary.md      # Securities domain terminology
│   │   ├── data_engineering.md     # PySpark and SQL patterns for Databricks
│   │   ├── dq_patterns.md          # DQ rule patterns (null check, range, referential)
│   │   ├── known_issues.md         # Common pitfalls and workarounds
│   │   ├── lineage_governance.md   # Unity Catalog lineage tagging patterns
│   │   ├── ontology.md             # Securities domain ontology
│   │   ├── orchestration.md        # Databricks Workflows + Asset Bundle patterns
│   │   ├── performance.md          # Delta Lake optimization (Z-ORDER, liquid clustering)
│   │   ├── schema_drift.md         # Schema evolution handling
│   │   ├── security_governance.md  # Row/column-level security patterns
│   │   ├── streaming_cdc.md        # Change Data Capture patterns
│   │   └── testing_patterns.py     # Pytest patterns for Databricks notebooks
│   │
│   └── use-cases/
│       └── securities-master/
│           ├── request.yaml        # The ONLY file you author — everything else is generated
│           ├── known_issues.md     # Use-case-specific known issues
│           └── specs/              # Generated by BA Agent (6 YAML files)
│               ├── bronze/
│               │   ├── tables.yaml # Bronze table schemas (one entry per source CSV)
│               │   └── rules.yaml  # Bronze ingestion rules
│               ├── silver/
│               │   ├── tables.yaml # Silver table schemas (DQ + SCD2 config)
│               │   └── rules.yaml  # Silver DQ rules (128 rules from catalog)
│               └── gold/
│                   ├── tables.yaml # Gold mart schemas (dim_* and fact_* tables)
│                   └── rules.yaml  # Gold validation rules
│
├── .env                            # Credentials — gitignored, never commit
├── .mcp.json                       # Databricks Genie MCP server config — gitignored
├── .gitignore                      # Ignores .env, .mcp.json, agent_state.yaml, __pycache__
├── CLAUDE.md                       # Coding standards and naming conventions (read by all agents)
├── config.yml                      # Global pipeline config (catalog, model, volume path)
└── requirements.txt                # Python dependencies
```

---

## Agent Pipeline

| Stage | Agent | Input | Output |
|-------|-------|-------|--------|
| `ba_review` | BA Agent | `request.yaml` + `skills/` | 6 YAML spec files |
| `architect_review` | Architect Agent | 6 spec YAMLs | Reviewed + finalized specs |
| `code_gen` | Developer Agent | Finalized specs | `notebooks/bronze_ingest.py`, `silver_conform.sql`, `gold_build.sql` |
| `qa` | QA Agent | Generated notebooks | `tests/test_bronze.py`, `test_silver.py`, `test_gold.py` |
| `doc` | Doc Agent | Generated notebooks | `docs/lineage.md`, `docs/hld.md`, `docs/genie_comments.sql` |
| `deploy` | Deploy Agent | All outputs | GitHub PR with all generated files |

The pipeline state is persisted in `use-cases/<name>/agent_state.yaml` (gitignored).
If you interrupt a run, `sml generate` resumes from where you left off.

---

## Security Master Data Model

24-entity class-table inheritance hierarchy. The join key across all tables is `product_id`.

```
Product  (id, rf_type, type, status, settlement_type, description, issue_date, issue_price, sub_type)
  ├── Debt  (par_value, endre_status, share_class, series, security_code)
  │     ├── Bond  (inflation_linked, inflation_lag, reference_index_rate, conversion_rule)
  │     │     └── Muni  (pledge_type)
  │     └── PoolBackedSecurity  (weighted_average_coupon, net_coupon, reference_index_rate,
  │                               weighted_average_maturity)
  ├── Fund  (endre_status, share_class, mutual_fund_load_type, mutual_fund_type)
  ├── Right  (exercise_style, strike_price, option_type)
  ├── ListedDerivative  (contract_year, contract_month, contract_size, last_trade_date)
  │     ├── Option  (option_type, exercise_style, strike_price, margin_style)
  │     └── Future  (first_delivery_datetime_utc, last_delivery_datetime_utc, valuation_method)
  └── Stock  (ipo_date, stock_class, has_voting_rights, depository_type)
        ├── CommonStock  (reference_obligation)
        └── PreferredStock  (par_value, dividend_right, is_perpetual)

Supporting entities:
  LegalEntity          (id, legal_name, formation_date, is_fin_entity, legal_structure)
  Identifiers          (id, bloomberg_id, bloomberg_ticker, cusip, isin)
  Classification       (id, legacy_product_id)
  ProductRating        (id, product_id FK, rating_agency, rating_code, effective_from_date)
  ProductRatingType    (id, rating_type_code, duration)
  Coupon               (id, product_id FK, coupon_rule, coupon_type, is_auto_callable, call_feature)
  PrincipalRedemption  (id, product_id FK, amount_outstanding, is_auto_callable, call_feature)
  Currency             (id, name, is_deprecated, is_crypto_currency)
  TickLadderScale      (id, lower_bound, tick_size)
  Tick                 (id, scale_id FK, tick_size, price_range)
  Series               (id, product_id FK→ListedDerivative, description)
```

Key associations:
- `Product.id → LegalEntity.id` (issuer, 1:1)
- `Product.id → Identifiers.id` (symbology, 1:0..1)
- `Product.id → ProductRating.product_id` (ratings, 1:many)
- `Bond.id → Coupon.product_id` (coupons, 1:many)
- `Bond.id → PrincipalRedemptionProvision.product_id` (1:1)
- `ListedDerivative.id → Series.product_id` (series, 1:many)

---

## Unity Catalog Structure

| Layer | Catalog | Schema | Example Table |
|-------|---------|--------|---------------|
| Bronze | `statestreet` | `b_statestreet` | `statestreet.b_statestreet.product` |
| Silver | `statestreet` | `s_statestreet` | `statestreet.s_statestreet.product` |
| Silver Rejects | `statestreet` | `s_statestreet` | `statestreet.s_statestreet.product_rejects` |
| Gold | `statestreet` | `g_statestreet` | `statestreet.g_statestreet.securities_master` |
| Volume | `statestreet` | `securities_master` | `/Volumes/statestreet/securities_master/raw_files/` |

29 source CSV files live in the Volume. Bronze ingestion reads from this path.

Gold produces a **single wide table** (`g_statestreet.securities_master`) — all product subtype
attributes flattened and joined on `product_id`. No separate dim/fact tables.

SCD2 tracking (Silver layer): `product`, `legal_entity`, `product_rating`
— tracked with `effective_start_date`, `effective_end_date`, `is_current` columns.

---

## Prerequisites

| Tool | Install | Purpose |
|------|---------|---------|
| Python 3.9+ | [python.org](https://python.org) | Runtime |
| Databricks CLI | `pip install databricks-cli` | Bundle deploy |
| GitHub CLI | `brew install gh` | PR creation |
| `gh auth login` | After installing gh | Authenticate to GitHub |

---

## Quick Start

### Step 1 — Configure credentials

Copy and fill in your credentials:

```bash
# Edit .env with your values:
DATABRICKS_HOST=https://dbc-xxxxxxxx.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
DATABRICKS_WAREHOUSE_ID=<your-warehouse-id>
```

> Find `DATABRICKS_WAREHOUSE_ID` in Databricks UI: SQL Warehouses → your warehouse → Connection Details → HTTP Path (the ID is the last segment).

### Step 2 — Install the CLI

```bash
cd agentic
pip install -e .
sml --help    # verify installation
```

### Step 3 — Provision Databricks workspace

```bash
# Creates catalog, schemas, volume, and uploads your CSVs
sml setup --local-data-dir ./data/
```

This runs entirely from your laptop using the Databricks SDK — no cluster or Spark required.
It creates:
- `statestreet` catalog
- `b_statestreet`, `s_statestreet`, `g_statestreet`, `securities_master` schemas
- `/Volumes/statestreet/securities_master/raw_files/` volume
- Uploads all CSVs from `--local-data-dir`

### Step 4 — Generate code

```bash
sml generate --use-case securities-master
```

The pipeline will walk through all 7 stages. At each stage you will see the agent output
and be prompted:

```
[BA AGENT] Output shown above.
Approve to continue, or type 'reject: <reason>' to re-run this stage.
> approve
```

### Step 5 — Deploy

After the Deploy Agent creates and you merge the GitHub PR:

```bash
# Validate and deploy the Databricks Asset Bundle
sml deploy --use-case securities-master

# Trigger the Bronze ingestion job
sml run --use-case securities-master --job bronze_ingest_job

# Check job status
sml status --use-case securities-master --job bronze_ingest_job
```

---

## All CLI Commands

```bash
sml setup    --local-data-dir <path>        # Provision Databricks workspace
sml generate --use-case <name>              # Run full 7-stage agent pipeline
sml deploy   --use-case <name>              # Deploy Databricks Asset Bundle
sml run      --use-case <name> --job <job>  # Trigger a Databricks job
sml status   --use-case <name> --job <job>  # Check job run status
sml debug    --use-case <name> --job <job>  # Diagnose failing job via MCP logs
sml validate --use-case <name>              # Validate spec YAMLs against CLAUDE.md
```

---

## Authoring a Use Case

The only file you write by hand is `project/use-cases/<name>/request.yaml`. Example:

```yaml
use_case_name: securities-master

description: |
  Ingest 29 security CSV files from Databricks Volume into a Bronze/Silver/Gold medallion
  lakehouse. Data model uses class-table inheritance — every security has one row in
  'product' plus one row in each applicable subtype table. Join key: product_id.

source:
  type: volume
  path: /Volumes/statestreet/securities_master/raw_files/
  format: csv
  delimiter: ","
  header: true
  tables:
    - product
    - bond
    - stock
    - fund
    # ... (all 29 tables)

catalog:
  name: statestreet
  bronze_schema: b_statestreet
  silver_schema:  s_statestreet
  gold_schema:    g_statestreet

scd2_tables:
  - product
  - legal_entity
  - product_rating

gold_table: securities_master    # single wide table — all subtypes flattened, joined on product_id

code_generation:
  bronze: python
  silver: sql
  gold:   sql
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABRICKS_HOST` | Yes | Workspace URL |
| `DATABRICKS_TOKEN` | Yes | Personal Access Token |
| `DATABRICKS_WAREHOUSE_ID` | Yes (for `sml setup`) | SQL Warehouse for DDL execution |

> **Note:** `ANTHROPIC_API_KEY` is **not required** when using Claude Code IDE — the IDE handles
> all AI calls. It is only needed if you run `sml generate` as a standalone CLI tool.

Credentials are loaded from `.env` at the repo root. The `.env` file is gitignored — never commit it.

---

## MCP Server (Databricks Genie)

The Debug Agent uses the Databricks Genie MCP server to fetch live job logs without guessing.
Configuration is in `.mcp.json` (gitignored). To enable:

```json
{
  "mcpServers": {
    "databricks-genie": {
      "command": "npx",
      "args": ["-y", "databricks-genie-mcp"],
      "env": {
        "DATABRICKS_HOST": "...",
        "DATABRICKS_TOKEN": "...",
        "GENIE_SPACE_ID": "..."
      }
    }
  }
}
```

---

## Git / Contributing

- Never commit directly to `main` — always raise a PR
- Branch naming: `feat/<use-case>-<stage>` (e.g. `feat/securities-master-silver`)
- `agent_state.yaml` is gitignored — do not commit pipeline state
- `.env` and `.mcp.json` are gitignored — never commit credentials
- The Deploy Agent automates PR creation via `gh pr create` (requires `gh auth login`)
