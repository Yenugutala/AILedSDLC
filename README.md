# AI-Led Data Lifecycle (AI-DLC) — Securities Master Lakehouse

> A Jira ticket triggers an AI agent pipeline that produces deployed, production-ready Databricks
> code. Every agent is grounded in real codebase knowledge, live Databricks schema, and domain
> skills. Nothing is hardcoded — all knowledge is retrieved from ChromaDB at runtime.

---

## The Use Case

**Client:** State Street Corporation

**Problem:** Adding a new analytics column to the Gold layer requires engineers to manually trace
data through Bronze → Silver → Gold, write notebooks, tests, and docs, raise a PR, deploy, and
run jobs. This takes **days per change**.

**Solution:** A Jira ticket drives an AI agent pipeline that does all of it in one session.

**ONE ticket. One `sml demo` run. Jira → live data in Databricks.**

---

## System Architecture

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                         AI-DLC AGENT PIPELINE                                   ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                  ║
║  EXTERNAL SYSTEMS              KNOWLEDGE LAYER              AGENT BEATS          ║
║  ───────────────               ───────────────              ───────────          ║
║                                                                                  ║
║  ┌──────────┐                 ┌─────────────────────┐                           ║
║  │  JIRA    │────────────────►│   ChromaDB          │                           ║
║  │ Tickets  │  ticket text    │                     │   Beat 1: BA (Pull)       ║
║  └──────────┘  indexed        │  Collection:        │   Beat 2: BA (Clarify)    ║
║                               │  "ai-dlc"           │   Beat 3: Verify ×3       ║
║  ┌──────────┐                 │                     │   Beat 4: Dev + QA        ║
║  │DATABRICKS│────────────────►│  • Notebooks        │   Beat 4b: Deploy         ║
║  │   DATA   │ Data profiling  │  • Spec YAMLs       │   Beat 5: Genie           ║
║  │ CATALOG  │ + column        │  • Jira ticket      │   Beat 6: Observe         ║
║  └──────────┘  descriptions   │  • CLAUDE.md        │                           ║
║                               │  • Skills           │                           ║
║  ┌──────────────────────────┐ │                     │   ⏸ Human gate after      ║
║  │  project/skills/         │ │  Collection:        │      each beat            ║
║  │  • Domain knowledge     │►│  "data_catalog"     │                           ║
║  │  • DQ patterns          │ │                     │                           ║
║  │  • Engineering guides   │ │  • AI-generated     │                           ║
║  │  • Governance standards │ │    column desc.     │                           ║
║  │  + more skill files     │ │  • Data profile     │                           ║
║  └──────────────────────────┘ │  • Join key map     │                           ║
║                               └─────────────────────┘                           ║
║                                                                                  ║
║  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ ║
║  │CLAUDE.md │  │ GitHub   │  │Databricks│  │  Genie   │  │ Dashboard        │ ║
║  │Standards │  │ PR + Git │  │Bundle CLI│  │ REST API │  │ localhost:8765   │ ║
║  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════════════════╝
```

---

## Knowledge Layer — What Goes Into ChromaDB

### Catalog Priority (used in Beat 3 spec validation)

```
sml profile ran?  → data_catalog    (BEST: AI descriptions from real data + join map)
sml schema ran?   → schema_catalog  (GOOD: raw column names + types from INFORMATION_SCHEMA)
neither ran?      → Greenfield mode (Claude reasons from ticket text alone)
```

### Collection: `ai-dlc` (Codebase + Skills)

Built by `sml index`. Every file is chunked (800 chars, 20% overlap) and embedded with
`sentence-transformers/all-MiniLM-L6-v2`.

```
project/skills/                     ← All domain knowledge files (14+ skill files)
project/notebooks/*.py / *.sql      ← Generated notebooks (actual code patterns)
project/use-cases/**/*.yaml         ← All spec YAMLs (bronze, silver, gold tables + rules)
project/docs/*.md                   ← Generated documentation
CLAUDE.md                           ← Coding standards and naming conventions
Jira ticket text                    ← Indexed automatically after Beat 1 ticket selection
```

### Collection: `data_catalog` (AI-Profiled Data — Preferred)

Built by `sml profile`. Profiles every column from actual Databricks data. Generates LLM
descriptions from real data values, not static documentation. Detects join relationships
automatically by finding columns that appear across multiple tables.

```
Per column stored:
  • null %              ← from COUNT / COUNT(col) SQL
  • cardinality         ← COUNT(DISTINCT col) SQL
  • sample values       ← top-5 values or MIN/MAX for numeric columns
  • AI description      ← Claude Haiku reads the profile → writes business description
  • is_join_key         ← True if column appears in 2+ tables
  • join_tables         ← list of tables it links

Also produces join_map.yaml:
  Documents how tables are related (auto-detected from shared column names)
```

### Collection: `schema_catalog` (Fallback — Raw Schema)

Built by `sml schema`. Queries `INFORMATION_SCHEMA.COLUMNS` for the full live Databricks schema.

```
Each document = one column from the live Databricks lakehouse:
  Text:     "<catalog>.<schema>.<table>.<column>: <comment or data_type>"
  Metadata: { layer: "silver|bronze|gold", table: "...", column: "...", data_type, nullable }

Used by Beat 3 Check 3 to determine:
  Does this column already exist in the lakehouse?
  If yes → surface it from the existing layer.
  If no  → propose a new column with an inferred definition.
```

---

## Agent Internal Flows

### Beat 1 — BA Agent: Pull Ticket

```
┌─────────────────────────────────────────────────────────────────┐
│  INPUT                                                          │
│  Jira credentials from .env                                     │
│  JIRA_URL / JIRA_USERNAME / JIRA_API_TOKEN                      │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 1: Fetch open tickets                                     │
│  GET /rest/api/3/search?jql=project=SCRUM AND status=Open      │
│  → Returns list of issues with summary, status, priority        │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 2: Display ticket list to user                            │
│  User selects ticket number from the list                       │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 3: Fetch full ticket                                      │
│  GET /rest/api/3/issue/{ticket_key}                             │
│  → Returns ADF (Atlassian Document Format) description          │
│  → ADF parser extracts structured requirements table            │
│     Outputs: TicketContext { key, summary, requirements[] }     │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 4: Index ticket into ChromaDB                             │
│  ticket_text = summary + description + all REQ text             │
│  → chunked + upserted into "ai-dlc" collection                  │
│  → enables downstream agents to search ticket requirements      │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
                  ⏸ HUMAN GATE
            Engineer reviews parsed REQs
                 Types: approve
```

---

### Beat 2 — BA Agent: Clarify

```
┌─────────────────────────────────────────────────────────────────┐
│  INPUT: TicketContext (ticket key + parsed requirements)         │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 1: Vector search ChromaDB "ai-dlc"                        │
│  Query: ticket summary + all requirement text concatenated      │
│                                                                 │
│  Top results retrieved (ranked by semantic similarity):         │
│  • Relevant skill files (domain terminology, DQ patterns)       │
│  • Matching spec YAMLs (bronze, silver, gold layer schemas)     │
│  • Existing generated notebooks (code patterns already in use)  │
│  • Standards from CLAUDE.md                                     │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 2: Build Claude prompt                                    │
│  System: "You are a BA Agent. Ask the ONE most important        │
│           clarification question needed to safely build this.   │
│           You have the following codebase context."             │
│  User:   Ticket summary + requirements                          │
│          + retrieved codebase chunks                            │
│                                                                 │
│  Claude reasons from the retrieved knowledge to generate        │
│  a grounded clarification question — not a generic one          │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 3: Post to Jira                                           │
│  POST /rest/api/3/issue/{ticket_key}/comment                    │
│  Body: Clarification question written to the ticket             │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
                  ⏸ HUMAN GATE
         Engineer reads question, types answer in terminal
                 Types: approve
```

---

### Beat 3 — Verify Agents (3 Checks)

```
┌─────────────────────────────────────────────────────────────────┐
│  INPUT: TicketContext + clarification Q&A                       │
└──────────────────────┬──────────────────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
    CHECK 1        CHECK 2        CHECK 3
  BA Agent       Architect      Validate Spec
  (Clarity)     (Conformance)     Agent

┌────────────┐  ┌────────────┐  ┌──────────────────────────────┐
│ Input:     │  │ Input:     │  │ Input: ticket + data catalog  │
│ REQs +     │  │ ticket +   │  │                              │
│ clarif. A  │  │ CLAUDE.md  │  │ STEP A: Reset gold spec      │
│            │  │            │  │  gold/tables.yaml → empty    │
│ Claude Q:  │  │ Claude Q:  │  │  (ensures fresh demo run)    │
│ "Are reqs  │  │ "Does this │  │                              │
│ clear and  │  │ conform to │  │ STEP B: Semantic search       │
│ specific   │  │ naming,    │  │  Query: requirement text      │
│ enough?"   │  │ language   │  │  Collection: "data_catalog"  │
│            │  │ split, no  │  │  or "schema_catalog"         │
│ Output:    │  │ dim_/fact_ │  │  → finds similar columns in  │
│ PASS / FAIL│  │ prefix?"   │  │    the live lakehouse        │
│            │  │            │  │                              │
│            │  │ Output:    │  │ STEP C: Call Claude           │
│            │  │ PASS / FAIL│  │  System: "Identify the ONE   │
│            │  │            │  │   primary metric required.   │
│            │  │            │  │   Surface from silver or     │
└────────────┘  └────────────┘  │   create new?"               │
                                │  User: REQs + schema context  │
                                │                              │
                                │  Claude decides:             │
                                │  • Column exists in silver   │
                                │    → action = "surface"      │
                                │  • Column is new             │
                                │    → action = "create"       │
                                │                              │
                                │ STEP D: Check gold spec      │
                                │  Is the required column in   │
                                │  gold/tables.yaml?           │
                                │  → NO (was reset) → FAIL     │
                                │                              │
                                │ STEP E: Auto-fix             │
                                │  Add column definition to    │
                                │  gold/tables.yaml            │
                                │                              │
                                │ STEP F: Re-check → PASS      │
                                └──────────────────────────────┘
                       │
                       ▼
              All 3 checks PASS
                       │
                       ▼
         Build-ready stamp posted to Jira ticket
                       │
                       ▼
                  ⏸ HUMAN GATE
              Types: approve
```

---

### Beat 4 — Developer Agent + QA Agent

```
┌─────────────────────────────────────────────────────────────────┐
│  INPUT: Updated gold/tables.yaml + ticket requirements          │
└──────────────────────┬──────────────────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
  DEVELOPER AGENT             QA AGENT

┌────────────────────────┐  ┌─────────────────────────────────┐
│ STEP 1: Vector search  │  │ STEP 1: Reads generated SQL     │
│  Collection: "ai-dlc"  │  │                                 │
│  Query: gold spec      │  │ STEP 2: Vector search           │
│         + ticket       │  │  Collection: "ai-dlc"           │
│                        │  │  Query: test patterns for       │
│  Top results:          │  │   the generated layer           │
│  • Engineering skills  │  │  → testing_patterns.md          │
│    (MERGE INTO, CASE)  │  │  → dq_patterns.md               │
│  • Known issues /      │  │                                 │
│    column mappings     │  │ STEP 3: Build Claude prompt     │
│  • Existing notebooks  │  │  "Generate pytest for all REQs  │
│    (existing patterns) │  │   using these test patterns"    │
│  • Domain skills       │  │                                 │
│                        │  │ STEP 4: Generates test file     │
│ STEP 2: Build prompt   │  │  Covers each requirement with   │
│  System: "You are a    │  │  positive + negative test cases │
│  Developer Agent. Gen  │  │                                 │
│  Gold SQL per CLAUDE.md│  └─────────────────────────────────┘
│  + skills context"     │
│                        │
│ STEP 3: Claude writes  │
│  Gold SQL notebook:    │
│  • CREATE OR REPLACE   │
│  • MERGE INTO pattern  │
│  • TBLPROPERTIES       │
│    (iceberg UniForm)   │
│  • COMMENT ON TABLE    │
│  • COMMENT ON COLUMN   │
│    (Genie-readable)    │
└────────────────────────┘
                       │
                       ▼
                  ⏸ HUMAN GATE
              Types: approve
```

---

### Beat 4b — Deploy Agent

```
┌─────────────────────────────────────────────────────────────────┐
│  INPUT: All files in project/ (generated notebooks + spec)      │
│  No LLM calls — pure tooling                                    │
└──────────────────────┬──────────────────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
       STEP A        STEP B       STEP C
       Git + PR    Bundle Deploy  Job Trigger

┌────────────┐  ┌────────────┐  ┌────────────────────────────┐
│ GitPython  │  │ Databricks │  │ databricks bundle run      │
│            │  │ CLI        │  │ <job_name> --no-wait        │
│ git add    │  │            │  │                            │
│ git commit │  │ bundle     │  │ Returns immediately —      │
│ git push   │  │ validate   │  │ fire-and-forget submission  │
│            │  │ ↓          │  │                            │
│ gh pr      │  │ bundle     │  │ Monitor progress in        │
│ create     │  │ deploy     │  │ Databricks UI → Jobs       │
│ → PR URL   │  │            │  │                            │
└────────────┘  └────────────┘  └────────────────────────────┘
                       │
                       ▼
                  ⏸ HUMAN GATE
              Types: approve
```

---

### Beat 5 — Genie (Natural Language → Live Data)

```
┌─────────────────────────────────────────────────────────────────┐
│  INPUT: Natural language question about the Gold table          │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  Databricks Genie REST API                                      │
│  POST /api/2.0/genie/spaces/{space_id}/start-conversation      │
│  Body: { message: "<natural language question>" }               │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  Genie AI (hosted in Databricks)                               │
│  • Reads COMMENT ON TABLE/COLUMN written by Developer Agent    │
│  • Those comments teach Genie about the table's business domain│
│  • Generates SQL from the natural language question            │
│  • Executes against the live Gold table                        │
│  • Returns result rows                                         │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│  RESULT: Live rows from the Gold table                          │
│  End-to-end: Jira ticket → deployed code → queryable live data  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Coding Standards (CLAUDE.md — read by every agent)

| Rule | Standard |
|------|----------|
| Bronze notebooks | **PySpark** (Python) — CSV → Delta, MERGE INTO, rescuedData |
| Silver notebooks | **SQL** (Databricks SQL) — DQ assertions + SCD2 MERGE |
| Gold notebooks | **SQL** (Databricks SQL) — JOIN + window functions |
| Unity Catalog | 3-part names: `catalog.schema.table` |
| Idempotency | Always `MERGE INTO` — never `INSERT OVERWRITE` |
| Timestamps | `current_timestamp()` — never `NOW()` or `GETDATE()` |
| Regex | `RLIKE` — never `REGEXP_LIKE` or `LIKE` with wildcards |
| Iceberg | All gold tables: `TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')` |
| Gold table name | No `dim_`/`fact_` prefix — single wide table per use case |
| Columns | `snake_case`, metadata columns `_`-prefixed |
| SCD2 | `effective_start_date`, `effective_end_date`, `is_current` on Silver + Gold |

**Enforced at Beat 3 Check 2.** If the proposed change violates these, Check 2 fails.

---

## Data Flow: Bronze → Silver → Gold

```
/Volumes/<catalog>/<schema>/<volume>/ (source files)
           │
           │  Bronze Notebook (PySpark)
           │  • Auto-detect schema from source file headers
           │  • MERGE INTO on primary key (idempotent)
           │  • Add pipeline metadata: _ingestion_ts, _source_file, _batch_id, _row_hash
           │  • rescuedData column catches schema drift automatically
           ▼
<catalog>.b_<schema>.* (Bronze Delta tables)
           │
           │  Silver Notebook (Databricks SQL)
           │  • DQ rules evaluated as SELECT assertions
           │  • Failed rows → *_rejects table with failure_reason
           │  • SCD2 MERGE: effective_start_date, effective_end_date, is_current
           │  • _dq_rule_version tracks which rule version was applied
           ▼
<catalog>.s_<schema>.* (Silver tables + _rejects tables)
           │
           │  Gold Notebook (Databricks SQL)
           │  • JOIN across Silver tables on shared keys
           │  • Filter: is_current = TRUE (active versions only)
           │  • Business metrics derived from Silver source columns
           │  • COMMENT ON TABLE/COLUMN (Genie AI context)
           ▼
<catalog>.g_<schema>.<use_case_name> (single wide Gold table)
           │
           │  Databricks Genie AI/BI
           │  Natural language → SQL → live results
           ▼
Business analysts query in plain English — no SQL required
```

---

## Folder Structure

```
AILedSDLC/
│
├── agentic/                              ← Agent orchestration framework
│   ├── agents/
│   │   ├── cli.py                        ← sml CLI: demo / index / schema / profile / deploy / ...
│   │   ├── check_agents.py               ← Beat 3: clarity + arch + validate spec
│   │   ├── clarify_agent.py              ← Beat 2: codebase-grounded Q generation
│   │   ├── deploy_agent.py               ← Beat 4b: git + bundle deploy + job trigger
│   │   ├── schema_discovery_agent.py     ← Indexes live Databricks schema → ChromaDB
│   │   ├── data_profiler_agent.py        ← Profiles columns + AI descriptions → ChromaDB
│   │   ├── debug_agent.py                ← Diagnoses failing jobs via Databricks MCP
│   │   └── ...
│   │
│   └── demo/
│       ├── harness.py                    ← Main orchestrator: runs all 6 beats
│       ├── beats/                        ← One file per beat
│       ├── knowledge/
│       │   ├── indexer.py                ← Chunks + embeds all files → ChromaDB "ai-dlc"
│       │   └── knowledge_agent.py        ← Post-demo Q&A REPL
│       └── tools/
│           ├── jira_client.py            ← Jira REST + ADF parser
│           ├── schema_catalog.py         ← Vector search over "schema_catalog" collection
│           ├── data_catalog.py           ← Vector search over "data_catalog" collection
│           ├── codebase.py               ← Gold spec YAML reader/writer
│           └── genie_client.py           ← Databricks Genie REST API
│
├── project/
│   ├── notebooks/                        ← Generated Databricks notebooks
│   ├── skills/                           ← Domain knowledge (all indexed into ChromaDB)
│   │   ├── known_issues.md               ← Column mappings, pitfalls
│   │   └── ... (14+ skill files total)
│   └── use-cases/securities-master/
│       └── specs/                        ← Spec YAMLs (bronze, silver, gold)
│
├── CLAUDE.md                             ← Coding standards (read by every agent)
├── README.md                             ← This file: architecture + internal flows
├── AI_DLC.md                             ← Client-facing AI-DLC alignment document
└── DEMO_SCRIPT.md                        ← Presenter reference: what to type at each gate
```

---

## CLI Reference

```bash
# Before demo (recommended order)
sml index          # Index codebase + skills + specs → ChromaDB "ai-dlc"
sml schema         # Index live Databricks schema → ChromaDB "schema_catalog" (fallback)
sml profile        # Profile tables + AI descriptions + join map → ChromaDB "data_catalog" (preferred)

# Demo
sml demo           # Run the full 6-beat live demo

# Pipeline (standalone)
sml generate --use-case <name>
sml deploy   --use-case <name>
sml run      --use-case <name> --job <job_name>
sml status   --use-case <name> --job <job_name>
sml debug    --use-case <name> --job <job_name>
sml validate --use-case <name>

# Knowledge REPL
sml ask            # Q&A against indexed codebase
```

---

## Pre-Demo Checklist

```bash
# 1. Install
cd agentic && pip install -e . && cd ..

# 2. Credentials in .env
DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_WAREHOUSE_ID
JIRA_URL / JIRA_USERNAME / JIRA_API_TOKEN / GENIE_SPACE_ID
ANTHROPIC_API_KEY

# 3. Index knowledge base (run in order)
sml index      # codebase + skills + specs → "ai-dlc"
sml schema     # raw schema from INFORMATION_SCHEMA → "schema_catalog"   (fallback)
sml profile    # data profiling + AI descriptions + join map → "data_catalog" (preferred)

# 4. Run demo
sml demo
```

**See [DEMO_SCRIPT.md](DEMO_SCRIPT.md) — what to type at each gate.**
**See [AGENT_REFERENCE.md](AGENT_REFERENCE.md) — full input/output for every agent and command.**
