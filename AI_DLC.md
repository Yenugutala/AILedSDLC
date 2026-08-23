# AI-Driven Development Lifecycle (AI-DLC)
## Securities Master Data Lakehouse — Enterprise Implementation

---

## What is AI-DLC?

The **AI-Driven Development Lifecycle (AI-DLC)** is a structured software delivery methodology where AI agents serve as primary collaborators across every phase of development — from requirements clarification through code generation, deployment, and live operations. Unlike traditional AI-assisted tools that autocomplete code in isolation, AI-DLC maintains persistent context, enforces standards, coordinates specialized agents, and gates every critical decision with explicit human approval.

In AI-DLC, humans set intent and validate outcomes. AI agents propose, execute, and explain. The result is a system that moves faster than human-only development while remaining fully auditable, traceable, and aligned with engineering standards. This implementation applies AI-DLC specifically to Databricks medallion architecture pipelines: any Jira ticket describing a data engineering requirement is automatically analyzed, validated, code-generated, deployed, and queryable — without any agent logic changes across different tickets or use cases.

---

## AI-DLC Alignment Matrix

| AI-DLC Element | This Implementation | Alignment |
|---|---|---|
| **Intent** | Jira ticket (SCRUM-5) + parsed REQ-IDs from description/table | Strong |
| **Units / Stories** | Individual requirements (REQ-01 …) treated as work items | Good |
| **Bolt / Rapid Iteration** | 7-iteration sequence (Pull → Clarify → Verify → Generate → Deploy → Query → Observe) — hours-scale, ticket-scoped | Strong |
| **Inception Phase** | Iteration 1 (pull) + Iteration 2 (codebase-grounded clarify) + Iteration 3 Checks 1–2 | Strong |
| **Construction Phase** | Iteration 3 Check 3 (spec update) + Iteration 4 (SQL + tests generation) | Strong |
| **Operations Phase** | Iteration 4b (PR → deploy → job trigger) + Iteration 5 (Genie NL→SQL on live data) | Strong |
| **Domain / Spec Design** | `gold/tables.yaml` as living, validated gold-layer contract; "surface vs create" decision | Strong |
| **Multi-Agent Team** | BA Agent, Architect Agent, Validate Spec Agent, Developer + QA Agent, Deploy Agent, Genie | Strong |
| **Human Checkpoints** | Ticket selection, clarification answer, fix-panel approval, beat-to-beat explicit "approve" gate | Strong |
| **Persistent Context / Memory Bank** | ChromaDB (`schema_catalog` + `codebase`), `gold/tables.yaml`, `CLAUDE.md` standards, Jira comments | Strong |
| **Standards / Guardrails** | `CLAUDE.md` (snake_case, gold naming, SQL layer rules), similarity threshold, architecture checks | Good |
| **Adaptive / Dynamic** | Column names, types, descriptions, surface/create, source tables, clarification questions — all derived from live Databricks schema + vector search + Claude reasoning | Excellent |
| **Governance / Traceability** | Jira comments (ADF format), build-ready stamp with 3-check results, GitHub PR, Databricks job run logs | Good |

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AI-DLC KNOWLEDGE LAYER                              │
│                                                                             │
│  ┌──────────────────────┐   ┌──────────────────────┐   ┌────────────────┐  │
│  │  ChromaDB            │   │  ChromaDB             │   │  CLAUDE.md     │  │
│  │  "codebase"          │   │  "schema_catalog"     │   │  Standards &   │  │
│  │  collection          │   │  collection           │   │  Guardrails    │  │
│  │                      │   │                       │   │                │  │
│  │  YAML specs          │   │  Live Databricks       │   │  Naming rules  │  │
│  │  Notebooks           │   │  INFORMATION_SCHEMA    │   │  Layer split   │  │
│  │  Tests               │   │  All bronze/silver/    │   │  UC structure  │  │
│  │  Docs                │   │  gold columns          │   │                │  │
│  │  CLAUDE.md           │   │  (real-time indexed)   │   │                │  │
│  └──────────────────────┘   └──────────────────────┘   └────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
         ↕ vector search              ↕ vector similarity                ↕ read
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AI-DLC AGENT TEAM                                   │
│                                                                             │
│  BA Agent          Architect Agent     Validate Spec Agent                  │
│  (clarify_agent)   (check_agents)      (check_agents)                       │
│      ↓                   ↓                    ↓                             │
│  Clarification     Architecture        "surface vs create"                  │
│  question          conformance         column reasoning                     │
│  grounded in       check against       grounded in live                     │
│  your codebase     CLAUDE.md           Databricks schema                    │
│                                                                             │
│  Developer Agent   QA Agent            Deploy Agent                         │
│  (developer_agent) (qa_agent)          (deploy_agent)                       │
│      ↓                   ↓                    ↓                             │
│  Gold SQL          pytest tests        git commit + push                    │
│  notebook          (SQL + Python)      PR + bundle deploy                   │
│                                        + job trigger                        │
└─────────────────────────────────────────────────────────────────────────────┘
         ↕ human approval gates at every phase boundary
┌─────────────────────────────────────────────────────────────────────────────┐
│                    EXTERNAL SYSTEMS INTEGRATION                              │
│                                                                             │
│   Jira (REST v3)       GitHub (gh CLI)      Databricks                      │
│   ┌────────────┐       ┌─────────────┐      ┌─────────────────────────┐    │
│   │ Read ticket│       │ Create PR   │      │ SQL warehouse            │    │
│   │ Post ADF   │       │ Merge review│      │ INFORMATION_SCHEMA query │    │
│   │ comments   │       │             │      │ Bundle deploy            │    │
│   │ Add labels │       │             │      │ Job trigger + streaming  │    │
│   │ Build-ready│       │             │      │ Genie AI/BI queries      │    │
│   │ stamp      │       │             │      │                          │    │
│   └────────────┘       └─────────────┘      └─────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## The Seven-Iteration AI-DLC Flow

### Iteration 1 — BA Agent: Pull Ticket (Inception)

The BA Agent connects to Jira via REST API v3, retrieves all open tickets from the project, and presents them for selection. Upon selection, it parses structured requirements (REQ-ID format) from the Jira description — including table-formatted requirements using ADF (Atlassian Document Format) parsing. The selected ticket and its requirements become the **intent artifact** for all downstream agents.

**Human checkpoint:** Engineer selects the ticket. No intent is assumed.

---

### Iteration 2 — BA Agent: Clarification (Inception)

The BA Agent searches the vector knowledge base (ChromaDB `codebase` collection, which includes all YAML specs, notebooks, tests, and documentation) to ground its clarification question in the actual codebase. It generates exactly one targeted question — not generic boilerplate — and posts the Q&A back to Jira as a structured ADF comment visible to all stakeholders.

**Human checkpoint:** Engineer answers the clarification question live. Answer is recorded in Jira for audit.

---

### Iteration 3 — Verify: Three-Check Gate (Inception → Construction)

Three specialized agents run in sequence, each checking a different dimension of readiness:

**Check 1 — BA Agent: Requirement Clarity**
Confirms the requirements, as enriched by the clarification answer, are specific and buildable. Claude reasons purely from the ticket text — no domain assumptions.

**Check 2 — Architect Agent: Architecture Conformance**
Verifies the proposed change aligns with `CLAUDE.md` standards: Unity Catalog structure, naming conventions (snake_case, no `dim_`/`fact_` prefix), language split (Bronze=Python, Silver/Gold=SQL). Claude reads `CLAUDE.md` dynamically — the standards file is the authority, not the agent prompt.

**Check 3 — Validate Spec Agent: Gold Spec Completeness**
This check demonstrates the core AI-DLC dynamic reasoning capability:

1. The live gold spec (`gold/tables.yaml`) is cleared to its skeleton — ensuring each run demonstrates the full cycle
2. A semantic vector search is executed against the **live Databricks INFORMATION_SCHEMA** (indexed in ChromaDB as `schema_catalog`)
3. Claude receives the real catalog entries with similarity scores and reasons: does this required column already exist in silver or bronze? Should we **surface** it (promote existing data) or **create** it (greenfield)?
4. If columns are missing, the Validate Spec Agent presents a structured fix panel showing the source table for surfaced columns or the inferred definition for new ones
5. After human approval, the spec is patched and the check reruns to confirm

**Human checkpoint:** Engineer approves or declines the proposed spec fix. A build-ready stamp with all three check results is posted to Jira.

---

### Iteration 4 — Developer + QA Agent: Code Generation (Construction)

The Developer Agent generates the Gold SQL notebook (`05_gold_build.sql`) referencing the silver layer source tables identified during Check 3. The QA Agent generates corresponding test files. Both agents read `CLAUDE.md` and the approved `gold/tables.yaml` spec — code is always spec-derived, never speculative.

**Human checkpoint:** Engineer reviews and approves generated artifacts before deployment.

---

### Iteration 4b — Deploy Agent: Publish + Deploy (Operations)

The Deploy Agent executes the full publish-to-production sequence:

- **Step A**: Stages and commits all generated files, pushes to the designated branch, and creates a GitHub Pull Request with a structured description including test plan checklist
- **Step B**: Runs `databricks bundle validate` followed by `databricks bundle deploy` — the Databricks Asset Bundle is deployed to the workspace
- **Step C**: Triggers `gold_mart_job` and streams live job output to the terminal — the gold mart table is populated with real data

Each step degrades gracefully: if the Databricks CLI is not configured, a clear advisory panel is shown with the manual equivalent command.

**Human checkpoint:** Engineer approves the deploy. This explicit gate separates code review from production deployment.

---

### Iteration 5 — Genie: Natural Language Query on Live Data (Operations)

With the gold mart populated, the engineer poses a natural language question to Databricks Genie. Genie translates the question to SQL against `statestreet.g_statestreet.securities_master` and returns real rows — not mock data, not a simulated response. This closes the AI-DLC loop: intent became code, code became deployed pipeline, deployed pipeline became queryable live data.

---

### Iteration 6 — Observe: KPI Dashboard (Feedback)

A live HTML dashboard displays agent performance metrics: token usage per agent, latency per iteration, number of Jira artifacts written, ChromaDB chunks indexed, and checks passed/failed. This is the AI-DLC feedback layer — every run produces a quantified audit of agent behavior.

---

## Dynamic Architecture — How Knowledge Drives Everything

The defining characteristic of this AI-DLC implementation is that **no business knowledge is embedded in agent code or prompts**. All domain-specific reasoning comes from three dynamic knowledge sources:

### 1. Live Databricks Schema Catalog

```
sml schema  →  schema_discovery_agent queries INFORMATION_SCHEMA.COLUMNS
            →  every column (bronze + silver + gold) indexed into ChromaDB
            →  document: "statestreet.s_statestreet.bond.net_settlement_amount: Net cash settlement amount (DECIMAL(18,6))"
            →  metadata: {layer: "silver", table: "statestreet.s_statestreet.bond", data_type: "DECIMAL(18,6)"}
```

When a ticket says "add net settlement amount for bonds", the Validate Spec Agent searches this live catalog and finds the column with similarity score 0.94 — in silver layer, already typed correctly. Claude surfaces it to gold with the exact name and type from the real schema.

When a ticket says "add country of issue" (a new concept not yet in the lakehouse), the search returns no high-confidence matches — Claude proposes a new column definition following naming conventions, which the engineer reviews before it is written to the spec.

This is functionally equivalent to how enterprise data catalog tools like Alation, Collibra, or DataHub operate — except the catalog is populated automatically from the live warehouse and reasoned over by Claude in real time.

### 2. Codebase Knowledge Base

All YAML specs, notebooks, tests, and documentation are indexed into a separate ChromaDB collection. The BA Agent searches this when generating clarification questions — ensuring questions are grounded in the actual system state rather than generic patterns. If a column already appears in bronze specs, the question references that table. If a similar computation exists in a different notebook, the question surfaces it.

### 3. CLAUDE.md Engineering Standards

`CLAUDE.md` is the authoritative standards document: Unity Catalog structure, naming conventions, language split rules, and Databricks compatibility requirements. The Architect Agent reads this file at runtime — it does not carry these standards in its prompt. Updating `CLAUDE.md` immediately changes what the Architect Agent checks and enforces, with no code changes.

---

## Deployment Configuration vs. Business Logic

Every enterprise application carries **deployment-environment configuration** — database hostnames, project keys, job names, environment-specific identifiers. These are not business logic. They belong in configuration files (`.env`, `application.yml`, `settings.json`) and change per environment. This implementation follows the same principle.

| Configuration Value | Type | Location | Rationale |
|---|---|---|---|
| Jira project key (`SCRUM`) | Environment config | `beat1_pull_ticket.py:JIRA_PROJECT` | Identifies which Jira project to read tickets from. Changes per client deployment — same as any Jira integration. |
| Git branch name (`securities-master-dev`) | Environment config | `deploy_agent.py` | The working branch for this use case. Parameterized per use-case name in multi-use-case setups. |
| Use-case folder (`securities-master`) | Environment config | `codebase.py` | The folder name for this pipeline. Passed as parameter to all `sml` CLI commands. |
| Databricks job key (`gold_mart_job`) | Environment config | `deploy_agent.py` | The bundle resource key for the gold mart job. Defined in `databricks.yml`. |
| Similarity threshold (`0.6`) | Tunable parameter | `check_agents.py` | Controls the sensitivity of "surface vs create" classification. Adjustable without code changes by updating the constant. |

**None of these values contain business rules, column definitions, or domain knowledge.** They are deployment coordinates — equivalent to a database connection string. The agent reasoning, Claude prompts, spec logic, and validation rules contain zero domain-specific values.

---

## What Aligns Particularly Well with AI-DLC

**1. AI proposes, human validates**
Every agent output is a proposal. The engineer answers clarifications, approves spec fixes, reviews generated code, and confirms deployment. The AI does not act autonomously on production systems.

**2. Spec-first / design-before-code**
`gold/tables.yaml` is validated and human-approved before any SQL is generated. This is the data engineering equivalent of Domain Design → Logical Design → Code — the foundational AI-DLC principle.

**3. Grounded, dynamic reasoning**
No column names or types appear anywhere in agent code or prompts. Everything is derived from live INFORMATION_SCHEMA data via vector similarity, with Claude reasoning over real catalog entries. Different tickets produce different outputs from the same agent logic.

**4. Multi-agent specialization with shared context**
Six specialized agents each have a distinct responsibility and a distinct "voice" — but they all read from the same knowledge layer (ChromaDB + `CLAUDE.md` + `gold/tables.yaml`). This is the AI-DLC shared memory bank pattern.

**5. End-to-end closure into real operations**
The loop closes: intent (Jira ticket) → spec (YAML) → code (SQL notebook) → deployment (Databricks bundle) → live data (gold mart table) → natural language query (Genie). No manual steps between generated code and queryable data.

**6. Human-in-the-loop at the high-value decision points**
Approval gates are not friction — they are deliberate checkpoints placed at phase boundaries where human judgment adds the most value: accepting intent, answering domain questions, approving spec changes, and releasing to production. Routine agent work runs without interruption.

---

## Scaling to New Use Cases

Supporting a new use case (e.g., "trade-reconciliation" or "portfolio-analytics") requires **zero changes to any agent code**. The agent reasoning, Claude prompts, ChromaDB search logic, validation rules, and deploy flow are all use-case agnostic.

What changes:

```
.env
  USE_CASE_NAME=trade-reconciliation
  JIRA_PROJECT=TREC

databricks.yml
  resources:
    jobs:
      gold_mart_job:
        name: trade_reconciliation_gold_job

project/use-cases/trade-reconciliation/specs/gold/tables.yaml
  # (initially empty — agents populate it from the Jira ticket)
```

The `sml schema` command re-indexes INFORMATION_SCHEMA for the new catalog (if different). The `sml index` command re-indexes the new use-case specs. The demo flow runs identically — different ticket, different columns, same agents, same governance.

---

## Roadmap: Enhancing AI-DLC Alignment

The following improvements would deepen alignment with the full AI-DLC methodology without changing the core architecture:

| Enhancement | AI-DLC Benefit | Effort |
|---|---|---|
| Explicit Intent → Unit → Story hierarchy as YAML artifacts | Adds formal traceability from business objective down to individual REQ-IDs | Medium |
| Multi-use-case parameterization (use-case name + job key in config) | Removes the five deployment-config values from source files entirely | Low |
| Automated post-condition verification after each iteration | Agents self-verify their outputs before requesting human approval | Medium |
| Richer audit log (structured JSON per agent run, per iteration) | Feeds compliance reporting and AI-DLC maturity assessment | Low |
| DDD terminology alignment (rename "Beats" → "Bolts", add Inception/Construction labels) | Aligns terminology for stakeholders familiar with AI-DLC formal notation | Low |
| Feedback loop from Genie query results back to spec | Closes the observe → adapt cycle for continuous improvement | High |

---

## Conclusion

This implementation is a **production-ready AI-DLC realization for Databricks data engineering**. It embodies the core AI-DLC philosophy — AI as central collaborator under human oversight, rapid grounded iterations, persistent context as the memory bank, spec-driven generation, multi-agent specialization, and real operations closure — more completely than most "AI-assisted" development pipelines in production today.

The five deployment-environment configuration values are scoped exactly where they belong: as operational coordinates in configuration files, not as business logic in agent reasoning. Everything that should be dynamic is dynamic: column discovery, type inference, surface-vs-create decisions, clarification questions, architecture checks, SQL generation, and test generation are all fully ticket-driven with zero agent-code changes across use cases.

For a new client engagement or a second use case, the path to production is: add a new Jira project, add a new Databricks job key in `databricks.yml`, run `sml schema` and `sml index`, and run `sml demo`. The AI-DLC agent team handles the rest.
