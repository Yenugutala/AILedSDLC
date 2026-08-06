# Build Pipeline — How to Use the sml CLI and BMAD Agent Loop

Complete guide for running the Securities Master Data Lakehouse pipeline from start to finish.

---

## 1. First-Time Setup

```bash
# Step 1: Clone repo
git clone https://github.com/<org>/securities-master-lakehouse
cd securities-master-lakehouse

# Step 2: Install sml CLI (from the agentic/ folder — where pyproject.toml lives)
cd agentic
pip install -e .
sml --help    # verify install

# Step 3: Set ANTHROPIC_API_KEY (required for BMAD agents)
export ANTHROPIC_API_KEY="sk-ant-..."

# Step 4: (Optional) Configure Databricks MCP for debug commands
# See Section 8 — Databricks MCP Configuration
```

---

## 2. Quick Start — Full Pipeline

```bash
# Fill in your use case details
cp project/use-cases/securities-master/request.yaml project/use-cases/my-use-case/request.yaml
# Edit request.yaml with your source, catalog, and description

# Run the BMAD agent loop
cd agentic
sml generate --use-case my-use-case

# The pipeline:
#  1. Loads CLAUDE.md + skills/ + request.yaml + known_issues.md
#  2. Runs BA Agent → prints spec plan → asks "approve / reject: <feedback>"
#  3. Runs Architect Agent → prints schema decisions → asks "approve / reject: <feedback>"
#  4. Runs Developer Agent + QA Agent + Doc Agent → generates code
#  5. Creates GitHub PR automatically

# After PR is merged → GitHub Actions deploys to Databricks

# In Databricks — run notebooks in order:
#  01_setup_catalog.py   (one time)
#  02_upload_raw_files.py (one time, or when files change)
#  03_bronze_ingest.py
#  04_silver_conform.sql
#  05_gold_build.sql
#  06_setup_genie.py     (one time)
```

---

## 3. request.yaml — The Only Manual Input

Human fills this file. Agents generate everything else.

```yaml
# use-cases/securities-master/request.yaml
use_case_name: securities-master
description: |
  Ingest 29 security CSV files (product, bond, stock, fund, identifiers, etc.)
  from Databricks Volume. Each security follows class-table inheritance:
  every security has one row in 'product' plus rows in type-specific tables.
  Bronze = raw landing. Silver = DQ-conformed + rejects. Gold = 4 dimensional marts.

source:
  type: volume                   # volume | jdbc | api | kafka | delta | s3 | adls | gcs
  path: /Volumes/statestreet/securities_master/raw_files/
  format: csv
  delimiter: ","
  header: true

code_generation:
  bronze: python        # PySpark notebooks
  silver: sql           # Databricks SQL notebooks
  gold: sql             # Databricks SQL notebooks

catalog:
  name: statestreet
  bronze_schema: b_statestreet
  silver_schema: s_statestreet
  gold_schema: g_statestreet

stakeholders:
  - role: data_owner
    approver: john.doe@statestreet.com
  - role: architect_approver
    approver: jane.smith@statestreet.com
```

---

## 4. Approval Gate Reference

| Gate | When | Who Approves | What to Type |
|------|------|-------------|--------------|
| **Gate 1** | After BA Agent outputs specs | Use-case team | `approve` or `reject: <feedback>` |
| **Gate 2** | After Architect Agent finalizes schema | Senior architect | `approve` or `reject: <feedback>` |
| **Gate 3** | Generated code PR ready | Any team member | GitHub PR review → merge |
| **Gate 4a** | After Silver DQ scan — reject report ready | Data owner | Databricks webhook: approve Silver promotion |
| **Gate 4b** | DQ rule change rescan needed | Data owner | Databricks webhook: approve rescan |

**Human never specifies which agent runs next** — the pipeline reads `agent_state.yaml` and knows the current stage automatically.

### Example Session

```
$ sml generate --use-case securities-master

[SML] Loading context...
  ✓ CLAUDE.md loaded
  ✓ skills/ loaded (9 files)
  ✓ request.yaml loaded
  ✓ known_issues.md loaded

[BA AGENT] Analyzing requirements...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ BA AGENT OUTPUT ━━━━━━━━━━━━━━━
  Bronze schema: statestreet.b_statestreet (29 tables)
  Silver schema: statestreet.s_statestreet (29 tables + 29 rejects)
  Gold schema:   statestreet.g_statestreet (4 marts)
  Key decisions: SCD2 on product, legal_entity, product_rating
                 Partition Bronze by _ingestion_date
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[GATE 1] Review the BA output above.
  Type 'approve' to continue
  Type 'reject: <your feedback>' to revise
  > reject: The gold fact_coupon_schedule grain should be one row per bond per payment date, not per bond

[yellow] Feedback recorded. Re-running BA Agent...

... (BA Agent re-runs with feedback) ...

[GATE 1] Review the revised BA output above.
  > approve

[ARCHITECT AGENT] Finalizing schema design...
... (same approve/reject flow) ...

[DEVELOPER AGENT] Generating Bronze notebooks (Python)...
[QA AGENT] Generating test files...
[DOC AGENT] Adding Gold column comments for Genie...

[SML] Creating GitHub PR...
  ✓ PR #23 created: https://github.com/<org>/securities-master-lakehouse/pull/23

[GATE 3] Review PR #23 and merge when ready.
```

---

## 5. sml CLI Command Reference

```bash
# Generate specs + code for a use case (full BMAD loop)
sml generate --use-case securities-master

# Debug a failing Databricks job (fetches logs via MCP → proposes fix)
sml debug --use-case securities-master --job silver_conform_job

# Check status of a running Databricks job
sml status --use-case securities-master --job orchestrate_pipeline_job

# Re-run a specific Databricks job
sml run --use-case securities-master --job bronze_ingest_job

# Validate spec files against CLAUDE.md (also runs in CI)
sml validate --use-case securities-master

# Help
sml --help
sml generate --help
```

---

## 6. Notebook Execution Order in Databricks

After code is deployed (via PR merge + GitHub Actions), run these in order:

| Step | Notebook | Language | When to Run |
|------|----------|----------|------------|
| 1 | `notebooks/01_setup_catalog.py` | Python | Once (first time only) |
| 2 | `notebooks/02_upload_raw_files.py` | Python | Once per new data drop |
| 3 | `notebooks/03_bronze_ingest.py` | Python | Every pipeline run |
| 4 | `notebooks/04_silver_conform.sql` | SQL | Every pipeline run |
| 5 | `notebooks/05_gold_build.sql` | SQL | Every pipeline run |
| 6 | `notebooks/06_setup_genie.py` | Python | Once (first time only) |

Or trigger all via orchestrator job:
```
Databricks → Jobs → orchestrate_pipeline_job → Run now
```

---

## 7. Updating DQ Rules (After Initial Setup)

When DQ rules need to change (new rule, fixed SQL threshold, rule removal):

```bash
# 1. Edit the DQ rules file
vi project/use-cases/securities-master/specs/silver/rules.yaml

# 2. Validate YAML syntax
sml validate --use-case securities-master

# 3. Raise a PR
git checkout -b feat/securities-master-dq-update
git add project/use-cases/securities-master/specs/silver/rules.yaml
git commit -m "fix: update RULE0045 null threshold for sub_type"
git push -u origin feat/securities-master-dq-update
gh pr create

# 4. After PR merge:
#    - New SHA256 version is auto-computed from rules.yaml
#    - Pipeline identifies Silver tables with stale _dq_rule_version
#    - Selective rescan is triggered on only those tables
#    - Approve via Gate 4b Databricks webhook
```

---

## 8. Updating Agent Prompts (Shared Across All Teams)

```bash
# Edit prompts in the agentic folder
vi agentic/agents/prompts/ba_agent.md

# Raise PR — team reviews the prompt change
git checkout -b feat/ba-agent-prompt-v2
git add agentic/agents/prompts/ba_agent.md
git commit -m "feat: add SCD2 guidance to BA agent prompt"
git push
gh pr create

# After merge → all future sml generate runs use the updated prompt
```

---

## 9. Adding a New Use Case

```bash
# Create use-case directory
mkdir -p project/use-cases/my-new-use-case

# Copy and edit request.yaml
cp project/use-cases/securities-master/request.yaml project/use-cases/my-new-use-case/
# Edit: use_case_name, description, source, catalog

# (Optional) Add known issues specific to this use case
vi project/use-cases/my-new-use-case/known_issues.md

# Generate specs and code
cd agentic
sml generate --use-case my-new-use-case
```

---

## 10. Resuming After Interruption

The pipeline saves state after every agent completes. If interrupted, just re-run:

```bash
sml generate --use-case securities-master
# Pipeline reads agent_state.yaml and resumes from current stage
# Previously approved stages are NOT re-run
```

To restart from scratch (delete state):
```bash
rm project/use-cases/securities-master/agent_state.yaml
sml generate --use-case securities-master
# Starts fresh from ba_review stage
```

---

## 11. Debugging a Failed Databricks Job

```bash
# Option 1: sml debug (uses Databricks MCP to fetch logs)
sml debug --use-case securities-master --job silver_conform_job

# Claude will:
#   1. Fetch the last run logs via Databricks MCP
#   2. Identify the root cause (schema mismatch? DQ SQL error? Python bug?)
#   3. Propose a fix (spec update, code patch, or known_issues.md entry)
#   4. Ask: "approve / reject: <feedback>"
#   5. Apply fix → create PR → re-run job → confirm success

# Option 2: Manual (without MCP)
#   Databricks → Jobs → <job_name> → Last run → View output
#   Copy error → diagnose → fix spec or code → re-run via sml run
```

---

## 12. Databricks MCP Configuration

Enables `sml debug` and `sml status` to fetch live Databricks logs.

```json
// .claude/settings.json (gitignored — never commit)
{
  "mcpServers": {
    "databricks": {
      "command": "uvx",
      "args": ["databricks-mcp"],
      "env": {
        "DATABRICKS_HOST": "https://<your-workspace>.azuredatabricks.net",
        "DATABRICKS_TOKEN": "<your-pat-token>"
      }
    }
  }
}
```

```bash
# Install databricks-mcp
pip install uvx
uvx databricks-mcp --help   # verify
```

**Security:** Use an environment variable for `DATABRICKS_TOKEN` instead of hardcoding in the JSON:
```json
{
  "mcpServers": {
    "databricks": {
      "command": "uvx",
      "args": ["databricks-mcp"],
      "env": {
        "DATABRICKS_HOST": "${DATABRICKS_HOST}",
        "DATABRICKS_TOKEN": "${DATABRICKS_TOKEN}"
      }
    }
  }
}
```

---

## 13. CI/CD — GitHub Actions

Two workflows run automatically:

### On Pull Request (`validate-specs.yml`)
- Validates all spec YAML files against CLAUDE.md naming conventions
- Runs `sml validate --use-case` for all use cases
- Must pass before merge is allowed

### On Push to Main (`deploy-bundle.yml`)
- Runs `databricks bundle validate` (validates DAB YAML)
- Runs `databricks bundle deploy` (uploads to Databricks workspace)
- Creates/updates all Databricks jobs automatically

```bash
# Check CI status
gh pr checks <pr-number>
gh run list --workflow=validate-specs.yml
```
