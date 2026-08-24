# Demo Script — AI-DLC Live Demo Reference

> Keep this open during the demo. Every human gate is listed with the exact input to type.

---

## Pre-Demo Setup (before audience arrives)

```bash
sml index     # ~2 min — indexes codebase + all skills into ChromaDB
sml schema    # ~1 min — indexes live Databricks schema
sml demo      # start the demo
```

Open `http://localhost:8765` in a browser for the live KPI dashboard.

---

## Beat 1 — Pull Ticket

**What you see:** Terminal lists open Jira tickets.

→ Type the **number next to SCRUM-5** and press Enter.

**What the system shows:**
```
REQ-01  Gold table must include net_settlement_amount — the net cash amount due at settlement
REQ-02  net_settlement_amount must be derived from bond silver layer: principal_amount × (1 + accrued_interest_rate)
REQ-03  Settlement fields must apply to bond and muni security types only
REQ-04  Gold table grain remains: one row per active security version (is_current = TRUE)
```

**Gate prompt:** `Your decision:`

→ Type `approve`

**Talking point:** *"The BA Agent connected to Jira, pulled the ticket, parsed the structured requirement rows from the Atlassian Document Format, and indexed the ticket text into ChromaDB. Everything downstream is grounded in this ticket — no copy-paste, no manual briefing of the agent."*

---

## Beat 2 — Clarify

**What you see:** The BA Agent generates ONE clarification question and posts it to Jira as a comment.

The question will be ONE of these (grounded in codebase context from ontology.md, data_dictionary.md, known_issues.md):

---

**If question is about SCD2 filtering:**

> *"Does `statestreet.s_statestreet.bond` have its own `is_current` flag, or does the gold layer derive currency from `effective_end_date`?"*

**Answer to type:**
```
Yes — silver bond has is_current BOOLEAN as a standard SCD2 column per our architecture. Gold filters on is_current = TRUE directly. No derivation from effective_end_date needed.
```

---

**If question is about muni as a subtype of bond:**

> *"Should `net_settlement_amount` apply to muni bonds (a bond subtype), or only plain corporate bonds?"*

**Answer to type:**
```
Yes — munis are a subtype of bond per the class hierarchy. Filter predicate is product_type IN ('bond', 'muni') per REQ-03.
```

---

**If question is about accrued_interest_rate source:**

> *"Is `accrued_interest_rate` available in the silver bond or debt table, or must it be sourced from the coupon schedule?"*

**Answer to type:**
```
accrued_interest_rate does not exist as a dedicated column in the current silver schema. Use NULL for now with a COMMENT directing analysts to fact_coupon_schedule.coupon_rate as a future migration path.
```

---

**Gate prompt:** `Your decision:`

→ Type `approve`

**Talking point:** *"The BA Agent retrieved the top-10 most relevant chunks from ChromaDB before generating this question — that's why it asked about SCD2 and not something generic. It read ontology.md (Bond→Muni class hierarchy), data_dictionary.md (settlement definition), and the existing gold notebook. Grounded, not hallucinated."*

---

## Beat 3 — Verify

**What you see:** Three checks run automatically. No input needed.

```
✓ Check 1 — Requirements Clarity        PASS
✓ Check 2 — Architecture Conformance    PASS
✗ Check 3 — Gold Spec Validation        FAIL
  → net_settlement_amount missing from gold/tables.yaml
  [Fix] action=create — adding DECIMAL(18,6) to gold spec
  Re-checking...
✓ Check 3 — Gold Spec Validation        PASS (after fix)
```

**Gate prompt:** `Your decision:`

→ Type `approve`

**Talking point:** *"Check 3 searched the live Databricks schema catalog — 'Does net_settlement_amount already exist in bronze or silver?' It doesn't. So the action is 'create', not 'surface'. Then it detects the column is missing from the gold spec, auto-patches the spec file, and re-runs the check. This is the Validate Spec Agent acting as a data steward — ensuring the spec matches what the ticket requires before any code is written."*

---

## Beat 4 — Build

**What you see:** Developer Agent generates `05_gold_build.sql`, QA Agent generates `test_gold.py`.

Look for in the SQL output:
- `net_settlement_amount` in the SELECT list
- `CASE WHEN product_type IN ('bond','muni') THEN principal_amount * (1 + accrued_interest_rate) ELSE NULL END`
- `TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')`
- `COMMENT ON COLUMN` statements (Genie context)

Look for in the test output:
- `def test_net_settlement_null_for_non_bond()`
- `def test_is_current_only()`

**Gate prompt:** `Your decision:`

→ Type `approve`

**Talking point:** *"The Developer Agent retrieved the existing gold notebook pattern from ChromaDB, read known_issues.md to get the correct column mapping (face_amount → total_amount_issued), and read data_engineering.md for Databricks SQL conventions. It generated standards-compliant SQL on the first attempt because it read CLAUDE.md before writing a single line."*

---

## Beat 4b — Deploy

**What you see:**

```
Step A: git commit → push → gh pr create → PR URL: https://github.com/.../pull/XX
Step B: databricks bundle validate → databricks bundle deploy ✓
Step C: databricks bundle run gold_mart_job --no-wait → Job submitted ✓
        Monitor: Databricks → Jobs → [SML] 05 Gold Mart Build
```

**Gate prompt:** `Your decision:`

→ Type `approve`

**Talking point:** *"The Deploy Agent committed all generated files, raised a GitHub PR (which can be reviewed and merged), deployed the Databricks bundle, and submitted the gold mart job. The `--no-wait` flag means we don't block the demo waiting for cluster startup. The job is running in Databricks right now."*

**If Databricks CLI is not configured:** Steps B and C show yellow advisory panels and are skipped. PR is still created. Deploy manually with `databricks bundle deploy` then `databricks bundle run gold_mart_job`.

---

## Beat 5 — Genie

**What you see:** Genie returns live SQL and result rows from the Gold table.

**No input needed.**

Query: *"What is the total net settlement amount by security type?"*

**Talking point:** *"Genie knows what `net_settlement_amount` means because the Developer Agent wrote `COMMENT ON COLUMN` statements describing it in business terms. That's the handshake between the code generation agent and the AI query interface — no extra setup required."*

---

## Beat 6 — Observe

**What you see:** Final KPI summary — token counts, latency per beat, agent calls, cost estimate.

**No input needed.**

**Talking point:** *"Every Claude API call is tracked. You can see exactly which agent consumed how many tokens, what the latency was per beat, and what the total cost of this pipeline run was. Full observability — no black box."*

---

## Post-Demo: Knowledge Agent REPL

After Beat 6, the REPL starts. Use it for audience Q&A:

```
> What DQ rules apply to the bond table?
> How does the SCD2 merge work in silver conformance?
> Why is product_type used as the partition column in gold?
> change: rename net_settlement_amount to bond_settlement_amount
```

Type `exit` to quit.

---

## Common Questions From Audience

**Q: Is this really dynamic or are the column names hardcoded?**
> The check agent receives the live Databricks schema as context and reads the ticket requirements. The only input is the Jira ticket — change the ticket text and the agent proposes a different column.

**Q: What happens if the column already exists in silver?**
> The schema_catalog vector search returns it with a high similarity score. Claude's action becomes "surface" (bring existing column to gold) instead of "create" (new column). The fix panel shows which silver table it came from.

**Q: What if an engineer rejects a beat?**
> Type `reject: <reason>` at any gate. The harness stops. The agent does not re-run automatically — the rejection reason is captured for the next session.

**Q: How do you extend this to a new use case?**
> Change the Jira ticket. Everything else — the ChromaDB context, the CLAUDE.md standards, the skills — stays the same. No agent code changes needed.

**Q: What is the cost per run?**
> Approximately $0.20–$0.50 in Claude API tokens for a full Beat 1 through Beat 4 run, depending on how much codebase context is retrieved. Beat 6 shows the exact figure.
