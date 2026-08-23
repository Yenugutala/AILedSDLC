from __future__ import annotations
"""
check_agents.py
Verification agents used in Beat 3 of the demo harness.

check_req_clarity          — BA Agent: are requirements clear enough to build?
check_arch_conformance     — Architect Agent: does the ticket conform to CLAUDE.md standards?
extract_required_gold_columns — Validate Spec Agent: discovers required columns from live Databricks schema.
                                Uses vector similarity — no column names hardcoded anywhere.
"""

import json
import time

import anthropic

_MODEL = "claude-sonnet-4-6"


def check_req_clarity(
    ticket_summary: str,
    requirements: list[dict],
    clarification_q: str,
    clarification_a: str,
) -> tuple[bool, str, int, int, int]:
    """
    BA Agent — checks that requirements are clear, unambiguous, and complete.

    Args:
        ticket_summary: Jira ticket summary
        requirements: list of dicts with keys 'id' and 'text'
        clarification_q: the clarification question asked in Beat 2
        clarification_a: the engineer's answer

    Returns:
        (passed, detail, input_tokens, output_tokens, latency_ms)
    """
    req_block = "\n".join(f"- {r['id']}: {r['text']}" for r in requirements)

    system = (
        "You are a senior requirements analyst. "
        "Review the ticket requirements and clarification and decide: "
        "are they clear, unambiguous, and complete enough to start building? "
        "Reply with exactly: PASS: <one-line reason>  OR  FAIL: <one-line reason>"
    )
    user = (
        f"Ticket: {ticket_summary}\n\nRequirements:\n{req_block}\n\n"
        f"Clarification Q: {clarification_q}\n"
        f"Clarification A: {clarification_a}"
    )

    t0 = time.monotonic()
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=_MODEL, max_tokens=128,
        system=system, messages=[{"role": "user", "content": user}],
    )
    latency_ms = int((time.monotonic() - t0) * 1000)

    text = resp.content[0].text.strip()
    passed = text.upper().startswith("PASS")
    detail = text.split(":", 1)[-1].strip() if ":" in text else text
    return passed, detail, resp.usage.input_tokens, resp.usage.output_tokens, latency_ms  # noqa: E501


def check_arch_conformance(
    ticket_summary: str,
    clarification_a: str,
    claude_md: str,
) -> tuple[bool, str, int, int, int]:
    """
    Architect Agent — checks that the proposed change conforms to CLAUDE.md coding standards.

    Args:
        ticket_summary: Jira ticket summary
        clarification_a: engineer's clarification answer (adds context)
        claude_md: contents of CLAUDE.md

    Returns:
        (passed, detail, input_tokens, output_tokens, latency_ms)
    """
    system = (
        "You are a data architect. Check if the proposed change conforms to CLAUDE.md coding standards. "
        "Focus on: naming conventions, Unity Catalog structure, language split (Bronze=Python, Silver/Gold=SQL), "
        "and the no dim_/fact_ prefix rule for gold tables. "
        "Reply with exactly: PASS: <one-line reason>  OR  FAIL: <one-line reason>"
    )
    user = (
        f"Ticket: {ticket_summary}\n\n"
        f"Clarification: {clarification_a}\n\n"
        f"CLAUDE.md standards:\n{claude_md[:2000]}"
    )

    t0 = time.monotonic()
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=_MODEL, max_tokens=128,
        system=system, messages=[{"role": "user", "content": user}],
    )
    latency_ms = int((time.monotonic() - t0) * 1000)

    text = resp.content[0].text.strip()
    passed = text.upper().startswith("PASS")
    detail = text.split(":", 1)[-1].strip() if ":" in text else text
    return passed, detail, resp.usage.input_tokens, resp.usage.output_tokens, latency_ms


def extract_required_gold_columns(
    ticket_summary: str,
    requirements: list[dict],
    existing_schema_context: str,
) -> tuple[list[dict], int, int, int]:
    """
    Validate Spec Agent — discovers which gold columns are required by the ticket.

    Uses LIVE Databricks schema context (from SchemaCatalog.format_for_agent()).
    No column names, types, or business rules are hardcoded here.

    For each required column Claude decides:
      action="surface" — column already exists in bronze/silver → bring to gold
      action="create"  — truly new column → propose definition from naming conventions

    Args:
        ticket_summary: Jira ticket summary
        requirements: list of dicts with keys 'id' and 'text'
        existing_schema_context: formatted text from SchemaCatalog.format_for_agent()

    Returns:
        (columns, input_tokens, output_tokens, latency_ms)
        Each column dict has: req_id, action, name, sql_type, description, nullable,
        and source_table (only for action=surface).
    """
    req_block = "\n".join(f"- {r['id']}: {r['text']}" for r in requirements)

    system = """You are a Data Architect and Data Steward for a financial data lakehouse.

You have access to a live schema catalog showing ALL existing columns in the bronze, silver, and gold layers.
The catalog was auto-discovered from Databricks INFORMATION_SCHEMA — it reflects the real state of the lakehouse.

For each column implied by the ticket requirements:

1. Search the EXISTING SCHEMA CATALOG carefully.
2. If a similar or identical column EXISTS in bronze or silver (similarity score >= 0.6):
   → Return action="surface".
   → Use the EXACT column name, data type, and comment from the catalog entry.
   → Set source_table to the full 3-part table name (e.g. statestreet.s_statestreet.bond).
   → Meaning: "this data already exists in a lower layer — surface it in gold mart."
3. If NO sufficiently similar column exists:
   → Return action="create".
   → Propose a new definition following naming conventions (snake_case).
   → Infer sql_type from name patterns:
       *_amount / *_price / *_rate / *_value → DECIMAL(18,6)
       *_date / *_start_date / *_end_date → DATE
       *_ts / *_timestamp → TIMESTAMP
       *_id / *_key → STRING
       *_flag / is_* / has_* / *_indicator → BOOLEAN
       *_count / *_qty / *_quantity → INT
       default → STRING
   → Write a clear business description that references the requirement ID.
   → Do NOT set source_table for created columns.

Output ONLY a valid JSON array — no markdown, no explanation, no code fences:
[{
  "req_id": "REQ-01",
  "action": "surface",
  "name": "exact_column_name_from_catalog",
  "sql_type": "DECIMAL(18,6)",
  "description": "Business description referencing REQ-01.",
  "nullable": true,
  "source_table": "statestreet.s_statestreet.bond"
}]

CRITICAL: For action=surface, use the EXACT column name from the catalog. Never invent names."""

    user = (
        f"Ticket: {ticket_summary}\n\n"
        f"Requirements:\n{req_block}\n\n"
        f"{existing_schema_context}"
    )

    t0 = time.monotonic()
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=_MODEL, max_tokens=1024,
        system=system, messages=[{"role": "user", "content": user}],
    )
    latency_ms = int((time.monotonic() - t0) * 1000)

    raw = resp.content[0].text.strip()
    # Strip markdown code fences if Claude added them
    if raw.startswith("```"):
        raw = "\n".join(raw.split("\n")[1:])
    if raw.endswith("```"):
        raw = raw[:-3].rstrip()

    try:
        columns = json.loads(raw)
        if not isinstance(columns, list):
            columns = []
    except json.JSONDecodeError:
        columns = []

    return columns, resp.usage.input_tokens, resp.usage.output_tokens, latency_ms
