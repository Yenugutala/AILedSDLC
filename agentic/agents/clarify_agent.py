from __future__ import annotations
"""
clarify_agent.py
BA Agent — generates ONE codebase-grounded clarification question before implementation.

The question is grounded in real column names, table names, and patterns from the codebase
retrieved via vector DB semantic search. Used by Beat 2 of the demo harness.
"""

import anthropic

_MODEL = "claude-sonnet-4-6"


def run(
    ticket_summary: str,
    ticket_key: str,
    requirements: list[dict],
    codebase_context: str,
) -> tuple[str, int, int, int]:
    """
    Generate ONE codebase-grounded clarification question.

    Args:
        ticket_summary: Jira ticket summary line
        ticket_key: Jira ticket key (e.g. AIDLC-42)
        requirements: list of dicts with keys 'id' and 'text'
        codebase_context: pre-retrieved vector DB context (formatted string)

    Returns:
        (question, input_tokens, output_tokens, latency_ms)
    """
    req_block = "\n".join(f"- {r['id']}: {r['text']}" for r in requirements)

    system = (
        "You are an experienced data engineer reviewing a Jira ticket before implementation. "
        "You have read the codebase carefully and have ONE critical clarifying question. "
        "The question must be impossible to ask without having read the actual code — "
        "it should reference specific column names, table names, or patterns you found. "
        "Ask only the single most important question. No preamble, no multiple questions."
    )
    user = (
        f"Ticket: {ticket_key} — {ticket_summary}\n\n"
        f"Requirements:\n{req_block}\n\n"
        f"Relevant codebase context:\n{codebase_context}\n\n"
        "What is your ONE critical clarifying question before starting implementation?"
    )

    import time
    t0 = time.monotonic()
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model=_MODEL,
        max_tokens=256,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    latency_ms = int((time.monotonic() - t0) * 1000)

    question = resp.content[0].text.strip()
    return question, resp.usage.input_tokens, resp.usage.output_tokens, latency_ms
