# Skill: AI Agents

## Overview
Autonomous LLM-powered agents that reason, plan, use tools, and complete multi-step tasks.

## Key Patterns

### Agent Loop (ReAct Pattern)
```
Thought → Action (tool call) → Observation (tool result) → Thought → ... → Final Answer
```
- LLM reasons about what to do next, calls a tool, observes the result, and continues
- Loop terminates when LLM produces a final answer or hits max iterations

### Tool Design
- Each tool has a clear name, description, and typed parameters
- Tools should be idempotent where possible
- Provide error messages the LLM can reason about and recover from
- Limit tool scope — one tool per action type

### Memory Systems
| Type | Description | Implementation |
|---|---|---|
| In-context | Recent conversation history | Message array in prompt |
| External (episodic) | Past interactions retrieved by relevance | Vector DB + RAG |
| Semantic | Extracted facts about user/world | Key-value store |
| Procedural | Learned skills/workflows | System prompt or few-shot |

### Multi-Agent Patterns
- **Orchestrator-Worker** — one planner agent delegates subtasks to specialist agents
- **Pipeline** — agents pass outputs sequentially (extract → validate → summarise)
- **Parallel** — multiple agents run simultaneously; results merged
- **Debate** — two agents argue positions; third judges

### Guardrails
- Input guardrails — validate and sanitise user inputs before passing to agent
- Output guardrails — check LLM output for policy violations, hallucinations, PII
- Tool execution guardrails — require human approval for irreversible actions
- Max iteration limits — prevent infinite loops

## Best Practices
- Define clear termination conditions for the agent loop
- Log every thought, action, and observation for debugging
- Test with adversarial inputs — prompt injection attempts
- Use structured tool outputs so LLM can reliably parse results
- Require human-in-the-loop for high-risk actions (delete, publish, pay)

## Common Pitfalls
- Agent loops that never terminate (no max iterations)
- Tools with side effects called repeatedly (not idempotent)
- LLM hallucinating tool call parameters instead of using real data
- Over-autonomous agents acting without user confirmation

## Tools
- **Claude Agent SDK** — Anthropic's SDK for building agents
- **LangGraph** — stateful multi-agent orchestration
- **AutoGen** — Microsoft multi-agent framework
- **CrewAI** — role-based multi-agent collaboration
- **Pydantic AI** — type-safe agent framework
