# Skill: LLM Integration

## Overview
Patterns for integrating LLM APIs into production applications reliably and cost-effectively.

## Key Patterns

### API Usage
- **System / User / Assistant** message roles — structure conversations correctly
- **Streaming** — stream tokens for responsive UIs; handle partial JSON carefully
- **Tool / Function Calling** — let LLM invoke structured functions; validate outputs
- **Context window management** — summarise history when approaching token limit

### Tool Use / Function Calling
```json
{
  "name": "get_stock_price",
  "description": "Get the current price of a stock ticker",
  "parameters": {
    "type": "object",
    "properties": { "ticker": { "type": "string" } },
    "required": ["ticker"]
  }
}
```
- LLM decides when and how to call tools
- Always validate tool call arguments before executing
- Return structured results back to LLM for final response

### Rate Limiting and Retries
- Implement exponential backoff with jitter on 429 responses
- Use a queue to smooth request spikes
- Cache identical prompts with deterministic outputs (`temperature=0`)

### Cost Management
- Track token usage per request — log `prompt_tokens` and `completion_tokens`
- Use smaller models for classification/routing; large models for generation
- Cache responses for repeated identical prompts
- Prompt compression — remove redundant tokens while preserving meaning

### Structured Output
- Use JSON mode or `response_format: { type: 'json_object' }` where available
- Validate with Zod / Pydantic before trusting LLM output
- Include retry logic when output fails schema validation

## Best Practices
- Never expose raw LLM output directly to users without validation
- Log all prompts and responses for debugging and auditing
- Version-control prompt templates alongside application code
- Set `max_tokens` to prevent runaway responses and cost spikes
- Use model aliases not specific versions to auto-receive minor updates

## Common Pitfalls
- Trusting LLM JSON output without schema validation (causes runtime errors)
- No fallback when LLM API is unavailable
- Sending entire conversation history each time (token and cost waste)
- Leaking sensitive data in prompts logged to third-party providers

## Tools
- **Anthropic Claude API** — claude-sonnet-4-6 for complex tasks, claude-haiku-4-5 for speed
- **OpenAI API** — GPT-4o, structured outputs, function calling
- **LangSmith** — prompt tracing and evaluation
- **Portkey / Helicone** — LLM gateway with caching and monitoring
- **Instructor** — structured LLM output with Pydantic
