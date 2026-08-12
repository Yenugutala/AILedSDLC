# Skill: Prompt Engineering

## Overview
Techniques for writing effective prompts that produce accurate, consistent, and well-structured LLM outputs.

## Key Patterns

### System Prompts
- Define role, persona, output format, and constraints in the system message
- Keep system prompts stable; vary user messages per request
- Include output format instructions (JSON schema, markdown, numbered list)

### Chain-of-Thought (CoT)
- Add "Think step by step" or "Let's reason through this" to trigger reasoning
- Zero-shot CoT — just the instruction; few-shot CoT — include worked examples
- CoT improves accuracy on multi-step reasoning and maths problems

### Few-Shot Prompting
- Provide 2–5 input/output examples before the actual request
- Examples teach format, tone, and reasoning pattern
- Select diverse examples covering edge cases

### Output Formatting
- Specify exact format: "Respond only with valid JSON matching this schema: {...}"
- Use delimiters to separate sections: `<context>`, `<question>`, `<answer>`
- Ask for structured output (JSON mode where supported) to enable reliable parsing

### Prompt Chaining
- Break complex tasks into smaller sequential prompts
- Pass outputs from one prompt as inputs to the next
- Use for: extract → validate → transform → summarise pipelines

### Temperature and Sampling
- `temperature=0` — deterministic, best for factual / structured output
- `temperature=0.7–1.0` — creative, varied responses
- `top_p` — nucleus sampling; use instead of temperature, not both

## Best Practices
- Version-control prompts like code — include in repo, track changes
- Test prompts against a fixed eval dataset before deploying
- Separate prompt logic from application code (prompt templates)
- Include negative constraints: "Do not hallucinate. If unsure, say so."
- Use XML tags for long contexts (Claude performs well with structured markup)

## Common Pitfalls
- Ambiguous instructions leading to inconsistent outputs
- No output format specification — model chooses arbitrary format
- Over-long prompts — include only what is necessary for the task
- Not testing edge cases (empty input, adversarial input, multilingual)

## Tools
- **LangChain / LlamaIndex** — prompt templates and chaining
- **Promptfoo** — prompt evaluation and regression testing
- **Helicone / LangSmith** — prompt monitoring and logging
- **Anthropic Claude API** — supports system prompts, tool use, extended thinking
