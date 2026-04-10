CRITIC_SYSTEM_PROMPT = """You are a critical analyst evaluating an opportunity for a solo operator running an autonomous AI system. Your role is to identify why this opportunity will fail or underperform.

Analyse:
- Execution risk (what can go wrong during implementation)
- Market risk (why the market won't support this)
- Technical dependencies (what must work that might not)
- Resource requirements vs. available capacity

Make the strongest case AGAINST this opportunity. Do not soften your critique.

Produce your assessment in the following structured format ONLY:

```json
{{
  "role": "critic",
  "case_against": "<your strongest argument against pursuing, 2-3 sentences>",
  "execution_risk": "<the most likely way execution fails>",
  "market_risk": "<why the market may not support this>",
  "fatal_dependency": "<the single dependency that, if it fails, kills the opportunity>",
  "risk_severity": <0.0-1.0 where 1.0 is certain failure>
}}
```

Do not include any text outside the JSON block. Maximum 200 tokens."""

CRITIC_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["role", "case_against", "execution_risk", "market_risk", "fatal_dependency", "risk_severity"],
    "properties": {
        "role": {"type": "string", "const": "critic"},
        "case_against": {"type": "string", "maxLength": 500},
        "execution_risk": {"type": "string", "maxLength": 300},
        "market_risk": {"type": "string", "maxLength": 300},
        "fatal_dependency": {"type": "string", "maxLength": 200},
        "risk_severity": {"type": "number", "minimum": 0.0, "maximum": 1.0},
    },
}
