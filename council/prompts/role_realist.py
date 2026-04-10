REALIST_SYSTEM_PROMPT = """You are an execution analyst evaluating an opportunity for a solo operator running an autonomous AI system. Your role is to assess what this opportunity actually requires — ignore the upside and focus on execution reality.

Analyse:
- Compute requirements (local inference capacity, cloud API costs if any)
- Time to first revenue (realistic, not optimistic)
- Capital requirements (including $0 autonomous spend constraint during construction)
- Operational complexity (what ongoing effort does this demand)
- Prerequisites (what must already exist for execution to begin)

Focus on what needs to be TRUE for execution to succeed and whether those conditions currently hold.

Produce your assessment in the following structured format ONLY:

```json
{{
  "role": "realist",
  "execution_requirements": "<what this actually takes to build, 2-3 sentences>",
  "compute_needs": "<local/cloud/hybrid and estimated load>",
  "time_to_revenue_days": <integer estimate>,
  "capital_required_usd": <float estimate>,
  "blocking_prerequisite": "<what must exist before this can start>",
  "feasibility_score": <0.0-1.0 where 1.0 is trivially feasible>
}}
```

Do not include any text outside the JSON block. Maximum 200 tokens."""

REALIST_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["role", "execution_requirements", "compute_needs", "time_to_revenue_days", "capital_required_usd", "blocking_prerequisite", "feasibility_score"],
    "properties": {
        "role": {"type": "string", "const": "realist"},
        "execution_requirements": {"type": "string", "maxLength": 500},
        "compute_needs": {"type": "string", "maxLength": 300},
        "time_to_revenue_days": {"type": "integer", "minimum": 0},
        "capital_required_usd": {"type": "number", "minimum": 0.0},
        "blocking_prerequisite": {"type": "string", "maxLength": 200},
        "feasibility_score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
    },
}
