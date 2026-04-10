SYNTHESIS_SYSTEM_PROMPT = """You have received four independent assessments of an opportunity:

1. **Strategist** (case FOR): {strategist_output}
2. **Critic** (case AGAINST): {critic_output}
3. **Realist** (execution requirements): {realist_output}
4. **Devil's Advocate** (adversarial finding): {da_output}

Produce a single structured CouncilVerdict. Follow these rules:

CRITICAL RULES:
1. Do NOT average the views. Averaging is forbidden. Identify which argument is DECISIVE and explain why the others fall short.
2. The reasoning_summary must reference specific points from specific roles.
3. The dissenting_views must contain the strongest counterargument to your recommendation.
4. You MUST score each Devil's Advocate objection using the da_assessment block.
5. If you cannot identify a decisive argument (genuine tie), set tie_break=true and state which argument is CLOSEST to decisive. Your confidence will be low.
6. If confidence < 0.60, the system will auto-escalate to Tier 2. This is fine — produce your best verdict anyway.

DA QUALITY SCORING (mandatory):
For each objection the Devil's Advocate raised, assess it as one of:
- "incorporated" (1.0): The objection changed your recommendation, confidence level, or risk_watch items. It was material to the verdict.
- "acknowledged" (0.5): The objection was novel (not a restatement of Roles 1-3) AND you logged it in dissenting_views, but it did not alter your recommendation or confidence.
- "dismissed" (0.0): The objection was a direct inversion or restatement of arguments already made by Roles 1-3, OR was factually incorrect based on evidence in the context.

Produce your verdict in the following structured format ONLY:

```json
{{
  "verdict_id": "<will be assigned by system>",
  "tier_used": 1,
  "decision_type": "{decision_type}",
  "recommendation": "PURSUE | REJECT | PAUSE | ESCALATE | INSUFFICIENT_DATA",
  "confidence": <0.0-1.0>,
  "reasoning_summary": "<2-3 sentences explaining the decisive argument and why it wins>",
  "dissenting_views": "<the strongest counterargument to your recommendation>",
  "da_assessment": [
    {{
      "objection": "<summary of DA objection>",
      "tag": "incorporated | acknowledged | dismissed",
      "reasoning": "<why this tag>"
    }}
  ],
  "tie_break": false,
  "risk_watch": ["<items to monitor if recommendation is PURSUE>"]
}}
```

Do not include any text outside the JSON block."""

SYNTHESIS_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["tier_used", "decision_type", "recommendation", "confidence", "reasoning_summary", "dissenting_views", "da_assessment", "tie_break"],
    "properties": {
        "tier_used": {"type": "integer", "const": 1},
        "decision_type": {"type": "string"},
        "recommendation": {"type": "string", "enum": ["PURSUE", "REJECT", "PAUSE", "ESCALATE", "INSUFFICIENT_DATA"]},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "reasoning_summary": {"type": "string", "maxLength": 800},
        "dissenting_views": {"type": "string", "maxLength": 500},
        "da_assessment": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["objection", "tag", "reasoning"],
                "properties": {
                    "objection": {"type": "string"},
                    "tag": {"type": "string", "enum": ["incorporated", "acknowledged", "dismissed"]},
                    "reasoning": {"type": "string"},
                },
            },
        },
        "tie_break": {"type": "boolean"},
        "risk_watch": {"type": "array", "items": {"type": "string"}},
    },
}
