from __future__ import annotations

from typing import List, Optional

from council.prompts.common import enforce_token_budget
from council.types import ContextPacket, DecisionType, RoleName

CALLER_TOKEN_BUDGETS: dict = {
    DecisionType.OPPORTUNITY_SCREEN: 800,
    DecisionType.PHASE_GATE: 1600,
    DecisionType.KILL_REC: 1000,
    DecisionType.GO_NO_GO: 1200,
    DecisionType.OPERATOR_STRATEGIC: 1500,
    DecisionType.SYSTEM_CRITICAL: 2000,
}

ROLE_OUTPUT_LIMITS: dict = {
    RoleName.STRATEGIST: 200,
    RoleName.CRITIC: 200,
    RoleName.REALIST: 200,
    RoleName.DEVILS_ADVOCATE: 150,
}


def _estimate_tokens(text: str) -> int:
    return max(1, int(len(text.split()) / 0.75)) if text.strip() else 0


def build_context_packet(
    decision_type: DecisionType,
    subject_id: str,
    raw_context: str,
    source_briefs: Optional[List[str]] = None,
) -> ContextPacket:
    max_tokens = CALLER_TOKEN_BUDGETS[decision_type]
    context_text = enforce_token_budget(raw_context, max_tokens)
    return ContextPacket(
        decision_type=decision_type,
        subject_id=subject_id,
        context_text=context_text,
        token_count=_estimate_tokens(context_text),
        max_tokens=max_tokens,
        source_briefs=source_briefs,
    )


def check_context_growth(recent_packets: List[ContextPacket], window_days: int = 7) -> Optional[str]:
    if not recent_packets:
        return None
    compressed = sum(1 for p in recent_packets if "[TRUNCATED]" in p.context_text)
    if compressed / len(recent_packets) > 0.20:
        return "CONTEXT_GROWTH_TREND"
    return None
