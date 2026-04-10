from __future__ import annotations

from abc import ABC, abstractmethod
import json
import uuid
from typing import List, Optional, Tuple

from council.context_budget import ROLE_OUTPUT_LIMITS
from council.da_scorer import parse_da_assessment, score_da_quality
from council.prompts.common import (
    format_batch_a_for_da,
    format_context_packet,
    parse_json_output,
)
from council.prompts.role_critic import CRITIC_SYSTEM_PROMPT
from council.prompts.role_devils_advocate import DEVILS_ADVOCATE_SYSTEM_PROMPT
from council.prompts.role_realist import REALIST_SYSTEM_PROMPT
from council.prompts.role_strategist import STRATEGIST_SYSTEM_PROMPT
from council.prompts.synthesis import SYNTHESIS_OUTPUT_SCHEMA, SYNTHESIS_SYSTEM_PROMPT
from council.types import CouncilVerdict, DEFAULT_ROLE_WEIGHTS, DecisionType, RoleName, RoleOutput, Recommendation, iso_utc_now
from council.validators import validate_role_output, validate_verdict


class SubagentDispatcher(ABC):
    @abstractmethod
    def dispatch_parallel(self, prompts: List[Tuple[RoleName, str, str]]) -> List[RoleOutput]:
        ...

    @abstractmethod
    def dispatch_sequential(self, role: RoleName, system_prompt: str, user_prompt: str) -> RoleOutput:
        ...

    @abstractmethod
    def dispatch_synthesis(self, system_prompt: str, user_prompt: str) -> str:
        ...


class MockDispatcher(SubagentDispatcher):
    def __init__(self, identical: bool = False, bad_json: bool = False, low_confidence: bool = False) -> None:
        self.identical = identical
        self.bad_json = bad_json
        self.low_confidence = low_confidence
        self.parallel_called = 0
        self.sequential_called = 0
        self.synthesis_called = 0
        self.last_sequential_prompt = ""
        self.last_synthesis_prompt = ""

    def dispatch_parallel(self, prompts: List[Tuple[RoleName, str, str]]) -> List[RoleOutput]:
        self.parallel_called += 1
        out: List[RoleOutput] = []
        for role, _, _ in prompts:
            if self.identical:
                payload = {"role": role.value, "case_for": "x", "market_fit_score": 0.5, "timing_assessment": "x", "strategic_alignment": "x", "key_assumption": "x"}
            elif role == RoleName.STRATEGIST:
                payload = {"role": "strategist", "case_for": "Strong timing and distribution edge.", "market_fit_score": 0.78, "timing_assessment": "Demand inflecting now", "strategic_alignment": "Fits autonomous local-first stack", "key_assumption": "Conversion from pilot users"}
            elif role == RoleName.CRITIC:
                payload = {"role": "critic", "case_against": "Customer acquisition cost may exceed LTV.", "execution_risk": "Pipeline fragility", "market_risk": "Crowded alternatives", "fatal_dependency": "Stable third-party API", "risk_severity": 0.72}
            else:
                payload = {"role": "realist", "execution_requirements": "Needs robust ingestion and eval loop.", "compute_needs": "Hybrid local with occasional cloud burst", "time_to_revenue_days": 60, "capital_required_usd": 1200.0, "blocking_prerequisite": "Validated ICP list", "feasibility_score": 0.66}
            out.append(RoleOutput(role=role, content=json.dumps(payload), token_count=20, max_tokens=ROLE_OUTPUT_LIMITS[role]))
        return out

    def dispatch_sequential(self, role: RoleName, system_prompt: str, user_prompt: str) -> RoleOutput:
        self.sequential_called += 1
        self.last_sequential_prompt = system_prompt + "\n" + user_prompt
        payload = {
            "role": "devils_advocate",
            "shared_assumption": "All assume immediate distribution channel success",
            "novel_risk": "Platform policy shift can remove integration path",
            "material_disagreement": "Disagree with strategist market timing certainty",
            "alternative_interpretation": "If channels close, revenue is delayed >12 months",
        }
        return RoleOutput(role=role, content=json.dumps(payload), token_count=30, max_tokens=ROLE_OUTPUT_LIMITS[role])

    def dispatch_synthesis(self, system_prompt: str, user_prompt: str) -> str:
        self.synthesis_called += 1
        self.last_synthesis_prompt = system_prompt + "\n" + user_prompt
        if self.bad_json:
            return "not json"
        conf = 0.52 if self.low_confidence else 0.78
        return json.dumps(
            {
                "tier_used": 1,
                "decision_type": "opportunity_screen",
                "recommendation": "PURSUE",
                "confidence": conf,
                "reasoning_summary": "Critic dependency risk is manageable; realist prerequisites are already met.",
                "dissenting_views": "DA warns distribution channel policy could shift.",
                "da_assessment": [
                    {
                        "objection": "Platform policy shift",
                        "tag": "acknowledged",
                        "reasoning": "Novel but not recommendation changing.",
                    }
                ],
                "tie_break": False,
                "risk_watch": ["Channel policy updates"],
            }
        )


def _uuid7_str() -> str:
    generator = getattr(uuid, "uuid7", None)
    return str(generator() if callable(generator) else uuid.uuid4())


def _check_auto_escalation(verdict_data: dict) -> dict:
    if verdict_data.get("confidence", 0.0) < 0.60:
        verdict_data = dict(verdict_data)
        verdict_data["recommendation"] = Recommendation.ESCALATE.value
    return verdict_data


def _apply_confidence_cap(verdict_data: dict, cap: float = 0.70) -> dict:
    verdict_data = dict(verdict_data)
    if verdict_data.get("confidence", 0.0) > cap:
        verdict_data["confidence"] = cap
    return verdict_data


def _assert_noncollapse(batch_a_outputs: List[RoleOutput]) -> None:
    contents = [o.content for o in batch_a_outputs]
    if len(set(contents)) == 1:
        raise ValueError("Anti-collapse violation: Batch A outputs are identical")


def run_tier1_deliberation(context, dispatcher: SubagentDispatcher, role_weights: Optional[dict] = None) -> CouncilVerdict:
    if context.token_count > context.max_tokens:
        raise ValueError("Context token budget exceeded")
    del role_weights
    user_prompt = format_context_packet(context)
    prompts = [
        (RoleName.STRATEGIST, STRATEGIST_SYSTEM_PROMPT, user_prompt),
        (RoleName.CRITIC, CRITIC_SYSTEM_PROMPT, user_prompt),
        (RoleName.REALIST, REALIST_SYSTEM_PROMPT, user_prompt),
    ]
    batch_a = dispatcher.dispatch_parallel(prompts)
    _assert_noncollapse(batch_a)
    for out in batch_a:
        _, errors = validate_role_output(out.content, out.role)
        if errors:
            raise ValueError("; ".join(errors))

    batch_text = format_batch_a_for_da(batch_a)
    da_system = DEVILS_ADVOCATE_SYSTEM_PROMPT.format(batch_a_outputs=batch_text)
    da_out = dispatcher.dispatch_sequential(RoleName.DEVILS_ADVOCATE, da_system, user_prompt)
    _, da_errors = validate_role_output(da_out.content, RoleName.DEVILS_ADVOCATE)
    if da_errors:
        raise ValueError("; ".join(da_errors))

    by_role = {o.role: o.content for o in batch_a}
    syn_system = SYNTHESIS_SYSTEM_PROMPT.format(
        strategist_output=by_role[RoleName.STRATEGIST],
        critic_output=by_role[RoleName.CRITIC],
        realist_output=by_role[RoleName.REALIST],
        da_output=da_out.content,
        decision_type=context.decision_type.value,
    )
    raw = dispatcher.dispatch_synthesis(syn_system, user_prompt)
    verdict_data = parse_json_output(raw, SYNTHESIS_OUTPUT_SCHEMA)
    verdict_data = _check_auto_escalation(verdict_data)

    da_assessment = parse_da_assessment(verdict_data.get("da_assessment", []))
    da_quality_score = score_da_quality(da_assessment)

    errors = validate_verdict(verdict_data, context.decision_type)
    hard_errors = [e for e in errors if not e.startswith("warning")]
    if hard_errors:
        raise ValueError("; ".join(hard_errors))

    return CouncilVerdict(
        verdict_id=_uuid7_str(),
        tier_used=1,
        decision_type=context.decision_type,
        recommendation=Recommendation(verdict_data["recommendation"]),
        confidence=float(verdict_data["confidence"]),
        reasoning_summary=verdict_data["reasoning_summary"],
        dissenting_views=verdict_data["dissenting_views"],
        minority_positions=None,
        full_debate_record=None,
        cost_usd=0.0,
        project_id=context.subject_id,
        da_assessment=da_assessment,
        da_quality_score=da_quality_score,
        tie_break=bool(verdict_data.get("tie_break", False)),
        degraded=False,
        confidence_cap=None,
        created_at=iso_utc_now(),
    )
