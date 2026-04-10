import json
import unittest

from council.context_budget import build_context_packet
from council.orchestrator import MockDispatcher, SubagentDispatcher, run_tier1_deliberation
from council.types import DecisionType, RoleName, RoleOutput


class AdversarialDispatcher(SubagentDispatcher):
    def dispatch_parallel(self, prompts):
        return [RoleOutput(role=p[0], content=json.dumps({"role": p[0].value, "x": "same"}), token_count=1, max_tokens=10) for p in prompts]

    def dispatch_sequential(self, role, system_prompt, user_prompt):
        return RoleOutput(role=role, content=json.dumps({"role": "devils_advocate", "shared_assumption": "x", "novel_risk": "x", "material_disagreement": "x", "alternative_interpretation": "x"}), token_count=1, max_tokens=10)

    def dispatch_synthesis(self, system_prompt, user_prompt):
        return json.dumps({"tier_used": 1, "decision_type": "opportunity_screen", "recommendation": "PURSUE", "confidence": 0.9, "reasoning_summary": "x", "dissenting_views": "y", "da_assessment": [{"objection": "o", "tag": "dismissed", "reasoning": "r"}], "tie_break": False})


class TestAntiCollapse(unittest.TestCase):
    def test_role_outputs_are_different(self):
        d = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, d)
        self.assertNotEqual(d.parallel_called, 0)

    def test_da_references_specific_points(self):
        d = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, d)
        self.assertIn("Disagree with strategist", d.last_synthesis_prompt)

    def test_da_novel_point_present(self):
        d = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        verdict = run_tier1_deliberation(ctx, d)
        self.assertIn("distribution channel policy could shift", verdict.dissenting_views)

    def test_synthesis_not_averaging(self):
        d = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        verdict = run_tier1_deliberation(ctx, d)
        self.assertNotIn("average", verdict.reasoning_summary.lower())

    def test_identical_outputs_flagged(self):
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        with self.assertRaises(ValueError):
            run_tier1_deliberation(ctx, AdversarialDispatcher())
