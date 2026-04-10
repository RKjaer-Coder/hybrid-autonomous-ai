import unittest

from council.context_budget import build_context_packet
from council.orchestrator import MockDispatcher, run_tier1_deliberation
from council.types import DecisionType, Recommendation


class TestOrchestrator(unittest.TestCase):
    def test_pipeline_returns_verdict(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        verdict = run_tier1_deliberation(ctx, dispatcher)
        self.assertEqual(verdict.tier_used, 1)

    def test_batch_a_parallel_called_once(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, dispatcher)
        self.assertEqual(dispatcher.parallel_called, 1)

    def test_batch_b_da_called_once(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, dispatcher)
        self.assertEqual(dispatcher.sequential_called, 1)

    def test_da_input_contains_batch(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, dispatcher)
        self.assertIn("[STRATEGIST]", dispatcher.last_sequential_prompt)

    def test_synthesis_receives_all_roles(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, dispatcher)
        for marker in ["strategist", "critic", "realist", "devils_advocate"]:
            self.assertIn(marker, dispatcher.last_synthesis_prompt)

    def test_auto_escalation_for_low_confidence(self):
        dispatcher = MockDispatcher(low_confidence=True)
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        verdict = run_tier1_deliberation(ctx, dispatcher)
        self.assertEqual(verdict.recommendation, Recommendation.ESCALATE)

    def test_synthesis_parse_failure_raises(self):
        dispatcher = MockDispatcher(bad_json=True)
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        with self.assertRaises(ValueError):
            run_tier1_deliberation(ctx, dispatcher)

    def test_identical_outputs_rejected(self):
        dispatcher = MockDispatcher(identical=True)
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        with self.assertRaises(ValueError):
            run_tier1_deliberation(ctx, dispatcher)
