import unittest

from council.context_budget import CALLER_TOKEN_BUDGETS, build_context_packet, check_context_growth
from council.types import ContextPacket, DecisionType


class TestContextBudget(unittest.TestCase):
    def test_each_decision_budget_present(self):
        self.assertEqual(len(CALLER_TOKEN_BUDGETS), 6)

    def test_budget_opportunity(self):
        self.assertEqual(CALLER_TOKEN_BUDGETS[DecisionType.OPPORTUNITY_SCREEN], 800)

    def test_budget_phase(self):
        self.assertEqual(CALLER_TOKEN_BUDGETS[DecisionType.PHASE_GATE], 1600)

    def test_budget_kill(self):
        self.assertEqual(CALLER_TOKEN_BUDGETS[DecisionType.KILL_REC], 1000)

    def test_budget_go_no_go(self):
        self.assertEqual(CALLER_TOKEN_BUDGETS[DecisionType.GO_NO_GO], 1200)

    def test_budget_operator(self):
        self.assertEqual(CALLER_TOKEN_BUDGETS[DecisionType.OPERATOR_STRATEGIC], 1500)

    def test_budget_system(self):
        self.assertEqual(CALLER_TOKEN_BUDGETS[DecisionType.SYSTEM_CRITICAL], 2000)

    def test_over_budget_truncated(self):
        pkt = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "w " * 1000)
        self.assertIn("[TRUNCATED]", pkt.context_text)

    def test_under_budget_not_truncated(self):
        pkt = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "small")
        self.assertNotIn("[TRUNCATED]", pkt.context_text)

    def test_compression_rate_alert(self):
        p1 = ContextPacket(DecisionType.GO_NO_GO, "1", "x [TRUNCATED]", 1, 2)
        p2 = ContextPacket(DecisionType.GO_NO_GO, "2", "x", 1, 2)
        p3 = ContextPacket(DecisionType.GO_NO_GO, "3", "x", 1, 2)
        p4 = ContextPacket(DecisionType.GO_NO_GO, "4", "x", 1, 2)
        self.assertEqual(check_context_growth([p1, p2, p3, p4]), "CONTEXT_GROWTH_TREND")

    def test_compression_rate_no_alert(self):
        p1 = ContextPacket(DecisionType.GO_NO_GO, "1", "x", 1, 2)
        p2 = ContextPacket(DecisionType.GO_NO_GO, "2", "x", 1, 2)
        self.assertIsNone(check_context_growth([p1, p2]))
