import unittest
from dataclasses import FrozenInstanceError

from council.types import (
    BriefSignal,
    ContextPacket,
    CouncilTier,
    CouncilVerdict,
    DATag,
    DecisionType,
    Recommendation,
    RoleName,
)


class TestTypes(unittest.TestCase):
    def test_context_packet_is_frozen(self):
        packet = ContextPacket(DecisionType.GO_NO_GO, "id", "txt", 10, 20)
        with self.assertRaises(FrozenInstanceError):
            packet.context_text = "other"

    def test_council_verdict_confidence_low_rejected(self):
        with self.assertRaises(ValueError):
            CouncilVerdict("id", 1, DecisionType.GO_NO_GO, Recommendation.PURSUE, -0.1, "a", "b", None, None, 0.0, None)

    def test_council_verdict_confidence_high_rejected(self):
        with self.assertRaises(ValueError):
            CouncilVerdict("id", 1, DecisionType.GO_NO_GO, Recommendation.PURSUE, 1.1, "a", "b", None, None, 0.0, None)

    def test_council_verdict_degraded_cap(self):
        with self.assertRaises(ValueError):
            CouncilVerdict("id", 1, DecisionType.GO_NO_GO, Recommendation.PURSUE, 0.9, "a", "b", None, None, 0.0, None, degraded=True)

    def test_decision_enum_count(self):
        self.assertEqual(len(DecisionType), 6)

    def test_recommendation_enum_count(self):
        self.assertEqual(len(Recommendation), 5)

    def test_role_enum_count(self):
        self.assertEqual(len(RoleName), 4)

    def test_da_tag_enum_count(self):
        self.assertEqual(len(DATag), 3)

    def test_brief_signal_enum_count(self):
        self.assertEqual(len(BriefSignal), 3)

    def test_council_tier_enum_count(self):
        self.assertEqual(len(CouncilTier), 2)
