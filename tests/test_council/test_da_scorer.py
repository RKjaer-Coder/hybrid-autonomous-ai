import unittest

from council.da_scorer import check_da_recovery, check_da_thresholds, parse_da_assessment, score_da_quality
from council.types import DATag, DAAssessment, DecisionType


class TestDAScorer(unittest.TestCase):
    def test_score_all_incorporated(self):
        s = score_da_quality([DAAssessment("o", DATag.INCORPORATED, "r"), DAAssessment("o2", DATag.INCORPORATED, "r")])
        self.assertEqual(s, 1.0)

    def test_score_ack_and_dismissed(self):
        s = score_da_quality([DAAssessment("o", DATag.ACKNOWLEDGED, "r"), DAAssessment("o2", DATag.DISMISSED, "r")])
        self.assertEqual(s, 0.25)

    def test_score_empty_is_zero(self):
        self.assertEqual(score_da_quality([]), 0.0)

    def test_threshold_collapse(self):
        evt = check_da_thresholds(0.8, DecisionType.GO_NO_GO, [0.39] * 10)
        self.assertEqual(evt, "DA_COLLAPSE")

    def test_threshold_degraded(self):
        evt = check_da_thresholds(0.8, DecisionType.GO_NO_GO, [0.29] * 10)
        self.assertEqual(evt, "COUNCIL_DEGRADED")

    def test_threshold_silent(self):
        evt = check_da_thresholds(0.0, DecisionType.GO_NO_GO, [0.8] * 10)
        self.assertEqual(evt, "DA_SILENT")

    def test_threshold_none(self):
        evt = check_da_thresholds(0.7, DecisionType.GO_NO_GO, [0.8] * 10)
        self.assertIsNone(evt)

    def test_parse_assessment_valid(self):
        parsed = parse_da_assessment([{"objection": "x", "tag": "dismissed", "reasoning": "y"}])
        self.assertEqual(parsed[0].tag, DATag.DISMISSED)

    def test_parse_assessment_invalid_tag(self):
        with self.assertRaises(ValueError):
            parse_da_assessment([{"objection": "x", "tag": "unknown", "reasoning": "y"}])

    def test_recovery_true_14(self):
        self.assertTrue(check_da_recovery([0.5] * 14, True))

    def test_recovery_false_13(self):
        self.assertFalse(check_da_recovery([0.5] * 13, True))
