import unittest

from council.calibration import (
    compute_binary_outcome,
    compute_prediction_correct,
    detect_oscillation,
    propose_weight_adjustment,
)
from council.types import DEFAULT_ROLE_WEIGHTS, Recommendation, RoleName


class TestCalibration(unittest.TestCase):
    def test_binary_80_plus(self):
        self.assertEqual(compute_binary_outcome(80, 100), 1.0)

    def test_binary_50_to_80(self):
        self.assertEqual(compute_binary_outcome(70, 100), 0.5)

    def test_binary_below_50(self):
        self.assertEqual(compute_binary_outcome(20, 100), 0.0)

    def test_binary_project_killed(self):
        self.assertEqual(compute_binary_outcome(100, 100, project_killed=True), 0.0)

    def test_binary_non_cashflow_rating(self):
        self.assertEqual(compute_binary_outcome(None, None, is_cashflow_type=False, operator_rating=0.5), 0.5)

    def test_binary_non_cashflow_missing_rating(self):
        self.assertEqual(compute_binary_outcome(None, None, is_cashflow_type=False), -1.0)

    def test_prediction_pursue_win(self):
        self.assertEqual(compute_prediction_correct(Recommendation.PURSUE, 0.5), 1.0)

    def test_prediction_pursue_lose(self):
        self.assertEqual(compute_prediction_correct(Recommendation.PURSUE, 0.0), 0.0)

    def test_prediction_reject_win(self):
        self.assertEqual(compute_prediction_correct(Recommendation.REJECT, 0.0), 1.0)

    def test_adjust_reduce_055(self):
        w, _ = propose_weight_adjustment(DEFAULT_ROLE_WEIGHTS, DEFAULT_ROLE_WEIGHTS, {RoleName.STRATEGIST: 0.55})
        self.assertLess(w[RoleName.STRATEGIST], DEFAULT_ROLE_WEIGHTS[RoleName.STRATEGIST])

    def test_adjust_increase_075(self):
        w, _ = propose_weight_adjustment(DEFAULT_ROLE_WEIGHTS, DEFAULT_ROLE_WEIGHTS, {RoleName.CRITIC: 0.75})
        self.assertGreater(w[RoleName.CRITIC], DEFAULT_ROLE_WEIGHTS[RoleName.CRITIC])

    def test_weight_floor(self):
        cur = dict(DEFAULT_ROLE_WEIGHTS)
        cur[RoleName.DEVILS_ADVOCATE] = 0.01
        w, _ = propose_weight_adjustment(cur, DEFAULT_ROLE_WEIGHTS, {RoleName.DEVILS_ADVOCATE: 0.1})
        self.assertGreaterEqual(w[RoleName.DEVILS_ADVOCATE], 0.10 / sum(w.values()))

    def test_weight_cap(self):
        cur = dict(DEFAULT_ROLE_WEIGHTS)
        cur[RoleName.CRITIC] = 0.9
        w, _ = propose_weight_adjustment(cur, DEFAULT_ROLE_WEIGHTS, {RoleName.CRITIC: 0.9})
        self.assertLessEqual(w[RoleName.CRITIC], 1.0)

    def test_drift_limit_per_cycle(self):
        w, _ = propose_weight_adjustment(DEFAULT_ROLE_WEIGHTS, DEFAULT_ROLE_WEIGHTS, {RoleName.CRITIC: 1.0})
        self.assertLessEqual(abs(w[RoleName.CRITIC] - DEFAULT_ROLE_WEIGHTS[RoleName.CRITIC]), 0.10)

    def test_abs_drift_ceiling(self):
        current = dict(DEFAULT_ROLE_WEIGHTS)
        current[RoleName.STRATEGIST] = 0.8
        w, events = propose_weight_adjustment(current, DEFAULT_ROLE_WEIGHTS, {RoleName.STRATEGIST: 0.9})
        self.assertTrue(events)
        self.assertLessEqual(w[RoleName.STRATEGIST], 1.0)

    def test_normalised(self):
        w, _ = propose_weight_adjustment(DEFAULT_ROLE_WEIGHTS, DEFAULT_ROLE_WEIGHTS, {})
        self.assertAlmostEqual(sum(w.values()), 1.0)

    def test_oscillation_true(self):
        h = [
            {RoleName.CRITIC: 0.30},
            {RoleName.CRITIC: 0.35},
            {RoleName.CRITIC: 0.32},
            {RoleName.CRITIC: 0.34},
        ]
        self.assertTrue(detect_oscillation(h, RoleName.CRITIC))

    def test_oscillation_false(self):
        h = [
            {RoleName.CRITIC: 0.30},
            {RoleName.CRITIC: 0.31},
            {RoleName.CRITIC: 0.32},
            {RoleName.CRITIC: 0.33},
        ]
        self.assertFalse(detect_oscillation(h, RoleName.CRITIC))
