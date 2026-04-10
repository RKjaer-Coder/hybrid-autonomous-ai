from __future__ import annotations

import datetime
import uuid
from typing import List, Optional, Tuple

from council.types import (
    CalibrationRecord,
    CouncilVerdict,
    DecisionType,
    DEFAULT_ROLE_WEIGHTS,
    Recommendation,
    RoleName,
    WEIGHT_CAP,
    WEIGHT_DRIFT_ABSOLUTE,
    WEIGHT_DRIFT_PER_CYCLE,
    WEIGHT_FLOOR,
)


def _uuid7_str() -> str:
    generator = getattr(uuid, "uuid7", None)
    return str(generator() if callable(generator) else uuid.uuid4())


def compute_binary_outcome(cashflow_actual: Optional[float], cashflow_target: Optional[float], project_killed: bool = False, operator_rating: Optional[float] = None, is_cashflow_type: bool = True) -> float:
    if project_killed:
        return 0.0
    if not is_cashflow_type:
        if operator_rating is not None:
            return operator_rating
        return -1.0
    if cashflow_actual is None or cashflow_target is None or cashflow_target <= 0:
        return 0.0
    ratio = cashflow_actual / cashflow_target
    if ratio >= 0.80:
        return 1.0
    if ratio >= 0.50:
        return 0.5
    return 0.0


def compute_prediction_correct(recommendation: Recommendation, binary_outcome: float) -> float:
    if recommendation == Recommendation.PURSUE:
        return 1.0 if binary_outcome >= 0.5 else 0.0
    if recommendation == Recommendation.REJECT:
        return 1.0 if binary_outcome < 0.5 else 0.0
    return 0.0


def build_calibration_record(verdict: CouncilVerdict, actual_outcome: float) -> CalibrationRecord:
    return CalibrationRecord(
        calibration_id=_uuid7_str(),
        verdict_id=verdict.verdict_id,
        decision_type=verdict.decision_type,
        predicted_outcome=verdict.confidence,
        actual_outcome=actual_outcome,
        prediction_correct=compute_prediction_correct(verdict.recommendation, actual_outcome),
        role_weights_used={role.value: weight for role, weight in DEFAULT_ROLE_WEIGHTS.items()},
        which_role_was_right=None,
        da_quality_score=verdict.da_quality_score,
        tie_break=verdict.tie_break,
        created_at=datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
    )


def compute_role_accuracy(records: List[CalibrationRecord], role: RoleName, window_days: int = 90, min_observations: int = 10) -> Optional[float]:
    del window_days
    selected = [r.prediction_correct for r in records if r.which_role_was_right == role and r.prediction_correct is not None]
    if len(selected) < min_observations:
        return None
    return sum(selected) / len(selected)


def propose_weight_adjustment(current_weights: dict, initial_weights: dict, role_accuracies: dict) -> Tuple[dict, List[str]]:
    events: List[str] = []
    new_weights = dict(current_weights)
    for role, current in current_weights.items():
        acc = role_accuracies.get(role)
        delta = 0.0
        if acc is None:
            continue
        if acc >= 0.75:
            delta = 0.05
        elif acc <= 0.55:
            delta = -0.05
        delta = max(-WEIGHT_DRIFT_PER_CYCLE, min(WEIGHT_DRIFT_PER_CYCLE, delta))
        proposed = current + delta
        init = initial_weights.get(role, current)
        low = init - WEIGHT_DRIFT_ABSOLUTE
        high = init + WEIGHT_DRIFT_ABSOLUTE
        bounded = max(low, min(high, proposed))
        bounded = max(WEIGHT_FLOOR, min(WEIGHT_CAP, bounded))
        if bounded != proposed:
            events.append("CALIBRATION_DRIFT_CAPPED")
        new_weights[role] = bounded
    total = sum(new_weights.values())
    if total > 0:
        new_weights = {k: v / total for k, v in new_weights.items()}
    return new_weights, events


def detect_oscillation(weight_history: List[dict], role: RoleName) -> bool:
    if len(weight_history) < 4:
        return False
    series = [entry[role] for entry in weight_history[-4:]]
    diffs = [series[i + 1] - series[i] for i in range(len(series) - 1)]
    signs = [1 if d > 0 else -1 if d < 0 else 0 for d in diffs]
    return signs == [1, -1, 1] or signs == [-1, 1, -1]
