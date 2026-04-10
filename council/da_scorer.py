from __future__ import annotations

from typing import List, Optional

from council.types import DAAssessment, DATag, DecisionType

TAG_VALUES = {
    DATag.INCORPORATED: 1.0,
    DATag.ACKNOWLEDGED: 0.5,
    DATag.DISMISSED: 0.0,
}


def score_da_quality(da_assessment: List[DAAssessment]) -> float:
    if not da_assessment:
        return 0.0
    return sum(TAG_VALUES[item.tag] for item in da_assessment) / len(da_assessment)


def parse_da_assessment(raw_assessment: List[dict]) -> List[DAAssessment]:
    parsed: List[DAAssessment] = []
    for row in raw_assessment:
        try:
            tag = DATag(row["tag"])
        except Exception as exc:
            raise ValueError(f"Invalid DA tag: {row.get('tag')}") from exc
        parsed.append(DAAssessment(objection=row["objection"], tag=tag, reasoning=row["reasoning"]))
    return parsed


def check_da_thresholds(
    quality_score: float,
    decision_type: DecisionType,
    rolling_30d_scores: Optional[List[float]] = None,
) -> Optional[str]:
    del decision_type
    if quality_score == 0.0:
        return "DA_SILENT"
    if not rolling_30d_scores:
        return None
    avg = sum(rolling_30d_scores) / len(rolling_30d_scores)
    if avg < 0.30:
        return "COUNCIL_DEGRADED"
    if avg < 0.40:
        return "DA_COLLAPSE"
    return None


def check_da_recovery(rolling_14d_scores: List[float], previous_collapse_event: bool) -> bool:
    if not previous_collapse_event or len(rolling_14d_scores) < 14:
        return False
    return all(score >= 0.50 for score in rolling_14d_scores[-14:])
