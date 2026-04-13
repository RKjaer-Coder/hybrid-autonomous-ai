from __future__ import annotations

import datetime
import uuid
from typing import Optional

from skills.db_manager import DatabaseManager


class OpportunityPipelineSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def create_opportunity(self, title: str, thesis: str, income_mechanism: str = "software_product") -> str:
        opportunity_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            "INSERT INTO opportunity_records (opportunity_id, income_mechanism, title, thesis, detected_by, council_verdict_id, validation_spend, validation_report, cashflow_estimate, status, project_id, learning_record, provenance_links, provenance_degraded, trust_tier, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (opportunity_id, income_mechanism, title, thesis, "operator", None, 0.0, None, '{"monthly":0}', "DETECTED", None, None, "[]", 0, 2, now, now),
        )
        conn.commit()
        return opportunity_id


_SKILL: Optional[OpportunityPipelineSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = OpportunityPipelineSkill(db_manager)


def opportunity_pipeline_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("opportunity pipeline skill not configured")
    if action == "create_opportunity":
        return _SKILL.create_opportunity(kwargs["title"], kwargs["thesis"], kwargs.get("income_mechanism", "software_product"))
    raise ValueError(f"Unknown action: {action}")
