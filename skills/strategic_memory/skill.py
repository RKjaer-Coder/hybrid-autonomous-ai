from __future__ import annotations

import datetime
import json
import uuid
from typing import Optional

from skills.db_manager import DatabaseManager


class StrategicMemorySkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def write_brief(self, task_id: str, title: str, summary: str, confidence: float = 0.5) -> str:
        brief_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            "INSERT OR IGNORE INTO research_tasks (task_id, domain, source, title, brief, priority, status, max_spend_usd, actual_spend_usd, output_brief_id, follow_up_tasks, stale_after, tags, depth_upgrade, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (task_id, 2, "operator", title, summary, "P2_NORMAL", "PENDING", 0.0, 0.0, None, "[]", None, "[]", 0, now, now),
        )
        conn.execute(
            "INSERT INTO intelligence_briefs (brief_id, task_id, domain, title, summary, detail, source_urls, source_assessments, confidence, uncertainty_statement, counter_thesis, actionability, urgency, depth_tier, action_type, spawned_tasks, spawned_opportunity_id, related_brief_ids, tags, quality_warning, source_diversity_hold, provenance_links, trust_tier, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (brief_id, task_id, 2, title, summary, None, "[]", "[]", confidence, None, None, "INFORMATIONAL", "ROUTINE", "QUICK", "none", "[]", None, "[]", "[]", 0, 0, "[]", 3, now),
        )
        conn.commit()
        return brief_id

    def read_brief(self, brief_id: str) -> dict:
        conn = self._db.get_connection("strategic_memory")
        row = conn.execute("SELECT brief_id, title, summary, confidence, created_at FROM intelligence_briefs WHERE brief_id = ?", (brief_id,)).fetchone()
        if row is None:
            raise KeyError(brief_id)
        return dict(row)


_SKILL: Optional[StrategicMemorySkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = StrategicMemorySkill(db_manager)


def strategic_memory_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("strategic memory skill not configured")
    if action == "write_brief":
        return _SKILL.write_brief(kwargs["task_id"], kwargs["title"], kwargs["summary"], kwargs.get("confidence", 0.5))
    if action == "read_brief":
        return _SKILL.read_brief(kwargs["brief_id"])
    raise ValueError(f"Unknown action: {action}")
