from __future__ import annotations

import datetime
import uuid
from typing import Optional

from skills.db_manager import DatabaseManager


class ResearchDomainSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def create_task(self, title: str, brief: str, priority: str = "P2_NORMAL") -> str:
        task_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            "INSERT INTO research_tasks (task_id, domain, source, title, brief, priority, status, max_spend_usd, actual_spend_usd, output_brief_id, follow_up_tasks, stale_after, tags, depth_upgrade, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (task_id, 2, "operator", title, brief, priority, "PENDING", 0.0, 0.0, None, "[]", None, "[]", 0, now, now),
        )
        conn.commit()
        return task_id


_SKILL: Optional[ResearchDomainSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = ResearchDomainSkill(db_manager)


def research_domain_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("research domain skill not configured")
    if action == "create_task":
        return _SKILL.create_task(kwargs["title"], kwargs["brief"], kwargs.get("priority", "P2_NORMAL"))
    raise ValueError(f"Unknown action: {action}")
