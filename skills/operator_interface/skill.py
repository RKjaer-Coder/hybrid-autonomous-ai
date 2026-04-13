from __future__ import annotations

import datetime
import uuid
from typing import Optional

from skills.db_manager import DatabaseManager


class OperatorInterfaceSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def alert(self, tier: str, alert_type: str, content: str) -> str:
        alert_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
        conn = self._db.get_connection("operator_digest")
        conn.execute(
            "INSERT INTO alert_log (alert_id, tier, alert_type, content, channel_delivered, suppressed, acknowledged, acknowledged_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (alert_id, tier, alert_type, content, None, 0, 0, None, now),
        )
        conn.commit()
        return alert_id


_SKILL: Optional[OperatorInterfaceSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = OperatorInterfaceSkill(db_manager)


def operator_interface_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("operator interface skill not configured")
    if action == "alert":
        return _SKILL.alert(kwargs["tier"], kwargs["alert_type"], kwargs["content"])
    raise ValueError(f"Unknown action: {action}")
