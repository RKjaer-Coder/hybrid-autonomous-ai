from __future__ import annotations

from typing import Optional

from skills.append_buffer import AppendBuffer
from skills.db_manager import DatabaseManager


class ObservabilitySkill:
    def __init__(self, db_manager: DatabaseManager, telemetry_buffer: Optional[AppendBuffer], immune_buffer: Optional[AppendBuffer]):
        self._db = db_manager
        self._telemetry_buffer = telemetry_buffer
        self._immune_buffer = immune_buffer

    def query_immune_verdicts(self, limit: int = 20, outcome: str | None = None) -> list[dict]:
        conn = self._db.get_connection("immune")
        if outcome:
            rows = conn.execute("SELECT * FROM immune_verdicts WHERE result = ? ORDER BY timestamp DESC LIMIT ?", (outcome, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM immune_verdicts ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def query_telemetry(self, skill_name: str | None = None, limit: int = 50) -> list[dict]:
        conn = self._db.get_connection("telemetry")
        if skill_name:
            rows = conn.execute("SELECT * FROM step_outcomes WHERE skill = ? ORDER BY timestamp DESC LIMIT ?", (skill_name, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM step_outcomes ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def buffer_stats(self) -> dict:
        return {
            "telemetry": self._telemetry_buffer.stats if self._telemetry_buffer else None,
            "immune": self._immune_buffer.stats if self._immune_buffer else None,
        }

    def system_health(self) -> dict:
        return {
            "db_status": self._db.verify_all_databases(),
            "buffer_stats": self.buffer_stats(),
        }


_SKILL: Optional[ObservabilitySkill] = None


def configure_skill(db_manager: DatabaseManager, telemetry_buffer: Optional[AppendBuffer], immune_buffer: Optional[AppendBuffer]):
    global _SKILL
    _SKILL = ObservabilitySkill(db_manager, telemetry_buffer, immune_buffer)


def observability_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("observability skill not configured")
    if action == "query_immune_verdicts":
        return _SKILL.query_immune_verdicts(kwargs.get("limit", 20), kwargs.get("outcome"))
    if action == "query_telemetry":
        return _SKILL.query_telemetry(kwargs.get("skill_name"), kwargs.get("limit", 50))
    if action == "buffer_stats":
        return _SKILL.buffer_stats()
    if action == "system_health":
        return _SKILL.system_health()
    raise ValueError(f"Unknown action: {action}")
