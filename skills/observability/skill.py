from __future__ import annotations

import datetime
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

    def query_telemetry(
        self,
        skill_name: str | None = None,
        limit: int = 50,
        chain_id: str | None = None,
        outcome: str | None = None,
    ) -> list[dict]:
        conn = self._db.get_connection("telemetry")
        where: list[str] = []
        params: list[object] = []
        if skill_name:
            where.append("skill = ?")
            params.append(skill_name)
        if chain_id:
            where.append("chain_id = ?")
            params.append(chain_id)
        if outcome:
            where.append("outcome = ?")
            params.append(outcome)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM step_outcomes {where_sql} ORDER BY timestamp DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_alert_history(
        self,
        limit: int = 20,
        tier: str | None = None,
        unacknowledged_only: bool = False,
    ) -> list[dict]:
        conn = self._db.get_connection("operator_digest")
        where: list[str] = []
        params: list[object] = []
        if tier:
            where.append("tier = ?")
            params.append(tier)
        if unacknowledged_only:
            where.append("acknowledged = 0")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM alert_log {where_sql} ORDER BY created_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def recent_digests(self, limit: int = 5) -> list[dict]:
        conn = self._db.get_connection("operator_digest")
        rows = conn.execute(
            "SELECT * FROM digest_history ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def reliability_dashboard(self, limit: int = 20) -> dict:
        telemetry = self._db.get_connection("telemetry")
        degraded_steps = telemetry.execute(
            """
            SELECT step_type, skill, reliability_7d, reliability_30d
            FROM reliability_by_step
            WHERE reliability_7d IS NOT NULL
            ORDER BY reliability_7d ASC, skill ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        chain_rows = telemetry.execute(
            """
            SELECT chain_type, chain_reliability_7d, chain_reliability_30d
            FROM chain_reliability
            ORDER BY chain_type ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return {
            "steps": [dict(row) for row in degraded_steps],
            "chains": [dict(row) for row in chain_rows],
        }

    def buffer_stats(self) -> dict:
        return {
            "telemetry": self._telemetry_buffer.stats if self._telemetry_buffer else None,
            "immune": self._immune_buffer.stats if self._immune_buffer else None,
        }

    def system_health(self) -> dict:
        operator = self._db.get_connection("operator_digest")
        heartbeat = operator.execute(
            "SELECT timestamp FROM operator_heartbeat ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        alert_counts = operator.execute(
            "SELECT tier, COUNT(*) AS count FROM alert_log GROUP BY tier ORDER BY tier"
        ).fetchall()
        pending_gates = operator.execute(
            "SELECT COUNT(*) FROM gate_log WHERE status = 'PENDING'"
        ).fetchone()[0]
        heartbeat_state = self._heartbeat_state(heartbeat["timestamp"]) if heartbeat is not None else "ABSENT"
        return {
            "db_status": self._db.verify_all_databases(),
            "buffer_stats": self.buffer_stats(),
            "heartbeat_state": heartbeat_state,
            "last_heartbeat_at": heartbeat["timestamp"] if heartbeat is not None else None,
            "pending_gates": pending_gates,
            "alert_counts": {row["tier"]: row["count"] for row in alert_counts},
        }

    @staticmethod
    def _heartbeat_state(last_timestamp: str) -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        seen = datetime.datetime.fromisoformat(last_timestamp)
        hours = (now - seen).total_seconds() / 3600
        if hours < 72:
            return "ACTIVE"
        if hours < 168:
            return "CONSERVATIVE"
        return "ABSENT"


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
        return _SKILL.query_telemetry(
            kwargs.get("skill_name"),
            kwargs.get("limit", 50),
            chain_id=kwargs.get("chain_id"),
            outcome=kwargs.get("outcome"),
        )
    if action == "query_alert_history":
        return _SKILL.query_alert_history(
            kwargs.get("limit", 20),
            tier=kwargs.get("tier"),
            unacknowledged_only=kwargs.get("unacknowledged_only", False),
        )
    if action == "recent_digests":
        return _SKILL.recent_digests(kwargs.get("limit", 5))
    if action == "reliability_dashboard":
        return _SKILL.reliability_dashboard(kwargs.get("limit", 20))
    if action == "buffer_stats":
        return _SKILL.buffer_stats()
    if action == "system_health":
        return _SKILL.system_health()
    raise ValueError(f"Unknown action: {action}")
