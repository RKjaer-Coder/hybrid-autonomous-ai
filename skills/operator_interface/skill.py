from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import asdict, dataclass
from typing import Optional

from skills.db_manager import DatabaseManager


@dataclass(frozen=True)
class AlertRecord:
    alert_id: str
    tier: str
    alert_type: str
    content: str
    channel_delivered: str | None
    suppressed: bool
    acknowledged: bool
    created_at: str


@dataclass(frozen=True)
class DigestRecord:
    digest_id: str
    digest_type: str
    content: str
    sections_included: list[str]
    word_count: int
    operator_state: str
    created_at: str


class OperatorInterfaceSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def alert(
        self,
        tier: str,
        alert_type: str,
        content: str,
        *,
        channel_delivered: str | None = "CLI",
        suppressed: bool = False,
    ) -> str:
        alert_id = str(uuid.uuid4())
        now = self._utc_now()
        conn = self._db.get_connection("operator_digest")
        conn.execute(
            "INSERT INTO alert_log (alert_id, tier, alert_type, content, channel_delivered, suppressed, acknowledged, acknowledged_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (alert_id, tier, alert_type, content, channel_delivered, 1 if suppressed else 0, 0, None, now),
        )
        conn.commit()
        return alert_id

    def list_alerts(
        self,
        *,
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
            f"""
            SELECT alert_id, tier, alert_type, content, channel_delivered, suppressed, acknowledged, created_at
            FROM alert_log
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [
            asdict(
                AlertRecord(
                    alert_id=row["alert_id"],
                    tier=row["tier"],
                    alert_type=row["alert_type"],
                    content=row["content"],
                    channel_delivered=row["channel_delivered"],
                    suppressed=bool(row["suppressed"]),
                    acknowledged=bool(row["acknowledged"]),
                    created_at=row["created_at"],
                )
            )
            for row in rows
        ]

    def record_heartbeat(self, interaction_type: str, channel: str = "CLI") -> str:
        entry_id = str(uuid.uuid4())
        now = self._utc_now()
        conn = self._db.get_connection("operator_digest")
        conn.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (entry_id, interaction_type, channel, now),
        )
        conn.commit()
        return entry_id

    def generate_digest(self, digest_type: str = "daily", operator_state: str = "ACTIVE") -> dict:
        now = self._utc_now()
        sections = self._build_digest_sections()
        ordered_names = [
            "PORTFOLIO HEALTH",
            "PIPELINE STATUS",
            "INTELLIGENCE HIGHLIGHTS",
            "SYSTEM HEALTH",
            "PENDING DECISIONS",
            "FINANCIAL SUMMARY",
        ]
        lines = [f"{name}: {sections[name]}" for name in ordered_names]
        content = "\n".join(lines)
        digest_id = str(uuid.uuid4())
        conn = self._db.get_connection("operator_digest")
        conn.execute(
            """
            INSERT INTO digest_history (
                digest_id, digest_type, content, sections_included, word_count,
                operator_state, delivered_at, acknowledged_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                digest_id,
                digest_type,
                content,
                json.dumps(ordered_names),
                len(content.split()),
                operator_state,
                None,
                None,
                now,
            ),
        )
        conn.commit()
        return asdict(
            DigestRecord(
                digest_id=digest_id,
                digest_type=digest_type,
                content=content,
                sections_included=ordered_names,
                word_count=len(content.split()),
                operator_state=operator_state,
                created_at=now,
            )
        )

    def _build_digest_sections(self) -> dict[str, str]:
        financial = self._db.get_connection("financial_ledger")
        strategic = self._db.get_connection("strategic_memory")
        operator = self._db.get_connection("operator_digest")
        telemetry = self._db.get_connection("telemetry")

        active_projects = financial.execute("SELECT COUNT(*) FROM projects WHERE status = 'ACTIVE'").fetchone()[0]
        kill_watch = financial.execute("SELECT COUNT(*) FROM projects WHERE kill_score_watch = 1").fetchone()[0]
        opportunity_rows = strategic.execute(
            "SELECT status, COUNT(*) AS count FROM opportunity_records GROUP BY status ORDER BY status"
        ).fetchall()
        recent_briefs = strategic.execute(
            "SELECT title, actionability FROM intelligence_briefs ORDER BY created_at DESC LIMIT 3"
        ).fetchall()
        t2_t3_alerts = operator.execute(
            "SELECT COUNT(*) FROM alert_log WHERE tier IN ('T2', 'T3')"
        ).fetchone()[0]
        pending_gates = operator.execute(
            "SELECT gate_type, expires_at FROM gate_log WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT 3"
        ).fetchall()
        telemetry_steps = telemetry.execute("SELECT COUNT(*) FROM step_outcomes").fetchone()[0]
        last_treasury = financial.execute(
            "SELECT balance_after FROM treasury ORDER BY created_at DESC LIMIT 1"
        ).fetchone()

        portfolio = f"{active_projects} active project(s); {kill_watch} on kill watch."
        pipeline = "No open opportunities." if not opportunity_rows else ", ".join(
            f"{row['status']}={row['count']}" for row in opportunity_rows
        )
        intelligence = "No briefs recorded." if not recent_briefs else " | ".join(
            f"{row['title']} ({row['actionability']})" for row in recent_briefs
        )
        system_health = "All green." if t2_t3_alerts == 0 else f"{t2_t3_alerts} actionable alert(s) logged."
        system_health = f"{system_health} {telemetry_steps} telemetry step(s) recorded."
        pending = "No pending gates." if not pending_gates else " | ".join(
            f"{row['gate_type']} pending until {row['expires_at']}" for row in pending_gates
        )
        financial_summary = (
            f"Treasury balance ${last_treasury['balance_after']:.2f}."
            if last_treasury is not None
            else "No treasury entries yet."
        )

        return {
            "PORTFOLIO HEALTH": portfolio,
            "PIPELINE STATUS": pipeline,
            "INTELLIGENCE HIGHLIGHTS": intelligence,
            "SYSTEM HEALTH": system_health,
            "PENDING DECISIONS": pending,
            "FINANCIAL SUMMARY": financial_summary,
        }

    @staticmethod
    def _utc_now() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


_SKILL: Optional[OperatorInterfaceSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = OperatorInterfaceSkill(db_manager)


def operator_interface_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("operator interface skill not configured")
    if action == "alert":
        return _SKILL.alert(
            kwargs["tier"],
            kwargs["alert_type"],
            kwargs["content"],
            channel_delivered=kwargs.get("channel_delivered", "CLI"),
            suppressed=kwargs.get("suppressed", False),
        )
    if action == "list_alerts":
        return _SKILL.list_alerts(
            limit=kwargs.get("limit", 20),
            tier=kwargs.get("tier"),
            unacknowledged_only=kwargs.get("unacknowledged_only", False),
        )
    if action == "record_heartbeat":
        return _SKILL.record_heartbeat(kwargs["interaction_type"], kwargs.get("channel", "CLI"))
    if action == "generate_digest":
        return _SKILL.generate_digest(
            digest_type=kwargs.get("digest_type", "daily"),
            operator_state=kwargs.get("operator_state", "ACTIVE"),
        )
    raise ValueError(f"Unknown action: {action}")
