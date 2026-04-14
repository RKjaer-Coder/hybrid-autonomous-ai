from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Optional

from skills.db_manager import DatabaseManager


@dataclass(frozen=True)
class IntelligenceBriefRecord:
    brief_id: str
    task_id: str
    domain: int
    title: str
    summary: str
    confidence: float
    actionability: str
    urgency: str
    depth_tier: str
    action_type: str
    tags: list[str]
    provenance_links: list[str]
    created_at: str


class StrategicMemorySkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def write_brief(
        self,
        task_id: str,
        title: str,
        summary: str,
        confidence: float = 0.5,
        *,
        domain: int = 2,
        source: str = "operator",
        actionability: str = "INFORMATIONAL",
        urgency: str = "ROUTINE",
        depth_tier: str = "QUICK",
        action_type: str = "none",
        tags: list[str] | None = None,
        provenance_links: list[str] | None = None,
        detail: str | None = None,
    ) -> str:
        brief_id = str(uuid.uuid4())
        now = self._utc_now()
        tags_json = json.dumps(tags or [])
        provenance_json = json.dumps(provenance_links or [])
        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            """
            INSERT INTO research_tasks (
                task_id, domain, source, title, brief, priority, status,
                max_spend_usd, actual_spend_usd, output_brief_id, follow_up_tasks,
                stale_after, tags, depth_upgrade, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                domain=excluded.domain,
                source=excluded.source,
                title=excluded.title,
                brief=excluded.brief,
                tags=excluded.tags,
                updated_at=excluded.updated_at
            """,
            (task_id, domain, source, title, summary, "P2_NORMAL", "PENDING", 0.0, 0.0, brief_id, "[]", None, tags_json, 0, now, now),
        )
        conn.execute(
            "INSERT INTO intelligence_briefs (brief_id, task_id, domain, title, summary, detail, source_urls, source_assessments, confidence, uncertainty_statement, counter_thesis, actionability, urgency, depth_tier, action_type, spawned_tasks, spawned_opportunity_id, related_brief_ids, tags, quality_warning, source_diversity_hold, provenance_links, trust_tier, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                brief_id,
                task_id,
                domain,
                title,
                summary,
                detail,
                "[]",
                "[]",
                confidence,
                None,
                None,
                actionability,
                urgency,
                depth_tier,
                action_type,
                "[]",
                None,
                "[]",
                tags_json,
                0,
                0,
                provenance_json,
                3,
                now,
            ),
        )
        conn.commit()
        return brief_id

    def read_brief(self, brief_id: str) -> dict[str, Any]:
        conn = self._db.get_connection("strategic_memory")
        row = conn.execute(
            """
            SELECT
                brief_id, task_id, domain, title, summary, confidence, actionability,
                urgency, depth_tier, action_type, tags, provenance_links, created_at
            FROM intelligence_briefs
            WHERE brief_id = ?
            """,
            (brief_id,),
        ).fetchone()
        if row is None:
            raise KeyError(brief_id)
        return asdict(
            IntelligenceBriefRecord(
                brief_id=row["brief_id"],
                task_id=row["task_id"],
                domain=row["domain"],
                title=row["title"],
                summary=row["summary"],
                confidence=row["confidence"],
                actionability=row["actionability"],
                urgency=row["urgency"],
                depth_tier=row["depth_tier"],
                action_type=row["action_type"],
                tags=json.loads(row["tags"]),
                provenance_links=json.loads(row["provenance_links"]),
                created_at=row["created_at"],
            )
        )

    def list_briefs(
        self,
        *,
        limit: int = 20,
        task_id: str | None = None,
        actionability: str | None = None,
    ) -> list[dict[str, Any]]:
        conn = self._db.get_connection("strategic_memory")
        where: list[str] = []
        params: list[Any] = []
        if task_id:
            where.append("task_id = ?")
            params.append(task_id)
        if actionability:
            where.append("actionability = ?")
            params.append(actionability)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT
                brief_id, task_id, domain, title, summary, confidence, actionability,
                urgency, depth_tier, action_type, tags, provenance_links, created_at
            FROM intelligence_briefs
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [
            asdict(
                IntelligenceBriefRecord(
                    brief_id=row["brief_id"],
                    task_id=row["task_id"],
                    domain=row["domain"],
                    title=row["title"],
                    summary=row["summary"],
                    confidence=row["confidence"],
                    actionability=row["actionability"],
                    urgency=row["urgency"],
                    depth_tier=row["depth_tier"],
                    action_type=row["action_type"],
                    tags=json.loads(row["tags"]),
                    provenance_links=json.loads(row["provenance_links"]),
                    created_at=row["created_at"],
                )
            )
            for row in rows
        ]

    @staticmethod
    def _utc_now() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


_SKILL: Optional[StrategicMemorySkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = StrategicMemorySkill(db_manager)


def strategic_memory_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("strategic memory skill not configured")
    if action == "write_brief":
        return _SKILL.write_brief(
            kwargs["task_id"],
            kwargs["title"],
            kwargs["summary"],
            kwargs.get("confidence", 0.5),
            domain=kwargs.get("domain", 2),
            source=kwargs.get("source", "operator"),
            actionability=kwargs.get("actionability", "INFORMATIONAL"),
            urgency=kwargs.get("urgency", "ROUTINE"),
            depth_tier=kwargs.get("depth_tier", "QUICK"),
            action_type=kwargs.get("action_type", "none"),
            tags=kwargs.get("tags"),
            provenance_links=kwargs.get("provenance_links"),
            detail=kwargs.get("detail"),
        )
    if action == "read_brief":
        return _SKILL.read_brief(kwargs["brief_id"])
    if action == "list_briefs":
        return _SKILL.list_briefs(
            limit=kwargs.get("limit", 20),
            task_id=kwargs.get("task_id"),
            actionability=kwargs.get("actionability"),
        )
    raise ValueError(f"Unknown action: {action}")
