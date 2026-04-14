from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import asdict, dataclass
from typing import Optional

from skills.db_manager import DatabaseManager


@dataclass(frozen=True)
class ResearchTaskRecord:
    task_id: str
    domain: int
    source: str
    title: str
    brief: str
    priority: str
    status: str
    output_brief_id: str | None
    tags: list[str]
    depth_upgrade: bool
    created_at: str
    updated_at: str


class ResearchDomainSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def create_task(
        self,
        title: str,
        brief: str,
        priority: str = "P2_NORMAL",
        *,
        domain: int = 2,
        source: str = "operator",
        tags: list[str] | None = None,
    ) -> str:
        task_id = str(uuid.uuid4())
        now = self._utc_now()
        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            "INSERT INTO research_tasks (task_id, domain, source, title, brief, priority, status, max_spend_usd, actual_spend_usd, output_brief_id, follow_up_tasks, stale_after, tags, depth_upgrade, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (task_id, domain, source, title, brief, priority, "PENDING", 0.0, 0.0, None, "[]", None, json.dumps(tags or []), 0, now, now),
        )
        conn.commit()
        return task_id

    def list_tasks(
        self,
        *,
        limit: int = 20,
        status: str | None = None,
        domain: int | None = None,
    ) -> list[dict]:
        conn = self._db.get_connection("strategic_memory")
        where: list[str] = []
        params: list[object] = []
        if status:
            where.append("status = ?")
            params.append(status)
        if domain is not None:
            where.append("domain = ?")
            params.append(domain)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT
                task_id, domain, source, title, brief, priority, status, output_brief_id,
                tags, depth_upgrade, created_at, updated_at
            FROM research_tasks
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [
            asdict(
                ResearchTaskRecord(
                    task_id=row["task_id"],
                    domain=row["domain"],
                    source=row["source"],
                    title=row["title"],
                    brief=row["brief"],
                    priority=row["priority"],
                    status=row["status"],
                    output_brief_id=row["output_brief_id"],
                    tags=json.loads(row["tags"]),
                    depth_upgrade=bool(row["depth_upgrade"]),
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            )
            for row in rows
        ]

    def complete_task(
        self,
        task_id: str,
        *,
        output_brief_id: str | None = None,
        actual_spend_usd: float | None = None,
    ) -> dict:
        now = self._utc_now()
        conn = self._db.get_connection("strategic_memory")
        row = conn.execute("SELECT * FROM research_tasks WHERE task_id = ?", (task_id,)).fetchone()
        if row is None:
            raise KeyError(task_id)
        conn.execute(
            """
            UPDATE research_tasks
            SET status = 'COMPLETE',
                output_brief_id = COALESCE(?, output_brief_id),
                actual_spend_usd = COALESCE(?, actual_spend_usd),
                updated_at = ?
            WHERE task_id = ?
            """,
            (output_brief_id, actual_spend_usd, now, task_id),
        )
        conn.commit()
        updated = conn.execute(
            """
            SELECT
                task_id, domain, source, title, brief, priority, status, output_brief_id,
                tags, depth_upgrade, created_at, updated_at
            FROM research_tasks WHERE task_id = ?
            """,
            (task_id,),
        ).fetchone()
        assert updated is not None
        return asdict(
            ResearchTaskRecord(
                task_id=updated["task_id"],
                domain=updated["domain"],
                source=updated["source"],
                title=updated["title"],
                brief=updated["brief"],
                priority=updated["priority"],
                status=updated["status"],
                output_brief_id=updated["output_brief_id"],
                tags=json.loads(updated["tags"]),
                depth_upgrade=bool(updated["depth_upgrade"]),
                created_at=updated["created_at"],
                updated_at=updated["updated_at"],
            )
        )

    @staticmethod
    def _utc_now() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


_SKILL: Optional[ResearchDomainSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = ResearchDomainSkill(db_manager)


def research_domain_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("research domain skill not configured")
    if action == "create_task":
        return _SKILL.create_task(
            kwargs["title"],
            kwargs["brief"],
            kwargs.get("priority", "P2_NORMAL"),
            domain=kwargs.get("domain", 2),
            source=kwargs.get("source", "operator"),
            tags=kwargs.get("tags"),
        )
    if action == "list_tasks":
        return _SKILL.list_tasks(
            limit=kwargs.get("limit", 20),
            status=kwargs.get("status"),
            domain=kwargs.get("domain"),
        )
    if action == "complete_task":
        return _SKILL.complete_task(
            kwargs["task_id"],
            output_brief_id=kwargs.get("output_brief_id"),
            actual_spend_usd=kwargs.get("actual_spend_usd"),
        )
    raise ValueError(f"Unknown action: {action}")
