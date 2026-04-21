from __future__ import annotations

import datetime
import json
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from typing import Any


_REQUIRED_TABLES = {
    "execution_traces",
    "harness_variants",
}


def _parse_ts(value: str) -> datetime.datetime:
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def _to_iso(value: datetime.datetime) -> str:
    return value.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


@dataclass(frozen=True)
class ExecutionTraceStep:
    step_index: int
    tool_call: str
    tool_result: str
    tool_result_file: str | None
    tokens_in: int
    tokens_out: int
    latency_ms: int
    model_used: str
    model_switch: dict[str, str] | None = None


@dataclass(frozen=True)
class ExecutionTrace:
    trace_id: str
    task_id: str
    role: str
    skill_name: str
    harness_version: str
    intent_goal: str
    steps: list[ExecutionTraceStep]
    prompt_template: str
    context_assembled: str
    retrieval_queries: list[str]
    judge_verdict: str
    judge_reasoning: str
    outcome_score: float
    cost_usd: float
    duration_ms: int
    training_eligible: bool
    retention_class: str
    source_chain_id: str | None
    source_session_id: str | None
    source_trace_id: str | None
    created_at: str


@dataclass(frozen=True)
class HarnessVariant:
    variant_id: str
    skill_name: str
    parent_version: str
    diff: str
    source: str
    status: str
    created_at: str
    prompt_prelude: str = ""
    retrieval_strategy_diff: str = ""
    scoring_formula_diff: str = ""
    context_assembly_diff: str = ""
    touches_infrastructure: bool = False
    reject_reason: str | None = None
    eval_result: dict[str, Any] | None = None
    promoted_at: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True)
class VariantEvalResult:
    variant_id: str
    skill_name: str
    benchmark_name: str
    baseline_outcome_scores: list[float]
    variant_outcome_scores: list[float]
    regression_rate: float
    gate_0_pass: bool
    known_bad_block_rate: float
    gate_1_pass: bool
    baseline_mean_score: float
    variant_mean_score: float
    quality_delta: float
    gate_2_pass: bool
    baseline_std: float
    variant_std: float
    gate_3_pass: bool
    regressed_trace_count: int
    improved_trace_count: int
    net_trace_gain: int
    traces_evaluated: int
    compute_cost_cu: float
    eval_duration_ms: int
    created_at: str

    @property
    def all_gates_pass(self) -> bool:
        return self.gate_0_pass and self.gate_1_pass and self.gate_2_pass and self.gate_3_pass

    def ranking_key(self) -> tuple[int, float, int, float]:
        return (
            -self.regressed_trace_count,
            round(self.quality_delta, 10),
            self.net_trace_gain,
            round(-self.eval_duration_ms / 1000.0, 10),
        )


class HarnessVariantManager:
    """Persistence and lifecycle manager for initial §8.3b telemetry substrate."""

    def __init__(self, telemetry_db_path: str):
        self._telemetry_db_path = telemetry_db_path
        self._available = self._verify_tables()

    @property
    def available(self) -> bool:
        return self._available

    def log_execution_trace(self, trace: ExecutionTrace) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Execution trace tables are not available")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO execution_traces (
                    trace_id, task_id, role, skill_name, harness_version, intent_goal,
                    steps_json, prompt_template, context_assembled, retrieval_queries_json,
                    judge_verdict, judge_reasoning, outcome_score, cost_usd, duration_ms,
                    training_eligible, retention_class, source_chain_id, source_session_id,
                    source_trace_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace.trace_id,
                    trace.task_id,
                    trace.role,
                    trace.skill_name,
                    trace.harness_version,
                    trace.intent_goal,
                    _json([asdict(step) for step in trace.steps]),
                    trace.prompt_template,
                    trace.context_assembled,
                    _json(trace.retrieval_queries),
                    trace.judge_verdict,
                    trace.judge_reasoning,
                    trace.outcome_score,
                    trace.cost_usd,
                    trace.duration_ms,
                    1 if trace.training_eligible else 0,
                    trace.retention_class,
                    trace.source_chain_id,
                    trace.source_session_id,
                    trace.source_trace_id,
                    trace.created_at,
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM execution_traces WHERE trace_id = ? LIMIT 1",
                (trace.trace_id,),
            ).fetchone()
        assert row is not None
        return self._trace_row_to_dict(row)

    def list_execution_traces(
        self,
        *,
        limit: int = 20,
        skill_name: str | None = None,
        training_eligible: bool | None = None,
        judge_verdict: str | None = None,
    ) -> list[dict[str, Any]]:
        if not self._available:
            return []
        where: list[str] = []
        params: list[object] = []
        if skill_name is not None:
            where.append("skill_name = ?")
            params.append(skill_name)
        if training_eligible is not None:
            where.append("training_eligible = ?")
            params.append(1 if training_eligible else 0)
        if judge_verdict is not None:
            where.append("judge_verdict = ?")
            params.append(judge_verdict)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM execution_traces
                {where_sql}
                ORDER BY created_at DESC, trace_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._trace_row_to_dict(row) for row in rows]

    def execution_trace_summary(self) -> dict[str, Any]:
        if not self._available:
            return {
                "available": False,
                "total_count": 0,
                "training_eligible_count": 0,
                "failure_audit_count": 0,
                "recent": [],
            }
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN training_eligible = 1 THEN 1 ELSE 0 END) AS training_eligible_count,
                    SUM(CASE WHEN retention_class = 'FAILURE_AUDIT' THEN 1 ELSE 0 END) AS failure_audit_count
                FROM execution_traces
                """
            ).fetchone()
        return {
            "available": True,
            "total_count": int(row["total_count"] or 0),
            "training_eligible_count": int(row["training_eligible_count"] or 0),
            "failure_audit_count": int(row["failure_audit_count"] or 0),
            "recent": self.list_execution_traces(limit=3),
        }

    def propose_variant(
        self,
        *,
        skill_name: str,
        parent_version: str,
        diff: str,
        source: str,
        prompt_prelude: str = "",
        retrieval_strategy_diff: str = "",
        scoring_formula_diff: str = "",
        context_assembly_diff: str = "",
        touches_infrastructure: bool = False,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Harness variant tables are not available")
        now = self._now(reference_time)
        status = "PROPOSED"
        reject_reason = None
        if touches_infrastructure:
            status = "REJECTED"
            reject_reason = "SCOPE_VIOLATION"
        elif self._has_active_variant(skill_name):
            status = "REJECTED"
            reject_reason = "CONCURRENT_VARIANT"
        elif self._variant_created_since(skill_name, _to_iso(_parse_ts(now) - datetime.timedelta(hours=24))):
            status = "REJECTED"
            reject_reason = "RATE_LIMITED"

        variant_id = str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO harness_variants (
                    variant_id, skill_name, parent_version, diff, source, status,
                    prompt_prelude, retrieval_strategy_diff, scoring_formula_diff,
                    context_assembly_diff, touches_infrastructure, reject_reason,
                    eval_result_json, promoted_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    variant_id,
                    skill_name,
                    parent_version,
                    diff,
                    source,
                    status,
                    prompt_prelude,
                    retrieval_strategy_diff,
                    scoring_formula_diff,
                    context_assembly_diff,
                    1 if touches_infrastructure else 0,
                    reject_reason,
                    None,
                    None,
                    now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
        assert row is not None
        return self._variant_row_to_dict(row)

    def start_shadow_eval(self, variant_id: str, *, reference_time: str | None = None) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Harness variant tables are not available")
        now = self._now(reference_time)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
            if row is None:
                raise KeyError(variant_id)
            if row["status"] != "PROPOSED":
                return self._variant_row_to_dict(row)
            conn.execute(
                """
                UPDATE harness_variants
                SET status = 'SHADOW_EVAL',
                    updated_at = ?
                WHERE variant_id = ?
                """,
                (now, variant_id),
            )
            conn.commit()
            updated = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
        assert updated is not None
        return self._variant_row_to_dict(updated)

    def record_eval_result(
        self,
        variant_id: str,
        eval_result: VariantEvalResult,
        *,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Harness variant tables are not available")
        now = self._now(reference_time)
        status = "PROMOTED" if eval_result.all_gates_pass else "REJECTED"
        promoted_at = now if eval_result.all_gates_pass else None
        reject_reason = None if eval_result.all_gates_pass else "EVAL_GATE_FAILED"
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
            if row is None:
                raise KeyError(variant_id)
            conn.execute(
                """
                UPDATE harness_variants
                SET status = ?,
                    reject_reason = COALESCE(?, reject_reason),
                    eval_result_json = ?,
                    promoted_at = ?,
                    updated_at = ?
                WHERE variant_id = ?
                """,
                (
                    status,
                    reject_reason,
                    _json(asdict(eval_result)),
                    promoted_at,
                    now,
                    variant_id,
                ),
            )
            conn.commit()
            updated = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
        assert updated is not None
        return self._variant_row_to_dict(updated)

    def list_variants(
        self,
        *,
        limit: int = 20,
        skill_name: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        if not self._available:
            return []
        where: list[str] = []
        params: list[object] = []
        if skill_name is not None:
            where.append("skill_name = ?")
            params.append(skill_name)
        if status is not None:
            where.append("status = ?")
            params.append(status)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM harness_variants
                {where_sql}
                ORDER BY created_at DESC, variant_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._variant_row_to_dict(row) for row in rows]

    def frontier(self, *, limit: int = 20, skill_name: str | None = None) -> list[dict[str, Any]]:
        if not self._available:
            return []
        where_sql = ""
        params: list[object] = []
        if skill_name is not None:
            where_sql = "WHERE skill_name = ?"
            params.append(skill_name)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM harness_frontier
                {where_sql}
                ORDER BY promoted_at DESC, variant_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def summary(self) -> dict[str, Any]:
        if not self._available:
            return {
                "available": False,
                "active_count": 0,
                "proposed_count": 0,
                "shadow_eval_count": 0,
                "promoted_count": 0,
                "rejected_24h": 0,
                "frontier": [],
                "recent": [],
            }
        cutoff = _to_iso(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24))
        with self._connect() as conn:
            counts = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status = 'PROPOSED' THEN 1 ELSE 0 END) AS proposed_count,
                    SUM(CASE WHEN status = 'SHADOW_EVAL' THEN 1 ELSE 0 END) AS shadow_eval_count,
                    SUM(CASE WHEN status = 'PROMOTED' THEN 1 ELSE 0 END) AS promoted_count,
                    SUM(CASE WHEN status = 'REJECTED' AND created_at >= ? THEN 1 ELSE 0 END) AS rejected_24h
                FROM harness_variants
                """,
                (cutoff,),
            ).fetchone()
        proposed_count = int(counts["proposed_count"] or 0)
        shadow_eval_count = int(counts["shadow_eval_count"] or 0)
        return {
            "available": True,
            "active_count": proposed_count + shadow_eval_count,
            "proposed_count": proposed_count,
            "shadow_eval_count": shadow_eval_count,
            "promoted_count": int(counts["promoted_count"] or 0),
            "rejected_24h": int(counts["rejected_24h"] or 0),
            "frontier": self.frontier(limit=3),
            "recent": self.list_variants(limit=3),
        }

    def _has_active_variant(self, skill_name: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM harness_variants
                WHERE skill_name = ? AND status IN ('PROPOSED','SHADOW_EVAL')
                LIMIT 1
                """,
                (skill_name,),
            ).fetchone()
        return row is not None

    def _variant_created_since(self, skill_name: str, cutoff: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM harness_variants
                WHERE skill_name = ? AND created_at > ?
                LIMIT 1
                """,
                (skill_name, cutoff),
            ).fetchone()
        return row is not None

    def _trace_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "trace_id": row["trace_id"],
            "task_id": row["task_id"],
            "role": row["role"],
            "skill_name": row["skill_name"],
            "harness_version": row["harness_version"],
            "intent_goal": row["intent_goal"],
            "steps": json.loads(row["steps_json"]),
            "prompt_template": row["prompt_template"],
            "context_assembled": row["context_assembled"],
            "retrieval_queries": json.loads(row["retrieval_queries_json"]),
            "judge_verdict": row["judge_verdict"],
            "judge_reasoning": row["judge_reasoning"],
            "outcome_score": float(row["outcome_score"]),
            "cost_usd": float(row["cost_usd"]),
            "duration_ms": int(row["duration_ms"]),
            "training_eligible": bool(row["training_eligible"]),
            "retention_class": row["retention_class"],
            "source_chain_id": row["source_chain_id"],
            "source_session_id": row["source_session_id"],
            "source_trace_id": row["source_trace_id"],
            "created_at": row["created_at"],
        }

    def _variant_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "variant_id": row["variant_id"],
            "skill_name": row["skill_name"],
            "parent_version": row["parent_version"],
            "diff": row["diff"],
            "source": row["source"],
            "status": row["status"],
            "prompt_prelude": row["prompt_prelude"],
            "retrieval_strategy_diff": row["retrieval_strategy_diff"],
            "scoring_formula_diff": row["scoring_formula_diff"],
            "context_assembly_diff": row["context_assembly_diff"],
            "touches_infrastructure": bool(row["touches_infrastructure"]),
            "reject_reason": row["reject_reason"],
            "eval_result": None if row["eval_result_json"] is None else json.loads(row["eval_result_json"]),
            "promoted_at": row["promoted_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._telemetry_db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _verify_tables(self) -> bool:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
        except sqlite3.DatabaseError:
            return False
        present = {row["name"] for row in rows}
        return _REQUIRED_TABLES.issubset(present)

    @staticmethod
    def _now(reference_time: str | None) -> str:
        if reference_time:
            return _to_iso(_parse_ts(reference_time))
        return _to_iso(datetime.datetime.now(datetime.timezone.utc))
