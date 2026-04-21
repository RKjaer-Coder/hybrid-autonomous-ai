from __future__ import annotations

import datetime
import json
import logging
import sqlite3
import uuid
from typing import Any, Optional

from financial_router.router import (
    _DEFAULT_RESERVATIONS,
    commit_paid_reservation,
    route_task,
)
from financial_router.types import (
    BudgetState,
    CostStatus,
    DispatchStatus,
    G3Path,
    G3RequestStatus,
    JWTClaims,
    ModelInfo,
    RoutingDecision,
    RoutingTier,
    TaskMetadata,
)
from skills.db_manager import DatabaseManager

LOGGER = logging.getLogger(__name__)


class FinancialRouterSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def route(self, task: TaskMetadata, models: list[ModelInfo], budget: BudgetState, jwt: JWTClaims) -> RoutingDecision:
        now = self._utc_now()
        self.expire_stale_g3_requests(reference_time=now)
        correlation_id = task.idempotency_key or task.task_id
        financial = self._db.get_connection("financial_ledger")
        existing = self._latest_routing_row(financial, correlation_id)
        if existing is not None:
            return self._decision_from_row(existing)

        decision = route_task(task, models, budget, jwt)
        g3_status = None
        approval_request_id = None
        dispatch_status = DispatchStatus.NOT_APPLICABLE.value
        if decision.tier == RoutingTier.PAID_CLOUD:
            if decision.requires_operator_approval:
                g3_status = G3RequestStatus.PENDING.value
                dispatch_status = DispatchStatus.AWAITING_APPROVAL.value
            else:
                g3_status = G3RequestStatus.APPROVED.value
                dispatch_status = DispatchStatus.APPROVED_PENDING_DISPATCH.value

        decision_id = str(uuid.uuid4())
        financial.execute(
            """
            INSERT INTO routing_decisions (
                decision_id, task_id, project_id, session_id, chain_id, correlation_id,
                role, route_selected, model_used, commercial_use_ok, quality_warning,
                cost_usd, cost_status, justification, g3_required, g3_status,
                reservation_id, created_at, approval_request_id, dispatch_status,
                dispatched_at, finalized_at, final_cost_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_id,
                task.task_id,
                task.project_id,
                jwt.session_id,
                task.idempotency_key or task.task_id,
                correlation_id,
                "Primary Reasoning",
                decision.tier.value,
                decision.model_id,
                1,
                1 if decision.quality_warning else 0,
                decision.estimated_cost_usd,
                CostStatus.NOT_APPLICABLE.value,
                decision.justification,
                1 if decision.requires_operator_approval else 0,
                g3_status,
                None,
                now,
                approval_request_id,
                dispatch_status,
                None,
                None,
                None,
            ),
        )

        if decision.tier == RoutingTier.PAID_CLOUD and decision.requires_operator_approval:
            approval_request_id = self._create_g3_request(
                financial=financial,
                task=task,
                jwt=jwt,
                decision_id=decision_id,
                decision=decision,
                correlation_id=correlation_id,
                requested_at=now,
                timeout_hours=budget.g3_timeout_hours,
            )
            financial.execute(
                "UPDATE routing_decisions SET approval_request_id = ? WHERE decision_id = ?",
                (approval_request_id, decision_id),
            )

        financial.commit()
        return decision

    def list_g3_approval_requests(
        self,
        *,
        limit: int = 20,
        status: str | None = None,
        reference_time: str | None = None,
    ) -> list[dict[str, Any]]:
        now = self._resolve_now(reference_time)
        self.expire_stale_g3_requests(reference_time=now)
        financial = self._db.get_connection("financial_ledger")
        where: list[str] = []
        params: list[object] = []
        if status:
            where.append("status = ?")
            params.append(status.upper())
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = financial.execute(
            f"""
            SELECT *
            FROM g3_approval_requests
            {where_sql}
            ORDER BY requested_at DESC, request_id DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [self._g3_request_row_to_dict(row) for row in rows]

    def g3_request_summary(
        self,
        *,
        reference_time: str | None = None,
        recent_limit: int = 3,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        self.expire_stale_g3_requests(reference_time=now)
        financial = self._db.get_connection("financial_ledger")
        since = self._to_iso(self._parse_ts(now) - datetime.timedelta(hours=24))
        counts = financial.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN status = 'APPROVED' AND responded_at >= ? THEN 1 ELSE 0 END) AS approved_24h,
                SUM(CASE WHEN status = 'DENIED' AND responded_at >= ? THEN 1 ELSE 0 END) AS denied_24h,
                SUM(CASE WHEN status = 'EXPIRED' AND responded_at >= ? THEN 1 ELSE 0 END) AS expired_24h
            FROM g3_approval_requests
            """,
            (since, since, since),
        ).fetchone()
        recent = self.list_g3_approval_requests(limit=recent_limit, reference_time=now)
        return {
            "pending_count": int(counts["pending_count"] or 0),
            "approved_24h": int(counts["approved_24h"] or 0),
            "denied_24h": int(counts["denied_24h"] or 0),
            "expired_24h": int(counts["expired_24h"] or 0),
            "recent": recent,
            "timestamp": now,
        }

    def review_g3_approval_request(
        self,
        request_id: str,
        decision: str,
        *,
        operator_notes: str | None = None,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        self.expire_stale_g3_requests(reference_time=now)
        normalized = decision.upper()
        status_map = {
            "APPROVE": G3RequestStatus.APPROVED.value,
            "DENY": G3RequestStatus.DENIED.value,
            "EXPIRE": G3RequestStatus.EXPIRED.value,
        }
        if normalized not in status_map:
            raise ValueError(f"Unknown G3 review decision: {decision}")

        financial = self._db.get_connection("financial_ledger")
        row = financial.execute(
            "SELECT * FROM g3_approval_requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            raise KeyError(request_id)
        if row["status"] != G3RequestStatus.PENDING.value:
            return self._g3_request_row_to_dict(row)

        self._apply_g3_request_status(
            financial=financial,
            request_row=row,
            status=status_map[normalized],
            operator_notes=operator_notes,
            responded_at=now,
        )
        financial.commit()
        updated = financial.execute(
            "SELECT * FROM g3_approval_requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        assert updated is not None
        return self._g3_request_row_to_dict(updated)

    def expire_stale_g3_requests(self, *, reference_time: str | None = None) -> list[dict[str, Any]]:
        now = self._resolve_now(reference_time)
        financial = self._db.get_connection("financial_ledger")
        rows = financial.execute(
            """
            SELECT *
            FROM g3_approval_requests
            WHERE status = 'PENDING' AND expires_at <= ?
            ORDER BY expires_at ASC, request_id ASC
            """,
            (now,),
        ).fetchall()
        if not rows:
            return []
        updated: list[dict[str, Any]] = []
        for row in rows:
            self._apply_g3_request_status(
                financial=financial,
                request_row=row,
                status=G3RequestStatus.EXPIRED.value,
                operator_notes="Timed out before operator response.",
                responded_at=now,
            )
            updated.append({**self._g3_request_row_to_dict(row), "status": G3RequestStatus.EXPIRED.value, "responded_at": now})
        financial.commit()
        return updated

    def dispatch_approved_paid_route(
        self,
        *,
        correlation_id: str,
        jwt: JWTClaims,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        self.expire_stale_g3_requests(reference_time=now)
        financial = self._db.get_connection("financial_ledger")
        route_row = self._latest_routing_row(financial, correlation_id)
        if route_row is None:
            raise KeyError(correlation_id)
        if route_row["route_selected"] != RoutingTier.PAID_CLOUD.value:
            raise ValueError("Only paid_cloud routes can be dispatched.")
        if route_row["session_id"] != jwt.session_id:
            raise ValueError("JWT session does not match the routed session.")
        if route_row["g3_status"] != G3RequestStatus.APPROVED.value:
            raise ValueError("Only approved paid_cloud routes can be dispatched.")
        if route_row["dispatch_status"] == DispatchStatus.AWAITING_APPROVAL.value:
            raise ValueError("Path B paid route is still awaiting operator approval.")
        if route_row["dispatch_status"] in {DispatchStatus.DENIED.value, DispatchStatus.EXPIRED.value}:
            raise ValueError("Route is no longer dispatchable.")

        estimated_cost = float(route_row["cost_usd"] or 0.0)
        reservation_id = route_row["reservation_id"] or correlation_id
        if route_row["dispatch_status"] != DispatchStatus.DISPATCHED.value:
            reserved = _DEFAULT_RESERVATIONS.reserve(
                session_id=jwt.session_id,
                request_id=reservation_id,
                current_spend=jwt.current_session_spend_usd,
                cap=jwt.max_api_spend_usd,
                amount=estimated_cost,
            )
            if not reserved:
                raise RuntimeError("Atomic reservation rejected for paid dispatch.")

        cost_row = self._latest_cost_row(financial, correlation_id)
        if cost_row is None:
            cost_record_id = str(uuid.uuid4())
            financial.execute(
                """
                INSERT INTO cost_records (
                    record_id, project_id, cost_category, amount_usd, description,
                    provider, task_id, correlation_id, route_decision_id, cost_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cost_record_id,
                    self._project_id_for_cost(financial, route_row["project_id"]),
                    "cloud_api",
                    estimated_cost,
                    "Estimated paid cloud dispatch reserved before final reconciliation",
                    route_row["model_used"],
                    route_row["task_id"],
                    correlation_id,
                    route_row["decision_id"],
                    CostStatus.ESTIMATED.value,
                    now,
                ),
            )
        else:
            cost_record_id = str(cost_row["record_id"])
            if cost_row["cost_status"] == CostStatus.DISPUTED.value:
                raise ValueError("Disputed paid routes cannot be dispatched again without operator recovery.")
            financial.execute(
                """
                UPDATE cost_records
                SET amount_usd = ?, cost_status = ?, route_decision_id = COALESCE(route_decision_id, ?)
                WHERE record_id = ?
                """,
                (estimated_cost, CostStatus.ESTIMATED.value, route_row["decision_id"], cost_record_id),
            )

        financial.execute(
            """
            UPDATE routing_decisions
            SET reservation_id = ?,
                dispatch_status = ?,
                dispatched_at = COALESCE(dispatched_at, ?),
                cost_status = ?
            WHERE decision_id = ?
            """,
            (
                reservation_id,
                DispatchStatus.DISPATCHED.value,
                now,
                CostStatus.ESTIMATED.value,
                route_row["decision_id"],
            ),
        )
        financial.commit()
        return {
            "correlation_id": correlation_id,
            "route_decision_id": route_row["decision_id"],
            "cost_record_id": cost_record_id,
            "reservation_id": reservation_id,
            "dispatch_status": DispatchStatus.DISPATCHED.value,
            "cost_status": CostStatus.ESTIMATED.value,
            "estimated_cost_usd": estimated_cost,
            "dispatched_at": now,
        }

    def finalize_paid_dispatch(
        self,
        *,
        correlation_id: str,
        final_cost_usd: float,
        provider: str | None = None,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        if final_cost_usd < 0:
            raise ValueError("Final paid cost must be non-negative.")
        now = self._resolve_now(reference_time)
        financial = self._db.get_connection("financial_ledger")
        route_row = self._latest_routing_row(financial, correlation_id)
        if route_row is None:
            raise KeyError(correlation_id)
        if route_row["route_selected"] != RoutingTier.PAID_CLOUD.value:
            raise ValueError("Only paid_cloud routes can be finalized.")
        if route_row["dispatch_status"] not in {DispatchStatus.DISPATCHED.value, DispatchStatus.FINALIZED.value}:
            raise ValueError("Paid route must be dispatched before finalization.")
        cost_row = self._latest_cost_row(financial, correlation_id)
        if cost_row is None:
            raise KeyError(correlation_id)
        if cost_row["cost_status"] == CostStatus.DISPUTED.value:
            raise ValueError("Disputed paid routes cannot be finalized until dispute review completes.")
        existing_amount = float(cost_row["amount_usd"] or 0.0)
        if route_row["dispatch_status"] == DispatchStatus.FINALIZED.value and cost_row["cost_status"] == CostStatus.FINAL.value:
            if abs(existing_amount - final_cost_usd) > 1e-9:
                raise ValueError("Paid route already finalized with a different final cost.")
            return {
                "correlation_id": correlation_id,
                "route_decision_id": route_row["decision_id"],
                "cost_record_id": cost_row["record_id"],
                "dispatch_status": DispatchStatus.FINALIZED.value,
                "cost_status": CostStatus.FINAL.value,
                "final_cost_usd": existing_amount,
                "finalized_at": route_row["finalized_at"],
            }

        financial.execute(
            """
            UPDATE cost_records
            SET amount_usd = ?,
                provider = COALESCE(?, provider),
                cost_status = ?
            WHERE record_id = ?
            """,
            (final_cost_usd, provider, CostStatus.FINAL.value, cost_row["record_id"]),
        )
        financial.execute(
            """
            UPDATE routing_decisions
            SET dispatch_status = ?,
                finalized_at = COALESCE(finalized_at, ?),
                final_cost_usd = ?,
                cost_status = ?
            WHERE decision_id = ?
            """,
            (
                DispatchStatus.FINALIZED.value,
                now,
                final_cost_usd,
                CostStatus.FINAL.value,
                route_row["decision_id"],
            ),
        )
        financial.commit()

        reservation_id = route_row["reservation_id"] or correlation_id
        if not commit_paid_reservation(route_row["session_id"], reservation_id):
            LOGGER.warning(
                "paid_reservation_commit_failed_after_finalization",
                extra={"correlation_id": correlation_id, "reservation_id": reservation_id},
            )
        return {
            "correlation_id": correlation_id,
            "route_decision_id": route_row["decision_id"],
            "cost_record_id": cost_row["record_id"],
            "dispatch_status": DispatchStatus.FINALIZED.value,
            "cost_status": CostStatus.FINAL.value,
            "final_cost_usd": final_cost_usd,
            "finalized_at": now,
        }

    def quarantine_inflight_paid_response(
        self,
        *,
        correlation_id: str,
        response_payload: dict | list | str,
        received_at: str | None = None,
    ) -> dict:
        financial = self._db.get_connection("financial_ledger")
        immune = self._db.get_connection("immune")
        now = self._utc_now()
        received_ts = received_at or now
        route_row = self._latest_routing_row(financial, correlation_id)
        if route_row is None:
            raise KeyError(correlation_id)
        if route_row["route_selected"] != RoutingTier.PAID_CLOUD.value:
            raise ValueError("Only paid_cloud decisions can be quarantined as disputed in-flight calls.")
        if route_row["g3_status"] != G3RequestStatus.APPROVED.value:
            raise ValueError("Only approved paid_cloud decisions can be quarantined as disputed in-flight calls.")
        if route_row["dispatch_status"] != DispatchStatus.DISPATCHED.value:
            raise ValueError("Only dispatched in-flight paid_cloud decisions can be quarantined as disputed.")

        amount_usd = float(route_row["final_cost_usd"] or route_row["cost_usd"] or 0.0)
        project_id = self._project_id_for_cost(financial, route_row["project_id"])
        cost_row = self._latest_cost_row(financial, correlation_id)
        if cost_row is None:
            cost_record_id = str(uuid.uuid4())
            financial.execute(
                """
                INSERT INTO cost_records (
                    record_id, project_id, cost_category, amount_usd, description,
                    provider, task_id, correlation_id, route_decision_id, cost_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cost_record_id,
                    project_id,
                    "cloud_api",
                    amount_usd,
                    "Interrupted paid cloud response quarantined during SECURITY_CASCADE",
                    route_row["model_used"],
                    route_row["task_id"],
                    correlation_id,
                    route_row["decision_id"],
                    CostStatus.DISPUTED.value,
                    now,
                ),
            )
        else:
            cost_record_id = str(cost_row["record_id"])
            financial.execute(
                """
                UPDATE cost_records
                SET cost_status = ?, amount_usd = ?, route_decision_id = COALESCE(route_decision_id, ?)
                WHERE record_id = ?
                """,
                (CostStatus.DISPUTED.value, amount_usd, route_row["decision_id"], cost_record_id),
            )

        financial.execute(
            """
            UPDATE routing_decisions
            SET cost_status = ?
            WHERE decision_id = ?
            """,
            (CostStatus.DISPUTED.value, route_row["decision_id"]),
        )
        financial.commit()

        payload_format = "text" if isinstance(response_payload, str) else "json"
        payload_text = response_payload if isinstance(response_payload, str) else json.dumps(response_payload, sort_keys=True, separators=(",", ":"))
        quarantine_id: str | None = None
        quarantine_persisted = False
        try:
            quarantine_id = str(uuid.uuid4())
            immune.execute(
                """
                INSERT INTO quarantined_responses (
                    quarantine_id, correlation_id, session_id, project_id, task_id,
                    route_decision_id, cost_record_id, reservation_id, source_breaker,
                    provider, model_used, payload_format, payload_text, received_at,
                    quarantined_at, review_status, operator_decision, review_notes,
                    review_digest_id, reviewed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    quarantine_id,
                    correlation_id,
                    route_row["session_id"],
                    route_row["project_id"],
                    route_row["task_id"],
                    route_row["decision_id"],
                    cost_record_id,
                    route_row["reservation_id"],
                    "SECURITY_CASCADE",
                    route_row["model_used"],
                    route_row["model_used"],
                    payload_format,
                    payload_text,
                    received_ts,
                    now,
                    "PENDING",
                    None,
                    None,
                    None,
                    None,
                ),
            )
            immune.commit()
            quarantine_persisted = True
        except sqlite3.DatabaseError:
            LOGGER.warning(
                "quarantine_persistence_failed",
                extra={"correlation_id": correlation_id, "route_decision_id": route_row["decision_id"]},
                exc_info=True,
            )

        return {
            "correlation_id": correlation_id,
            "route_decision_id": route_row["decision_id"],
            "cost_record_id": cost_record_id,
            "quarantine_id": quarantine_id,
            "quarantine_persisted": quarantine_persisted,
            "cost_status": CostStatus.DISPUTED.value,
            "amount_usd": amount_usd,
            "received_at": received_ts,
            "quarantined_at": now if quarantine_persisted else None,
        }

    def _create_g3_request(
        self,
        *,
        financial,
        task: TaskMetadata,
        jwt: JWTClaims,
        decision_id: str,
        decision: RoutingDecision,
        correlation_id: str,
        requested_at: str,
        timeout_hours: float,
    ) -> str:
        expires_at = self._to_iso(self._parse_ts(requested_at) + datetime.timedelta(hours=max(0.0, timeout_hours)))
        request_id = str(uuid.uuid4())
        gate_id = str(uuid.uuid4())
        financial.execute(
            """
            INSERT INTO g3_approval_requests (
                request_id, correlation_id, project_id, session_id, task_id,
                route_decision_id, gate_id, requested_model, estimated_cost_usd,
                justification, requested_at, expires_at, status, operator_notes, responded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                correlation_id,
                task.project_id,
                jwt.session_id,
                task.task_id,
                decision_id,
                gate_id,
                decision.model_id,
                decision.estimated_cost_usd,
                decision.justification,
                requested_at,
                expires_at,
                G3RequestStatus.PENDING.value,
                None,
                None,
            ),
        )

        operator = self._db.get_connection("operator_digest")
        operator.execute(
            """
            INSERT INTO gate_log (
                gate_id, gate_type, trigger_description, context_packet, project_id,
                status, timeout_hours, operator_response, created_at, responded_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gate_id,
                "G3",
                "Per-call paid cloud approval required outside approved project budget.",
                json.dumps(
                    {
                        "approval_request_id": request_id,
                        "correlation_id": correlation_id,
                        "project_id": task.project_id,
                        "session_id": jwt.session_id,
                        "task_id": task.task_id,
                        "requested_model": decision.model_id,
                        "estimated_cost_usd": decision.estimated_cost_usd,
                        "justification": decision.justification,
                    },
                    sort_keys=True,
                ),
                task.project_id,
                "PENDING",
                max(0.0, timeout_hours),
                None,
                requested_at,
                None,
                expires_at,
            ),
        )
        operator.commit()
        return request_id

    def _apply_g3_request_status(
        self,
        *,
        financial,
        request_row,
        status: str,
        operator_notes: str | None,
        responded_at: str,
    ) -> None:
        route_status, dispatch_status, gate_status, operator_response = self._request_status_transitions(status)
        financial.execute(
            """
            UPDATE g3_approval_requests
            SET status = ?,
                operator_notes = ?,
                responded_at = ?
            WHERE request_id = ?
            """,
            (status, operator_notes, responded_at, request_row["request_id"]),
        )
        financial.execute(
            """
            UPDATE routing_decisions
            SET g3_status = ?,
                dispatch_status = ?
            WHERE decision_id = ?
            """,
            (route_status, dispatch_status, request_row["route_decision_id"]),
        )
        operator = self._db.get_connection("operator_digest")
        if request_row["gate_id"]:
            operator.execute(
                """
                UPDATE gate_log
                SET status = ?,
                    operator_response = ?,
                    responded_at = ?
                WHERE gate_id = ?
                """,
                (gate_status, operator_response, responded_at, request_row["gate_id"]),
            )
            operator.commit()

    @staticmethod
    def _request_status_transitions(status: str) -> tuple[str, str, str, str]:
        if status == G3RequestStatus.APPROVED.value:
            return (
                G3RequestStatus.APPROVED.value,
                DispatchStatus.APPROVED_PENDING_DISPATCH.value,
                "APPROVED",
                "APPROVE",
            )
        if status == G3RequestStatus.DENIED.value:
            return (
                "BLOCKED",
                DispatchStatus.DENIED.value,
                "REJECTED",
                "DENY",
            )
        if status == G3RequestStatus.EXPIRED.value:
            return (
                G3RequestStatus.EXPIRED.value,
                DispatchStatus.EXPIRED.value,
                "EXPIRED",
                "EXPIRE",
            )
        raise ValueError(f"Unsupported G3 request status transition: {status}")

    @staticmethod
    def _decision_from_row(row) -> RoutingDecision:
        if row["route_selected"] == RoutingTier.PAID_CLOUD.value:
            g3_path = G3Path.OUTSIDE_BUDGET if bool(row["g3_required"]) else G3Path.WITHIN_BUDGET
        else:
            g3_path = G3Path.NOT_APPLICABLE
        return RoutingDecision(
            tier=RoutingTier(row["route_selected"]),
            model_id=row["model_used"],
            g3_path=g3_path,
            estimated_cost_usd=float(row["cost_usd"] or 0.0),
            quality_warning=bool(row["quality_warning"]),
            justification=row["justification"] or "",
            skipped_reasons={},
            requires_operator_approval=bool(row["g3_required"]),
            compute_starved=row["route_selected"] == RoutingTier.COMPUTE_STARVED.value,
            reservation_id=row["reservation_id"],
        )

    @staticmethod
    def _g3_request_row_to_dict(row) -> dict[str, Any]:
        return {
            "request_id": row["request_id"],
            "correlation_id": row["correlation_id"],
            "project_id": row["project_id"],
            "session_id": row["session_id"],
            "task_id": row["task_id"],
            "route_decision_id": row["route_decision_id"],
            "gate_id": row["gate_id"],
            "requested_model": row["requested_model"],
            "estimated_cost_usd": float(row["estimated_cost_usd"]),
            "justification": row["justification"],
            "requested_at": row["requested_at"],
            "expires_at": row["expires_at"],
            "status": row["status"],
            "operator_notes": row["operator_notes"],
            "responded_at": row["responded_at"],
        }

    @staticmethod
    def _latest_routing_row(conn, correlation_id: str | None):
        if not correlation_id:
            return None
        return conn.execute(
            """
            SELECT *
            FROM routing_decisions
            WHERE correlation_id = ?
            ORDER BY created_at DESC, decision_id DESC
            LIMIT 1
            """,
            (correlation_id,),
        ).fetchone()

    @staticmethod
    def _latest_cost_row(conn, correlation_id: str):
        return conn.execute(
            """
            SELECT *
            FROM cost_records
            WHERE correlation_id = ?
            ORDER BY created_at DESC, record_id DESC
            LIMIT 1
            """,
            (correlation_id,),
        ).fetchone()

    @staticmethod
    def _project_id_for_cost(conn, project_id: str | None) -> str | None:
        if not project_id:
            return None
        row = conn.execute(
            "SELECT 1 FROM projects WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        return project_id if row is not None else None

    @staticmethod
    def _parse_ts(value: str) -> datetime.datetime:
        dt = datetime.datetime.fromisoformat(value)
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=datetime.timezone.utc)

    @staticmethod
    def _to_iso(value: datetime.datetime) -> str:
        return value.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat()

    def _resolve_now(self, reference_time: str | None) -> str:
        return self._utc_now() if reference_time is None else self._to_iso(self._parse_ts(reference_time))

    @staticmethod
    def _utc_now() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


_SKILL: Optional[FinancialRouterSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = FinancialRouterSkill(db_manager)


def financial_router_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("financial router skill not configured")
    if action == "route":
        return _SKILL.route(kwargs["task"], kwargs["models"], kwargs["budget"], kwargs["jwt"])
    if action == "list_g3_approval_requests":
        return _SKILL.list_g3_approval_requests(
            limit=kwargs.get("limit", 20),
            status=kwargs.get("status"),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "review_g3_approval_request":
        return _SKILL.review_g3_approval_request(
            kwargs["request_id"],
            kwargs["decision"],
            operator_notes=kwargs.get("operator_notes"),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "expire_stale_g3_requests":
        return _SKILL.expire_stale_g3_requests(reference_time=kwargs.get("reference_time"))
    if action == "dispatch_approved_paid_route":
        return _SKILL.dispatch_approved_paid_route(
            correlation_id=kwargs["correlation_id"],
            jwt=kwargs["jwt"],
            reference_time=kwargs.get("reference_time"),
        )
    if action == "finalize_paid_dispatch":
        return _SKILL.finalize_paid_dispatch(
            correlation_id=kwargs["correlation_id"],
            final_cost_usd=kwargs["final_cost_usd"],
            provider=kwargs.get("provider"),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "quarantine_inflight_paid_response":
        return _SKILL.quarantine_inflight_paid_response(
            correlation_id=kwargs["correlation_id"],
            response_payload=kwargs["response_payload"],
            received_at=kwargs.get("received_at"),
        )
    raise ValueError(f"Unknown action: {action}")
