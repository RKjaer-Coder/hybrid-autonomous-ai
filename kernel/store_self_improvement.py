from __future__ import annotations

from typing import Any

from .records import (
    SelfImprovementEvalRecord,
    SelfImprovementPromotionPacket,
    SelfImprovementProposal,
    SelfImprovementReplayProjectionComparison,
    SelfImprovementRollbackRecord,
)
from .store_common import (
    _loads,
    _self_improvement_comparison_payload,
    _self_improvement_eval_payload,
    _self_improvement_promotion_payload,
    _self_improvement_proposal_payload,
    _self_improvement_rollback_payload,
    canonical_json,
)


PINNED_POLICY_AREAS = {
    "control_kernel_policy",
    "spend_rules",
    "gate_rules",
    "operator_auth",
    "event_log_schema",
    "capability_broker",
    "critical_model_promotion_thresholds",
    "security_allowlists",
    "frozen_eval_holdouts",
    "data_retention_deletion_rules",
    "side_effect_authority_rules",
}


class SelfImprovementKernelTransactionMixin:
    def record_self_improvement_proposal(self, proposal: SelfImprovementProposal) -> str:
        if not proposal.problem_evidence:
            raise ValueError("self-improvement proposals require durable problem evidence")
        for field_name in ["proposed_change", "expected_benefit", "risk_assessment", "eval_plan", "rollback_plan"]:
            if not str(getattr(proposal, field_name)).strip():
                raise ValueError(f"self-improvement proposal requires {field_name}")
        if proposal.status not in {"proposed", "eval_running"}:
            raise PermissionError("new self-improvement proposals cannot start approved, promoted, or rolled back")
        if proposal.target_type in {"workflow", "policy"} and proposal.authority_required != "operator_gate":
            raise PermissionError("workflow and policy improvement proposals require operator gate authority")
        if PINNED_POLICY_AREAS.intersection(proposal.affected_policy_areas) and proposal.authority_required != "operator_gate":
            raise PermissionError("pinned policy areas require operator gate authority")
        if self.command.requested_by in {"agent", "model"} and proposal.authority_required in {"rule", "single_agent"}:
            raise PermissionError("workers cannot assign low authority to their own improvement proposal")
        if self.command.requested_authority and self.command.requested_authority != proposal.authority_required:
            raise PermissionError("command requested authority does not match self-improvement policy")
        payload = _self_improvement_proposal_payload(proposal)
        event_id = self.append_event("self_improvement_proposal_recorded", "self_improvement", proposal.proposal_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_proposals (
              proposal_id, target_type, target_id, problem_evidence_json,
              proposed_change, expected_benefit, risk_assessment, eval_plan,
              rollback_plan, authority_required, proposer_type, proposer_id,
              affected_policy_areas_json, data_classes_json, status,
              created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                proposal.proposal_id,
                proposal.target_type,
                proposal.target_id,
                canonical_json(proposal.problem_evidence),
                proposal.proposed_change,
                proposal.expected_benefit,
                proposal.risk_assessment,
                proposal.eval_plan,
                proposal.rollback_plan,
                proposal.authority_required,
                proposal.proposer_type,
                proposal.proposer_id,
                canonical_json(proposal.affected_policy_areas),
                canonical_json(proposal.data_classes),
                proposal.status,
                proposal.created_at,
                proposal.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "self_improvement_proposal_projection")
        return proposal.proposal_id

    def record_self_improvement_eval(self, record: SelfImprovementEvalRecord) -> str:
        proposal = self.conn.execute(
            "SELECT proposal_id, authority_required, status FROM self_improvement_proposals WHERE proposal_id=?",
            (record.proposal_id,),
        ).fetchone()
        if proposal is None:
            raise ValueError("self-improvement eval requires a recorded proposal")
        if proposal["status"] in {"promoted", "rolled_back", "rejected"}:
            raise ValueError("closed self-improvement proposals cannot receive new eval records")
        if record.authority_effect != "evidence_only":
            raise PermissionError("self-improvement eval records are evidence only")
        if not record.dataset_refs:
            raise ValueError("self-improvement eval requires governed dataset or trace references")
        if "overall" not in record.metrics:
            raise ValueError("self-improvement eval metrics require an overall score")
        if record.eval_type in {"replay", "shadow"} and record.side_effect_safety.get("reexecuted_side_effects") not in {False, 0}:
            raise PermissionError("self-improvement replay/shadow evals must not re-execute side effects")
        payload = _self_improvement_eval_payload(record)
        event_id = self.append_event("self_improvement_eval_recorded", "self_improvement", record.eval_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_eval_records (
              eval_id, proposal_id, eval_type, baseline_ref, candidate_ref,
              dataset_refs_json, metrics_json, regression_thresholds_json,
              failure_examples_json, side_effect_safety_json, status,
              authority_effect, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.eval_id,
                record.proposal_id,
                record.eval_type,
                record.baseline_ref,
                record.candidate_ref,
                canonical_json(record.dataset_refs),
                canonical_json(record.metrics),
                canonical_json(record.regression_thresholds),
                canonical_json(record.failure_examples),
                canonical_json(record.side_effect_safety),
                record.status,
                record.authority_effect,
                record.created_at,
            ),
        )
        self.conn.execute(
            "UPDATE self_improvement_proposals SET status='eval_running', updated_at=? WHERE proposal_id=? AND status='proposed'",
            (record.created_at, record.proposal_id),
        )
        self.enqueue_projection(event_id, "self_improvement_eval_projection")
        return record.eval_id

    def create_self_improvement_promotion_packet(self, packet: SelfImprovementPromotionPacket) -> str:
        proposal = self.conn.execute(
            "SELECT proposal_id, authority_required, status FROM self_improvement_proposals WHERE proposal_id=?",
            (packet.proposal_id,),
        ).fetchone()
        if proposal is None:
            raise ValueError("self-improvement promotion packet requires a recorded proposal")
        if packet.required_authority != proposal["authority_required"]:
            raise PermissionError("promotion packet authority must match proposal authority")
        if packet.required_authority != "operator_gate":
            raise PermissionError("pre-Hermes self-improvement promotion remains operator-gated")
        if self.command.requested_by in {"agent", "model"}:
            raise PermissionError("workers may not create self-improvement promotion packets")
        if not packet.eval_record_ids:
            raise ValueError("promotion packet requires eval evidence")
        if not packet.evidence_refs:
            raise ValueError("promotion packet requires evidence refs")
        decision = self.conn.execute(
            "SELECT decision_type, required_authority, status, recommendation FROM decisions WHERE decision_id=?",
            (packet.decision_id,),
        ).fetchone()
        if decision is None:
            raise ValueError("promotion packet requires a Decision record")
        if decision["decision_type"] != "system_improvement":
            raise ValueError("promotion packet Decision must be system_improvement")
        if decision["required_authority"] != packet.required_authority:
            raise PermissionError("promotion packet Decision authority mismatch")
        if decision["status"] != packet.status:
            raise ValueError("promotion packet status must match Decision status")
        for eval_id in packet.eval_record_ids:
            eval_row = self.conn.execute(
                "SELECT proposal_id, status, authority_effect FROM self_improvement_eval_records WHERE eval_id=?",
                (eval_id,),
            ).fetchone()
            if eval_row is None:
                raise ValueError("promotion packet references unknown eval record")
            if eval_row["proposal_id"] != packet.proposal_id:
                raise ValueError("promotion packet eval proposal mismatch")
            if eval_row["authority_effect"] != "evidence_only":
                raise PermissionError("promotion packet eval evidence must be evidence-only")
            if packet.recommendation == "approve" and eval_row["status"] != "passed":
                raise ValueError("approval recommendation requires passed eval records")
        payload = _self_improvement_promotion_payload(packet)
        event_id = self.append_event("self_improvement_promotion_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_promotion_packets (
              packet_id, proposal_id, decision_id, recommendation,
              required_authority, eval_record_ids_json, evidence_refs_json,
              risk_flags_json, gate_packet_json, default_on_timeout, status,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.proposal_id,
                packet.decision_id,
                packet.recommendation,
                packet.required_authority,
                canonical_json(packet.eval_record_ids),
                canonical_json(packet.evidence_refs),
                canonical_json(packet.risk_flags),
                canonical_json(packet.gate_packet),
                packet.default_on_timeout,
                packet.status,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "self_improvement_promotion_projection")
        return packet.packet_id

    def record_self_improvement_rollback(self, record: SelfImprovementRollbackRecord) -> str:
        packet = self.conn.execute(
            "SELECT proposal_id FROM self_improvement_promotion_packets WHERE packet_id=?",
            (record.packet_id,),
        ).fetchone()
        if packet is None or packet["proposal_id"] != record.proposal_id:
            raise ValueError("rollback requires matching proposal and promotion packet")
        if record.status == "applied" and (not record.receipt_ref or not record.receipt_hash):
            raise PermissionError("applied rollback requires durable receipt reference and hash")
        payload = _self_improvement_rollback_payload(record)
        event_id = self.append_event("self_improvement_rollback_recorded", "self_improvement", record.rollback_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_rollbacks (
              rollback_id, proposal_id, packet_id, previous_ref,
              rollback_reason, receipt_ref, receipt_hash, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.rollback_id,
                record.proposal_id,
                record.packet_id,
                record.previous_ref,
                record.rollback_reason,
                record.receipt_ref,
                record.receipt_hash,
                record.status,
                record.created_at,
            ),
        )
        if record.status == "applied":
            self.conn.execute(
                "UPDATE self_improvement_proposals SET status='rolled_back', updated_at=? WHERE proposal_id=?",
                (record.created_at, record.proposal_id),
            )
        self.enqueue_projection(event_id, "self_improvement_rollback_projection")
        return record.rollback_id

    def compare_self_improvement_replay_to_projection(self, scope: str = "self_improvement") -> SelfImprovementReplayProjectionComparison:
        replay = self._replay_from_connection(self.conn)
        proposal_rows = [self._self_improvement_proposal_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_proposals ORDER BY proposal_id")]
        eval_rows = [self._self_improvement_eval_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_eval_records ORDER BY eval_id")]
        packet_rows = [self._self_improvement_packet_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_promotion_packets ORDER BY packet_id")]
        rollback_rows = [self._self_improvement_rollback_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_rollbacks ORDER BY rollback_id")]
        replay_proposals = sorted(replay.self_improvement_proposals.values(), key=lambda item: item["proposal_id"])
        replay_evals = sorted(replay.self_improvement_eval_records.values(), key=lambda item: item["eval_id"])
        replay_packets = sorted(replay.self_improvement_promotion_packets.values(), key=lambda item: item["packet_id"])
        replay_rollbacks = sorted(replay.self_improvement_rollbacks.values(), key=lambda item: item["rollback_id"])
        mismatches: list[str] = []
        if replay_proposals != proposal_rows:
            mismatches.append("proposal_projection_mismatch")
        if replay_evals != eval_rows:
            mismatches.append("eval_projection_mismatch")
        if replay_packets != packet_rows:
            mismatches.append("promotion_packet_projection_mismatch")
        if replay_rollbacks != rollback_rows:
            mismatches.append("rollback_projection_mismatch")
        comparison = SelfImprovementReplayProjectionComparison(
            scope=scope,
            replay_proposals=replay_proposals,
            projection_proposals=proposal_rows,
            replay_eval_records=replay_evals,
            projection_eval_records=eval_rows,
            replay_promotion_packets=replay_packets,
            projection_promotion_packets=packet_rows,
            replay_rollbacks=replay_rollbacks,
            projection_rollbacks=rollback_rows,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _self_improvement_comparison_payload(comparison)
        event_id = self.append_event("self_improvement_replay_projection_compared", "self_improvement", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_replay_projection_comparisons (
              comparison_id, scope, replay_proposals_json, projection_proposals_json,
              replay_eval_records_json, projection_eval_records_json,
              replay_promotion_packets_json, projection_promotion_packets_json,
              replay_rollbacks_json, projection_rollbacks_json, matches,
              mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.scope,
                canonical_json(comparison.replay_proposals),
                canonical_json(comparison.projection_proposals),
                canonical_json(comparison.replay_eval_records),
                canonical_json(comparison.projection_eval_records),
                canonical_json(comparison.replay_promotion_packets),
                canonical_json(comparison.projection_promotion_packets),
                canonical_json(comparison.replay_rollbacks),
                canonical_json(comparison.projection_rollbacks),
                1 if comparison.matches else 0,
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "self_improvement_replay_projection_comparison")
        return comparison

    @staticmethod
    def _self_improvement_proposal_row(row: Any) -> dict[str, Any]:
        return {
            "proposal_id": row["proposal_id"],
            "target_type": row["target_type"],
            "target_id": row["target_id"],
            "problem_evidence": _loads(row["problem_evidence_json"]),
            "proposed_change": row["proposed_change"],
            "expected_benefit": row["expected_benefit"],
            "risk_assessment": row["risk_assessment"],
            "eval_plan": row["eval_plan"],
            "rollback_plan": row["rollback_plan"],
            "authority_required": row["authority_required"],
            "proposer_type": row["proposer_type"],
            "proposer_id": row["proposer_id"],
            "affected_policy_areas": _loads(row["affected_policy_areas_json"]),
            "data_classes": _loads(row["data_classes_json"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _self_improvement_eval_row(row: Any) -> dict[str, Any]:
        return {
            "eval_id": row["eval_id"],
            "proposal_id": row["proposal_id"],
            "eval_type": row["eval_type"],
            "baseline_ref": row["baseline_ref"],
            "candidate_ref": row["candidate_ref"],
            "dataset_refs": _loads(row["dataset_refs_json"]),
            "metrics": _loads(row["metrics_json"]),
            "regression_thresholds": _loads(row["regression_thresholds_json"]),
            "failure_examples": _loads(row["failure_examples_json"]),
            "side_effect_safety": _loads(row["side_effect_safety_json"]),
            "status": row["status"],
            "authority_effect": row["authority_effect"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _self_improvement_packet_row(row: Any) -> dict[str, Any]:
        return {
            "packet_id": row["packet_id"],
            "proposal_id": row["proposal_id"],
            "decision_id": row["decision_id"],
            "recommendation": row["recommendation"],
            "required_authority": row["required_authority"],
            "eval_record_ids": _loads(row["eval_record_ids_json"]),
            "evidence_refs": _loads(row["evidence_refs_json"]),
            "risk_flags": _loads(row["risk_flags_json"]),
            "gate_packet": _loads(row["gate_packet_json"]),
            "default_on_timeout": row["default_on_timeout"],
            "status": row["status"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _self_improvement_rollback_row(row: Any) -> dict[str, Any]:
        return {
            "rollback_id": row["rollback_id"],
            "proposal_id": row["proposal_id"],
            "packet_id": row["packet_id"],
            "previous_ref": row["previous_ref"],
            "rollback_reason": row["rollback_reason"],
            "receipt_ref": row["receipt_ref"],
            "receipt_hash": row["receipt_hash"],
            "status": row["status"],
            "created_at": row["created_at"],
        }
