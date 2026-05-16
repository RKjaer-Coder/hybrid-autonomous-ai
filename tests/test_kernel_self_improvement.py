from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kernel import (
    KernelSelfImprovement,
    KernelStore,
    SelfImprovementEvalRecord,
    SelfImprovementPromotionPacket,
    SelfImprovementProposal,
    SelfImprovementRollbackRecord,
    self_improvement_command,
)
from kernel.records import sha256_text


class KernelSelfImprovementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = KernelStore(Path(self.tmp.name) / "kernel.db")
        self.si = KernelSelfImprovement(self.store)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def proposal(self) -> SelfImprovementProposal:
        return SelfImprovementProposal(
            proposal_id="proposal-harness-summary-v2",
            target_type="harness",
            target_id="research.quick_summary.prompt@v1",
            problem_evidence=[
                "artifact://replay/failure-examples/unsupported-claim-001",
                "kernel:model_eval_runs/shadow/quick-summary-regression",
            ],
            proposed_change="Add a citation-presence check and tighten the final summary format.",
            expected_benefit="Reduce unsupported claim leakage in quick research summaries.",
            risk_assessment="Low runtime risk; possible concision loss measured by regression eval.",
            eval_plan="Run replay, known-bad regression, and shadow scoring before any promotion packet.",
            rollback_plan="Restore research.quick_summary.prompt@v1 and keep candidate traces for audit.",
            authority_required="operator_gate",
            proposer_type="agent",
            proposer_id="system-improvement-worker",
            affected_policy_areas=[],
            data_classes=["public", "internal"],
        )

    def eval_record(self, proposal_id: str) -> SelfImprovementEvalRecord:
        return SelfImprovementEvalRecord(
            eval_id="eval-harness-summary-v2-replay",
            proposal_id=proposal_id,
            eval_type="replay",
            baseline_ref="harness://research.quick_summary.prompt@v1",
            candidate_ref="harness://research.quick_summary.prompt@v2-candidate",
            dataset_refs=[
                "artifact://evals/research/quick-summary/regression-2026-05",
                "artifact://evals/research/quick-summary/known-bad-2026-05",
            ],
            metrics={
                "overall": 0.91,
                "unsupported_claim_rate": 0.0,
                "citation_coverage": 0.98,
                "latency_delta_ms_p95": 120,
            },
            regression_thresholds={
                "unsupported_claim_rate_max": 0.0,
                "citation_coverage_min": 0.95,
                "latency_delta_ms_p95_max": 500,
            },
            failure_examples=[],
            side_effect_safety={
                "reexecuted_side_effects": False,
                "external_intents_reconstructed_only": True,
            },
            status="passed",
        )

    def test_proposal_eval_packet_rollback_and_replay_comparison_are_kernel_owned(self):
        proposal = self.proposal()
        proposal_id = self.si.record_proposal(
            self_improvement_command(
                "self_improvement.proposal.record",
                "proposal-harness-summary-v2",
                requested_by="agent",
                requester_id="system-improvement-worker",
                requested_authority="operator_gate",
                payload={"target_id": proposal.target_id},
            ),
            proposal,
        )
        eval_record = self.eval_record(proposal_id)
        eval_id = self.si.record_eval(
            self_improvement_command("self_improvement.eval.record", "eval-harness-summary-v2-replay"),
            eval_record,
        )
        decision = self.si.promotion_decision(
            proposal=proposal,
            question="Approve the quick-summary harness v2 candidate after replay and known-bad evals?",
            evidence_refs=[eval_id, *proposal.problem_evidence],
            confidence=0.91,
        )
        decision_id = self.store.create_decision(
            self_improvement_command("decision.create", "decision-harness-summary-v2"),
            decision,
        )
        packet = SelfImprovementPromotionPacket(
            packet_id="packet-harness-summary-v2",
            proposal_id=proposal_id,
            decision_id=decision_id,
            recommendation="approve",
            required_authority="operator_gate",
            eval_record_ids=[eval_id],
            evidence_refs=[eval_id, *proposal.problem_evidence],
            risk_flags=["operator_gate_required_before_active_harness_change"],
            gate_packet={
                "decision_type": "system_improvement",
                "proposal_id": proposal_id,
                "authority_route": "operator_gate",
            },
            default_on_timeout="keep_current_behavior",
        )
        packet_id = self.si.create_promotion_packet(
            self_improvement_command("self_improvement.promotion_packet.create", "packet-harness-summary-v2"),
            packet,
        )
        rollback = SelfImprovementRollbackRecord(
            rollback_id="rollback-harness-summary-v2",
            proposal_id=proposal_id,
            packet_id=packet_id,
            previous_ref="harness://research.quick_summary.prompt@v1",
            rollback_reason="Operator chose to restore previous harness after post-promotion regression.",
            receipt_ref="artifact://receipts/self-improvement/rollback-harness-summary-v2",
            receipt_hash=sha256_text("rollback receipt"),
            status="applied",
        )
        rollback_id = self.si.record_rollback(
            self_improvement_command("self_improvement.rollback.record", "rollback-harness-summary-v2"),
            rollback,
        )
        comparison = self.si.compare_replay_to_projection(
            self_improvement_command("self_improvement.replay.compare", "compare-self-improvement"),
        )

        self.assertEqual(rollback_id, rollback.rollback_id)
        self.assertTrue(comparison.matches, comparison.mismatches)
        self.assertEqual(comparison.projection_proposals[0]["status"], "rolled_back")
        self.assertEqual(comparison.projection_eval_records[0]["authority_effect"], "evidence_only")
        self.assertEqual(comparison.projection_promotion_packets[0]["required_authority"], "operator_gate")

    def test_workers_cannot_downgrade_pinned_policy_or_create_promotion_packets(self):
        pinned = SelfImprovementProposal(
            proposal_id="proposal-policy-bypass",
            target_type="policy",
            target_id="capability-broker",
            problem_evidence=["artifact://incident/bypass-attempt"],
            proposed_change="Relax capability broker checks for convenience.",
            expected_benefit="Less friction.",
            risk_assessment="Unsafe.",
            eval_plan="None.",
            rollback_plan="Restore policy.",
            authority_required="single_agent",
            proposer_type="agent",
            proposer_id="worker",
            affected_policy_areas=["capability_broker"],
            data_classes=["internal"],
        )
        with self.assertRaises(PermissionError):
            self.si.record_proposal(
                self_improvement_command(
                    "self_improvement.proposal.record",
                    "proposal-policy-bypass",
                    requested_by="agent",
                    requester_id="worker",
                    requested_authority="single_agent",
                ),
                pinned,
            )

        proposal = self.proposal()
        proposal_id = self.si.record_proposal(
            self_improvement_command("self_improvement.proposal.record", "proposal-worker-packet"),
            proposal,
        )
        eval_id = self.si.record_eval(
            self_improvement_command("self_improvement.eval.record", "eval-worker-packet"),
            self.eval_record(proposal_id),
        )
        decision = self.si.promotion_decision(
            proposal=proposal,
            question="Approve candidate?",
            evidence_refs=[eval_id],
            confidence=0.91,
        )
        decision_id = self.store.create_decision(
            self_improvement_command("decision.create", "decision-worker-packet"),
            decision,
        )
        packet = SelfImprovementPromotionPacket(
            proposal_id=proposal_id,
            decision_id=decision_id,
            recommendation="approve",
            required_authority="operator_gate",
            eval_record_ids=[eval_id],
            evidence_refs=[eval_id],
            risk_flags=[],
            gate_packet={"decision_type": "system_improvement"},
            default_on_timeout="keep_current_behavior",
        )
        with self.assertRaises(PermissionError):
            self.si.create_promotion_packet(
                self_improvement_command(
                    "self_improvement.promotion_packet.create",
                    "worker-promotion-packet",
                    requested_by="agent",
                    requester_id="worker",
                    requested_authority="operator_gate",
                ),
                packet,
            )

    def test_eval_replay_must_not_reexecute_side_effects(self):
        proposal = self.proposal()
        proposal_id = self.si.record_proposal(
            self_improvement_command("self_improvement.proposal.record", "proposal-side-effect-safety"),
            proposal,
        )
        unsafe = self.eval_record(proposal_id)
        object.__setattr__(unsafe, "side_effect_safety", {"reexecuted_side_effects": True})

        with self.assertRaises(PermissionError):
            self.si.record_eval(
                self_improvement_command("self_improvement.eval.record", "eval-side-effect-unsafe"),
                unsafe,
            )


if __name__ == "__main__":
    unittest.main()
