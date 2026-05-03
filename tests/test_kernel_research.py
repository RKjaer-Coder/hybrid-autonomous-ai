from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from kernel import (
    CapabilityGrant,
    ClaimRecord,
    EvidenceBundle,
    KernelCommercialResearchWorkflow,
    KernelResearchEngine,
    KernelStore,
    ProjectArtifactReceipt,
    ProjectCustomerFeedback,
    ProjectOperatorLoadRecord,
    ProjectOutcome,
    ProjectRevenueAttribution,
    ProjectTaskAssignment,
    ResearchRequest,
    SideEffectIntent,
    SideEffectReceipt,
    SourceAcquisitionCheck,
    SourcePlan,
    SourceRecord,
)
from kernel.records import new_id, payload_hash, sha256_text
from kernel.store import KERNEL_POLICY_VERSION
from kernel.research import (
    evidence_bundle_command,
    research_request_command,
    retrieval_grant_command,
    source_acquisition_command,
    source_plan_command,
)
from kernel.commercial import (
    commercial_decision_packet_command,
    g1_project_approval_command,
    project_artifact_receipt_command,
    project_close_decision_command,
    project_feedback_command,
    project_operator_load_command,
    project_outcome_command,
    project_replay_comparison_command,
    project_revenue_attribution_command,
    project_status_rollup_command,
    project_task_command,
)
from migrate import apply_schema
from skills.db_manager import DatabaseManager


def request_command(key: str, payload: dict | None = None):
    return research_request_command(key=key, payload=payload or {"key": key})


class KernelResearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = KernelStore(self.root / "kernel.db")
        self.engine = KernelResearchEngine(self.store)
        self.commercial = KernelCommercialResearchWorkflow(self.store)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def request(self) -> ResearchRequest:
        return ResearchRequest(
            request_id=new_id(),
            profile="commercial",
            question="Validate demand for a local-first agent operations package.",
            decision_target="project-alpha",
            freshness_horizon="P30D",
            depth="standard",
            source_policy={
                "allowed_source_types": ["official", "primary_data", "reputable_media", "internal_record"],
                "blocked_source_types": ["model_generated"],
            },
            evidence_requirements={
                "minimum_sources": 2,
                "require_uncertainty": True,
                "high_stakes_claims_require_independent_sources": True,
            },
            max_cost_usd=Decimal("2.50"),
            max_latency="PT30M",
            autonomy_class="A2",
        )

    def bundle(self, request_id: str) -> EvidenceBundle:
        return self.bundle_for_plan(request_id, new_id())

    def plan(self, request_id: str) -> SourcePlan:
        return SourcePlan(
            source_plan_id=new_id(),
            request_id=request_id,
            profile="commercial",
            depth="standard",
            planned_sources=[
                {
                    "url_or_ref": "https://example.com/pricing",
                    "source_type": "official",
                    "access_method": "public_web",
                    "data_class": "public",
                    "purpose": "pricing signal",
                },
                {
                    "url_or_ref": "internal://operator/customer-call-1",
                    "source_type": "internal_record",
                    "access_method": "operator_provided",
                    "data_class": "internal",
                    "purpose": "buyer evidence",
                },
            ],
            retrieval_strategy="prefer official/public web first; use operator-provided notes only with grant",
            created_by="kernel",
        )

    def bundle_for_plan(self, request_id: str, source_plan_id: str) -> EvidenceBundle:
        official = SourceRecord(
            source_id=new_id(),
            url_or_ref="https://example.com/pricing",
            source_type="official",
            retrieved_at="2026-05-02T08:00:00Z",
            source_date="2026-05-01",
            relevance=0.91,
            reliability=0.95,
            content_hash=sha256_text("pricing"),
            access_method="public_web",
            data_class="public",
            license_or_tos_notes="metadata-only cache",
        )
        market = SourceRecord(
            source_id=new_id(),
            url_or_ref="internal://operator/customer-call-1",
            source_type="internal_record",
            retrieved_at="2026-05-02T08:01:00Z",
            source_date="2026-04-29",
            relevance=0.87,
            reliability=0.82,
            content_hash=sha256_text("customer-call"),
            access_method="operator_provided",
            data_class="internal",
        )
        return EvidenceBundle(
            bundle_id=new_id(),
            request_id=request_id,
            source_plan_id=source_plan_id,
            sources=[official, market],
            claims=[
                ClaimRecord(
                    text=(
                        "The package has plausible willingness-to-pay evidence from operator-provided customer notes, "
                        "with low expected operator load for validation."
                    ),
                    claim_type="interpretation",
                    source_ids=[official.source_id, market.source_id],
                    confidence=0.74,
                    freshness="current",
                    importance="high",
                )
            ],
            contradictions=[],
            unsupported_claims=["Exact conversion rate is not yet known."],
            freshness_summary="Both sources were retrieved within the 30 day horizon.",
            confidence=0.74,
            uncertainty="Demand breadth is still uncertain until more buyer conversations exist.",
            counter_thesis="The demand may be narrow consulting pull rather than repeatable product pull.",
            quality_gate_result="pass",
            data_classes=["public", "internal"],
            retention_policy="retain-90d",
        )

    def test_research_request_and_bundle_are_replayable_kernel_state(self):
        request = self.request()
        self.engine.create_request(request_command("research-create"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-create"), plan)
        self.engine.start_collection(request_command("research-collect"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize"), request.request_id)
        bundle = self.bundle_for_plan(request.request_id, plan.source_plan_id)
        bundle_id = self.engine.commit_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-commit"),
            bundle,
        )

        self.assertEqual(bundle_id, bundle.bundle_id)
        with self.store.connect() as conn:
            request_row = conn.execute(
                "SELECT profile, status, max_cost_usd FROM research_requests WHERE request_id=?",
                (request.request_id,),
            ).fetchone()
            bundle_row = conn.execute(
                "SELECT quality_gate_result, confidence FROM evidence_bundles WHERE bundle_id=?",
                (bundle.bundle_id,),
            ).fetchone()
            gate_row = conn.execute(
                "SELECT result, profile FROM quality_gate_events WHERE bundle_id=?",
                (bundle.bundle_id,),
            ).fetchone()
            events = [
                row["event_type"]
                for row in conn.execute("SELECT event_type FROM events ORDER BY event_seq").fetchall()
            ]

        self.assertEqual(request_row["profile"], "commercial")
        self.assertEqual(request_row["status"], "completed")
        self.assertEqual(request_row["max_cost_usd"], "2.50")
        self.assertEqual(bundle_row["quality_gate_result"], "pass")
        self.assertEqual(bundle_row["confidence"], 0.74)
        self.assertEqual(gate_row["result"], "pass")
        self.assertEqual(gate_row["profile"], "commercial")
        self.assertEqual(
            events,
            [
                "research_request_created",
                "source_plan_created",
                "research_request_transitioned",
                "research_request_transitioned",
                "quality_gate_evaluated",
                "evidence_bundle_committed",
            ],
        )

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.research_requests[request.request_id]["status"], "completed")
        self.assertEqual(replay.source_plans[plan.source_plan_id]["request_id"], request.request_id)
        self.assertEqual(replay.evidence_bundles[bundle.bundle_id]["quality_gate_result"], "pass")
        self.assertEqual(next(iter(replay.quality_gate_events.values()))["result"], "pass")
        self.assertEqual(replay.evidence_bundles[bundle.bundle_id]["claims"][0]["source_ids"], [
            bundle.sources[0].source_id,
            bundle.sources[1].source_id,
        ])

    def test_source_plan_grants_and_acquisition_boundaries_are_kernel_authority(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-boundary"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-boundary"), plan)

        grant_ids = self.engine.issue_retrieval_grants(
            lambda grant, idx: retrieval_grant_command(grant_id=grant.grant_id, key=f"retrieval-grant-{idx}"),
            plan,
            expires_at="9999-12-31T23:59:59Z",
        )
        self.assertEqual(len(grant_ids), 1)

        blocked = SourceAcquisitionCheck(
            request_id=request.request_id,
            source_plan_id=plan.source_plan_id,
            source_ref="internal://operator/customer-call-1",
            access_method="operator_provided",
            data_class="internal",
            source_type="internal_record",
            result="allowed",
            reason="operator notes require explicit retrieval grant",
        )
        with self.assertRaises(PermissionError):
            self.engine.record_source_acquisition_check(
                source_acquisition_command(source_plan_id=plan.source_plan_id, key="source-check-blocked"),
                blocked,
            )

        allowed = SourceAcquisitionCheck(
            request_id=request.request_id,
            source_plan_id=plan.source_plan_id,
            source_ref="internal://operator/customer-call-1",
            access_method="operator_provided",
            data_class="internal",
            source_type="internal_record",
            result="allowed",
            reason="explicit retrieval grant covers operator-provided note metadata",
            grant_id=grant_ids[0],
        )
        check_id = self.engine.record_source_acquisition_check(
            source_acquisition_command(source_plan_id=plan.source_plan_id, key="source-check-allowed"),
            allowed,
        )

        with self.store.connect() as conn:
            grant_row = conn.execute("SELECT capability_type, used_count FROM capability_grants").fetchone()
            check_row = conn.execute("SELECT result, grant_id FROM source_acquisition_checks WHERE check_id=?", (check_id,)).fetchone()

        self.assertEqual(grant_row["capability_type"], "file")
        self.assertEqual(grant_row["used_count"], 0)
        self.assertEqual(check_row["result"], "allowed")
        self.assertEqual(check_row["grant_id"], grant_ids[0])
        replay = self.store.replay_critical_state()
        self.assertIn(check_id, replay.source_acquisition_checks)

    def test_commercial_workflow_creates_replayable_opportunity_project_decision_packet(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-commercial-packet"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-commercial-packet"), plan)
        self.engine.start_collection(request_command("research-collect-commercial-packet"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-commercial-packet"), request.request_id)
        bundle = self.bundle_for_plan(request.request_id, plan.source_plan_id)
        self.engine.commit_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-commercial-packet"),
            bundle,
        )

        packet = self.commercial.create_decision_packet(
            commercial_decision_packet_command(evidence_bundle_id=bundle.bundle_id, key="commercial-packet-create"),
            bundle.bundle_id,
            project_name="Local Agent Ops Package",
            revenue_mechanism="software",
        )

        self.assertEqual(packet.request_id, request.request_id)
        self.assertEqual(packet.evidence_bundle_id, bundle.bundle_id)
        self.assertTrue(packet.decision_id)
        self.assertEqual(packet.decision_target, request.decision_target)
        self.assertEqual(packet.required_authority, "operator_gate")
        self.assertEqual(packet.recommendation, "pursue")
        self.assertEqual(packet.default_on_timeout, "pause")
        self.assertEqual(packet.opportunity["status"], "gated")
        self.assertEqual(packet.project["status"], "proposed")
        self.assertEqual(packet.gate_packet["side_effects_authorized"], [])
        self.assertIn(bundle.claims[0].claim_id, packet.evidence_used)

        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT decision_id, recommendation, required_authority, status, project_json, gate_packet_json
                FROM commercial_decision_packets
                WHERE packet_id=?
                """,
                (packet.packet_id,),
            ).fetchone()
            decision_row = conn.execute(
                """
                SELECT decision_type, required_authority, status, recommendation, default_on_timeout
                FROM decisions
                WHERE decision_id=?
                """,
                (packet.decision_id,),
            ).fetchone()
            events = [
                event["event_type"]
                for event in conn.execute("SELECT event_type FROM events ORDER BY event_seq").fetchall()
            ]

        self.assertEqual(row["decision_id"], packet.decision_id)
        self.assertEqual(decision_row["decision_type"], "project_approval")
        self.assertEqual(decision_row["required_authority"], "operator_gate")
        self.assertEqual(decision_row["status"], "gated")
        self.assertEqual(decision_row["recommendation"], "pursue")
        self.assertEqual(decision_row["default_on_timeout"], "pause")
        self.assertEqual(row["recommendation"], "pursue")
        self.assertEqual(row["required_authority"], "operator_gate")
        self.assertEqual(row["status"], "gated")
        self.assertIn("decision_recorded", events)
        self.assertIn("commercial_decision_packet_created", events)
        replay = self.store.replay_critical_state()
        self.assertEqual(replay.decisions[packet.decision_id]["decision_type"], "project_approval")
        self.assertEqual(replay.commercial_decision_packets[packet.packet_id]["recommendation"], "pursue")
        self.assertEqual(
            replay.commercial_decision_packets[packet.packet_id]["gate_packet"]["default_on_timeout"],
            "pause",
        )

    def test_degraded_commercial_bundle_produces_insufficient_evidence_packet(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-degraded-commercial-packet"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-degraded-commercial-packet"), plan)
        self.engine.start_collection(request_command("research-collect-degraded-commercial-packet"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-degraded-commercial-packet"), request.request_id)
        bundle = self.bundle_for_plan(request.request_id, plan.source_plan_id)
        degraded = EvidenceBundle(
            request_id=bundle.request_id,
            source_plan_id=bundle.source_plan_id,
            sources=bundle.sources,
            claims=bundle.claims,
            contradictions=bundle.contradictions,
            unsupported_claims=["Pricing sensitivity is unknown.", "Conversion rate is unknown."],
            freshness_summary=bundle.freshness_summary,
            confidence=bundle.confidence,
            uncertainty=bundle.uncertainty,
            counter_thesis=bundle.counter_thesis,
            quality_gate_result="degraded",
            data_classes=bundle.data_classes,
            retention_policy=bundle.retention_policy,
        )
        self.engine.commit_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-degraded-commercial-packet"),
            degraded,
        )

        packet = self.commercial.create_decision_packet(
            commercial_decision_packet_command(evidence_bundle_id=degraded.bundle_id, key="degraded-commercial-packet-create"),
            degraded.bundle_id,
        )

        self.assertEqual(packet.recommendation, "insufficient_evidence")
        self.assertIn("quality_gate_degraded", packet.risk_flags)
        self.assertIn("unsupported_claims", packet.risk_flags)

    def test_g1_approval_creates_replayable_project_task_and_outcome_loop(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-project-loop"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-project-loop"), plan)
        self.engine.start_collection(request_command("research-collect-project-loop"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-project-loop"), request.request_id)
        bundle = self.bundle_for_plan(request.request_id, plan.source_plan_id)
        self.engine.commit_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-project-loop"),
            bundle,
        )
        packet = self.commercial.create_decision_packet(
            commercial_decision_packet_command(evidence_bundle_id=bundle.bundle_id, key="commercial-project-loop"),
            bundle.bundle_id,
            project_name="Local Agent Ops Package",
        )

        kickoff = self.commercial.approve_g1_validation_project(
            g1_project_approval_command(packet_id=packet.packet_id, key="g1-project-loop"),
            packet.packet_id,
            notes="approve bounded zero-spend validation",
        )
        with self.assertRaises(PermissionError):
            self.store.transition_project_task(
                project_task_command(project_id=kickoff["project_id"], key="project-loop-task-running-without-assignment"),
                kickoff["task_id"],
                "running",
                "running requires an accepted worker assignment",
            )
        grant = CapabilityGrant(
            task_id=kickoff["task_id"],
            subject_type="agent",
            subject_id="validation-worker-1",
            capability_type="file",
            actions=["read", "write"],
            resource={"kind": "project_workspace"},
            scope={"project_id": kickoff["project_id"]},
            conditions={"external_side_effects": "blocked"},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=2,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-assignment-grant"),
            grant,
        )
        assignment = ProjectTaskAssignment(
            task_id=kickoff["task_id"],
            project_id=kickoff["project_id"],
            worker_type="agent",
            worker_id="validation-worker-1",
            grant_ids=[grant_id],
            accepted_capabilities=[
                {"capability_type": "file", "actions": ["read", "write"], "scope": "project_workspace"}
            ],
            notes="bounded validation worker accepted the task",
        )
        assignment_id = self.store.assign_project_task(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-task-assignment"),
            assignment,
        )

        outcome = ProjectOutcome(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            phase_name="Validate",
            outcome_type="feedback",
            summary="Operator reviewed the validation artifact and accepted the first bounded loop.",
            artifact_refs=["artifact://local/project-loop/validation-note"],
            metrics={"validation_result": "accepted", "buyer_conversations": 1},
            feedback={"operator_rating": 0.8, "next_recommendation": "build_small_artifact"},
            revenue_impact={"amount": 0, "currency": "USD", "period": "one_time"},
            operator_load_actual="15 minutes",
            status="accepted",
        )
        outcome_id = self.commercial.record_project_outcome(
            project_outcome_command(project_id=kickoff["project_id"], key="project-loop-outcome"),
            outcome,
        )
        validation_artifact = ProjectArtifactReceipt(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            artifact_ref="artifact://local/project-loop/validation-note",
            artifact_kind="validation_artifact",
            summary="Validation note was recorded as a governed project artifact.",
            data_class="internal",
            delivery_channel="local_workspace",
            status="accepted",
        )
        validation_artifact_id = self.commercial.record_project_artifact_receipt(
            project_artifact_receipt_command(project_id=kickoff["project_id"], key="project-loop-validation-artifact"),
            validation_artifact,
        )

        side_effect_grant = CapabilityGrant(
            task_id=kickoff["task_id"],
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "publish", "artifact_ref": "artifact://local/project-loop/shipped-demo"},
            scope={"project_id": kickoff["project_id"]},
            conditions={"operator_approved": True},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        side_effect_grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-side-effect-grant"),
            side_effect_grant,
        )
        side_effect_intent = SideEffectIntent(
            task_id=kickoff["task_id"],
            side_effect_type="publish",
            target={"channel": "operator_review_link"},
            payload_hash=payload_hash({"artifact_ref": "artifact://local/project-loop/shipped-demo"}),
            required_authority="operator_gate",
            grant_id=side_effect_grant_id,
            timeout_policy="ask_operator",
        )
        side_effect_intent_id = self.store.prepare_side_effect(
            project_task_command(
                project_id=kickoff["project_id"],
                key="project-loop-side-effect-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            side_effect_intent,
        )
        side_effect_receipt = SideEffectReceipt(
            intent_id=side_effect_intent_id,
            receipt_type="success",
            receipt_hash=payload_hash({"published": True}),
            details={"artifact_ref": "artifact://local/project-loop/shipped-demo", "visible_to": "operator"},
        )
        side_effect_receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-side-effect-receipt"),
            side_effect_receipt,
        )
        shipped_artifact = ProjectArtifactReceipt(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            artifact_ref="artifact://local/project-loop/shipped-demo",
            artifact_kind="shipped_artifact",
            summary="The validation demo was shipped to the operator review channel.",
            data_class="internal",
            delivery_channel="operator_review_link",
            side_effect_intent_id=side_effect_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            customer_visible=True,
            status="accepted",
        )
        shipped_artifact_id = self.commercial.record_project_artifact_receipt(
            project_artifact_receipt_command(project_id=kickoff["project_id"], key="project-loop-shipped-artifact"),
            shipped_artifact,
        )
        feedback = ProjectCustomerFeedback(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            artifact_receipt_id=shipped_artifact_id,
            source_type="customer",
            customer_ref="operator-as-first-customer",
            summary="The first reviewer accepted the shipped artifact and asked for one scoped build follow-up.",
            sentiment="positive",
            evidence_refs=[f"kernel:project_artifact_receipts/{shipped_artifact_id}"],
            action_required=True,
            status="needs_followup",
        )
        feedback_id = self.commercial.record_project_customer_feedback(
            project_feedback_command(project_id=kickoff["project_id"], key="project-loop-feedback"),
            feedback,
        )
        revenue = ProjectRevenueAttribution(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            outcome_id=outcome_id,
            amount_usd=Decimal("0"),
            source="operator_reported",
            attribution_period="2026-05",
            confidence=0.35,
            status="needs_reconciliation",
        )
        revenue_id = self.commercial.record_project_revenue_attribution(
            project_revenue_attribution_command(project_id=kickoff["project_id"], key="project-loop-revenue"),
            revenue,
        )
        operator_load = ProjectOperatorLoadRecord(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            outcome_id=outcome_id,
            minutes=15,
            load_type="gate_review",
            source="operator_reported",
            notes="G1 approval and validation artifact review",
        )
        load_id = self.commercial.record_project_operator_load(
            project_operator_load_command(project_id=kickoff["project_id"], key="project-loop-operator-load"),
            operator_load,
        )
        rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=kickoff["project_id"], key="project-loop-rollup"),
            kickoff["project_id"],
        )
        close_packet = self.commercial.create_project_close_decision(
            project_close_decision_command(project_id=kickoff["project_id"], key="project-loop-close-decision"),
            kickoff["project_id"],
            rollup_id=rollup.rollup_id,
        )
        comparison = self.commercial.compare_project_replay_to_projection(
            project_replay_comparison_command(project_id=kickoff["project_id"], key="project-loop-replay-compare"),
            kickoff["project_id"],
        )

        with self.store.connect() as conn:
            decision_row = conn.execute(
                "SELECT status, verdict FROM decisions WHERE decision_id=?",
                (packet.decision_id,),
            ).fetchone()
            project_row = conn.execute(
                "SELECT status, decision_packet_id FROM projects WHERE project_id=?",
                (kickoff["project_id"],),
            ).fetchone()
            task_row = conn.execute(
                "SELECT status, task_type, authority_required FROM project_tasks WHERE task_id=?",
                (kickoff["task_id"],),
            ).fetchone()
            assignment_row = conn.execute(
                "SELECT status, worker_type, worker_id FROM project_task_assignments WHERE assignment_id=?",
                (assignment_id,),
            ).fetchone()
            outcome_row = conn.execute(
                "SELECT status, outcome_type FROM project_outcomes WHERE outcome_id=?",
                (outcome_id,),
            ).fetchone()
            shipped_row = conn.execute(
                "SELECT artifact_kind, customer_visible, side_effect_receipt_id FROM project_artifact_receipts WHERE receipt_id=?",
                (shipped_artifact_id,),
            ).fetchone()
            feedback_row = conn.execute(
                "SELECT source_type, sentiment, status FROM project_customer_feedback WHERE feedback_id=?",
                (feedback_id,),
            ).fetchone()
            revenue_row = conn.execute(
                "SELECT amount_usd, status, reconciliation_task_id FROM project_revenue_attributions WHERE attribution_id=?",
                (revenue_id,),
            ).fetchone()
            load_row = conn.execute(
                "SELECT minutes, load_type FROM project_operator_load WHERE load_id=?",
                (load_id,),
            ).fetchone()
            rollup_row = conn.execute(
                """
                SELECT recommended_status, close_recommendation, revenue_attributed_usd,
                       operator_load_minutes
                FROM project_status_rollups
                WHERE rollup_id=?
                """,
                (rollup.rollup_id,),
            ).fetchone()
            close_decision_row = conn.execute(
                """
                SELECT recommendation, required_authority, status
                FROM project_close_decision_packets
                WHERE packet_id=?
                """,
                (close_packet.packet_id,),
            ).fetchone()
            comparison_row = conn.execute(
                """
                SELECT matches, mismatches_json
                FROM project_replay_projection_comparisons
                WHERE comparison_id=?
                """,
                (comparison.comparison_id,),
            ).fetchone()

        self.assertEqual(decision_row["status"], "decided")
        self.assertEqual(decision_row["verdict"], "approve_validation")
        self.assertEqual(project_row["status"], "active")
        self.assertEqual(project_row["decision_packet_id"], packet.packet_id)
        self.assertEqual(task_row["task_type"], "validate")
        self.assertEqual(task_row["authority_required"], "single_agent")
        self.assertEqual(task_row["status"], "completed")
        self.assertEqual(assignment_row["status"], "accepted")
        self.assertEqual(assignment_row["worker_type"], "agent")
        self.assertEqual(assignment_row["worker_id"], "validation-worker-1")
        self.assertEqual(outcome_row["status"], "accepted")
        self.assertEqual(outcome_row["outcome_type"], "feedback")
        self.assertEqual(shipped_row["artifact_kind"], "shipped_artifact")
        self.assertEqual(shipped_row["customer_visible"], 1)
        self.assertEqual(shipped_row["side_effect_receipt_id"], side_effect_receipt_id)
        self.assertEqual(feedback_row["source_type"], "customer")
        self.assertEqual(feedback_row["sentiment"], "positive")
        self.assertEqual(feedback_row["status"], "needs_followup")
        self.assertEqual(revenue_row["amount_usd"], "0")
        self.assertEqual(revenue_row["status"], "needs_reconciliation")
        self.assertTrue(revenue_row["reconciliation_task_id"])
        self.assertEqual(load_row["minutes"], 15)
        self.assertEqual(load_row["load_type"], "gate_review")
        self.assertEqual(rollup_row["recommended_status"], "active")
        self.assertEqual(rollup_row["close_recommendation"], "continue")
        self.assertEqual(rollup_row["revenue_attributed_usd"], "0")
        self.assertEqual(rollup_row["operator_load_minutes"], 15)
        self.assertEqual(close_decision_row["recommendation"], "continue")
        self.assertEqual(close_decision_row["required_authority"], "operator_gate")
        self.assertEqual(close_decision_row["status"], "gated")
        self.assertEqual(comparison_row["matches"], 1)
        self.assertEqual(comparison_row["mismatches_json"], "[]")

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.decisions[packet.decision_id]["verdict"], "approve_validation")
        self.assertEqual(replay.projects[kickoff["project_id"]]["status"], "active")
        self.assertEqual(replay.project_task_assignments[assignment_id]["grant_ids"], [grant_id])
        self.assertEqual(replay.project_tasks[kickoff["task_id"]]["status"], "completed")
        self.assertEqual(replay.project_outcomes[outcome_id]["feedback"]["next_recommendation"], "build_small_artifact")
        self.assertEqual(replay.project_artifact_receipts[validation_artifact_id]["status"], "accepted")
        self.assertEqual(replay.project_artifact_receipts[shipped_artifact_id]["side_effect_receipt_id"], side_effect_receipt_id)
        self.assertEqual(replay.project_customer_feedback[feedback_id]["action_required"], True)
        self.assertEqual(replay.project_revenue_attributions[revenue_id]["status"], "needs_reconciliation")
        self.assertIn(revenue_row["reconciliation_task_id"], replay.project_tasks)
        self.assertEqual(replay.project_operator_load[load_id]["minutes"], 15)
        self.assertEqual(replay.project_status_rollups[rollup.rollup_id]["close_recommendation"], "continue")
        self.assertEqual(replay.project_close_decision_packets[close_packet.packet_id]["recommendation"], "continue")
        self.assertTrue(replay.project_replay_projection_comparisons[comparison.comparison_id]["matches"])

    def test_bundle_rejects_unsupported_source_references(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-missing-source"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-missing-source"), plan)
        self.engine.start_collection(request_command("research-collect-missing-source"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-missing-source"), request.request_id)
        source = SourceRecord(
            url_or_ref="https://example.com/source",
            source_type="official",
            retrieved_at="2026-05-02T08:00:00Z",
            source_date="2026-05-01",
            relevance=0.9,
            reliability=0.9,
            content_hash=sha256_text("source"),
            access_method="public_web",
            data_class="public",
        )
        bad_bundle = EvidenceBundle(
            request_id=request.request_id,
            source_plan_id=plan.source_plan_id,
            sources=[source],
            claims=[
                ClaimRecord(
                    text="This claim points at a missing source.",
                    claim_type="fact",
                    source_ids=["missing-source"],
                    confidence=0.5,
                    freshness="unknown",
                    importance="medium",
                )
            ],
            contradictions=[],
            unsupported_claims=[],
            freshness_summary="unknown",
            confidence=0.5,
            uncertainty="source missing",
            counter_thesis=None,
            quality_gate_result="fail",
            data_classes=["public"],
            retention_policy="retain-30d",
        )

        with self.assertRaises(ValueError):
            self.engine.commit_evidence_bundle(
                evidence_bundle_command(request_id=request.request_id, key="bad-evidence"),
                bad_bundle,
            )

        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM evidence_bundles").fetchone()[0], 0)

    def test_profile_validator_rejects_commercial_willingness_to_pay_without_buyer_evidence(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-profile-validator"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-profile-validator"), plan)
        self.engine.start_collection(request_command("research-collect-profile-validator"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-profile-validator"), request.request_id)
        community = SourceRecord(
            url_or_ref="https://forum.example.com/thread",
            source_type="community",
            retrieved_at="2026-05-02T08:00:00Z",
            source_date="2026-05-01",
            relevance=0.4,
            reliability=0.3,
            content_hash=sha256_text("forum"),
            access_method="public_web",
            data_class="public",
        )
        bad_bundle = EvidenceBundle(
            request_id=request.request_id,
            source_plan_id=plan.source_plan_id,
            sources=[community],
            claims=[
                ClaimRecord(
                    text="There is willingness-to-pay for the package.",
                    claim_type="interpretation",
                    source_ids=[community.source_id],
                    confidence=0.5,
                    freshness="current",
                    importance="high",
                )
            ],
            contradictions=[],
            unsupported_claims=[],
            freshness_summary="fresh but weak",
            confidence=0.5,
            uncertainty="buyer evidence is not present",
            counter_thesis=None,
            quality_gate_result="pass",
            data_classes=["public"],
            retention_policy="retain-30d",
        )

        with self.assertRaises(ValueError):
            self.engine.commit_evidence_bundle(
                evidence_bundle_command(request_id=request.request_id, key="bad-commercial-quality"),
                bad_bundle,
            )

        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM quality_gate_events").fetchone()[0], 0)

    def test_legacy_projection_is_non_authoritative_compatibility_surface(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-projection"), request)
        projection_data = self.root / "projection-data"
        projection_data.mkdir()
        repo_root = Path(__file__).resolve().parents[1]
        apply_schema(projection_data / "strategic_memory.db", repo_root / "schemas" / "strategic_memory.sql")
        db = DatabaseManager(str(projection_data))
        projection = self.engine.project_request_to_legacy_task(request.request_id, db)

        strategic = db.get_connection("strategic_memory")
        row = strategic.execute(
            "SELECT title, source, max_spend_usd, tags FROM research_tasks WHERE task_id=?",
            (projection.task_id,),
        ).fetchone()

        self.assertEqual(projection.request_id, request.request_id)
        self.assertEqual(row["title"], request.question)
        self.assertEqual(row["source"], "operator")
        self.assertEqual(row["max_spend_usd"], 2.5)
        self.assertIn(request.request_id, row["tags"])
        replay = self.store.replay_critical_state()
        self.assertIn(request.request_id, replay.research_requests)
        self.assertNotIn(projection.task_id, replay.research_requests)


if __name__ == "__main__":
    unittest.main()
