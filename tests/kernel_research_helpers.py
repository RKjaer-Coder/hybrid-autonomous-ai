from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from kernel import (
    Budget,
    CapabilityGrant,
    ClaimRecord,
    EvidenceBundle,
    KernelCommercialResearchWorkflow,
    KernelResearchEngine,
    KernelStore,
    ProjectResearchInput,
    Project,
    ProjectArtifactReceipt,
    ProjectCustomerCommitmentReceipt,
    ProjectCustomerFeedback,
    ProjectOperatorLoadRecord,
    ProjectOutcome,
    ProjectRevenueAttribution,
    ProjectTask,
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
    project_close_resolution_command,
    project_customer_commitment_receipt_command,
    project_customer_visible_packet_command,
    project_customer_visible_replay_comparison_command,
    project_customer_visible_resolution_command,
    project_feedback_command,
    project_followup_delivery_command,
    project_operate_followup_outcome_command,
    project_operator_load_command,
    project_outcome_command,
    project_portfolio_packet_command,
    project_portfolio_replay_comparison_command,
    project_portfolio_resolution_command,
    project_post_ship_evidence_command,
    project_replay_comparison_command,
    project_revenue_attribution_command,
    project_scheduling_assignment_packet_command,
    project_scheduling_assignment_resolution_command,
    project_scheduling_intent_command,
    project_scheduling_priority_packet_command,
    project_scheduling_priority_replay_comparison_command,
    project_scheduling_priority_resolution_command,
    project_scheduling_replay_comparison_command,
    project_scheduling_task_outcome_command,
    project_status_rollup_command,
    project_task_command,
)
from migrate import apply_schema
from skills.db_manager import DatabaseManager


def request_command(key: str, payload: dict | None = None):
    return research_request_command(key=key, payload=payload or {"key": key})


class BaseKernelResearchTest(unittest.TestCase):
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

    def active_project_with_shipped_artifact(self, key: str) -> dict[str, str]:
        project = Project(
            name=f"Operate Follow-up {key}",
            objective="Exercise post-ship operate follow-up governance.",
            revenue_mechanism="software",
            operator_role="client_owner",
            external_commitment_policy="operator_only",
            phases=[
                {"name": "Validate", "objective": "Validate demand."},
                {"name": "Build", "objective": "Build artifact."},
                {"name": "Ship", "objective": "Ship artifact."},
                {"name": "Operate", "objective": "Operate customer-visible artifact."},
            ],
            success_metrics=["accepted customer feedback"],
            kill_criteria=["negative feedback without revenue"],
            status="active",
        )
        self.store.create_project(project_task_command(project_id=project.project_id, key=f"{key}-project"), project)
        task = ProjectTask(
            project_id=project.project_id,
            phase_name="Ship",
            task_type="ship",
            autonomy_class="A2",
            objective="Deliver a customer-visible artifact under operator gate.",
            inputs={"project_id": project.project_id},
            expected_output_schema={"type": "object", "required": ["side_effect_receipt_id"]},
            risk_level="medium",
            required_capabilities=[
                {
                    "capability_type": "side_effect",
                    "actions": ["prepare"],
                    "scope": "project_delivery",
                    "grant_required_before_run": True,
                }
            ],
            model_requirement={"task_class": "coding_small_patch", "local_allowed_only_if_promoted": True},
            authority_required="operator_gate",
            recovery_policy="ask_operator",
        )
        self.store.create_project_task(project_task_command(project_id=project.project_id, key=f"{key}-ship-task"), task)
        grant = CapabilityGrant(
            task_id=task.task_id,
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "publish", "artifact_ref": f"artifact://local/{key}/shipped"},
            scope={"project_id": project.project_id},
            conditions={"operator_approved": True},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=project.project_id, key=f"{key}-ship-grant"),
            grant,
        )
        self.store.assign_project_task(
            project_task_command(project_id=project.project_id, key=f"{key}-ship-assignment"),
            ProjectTaskAssignment(
                task_id=task.task_id,
                project_id=project.project_id,
                worker_type="agent",
                worker_id="ship-worker",
                grant_ids=[grant_id],
                accepted_capabilities=[
                    {"capability_type": "side_effect", "actions": ["prepare"], "scope": "project_delivery"}
                ],
            ),
        )
        intent = SideEffectIntent(
            task_id=task.task_id,
            side_effect_type="publish",
            target={"channel": "customer_review"},
            payload_hash=payload_hash({"artifact_ref": f"artifact://local/{key}/shipped"}),
            required_authority="operator_gate",
            grant_id=grant_id,
            timeout_policy="ask_operator",
        )
        intent_id = self.store.prepare_side_effect(
            project_task_command(
                project_id=project.project_id,
                key=f"{key}-side-effect-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            intent,
        )
        receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=project.project_id, key=f"{key}-side-effect-receipt"),
            SideEffectReceipt(
                intent_id=intent_id,
                receipt_type="success",
                receipt_hash=payload_hash({"published": True, "key": key}),
                details={"channel": "customer_review"},
            ),
        )
        artifact_id = self.commercial.record_project_artifact_receipt(
            project_artifact_receipt_command(project_id=project.project_id, key=f"{key}-shipped-artifact"),
            ProjectArtifactReceipt(
                project_id=project.project_id,
                task_id=task.task_id,
                artifact_ref=f"artifact://local/{key}/shipped",
                artifact_kind="shipped_artifact",
                summary="Accepted customer-visible shipped artifact.",
                data_class="internal",
                delivery_channel="customer_review",
                side_effect_intent_id=intent_id,
                side_effect_receipt_id=receipt_id,
                customer_visible=True,
                status="accepted",
            ),
        )
        return {
            "project_id": project.project_id,
            "task_id": task.task_id,
            "artifact_receipt_id": artifact_id,
            "side_effect_receipt_id": receipt_id,
        }

    def record_post_ship_evidence(
        self,
        key: str,
        shipped: dict[str, str],
        *,
        summary: str,
        sentiment: str = "positive",
        action_required: bool = True,
        revenue_amount: Decimal = Decimal("100"),
        revenue_status: str = "reconciled",
        revenue_confidence: float = 0.9,
        load_minutes: int = 5,
    ) -> dict[str, str]:
        return self.commercial.record_project_post_ship_evidence(
            project_post_ship_evidence_command(
                project_id=shipped["project_id"],
                artifact_receipt_id=shipped["artifact_receipt_id"],
                key=f"{key}-post-ship-evidence",
            ),
            shipped["artifact_receipt_id"],
            feedback=ProjectCustomerFeedback(
                project_id=shipped["project_id"],
                task_id=shipped["task_id"],
                source_type="customer",
                customer_ref=f"customer-{key}",
                summary=summary,
                sentiment=sentiment,  # type: ignore[arg-type]
                action_required=action_required,
                operator_review_required=False,
                status="accepted",
            ),
            revenue=ProjectRevenueAttribution(
                project_id=shipped["project_id"],
                task_id=shipped["task_id"],
                amount_usd=revenue_amount,
                source="operator_reported",
                attribution_period="2026-05",
                confidence=revenue_confidence,
                external_ref=f"operator://revenue/{key}" if revenue_status == "reconciled" else None,
                status=revenue_status,  # type: ignore[arg-type]
            ),
            operator_load=ProjectOperatorLoadRecord(
                project_id=shipped["project_id"],
                task_id=shipped["task_id"],
                minutes=load_minutes,
                load_type="client_sales",
                source="operator_reported",
                notes="Post-ship customer evidence review",
            ),
        )

    def running_operate_followup_task(self, key: str, *, summary: str) -> dict[str, str]:
        shipped = self.active_project_with_shipped_artifact(key)
        self.record_post_ship_evidence(key, shipped, summary=summary)
        rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=shipped["project_id"], key=f"{key}-rollup"),
            shipped["project_id"],
        )
        close_packet = self.commercial.create_project_close_decision(
            project_close_decision_command(project_id=shipped["project_id"], key=f"{key}-close"),
            shipped["project_id"],
            rollup_id=rollup.rollup_id,
        )
        resolution = self.commercial.resolve_project_close_decision(
            project_close_resolution_command(
                packet_id=close_packet.packet_id,
                verdict="continue",
                key=f"{key}-resolution",
            ),
            close_packet.packet_id,
            verdict="continue",
            operator_id="operator",
            notes="Continue with governed Operate follow-up.",
        )
        task_id = resolution["followup_task_id"]
        grant = CapabilityGrant(
            task_id=task_id,
            subject_type="agent",
            subject_id="operate-worker",
            capability_type="memory_write",
            actions=["record"],
            resource={"kind": "project_operate_followup"},
            scope={"project_id": shipped["project_id"]},
            conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=shipped["project_id"], key=f"{key}-operate-grant"),
            grant,
        )
        assignment_id = self.store.assign_project_task(
            project_task_command(project_id=shipped["project_id"], key=f"{key}-operate-assignment"),
            ProjectTaskAssignment(
                task_id=task_id,
                project_id=shipped["project_id"],
                worker_type="agent",
                worker_id="operate-worker",
                grant_ids=[grant_id],
                accepted_capabilities=[
                    {"capability_type": "memory_write", "actions": ["record"], "scope": "project_operate_followup"}
                ],
                notes="bounded operate worker accepted the follow-up",
            ),
        )
        return {
            **shipped,
            "followup_task_id": task_id,
            "operate_grant_id": grant_id,
            "operate_assignment_id": assignment_id,
        }

    def staged_operate_side_effect(self, key: str, project_id: str, task_id: str) -> dict[str, str]:
        grant = CapabilityGrant(
            task_id=task_id,
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "message", "target": f"customer-{key}"},
            scope={"project_id": project_id},
            conditions={"operator_approved": True},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=project_id, key=f"{key}-operate-side-effect-grant"),
            grant,
        )
        intent_id = self.store.prepare_side_effect(
            project_task_command(
                project_id=project_id,
                key=f"{key}-operate-side-effect-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            SideEffectIntent(
                task_id=task_id,
                side_effect_type="message",
                target={"customer_ref": f"customer-{key}", "channel": "support_desk"},
                payload_hash=payload_hash({"message_ref": f"artifact://local/{key}/support-response"}),
                required_authority="operator_gate",
                grant_id=grant_id,
                timeout_policy="ask_operator",
            ),
        )
        receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=project_id, key=f"{key}-operate-side-effect-receipt"),
            SideEffectReceipt(
                intent_id=intent_id,
                receipt_type="success",
                receipt_hash=payload_hash({"sent": True, "key": key}),
                details={"message_ref": f"artifact://local/{key}/support-response"},
            ),
        )
        return {"grant_id": grant_id, "intent_id": intent_id, "receipt_id": receipt_id}

    def budgeted_running_operate_task(
        self,
        key: str,
        *,
        budget_cap: Decimal,
        reserved_budget: Decimal = Decimal("0"),
        followup_type: str = "revenue_reconciliation",
    ) -> dict[str, str]:
        project_id = new_id()
        budget = Budget(
            owner_type="project",
            owner_id=project_id,
            approved_by="operator",
            cap_usd=budget_cap,
            expires_at="2999-01-01T00:00:00Z",
        )
        budget_id = self.store.create_budget(
            project_task_command(project_id=project_id, key=f"{key}-budget", requested_by="operator"),
            budget,
        )
        if reserved_budget:
            self.store.reserve_budget(
                project_task_command(project_id=project_id, key=f"{key}-budget-reserve", requested_by="operator"),
                budget_id,
                reserved_budget,
            )
        project = Project(
            project_id=project_id,
            name=f"Portfolio Project {key}",
            objective="Exercise portfolio tradeoff scoring.",
            revenue_mechanism="software",
            operator_role="client_owner",
            external_commitment_policy="operator_only",
            budget_id=budget_id,
            phases=[{"name": "Operate", "objective": "Operate customer-facing commercial loop."}],
            success_metrics=["reconciled revenue", "retained customers"],
            kill_criteria=["operator load exceeds value"],
            status="active",
        )
        self.store.create_project(project_task_command(project_id=project_id, key=f"{key}-project"), project)
        task = ProjectTask(
            project_id=project_id,
            phase_name="Operate",
            task_type="operate",
            autonomy_class="A1",
            objective="Record governed portfolio evidence.",
            inputs={
                "operate_followup_type": followup_type,
                "external_commitment_policy": "draft_or_internal_only_without_side_effect_receipt",
                "default_operator_load_type": "reconciliation" if followup_type == "revenue_reconciliation" else "client_sales",
            },
            expected_output_schema={"type": "object", "required": ["internal_result_ref"]},
            risk_level="low",
            required_capabilities=[
                {
                    "capability_type": "memory_write",
                    "actions": ["record"],
                    "scope": "project_operate_followup",
                    "grant_required_before_run": True,
                }
            ],
            model_requirement={"task_class": "quick_research_summarization", "local_allowed_only_if_promoted": True},
            budget_id=budget_id,
            authority_required="rule",
            recovery_policy="ask_operator",
        )
        self.store.create_project_task(project_task_command(project_id=project_id, key=f"{key}-task"), task)
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=project_id, key=f"{key}-grant"),
            CapabilityGrant(
                task_id=task.task_id,
                subject_type="agent",
                subject_id="portfolio-worker",
                capability_type="memory_write",
                actions=["record"],
                resource={"kind": "portfolio_evidence"},
                scope={"project_id": project_id},
                conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        self.store.assign_project_task(
            project_task_command(project_id=project_id, key=f"{key}-assignment"),
            ProjectTaskAssignment(
                task_id=task.task_id,
                project_id=project_id,
                worker_type="agent",
                worker_id="portfolio-worker",
                grant_ids=[grant_id],
                accepted_capabilities=[
                    {"capability_type": "memory_write", "actions": ["record"], "scope": "project_operate_followup"}
                ],
            ),
        )
        return {"project_id": project_id, "task_id": task.task_id, "budget_id": budget_id}

    def accepted_priority_created_task(self, key: str, *, budget_cap: Decimal = Decimal("650")) -> dict[str, str]:
        running = self.budgeted_running_operate_task(key, budget_cap=budget_cap)
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["task_id"],
                key=f"{key}-priority-outcome",
            ),
            running["task_id"],
            summary="Reconciled revenue evidence should drive a scheduling-created internal queue item.",
            internal_result_ref=f"artifact://local/{key}/priority-revenue",
            operator_load_minutes=4,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "600", "currency": "USD", "period": "2026-05"},
        )
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[running["project_id"]], key=f"{key}-priority-portfolio"),
            [running["project_id"]],
            constraints={"high_revenue_usd": "500"},
        )
        self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_prioritization",
                key=f"{key}-priority-portfolio-accepted",
            ),
            packet.packet_id,
            verdict="accept_prioritization",
        )
        intent = self.commercial.create_project_scheduling_intent(
            project_scheduling_intent_command(packet_id=packet.packet_id, key=f"{key}-priority-intent"),
            packet.packet_id,
        )
        priority_packet = self.commercial.create_project_scheduling_priority_change_packet(
            project_scheduling_priority_packet_command(intent_id=intent.intent_id, key=f"{key}-priority-packet"),
            intent.intent_id,
        )
        resolution = self.commercial.resolve_project_scheduling_priority_change_packet(
            project_scheduling_priority_resolution_command(
                packet_id=priority_packet.packet_id,
                verdict="accept_priority_changes",
                key=f"{key}-priority-resolution",
            ),
            priority_packet.packet_id,
            verdict="accept_priority_changes",
        )
        created = next(change for change in resolution["applied_changes"] if change["status"] == "queued")
        return {
            "project_id": running["project_id"],
            "source_task_id": running["task_id"],
            "task_id": created["task_id"],
            "budget_id": running["budget_id"],
            "priority_packet_id": priority_packet.packet_id,
        }

    def accepted_assigned_priority_created_task(self, key: str) -> dict[str, str]:
        created = self.accepted_priority_created_task(key)
        worker_id = f"{key}-worker"
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=created["project_id"], key=f"{key}-worker-grant"),
            CapabilityGrant(
                task_id=created["task_id"],
                subject_type="agent",
                subject_id=worker_id,
                capability_type="memory_write",
                actions=["record"],
                resource={"kind": "project_internal_scheduling"},
                scope={"project_id": created["project_id"]},
                conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        assignment_id = self.commercial.create_project_scheduling_worker_assignment_packet(
            project_scheduling_assignment_packet_command(task_id=created["task_id"], key=f"{key}-assignment-packet"),
            created["task_id"],
            worker_id=worker_id,
            grant_ids=[grant_id],
        )
        self.commercial.resolve_project_scheduling_worker_assignment(
            project_scheduling_assignment_resolution_command(
                assignment_id=assignment_id,
                verdict="accept",
                key=f"{key}-assignment-accept",
                requester_id=worker_id,
            ),
            assignment_id,
            verdict="accept",
            worker_id=worker_id,
            accepted_capabilities=[
                {"capability_type": "memory_write", "actions": ["record"], "scope": "project_internal_scheduling"}
            ],
        )
        return {**created, "grant_id": grant_id, "assignment_id": assignment_id, "worker_id": worker_id}

    def completed_internal_scheduling_outcome(self, key: str) -> dict[str, str]:
        created = self.accepted_assigned_priority_created_task(key)
        outcome = self.commercial.record_project_scheduling_task_outcome(
            project_scheduling_task_outcome_command(
                project_id=created["project_id"],
                task_id=created["task_id"],
                key=f"{key}-internal-outcome",
                requester_id=created["worker_id"],
            ),
            created["task_id"],
            summary="Completed internal customer-support response draft with preserved scheduling evidence.",
            internal_result_ref=f"artifact://local/{key}/customer-response-draft",
            result={"scheduling_outcome_type": "customer_support", "support_status": "drafted"},
        )
        return {**created, "outcome_id": outcome["outcome_id"]}

    def staged_customer_visible_intent(self, key: str, project_id: str, task_id: str) -> dict[str, str]:
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=project_id, key=f"{key}-customer-visible-grant"),
            CapabilityGrant(
                task_id=task_id,
                subject_type="adapter",
                subject_id="side_effect_broker",
                capability_type="side_effect",
                actions=["prepare"],
                resource={"kind": "message", "target": f"customer-{key}"},
                scope={"project_id": project_id},
                conditions={"operator_approved": True},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        intent_id = self.store.prepare_side_effect(
            project_task_command(
                project_id=project_id,
                key=f"{key}-customer-visible-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            SideEffectIntent(
                task_id=task_id,
                side_effect_type="message",
                target={"customer_ref": f"customer-{key}", "channel": "email"},
                payload_hash=payload_hash({"payload_ref": f"artifact://local/{key}/customer-response-draft"}),
                required_authority="operator_gate",
                grant_id=grant_id,
                timeout_policy="ask_operator",
            ),
        )
        return {"grant_id": grant_id, "intent_id": intent_id}

    def accepted_customer_visible_commitment(self, key: str) -> dict[str, str]:
        completed = self.completed_internal_scheduling_outcome(key)
        intent = self.staged_customer_visible_intent(key, completed["project_id"], completed["task_id"])
        packet = self.commercial.create_project_customer_visible_packet(
            project_customer_visible_packet_command(
                outcome_id=completed["outcome_id"],
                key=f"{key}-customer-visible-create",
            ),
            completed["outcome_id"],
            packet_type="customer_message",
            customer_ref=f"customer-{key}",
            channel="email",
            subject="Support response draft",
            summary="Operator packet for a customer-visible support response.",
            payload_ref=f"artifact://local/{key}/customer-response-draft",
            side_effect_intent_id=intent["intent_id"],
        )
        receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=completed["project_id"], key=f"{key}-customer-visible-receipt"),
            SideEffectReceipt(
                intent_id=intent["intent_id"],
                receipt_type="success",
                receipt_hash=payload_hash({"sent": True, "packet": packet.packet_id}),
                details={"message_ref": f"artifact://local/{key}/customer-response-draft"},
            ),
        )
        resolution = self.commercial.resolve_project_customer_visible_packet(
            project_customer_visible_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_customer_visible_packet",
                key=f"{key}-customer-visible-resolution",
            ),
            packet.packet_id,
            verdict="accept_customer_visible_packet",
            side_effect_receipt_id=receipt_id,
        )
        return {
            **completed,
            "packet_id": packet.packet_id,
            "intent_id": intent["intent_id"],
            "side_effect_receipt_id": receipt_id,
            "commitment_id": resolution["customer_commitment_id"],
        }

    def running_commitment_receipt_followup_task(
        self,
        key: str,
        *,
        receipt_type: str,
        summary: str,
        source_type: str = "platform",
    ) -> dict[str, str]:
        accepted = self.accepted_customer_visible_commitment(key)
        result = self.commercial.record_project_customer_commitment_receipt(
            project_customer_commitment_receipt_command(
                commitment_id=accepted["commitment_id"],
                key=f"{key}-receipt-record",
            ),
            ProjectCustomerCommitmentReceipt(
                commitment_id=accepted["commitment_id"],
                project_id=accepted["project_id"],
                receipt_type=receipt_type,  # type: ignore[arg-type]
                source_type=source_type,  # type: ignore[arg-type]
                summary=summary,
                evidence_refs=[f"platform://commitment-receipts/{key}"],
                action_required=True,
                status="needs_followup",
            ),
        )
        task_id = result["followup_task_id"]
        grant = CapabilityGrant(
            task_id=task_id,
            subject_type="agent",
            subject_id=f"{key}-receipt-worker",
            capability_type="memory_write",
            actions=["record"],
            resource={"kind": "project_commitment_receipt_followup"},
            scope={"project_id": accepted["project_id"]},
            conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=accepted["project_id"], key=f"{key}-receipt-grant"),
            grant,
        )
        assignment_id = self.store.assign_project_task(
            project_task_command(project_id=accepted["project_id"], key=f"{key}-receipt-assignment"),
            ProjectTaskAssignment(
                task_id=task_id,
                project_id=accepted["project_id"],
                worker_type="agent",
                worker_id=f"{key}-receipt-worker",
                grant_ids=[grant_id],
                accepted_capabilities=[
                    {
                        "capability_type": "memory_write",
                        "actions": ["record"],
                        "scope": "project_commitment_receipt_followup",
                    }
                ],
                notes="bounded worker accepted the customer commitment receipt follow-up",
            ),
        )
        return {
            **accepted,
            "receipt_id": result["receipt_id"],
            "followup_task_id": task_id,
            "receipt_grant_id": grant_id,
            "receipt_assignment_id": assignment_id,
        }
