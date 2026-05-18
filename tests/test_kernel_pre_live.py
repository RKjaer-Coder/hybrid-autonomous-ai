from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from kernel import KernelStore, PRE_LIVE_GOALS, summarize_pre_live_completion
from kernel.records import sha256_text


NOW = "2026-05-17T00:00:00Z"
J0 = "[]"
J1 = "{}"


def j(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def insert(conn, table: str, **values):
    columns = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    conn.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", tuple(values.values()))


class KernelPreLiveCompletionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = KernelStore(Path(self.tmp.name) / "kernel.db")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_empty_kernel_reports_all_pre_live_goal_blockers(self):
        bundle = summarize_pre_live_completion(self.store)

        self.assertFalse(bundle.complete)
        self.assertEqual(bundle.completed_goals, 0)
        self.assertEqual(bundle.total_goals, len(PRE_LIVE_GOALS))
        self.assertEqual({goal.goal_id for goal in bundle.goals}, {goal_id for goal_id, _ in PRE_LIVE_GOALS})
        self.assertTrue(bundle.next_blockers)

    def test_completion_bundle_turns_green_when_all_ten_repo_proofs_exist(self):
        self.populate_completion_state()

        bundle = summarize_pre_live_completion(self.store)

        self.assertTrue(bundle.complete, bundle.to_dict())
        self.assertEqual(bundle.completed_goals, 10)
        self.assertEqual(bundle.next_blockers, [])
        self.assertEqual(bundle.goals[2].evidence["task_classes"], [
            "coding_small_patch",
            "quick_research_summarization",
            "source_claim_extraction",
        ])

    def populate_completion_state(self) -> None:
        with self.store.connect() as conn:
            for command_id, authority in (("cmd-rule", None), ("cmd-gate", "operator_gate"), ("cmd-runtime", "rule")):
                insert(
                    conn,
                    "commands",
                    command_id=command_id,
                    command_type="prelive.test",
                    requested_by="operator",
                    requester_id="operator",
                    target_entity_type="kernel",
                    target_entity_id=None,
                    requested_authority=authority,
                    payload_hash=sha256_text(command_id),
                    payload_json=j({"command_id": command_id}),
                    idempotency_key=command_id,
                    submitted_at=NOW,
                    status="applied",
                    result_event_id=None,
                )
            insert(
                conn,
                "events",
                event_id="event-1",
                event_schema_version=1,
                event_type="pre_live_completion_fixture",
                entity_type="policy",
                entity_id="pre-live",
                transaction_id="tx-1",
                command_id="cmd-rule",
                correlation_id=None,
                causation_event_id=None,
                actor_type="kernel",
                actor_id="kernel",
                timestamp=NOW,
                policy_version="test",
                data_class="internal",
                payload_hash=sha256_text("event"),
                payload_json=j({"ok": True}),
                prev_event_hash=None,
                event_hash=sha256_text("event-hash"),
            )

            # Research, retrieval, commercial packet, and Council recommendation.
            insert(
                conn,
                "research_requests",
                request_id="research-1",
                profile="commercial",
                question="Validate governed model efficiency service.",
                decision_target="model-efficiency-service",
                freshness_horizon="P30D",
                depth="standard",
                source_policy_json=j({"allowed_source_types": ["official", "internal_record"]}),
                evidence_requirements_json=j({"minimum_sources": 2}),
                max_cost_usd="0",
                max_latency="PT30M",
                autonomy_class="A2",
                status="completed",
                created_at=NOW,
                updated_at=NOW,
            )
            insert(
                conn,
                "source_plans",
                source_plan_id="source-plan-1",
                request_id="research-1",
                profile="commercial",
                depth="standard",
                planned_sources_json=j([{"url_or_ref": "internal://customer", "access_method": "operator_provided"}]),
                retrieval_strategy="grant, acquire, dedupe, synthesize",
                created_by="kernel",
                status="completed",
                created_at=NOW,
            )
            for grant_id, capability, subject in (
                ("grant-file", "file", "research_retrieval_broker"),
                ("grant-network", "network", "local_forward_proxy"),
                ("grant-side-effect", "side_effect", "side_effect_broker"),
            ):
                insert(
                    conn,
                    "capability_grants",
                    grant_id=grant_id,
                    task_id="task-ship",
                    subject_type="adapter",
                    subject_id=subject,
                    capability_type=capability,
                    actions_json=j(["retrieve" if capability != "side_effect" else "prepare"]),
                    resource_json=j({"kind": capability}),
                    scope_json=j({"project_id": "project-1"}),
                    conditions_json=J1,
                    issued_at=NOW,
                    expires_at="2999-01-01T00:00:00Z",
                    max_uses=1,
                    used_count=0,
                    issuer="kernel",
                    policy_version="test",
                    revalidate_on_use=1,
                    status="active",
                )
            insert(
                conn,
                "source_acquisition_checks",
                check_id="source-check-1",
                request_id="research-1",
                source_plan_id="source-plan-1",
                source_ref="internal://customer",
                access_method="operator_provided",
                data_class="internal",
                source_type="internal_record",
                result="allowed",
                reason="grant present",
                grant_id="grant-file",
                checked_at=NOW,
            )
            source_id = "source-1"
            claim_id = "claim-1"
            insert(
                conn,
                "evidence_bundles",
                bundle_id="bundle-1",
                request_id="research-1",
                source_plan_id="source-plan-1",
                sources_json=j([{"source_id": source_id, "url_or_ref": "internal://customer"}]),
                claims_json=j([{"claim_id": claim_id, "importance": "high", "source_ids": [source_id]}]),
                contradictions_json=J0,
                unsupported_claims_json=J0,
                freshness_summary="fresh",
                confidence=0.82,
                uncertainty="bounded pre-live evidence",
                counter_thesis="may remain narrow",
                quality_gate_result="pass",
                data_classes_json=j(["internal"]),
                retention_policy="retain-90d",
                created_at=NOW,
            )
            insert_decision(conn, "decision-commercial", "project_approval", "operator_gate", "gated")
            insert_decision(conn, "decision-promotion", "model_promotion", "operator_gate", "gated")
            insert_decision(conn, "decision-customer", "other", "operator_gate", "gated")
            insert(
                conn,
                "commercial_decision_packets",
                packet_id="commercial-packet-1",
                decision_id="decision-commercial",
                request_id="research-1",
                evidence_bundle_id="bundle-1",
                decision_target="model-efficiency-service",
                question="Approve service packet?",
                recommendation="pursue",
                required_authority="operator_gate",
                opportunity_json=j({"status": "gated"}),
                project_json=j({"status": "proposed"}),
                gate_packet_json=j({"default_on_timeout": "pause"}),
                evidence_used_json=j([claim_id]),
                risk_flags_json=J0,
                default_on_timeout="pause",
                status="gated",
                created_at=NOW,
            )
            insert(
                conn,
                "commercial_decision_recommendations",
                record_id="commercial-recommendation-1",
                packet_id="commercial-packet-1",
                decision_id="decision-commercial",
                request_id="research-1",
                evidence_bundle_id="bundle-1",
                recommendation_authority="council",
                recommendation="pursue",
                confidence=0.82,
                decisive_factors_json=j([claim_id]),
                decisive_uncertainty="high customer impact",
                evidence_used_json=j([claim_id]),
                evidence_refs_json=j(["kernel:evidence_bundles/bundle-1"]),
                quality_gate_context_json=j({"result": "pass"}),
                risk_flags_json=J0,
                operator_gate_defaults_json=j({"required_authority": "operator_gate"}),
                rationale="Council required because this is a commercial high-uncertainty decision.",
                model_routes_used_json=J0,
                degraded=0,
                created_at=NOW,
            )

            # Project loop, customer-visible packet, side effects, and replay packaging.
            insert_project(conn)
            for task_id, task_type, authority, risk in (
                ("task-validate", "validate", "single_agent", "medium"),
                ("task-build", "build", "single_agent", "medium"),
                ("task-ship", "ship", "operator_gate", "medium"),
                ("task-council", "operate", "council", "high"),
            ):
                insert_project_task(conn, task_id, task_type, authority, risk)
            insert(
                conn,
                "side_effect_intents",
                intent_id="intent-message",
                task_id="task-ship",
                side_effect_type="message",
                target_json=j({"customer_ref": "customer-1"}),
                payload_hash=sha256_text("message"),
                required_authority="operator_gate",
                grant_id="grant-side-effect",
                timeout_policy="ask_operator",
                status="prepared",
            )
            insert(
                conn,
                "side_effect_intents",
                intent_id="intent-provider",
                task_id="task-validate",
                side_effect_type="provider_call",
                target_json=j({"endpoint": "http://127.0.0.1:1234"}),
                payload_hash=sha256_text("provider"),
                required_authority="rule",
                grant_id="grant-network",
                timeout_policy="deny",
                status="prepared",
            )
            insert(
                conn,
                "side_effect_receipts",
                receipt_id="receipt-message",
                intent_id="intent-message",
                receipt_type="success",
                receipt_hash=sha256_text("receipt"),
                details_json=j({"sent": True}),
                recorded_at=NOW,
            )
            for outcome_id, task_id, outcome_type in (
                ("outcome-validation", "task-validate", "validation"),
                ("outcome-build", "task-build", "build_artifact"),
                ("outcome-ship", "task-ship", "shipped_artifact"),
                ("outcome-feedback", "task-ship", "feedback"),
            ):
                insert(
                    conn,
                    "project_outcomes",
                    outcome_id=outcome_id,
                    project_id="project-1",
                    task_id=task_id,
                    phase_name=outcome_type,
                    outcome_type=outcome_type,
                    summary=f"{outcome_type} complete",
                    artifact_refs_json=j([f"artifact://{outcome_type}"]),
                    metrics_json=J1,
                    feedback_json=J1,
                    revenue_impact_json=J1,
                    operator_load_actual="5 minutes",
                    side_effect_intent_id="intent-message" if outcome_type == "shipped_artifact" else None,
                    side_effect_receipt_id="receipt-message" if outcome_type == "shipped_artifact" else None,
                    status="accepted",
                    created_at=NOW,
                )
            for receipt_id, task_id, kind in (
                ("artifact-validation", "task-validate", "validation_artifact"),
                ("artifact-build", "task-build", "build_artifact"),
                ("artifact-ship", "task-ship", "shipped_artifact"),
            ):
                insert(
                    conn,
                    "project_artifact_receipts",
                    receipt_id=receipt_id,
                    project_id="project-1",
                    task_id=task_id,
                    artifact_ref=f"artifact://{kind}",
                    artifact_kind=kind,
                    summary=kind,
                    data_class="internal",
                    delivery_channel="local",
                    side_effect_intent_id="intent-message" if kind == "shipped_artifact" else None,
                    side_effect_receipt_id="receipt-message" if kind == "shipped_artifact" else None,
                    customer_visible=1 if kind == "shipped_artifact" else 0,
                    status="accepted",
                    created_at=NOW,
                )
            insert_customer_visible(conn)
            insert_comparison(conn, "project_replay_projection_comparisons", comparison_id="project-compare", project_id="project-1")

            # Seed model intelligence and model-efficiency shadow proof.
            for task_class in ("quick_research_summarization", "source_claim_extraction", "coding_small_patch"):
                insert_model_task_class(conn, task_class)
                insert_model_eval_set_chain(conn, task_class)
            insert(
                conn,
                "model_candidates",
                candidate_id="candidate-1",
                model_id="mlx/test-local",
                provider="mlx",
                access_mode="local",
                source_ref="hf://example/test-local",
                artifact_hash=sha256_text("model"),
                license="apache-2.0",
                commercial_use="allowed",
                terms_verified_at=NOW,
                context_window=32768,
                modalities_json=j(["text"]),
                hardware_fit="good",
                sandbox_profile="local-readonly",
                data_residency="local_only",
                cost_profile_json=j({"usd_per_1k": "0"}),
                latency_profile_json=j({"p95_ms": 10000}),
                routing_metadata_json=J1,
                promotion_state="shadow",
                last_verified_at=NOW,
            )
            for task_class in ("quick_research_summarization", "source_claim_extraction", "coding_small_patch"):
                insert_model_eval_run(conn, task_class)
            insert(
                conn,
                "model_route_decisions",
                route_decision_id="route-shadow",
                task_id="task-validate",
                task_class="quick_research_summarization",
                data_class="internal",
                risk_level="medium",
                selected_route="shadow",
                selected_model_id=None,
                candidate_model_id="mlx/test-local",
                eval_set_id="eval-quick_research_summarization",
                reasons_json=j(["shadow only"]),
                required_authority="operator_gate",
                decision_id=None,
                local_offload_estimate_json=j({"estimated_savings_usd_per_1k": "10"}),
                frontier_fallback_json=j({"route_effect": "unchanged"}),
                created_at=NOW,
            )
            insert(
                conn,
                "model_promotion_decision_packets",
                packet_id="promotion-packet",
                decision_id="decision-promotion",
                model_id="mlx/test-local",
                task_class="quick_research_summarization",
                proposed_routing_role="research_local",
                recommendation="keep_shadow",
                required_authority="operator_gate",
                eval_run_ids_json=j(["eval-run-quick_research_summarization"]),
                holdout_use_ids_json=J0,
                evidence_refs_json=j(["kernel:model_eval_runs/eval-run-quick_research_summarization"]),
                frozen_holdout_confidence=0.82,
                confidence_threshold=0.9,
                gate_packet_json=j({"default_on_timeout": "keep_current_route"}),
                risk_flags_json=j(["shadow_only"]),
                default_on_timeout="keep_current_route",
                status="gated",
                created_at=NOW,
            )

            # Data governance, recovery, Hermes adapter, migration, and packaging comparisons.
            insert_data_governance(conn)
            insert_readiness_and_migration(conn)
            insert_packaging_comparisons(conn)


def insert_decision(conn, decision_id: str, decision_type: str, required_authority: str, status: str) -> None:
    insert(
        conn,
        "decisions",
        decision_id=decision_id,
        decision_type=decision_type,
        question=f"{decision_type}?",
        options_json=j([{"option_id": "approve"}]),
        stakes="high" if required_authority == "operator_gate" else "medium",
        evidence_bundle_ids_json=j(["bundle-1"]) if decision_type == "project_approval" else J0,
        evidence_refs_json=J0,
        requested_by="model_intelligence" if decision_type == "model_promotion" else "research",
        required_authority=required_authority,
        authority_policy_version="test",
        deadline=None,
        status=status,
        recommendation="approve",
        verdict=None,
        confidence=0.8,
        decisive_factors_json=J0,
        decisive_uncertainty="bounded",
        risk_flags_json=J0,
        default_on_timeout="pause",
        gate_packet_json=j({"required_authority": required_authority}),
        created_at=NOW,
        decided_at=None,
    )


def insert_project(conn) -> None:
    insert(
        conn,
        "projects",
        project_id="project-1",
        opportunity_id=None,
        decision_packet_id="commercial-packet-1",
        decision_id="decision-commercial",
        name="Governed Model Efficiency Service",
        objective="Prove pre-live project loop.",
        revenue_mechanism="service",
        operator_role="client_owner",
        external_commitment_policy="operator_only",
        phases_json=j([{"name": "Validate"}, {"name": "Build"}, {"name": "Ship"}, {"name": "Operate"}]),
        success_metrics_json=j(["quality"]),
        kill_criteria_json=j(["no savings"]),
        evidence_refs_json=j(["kernel:evidence_bundles/bundle-1"]),
        status="active",
        created_at=NOW,
        updated_at=NOW,
    )


def insert_project_task(conn, task_id: str, task_type: str, authority: str, risk: str) -> None:
    insert(
        conn,
        "project_tasks",
        task_id=task_id,
        project_id="project-1",
        phase_name=task_type.title(),
        task_type=task_type,
        autonomy_class="A2",
        objective=f"{task_type} objective",
        inputs_json=J1,
        expected_output_schema_json=J1,
        risk_level=risk,
        required_capabilities_json=J0,
        model_requirement_json=j({"task_class": "quick_research_summarization"}),
        budget_id=None,
        deadline=None,
        status="completed" if task_type in {"validate", "build", "ship"} else "queued",
        authority_required=authority,
        recovery_policy="ask_operator",
        command_id=None,
        policy_version="test",
        idempotency_key=task_id,
        evidence_refs_json=J0,
        created_at=NOW,
        updated_at=NOW,
    )


def insert_customer_visible(conn) -> None:
    insert(
        conn,
        "project_customer_visible_packets",
        packet_id="customer-packet",
        project_id="project-1",
        outcome_id="outcome-ship",
        decision_id="decision-customer",
        packet_type="customer_message",
        customer_ref="customer-1",
        channel="email",
        subject="Delivery",
        summary="Operator-gated delivery",
        payload_ref="artifact://payload",
        side_effect_intent_id="intent-message",
        evidence_refs_json=J0,
        risk_flags_json=j(["customer_visible_commitment_requires_receipt"]),
        required_authority="operator_gate",
        default_on_timeout="pause",
        status="decided",
        verdict="accept_customer_visible_packet",
        created_at=NOW,
        decided_by="operator",
        decided_at=NOW,
    )
    insert(
        conn,
        "project_customer_commitments",
        commitment_id="commitment-1",
        packet_id="customer-packet",
        project_id="project-1",
        outcome_id="outcome-ship",
        side_effect_intent_id="intent-message",
        side_effect_receipt_id="receipt-message",
        customer_ref="customer-1",
        channel="email",
        commitment_type="message_sent",
        payload_ref="artifact://payload",
        summary="Sent",
        evidence_refs_json=J0,
        created_at=NOW,
    )
    insert(
        conn,
        "project_customer_commitment_receipts",
        receipt_id="commitment-receipt-1",
        commitment_id="commitment-1",
        project_id="project-1",
        receipt_type="customer_response",
        source_type="customer",
        customer_ref="customer-1",
        summary="Received",
        evidence_refs_json=J0,
        action_required=0,
        status="accepted",
        followup_task_id=None,
        created_at=NOW,
    )


def insert_model_task_class(conn, task_class: str) -> None:
    insert(
        conn,
        "model_task_classes",
        task_class_id=f"class-{task_class}",
        task_class=task_class,
        description=task_class,
        quality_threshold=0.8,
        reliability_threshold=0.9,
        latency_p95_ms=30000,
        local_offload_target=0.3,
        allowed_data_classes_json=j(["public", "internal"]),
        promotion_authority="operator_gate",
        expansion_allowed=0,
        status="seed",
        created_at=NOW,
    )


def insert_model_eval_set_chain(conn, task_class: str) -> None:
    insert(
        conn,
        "model_holdout_policies",
        policy_id=f"policy-{task_class}",
        task_class=task_class,
        dataset_version="seed-2026-05-17",
        access="scoring_service",
        min_sample_count=12,
        contamination_controls_json=j(["sealed"]),
        scorer_separation="separate",
        promotion_requires_decision=1,
        created_at=NOW,
    )
    insert(
        conn,
        "local_offload_eval_sets",
        eval_set_id=f"eval-{task_class}",
        task_class=task_class,
        dataset_version="seed-2026-05-17",
        artifact_ref=f"artifact://eval/{task_class}",
        split_counts_json=j({"frozen_holdout": 12}),
        data_classes_json=j(["public", "internal"]),
        retention_policy="retain-180d",
        scorer_profile_json=J1,
        holdout_policy_id=f"policy-{task_class}",
        status="active",
        created_at=NOW,
    )


def insert_model_eval_run(conn, task_class: str) -> None:
    insert(
        conn,
        "model_eval_runs",
        eval_run_id=f"eval-run-{task_class}",
        model_id="mlx/test-local",
        task_class=task_class,
        dataset_version="seed-2026-05-17",
        eval_set_id=f"eval-{task_class}",
        baseline_model_id=None,
        route_version="seed",
        route_metadata_json=J1,
        sample_count=12,
        quality_score=0.84,
        reliability_score=0.96,
        latency_p50_ms=1000,
        latency_p95_ms=2000,
        cost_per_1k_tasks="0",
        aggregate_scores_json=j({"overall": 0.9}),
        failure_categories_json=J0,
        failure_modes_json=J0,
        confidence_json=j({"score": 0.82}),
        frozen_holdout_result_json=j({"quality_score": 0.84}),
        verdict="shadow",
        scorer_id="scorer",
        decision_id=None,
        authority_effect="evidence_only",
        created_at=NOW,
    )


def insert_data_governance(conn) -> None:
    insert(
        conn,
        "artifact_refs",
        artifact_id="artifact-ref-1",
        artifact_uri="artifact://payload",
        data_class="internal",
        content_hash=sha256_text("payload"),
        retention_policy="retain-90d",
        deletion_policy="crypto-shred",
        encryption_status="encrypted",
        source_notes="pre-live",
        created_at=NOW,
    )
    insert(
        conn,
        "encrypted_storage_descriptors",
        descriptor_id="descriptor-1",
        storage_scope="artifact_payload",
        owner_ref="artifact://payload",
        descriptor_uri="storage://payload",
        storage_backend="local_encrypted_store",
        local_path_ref="/tmp/payload",
        data_class="internal",
        ciphertext_hash=sha256_text("cipher"),
        plaintext_hash=sha256_text("plain"),
        size_bytes=10,
        encryption_algorithm="xchacha20-poly1305",
        key_ref="key://payload",
        key_version="v1",
        key_status="active",
        access_policy_json=j({"read": ["kernel"]}),
        retention_policy="retain-90d",
        deletion_policy="crypto-shred",
        evidence_refs_json=J0,
        status="active",
        created_at=NOW,
        updated_at=NOW,
    )
    insert(
        conn,
        "payload_access_receipts",
        receipt_id="payload-receipt-1",
        descriptor_id="descriptor-1",
        operation="read",
        subject_type="kernel",
        subject_id="kernel",
        grant_id=None,
        access_result="allowed",
        verification_status="verified",
        payload_hash=sha256_text("plain"),
        receipt_ref="receipt://payload",
        receipt_hash=sha256_text("payload-receipt"),
        evidence_refs_json=J0,
        details_json=J1,
        created_at=NOW,
    )
    insert(
        conn,
        "encrypted_storage_access_verification_states",
        verification_id="storage-verify-1",
        descriptor_id="descriptor-1",
        last_receipt_id="payload-receipt-1",
        status="verified",
        fail_closed=0,
        verification_checks_json=j({"hash": True}),
        mismatch_summary_json=J0,
        evidence_refs_json=J0,
        verified_at=NOW,
    )
    insert(
        conn,
        "backup_cadence_records",
        cadence_id="cadence-1",
        scope="kernel.db",
        cadence="daily",
        backup_target="artifact://backup",
        encryption_required=1,
        retention_policy="retain-30d",
        recovery_point_objective="24h",
        next_due_at="2999-01-01T00:00:00Z",
        evidence_refs_json=J0,
        status="active",
        created_at=NOW,
        updated_at=NOW,
    )


def insert_readiness_and_migration(conn) -> None:
    insert(
        conn,
        "recovery_readiness_packets",
        packet_id="recovery-ready",
        scope="kernel.db",
        as_of=NOW,
        backup_cadence_summary_json=J1,
        restore_drill_summary_json=J1,
        encrypted_payload_descriptor_summary_json=J1,
        payload_access_failure_summary_json=J1,
        fail_closed_state_json=J1,
        next_operator_actions_json=J0,
        readiness_status="ready",
        evidence_refs_json=J0,
        live_controls_enabled=0,
        created_at=NOW,
    )
    insert(
        conn,
        "hermes_adapter_readiness_packets",
        packet_id="hermes-ready",
        adapter_name="hermes-v0.14",
        hermes_version="0.14.0",
        as_of=NOW,
        surface_checks_json=j([{"status": "passed"}]),
        reconciliation_checks_json=j([{"status": "passed"}]),
        recovery_readiness_packet_id="recovery-ready",
        next_operator_actions_json=J0,
        readiness_status="ready",
        evidence_refs_json=J0,
        live_controls_enabled=0,
        created_at=NOW,
    )
    insert(
        conn,
        "migration_readiness_records",
        record_id="migration-1",
        surface_ref="legacy.db",
        component_type="database",
        ownership_action="convert-to-projection",
        owner_domain="kernel",
        summary="Projection only",
        blockers_json=J0,
        evidence_refs_json=J0,
        next_operator_actions_json=J0,
        readiness_status="ready",
        live_controls_enabled=0,
        created_at=NOW,
    )


def insert_comparison(conn, table: str, **ids) -> None:
    insert(
        conn,
        table,
        **ids,
        replay_project_status="active",
        projection_project_status="active",
        replay_task_counts_json=J1,
        projection_task_counts_json=J1,
        replay_revenue_attributed_usd="0",
        projection_revenue_attributed_usd="0",
        replay_operator_load_minutes=0,
        projection_operator_load_minutes=0,
        replay_commercial_rollup_json=J1,
        projection_commercial_rollup_json=J1,
        matches=1,
        mismatches_json=J0,
        created_at=NOW,
    )


def insert_packaging_comparisons(conn) -> None:
    insert(
        conn,
        "project_customer_visible_replay_projection_comparisons",
        comparison_id="customer-visible-compare",
        packet_id="customer-packet",
        replay_packet_json=J1,
        projection_packet_json=J1,
        replay_commitments_json=J0,
        projection_commitments_json=J0,
        replay_commitment_receipts_json=J0,
        projection_commitment_receipts_json=J0,
        matches=1,
        mismatches_json=J0,
        created_at=NOW,
    )
    for table, packet_id, comparison_id in (
        ("recovery_readiness_replay_projection_comparisons", "recovery-ready", "recovery-compare"),
        ("hermes_adapter_readiness_replay_projection_comparisons", "hermes-ready", "hermes-compare"),
    ):
        insert(
            conn,
            table,
            comparison_id=comparison_id,
            packet_id=packet_id,
            replay_packet_json=J1,
            projection_packet_json=J1,
            matches=1,
            mismatches_json=J0,
            created_at=NOW,
        )
    insert(
        conn,
        "migration_readiness_replay_projection_comparisons",
        comparison_id="migration-compare",
        scope="all",
        replay_records_json=J1,
        projection_records_json=J1,
        matches=1,
        mismatches_json=J0,
        created_at=NOW,
    )
