"""Deterministic first-live-project fixture for target-machine handoff."""

from __future__ import annotations

from .common import DeterministicFactory


_LIVE_VALIDATION_CHECKS = [
    {
        "surface": "kanban_worker_lifecycle",
        "hermes_input": "native_kanban_card_transition",
        "kernel_evidence": [
            "project_task_created",
            "project_task_assignment",
            "worker_heartbeat",
            "task_retry_or_blocked_reason",
            "project_task_completed_or_failed",
        ],
        "authority_effect": "projection_only_until_reconciled",
        "blocked_without_evidence": True,
    },
    {
        "surface": "goal_checkpoint_gateway_resume",
        "hermes_input": "goal_resume_or_acp_queue_continue",
        "kernel_evidence": [
            "task_status_revalidated",
            "assignment_owner_revalidated",
            "capability_grants_revalidated",
            "budget_grants_revalidated",
            "side_effect_idempotency_revalidated",
            "policy_version_revalidated",
            "halt_state_revalidated",
        ],
        "authority_effect": "resume_blocked_on_any_failed_revalidation",
        "blocked_without_evidence": True,
    },
    {
        "surface": "no_agent_cron_watchdog",
        "hermes_input": "native_no_agent_cron_tick",
        "kernel_evidence": ["watchdog_script_hash", "non_empty_findings", "inspection_or_alert_record"],
        "authority_effect": "deterministic_watchdog_only",
        "blocked_without_evidence": True,
    },
    {
        "surface": "provider_plugins_and_model_profiles",
        "hermes_input": "provider_or_profile_tool_call",
        "kernel_evidence": [
            "capability_grant",
            "budget_grant",
            "data_class_grant",
            "provider_call_intent",
            "provider_call_receipt",
        ],
        "authority_effect": "no_paid_or_credentialed_call_without_kernel_grants",
        "blocked_without_evidence": True,
    },
    {
        "surface": "hermes_openai_compatible_proxy",
        "hermes_input": "oauth_provider_request_through_hermes_proxy",
        "kernel_evidence": [
            "capability_grant",
            "budget_grant",
            "data_class_grant",
            "provider_call_intent",
            "provider_call_receipt",
            "proxy_audit_record",
        ],
        "authority_effect": "oauth_proxy_is_transport_not_spend_or_data_authority",
        "blocked_without_evidence": True,
    },
    {
        "surface": "live_session_handoff",
        "hermes_input": "handoff_active_session_to_model_profile_or_persona",
        "kernel_evidence": [
            "task_status_revalidated",
            "assignment_owner_revalidated",
            "capability_grants_revalidated",
            "budget_grants_revalidated",
            "side_effect_idempotency_revalidated",
            "policy_version_revalidated",
            "halt_state_revalidated",
        ],
        "authority_effect": "handoff_revalidates_before_any_continuation",
        "blocked_without_evidence": True,
    },
    {
        "surface": "api_approval_event_stream",
        "hermes_input": "approval_required_event_from_api_run_stream",
        "kernel_evidence": ["operator_gate_request", "operator_gate_decision", "timeout_default", "audit_record"],
        "authority_effect": "approval_stream_can_prompt_but_kernel_commits_gate_state",
        "blocked_without_evidence": True,
    },
    {
        "surface": "prompt_cache_route_revalidation",
        "hermes_input": "cross_session_prompt_cache_or_cached_provider_prefix",
        "kernel_evidence": ["fresh_route_decision", "budget_grant", "data_class_grant", "cache_scope_record"],
        "authority_effect": "cached_context_never_reuses_stale_paid_or_sensitive_authority",
        "blocked_without_evidence": True,
    },
    {
        "surface": "mcp_sse_oauth_forwarding",
        "hermes_input": "mcp_or_oauth_forwarded_request",
        "kernel_evidence": [
            "scope_preserved",
            "timeout_preserved",
            "media_artifact_ref",
            "failure_record_on_error",
            "broker_boundary_verified",
        ],
        "authority_effect": "forwarding_is_non_authoritative",
        "blocked_without_evidence": True,
    },
    {
        "surface": "native_dashboard_controls",
        "hermes_input": "dashboard_profile_plugin_provider_or_kanban_control",
        "kernel_evidence": ["local_auth_check", "timeout_semantics", "replay_semantics", "audit_record"],
        "authority_effect": "read_only_or_projection_only",
        "blocked_without_evidence": True,
    },
    {
        "surface": "watchers_change_detection",
        "hermes_input": "watcher_tick_for_rss_http_json_or_github",
        "kernel_evidence": ["watcher_source_policy", "script_hash", "non_empty_findings", "inspection_or_alert_record"],
        "authority_effect": "watchers_create_inspection_records_not_autonomous_work",
        "blocked_without_evidence": True,
    },
    {
        "surface": "teams_graph_gateway_pipeline",
        "hermes_input": "teams_graph_webhook_or_outbound_delivery",
        "kernel_evidence": ["platform_allowlist_match", "redaction_applied", "side_effect_intent", "delivery_receipt"],
        "authority_effect": "teams_gateway_is_notification_transport_until_operator_gate",
        "blocked_without_evidence": True,
    },
    {
        "surface": "x_search_external_source",
        "hermes_input": "authenticated_x_search_query",
        "kernel_evidence": ["research_source_policy", "freshness_record", "source_rights_record", "claim_citation"],
        "authority_effect": "x_search_outputs_are_research_sources_not_decisions",
        "blocked_without_evidence": True,
    },
    {
        "surface": "computer_use_backend",
        "hermes_input": "computer_use_action_from_any_vision_capable_provider",
        "kernel_evidence": ["capability_grant", "screen_scope_record", "side_effect_intent", "operator_halt_check"],
        "authority_effect": "gui_control_requires_current_task_scope_and_halt_check",
        "blocked_without_evidence": True,
    },
    {
        "surface": "lsp_write_diagnostics",
        "hermes_input": "write_file_or_patch_diagnostic_footer",
        "kernel_evidence": ["artifact_hash", "mutation_receipt", "diagnostic_record", "test_or_review_decision"],
        "authority_effect": "diagnostics_are_evidence_not_acceptance_authority",
        "blocked_without_evidence": True,
    },
    {
        "surface": "plugin_llm_tool_override",
        "hermes_input": "plugin_ctx_llm_or_tool_override_call",
        "kernel_evidence": ["plugin_allowlist", "capability_grant", "model_route_decision", "tool_override_audit"],
        "authority_effect": "plugin_llm_and_override_paths_cannot_bypass_kernel_brokers",
        "blocked_without_evidence": True,
    },
    {
        "surface": "platform_allowlists_redaction_media",
        "hermes_input": "artifact_or_platform_interaction",
        "kernel_evidence": ["allowlist_match", "redaction_default_applied", "media_artifact_governance"],
        "authority_effect": "kernel_independently_enforces_boundary",
        "blocked_without_evidence": True,
    },
    {
        "surface": "lm_studio_local_provider_routes",
        "hermes_input": "local_model_route_candidate",
        "kernel_evidence": ["seed_model_intelligence_eval_run", "shadow_route_decision", "promotion_decision_packet"],
        "authority_effect": "shadow_only_until_eval_threshold_and_operator_gate",
        "blocked_without_evidence": True,
    },
    {
        "surface": "target_machine_recovery",
        "hermes_input": "backup_restore_or_payload_access_validation",
        "kernel_evidence": [
            "backup_manifest",
            "restore_drill_packet",
            "encrypted_descriptor_health",
            "payload_access_receipts",
            "recovery_readiness_packet",
        ],
        "authority_effect": "fail_closed_on_missing_recovery_evidence",
        "blocked_without_evidence": True,
    },
    {
        "surface": "break_glass_halt",
        "hermes_input": "halt_or_restart_request",
        "kernel_evidence": ["halt_record", "paid_routes_disabled", "autonomous_workers_disabled", "read_only_inspection_available"],
        "authority_effect": "halts_autonomy_preserves_inspection",
        "blocked_without_evidence": True,
    },
]


_AUTHORITY_BOUNDARY_CASES = [
    {
        "case_id": "paid-provider-without-budget",
        "attempted_action": "provider_plugin_paid_call",
        "missing_evidence": ["budget_grant"],
        "expected_verdict": "blocked",
        "required_kernel_guard": "durable_budget_before_paid_call",
    },
    {
        "case_id": "cached-paid-route-reuse",
        "attempted_action": "reuse_cached_paid_provider_route",
        "missing_evidence": ["fresh_budget_grant", "route_decision_revalidation"],
        "expected_verdict": "blocked",
        "required_kernel_guard": "cached_routes_are_not_authority",
    },
    {
        "case_id": "oauth-proxy-paid-call-without-grant",
        "attempted_action": "hermes_proxy_oauth_paid_provider_call",
        "missing_evidence": ["budget_grant", "data_class_grant", "provider_call_receipt"],
        "expected_verdict": "blocked",
        "required_kernel_guard": "oauth_proxy_is_not_budget_authority",
    },
    {
        "case_id": "prompt-cache-sensitive-reuse",
        "attempted_action": "reuse_cross_session_prompt_cache_with_sensitive_context",
        "missing_evidence": ["cache_scope_record", "fresh_data_class_grant"],
        "expected_verdict": "blocked",
        "required_kernel_guard": "prompt_cache_reuses_context_only_after_route_revalidation",
    },
    {
        "case_id": "x-search-without-source-policy",
        "attempted_action": "use_x_search_as_decision_evidence_without_source_policy",
        "missing_evidence": ["research_source_policy", "claim_citation"],
        "expected_verdict": "research_gate_failed",
        "required_kernel_guard": "external_search_outputs_require_evidence_contracts",
    },
    {
        "case_id": "computer-use-without-scope",
        "attempted_action": "drive_gui_with_computer_use_without_task_scope",
        "missing_evidence": ["capability_grant", "screen_scope_record", "operator_halt_check"],
        "expected_verdict": "blocked",
        "required_kernel_guard": "computer_use_requires_current_task_scope",
    },
    {
        "case_id": "plugin-tool-override-bypass",
        "attempted_action": "plugin_tool_override_calls_builtin_without_kernel_grant",
        "missing_evidence": ["plugin_allowlist", "capability_grant", "tool_override_audit"],
        "expected_verdict": "blocked",
        "required_kernel_guard": "tool_override_cannot_bypass_kernel_brokers",
    },
    {
        "case_id": "dashboard-gate-mutation",
        "attempted_action": "native_dashboard_write_to_operator_gate",
        "missing_evidence": ["operator_gate_decision", "audit_record"],
        "expected_verdict": "projection_only",
        "required_kernel_guard": "dashboard_controls_do_not_commit_kernel_state",
    },
    {
        "case_id": "customer-commitment-without-receipt",
        "attempted_action": "send_customer_visible_commitment",
        "missing_evidence": ["side_effect_receipt", "operator_gate_decision"],
        "expected_verdict": "blocked",
        "required_kernel_guard": "customer_commitments_require_receipts",
    },
    {
        "case_id": "replay-side-effect",
        "attempted_action": "replay_external_message_or_purchase",
        "missing_evidence": ["live_operator_side_effect_gate"],
        "expected_verdict": "reconstruct_only",
        "required_kernel_guard": "replay_never_executes_external_effects",
    },
    {
        "case_id": "model-promotion-without-eval",
        "attempted_action": "promote_local_model_route",
        "missing_evidence": ["eval_run", "holdout_governance", "promotion_decision_packet"],
        "expected_verdict": "shadow_only",
        "required_kernel_guard": "local_first_is_earned_by_evals",
    },
    {
        "case_id": "artifact-access-without-grant",
        "attempted_action": "read_sensitive_or_client_artifact",
        "missing_evidence": ["live_file_grant", "payload_access_receipt"],
        "expected_verdict": "denied",
        "required_kernel_guard": "artifact_access_requires_current_grants",
    },
    {
        "case_id": "autonomous-patch-application",
        "attempted_action": "apply_self_improvement_patch_from_packet",
        "missing_evidence": ["manual_patch_gate", "verification_evidence", "rollback_ref"],
        "expected_verdict": "review_only",
        "required_kernel_guard": "self_improvement_patches_are_manual_until_operator_gate",
    },
]


def generate_first_live_project_fixture(seed: int = 517) -> dict:
    """Return a narrow project fixture for first live Hermes validation."""

    f = DeterministicFactory(seed)
    project_id = f.uuid_v7()
    opportunity_id = f.uuid_v7()
    artifact_id = f.uuid_v7()
    customer_commitment_id = f.uuid_v7()

    tasks = [
        {
            "task_id": f.uuid_v7(),
            "phase": "validate",
            "title": "Confirm operator intent and acceptance criteria",
            "required_authority": "operator_gate",
            "allowed_capabilities": ["read_workspace_notes", "write_local_artifact"],
            "blocked_capabilities": ["external_message", "paid_provider_call", "customer_commitment"],
            "expected_evidence": ["validated_intent_packet", "capability_grant", "budget_record"],
        },
        {
            "task_id": f.uuid_v7(),
            "phase": "build",
            "title": "Draft local operator digest and readiness handoff pack",
            "required_authority": "kernel_assignment",
            "allowed_capabilities": ["read_kernel_packets", "write_local_artifact"],
            "blocked_capabilities": ["dashboard_write_control", "provider_plugin_call"],
            "expected_evidence": ["worker_assignment", "artifact_ref", "side_effects_absent"],
        },
        {
            "task_id": f.uuid_v7(),
            "phase": "ship",
            "title": "Prepare customer-visible delivery packet without sending it",
            "required_authority": "operator_gate",
            "allowed_capabilities": ["prepare_side_effect_intent"],
            "blocked_capabilities": ["send_message", "publish_publicly", "purchase_service"],
            "expected_evidence": ["side_effect_intent", "operator_gate_packet"],
        },
        {
            "task_id": f.uuid_v7(),
            "phase": "operate",
            "title": "Ingest operator feedback and close or continue",
            "required_authority": "operator_gate",
            "allowed_capabilities": ["record_feedback", "record_internal_value"],
            "blocked_capabilities": ["autonomous_revenue_claim", "autonomous_customer_commitment"],
            "expected_evidence": ["feedback_record", "value_receipt", "close_decision_packet"],
        },
    ]

    return {
        "fixture_id": "first-live-project-handoff-v1",
        "project": {
            "project_id": project_id,
            "opportunity_id": opportunity_id,
            "name": "Operator Digest Readiness Handoff",
            "income_mechanism": "client_work",
            "thesis": (
                "Ship a local-only handoff artifact that proves Hermes worker execution, "
                "kernel grants, artifact governance, side-effect gating, feedback ingestion, "
                "and close decision flow without live customer delivery."
            ),
            "status": "READY_FOR_TARGET_MACHINE_VALIDATION",
            "operator_load_budget_hours": 2.0,
            "cloud_spend_cap_usd": 0.0,
            "external_commitments_allowed": False,
        },
        "tasks": tasks,
        "artifact_expectations": {
            "artifact_id": artifact_id,
            "data_class": "internal_work_product",
            "storage": "local_encrypted_descriptor_required",
            "retention_policy": "retain_until_operator_close",
            "must_have_hash": True,
        },
        "side_effect_expectations": {
            "customer_commitment_id": customer_commitment_id,
            "prepared_intent_required": True,
            "receipt_required_before_commitment_active": True,
            "autonomous_delivery_allowed": False,
            "replay_reexecutes_side_effect": False,
        },
        "live_validation_surfaces": [
            "hermes_kanban_worker_assignment",
            "hermes_goal_checkpoint_resume_reconciliation",
            "hermes_handoff_reconciliation",
            "hermes_proxy_oauth_provider_boundary",
            "no_agent_cron_digest_watchdog",
            "watchers_change_detection",
            "api_approval_event_stream",
            "prompt_cache_route_revalidation",
            "x_search_research_source_policy",
            "computer_use_scope_boundary",
            "lsp_diagnostics_as_advisory_evidence",
            "plugin_llm_tool_override_boundary",
            "lm_studio_shadow_eval_route",
            "native_dashboard_read_only_projection",
            "break_glass_halt",
        ],
        "acceptance_criteria": [
            "Every task has a kernel event before derived state changes.",
            "Every worker action has an assignment and active capability grant.",
            "Cloud spend remains zero unless an operator budget gate changes it.",
            "The ship phase prepares but does not execute customer-visible delivery.",
            "Replay/projection comparison matches after feedback and close decision.",
            "Break-glass halt leaves read-only inspection available.",
        ],
    }


def generate_manual_patch_gate_rehearsal(
    *,
    patch_packet_id: str = "known-bad-follow-on-f45960c737b4cd4e3657f9e5",
    candidate_id: str = "council:known_bad_hardening",
) -> dict:
    """Return the manual-only review path for a known-bad hardening packet."""

    return {
        "name": "manual_known_bad_patch_gate_rehearsal",
        "candidate_id": candidate_id,
        "patch_packet_id": patch_packet_id,
        "authority": {
            "required_authority": "operator_gate",
            "manual_application_only": True,
            "autonomous_patch_application_enabled": False,
            "active_frontier_promotion": False,
            "route_updates_enabled": False,
            "side_effect_replay_enabled": False,
            "default_on_timeout": "keep_current_behavior",
        },
        "review_sequence": [
            "inspect_patch_packet_hash_and_changed_paths",
            "verify_evidence_refs_are_present",
            "apply_patch_in_human_reviewed_branch_only",
            "run_focused_known_bad_shadow_and_operator_review_tests",
            "run_schema_migration_verify",
            "run_full_pytest_suite",
            "record_rollback_ref_before_activation",
            "open_normal_code_review_pr",
        ],
        "rollback_requirements": [
            "preserve_current_active_harness_frontier",
            "record_pre_patch_route_state",
            "record_patch_packet_hash",
            "block_activation_if_verification_or_review_is_missing",
        ],
        "blocked_autonomous_actions": [
            "active_behavior_mutation",
            "autonomous_patch_application",
            "autonomous_harness_promotion",
            "frontier_route_update",
            "external_side_effect_reexecution",
        ],
    }


def generate_hermes_adapter_validation_harness(seed: int = 517) -> dict:
    """Return the day-one Hermes validation checklist as executable fixture data."""

    f = DeterministicFactory(seed)
    checks = []
    for index, check in enumerate(_LIVE_VALIDATION_CHECKS, start=1):
        checks.append(
            {
                "check_id": f"live-hermes-{index:02d}-{f.uuid_v7()}",
                **check,
                "durable_evidence_required": True,
                "live_controls_enabled_after_pass": False,
                "replay_executes_external_effects": False,
            }
        )
    return {
        "name": "hermes_adapter_validation_harness",
        "version": 1,
        "hermes_version_floor": "0.14.0",
        "target_environment": "physical_mac_studio_or_m5",
        "checks": checks,
        "pass_rule": "all_checks_have_kernel_owned_evidence_and_no_blockers",
        "failure_rule": "failed_missing_stale_or_ambiguous_evidence_keeps_surface_blocked",
    }


def generate_first_live_project_dry_run(seed: int = 517) -> dict:
    """Return a deterministic end-to-end rehearsal for the first live project."""

    fixture = generate_first_live_project_fixture(seed=seed)
    tasks = fixture["tasks"]
    phases = []
    for task in tasks:
        phases.append(
            {
                "phase": task["phase"],
                "task_id": task["task_id"],
                "event_before_projection": True,
                "assignment_required": task["phase"] in {"build", "ship", "operate"},
                "capability_grants_required": task["allowed_capabilities"],
                "blocked_capabilities": task["blocked_capabilities"],
                "expected_evidence": task["expected_evidence"],
                "external_side_effects_executed": False,
            }
        )
    return {
        "name": "first_live_project_end_to_end_dry_run",
        "fixture_id": fixture["fixture_id"],
        "project_id": fixture["project"]["project_id"],
        "phases": phases,
        "close_path": {
            "feedback_ingested": True,
            "internal_value_receipt_required": True,
            "close_or_continue_requires_operator_gate": True,
            "replay_projection_comparison_required": True,
        },
        "acceptance": {
            "local_artifact_only": True,
            "operator_gate_before_external_delivery": True,
            "cloud_spend_cap_usd": fixture["project"]["cloud_spend_cap_usd"],
            "autonomous_customer_commitments_allowed": False,
        },
    }


def generate_authority_boundary_gauntlet(seed: int = 517) -> dict:
    """Return adversarial cases proving Hermes-native surfaces stay non-authoritative."""

    f = DeterministicFactory(seed)
    cases = []
    for case in _AUTHORITY_BOUNDARY_CASES:
        cases.append(
            {
                "boundary_case_id": f"{case['case_id']}-{f.uuid_v7()}",
                **case,
                "kernel_event_required_before_state_change": True,
                "durable_failure_record_required": case["expected_verdict"] in {"blocked", "denied"},
                "live_controls_enabled": False,
            }
        )
    return {
        "name": "pre_live_authority_boundary_gauntlet",
        "version": 1,
        "cases": cases,
        "pass_rule": "every_adversarial_case_fails_closed_or_remains_projection_only",
        "activation_effect": "none",
    }


def generate_resume_side_effect_replay_fixture(seed: int = 517) -> dict:
    """Return deterministic R-16 resume and side-effect replay proof cases."""

    f = DeterministicFactory(seed)
    surfaces = [
        ("goal_resume", "task_status_revalidated"),
        ("checkpoint_resume", "assignment_owner_revalidated"),
        ("gateway_resume", "capability_grants_revalidated"),
        ("acp_steer", "budget_grants_revalidated"),
        ("acp_queue_continue", "side_effect_idempotency_revalidated"),
        ("no_agent_cron", "halt_state_revalidated"),
        ("provider_plugin_resume", "provider_route_policy_revalidated"),
        ("mcp_oauth_forward_resume", "scope_timeout_and_media_policy_revalidated"),
    ]
    cases = []
    for surface, decisive_check in surfaces:
        cases.append(
            {
                "case_id": f"resume-r16-{surface}-{f.uuid_v7()}",
                "surface": surface,
                "required_reconciliation": [
                    "task_status_revalidated",
                    "assignment_owner_revalidated",
                    "capability_grants_revalidated",
                    "budget_grants_revalidated",
                    "side_effect_idempotency_revalidated",
                    "policy_version_revalidated",
                    "halt_state_revalidated",
                ],
                "decisive_check": decisive_check,
                "stale_or_failed_check_result": "blocked_before_worker_continuation",
                "side_effect_replay_result": "reconstruct_intent_and_receipt_only",
                "external_side_effects_reexecuted": False,
                "inspection_task_required_on_failure": True,
                "live_controls_enabled": False,
            }
        )
    return {
        "name": "r16_resume_side_effect_replay_fixture",
        "version": 1,
        "cases": cases,
        "pass_rule": "every_resume_surface_revalidates_kernel_authority_and_replay_never_reexecutes_external_effects",
        "activation_effect": "none",
    }


def generate_model_efficiency_service_packet(seed: int = 517) -> dict:
    """Return the local-only commercial packet for the governed model-efficiency offer."""

    f = DeterministicFactory(seed)
    return {
        "packet_id": f"model-efficiency-service-{f.uuid_v7()}",
        "name": "governed_model_efficiency_service_packet",
        "version": 1,
        "offer": {
            "buyer_profiles": [
                "ai_native_saas_with_frontier_model_spend",
                "ai_heavy_agency_with_repeatable_client_workflows",
                "privacy_sensitive_internal_platform_team",
            ],
            "paid_audit_offer": "local-first model-efficiency audit with quality, privacy, spend, and operator-control report",
            "customer_visible_delivery": "operator_gate_required",
            "external_side_effects_allowed": False,
        },
        "seed_task_classes": [
            {
                "task_class": "quick_research_summarization",
                "frontier_baseline_metric": "quality_adjusted_cost_per_accepted_summary",
                "local_candidate_metric": "shadow_quality_delta_and_latency",
            },
            {
                "task_class": "source_claim_extraction",
                "frontier_baseline_metric": "citation_precision_per_dollar",
                "local_candidate_metric": "claim_extraction_f1_against_holdout",
            },
            {
                "task_class": "coding_small_patch",
                "frontier_baseline_metric": "verified_patch_cost_per_test_pass",
                "local_candidate_metric": "focused_test_pass_rate_and_review_defect_rate",
            },
        ],
        "eval_inputs": [
            "customer_workflow_trace_sample_with_sensitive_payloads_replaced_by_artifact_refs",
            "frontier_baseline_receipts_and_quality_scores",
            "local_shadow_outputs",
            "operator_review_labels",
            "holdout_cases_for_regression_and_data_leakage",
        ],
        "savings_report": {
            "required_sections": [
                "current_frontier_spend",
                "candidate_local_or_cheaper_routes",
                "quality_delta_by_task_class",
                "privacy_and_data_residency_constraints",
                "operator_load_delta",
                "recommended_shadow_to_promotion_gates",
            ],
            "promotion_authority": "operator_gate",
            "route_mutation_enabled": False,
        },
        "kill_criteria": [
            "no_task_class_reaches_quality_threshold_in_shadow",
            "operator_load_increases_materially",
            "savings_under_20_percent_after_quality_adjustment",
            "privacy_or_audit_requirements_cannot_be_met_locally",
            "customer_cannot_supply_representative_eval_inputs",
        ],
        "kernel_boundaries": {
            "research_engine_required": True,
            "model_intelligence_supplies_evidence_only": True,
            "operator_gate_before_customer_delivery": True,
            "paid_provider_calls_require_budget_grants": True,
            "live_controls_enabled": False,
        },
    }


def generate_first_live_project_test_set(seed: int = 517) -> dict:
    fixture = generate_first_live_project_fixture(seed=seed)
    return {
        "name": "first_live_project_handoff",
        "version": 1,
        "fixture": fixture,
        "manual_patch_gate_rehearsal": generate_manual_patch_gate_rehearsal(),
        "hermes_adapter_validation_harness": generate_hermes_adapter_validation_harness(seed=seed),
        "dry_run": generate_first_live_project_dry_run(seed=seed),
        "authority_boundary_gauntlet": generate_authority_boundary_gauntlet(seed=seed),
        "resume_side_effect_replay_fixture": generate_resume_side_effect_replay_fixture(seed=seed),
        "model_efficiency_service_packet": generate_model_efficiency_service_packet(seed=seed),
    }
