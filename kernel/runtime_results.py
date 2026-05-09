from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from skills.config import IntegrationConfig
from skills.hermes_interfaces import HermesSessionContext


@dataclass(frozen=True)
class RuntimeBootstrapResult:
    """Structured result for a Hermes integration bootstrap attempt."""

    ok: bool
    config: IntegrationConfig
    session_context: HermesSessionContext
    database_status: dict[str, bool]
    registered_tools: list[str]


@dataclass(frozen=True)
class RuntimeProfileInstallResult:
    """Filesystem bundle describing how Hermes should bootstrap this runtime."""

    config: IntegrationConfig
    repo_root: str
    profile_dir: str
    profile_config_path: str
    spec_profile_path: str
    profile_manifest_path: str
    launcher_paths: dict[str, str]
    linked_skill_paths: list[str]


@dataclass(frozen=True)
class HermesProfileValidationResult:
    """Structured validation result for the repo-owned Hermes profile artifacts."""

    ok: bool
    profile_dir: str
    profile_config_path: str
    spec_profile_path: str
    checks: dict[str, bool]
    issues: list[str]


@dataclass(frozen=True)
class RuntimeDoctorResult:
    """Health report for the prepared Hermes runtime layout."""

    ok: bool
    config: IntegrationConfig
    path_status: dict[str, bool]
    database_status: dict[str, bool]
    database_errors: dict[str, list[str]]
    registered_tools: list[str]
    missing_items: list[str]
    profile_manifest_path: str
    profile_validation: HermesProfileValidationResult


@dataclass(frozen=True)
class ExternalCommandResult:
    """Captured result from a Hermes CLI probe."""

    ok: bool
    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    error: str | None = None


@dataclass(frozen=True)
class HermesReadinessResult:
    """Readiness report for attaching the repo runtime to a real Hermes install."""

    ok: bool
    config: IntegrationConfig
    hermes_installed: bool
    hermes_version: str | None
    hermes_version_ok: bool
    profile_listed: bool
    live_tools: list[str]
    seed_tool_status: dict[str, bool]
    config_status: dict[str, bool]
    profile_validation: HermesProfileValidationResult
    path_status: dict[str, bool]
    database_status: dict[str, bool]
    legacy_database_files: list[str]
    cli_smoke_attempted: bool
    cli_smoke_ok: bool
    cli_smoke_marker: str | None
    cli_smoke_step_outcomes_delta: int
    cli_smoke_log_trace: bool
    cli_smoke_output: str | None
    one_shot_smoke_attempted: bool
    one_shot_smoke_ok: bool
    one_shot_smoke_output: str | None
    deferred_items: list[str]
    checkpoint_backup_path: str | None
    blocking_items: list[str]
    drift_items: list[str]
    install: RuntimeProfileInstallResult
    doctor: RuntimeDoctorResult
    contract_harness: HermesContractHarnessResult
    replay_report: dict[str, Any]
    council_isolation_canary: dict[str, Any]
    recommended_actions: list[str]


@dataclass(frozen=True)
class WorkflowObservabilitySnapshot:
    """Queryable runtime evidence produced by the operator workflow proof."""

    alert_history: list[dict[str, Any]]
    council_verdicts: list[dict[str, Any]]
    digest_history: list[dict[str, Any]]
    immune_verdicts: list[dict[str, Any]]
    telemetry_events: list[dict[str, Any]]
    reliability_dashboard: dict[str, Any]
    system_health: dict[str, Any]


@dataclass(frozen=True)
class OperatorWorkflowResult:
    """End-to-end operator workflow and council-backed project proof result."""

    ok: bool
    bootstrap: RuntimeBootstrapResult
    sheriff_outcome: str
    routing_tier: str | None
    brief_id: str | None
    readback: dict[str, Any] | None
    opportunity_id: str | None
    harvest_id: str | None
    project_id: str | None
    phase_gate_id: str | None
    phase_gate_verdict: str | None
    council_verdict_ids: list[str]
    alert_id: str | None
    digest_id: str | None
    digest: dict[str, Any] | None
    observability: WorkflowObservabilitySnapshot | None
    doctor: RuntimeDoctorResult
    trace_id: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class HermesContractHarnessResult:
    """Repo-local Hermes-parity lifecycle proof without requiring a live Hermes install."""

    ok: bool
    config: IntegrationConfig
    bootstrap: RuntimeBootstrapResult
    doctor: RuntimeDoctorResult
    contract_checks: dict[str, bool]
    route_decision: dict[str, Any] | None
    approval_request: dict[str, Any] | None
    approval_review: dict[str, Any] | None
    dispatch_result: dict[str, Any] | None
    judge_deadlock_event: dict[str, Any] | None
    runtime_halt: dict[str, Any] | None
    blocked_dispatch_pre_side_effect: bool
    blocked_dispatch_reason: str | None
    restart_result: dict[str, Any] | None
    final_runtime_status: dict[str, Any] | None
    v012_contract_checks: dict[str, bool]
    trace_id: str | None
    issues: list[str]


@dataclass(frozen=True)
class TaskLoopProofResult:
    ok: bool
    config: IntegrationConfig
    bootstrap: RuntimeBootstrapResult
    doctor: RuntimeDoctorResult
    task_id: str | None
    brief_id: str | None
    route_summary: dict[str, Any] | None
    trace_id: str | None
    issues: list[str]


@dataclass(frozen=True)
class ResearchCronProofResult:
    ok: bool
    config: IntegrationConfig
    bootstrap: RuntimeBootstrapResult
    doctor: RuntimeDoctorResult
    standing_brief_id: str | None
    scheduled_job_id: str | None
    queued_task_id: str | None
    trace_id: str | None
    issues: list[str]


@dataclass(frozen=True)
class ProxySelfTestResult:
    ok: bool
    config: IntegrationConfig
    proxy_url: str | None
    allowed_request_count: int
    blocked_request_count: int
    audit_log_path: str
    trace_id: str | None
    issues: list[str]


@dataclass(frozen=True)
class BootstrapStackResult:
    ok: bool
    install: RuntimeProfileInstallResult
    doctor: RuntimeDoctorResult
    operator_workflow: OperatorWorkflowResult
    contract_harness: HermesContractHarnessResult
    task_loop_proof: TaskLoopProofResult
    research_cron_proof: ResearchCronProofResult
    proxy_self_test: ProxySelfTestResult
    milestone_status: dict[str, Any]


@dataclass(frozen=True)
class EvidenceScenarioResult:
    scenario_id: str
    cycle_index: int
    classification: str
    ok: bool
    trace_id: str | None
    produced_skill_families: list[str]
    issues: list[str]
    details: dict[str, Any]


@dataclass(frozen=True)
class EvidenceBatchResult:
    ok: bool
    config: IntegrationConfig
    bootstrap: RuntimeBootstrapResult
    doctor: RuntimeDoctorResult
    requested_cycles: int
    cycles: int
    until_replay_ready: bool
    stopped_reason: str
    scenario_results: list[EvidenceScenarioResult]
    generated_trace_count: int
    generated_source_trace_count: int
    generated_activation_trace_count: int
    generated_known_bad_trace_count: int
    before_replay_report: dict[str, Any]
    replay_report: dict[str, Any]
    progress_projection: dict[str, Any]
    report_path: str


@dataclass(frozen=True)
class FlywheelDrillResult:
    ok: bool
    config: IntegrationConfig
    bootstrap: RuntimeBootstrapResult
    doctor: RuntimeDoctorResult
    workflow: OperatorWorkflowResult
    before_replay_report: dict[str, Any]
    replay_report: dict[str, Any]
    generated_trace_count: int
    generated_activation_trace_count: int
    generated_known_bad_trace_count: int
    trace_id: str | None
    artifact_path: str
    issues: list[str]


@dataclass(frozen=True)
class MacStudioDayOneResult:
    ok: bool
    install: RuntimeProfileInstallResult
    doctor: RuntimeDoctorResult
    bootstrap_stack: BootstrapStackResult
    evidence_batch: EvidenceBatchResult
    replay_report: dict[str, Any]
    handoff_path: str
    issues: list[str]
