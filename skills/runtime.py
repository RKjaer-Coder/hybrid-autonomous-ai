from __future__ import annotations

import argparse
import json
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from financial_router.types import BudgetState, JWTClaims, ModelInfo, TaskMetadata
from immune.types import JudgePayload, Outcome, SheriffPayload, generate_uuid_v7
from migrate import SCHEMAS, apply_schema, verify_database
from skills.bootstrap import BootstrapOrchestrator
from skills.config import IntegrationConfig
from skills.db_manager import CANONICAL_DATABASES, DatabaseManager
from skills.hermes_interfaces import HermesSessionContext, HermesToolRegistry, MockHermesRuntime

EXPECTED_CORE_TOOLS = (
    "immune_system",
    "financial_router",
    "strategic_memory",
    "operator_interface",
    "observability",
)


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
    profile_manifest_path: str
    launcher_paths: dict[str, str]
    linked_skill_paths: list[str]


@dataclass(frozen=True)
class RuntimeDoctorResult:
    """Health report for the prepared Hermes runtime layout."""

    ok: bool
    config: IntegrationConfig
    path_status: dict[str, bool]
    database_status: dict[str, bool]
    registered_tools: list[str]
    missing_items: list[str]
    profile_manifest_path: str


@dataclass(frozen=True)
class WorkflowObservabilitySnapshot:
    """Queryable runtime evidence produced by the operator workflow proof."""

    alert_history: list[dict[str, Any]]
    digest_history: list[dict[str, Any]]
    immune_verdicts: list[dict[str, Any]]
    telemetry_events: list[dict[str, Any]]
    reliability_dashboard: dict[str, Any]
    system_health: dict[str, Any]


@dataclass(frozen=True)
class OperatorWorkflowResult:
    """End-to-end Stage 0/1 operator workflow smoke test result."""

    ok: bool
    bootstrap: RuntimeBootstrapResult
    sheriff_outcome: str
    routing_tier: str | None
    brief_id: str | None
    readback: dict[str, Any] | None
    alert_id: str | None
    digest_id: str | None
    digest: dict[str, Any] | None
    observability: WorkflowObservabilitySnapshot | None
    doctor: RuntimeDoctorResult
    error: str | None = None


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _ensure_operator_workflow_chain_definition(config: IntegrationConfig, chain_type: str = "operator_workflow") -> None:
    db_manager = DatabaseManager(config.data_dir)
    try:
        conn = db_manager.get_connection("telemetry")
        now = _utc_now()
        steps = json.dumps(
            [
                {"step_type": "heartbeat", "skill": "operator_interface"},
                {"step_type": "sheriff", "skill": "immune_system"},
                {"step_type": "route", "skill": "financial_router"},
                {"step_type": "write_brief", "skill": "strategic_memory"},
                {"step_type": "read_brief", "skill": "strategic_memory"},
                {"step_type": "judge", "skill": "immune_system"},
                {"step_type": "alert", "skill": "operator_interface"},
                {"step_type": "digest", "skill": "operator_interface"},
            ]
        )
        conn.execute(
            """
            INSERT INTO chain_definitions (chain_type, steps, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chain_type) DO UPDATE SET
                steps=excluded.steps,
                updated_at=excluded.updated_at
            """,
            (chain_type, steps, now, now),
        )
        conn.commit()
    finally:
        db_manager.close_all()


def _persist_step_outcome(
    config: IntegrationConfig,
    *,
    chain_id: str,
    step_type: str,
    skill: str,
    outcome: str,
    latency_ms: float,
    quality_warning: bool = False,
    recovery_tier: int | None = None,
) -> None:
    db_manager = DatabaseManager(config.data_dir)
    try:
        conn = db_manager.get_connection("telemetry")
        conn.execute(
            """
            INSERT OR IGNORE INTO step_outcomes (
                event_id, step_type, skill, chain_id, outcome, latency_ms,
                quality_warning, recovery_tier, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                generate_uuid_v7(),
                step_type,
                skill,
                chain_id,
                outcome,
                int(latency_ms),
                1 if quality_warning else 0,
                recovery_tier,
                _utc_now(),
            ),
        )
        conn.commit()
    finally:
        db_manager.close_all()


def _persist_immune_verdict(config: IntegrationConfig, verdict: Any, latency_ms: float) -> None:
    db_manager = DatabaseManager(config.data_dir)
    try:
        conn = db_manager.get_connection("immune")
        conn.execute(
            """
            INSERT OR IGNORE INTO immune_verdicts (
                verdict_id, verdict_type, scan_tier, session_id, skill_name,
                result, match_pattern, latency_ms, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict.verdict_id,
                "sheriff_input" if verdict.check_type.value == "sheriff" else "judge_output",
                verdict.tier.value,
                verdict.session_id,
                verdict.skill_name,
                verdict.outcome.value,
                verdict.block_reason.value if verdict.block_reason else None,
                int(latency_ms),
                _utc_now(),
            ),
        )
        conn.commit()
    finally:
        db_manager.close_all()


def _record_tool_step(
    config: IntegrationConfig,
    *,
    chain_id: str,
    step_type: str,
    skill: str,
    result: Any,
    quality_warning: bool = False,
) -> None:
    _persist_step_outcome(
        config,
        chain_id=chain_id,
        step_type=step_type,
        skill=skill,
        outcome="PASS" if result.success else "FAIL",
        latency_ms=result.duration_ms,
        quality_warning=quality_warning,
    )


def _normalize_runtime_layout(config: IntegrationConfig) -> IntegrationConfig:
    defaults = IntegrationConfig()
    if config.data_dir == defaults.data_dir:
        return config

    base_dir = Path(config.data_dir).expanduser().resolve().parent
    skills_dir = config.skills_dir
    checkpoints_dir = config.checkpoints_dir
    alerts_dir = config.alerts_dir

    if skills_dir == defaults.skills_dir:
        skills_dir = str(base_dir / "skills" / "hybrid-autonomous-ai")
    if checkpoints_dir == defaults.checkpoints_dir:
        checkpoints_dir = str(Path(skills_dir) / "checkpoints")
    if alerts_dir == defaults.alerts_dir:
        alerts_dir = str(base_dir / "alerts")

    return IntegrationConfig(
        data_dir=config.data_dir,
        skills_dir=skills_dir,
        checkpoints_dir=checkpoints_dir,
        alerts_dir=alerts_dir,
        max_api_spend_usd=config.max_api_spend_usd,
        construction_phase=config.construction_phase,
        profile_name=config.profile_name,
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _runtime_bundle_dir(config: IntegrationConfig) -> Path:
    return Path(config.skills_dir) / "runtime"


def _runtime_profile_manifest_path(config: IntegrationConfig) -> Path:
    return _runtime_bundle_dir(config) / "profile_manifest.json"


def _runtime_launcher_paths(config: IntegrationConfig) -> dict[str, Path]:
    bin_dir = _runtime_bundle_dir(config) / "bin"
    return {
        "bootstrap": bin_dir / "bootstrap_runtime.sh",
        "doctor": bin_dir / "doctor_runtime.sh",
        "operator_workflow": bin_dir / "run_operator_workflow.sh",
    }


def _linked_skills_dir(config: IntegrationConfig) -> Path:
    return _runtime_bundle_dir(config) / "linked_skills"


def _command_args(config: IntegrationConfig) -> list[str]:
    return [
        "--data-dir",
        config.data_dir,
        "--skills-dir",
        config.skills_dir,
        "--checkpoints-dir",
        config.checkpoints_dir,
        "--alerts-dir",
        config.alerts_dir,
        "--profile-name",
        config.profile_name,
    ]


def _command_string(config: IntegrationConfig, action_flag: str, repo_root: Path) -> str:
    parts = [
        f"PYTHONPATH={shlex.quote(str(repo_root))}${{PYTHONPATH:+:${{PYTHONPATH}}}}",
        "python3",
        "-m",
        "skills.runtime",
        action_flag,
        *_command_args(config),
    ]
    return " ".join(shlex.quote(part) for part in parts)


def _write_launcher(path: Path, config: IntegrationConfig, repo_root: Path, action_flag: str) -> None:
    args = " ".join(shlex.quote(part) for part in [action_flag, *_command_args(config)])
    path.write_text(
        "\n".join(
            [
                "#!/bin/sh",
                "set -eu",
                f"REPO_ROOT={shlex.quote(str(repo_root))}",
                'if [ -n "${PYTHONPATH:-}" ]; then',
                '  export PYTHONPATH="$REPO_ROOT:$PYTHONPATH"',
                "else",
                '  export PYTHONPATH="$REPO_ROOT"',
                "fi",
                f"exec python3 -m skills.runtime {args} \"$@\"",
                "",
            ]
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _symlink_skill_directory(source: Path, dest: Path) -> None:
    if dest.is_symlink():
        if dest.resolve() == source.resolve():
            return
        dest.unlink()
    elif dest.exists():
        raise FileExistsError(f"Refusing to replace non-symlink path: {dest}")
    dest.symlink_to(source, target_is_directory=True)


def prepare_runtime_directories(config: IntegrationConfig) -> IntegrationConfig:
    """Resolve and create the filesystem layout expected by the integration layer."""
    resolved = _normalize_runtime_layout(config).resolve_paths()
    for raw_path in (
        resolved.data_dir,
        resolved.skills_dir,
        resolved.checkpoints_dir,
        resolved.alerts_dir,
    ):
        Path(raw_path).mkdir(parents=True, exist_ok=True)
    return resolved


def install_runtime_profile(config: IntegrationConfig, *, repo_root: str | None = None) -> RuntimeProfileInstallResult:
    """Install a filesystem bundle that a Hermes profile can bootstrap from."""
    resolved = prepare_runtime_directories(config)
    root = Path(repo_root).expanduser().resolve() if repo_root else _repo_root()
    runtime_dir = _runtime_bundle_dir(resolved)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    linked_skills_dir = _linked_skills_dir(resolved)
    linked_skills_dir.mkdir(parents=True, exist_ok=True)
    launcher_paths = _runtime_launcher_paths(resolved)
    for launcher in launcher_paths.values():
        launcher.parent.mkdir(parents=True, exist_ok=True)

    linked_skill_paths: list[str] = []
    for manifest_path in sorted(root.glob("skills/*/manifest.yaml")):
        skill_dir = manifest_path.parent.resolve()
        link_path = linked_skills_dir / skill_dir.name
        _symlink_skill_directory(skill_dir, link_path)
        linked_skill_paths.append(str(link_path))

    manifest = {
        "profile_name": resolved.profile_name,
        "repo_root": str(root),
        "data_dir": resolved.data_dir,
        "skills_dir": resolved.skills_dir,
        "checkpoints_dir": resolved.checkpoints_dir,
        "alerts_dir": resolved.alerts_dir,
        "linked_skills": linked_skill_paths,
        "commands": {
            "bootstrap": _command_string(resolved, "--bootstrap-live", root),
            "doctor": _command_string(resolved, "--doctor", root),
            "operator_workflow": _command_string(resolved, "--operator-workflow", root),
        },
    }
    manifest_path = _runtime_profile_manifest_path(resolved)
    manifest_path.write_text(f"{json.dumps(manifest, indent=2, sort_keys=True)}\n", encoding="utf-8")

    _write_launcher(launcher_paths["bootstrap"], resolved, root, "--bootstrap-live")
    _write_launcher(launcher_paths["doctor"], resolved, root, "--doctor")
    _write_launcher(launcher_paths["operator_workflow"], resolved, root, "--operator-workflow")

    return RuntimeProfileInstallResult(
        config=resolved,
        repo_root=str(root),
        profile_manifest_path=str(manifest_path),
        launcher_paths={name: str(path) for name, path in launcher_paths.items()},
        linked_skill_paths=linked_skill_paths,
    )


def migrate_runtime_databases(config: IntegrationConfig) -> dict[str, bool]:
    """Apply all schema files into the configured data directory and verify them."""
    resolved = prepare_runtime_directories(config)
    root = Path(__file__).resolve().parents[1]
    data_dir = Path(resolved.data_dir)
    status: dict[str, bool] = {}
    for db_name, schema_rel in SCHEMAS.items():
        db_path = data_dir / f"{db_name}.db"
        schema_path = root / schema_rel
        apply_schema(db_path, schema_path)
        ok, _errors = verify_database(db_path, db_name, schema_path)
        status[db_name] = ok
    return status


def make_session_context(
    config: IntegrationConfig,
    *,
    model_name: str = "local-default",
    session_id: str | None = None,
    jwt_claims: dict[str, Any] | None = None,
) -> HermesSessionContext:
    resolved = config.resolve_paths()
    return HermesSessionContext(
        session_id=session_id or generate_uuid_v7(),
        profile_name=resolved.profile_name,
        model_name=model_name,
        jwt_claims=jwt_claims or {},
        data_dir=resolved.data_dir,
    )


def bootstrap_runtime(
    tool_registry: HermesToolRegistry,
    *,
    config: IntegrationConfig | None = None,
    session_context: HermesSessionContext | None = None,
    model_name: str = "local-default",
    jwt_claims: dict[str, Any] | None = None,
) -> RuntimeBootstrapResult:
    """Prepare runtime state, migrate databases, and register integration skills."""
    resolved = prepare_runtime_directories(config or IntegrationConfig())
    db_status = migrate_runtime_databases(resolved)
    ctx = session_context or make_session_context(resolved, model_name=model_name, jwt_claims=jwt_claims)
    orchestrator = BootstrapOrchestrator(resolved, tool_registry, ctx)
    ok = orchestrator.run()
    return RuntimeBootstrapResult(
        ok=ok,
        config=resolved,
        session_context=ctx,
        database_status=db_status,
        registered_tools=tool_registry.list_tools(),
    )


def doctor_runtime(
    tool_registry: HermesToolRegistry | None = None,
    *,
    config: IntegrationConfig | None = None,
    bootstrap_if_needed: bool = True,
) -> RuntimeDoctorResult:
    """Verify the prepared runtime layout, database health, and core skill registration."""
    resolved = _normalize_runtime_layout(config or IntegrationConfig()).resolve_paths()
    launcher_paths = _runtime_launcher_paths(resolved)
    path_status = {
        "data_dir": Path(resolved.data_dir).is_dir(),
        "skills_dir": Path(resolved.skills_dir).is_dir(),
        "checkpoints_dir": Path(resolved.checkpoints_dir).is_dir(),
        "alerts_dir": Path(resolved.alerts_dir).is_dir(),
        "profile_manifest": _runtime_profile_manifest_path(resolved).is_file(),
        "bootstrap_launcher": launcher_paths["bootstrap"].is_file(),
        "doctor_launcher": launcher_paths["doctor"].is_file(),
        "operator_workflow_launcher": launcher_paths["operator_workflow"].is_file(),
    }

    database_status = {db_name: False for db_name in CANONICAL_DATABASES}
    if path_status["data_dir"]:
        db_manager = DatabaseManager(resolved.data_dir)
        try:
            database_status.update(db_manager.verify_all_databases())
        finally:
            db_manager.close_all()

    registered_tools: list[str] = []
    if tool_registry is not None:
        if bootstrap_if_needed and not tool_registry.list_tools():
            bootstrap_runtime(tool_registry, config=resolved)
        registered_tools = tool_registry.list_tools()

    missing_items = [name for name, ok in path_status.items() if not ok]
    missing_items.extend(f"db:{db_name}" for db_name, ok in database_status.items() if not ok)
    if tool_registry is not None:
        missing_items.extend(f"tool:{tool_name}" for tool_name in EXPECTED_CORE_TOOLS if tool_name not in registered_tools)

    return RuntimeDoctorResult(
        ok=not missing_items,
        config=resolved,
        path_status=path_status,
        database_status=database_status,
        registered_tools=registered_tools,
        missing_items=missing_items,
        profile_manifest_path=str(_runtime_profile_manifest_path(resolved)),
    )


def run_operator_workflow(
    tool_registry: HermesToolRegistry,
    *,
    config: IntegrationConfig | None = None,
    session_context: HermesSessionContext | None = None,
    model_name: str = "local-default",
    task_id: str = "stage0-operator-workflow",
    title: str = "Stage 0/1 operator workflow smoke test",
    summary: str = "Validated bootstrap, routing, memory, and operator alert smoke test.",
) -> OperatorWorkflowResult:
    """Run one narrow operator path end-to-end against the configured runtime."""
    resolved_config = config or IntegrationConfig()
    install_runtime_profile(resolved_config)
    bootstrap = bootstrap_runtime(
        tool_registry,
        config=resolved_config,
        session_context=session_context,
        model_name=model_name,
    )
    resolved = bootstrap.config
    session_id = bootstrap.session_context.session_id
    chain_id = task_id
    _ensure_operator_workflow_chain_definition(resolved)

    heartbeat = tool_registry.invoke_tool(
        "operator_interface",
        {
            "action": "record_heartbeat",
            "interaction_type": "command",
            "channel": "CLI",
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="heartbeat",
        skill="operator_interface",
        result=heartbeat,
    )
    if not heartbeat.success:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome="error",
            routing_tier=None,
            brief_id=None,
            readback=None,
            alert_id=None,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error=heartbeat.error,
        )

    sheriff = tool_registry.invoke_tool(
        "immune_system",
        {
            "action": "sheriff",
            "payload": SheriffPayload(
                session_id=session_id,
                skill_name="operator_workflow",
                tool_name="strategic_memory",
                arguments={"action": "write_brief", "task_id": task_id, "title": title},
                raw_prompt=summary,
                source_trust_tier=4,
                jwt_claims=bootstrap.session_context.jwt_claims,
            ),
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="sheriff",
        skill="immune_system",
        result=sheriff,
    )
    if not sheriff.success:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome="error",
            routing_tier=None,
            brief_id=None,
            readback=None,
            alert_id=None,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error=sheriff.error,
        )
    sheriff_verdict = sheriff.output
    _persist_immune_verdict(resolved, sheriff_verdict, sheriff.duration_ms)
    if sheriff_verdict.outcome == Outcome.BLOCK:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_verdict.outcome.value,
            routing_tier=None,
            brief_id=None,
            readback=None,
            alert_id=None,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error="workflow blocked by sheriff",
        )

    route = tool_registry.invoke_tool(
        "financial_router",
        {
            "action": "route",
            "task": TaskMetadata(
                task_id=task_id,
                task_type="operator_workflow",
                required_capability="strategic_memory",
                quality_threshold=0.4,
            ),
            "models": [ModelInfo("m-local-primary", "local", True, 0.95, 0.0)],
            "budget": BudgetState(),
            "jwt": JWTClaims(session_id=session_id),
        },
    )
    route_quality_warning = bool(route.success and getattr(route.output, "quality_warning", False))
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="route",
        skill="financial_router",
        result=route,
        quality_warning=route_quality_warning,
    )
    if not route.success:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_verdict.outcome.value,
            routing_tier=None,
            brief_id=None,
            readback=None,
            alert_id=None,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error=route.error,
        )
    routing_decision = route.output

    write_result = tool_registry.invoke_tool(
        "strategic_memory",
        {
            "action": "write_brief",
            "task_id": task_id,
            "title": title,
            "summary": summary,
            "confidence": 0.8,
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="write_brief",
        skill="strategic_memory",
        result=write_result,
    )
    if not write_result.success:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_verdict.outcome.value,
            routing_tier=routing_decision.tier.value,
            brief_id=None,
            readback=None,
            alert_id=None,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error=write_result.error,
        )
    brief_id = write_result.output

    read_result = tool_registry.invoke_tool(
        "strategic_memory",
        {
            "action": "read_brief",
            "brief_id": brief_id,
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="read_brief",
        skill="strategic_memory",
        result=read_result,
    )
    if not read_result.success:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_verdict.outcome.value,
            routing_tier=routing_decision.tier.value,
            brief_id=brief_id,
            readback=None,
            alert_id=None,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error=read_result.error,
        )

    judge = tool_registry.invoke_tool(
        "immune_system",
        {
            "action": "judge",
            "payload": JudgePayload(
                session_id=session_id,
                skill_name="operator_workflow",
                tool_name="strategic_memory",
                output=read_result.output,
            ),
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="judge",
        skill="immune_system",
        result=judge,
    )
    if not judge.success:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_verdict.outcome.value,
            routing_tier=routing_decision.tier.value,
            brief_id=brief_id,
            readback=read_result.output,
            alert_id=None,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error=judge.error,
        )
    judge_verdict = judge.output
    _persist_immune_verdict(resolved, judge_verdict, judge.duration_ms)
    if judge_verdict.outcome == Outcome.BLOCK:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_verdict.outcome.value,
            routing_tier=routing_decision.tier.value,
            brief_id=brief_id,
            readback=read_result.output,
            alert_id=None,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error="workflow blocked by judge",
        )

    alert_result = tool_registry.invoke_tool(
        "operator_interface",
        {
            "action": "alert",
            "tier": "T1",
            "alert_type": "WORKFLOW_SMOKE_TEST",
            "content": f"Workflow {task_id} stored brief {brief_id} via {routing_decision.tier.value}.",
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="alert",
        skill="operator_interface",
        result=alert_result,
    )
    if not alert_result.success:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_verdict.outcome.value,
            routing_tier=routing_decision.tier.value,
            brief_id=brief_id,
            readback=read_result.output,
            alert_id=None,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error=alert_result.error,
        )

    digest_result = tool_registry.invoke_tool(
        "operator_interface",
        {
            "action": "generate_digest",
            "digest_type": "daily",
            "operator_state": "ACTIVE",
        },
    )
    _record_tool_step(
        resolved,
        chain_id=chain_id,
        step_type="digest",
        skill="operator_interface",
        result=digest_result,
    )
    if not digest_result.success:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_verdict.outcome.value,
            routing_tier=routing_decision.tier.value,
            brief_id=brief_id,
            readback=read_result.output,
            alert_id=alert_result.output,
            digest_id=None,
            digest=None,
            observability=None,
            doctor=doctor,
            error=digest_result.error,
        )

    observability_alerts = tool_registry.invoke_tool(
        "observability",
        {"action": "query_alert_history", "limit": 5},
    )
    observability_digests = tool_registry.invoke_tool(
        "observability",
        {"action": "recent_digests", "limit": 3},
    )
    observability_immune = tool_registry.invoke_tool(
        "observability",
        {"action": "query_immune_verdicts", "limit": 5},
    )
    observability_telemetry = tool_registry.invoke_tool(
        "observability",
        {"action": "query_telemetry", "chain_id": chain_id, "limit": 20},
    )
    observability_reliability = tool_registry.invoke_tool(
        "observability",
        {"action": "reliability_dashboard", "limit": 10},
    )
    observability_health = tool_registry.invoke_tool(
        "observability",
        {"action": "system_health"},
    )
    observability_results = [
        observability_alerts,
        observability_digests,
        observability_immune,
        observability_telemetry,
        observability_reliability,
        observability_health,
    ]
    first_failure = next((item for item in observability_results if not item.success), None)
    if first_failure is not None:
        doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
        return OperatorWorkflowResult(
            ok=False,
            bootstrap=bootstrap,
            sheriff_outcome=sheriff_verdict.outcome.value,
            routing_tier=routing_decision.tier.value,
            brief_id=brief_id,
            readback=read_result.output,
            alert_id=alert_result.output,
            digest_id=digest_result.output.get("digest_id"),
            digest=digest_result.output,
            observability=None,
            doctor=doctor,
            error=first_failure.error,
        )

    observability = WorkflowObservabilitySnapshot(
        alert_history=observability_alerts.output,
        digest_history=observability_digests.output,
        immune_verdicts=observability_immune.output,
        telemetry_events=observability_telemetry.output,
        reliability_dashboard=observability_reliability.output,
        system_health=observability_health.output,
    )
    doctor = doctor_runtime(tool_registry, config=resolved, bootstrap_if_needed=False)
    return OperatorWorkflowResult(
        ok=doctor.ok,
        bootstrap=bootstrap,
        sheriff_outcome=sheriff_verdict.outcome.value,
        routing_tier=routing_decision.tier.value,
        brief_id=brief_id,
        readback=read_result.output,
        alert_id=alert_result.output,
        digest_id=digest_result.output["digest_id"],
        digest=digest_result.output,
        observability=observability,
        doctor=doctor,
        error=None if doctor.ok else "doctor reported missing runtime components",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and smoke-test the Hermes integration bootstrap")
    parser.add_argument("--bootstrap-live", action="store_true", help="Bootstrap the runtime against the selected registry")
    parser.add_argument("--install-profile", action="store_true", help="Install a local Hermes runtime profile bundle")
    parser.add_argument("--doctor", action="store_true", help="Verify runtime layout, databases, and skill registration")
    parser.add_argument("--operator-workflow", action="store_true", help="Run the Stage 0/1 operator workflow smoke test")
    parser.add_argument("--data-dir", default="~/.hermes/data/")
    parser.add_argument("--skills-dir", default="~/.hermes/skills/hybrid-autonomous-ai/")
    parser.add_argument("--checkpoints-dir", default="~/.hermes/skills/hybrid-autonomous-ai/checkpoints/")
    parser.add_argument("--alerts-dir", default="~/.hermes/alerts/")
    parser.add_argument("--profile-name", default="hybrid-autonomous-ai")
    parser.add_argument("--model-name", default="local-default")
    parser.add_argument("--repo-root", default=None, help="Override the repository root used for profile installation")
    parser.add_argument("--task-id", default="stage0-operator-workflow")
    parser.add_argument("--title", default="Stage 0/1 operator workflow smoke test")
    parser.add_argument(
        "--summary",
        default="Validated bootstrap, routing, memory, and operator alert smoke test.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    config = IntegrationConfig(
        data_dir=args.data_dir,
        skills_dir=args.skills_dir,
        checkpoints_dir=args.checkpoints_dir,
        alerts_dir=args.alerts_dir,
        profile_name=args.profile_name,
    )
    runtime = MockHermesRuntime(data_dir=str(Path(args.data_dir).expanduser()))

    if args.install_profile:
        result = install_runtime_profile(config, repo_root=args.repo_root)
        print(f"profile manifest={result.profile_manifest_path}")
        print(f"launchers={','.join(sorted(result.launcher_paths.values()))}")
        print(f"linked_skills={len(result.linked_skill_paths)}")
        return 0

    if args.doctor:
        result = doctor_runtime(runtime, config=config)
        print("doctor ok" if result.ok else "doctor failed")
        print(f"missing={','.join(result.missing_items) if result.missing_items else 'none'}")
        print(f"tools={','.join(result.registered_tools)}")
        return 0 if result.ok else 1

    if args.operator_workflow:
        result = run_operator_workflow(
            runtime,
            config=config,
            model_name=args.model_name,
            task_id=args.task_id,
            title=args.title,
            summary=args.summary,
        )
        print("workflow ok" if result.ok else "workflow failed")
        print(f"sheriff={result.sheriff_outcome}")
        print(f"route={result.routing_tier or 'none'}")
        print(f"brief_id={result.brief_id or 'none'}")
        print(f"alert_id={result.alert_id or 'none'}")
        if result.error:
            print(f"error={result.error}")
        return 0 if result.ok else 1

    result = bootstrap_runtime(runtime, config=config, model_name=args.model_name)
    print("bootstrap ok" if result.ok else "bootstrap failed")
    print(f"session_id={result.session_context.session_id}")
    print(f"tools={','.join(result.registered_tools)}")
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
