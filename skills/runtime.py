from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from migrate import SCHEMAS, apply_schema, verify_database
from skills.bootstrap import BootstrapOrchestrator
from skills.config import IntegrationConfig
from skills.hermes_interfaces import HermesSessionContext, HermesToolRegistry, MockHermesRuntime


@dataclass(frozen=True)
class RuntimeBootstrapResult:
    """Structured result for a Hermes integration bootstrap attempt."""

    ok: bool
    config: IntegrationConfig
    session_context: HermesSessionContext
    database_status: dict[str, bool]
    registered_tools: list[str]


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
        session_id=session_id or str(uuid.uuid4()),
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and smoke-test the Hermes integration bootstrap")
    parser.add_argument("--data-dir", default="~/.hermes/data/")
    parser.add_argument("--skills-dir", default="~/.hermes/skills/hybrid-autonomous-ai/")
    parser.add_argument("--checkpoints-dir", default="~/.hermes/skills/hybrid-autonomous-ai/checkpoints/")
    parser.add_argument("--alerts-dir", default="~/.hermes/alerts/")
    parser.add_argument("--profile-name", default="hybrid-autonomous-ai")
    parser.add_argument("--model-name", default="local-default")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    result = bootstrap_runtime(
        MockHermesRuntime(data_dir=str(Path(args.data_dir).expanduser())),
        config=IntegrationConfig(
            data_dir=args.data_dir,
            skills_dir=args.skills_dir,
            checkpoints_dir=args.checkpoints_dir,
            alerts_dir=args.alerts_dir,
            profile_name=args.profile_name,
        ),
        model_name=args.model_name,
    )
    print("bootstrap ok" if result.ok else "bootstrap failed")
    print(f"session_id={result.session_context.session_id}")
    print(f"tools={','.join(result.registered_tools)}")
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
