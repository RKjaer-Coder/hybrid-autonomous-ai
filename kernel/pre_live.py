from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .model_intelligence import SEED_TASK_CLASSES
from .store import KernelStore


PRE_LIVE_GOALS: tuple[tuple[str, str], ...] = (
    ("operator_project_loop", "End-to-end operator validate/build/ship/feedback loop"),
    ("model_efficiency_service", "Governed model-efficiency service packet and shadow evidence"),
    ("seed_model_intelligence", "Seed Model Intelligence registry, eval, route, and promotion gates"),
    ("research_retrieval", "Research Engine retrieval planning, grants, acquisition, and evidence bundles"),
    ("council_execution", "Council/scarce-deliberation records for high-uncertainty decisions"),
    ("hermes_adapter_proxy", "Hermes adapter, migration, and proxy enforcement readiness"),
    ("side_effect_delivery", "Side-effect and customer-visible delivery governance"),
    ("operator_gate_surface", "Local operator command and gate surface"),
    ("data_governance", "Artifact, redaction, encrypted storage, backup, and recovery proof"),
    ("evidence_packaging", "Replay/projection comparisons and pre-live evidence packaging"),
)


@dataclass(frozen=True)
class PreLiveGoalStatus:
    goal_id: str
    title: str
    complete: bool
    blockers: list[str]
    evidence: dict[str, Any]


@dataclass(frozen=True)
class PreLiveCompletionBundle:
    complete: bool
    completed_goals: int
    total_goals: int
    goals: list[PreLiveGoalStatus]
    next_blockers: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "completed_goals": self.completed_goals,
            "total_goals": self.total_goals,
            "goals": [
                {
                    "goal_id": goal.goal_id,
                    "title": goal.title,
                    "complete": goal.complete,
                    "blockers": goal.blockers,
                    "evidence": goal.evidence,
                }
                for goal in self.goals
            ],
            "next_blockers": self.next_blockers,
        }


def summarize_pre_live_completion(store: KernelStore) -> PreLiveCompletionBundle:
    """Summarize whether the deterministic pre-live finish line is coded and proven.

    This intentionally reads committed kernel state instead of narrative notes.
    Live Hermes, customer delivery, paid routes, and active promotion can remain
    closed; this report only asks whether the repo has the pre-live contracts,
    fixtures, gates, and replay evidence needed before target-machine work.
    """

    with store.connect() as conn:
        goals = [
            _operator_project_loop(conn),
            _model_efficiency_service(conn),
            _seed_model_intelligence(conn),
            _research_retrieval(conn),
            _council_execution(conn),
            _hermes_adapter_proxy(conn),
            _side_effect_delivery(conn),
            _operator_gate_surface(conn),
            _data_governance(conn),
            _evidence_packaging(conn),
        ]
    completed = sum(1 for goal in goals if goal.complete)
    next_blockers = [
        f"{goal.goal_id}: {blocker}"
        for goal in goals
        if not goal.complete
        for blocker in goal.blockers
    ]
    return PreLiveCompletionBundle(
        complete=completed == len(PRE_LIVE_GOALS),
        completed_goals=completed,
        total_goals=len(PRE_LIVE_GOALS),
        goals=goals,
        next_blockers=next_blockers,
    )


def _operator_project_loop(conn: Any) -> PreLiveGoalStatus:
    task_types = set(_column(conn, "SELECT DISTINCT task_type FROM project_tasks"))
    outcome_types = set(_column(conn, "SELECT DISTINCT outcome_type FROM project_outcomes"))
    artifact_kinds = set(_column(conn, "SELECT DISTINCT artifact_kind FROM project_artifact_receipts"))
    project_comparisons = _count(conn, "SELECT COUNT(*) FROM project_replay_projection_comparisons WHERE matches=1")
    evidence = {
        "projects": _count(conn, "SELECT COUNT(*) FROM projects"),
        "task_types": sorted(task_types),
        "outcome_types": sorted(outcome_types),
        "artifact_kinds": sorted(artifact_kinds),
        "matching_project_replay_comparisons": project_comparisons,
    }
    blockers = _missing(
        ("project", evidence["projects"] > 0),
        ("validate/build/ship tasks", {"validate", "build", "ship"}.issubset(task_types)),
        ("validation/build/shipped/feedback outcomes", {"validation", "build_artifact", "shipped_artifact", "feedback"}.issubset(outcome_types)),
        ("build and shipped artifact receipts", {"build_artifact", "shipped_artifact"}.issubset(artifact_kinds)),
        ("clean project replay/projection comparison", project_comparisons > 0),
    )
    return _goal("operator_project_loop", blockers, evidence)


def _model_efficiency_service(conn: Any) -> PreLiveGoalStatus:
    shadow_routes = _count(conn, "SELECT COUNT(*) FROM model_route_decisions WHERE selected_route='shadow'")
    promotion_packets = _count(conn, "SELECT COUNT(*) FROM model_promotion_decision_packets")
    savings_eval_runs = _count(
        conn,
        """
        SELECT COUNT(*) FROM model_eval_runs
        WHERE json_extract(aggregate_scores_json, '$.overall') IS NOT NULL
           OR json_extract(frozen_holdout_result_json, '$.quality_score') IS NOT NULL
        """,
    )
    commercial_packets = _count(conn, "SELECT COUNT(*) FROM commercial_decision_packets WHERE recommendation IN ('pursue','pause')")
    evidence = {
        "shadow_routes": shadow_routes,
        "promotion_packets": promotion_packets,
        "scored_eval_runs": savings_eval_runs,
        "commercial_decision_packets": commercial_packets,
    }
    blockers = _missing(
        ("shadow routing evidence", shadow_routes > 0),
        ("promotion gate packet", promotion_packets > 0),
        ("scored eval evidence", savings_eval_runs > 0),
        ("commercial service decision packet", commercial_packets > 0),
    )
    return _goal("model_efficiency_service", blockers, evidence)


def _seed_model_intelligence(conn: Any) -> PreLiveGoalStatus:
    task_classes = set(_column(conn, "SELECT task_class FROM model_task_classes WHERE status IN ('seed','active')"))
    eval_task_classes = set(_column(conn, "SELECT DISTINCT task_class FROM local_offload_eval_sets WHERE status='active'"))
    eval_run_task_classes = set(_column(conn, "SELECT DISTINCT task_class FROM model_eval_runs"))
    holdout_task_classes = set(_column(conn, "SELECT DISTINCT task_class FROM model_holdout_policies"))
    evidence = {
        "task_classes": sorted(task_classes),
        "eval_task_classes": sorted(eval_task_classes),
        "eval_run_task_classes": sorted(eval_run_task_classes),
        "holdout_task_classes": sorted(holdout_task_classes),
        "candidates": _count(conn, "SELECT COUNT(*) FROM model_candidates"),
    }
    seed = set(SEED_TASK_CLASSES)
    blockers = _missing(
        ("all three seed task classes registered", seed.issubset(task_classes)),
        ("all three seed eval sets registered", seed.issubset(eval_task_classes)),
        ("all three seed task classes have eval runs", seed.issubset(eval_run_task_classes)),
        ("all three seed holdout policies exist", seed.issubset(holdout_task_classes)),
        ("at least one model candidate registered", evidence["candidates"] > 0),
    )
    return _goal("seed_model_intelligence", blockers, evidence)


def _research_retrieval(conn: Any) -> PreLiveGoalStatus:
    acquisition_results = set(_column(conn, "SELECT DISTINCT result FROM source_acquisition_checks"))
    grant_types = set(_column(conn, "SELECT DISTINCT capability_type FROM capability_grants WHERE capability_type IN ('file','network')"))
    quality_results = set(_column(conn, "SELECT DISTINCT quality_gate_result FROM evidence_bundles"))
    evidence = {
        "source_plans": _count(conn, "SELECT COUNT(*) FROM source_plans"),
        "acquisition_results": sorted(acquisition_results),
        "retrieval_grant_types": sorted(grant_types),
        "evidence_bundles": _count(conn, "SELECT COUNT(*) FROM evidence_bundles"),
        "quality_results": sorted(quality_results),
    }
    blockers = _missing(
        ("source plan", evidence["source_plans"] > 0),
        ("source acquisition boundary check", bool(acquisition_results)),
        ("file or network retrieval grant", bool(grant_types)),
        ("evidence bundle", evidence["evidence_bundles"] > 0),
        ("passing or degraded quality gate evidence", bool(quality_results & {"pass", "degraded"})),
    )
    return _goal("research_retrieval", blockers, evidence)


def _council_execution(conn: Any) -> PreLiveGoalStatus:
    council_recommendations = _count(
        conn,
        "SELECT COUNT(*) FROM commercial_decision_recommendations WHERE recommendation_authority='council'",
    )
    high_stakes_council_tasks = _count(
        conn,
        "SELECT COUNT(*) FROM project_tasks WHERE authority_required='council' OR risk_level IN ('high','critical')",
    )
    gated_decisions = _count(
        conn,
        "SELECT COUNT(*) FROM decisions WHERE required_authority IN ('council','operator_gate')",
    )
    evidence = {
        "council_recommendations": council_recommendations,
        "high_stakes_or_council_tasks": high_stakes_council_tasks,
        "gated_decisions": gated_decisions,
    }
    blockers = _missing(
        ("Council recommendation record", council_recommendations > 0),
        ("high-stakes task or Council authority path", high_stakes_council_tasks > 0),
        ("gated decision records", gated_decisions > 0),
    )
    return _goal("council_execution", blockers, evidence)


def _hermes_adapter_proxy(conn: Any) -> PreLiveGoalStatus:
    ready_packets = _count(conn, "SELECT COUNT(*) FROM hermes_adapter_readiness_packets WHERE readiness_status='ready'")
    migration_records = _count(conn, "SELECT COUNT(*) FROM migration_readiness_records")
    adapter_grants = _count(conn, "SELECT COUNT(*) FROM capability_grants WHERE subject_type='adapter'")
    provider_intents = _count(conn, "SELECT COUNT(*) FROM side_effect_intents WHERE side_effect_type='provider_call'")
    comparisons = _count(conn, "SELECT COUNT(*) FROM hermes_adapter_readiness_replay_projection_comparisons WHERE matches=1")
    evidence = {
        "ready_adapter_packets": ready_packets,
        "migration_records": migration_records,
        "adapter_grants": adapter_grants,
        "provider_call_intents": provider_intents,
        "matching_adapter_comparisons": comparisons,
    }
    blockers = _missing(
        ("ready Hermes adapter packet", ready_packets > 0),
        ("migration ownership records", migration_records > 0),
        ("adapter capability grants", adapter_grants > 0),
        ("prepared provider-call/proxy side-effect intent", provider_intents > 0),
        ("clean Hermes adapter replay/projection comparison", comparisons > 0),
    )
    return _goal("hermes_adapter_proxy", blockers, evidence)


def _side_effect_delivery(conn: Any) -> PreLiveGoalStatus:
    intents = _count(conn, "SELECT COUNT(*) FROM side_effect_intents")
    receipts = _count(conn, "SELECT COUNT(*) FROM side_effect_receipts")
    customer_packets = _count(conn, "SELECT COUNT(*) FROM project_customer_visible_packets")
    customer_comparisons = _count(
        conn,
        "SELECT COUNT(*) FROM project_customer_visible_replay_projection_comparisons WHERE matches=1",
    )
    commitment_receipts = _count(conn, "SELECT COUNT(*) FROM project_customer_commitment_receipts")
    evidence = {
        "side_effect_intents": intents,
        "side_effect_receipts": receipts,
        "customer_visible_packets": customer_packets,
        "customer_commitment_receipts": commitment_receipts,
        "matching_customer_visible_comparisons": customer_comparisons,
    }
    blockers = _missing(
        ("side-effect intent", intents > 0),
        ("side-effect receipt", receipts > 0),
        ("operator-gated customer-visible packet", customer_packets > 0),
        ("customer commitment receipt", commitment_receipts > 0),
        ("clean customer-visible replay/projection comparison", customer_comparisons > 0),
    )
    return _goal("side_effect_delivery", blockers, evidence)


def _operator_gate_surface(conn: Any) -> PreLiveGoalStatus:
    gated_decisions = _count(conn, "SELECT COUNT(*) FROM decisions WHERE required_authority='operator_gate'")
    operator_gate_commands = _count(conn, "SELECT COUNT(*) FROM commands WHERE requested_authority='operator_gate'")
    rule_commands = _count(conn, "SELECT COUNT(*) FROM commands WHERE requested_authority='rule' OR requested_authority IS NULL")
    halted_live_controls = _count(
        conn,
        """
        SELECT
          (SELECT COUNT(*) FROM hermes_adapter_readiness_packets WHERE live_controls_enabled=0) +
          (SELECT COUNT(*) FROM migration_readiness_records WHERE live_controls_enabled=0)
        """,
    )
    evidence = {
        "operator_gate_decisions": gated_decisions,
        "operator_gate_commands": operator_gate_commands,
        "rule_or_local_commands": rule_commands,
        "closed_live_control_records": halted_live_controls,
    }
    blockers = _missing(
        ("operator-gated decisions", gated_decisions > 0),
        ("operator-gate commands", operator_gate_commands > 0),
        ("local/rule command path", rule_commands > 0),
        ("closed live-control records", halted_live_controls > 0),
    )
    return _goal("operator_gate_surface", blockers, evidence)


def _data_governance(conn: Any) -> PreLiveGoalStatus:
    artifact_refs = _count(conn, "SELECT COUNT(*) FROM artifact_refs")
    encrypted_descriptors = _count(conn, "SELECT COUNT(*) FROM encrypted_storage_descriptors")
    backup_cadence = _count(conn, "SELECT COUNT(*) FROM backup_cadence_records")
    recovery_ready = _count(conn, "SELECT COUNT(*) FROM recovery_readiness_packets WHERE readiness_status='ready'")
    access_verified = _count(
        conn,
        "SELECT COUNT(*) FROM encrypted_storage_access_verification_states WHERE status='verified'",
    )
    evidence = {
        "artifact_refs": artifact_refs,
        "encrypted_storage_descriptors": encrypted_descriptors,
        "backup_cadence_records": backup_cadence,
        "ready_recovery_packets": recovery_ready,
        "verified_storage_access_states": access_verified,
    }
    blockers = _missing(
        ("artifact references", artifact_refs > 0),
        ("encrypted storage descriptors", encrypted_descriptors > 0),
        ("backup cadence records", backup_cadence > 0),
        ("ready recovery packet", recovery_ready > 0),
        ("verified storage access state", access_verified > 0),
    )
    return _goal("data_governance", blockers, evidence)


def _evidence_packaging(conn: Any) -> PreLiveGoalStatus:
    events = _count(conn, "SELECT COUNT(*) FROM events")
    mismatches = _count(
        conn,
        """
        SELECT
          (SELECT COUNT(*) FROM project_replay_projection_comparisons WHERE matches=0) +
          (SELECT COUNT(*) FROM project_customer_visible_replay_projection_comparisons WHERE matches=0) +
          (SELECT COUNT(*) FROM recovery_readiness_replay_projection_comparisons WHERE matches=0) +
          (SELECT COUNT(*) FROM hermes_adapter_readiness_replay_projection_comparisons WHERE matches=0) +
          (SELECT COUNT(*) FROM migration_readiness_replay_projection_comparisons WHERE matches=0)
        """,
    )
    matching_comparisons = _count(
        conn,
        """
        SELECT
          (SELECT COUNT(*) FROM project_replay_projection_comparisons WHERE matches=1) +
          (SELECT COUNT(*) FROM project_customer_visible_replay_projection_comparisons WHERE matches=1) +
          (SELECT COUNT(*) FROM recovery_readiness_replay_projection_comparisons WHERE matches=1) +
          (SELECT COUNT(*) FROM hermes_adapter_readiness_replay_projection_comparisons WHERE matches=1) +
          (SELECT COUNT(*) FROM migration_readiness_replay_projection_comparisons WHERE matches=1)
        """,
    )
    data_classes = set(_column(conn, "SELECT DISTINCT data_class FROM events"))
    evidence = {
        "events": events,
        "matching_replay_projection_comparisons": matching_comparisons,
        "mismatching_replay_projection_comparisons": mismatches,
        "event_data_classes": sorted(data_classes),
    }
    blockers = _missing(
        ("append-only event evidence", events > 0),
        ("matching replay/projection comparison evidence", matching_comparisons >= 5),
        ("no replay/projection mismatches", mismatches == 0),
        ("governed event data classes", bool(data_classes)),
    )
    return _goal("evidence_packaging", blockers, evidence)


def _goal(goal_id: str, blockers: list[str], evidence: dict[str, Any]) -> PreLiveGoalStatus:
    title = dict(PRE_LIVE_GOALS)[goal_id]
    return PreLiveGoalStatus(goal_id=goal_id, title=title, complete=not blockers, blockers=blockers, evidence=evidence)


def _missing(*checks: tuple[str, bool]) -> list[str]:
    return [label for label, ok in checks if not ok]


def _count(conn: Any, query: str) -> int:
    return int(conn.execute(query).fetchone()[0] or 0)


def _column(conn: Any, query: str) -> list[Any]:
    return [row[0] for row in conn.execute(query).fetchall()]


def _json(value: str | None) -> Any:
    if value is None:
        return None
    return json.loads(value)
