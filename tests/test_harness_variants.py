from __future__ import annotations

import sqlite3
from pathlib import Path

from harness_variants import ExecutionTrace, ExecutionTraceStep, HarnessVariantManager, VariantEvalResult
from migrate import apply_schema


def _telemetry_manager(tmp_path: Path) -> HarnessVariantManager:
    db_path = tmp_path / "telemetry.db"
    apply_schema(db_path, Path("schemas/telemetry.sql"))
    return HarnessVariantManager(str(db_path))


def test_execution_trace_roundtrip_and_summary(tmp_path):
    manager = _telemetry_manager(tmp_path)

    first = manager.log_execution_trace(
        ExecutionTrace(
            trace_id="trace-1",
            task_id="task-1",
            role="runtime_contract",
            skill_name="runtime",
            harness_version="v1",
            intent_goal="prove contract",
            steps=[
                ExecutionTraceStep(
                    step_index=1,
                    tool_call="financial_router.route",
                    tool_result='{"tier":"paid_cloud"}',
                    tool_result_file=None,
                    tokens_in=0,
                    tokens_out=0,
                    latency_ms=4,
                    model_used="repo-contract",
                )
            ],
            prompt_template="contract harness",
            context_assembled="runtime+operator",
            retrieval_queries=[],
            judge_verdict="PASS",
            judge_reasoning="passed",
            outcome_score=1.0,
            cost_usd=0.0,
            duration_ms=12,
            training_eligible=True,
            retention_class="STANDARD",
            source_chain_id="chain-1",
            source_session_id="session-1",
            source_trace_id=None,
            created_at="2026-04-21T12:00:00+00:00",
        )
    )
    second = manager.log_execution_trace(
        ExecutionTrace(
            trace_id="trace-2",
            task_id="task-2",
            role="runtime_contract",
            skill_name="runtime",
            harness_version="v1",
            intent_goal="prove failure retention",
            steps=[],
            prompt_template="contract harness",
            context_assembled="runtime+operator",
            retrieval_queries=[],
            judge_verdict="FAIL",
            judge_reasoning="failed",
            outcome_score=0.0,
            cost_usd=0.0,
            duration_ms=8,
            training_eligible=False,
            retention_class="FAILURE_AUDIT",
            source_chain_id="chain-2",
            source_session_id="session-2",
            source_trace_id=None,
            created_at="2026-04-21T12:01:00+00:00",
        )
    )

    assert first["trace_id"] == "trace-1"
    assert second["retention_class"] == "FAILURE_AUDIT"
    traces = manager.list_execution_traces(limit=5, skill_name="runtime")
    assert [row["trace_id"] for row in traces] == ["trace-2", "trace-1"]
    summary = manager.execution_trace_summary()
    assert summary["total_count"] == 2
    assert summary["training_eligible_count"] == 1
    assert summary["failure_audit_count"] == 1


def test_harness_variant_lifecycle_and_frontier(tmp_path):
    manager = _telemetry_manager(tmp_path)

    proposed = manager.propose_variant(
        skill_name="research_domain",
        parent_version="abc123",
        diff="@@ -1 +1 @@\n-old\n+new\n",
        source="operator",
        prompt_prelude="Tighten harness prompt.",
        reference_time="2026-04-21T12:00:00+00:00",
    )
    assert proposed["status"] == "PROPOSED"

    concurrent = manager.propose_variant(
        skill_name="research_domain",
        parent_version="abc123",
        diff="@@ -1 +1 @@\n-old\n+alt\n",
        source="operator",
        reference_time="2026-04-21T12:01:00+00:00",
    )
    assert concurrent["status"] == "REJECTED"
    assert concurrent["reject_reason"] == "CONCURRENT_VARIANT"

    shadow = manager.start_shadow_eval(proposed["variant_id"], reference_time="2026-04-21T12:02:00+00:00")
    assert shadow["status"] == "SHADOW_EVAL"

    promoted = manager.record_eval_result(
        proposed["variant_id"],
        VariantEvalResult(
            variant_id=proposed["variant_id"],
            skill_name="research_domain",
            benchmark_name="shadow_replay_research_domain",
            baseline_outcome_scores=[0.7, 0.8, 0.75],
            variant_outcome_scores=[0.8, 0.82, 0.79],
            regression_rate=0.0,
            gate_0_pass=True,
            known_bad_block_rate=1.0,
            gate_1_pass=True,
            baseline_mean_score=0.75,
            variant_mean_score=0.8033,
            quality_delta=0.0533,
            gate_2_pass=True,
            baseline_std=0.04,
            variant_std=0.03,
            gate_3_pass=True,
            regressed_trace_count=0,
            improved_trace_count=3,
            net_trace_gain=3,
            traces_evaluated=3,
            compute_cost_cu=1.5,
            eval_duration_ms=250,
            created_at="2026-04-21T12:03:00+00:00",
        ),
        reference_time="2026-04-21T12:03:00+00:00",
    )
    assert promoted["status"] == "PROMOTED"
    assert promoted["promoted_at"] == "2026-04-21T12:03:00+00:00"

    frontier = manager.frontier(limit=5, skill_name="research_domain")
    assert len(frontier) == 1
    assert frontier[0]["variant_id"] == proposed["variant_id"]

    rate_limited = manager.propose_variant(
        skill_name="research_domain",
        parent_version="def456",
        diff="@@ -1 +1 @@\n-old\n+later\n",
        source="operator",
        reference_time="2026-04-21T13:00:00+00:00",
    )
    assert rate_limited["status"] == "REJECTED"
    assert rate_limited["reject_reason"] == "RATE_LIMITED"

    scope_violation = manager.propose_variant(
        skill_name="operator_interface",
        parent_version="ghi789",
        diff="@@ -1 +1 @@\n-old\n+infra\n",
        source="operator",
        touches_infrastructure=True,
        reference_time="2026-04-21T12:05:00+00:00",
    )
    assert scope_violation["status"] == "REJECTED"
    assert scope_violation["reject_reason"] == "SCOPE_VIOLATION"

    summary = manager.summary()
    assert summary["active_count"] == 0
    assert summary["promoted_count"] == 1
    assert summary["rejected_24h"] == 3
