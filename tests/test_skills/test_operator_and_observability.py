from __future__ import annotations

from skills.db_manager import DatabaseManager
from skills.observability.skill import ObservabilitySkill
from skills.operator_interface.skill import OperatorInterfaceSkill
from skills.research_domain.skill import ResearchDomainSkill
from skills.strategic_memory.skill import StrategicMemorySkill


def test_operator_interface_generates_deterministic_digest(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    memory = StrategicMemorySkill(db)
    operator = OperatorInterfaceSkill(db)

    memory.write_brief("task-1", "Fresh Brief", "Summary", actionability="ACTION_RECOMMENDED")
    operator.record_heartbeat("command")
    operator.alert("T1", "WORKFLOW_SMOKE_TEST", "Stored a brief.", channel_delivered="CLI")
    digest = operator.generate_digest()

    assert digest["digest_type"] == "daily"
    assert "PORTFOLIO HEALTH:" in digest["content"]
    assert "Fresh Brief (ACTION_RECOMMENDED)" in digest["content"]
    assert len(digest["sections_included"]) == 6


def test_observability_queries_alerts_reliability_and_health(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    operator = OperatorInterfaceSkill(db)
    observability = ObservabilitySkill(db, telemetry_buffer=None, immune_buffer=None)
    telemetry = db.get_connection("telemetry")

    operator.record_heartbeat("command")
    operator.alert("T2", "EXECUTOR_SATURATION", "Heads up.", channel_delivered="CLI")
    telemetry.execute(
        "INSERT INTO chain_definitions (chain_type, steps, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (
            "operator_workflow",
            '[{"step_type":"alert","skill":"operator_interface"}]',
            "2026-04-14T08:00:00+00:00",
            "2026-04-14T08:00:00+00:00",
        ),
    )
    telemetry.execute(
        "INSERT INTO step_outcomes VALUES (?,?,?,?,?,?,?,?,?)",
        (
            "evt-1",
            "alert",
            "operator_interface",
            "operator-workflow",
            "PASS",
            12,
            0,
            None,
            "2026-04-14T08:00:00+00:00",
        ),
    )
    telemetry.commit()

    alerts = observability.query_alert_history(limit=5, tier="T2")
    telemetry_rows = observability.query_telemetry(chain_id="operator-workflow")
    reliability = observability.reliability_dashboard()
    health = observability.system_health()

    assert alerts[0]["alert_type"] == "EXECUTOR_SATURATION"
    assert telemetry_rows[0]["skill"] == "operator_interface"
    assert reliability["chains"][0]["chain_type"] == "operator_workflow"
    assert health["heartbeat_state"] == "ACTIVE"
    assert health["alert_counts"]["T2"] == 1


def test_research_domain_can_list_and_complete_tasks(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    research = ResearchDomainSkill(db)

    task_id = research.create_task("Market scan", "Check competitors", tags=["market"])
    pending = research.list_tasks(status="PENDING")
    completed = research.complete_task(task_id, actual_spend_usd=0.0)

    assert pending[0]["task_id"] == task_id
    assert pending[0]["tags"] == ["market"]
    assert completed["status"] == "COMPLETE"
