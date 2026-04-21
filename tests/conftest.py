from __future__ import annotations

import sqlite3
from pathlib import Path
import tempfile

import pytest

from immune.types import ImmuneConfig, JudgePayload, SheriffPayload, generate_uuid_v7


@pytest.fixture
def default_config() -> ImmuneConfig:
    return ImmuneConfig(known_tool_registry=frozenset({"safe_tool", "web_fetch", "shell_command"}))


@pytest.fixture
def clean_sheriff_payload() -> SheriffPayload:
    return SheriffPayload(
        session_id=generate_uuid_v7(),
        skill_name="immune_system",
        tool_name="safe_tool",
        arguments={"query": "hello"},
        raw_prompt="hello",
        source_trust_tier=4,
        jwt_claims={"max_tool_calls": 5, "current_tool_calls": 1},
    )


@pytest.fixture
def clean_judge_payload(clean_sheriff_payload: SheriffPayload) -> JudgePayload:
    return JudgePayload(
        session_id=clean_sheriff_payload.session_id,
        skill_name=clean_sheriff_payload.skill_name,
        tool_name=clean_sheriff_payload.tool_name,
        output={"ok": True, "claimed_trust_tier": 4},
        expected_schema={
            "type": "object",
            "required": ["ok"],
            "properties": {
                "ok": {"type": "boolean"},
                "claimed_trust_tier": {"type": "integer"},
            },
        },
    )


@pytest.fixture
def test_db() -> str:
    tmp_dir = Path(tempfile.mkdtemp(prefix="immune-test-db-"))
    path = tmp_dir / "immune_system.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE immune_verdicts (
            verdict_id TEXT, verdict_type TEXT, scan_tier TEXT, session_id TEXT,
            skill_name TEXT, task_type TEXT, result TEXT, match_pattern TEXT,
            latency_ms INTEGER, judge_mode TEXT, timestamp TEXT
        );
        CREATE TABLE security_alerts (
            alert_id TEXT, source TEXT, severity TEXT, details TEXT,
            session_id TEXT, resolved INTEGER, resolved_at TEXT, timestamp TEXT
        );
        CREATE TABLE canary_audits (id TEXT);
        """
    )
    conn.commit()
    conn.close()
    operator_conn = sqlite3.connect(tmp_dir / "operator_digest.db")
    operator_conn.executescript(
        """
        CREATE TABLE runtime_control_state (
            state_id TEXT PRIMARY KEY,
            lifecycle_state TEXT,
            active_halt_id TEXT,
            last_halt_reason TEXT,
            last_transition_at TEXT,
            last_restart_id TEXT
        );
        CREATE TABLE runtime_halt_events (
            halt_id TEXT PRIMARY KEY,
            halt_scope TEXT,
            source TEXT,
            trigger_event_id TEXT,
            halt_reason TEXT,
            requires_human INTEGER,
            created_at TEXT,
            cleared_at TEXT,
            clear_reason TEXT,
            clear_restart_id TEXT,
            status TEXT
        );
        CREATE TABLE runtime_restart_history (
            restart_id TEXT PRIMARY KEY,
            halt_id TEXT,
            requested_at TEXT,
            completed_at TEXT,
            status TEXT,
            restart_reason TEXT,
            preflight_json TEXT,
            notes TEXT
        );
        """
    )
    operator_conn.commit()
    operator_conn.close()
    yield str(path)


@pytest.fixture
def mock_dispatch():
    def _dispatch(*args, **kwargs):
        return {"ok": True, "claimed_trust_tier": kwargs.get("claimed_trust_tier", 4)}

    return _dispatch
