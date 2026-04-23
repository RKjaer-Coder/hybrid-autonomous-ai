from __future__ import annotations

import datetime
import time
from pathlib import Path
from typing import Optional

from harness_variants import HarnessVariantManager
from immune.config import load_config
from immune.judge import judge_check
from immune.judge_lifecycle import JudgeLifecycleManager
from immune.sheriff import sheriff_check
from immune.types import ImmuneVerdict, JudgeMode, JudgePayload, Outcome, SheriffPayload
from skills.append_buffer import AppendBuffer


class ImmuneSystemSkill:
    def __init__(self, verdict_buffer: Optional[AppendBuffer] = None, immune_db_path: str | None = None):
        self._config = load_config()
        self._buffer = verdict_buffer
        self._judge_lifecycle = None if immune_db_path is None else JudgeLifecycleManager(immune_db_path, self._config)
        telemetry_db_path = None if immune_db_path is None else str(Path(immune_db_path).with_name("telemetry.db"))
        self._harness_variants = (
            None if telemetry_db_path is None else HarnessVariantManager(telemetry_db_path)
        )

    def check_sheriff(self, payload: SheriffPayload) -> ImmuneVerdict:
        start = time.monotonic_ns()
        verdict = sheriff_check(payload, self._config)
        latency_ms = (time.monotonic_ns() - start) / 1_000_000
        if self._buffer:
            self._buffer.append(self._verdict_to_row(verdict, latency_ms))
        self._log_trace(
            payload=payload,
            verdict=verdict,
            latency_ms=latency_ms,
            role="immune_sheriff_check",
            context_assembled=(
                f"skill_name={payload.skill_name}; tool_name={payload.tool_name}; "
                f"source_trust_tier={payload.source_trust_tier}"
            ),
        )
        return verdict

    def check_judge(self, payload: JudgePayload) -> ImmuneVerdict:
        start = time.monotonic_ns()
        active_event = None
        prepared = payload
        if self._judge_lifecycle is not None:
            prepared, active_event = self._judge_lifecycle.prepare_payload(payload)
        if active_event is not None and active_event["status"] == "HALTED":
            verdict = self._judge_lifecycle.halted_verdict(prepared)
        else:
            verdict = judge_check(prepared, self._config)
        latency_ms = (time.monotonic_ns() - start) / 1_000_000
        if self._buffer:
            self._buffer.append(self._verdict_to_row(verdict, latency_ms))
        if self._judge_lifecycle is not None:
            self._judge_lifecycle.record_verdict(prepared, verdict)
        self._log_trace(
            payload=prepared,
            verdict=verdict,
            latency_ms=latency_ms,
            role="immune_judge_check",
            context_assembled=(
                f"skill_name={prepared.skill_name}; tool_name={prepared.tool_name}; "
                f"task_type={prepared.task_type or prepared.skill_name}; "
                f"active_event_status={None if active_event is None else active_event['status']}"
            ),
        )
        return verdict

    def _log_trace(
        self,
        *,
        payload: SheriffPayload | JudgePayload,
        verdict: ImmuneVerdict,
        latency_ms: float,
        role: str,
        context_assembled: str,
    ) -> None:
        if self._harness_variants is None or not self._harness_variants.available:
            return
        is_training_eligible = (
            verdict.outcome == Outcome.PASS
            and (verdict.check_type.value != "judge" or verdict.judge_mode == JudgeMode.NORMAL)
        )
        self._harness_variants.log_skill_action_trace(
            task_id=payload.session_id,
            role=role,
            skill_name="immune_system",
            action_name=verdict.check_type.value,
            intent_goal=(
                f"Run immune {verdict.check_type.value} validation for "
                f"{payload.skill_name}.{payload.tool_name}."
            ),
            action_payload={
                "payload": payload.__dict__,
                "verdict_id": verdict.verdict_id,
                "outcome": verdict.outcome.value,
                "block_reason": None if verdict.block_reason is None else verdict.block_reason.value,
                "block_detail": verdict.block_detail,
                "judge_mode": None if verdict.judge_mode is None else verdict.judge_mode.value,
            },
            context_assembled=context_assembled,
            retrieval_queries=None,
            judge_verdict="PASS" if verdict.outcome == Outcome.PASS else "FAIL",
            judge_reasoning=verdict.block_detail or "Immune validation passed.",
            training_eligible=is_training_eligible,
            retention_class="STANDARD" if is_training_eligible else "FAILURE_AUDIT",
            outcome_score=1.0 if is_training_eligible else 0.0,
            duration_ms=int(latency_ms),
        )

    def _verdict_to_row(self, verdict: ImmuneVerdict, latency_ms: float) -> tuple:
        return (
            verdict.verdict_id,
            "sheriff_input" if verdict.check_type.value == "sheriff" else "judge_output",
            verdict.tier.value,
            verdict.session_id,
            verdict.skill_name,
            verdict.task_type,
            verdict.outcome.value,
            verdict.block_reason.value if verdict.block_reason else verdict.block_detail,
            int(latency_ms),
            verdict.judge_mode.value,
            datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
        )


_SKILL: Optional[ImmuneSystemSkill] = None


def configure_skill(verdict_buffer: Optional[AppendBuffer] = None, immune_db_path: str | None = None):
    global _SKILL
    _SKILL = ImmuneSystemSkill(verdict_buffer=verdict_buffer, immune_db_path=immune_db_path)


def immune_system_entry(action: str, **kwargs):
    if _SKILL is None:
        configure_skill()
    assert _SKILL is not None
    if action == "sheriff":
        return _SKILL.check_sheriff(kwargs["payload"])
    if action == "judge":
        return _SKILL.check_judge(kwargs["payload"])
    raise ValueError(f"Unknown action: {action}")
