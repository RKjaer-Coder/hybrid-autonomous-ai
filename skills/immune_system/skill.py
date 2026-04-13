from __future__ import annotations

import datetime
import time
from typing import Optional

from immune.config import load_config
from immune.judge import judge_check
from immune.sheriff import sheriff_check
from immune.types import ImmuneVerdict, JudgePayload, SheriffPayload
from skills.append_buffer import AppendBuffer


class ImmuneSystemSkill:
    def __init__(self, verdict_buffer: Optional[AppendBuffer] = None):
        self._config = load_config()
        self._buffer = verdict_buffer

    def check_sheriff(self, payload: SheriffPayload) -> ImmuneVerdict:
        start = time.monotonic_ns()
        verdict = sheriff_check(payload, self._config)
        latency_ms = (time.monotonic_ns() - start) / 1_000_000
        if self._buffer:
            self._buffer.append(self._verdict_to_row(verdict, latency_ms))
        return verdict

    def check_judge(self, payload: JudgePayload) -> ImmuneVerdict:
        start = time.monotonic_ns()
        verdict = judge_check(payload, self._config)
        latency_ms = (time.monotonic_ns() - start) / 1_000_000
        if self._buffer:
            self._buffer.append(self._verdict_to_row(verdict, latency_ms))
        return verdict

    def _verdict_to_row(self, verdict: ImmuneVerdict, latency_ms: float) -> tuple:
        return (
            verdict.verdict_id,
            "sheriff_input" if verdict.check_type.value == "sheriff" else "judge_output",
            verdict.tier.value,
            verdict.session_id,
            verdict.skill_name,
            verdict.outcome.value,
            verdict.block_reason.value if verdict.block_reason else None,
            int(latency_ms),
            datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
        )


_SKILL: Optional[ImmuneSystemSkill] = None


def configure_skill(verdict_buffer: Optional[AppendBuffer] = None):
    global _SKILL
    _SKILL = ImmuneSystemSkill(verdict_buffer=verdict_buffer)


def immune_system_entry(action: str, **kwargs):
    if _SKILL is None:
        configure_skill()
    assert _SKILL is not None
    if action == "sheriff":
        return _SKILL.check_sheriff(kwargs["payload"])
    if action == "judge":
        return _SKILL.check_judge(kwargs["payload"])
    raise ValueError(f"Unknown action: {action}")
