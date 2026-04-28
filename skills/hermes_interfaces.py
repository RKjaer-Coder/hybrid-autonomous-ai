from __future__ import annotations

import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class HermesToolResult:
    """Result from a Hermes tool invocation."""

    tool_name: str
    success: bool
    output: Any
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass(frozen=True)
class HermesSessionContext:
    """Hermes session context available to skills."""

    session_id: str
    profile_name: str
    model_name: str
    jwt_claims: Dict[str, Any]
    data_dir: str


class HermesToolRegistry(ABC):
    @abstractmethod
    def register_skill(self, name: str, entry_point: callable, manifest: dict) -> None:
        ...

    @abstractmethod
    def invoke_tool(self, tool_name: str, arguments: dict) -> HermesToolResult:
        ...

    @abstractmethod
    def list_tools(self) -> List[str]:
        ...


class HermesDelegateAPI(ABC):
    @abstractmethod
    def delegate_parallel(
        self,
        tasks: List[Tuple[str, str, str]],
        max_concurrency: int = 3,
    ) -> List[HermesToolResult]:
        ...

    @abstractmethod
    def delegate_sequential(self, name: str, system_prompt: str, user_prompt: str) -> HermesToolResult:
        ...


class HermesMixtureAPI(ABC):
    @abstractmethod
    def mixture_deliberate(self, prompt: str, models: List[str], rounds: int = 3) -> HermesToolResult:
        ...


class HermesMessagingAPI(ABC):
    @abstractmethod
    def send_alert(self, channel: str, message: str, severity: str) -> bool:
        ...


class HermesCronAPI(ABC):
    @abstractmethod
    def schedule_job(self, name: str, skill_name: str, cron_expr: str, model: Optional[str] = None) -> str:
        ...

    @abstractmethod
    def cancel_job(self, job_id: str) -> bool:
        ...


class MockHermesRuntime(
    HermesToolRegistry,
    HermesDelegateAPI,
    HermesMixtureAPI,
    HermesMessagingAPI,
    HermesCronAPI,
):
    """Complete mock Hermes runtime used by tests."""

    def __init__(self, data_dir: str = "/tmp/hermes-test-data"):
        self.data_dir = data_dir
        self.registered_skills: Dict[str, Any] = {}
        self.tool_invocations: List[Dict[str, Any]] = []
        self.delegate_calls: List[Dict[str, Any]] = []
        self.alerts_sent: List[Dict[str, Any]] = []
        self.scheduled_jobs: Dict[str, Dict[str, Any]] = {}
        self._mock_responses: Dict[str, Any] = {}

    def set_mock_response(self, tool_name: str, response: Any):
        self._mock_responses[tool_name] = response

    def register_skill(self, name: str, entry_point: callable, manifest: dict) -> None:
        self.registered_skills[name] = {"entry_point": entry_point, "manifest": manifest}

    def invoke_tool(self, tool_name: str, arguments: dict) -> HermesToolResult:
        start = time.monotonic_ns()
        if tool_name in self._mock_responses:
            output = self._mock_responses[tool_name]
            self.tool_invocations.append({"tool_name": tool_name, "arguments": arguments, "mocked": True})
            return HermesToolResult(tool_name=tool_name, success=True, output=output, duration_ms=(time.monotonic_ns() - start) / 1_000_000)
        item = self.registered_skills.get(tool_name)
        if not item:
            return HermesToolResult(tool_name=tool_name, success=False, output=None, error="tool not found")
        try:
            output = item["entry_point"](**arguments)
            self.tool_invocations.append({"tool_name": tool_name, "arguments": arguments, "mocked": False})
            return HermesToolResult(tool_name=tool_name, success=True, output=output, duration_ms=(time.monotonic_ns() - start) / 1_000_000)
        except Exception as exc:  # noqa: BLE001
            return HermesToolResult(tool_name=tool_name, success=False, output=None, error=str(exc), duration_ms=(time.monotonic_ns() - start) / 1_000_000)

    def list_tools(self) -> List[str]:
        return sorted(self.registered_skills.keys())

    def delegate_parallel(self, tasks: List[Tuple[str, str, str]], max_concurrency: int = 3) -> List[HermesToolResult]:
        self.delegate_calls.append({"mode": "parallel", "tasks": tasks, "max_concurrency": max_concurrency})
        results: List[HermesToolResult] = []
        for name, system_prompt, user_prompt in tasks:
            default = _default_delegate_response(name, system_prompt, user_prompt)
            mock = self._mock_responses.get(f"delegate:{name}", default)
            if isinstance(mock, Exception):
                results.append(HermesToolResult(tool_name=name, success=False, output=None, error=str(mock)))
            else:
                results.append(HermesToolResult(tool_name=name, success=True, output=mock))
        return results

    def delegate_sequential(self, name: str, system_prompt: str, user_prompt: str) -> HermesToolResult:
        self.delegate_calls.append({"mode": "sequential", "name": name, "system_prompt": system_prompt, "user_prompt": user_prompt})
        mock = self._mock_responses.get(f"delegate:{name}", _default_delegate_response(name, system_prompt, user_prompt))
        if isinstance(mock, Exception):
            return HermesToolResult(tool_name=name, success=False, output=None, error=str(mock))
        return HermesToolResult(tool_name=name, success=True, output=mock)

    def mixture_deliberate(self, prompt: str, models: List[str], rounds: int = 3) -> HermesToolResult:
        out = self._mock_responses.get("mixture", {"prompt": prompt, "models": models, "rounds": rounds})
        return HermesToolResult(tool_name="mixture_of_agents", success=True, output=out)

    def send_alert(self, channel: str, message: str, severity: str) -> bool:
        self.alerts_sent.append({"channel": channel, "message": message, "severity": severity})
        return True

    def schedule_job(self, name: str, skill_name: str, cron_expr: str, model: Optional[str] = None) -> str:
        job_id = f"job-{uuid.uuid4()}"
        self.scheduled_jobs[job_id] = {
            "name": name,
            "skill_name": skill_name,
            "cron_expr": cron_expr,
            "model": model,
        }
        return job_id

    def cancel_job(self, job_id: str) -> bool:
        return self.scheduled_jobs.pop(job_id, None) is not None


def _default_delegate_response(name: str, system_prompt: str, user_prompt: str) -> str:
    if name.startswith("isolation_"):
        suffix = name.rsplit("_", 1)[-1]
        if suffix in {"A", "B", "C", "D"}:
            return f"MARKER_{suffix}"
        if suffix == "memory":
            return "MEMORY.md is empty for this isolated subagent."
    return f"{name}::{len(system_prompt)}::{len(user_prompt)}"
