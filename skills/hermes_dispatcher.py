from __future__ import annotations

import json
from typing import List, Tuple

from council.orchestrator import MixtureDispatcher, SubagentDispatcher
from council.types import RoleName, RoleOutput

from skills.hermes_interfaces import HermesDelegateAPI, HermesMixtureAPI, HermesToolResult


ISOLATION_CANARY_KEY = "subagent_isolation_test"


class HermesSubagentDispatcher(SubagentDispatcher):
    def __init__(self, delegate_api: HermesDelegateAPI):
        self._delegate = delegate_api

    def dispatch_parallel(self, prompts: List[Tuple[RoleName, str, str]]) -> List[RoleOutput]:
        if len(prompts) > 3:
            raise ValueError("Hermes delegate_tool max concurrency is 3")
        tasks = [(role.value, system_prompt, user_prompt) for role, system_prompt, user_prompt in prompts]
        results: List[HermesToolResult] = self._delegate.delegate_parallel(tasks, max_concurrency=3)

        outputs: List[RoleOutput] = []
        for (role, _, _), result in zip(prompts, results):
            if not result.success:
                raise RuntimeError(f"Council subagent {role.value} failed: {result.error}")
            content = str(result.output)
            outputs.append(RoleOutput(role=role, content=content, token_count=len(content.split()), max_tokens=200 if role != RoleName.DEVILS_ADVOCATE else 150))
        return outputs

    def dispatch_sequential(self, role: RoleName, system_prompt: str, user_prompt: str) -> RoleOutput:
        result = self._delegate.delegate_sequential(role.value, system_prompt, user_prompt)
        if not result.success:
            raise RuntimeError(f"Council subagent {role.value} failed: {result.error}")
        content = str(result.output)
        return RoleOutput(role=role, content=content, token_count=len(content.split()), max_tokens=150)

    def dispatch_synthesis(self, system_prompt: str, user_prompt: str) -> str:
        result = self._delegate.delegate_sequential("synthesis", system_prompt, user_prompt)
        if not result.success:
            raise RuntimeError(f"Council synthesis failed: {result.error}")
        return str(result.output)


class HermesMixtureDispatcher(MixtureDispatcher):
    def __init__(self, mixture_api: HermesMixtureAPI):
        self._mixture = mixture_api

    def dispatch_mixture(self, prompt: str, models: List[str], rounds: int = 3) -> str:
        result: HermesToolResult = self._mixture.mixture_deliberate(prompt, models, rounds=rounds)
        if not result.success:
            raise RuntimeError(f"Council Tier 2 mixture failed: {result.error}")
        if isinstance(result.output, (dict, list)):
            return json.dumps(result.output)
        return str(result.output)


def run_subagent_isolation_canary(delegate_api: HermesDelegateAPI) -> dict:
    """Run the §3.1.5 Hermes delegate isolation canary."""
    markers = {"A": "MARKER_A", "B": "MARKER_B", "C": "MARKER_C"}
    parallel_tasks = [
        (
            f"isolation_{name}",
            "You are an isolation canary subagent. Return only visible marker values.",
            (
                f"Write {marker} to shared namespace key {ISOLATION_CANARY_KEY}. "
                f"Then read {ISOLATION_CANARY_KEY} and return all marker values you can see."
            ),
        )
        for name, marker in markers.items()
    ]
    checks: dict[str, bool] = {}
    details: list[str] = []
    try:
        parallel = delegate_api.delegate_parallel(parallel_tasks, max_concurrency=3)
        for name, marker in markers.items():
            result = parallel[ord(name) - ord("A")]
            text = str(result.output)
            other_markers = set(markers.values()) - {marker}
            ok = result.success and marker in text and not any(other in text for other in other_markers)
            checks[f"parallel_{name}"] = ok
            if not ok:
                details.append(f"parallel {name} saw unexpected marker set: {text}")

        sequential = delegate_api.delegate_sequential(
            "isolation_D",
            "You are an isolation canary subagent. Return only visible marker values.",
            f"Write MARKER_D to {ISOLATION_CANARY_KEY}. Then read {ISOLATION_CANARY_KEY} and return all values.",
        )
        sequential_text = str(sequential.output)
        sequential_ok = (
            sequential.success
            and "MARKER_D" in sequential_text
            and not any(marker in sequential_text for marker in markers.values())
        )
        checks["sequential_D"] = sequential_ok
        if not sequential_ok:
            details.append(f"sequential D saw leaked markers: {sequential_text}")

        memory = delegate_api.delegate_sequential(
            "isolation_memory",
            "You are an isolation canary subagent. Inspect only your own MEMORY.md.",
            "Read your MEMORY.md. Return whether it contains PARENT_CONTEXT and include the visible contents.",
        )
        memory_text = str(memory.output)
        memory_ok = memory.success and "PARENT_CONTEXT" not in memory_text
        checks["parent_memory"] = memory_ok
        if not memory_ok:
            details.append("parent MEMORY.md leaked into delegated subagent")
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "checks": checks, "details": [f"canary_error:{type(exc).__name__}:{exc}"]}

    return {"ok": all(checks.values()), "checks": checks, "details": details}
