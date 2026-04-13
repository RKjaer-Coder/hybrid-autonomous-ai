from __future__ import annotations

from typing import List, Tuple

from council.orchestrator import SubagentDispatcher
from council.types import RoleName, RoleOutput

from skills.hermes_interfaces import HermesDelegateAPI, HermesToolResult


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
