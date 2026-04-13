from __future__ import annotations

import pytest

from council.types import RoleName
from skills.hermes_dispatcher import HermesSubagentDispatcher
from skills.hermes_interfaces import MockHermesRuntime


def test_dispatch_parallel_sends_three_tasks():
    rt = MockHermesRuntime()
    d = HermesSubagentDispatcher(rt)
    prompts = [
        (RoleName.STRATEGIST, "s1", "u1"),
        (RoleName.CRITIC, "s2", "u2"),
        (RoleName.REALIST, "s3", "u3"),
    ]
    out = d.dispatch_parallel(prompts)
    assert len(out) == 3
    assert rt.delegate_calls[-1]["mode"] == "parallel"


def test_dispatch_parallel_more_than_three_raises():
    rt = MockHermesRuntime()
    d = HermesSubagentDispatcher(rt)
    prompts = [
        (RoleName.STRATEGIST, "s1", "u1"),
        (RoleName.CRITIC, "s2", "u2"),
        (RoleName.REALIST, "s3", "u3"),
        (RoleName.DEVILS_ADVOCATE, "s4", "u4"),
    ]
    with pytest.raises(ValueError):
        d.dispatch_parallel(prompts)


def test_dispatch_parallel_failure_raises_runtime_error():
    rt = MockHermesRuntime()
    rt.set_mock_response("delegate:critic", Exception("boom"))
    d = HermesSubagentDispatcher(rt)
    prompts = [
        (RoleName.STRATEGIST, "s1", "u1"),
        (RoleName.CRITIC, "s2", "u2"),
        (RoleName.REALIST, "s3", "u3"),
    ]
    with pytest.raises(RuntimeError):
        d.dispatch_parallel(prompts)


def test_dispatch_sequential_sends_one_task():
    rt = MockHermesRuntime()
    d = HermesSubagentDispatcher(rt)
    out = d.dispatch_sequential(RoleName.DEVILS_ADVOCATE, "sys", "usr")
    assert out.role == RoleName.DEVILS_ADVOCATE


def test_dispatch_synthesis_returns_raw_string():
    rt = MockHermesRuntime()
    rt.set_mock_response("delegate:synthesis", "{\"ok\":true}")
    d = HermesSubagentDispatcher(rt)
    raw = d.dispatch_synthesis("sys", "usr")
    assert raw == "{\"ok\":true}"


@pytest.mark.parametrize("role", [RoleName.STRATEGIST, RoleName.CRITIC, RoleName.REALIST])
def test_role_isolation(role):
    rt = MockHermesRuntime()
    rt.set_mock_response(f"delegate:{role.value}", f"isolation:{role.value}")
    d = HermesSubagentDispatcher(rt)
    out = d.dispatch_parallel([(role, "sys", "user")])
    assert out[0].content == f"isolation:{role.value}"
