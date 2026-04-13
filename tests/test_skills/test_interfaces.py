from __future__ import annotations

import pytest

from skills.hermes_interfaces import MockHermesRuntime


@pytest.mark.parametrize("tool", ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"])
def test_registry_register_and_list(tool):
    rt = MockHermesRuntime()
    rt.register_skill(tool, lambda **kwargs: kwargs, {"x": 1})
    assert tool in rt.list_tools()


@pytest.mark.parametrize("tool", ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa"])
def test_invoke_registered_tool(tool):
    rt = MockHermesRuntime()
    rt.register_skill(tool, lambda **kwargs: {"ok": kwargs.get("v")}, {})
    out = rt.invoke_tool(tool, {"v": 1})
    assert out.success is True
    assert out.output["ok"] == 1


@pytest.mark.parametrize("name", ["x1", "x2", "x3", "x4", "x5"])
def test_delegate_parallel_mock(name):
    rt = MockHermesRuntime()
    rt.set_mock_response(f"delegate:{name}", f"resp:{name}")
    results = rt.delegate_parallel([(name, "s", "u")])
    assert results[0].output == f"resp:{name}"


@pytest.mark.parametrize("name", ["s1", "s2", "s3", "s4", "s5"])
def test_delegate_sequential_mock(name):
    rt = MockHermesRuntime()
    rt.set_mock_response(f"delegate:{name}", f"resp:{name}")
    result = rt.delegate_sequential(name, "sys", "usr")
    assert result.output == f"resp:{name}"
