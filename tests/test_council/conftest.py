import json

from council.context_budget import build_context_packet
from council.types import DecisionType, RoleName, RoleOutput


def make_context(decision_type=DecisionType.OPPORTUNITY_SCREEN):
    return build_context_packet(decision_type, "00000000-0000-7000-8000-000000000001", "Opportunity context with execution details.")


def make_role_output(role: RoleName, marker: str):
    return RoleOutput(role=role, content=json.dumps({"role": role.value, "marker": marker}), token_count=20, max_tokens=200)
