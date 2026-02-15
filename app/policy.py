import json
import os
from typing import Any, Dict, List

from .config import Settings


class PolicyEngine:
    def __init__(self, policy_doc: Dict[str, Any]):
        self.policy = policy_doc or {}

    @staticmethod
    def _normalize_ops(ops: List[str]) -> set[str]:
        return {op.upper().strip() for op in ops if isinstance(op, str)}

    def _get_rules_for_principal(self, principal: str) -> Dict[str, Any]:
        principal_l = principal.lower().strip()
        principals = self.policy.get("principals", {})
        if principal_l in principals:
            return principals[principal_l]
        return self.policy.get("default", {})

    def assert_allowed(self, principal: str, operation: str, dataset: str, table: str) -> None:
        rules = self._get_rules_for_principal(principal)
        allowed_ops = self._normalize_ops(rules.get("operations", []))

        op_u = operation.upper().strip()
        if op_u not in allowed_ops:
            raise PermissionError(f"Operation '{op_u}' is not allowed for principal '{principal}'")

        ds_rules: Dict[str, List[str]] = rules.get("datasets", {})
        tables = ds_rules.get(dataset)
        if tables is None:
            raise PermissionError(f"Dataset '{dataset}' is not allowed for principal '{principal}'")

        if "*" in tables:
            return

        if table not in tables:
            raise PermissionError(
                f"Table '{dataset}.{table}' is not allowed for principal '{principal}'"
            )


def load_policy(settings: Settings) -> PolicyEngine:
    raw = settings.policy_json.strip()

    if raw.startswith("{"):
        policy_doc = json.loads(raw)
    else:
        if not os.path.exists(raw):
            raise ValueError(
                "POLICY_JSON must be a JSON string or path to JSON file."
            )
        with open(raw, "r", encoding="utf-8") as f:
            policy_doc = json.load(f)

    return PolicyEngine(policy_doc)
