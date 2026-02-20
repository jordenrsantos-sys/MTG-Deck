from __future__ import annotations

import os
from typing import List

import api.engine.constants as engine_constants


_TRUTHY_VALUES = {"1", "true", "yes", "on"}


def _env_truthy(var_name: str) -> bool:
    raw = os.getenv(var_name)
    if not isinstance(raw, str):
        return False
    return raw.strip().lower() in _TRUTHY_VALUES


def assert_runtime_safe_mode() -> None:
    violations: List[str] = []

    if _env_truthy("ENGINE_ENABLE_PROOF"):
        violations.append("ENGINE_ENABLE_PROOF")

    if _env_truthy("ENGINE_ENABLE_ORACLE_RUNTIME"):
        violations.append("ENGINE_ENABLE_ORACLE_RUNTIME")

    if _env_truthy("ENGINE_ALLOW_RUNTIME_ORACLE_TEXT"):
        violations.append("ENGINE_ALLOW_RUNTIME_ORACLE_TEXT_ENV")

    if getattr(engine_constants, "ENGINE_ALLOW_RUNTIME_ORACLE_TEXT", False) is True:
        violations.append("ENGINE_ALLOW_RUNTIME_ORACLE_TEXT_CONST")

    if len(violations) > 0:
        raise RuntimeError(
            "RUNTIME_SAFE_MODE_VIOLATION: "
            + ",".join(sorted(set(str(v) for v in violations if isinstance(v, str))))
        )
