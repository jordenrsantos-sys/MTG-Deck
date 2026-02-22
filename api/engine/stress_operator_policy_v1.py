from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


_STRESS_OPERATOR_POLICY_FILE = (
    Path(__file__).resolve().parent
    / "data"
    / "sufficiency"
    / "stress_operator_policy_v1.json"
)


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _require_nonnegative_int(value: Any, *, field_path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _runtime_error("STRESS_OPERATOR_POLICY_V1_INVALID", f"{field_path} must be int")
    if value < 0:
        raise _runtime_error("STRESS_OPERATOR_POLICY_V1_INVALID", f"{field_path} must be >= 0")
    return int(value)


def _normalize_precedence(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        raise _runtime_error("STRESS_OPERATOR_POLICY_V1_INVALID", "precedence must be an array")

    values: List[str] = []
    seen: set[str] = set()
    for index, value in enumerate(raw):
        op = _nonempty_str(value)
        if op is None:
            raise _runtime_error(
                "STRESS_OPERATOR_POLICY_V1_INVALID",
                f"precedence[{index}] must be a non-empty string",
            )
        if op in seen:
            raise _runtime_error(
                "STRESS_OPERATOR_POLICY_V1_INVALID",
                f"precedence contains duplicate operator {op}",
            )
        seen.add(op)
        values.append(op)

    if len(values) == 0:
        raise _runtime_error("STRESS_OPERATOR_POLICY_V1_INVALID", "precedence must be non-empty")

    return values


def _normalize_default_by_turn(raw: Any, *, allowed_ops: set[str]) -> Dict[str, int]:
    if not isinstance(raw, dict):
        raise _runtime_error("STRESS_OPERATOR_POLICY_V1_INVALID", "default_by_turn must be an object")

    normalized: Dict[str, int] = {}
    for op_key_raw in sorted(raw.keys(), key=lambda item: str(item)):
        op_key = _nonempty_str(op_key_raw)
        if op_key is None:
            raise _runtime_error(
                "STRESS_OPERATOR_POLICY_V1_INVALID",
                "default_by_turn keys must be non-empty strings",
            )
        if op_key not in allowed_ops:
            raise _runtime_error(
                "STRESS_OPERATOR_POLICY_V1_INVALID",
                f"default_by_turn.{op_key} references unknown operator",
            )
        normalized[op_key] = _require_nonnegative_int(
            raw.get(op_key_raw),
            field_path=f"default_by_turn.{op_key}",
        )

    return normalized


def _normalize_composition(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("STRESS_OPERATOR_POLICY_V1_INVALID", "composition must be an object")

    expected_keys = {"mode", "record_impacts"}
    if set(raw.keys()) != expected_keys:
        raise _runtime_error(
            "STRESS_OPERATOR_POLICY_V1_INVALID",
            f"composition keys must be exactly {sorted(expected_keys)}",
        )

    mode = _nonempty_str(raw.get("mode"))
    if mode != "sequential":
        raise _runtime_error(
            "STRESS_OPERATOR_POLICY_V1_INVALID",
            "composition.mode must be 'sequential'",
        )

    record_impacts = raw.get("record_impacts")
    if not isinstance(record_impacts, bool):
        raise _runtime_error(
            "STRESS_OPERATOR_POLICY_V1_INVALID",
            "composition.record_impacts must be bool",
        )

    return {
        "mode": mode,
        "record_impacts": record_impacts,
    }


def load_stress_operator_policy_v1() -> Dict[str, Any]:
    if not _STRESS_OPERATOR_POLICY_FILE.is_file():
        raise _runtime_error("STRESS_OPERATOR_POLICY_V1_MISSING", str(_STRESS_OPERATOR_POLICY_FILE))

    try:
        parsed = json.loads(_STRESS_OPERATOR_POLICY_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error(
            "STRESS_OPERATOR_POLICY_V1_INVALID_JSON",
            str(_STRESS_OPERATOR_POLICY_FILE),
        ) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("STRESS_OPERATOR_POLICY_V1_INVALID", "root must be an object")

    expected_keys = {"version", "precedence", "tie_break", "default_by_turn", "composition"}
    if set(parsed.keys()) != expected_keys:
        raise _runtime_error(
            "STRESS_OPERATOR_POLICY_V1_INVALID",
            f"root keys must be exactly {sorted(expected_keys)}",
        )

    version = _nonempty_str(parsed.get("version"))
    if version is None:
        raise _runtime_error("STRESS_OPERATOR_POLICY_V1_INVALID", "version must be a non-empty string")

    tie_break = _nonempty_str(parsed.get("tie_break"))
    if tie_break != "op_name_then_json":
        raise _runtime_error(
            "STRESS_OPERATOR_POLICY_V1_INVALID",
            "tie_break must be 'op_name_then_json'",
        )

    precedence = _normalize_precedence(parsed.get("precedence"))
    precedence_set = set(precedence)

    default_by_turn = _normalize_default_by_turn(
        parsed.get("default_by_turn"),
        allowed_ops=precedence_set,
    )
    composition = _normalize_composition(parsed.get("composition"))

    return {
        "version": version,
        "precedence": precedence,
        "tie_break": tie_break,
        "default_by_turn": default_by_turn,
        "composition": composition,
    }
