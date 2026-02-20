from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


_MULLIGAN_ASSUMPTIONS_FILE = (
    Path(__file__).resolve().parent
    / "data"
    / "sufficiency"
    / "mulligan_assumptions_v1.json"
)
_REQUIRED_POLICIES = ("DRAW10_SHUFFLE3", "FRIENDLY", "NORMAL")
_REQUIRED_CHECKPOINTS = (7, 9, 10, 12)


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _coerce_checkpoint_key(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, str):
        token = value.strip()
        if token.isdigit():
            return int(token)
    return None


def _coerce_numeric(value: Any, *, field_path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _runtime_error("MULLIGAN_ASSUMPTIONS_V1_INVALID", f"{field_path} must be numeric")
    return float(value)


def _normalize_effective_n_by_checkpoint(raw: Any, *, field_path: str) -> Dict[int, float]:
    if not isinstance(raw, dict):
        raise _runtime_error("MULLIGAN_ASSUMPTIONS_V1_INVALID", f"{field_path} must be an object")

    by_checkpoint: Dict[int, float] = {}
    for checkpoint_key_raw in raw.keys():
        checkpoint = _coerce_checkpoint_key(checkpoint_key_raw)
        if checkpoint is None:
            raise _runtime_error(
                "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                f"{field_path} has non-numeric checkpoint key: {checkpoint_key_raw!r}",
            )

        if checkpoint in by_checkpoint:
            raise _runtime_error(
                "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                f"{field_path} has duplicate checkpoint key: {checkpoint}",
            )

        by_checkpoint[checkpoint] = _coerce_numeric(
            raw.get(checkpoint_key_raw),
            field_path=f"{field_path}.{checkpoint}",
        )

    required = set(_REQUIRED_CHECKPOINTS)
    actual = set(by_checkpoint.keys())
    if actual != required:
        missing = sorted(required.difference(actual))
        unexpected = sorted(actual.difference(required))
        raise _runtime_error(
            "MULLIGAN_ASSUMPTIONS_V1_INVALID",
            (
                f"{field_path} checkpoints must be exactly {_REQUIRED_CHECKPOINTS}; "
                f"missing={missing}, unexpected={unexpected}"
            ),
        )

    return {checkpoint: float(by_checkpoint[checkpoint]) for checkpoint in _REQUIRED_CHECKPOINTS}


def _normalize_policy_payload(raw: Any, *, field_path: str) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("MULLIGAN_ASSUMPTIONS_V1_INVALID", f"{field_path} must be an object")

    if "effective_n_by_checkpoint" not in raw:
        raise _runtime_error(
            "MULLIGAN_ASSUMPTIONS_V1_INVALID",
            f"{field_path} must include effective_n_by_checkpoint",
        )

    return {
        "effective_n_by_checkpoint": _normalize_effective_n_by_checkpoint(
            raw.get("effective_n_by_checkpoint"),
            field_path=f"{field_path}.effective_n_by_checkpoint",
        )
    }


def _normalize_format_defaults(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("MULLIGAN_ASSUMPTIONS_V1_INVALID", "format_defaults must be an object")

    normalized: Dict[str, Any] = {}
    for format_key_raw in sorted(raw.keys(), key=lambda item: str(item)):
        format_key = _nonempty_str(format_key_raw)
        if format_key is None:
            raise _runtime_error(
                "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                "format_defaults keys must be non-empty strings",
            )

        format_payload = raw.get(format_key_raw)
        if not isinstance(format_payload, dict):
            raise _runtime_error(
                "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                f"format_defaults.{format_key} must be an object",
            )

        default_policy = _nonempty_str(format_payload.get("default_policy"))
        if default_policy is None:
            raise _runtime_error(
                "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                f"format_defaults.{format_key}.default_policy must be a non-empty string",
            )

        policies_raw = format_payload.get("policies")
        if not isinstance(policies_raw, dict):
            raise _runtime_error(
                "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                f"format_defaults.{format_key}.policies must be an object",
            )

        policy_key_map: Dict[str, Any] = {}
        for policy_key_raw in policies_raw.keys():
            policy_key = _nonempty_str(policy_key_raw)
            if policy_key is None:
                raise _runtime_error(
                    "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                    f"format_defaults.{format_key}.policies keys must be non-empty strings",
                )
            if policy_key in policy_key_map:
                raise _runtime_error(
                    "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                    f"format_defaults.{format_key}.policies has duplicate key {policy_key}",
                )
            policy_key_map[policy_key] = policy_key_raw

        required_policies = set(_REQUIRED_POLICIES)
        actual_policies = set(policy_key_map.keys())
        if actual_policies != required_policies:
            missing = sorted(required_policies.difference(actual_policies))
            unexpected = sorted(actual_policies.difference(required_policies))
            raise _runtime_error(
                "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                (
                    f"format_defaults.{format_key}.policies must be exactly {_REQUIRED_POLICIES}; "
                    f"missing={missing}, unexpected={unexpected}"
                ),
            )

        if default_policy not in required_policies:
            raise _runtime_error(
                "MULLIGAN_ASSUMPTIONS_V1_INVALID",
                f"format_defaults.{format_key}.default_policy must be one of {_REQUIRED_POLICIES}",
            )

        normalized_policies = {
            policy_key: _normalize_policy_payload(
                policies_raw.get(policy_key_map[policy_key]),
                field_path=f"format_defaults.{format_key}.policies.{policy_key}",
            )
            for policy_key in sorted(required_policies)
        }

        normalized[format_key] = {
            "default_policy": default_policy,
            "policies": normalized_policies,
        }

    return normalized


def load_mulligan_assumptions_v1() -> Dict[str, Any]:
    if not _MULLIGAN_ASSUMPTIONS_FILE.is_file():
        raise _runtime_error("MULLIGAN_ASSUMPTIONS_V1_MISSING", str(_MULLIGAN_ASSUMPTIONS_FILE))

    try:
        parsed = json.loads(_MULLIGAN_ASSUMPTIONS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("MULLIGAN_ASSUMPTIONS_V1_INVALID_JSON", str(_MULLIGAN_ASSUMPTIONS_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("MULLIGAN_ASSUMPTIONS_V1_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version is None:
        raise _runtime_error("MULLIGAN_ASSUMPTIONS_V1_INVALID", "version must be a non-empty string")

    return {
        "version": version,
        "format_defaults": _normalize_format_defaults(parsed.get("format_defaults")),
    }
