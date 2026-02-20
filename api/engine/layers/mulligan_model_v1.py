from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List


MULLIGAN_MODEL_V1_VERSION = "mulligan_model_v1"
_CHECKPOINTS: tuple[int, ...] = (7, 9, 10, 12)


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _round6_half_up(value: float) -> float:
    return float(Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _clamp_to_deck_size(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 99.0:
        return 99.0
    return float(value)


def _coerce_checkpoint_value(raw: Any) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return 0.0
    return float(raw)


def _base_payload(
    *,
    status: str,
    reason_code: str | None,
    assumptions_version: str | None,
    format_token: str,
) -> Dict[str, Any]:
    return {
        "version": MULLIGAN_MODEL_V1_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": [],
        "assumptions_version": assumptions_version,
        "format": format_token,
        "default_policy": None,
        "checkpoints": list(_CHECKPOINTS),
        "policy_effective_n": [],
    }


def run_mulligan_model_v1(format: Any, mulligan_assumptions_payload: Any) -> Dict[str, Any]:
    format_token = _nonempty_str(format) or ""

    if not isinstance(mulligan_assumptions_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="MULLIGAN_ASSUMPTIONS_UNAVAILABLE",
            assumptions_version=None,
            format_token=format_token,
        )

    assumptions_version = _nonempty_str(mulligan_assumptions_payload.get("version"))
    format_defaults = mulligan_assumptions_payload.get("format_defaults")
    if not isinstance(format_defaults, dict):
        return _base_payload(
            status="SKIP",
            reason_code="MULLIGAN_ASSUMPTIONS_UNAVAILABLE",
            assumptions_version=assumptions_version,
            format_token=format_token,
        )

    format_entry = format_defaults.get(format_token)
    if not isinstance(format_entry, dict):
        format_entry = format_defaults.get(format_token.lower()) if isinstance(format_token, str) else None

    if not isinstance(format_entry, dict):
        return _base_payload(
            status="SKIP",
            reason_code="FORMAT_ASSUMPTIONS_UNAVAILABLE",
            assumptions_version=assumptions_version,
            format_token=format_token,
        )

    default_policy = _nonempty_str(format_entry.get("default_policy"))
    policies_raw = format_entry.get("policies")
    if not isinstance(policies_raw, dict):
        policies_raw = {}

    policy_effective_n: List[Dict[str, Any]] = []
    for policy_key_raw in sorted(policies_raw.keys(), key=lambda item: str(item)):
        policy_key = _nonempty_str(policy_key_raw)
        if policy_key is None:
            continue

        policy_payload = policies_raw.get(policy_key_raw)
        if not isinstance(policy_payload, dict):
            continue

        checkpoint_values = policy_payload.get("effective_n_by_checkpoint")
        if not isinstance(checkpoint_values, dict):
            checkpoint_values = {}

        effective_n_rows: List[Dict[str, Any]] = []
        for checkpoint in _CHECKPOINTS:
            raw_value = checkpoint_values.get(checkpoint)
            if raw_value is None:
                raw_value = checkpoint_values.get(str(checkpoint))
            coerced = _coerce_checkpoint_value(raw_value)
            rounded = _round6_half_up(_clamp_to_deck_size(coerced))
            effective_n_rows.append(
                {
                    "checkpoint": int(checkpoint),
                    "effective_n": rounded,
                }
            )

        policy_effective_n.append(
            {
                "policy": policy_key,
                "effective_n_by_checkpoint": effective_n_rows,
            }
        )

    return {
        "version": MULLIGAN_MODEL_V1_VERSION,
        "status": "OK",
        "reason_code": None,
        "codes": [],
        "assumptions_version": assumptions_version,
        "format": format_token,
        "default_policy": default_policy,
        "checkpoints": list(_CHECKPOINTS),
        "policy_effective_n": policy_effective_n,
    }
