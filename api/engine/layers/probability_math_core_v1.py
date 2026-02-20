from __future__ import annotations

from math import floor
from typing import Any, Dict, List, Set

from api.engine.probability_math_core_v1 import (
    PROBABILITY_MATH_CORE_V1_VERSION,
    comb,
    hypergeom_p_ge_1,
    hypergeom_p_ge_x,
)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _available_functions() -> List[str]:
    return sorted(
        [
            "comb",
            "hypergeom_p_ge_1",
            "hypergeom_p_ge_x",
        ]
    )


def _clamp_to_deck_size(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 99.0:
        return 99.0
    return float(value)


def _base_payload(*, status: str, reason_code: str | None, codes: List[str], validated_buckets: int) -> Dict[str, Any]:
    return {
        "version": PROBABILITY_MATH_CORE_V1_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": sorted(set(codes)),
        "math_backend": "int_comb",
        "available_functions": _available_functions(),
        "validated_buckets": int(validated_buckets),
    }


def run_probability_math_core_v1(*, substitution_engine_v1_payload: Any) -> Dict[str, Any]:
    if not isinstance(substitution_engine_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE",
            codes=[],
            validated_buckets=0,
        )

    buckets_raw = substitution_engine_v1_payload.get("buckets")
    if not isinstance(buckets_raw, list):
        return _base_payload(
            status="SKIP",
            reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE",
            codes=[],
            validated_buckets=0,
        )

    rows = [row for row in buckets_raw if isinstance(row, dict)]
    if len(rows) == 0:
        return _base_payload(
            status="SKIP",
            reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE",
            codes=[],
            validated_buckets=0,
        )

    codes: Set[str] = set()
    validated_buckets = 0

    sorted_rows = sorted(
        rows,
        key=lambda row: str((row or {}).get("bucket") or ""),
    )

    for row in sorted_rows:
        effective_k_raw = row.get("effective_K")
        k_int_raw = row.get("K_int")

        effective_k_valid = _is_number(effective_k_raw)
        k_int_valid = isinstance(k_int_raw, int) and not isinstance(k_int_raw, bool)

        if not effective_k_valid:
            codes.add("PROBABILITY_MATH_BUCKET_EFFECTIVE_K_INVALID")
        if not k_int_valid:
            codes.add("PROBABILITY_MATH_BUCKET_K_INT_INVALID")

        if not (effective_k_valid and k_int_valid):
            continue

        expected_k_int = int(floor(_clamp_to_deck_size(float(effective_k_raw))))
        if int(k_int_raw) != expected_k_int:
            codes.add("PROBABILITY_MATH_K_INT_POLICY_VIOLATION")
            continue

        validated_buckets += 1

    try:
        if comb(5, 2) != 10:
            codes.add("PROBABILITY_MATH_RUNTIME_ERROR")
        if hypergeom_p_ge_1(99, 0, 7) != 0.0:
            codes.add("PROBABILITY_MATH_RUNTIME_ERROR")
        if hypergeom_p_ge_x(99, 5, 7, 0) != 1.0:
            codes.add("PROBABILITY_MATH_RUNTIME_ERROR")
    except RuntimeError:
        codes.add("PROBABILITY_MATH_RUNTIME_ERROR")

    if len(codes) > 0:
        return _base_payload(
            status="ERROR",
            reason_code=None,
            codes=sorted(codes),
            validated_buckets=validated_buckets,
        )

    return _base_payload(
        status="OK",
        reason_code=None,
        codes=[],
        validated_buckets=validated_buckets,
    )
