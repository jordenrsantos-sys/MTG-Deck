from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from math import floor
from typing import Any, Dict, List, Set

from api.engine.probability_math_core_v1 import hypergeom_p_ge_1


PROBABILITY_CHECKPOINT_LAYER_V1_VERSION = "probability_checkpoint_layer_v1"
_DECK_SIZE_N = 99
_CHECKPOINTS: tuple[int, ...] = (7, 9, 10, 12)

_ERROR_CODES = {
    "PROBABILITY_CHECKPOINT_DEFAULT_POLICY_UNAVAILABLE",
    "PROBABILITY_CHECKPOINT_POLICY_EFFECTIVE_N_INVALID",
    "PROBABILITY_CHECKPOINT_BUCKET_EFFECTIVE_K_INVALID",
    "PROBABILITY_CHECKPOINT_BUCKET_K_INT_INVALID",
    "PROBABILITY_CHECKPOINT_K_INT_POLICY_VIOLATION",
    "PROBABILITY_CHECKPOINT_MATH_RUNTIME_ERROR",
}
_WARN_CODES = {
    "PROBABILITY_CHECKPOINT_EFFECTIVE_N_FLOORED",
}


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _round6_half_up(value: float) -> float:
    return float(Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _clamp_to_deck_size(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > float(_DECK_SIZE_N):
        return float(_DECK_SIZE_N)
    return float(value)


def _base_payload(
    *,
    status: str,
    reason_code: str | None,
    codes: List[str],
    format_token: str,
    default_policy: str | None,
    checkpoint_draws: List[Dict[str, Any]],
    probabilities_by_bucket: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "version": PROBABILITY_CHECKPOINT_LAYER_V1_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": sorted(set(codes)),
        "format": format_token,
        "deck_size_N": _DECK_SIZE_N,
        "default_policy": default_policy,
        "checkpoints": list(_CHECKPOINTS),
        "checkpoint_draws": checkpoint_draws,
        "probabilities_by_bucket": probabilities_by_bucket,
    }


def _extract_checkpoint_draws(mulligan_model_v1_payload: Any, codes: Set[str]) -> tuple[str | None, List[Dict[str, Any]] | None]:
    if not isinstance(mulligan_model_v1_payload, dict):
        return None, None

    default_policy = _nonempty_str(mulligan_model_v1_payload.get("default_policy"))
    if default_policy is None:
        codes.add("PROBABILITY_CHECKPOINT_DEFAULT_POLICY_UNAVAILABLE")
        return None, []

    policy_rows_raw = mulligan_model_v1_payload.get("policy_effective_n")
    if not isinstance(policy_rows_raw, list):
        return default_policy, None

    selected_row = None
    for row in policy_rows_raw:
        if not isinstance(row, dict):
            continue
        policy_token = _nonempty_str(row.get("policy"))
        if policy_token == default_policy:
            selected_row = row
            break

    if not isinstance(selected_row, dict):
        codes.add("PROBABILITY_CHECKPOINT_DEFAULT_POLICY_UNAVAILABLE")
        return default_policy, []

    checkpoint_rows_raw = selected_row.get("effective_n_by_checkpoint")
    if not isinstance(checkpoint_rows_raw, list):
        return default_policy, None

    checkpoint_effective_n: Dict[int, Any] = {}
    for row in checkpoint_rows_raw:
        if not isinstance(row, dict):
            continue

        checkpoint_raw = row.get("checkpoint")
        checkpoint_value: int | None = None
        if isinstance(checkpoint_raw, int) and not isinstance(checkpoint_raw, bool):
            checkpoint_value = int(checkpoint_raw)
        elif isinstance(checkpoint_raw, str):
            token = checkpoint_raw.strip()
            if token.isdigit():
                checkpoint_value = int(token)

        if checkpoint_value not in _CHECKPOINTS:
            continue
        if checkpoint_value in checkpoint_effective_n:
            continue

        checkpoint_effective_n[checkpoint_value] = row.get("effective_n")

    if any(checkpoint not in checkpoint_effective_n for checkpoint in _CHECKPOINTS):
        return default_policy, None

    checkpoint_draws: List[Dict[str, Any]] = []
    for checkpoint in _CHECKPOINTS:
        effective_n_raw = checkpoint_effective_n.get(checkpoint)
        if not _is_number(effective_n_raw):
            codes.add("PROBABILITY_CHECKPOINT_POLICY_EFFECTIVE_N_INVALID")
            continue

        effective_n = _round6_half_up(_clamp_to_deck_size(float(effective_n_raw)))
        n_int = int(floor(effective_n))
        if float(n_int) != float(effective_n):
            codes.add("PROBABILITY_CHECKPOINT_EFFECTIVE_N_FLOORED")

        checkpoint_draws.append(
            {
                "checkpoint": int(checkpoint),
                "effective_n": float(effective_n),
                "n_int": int(n_int),
            }
        )

    return default_policy, checkpoint_draws


def run_probability_checkpoint_layer_v1(
    *,
    format: Any,
    substitution_engine_v1_payload: Any,
    mulligan_model_v1_payload: Any,
) -> Dict[str, Any]:
    format_token = _nonempty_str(format) or ""

    if not isinstance(substitution_engine_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            default_policy=None,
            checkpoint_draws=[],
            probabilities_by_bucket=[],
        )

    buckets_raw = substitution_engine_v1_payload.get("buckets")
    if not isinstance(buckets_raw, list):
        return _base_payload(
            status="SKIP",
            reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            default_policy=None,
            checkpoint_draws=[],
            probabilities_by_bucket=[],
        )

    rows = [row for row in buckets_raw if isinstance(row, dict)]
    if len(rows) == 0:
        return _base_payload(
            status="SKIP",
            reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            default_policy=None,
            checkpoint_draws=[],
            probabilities_by_bucket=[],
        )

    codes: Set[str] = set()
    default_policy, checkpoint_draws = _extract_checkpoint_draws(
        mulligan_model_v1_payload=mulligan_model_v1_payload,
        codes=codes,
    )

    if checkpoint_draws is None:
        return _base_payload(
            status="SKIP",
            reason_code="MULLIGAN_MODEL_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            default_policy=default_policy,
            checkpoint_draws=[],
            probabilities_by_bucket=[],
        )

    sorted_rows = sorted(rows, key=lambda row: str((row or {}).get("bucket") or ""))

    probabilities_by_bucket: List[Dict[str, Any]] = []
    for row in sorted_rows:
        bucket = _nonempty_str(row.get("bucket"))
        if bucket is None:
            continue

        effective_k_raw = row.get("effective_K")
        k_int_raw = row.get("K_int")

        effective_k_valid = _is_number(effective_k_raw)
        k_int_valid = isinstance(k_int_raw, int) and not isinstance(k_int_raw, bool)

        if not effective_k_valid:
            codes.add("PROBABILITY_CHECKPOINT_BUCKET_EFFECTIVE_K_INVALID")
        if not k_int_valid:
            codes.add("PROBABILITY_CHECKPOINT_BUCKET_K_INT_INVALID")

        if not (effective_k_valid and k_int_valid):
            continue

        effective_k = _round6_half_up(_clamp_to_deck_size(float(effective_k_raw)))
        expected_k_int = int(floor(effective_k))

        if int(k_int_raw) != expected_k_int:
            codes.add("PROBABILITY_CHECKPOINT_K_INT_POLICY_VIOLATION")
            continue

        probabilities_by_checkpoint: List[Dict[str, Any]] = []
        for draw_row in checkpoint_draws:
            checkpoint = int(draw_row.get("checkpoint") or 0)
            effective_n = float(draw_row.get("effective_n") or 0.0)
            n_int = int(draw_row.get("n_int") or 0)

            try:
                p_ge_1_raw = hypergeom_p_ge_1(
                    N=_DECK_SIZE_N,
                    K_int=int(k_int_raw),
                    n=n_int,
                )
            except RuntimeError:
                codes.add("PROBABILITY_CHECKPOINT_MATH_RUNTIME_ERROR")
                probabilities_by_checkpoint = []
                break

            probabilities_by_checkpoint.append(
                {
                    "checkpoint": checkpoint,
                    "effective_n": effective_n,
                    "n_int": n_int,
                    "p_ge_1": _round6_half_up(float(p_ge_1_raw)),
                }
            )

        if len(probabilities_by_checkpoint) == len(checkpoint_draws):
            probabilities_by_bucket.append(
                {
                    "bucket": bucket,
                    "effective_K": float(effective_k),
                    "K_int": int(k_int_raw),
                    "probabilities_by_checkpoint": probabilities_by_checkpoint,
                }
            )

    if len(codes.intersection(_ERROR_CODES)) > 0:
        return _base_payload(
            status="ERROR",
            reason_code=None,
            codes=sorted(codes),
            format_token=format_token,
            default_policy=default_policy,
            checkpoint_draws=checkpoint_draws,
            probabilities_by_bucket=probabilities_by_bucket,
        )

    if len(codes.intersection(_WARN_CODES)) > 0:
        return _base_payload(
            status="WARN",
            reason_code=None,
            codes=sorted(codes),
            format_token=format_token,
            default_policy=default_policy,
            checkpoint_draws=checkpoint_draws,
            probabilities_by_bucket=probabilities_by_bucket,
        )

    return _base_payload(
        status="OK",
        reason_code=None,
        codes=[],
        format_token=format_token,
        default_policy=default_policy,
        checkpoint_draws=checkpoint_draws,
        probabilities_by_bucket=probabilities_by_bucket,
    )
