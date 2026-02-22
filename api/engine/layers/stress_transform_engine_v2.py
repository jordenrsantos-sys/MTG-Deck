from __future__ import annotations

import json
from decimal import Decimal, ROUND_HALF_UP
from math import floor
from typing import Any, Dict, List, Set

from api.engine.probability_math_core_v1 import hypergeom_p_ge_1


STRESS_TRANSFORM_ENGINE_V2_VERSION = "stress_transform_engine_v2"
_DECK_SIZE_N = 99
_CHECKPOINTS: tuple[int, ...] = (7, 9, 10, 12)

_ERROR_CODES = {
    "STRESS_TRANSFORM_BUCKET_EFFECTIVE_K_INVALID",
    "STRESS_TRANSFORM_BUCKET_K_INT_INVALID",
    "STRESS_TRANSFORM_K_INT_POLICY_VIOLATION",
    "STRESS_TRANSFORM_CHECKPOINT_DRAW_INVALID",
    "STRESS_TRANSFORM_OPERATOR_INVALID",
    "STRESS_TRANSFORM_OPERATOR_POLICY_INVALID",
    "STRESS_TRANSFORM_MATH_RUNTIME_ERROR",
}


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and int(value) >= 0


def _round6_half_up(value: float) -> float:
    return float(Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _clamp_k(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > float(_DECK_SIZE_N):
        return float(_DECK_SIZE_N)
    return float(value)


def _clamp_probability(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _base_payload(
    *,
    status: str,
    reason_code: str | None,
    codes: List[str],
    format_token: str,
    selected_model_id: str | None,
    policy_version: str | None,
    operators_applied: List[Dict[str, Any]],
    checkpoint_draws: List[Dict[str, Any]],
    stress_adjusted_effective_k: List[Dict[str, Any]],
    stress_adjusted_probabilities_by_bucket: List[Dict[str, Any]],
    operator_impacts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "version": STRESS_TRANSFORM_ENGINE_V2_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": sorted(set(codes)),
        "format": format_token,
        "deck_size_N": _DECK_SIZE_N,
        "selected_model_id": selected_model_id,
        "policy_version": policy_version,
        "operators_applied": operators_applied,
        "checkpoints": list(_CHECKPOINTS),
        "checkpoint_draws": checkpoint_draws,
        "stress_adjusted_effective_K": stress_adjusted_effective_k,
        "stress_adjusted_probabilities_by_bucket": stress_adjusted_probabilities_by_bucket,
        "operator_impacts": operator_impacts,
    }


def _normalize_operator(
    *,
    row: Any,
    default_by_turn: Dict[str, int],
) -> Dict[str, Any] | None:
    if not isinstance(row, dict):
        return None

    op = _nonempty_str(row.get("op"))
    if op is None:
        return None

    if op == "TARGETED_REMOVAL":
        count = row.get("count")
        if not _is_nonnegative_int(count):
            return None
        by_turn = row.get("by_turn")
        if by_turn is None:
            by_turn = default_by_turn.get(op)
        if not _is_nonnegative_int(by_turn):
            return None
        return {
            "op": op,
            "count": int(count),
            "by_turn": int(by_turn),
        }

    if op == "BOARD_WIPE":
        by_turn = row.get("by_turn")
        surviving = row.get("surviving_engine_fraction")
        if not _is_nonnegative_int(by_turn):
            return None
        if not _is_number(surviving):
            return None
        surviving_float = float(surviving)
        if surviving_float < 0.0 or surviving_float > 1.0:
            return None
        return {
            "op": op,
            "by_turn": int(by_turn),
            "surviving_engine_fraction": _round6_half_up(surviving_float),
        }

    if op == "GRAVEYARD_HATE_WINDOW":
        turns_raw = row.get("turns")
        penalty = row.get("graveyard_penalty")
        if not isinstance(turns_raw, list):
            return None
        if not _is_number(penalty):
            return None
        turns = sorted({int(turn) for turn in turns_raw if _is_nonnegative_int(turn)})
        if len(turns) == 0:
            return None
        penalty_float = float(penalty)
        if penalty_float < 0.0 or penalty_float > 1.0:
            return None
        return {
            "op": op,
            "turns": turns,
            "by_turn": int(min(turns)),
            "graveyard_penalty": _round6_half_up(penalty_float),
        }

    if op == "STAX_TAX":
        by_turn = row.get("by_turn")
        inflation = row.get("inflation_factor")
        if not _is_nonnegative_int(by_turn):
            return None
        if not _is_number(inflation):
            return None
        inflation_float = float(inflation)
        if inflation_float < 0.0:
            return None
        return {
            "op": op,
            "by_turn": int(by_turn),
            "inflation_factor": _round6_half_up(inflation_float),
        }

    if op == "WHEEL":
        by_turn = row.get("by_turn")
        if by_turn is None:
            by_turn = default_by_turn.get(op)
        if not _is_nonnegative_int(by_turn):
            return None
        wheel_penalty = row.get("wheel_penalty", 0.9)
        if not _is_number(wheel_penalty):
            return None
        wheel_penalty_float = float(wheel_penalty)
        if wheel_penalty_float < 0.0 or wheel_penalty_float > 1.0:
            return None
        return {
            "op": op,
            "by_turn": int(by_turn),
            "wheel_penalty": _round6_half_up(wheel_penalty_float),
        }

    if op == "HAND_DISRUPTION":
        by_turn = row.get("by_turn")
        if by_turn is None:
            by_turn = default_by_turn.get(op)
        if not _is_nonnegative_int(by_turn):
            return None
        count = row.get("count", 1)
        if not _is_nonnegative_int(count):
            return None
        return {
            "op": op,
            "by_turn": int(by_turn),
            "count": int(count),
        }

    if op == "COMBAT_PRESSURE":
        by_turn = row.get("by_turn")
        if by_turn is None:
            by_turn = default_by_turn.get(op)
        if not _is_nonnegative_int(by_turn):
            return None
        pressure_penalty = row.get("pressure_penalty", 0.95)
        if not _is_number(pressure_penalty):
            return None
        pressure_penalty_float = float(pressure_penalty)
        if pressure_penalty_float < 0.0 or pressure_penalty_float > 1.0:
            return None
        return {
            "op": op,
            "by_turn": int(by_turn),
            "pressure_penalty": _round6_half_up(pressure_penalty_float),
        }

    return None


def _recompute_probabilities(
    *,
    k_int: int,
    checkpoint_draws_by_checkpoint: Dict[int, Dict[str, Any]],
    codes: Set[str],
) -> Dict[int, float] | None:
    out: Dict[int, float] = {}
    for checkpoint in _CHECKPOINTS:
        draw_row = checkpoint_draws_by_checkpoint.get(checkpoint)
        if not isinstance(draw_row, dict):
            codes.add("STRESS_TRANSFORM_CHECKPOINT_DRAW_INVALID")
            return None
        n_int = draw_row.get("n_int")
        if not _is_nonnegative_int(n_int):
            codes.add("STRESS_TRANSFORM_CHECKPOINT_DRAW_INVALID")
            return None
        try:
            probability_raw = hypergeom_p_ge_1(N=_DECK_SIZE_N, K_int=int(k_int), n=int(n_int))
        except RuntimeError:
            codes.add("STRESS_TRANSFORM_MATH_RUNTIME_ERROR")
            return None
        out[int(checkpoint)] = _round6_half_up(_clamp_probability(float(probability_raw)))
    return out


def _operator_sort_key(
    *,
    operator: Dict[str, Any],
    precedence_index: Dict[str, int],
) -> tuple[int, int, str, str]:
    op_name = str(operator.get("op") or "")
    by_turn = int(operator.get("by_turn") or 0)
    rank = int(precedence_index.get(op_name, 10**6))
    return (
        by_turn,
        rank,
        op_name,
        _stable_json(operator),
    )


def run_stress_transform_engine_v2(
    *,
    substitution_engine_v1_payload: Any,
    probability_checkpoint_layer_v1_payload: Any,
    stress_model_definition_v1_payload: Any,
    probability_math_core_v1_payload: Any,
    stress_operator_policy_v1_payload: Any,
) -> Dict[str, Any]:
    selected_model_id = None
    if isinstance(stress_model_definition_v1_payload, dict):
        selected_model_id = _nonempty_str(stress_model_definition_v1_payload.get("selected_model_id"))

    format_token = (
        _nonempty_str((stress_model_definition_v1_payload or {}).get("format"))
        or _nonempty_str((probability_checkpoint_layer_v1_payload or {}).get("format"))
        or _nonempty_str((substitution_engine_v1_payload or {}).get("format"))
        or ""
    )

    policy_payload = stress_operator_policy_v1_payload if isinstance(stress_operator_policy_v1_payload, dict) else {}
    policy_version = _nonempty_str(policy_payload.get("version"))

    if not isinstance(substitution_engine_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            selected_model_id=selected_model_id,
            policy_version=policy_version,
            operators_applied=[],
            checkpoint_draws=[],
            stress_adjusted_effective_k=[],
            stress_adjusted_probabilities_by_bucket=[],
            operator_impacts=[],
        )

    if not isinstance(probability_checkpoint_layer_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="PROBABILITY_CHECKPOINT_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            selected_model_id=selected_model_id,
            policy_version=policy_version,
            operators_applied=[],
            checkpoint_draws=[],
            stress_adjusted_effective_k=[],
            stress_adjusted_probabilities_by_bucket=[],
            operator_impacts=[],
        )

    if not isinstance(stress_model_definition_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="STRESS_MODEL_DEFINITION_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            selected_model_id=selected_model_id,
            policy_version=policy_version,
            operators_applied=[],
            checkpoint_draws=[],
            stress_adjusted_effective_k=[],
            stress_adjusted_probabilities_by_bucket=[],
            operator_impacts=[],
        )

    if not isinstance(probability_math_core_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="PROBABILITY_MATH_CORE_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            selected_model_id=selected_model_id,
            policy_version=policy_version,
            operators_applied=[],
            checkpoint_draws=[],
            stress_adjusted_effective_k=[],
            stress_adjusted_probabilities_by_bucket=[],
            operator_impacts=[],
        )

    codes: Set[str] = set()

    precedence = policy_payload.get("precedence") if isinstance(policy_payload.get("precedence"), list) else []
    precedence_list = [str(op) for op in precedence if _nonempty_str(op) is not None]
    precedence_index = {op: idx for idx, op in enumerate(precedence_list)}
    default_by_turn = policy_payload.get("default_by_turn") if isinstance(policy_payload.get("default_by_turn"), dict) else {}
    default_by_turn_norm = {
        str(key): int(value)
        for key, value in default_by_turn.items()
        if _nonempty_str(key) is not None and _is_nonnegative_int(value)
    }
    tie_break = _nonempty_str(policy_payload.get("tie_break"))
    composition = policy_payload.get("composition") if isinstance(policy_payload.get("composition"), dict) else {}
    composition_mode = _nonempty_str(composition.get("mode"))

    if policy_version is None or tie_break != "op_name_then_json" or composition_mode != "sequential":
        codes.add("STRESS_TRANSFORM_OPERATOR_POLICY_INVALID")

    checkpoint_draws_raw = probability_checkpoint_layer_v1_payload.get("checkpoint_draws")
    if not isinstance(checkpoint_draws_raw, list) or len(checkpoint_draws_raw) == 0:
        return _base_payload(
            status="SKIP",
            reason_code="PROBABILITY_CHECKPOINT_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            selected_model_id=selected_model_id,
            policy_version=policy_version,
            operators_applied=[],
            checkpoint_draws=[],
            stress_adjusted_effective_k=[],
            stress_adjusted_probabilities_by_bucket=[],
            operator_impacts=[],
        )

    buckets_raw = substitution_engine_v1_payload.get("buckets")
    if not isinstance(buckets_raw, list) or len(buckets_raw) == 0:
        return _base_payload(
            status="SKIP",
            reason_code="SUBSTITUTION_ENGINE_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            selected_model_id=selected_model_id,
            policy_version=policy_version,
            operators_applied=[],
            checkpoint_draws=[],
            stress_adjusted_effective_k=[],
            stress_adjusted_probabilities_by_bucket=[],
            operator_impacts=[],
        )

    operators_raw = stress_model_definition_v1_payload.get("operators")
    if not isinstance(operators_raw, list):
        return _base_payload(
            status="SKIP",
            reason_code="STRESS_MODEL_DEFINITION_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            selected_model_id=selected_model_id,
            policy_version=policy_version,
            operators_applied=[],
            checkpoint_draws=[],
            stress_adjusted_effective_k=[],
            stress_adjusted_probabilities_by_bucket=[],
            operator_impacts=[],
        )

    checkpoint_draws_by_checkpoint: Dict[int, Dict[str, Any]] = {}
    for row in checkpoint_draws_raw:
        if not isinstance(row, dict):
            continue
        checkpoint = row.get("checkpoint")
        effective_n = row.get("effective_n")
        n_int = row.get("n_int")
        if not _is_nonnegative_int(checkpoint):
            codes.add("STRESS_TRANSFORM_CHECKPOINT_DRAW_INVALID")
            continue
        checkpoint_int = int(checkpoint)
        if checkpoint_int not in _CHECKPOINTS:
            continue
        if not _is_number(effective_n) or not _is_nonnegative_int(n_int):
            codes.add("STRESS_TRANSFORM_CHECKPOINT_DRAW_INVALID")
            continue
        effective_n_norm = _round6_half_up(_clamp_k(float(effective_n)))
        expected_n_int = int(floor(effective_n_norm))
        if int(n_int) != expected_n_int:
            codes.add("STRESS_TRANSFORM_CHECKPOINT_DRAW_INVALID")
            continue
        checkpoint_draws_by_checkpoint[checkpoint_int] = {
            "checkpoint": checkpoint_int,
            "effective_n": effective_n_norm,
            "n_int": int(n_int),
        }

    if any(checkpoint not in checkpoint_draws_by_checkpoint for checkpoint in _CHECKPOINTS):
        codes.add("STRESS_TRANSFORM_CHECKPOINT_DRAW_INVALID")

    baseline_by_bucket: Dict[str, Dict[str, Any]] = {}
    for row in sorted([entry for entry in buckets_raw if isinstance(entry, dict)], key=lambda item: str(item.get("bucket") or "")):
        bucket = _nonempty_str(row.get("bucket"))
        if bucket is None:
            continue
        effective_k_raw = row.get("effective_K")
        k_int_raw = row.get("K_int")
        if not _is_number(effective_k_raw):
            codes.add("STRESS_TRANSFORM_BUCKET_EFFECTIVE_K_INVALID")
            continue
        if not isinstance(k_int_raw, int) or isinstance(k_int_raw, bool):
            codes.add("STRESS_TRANSFORM_BUCKET_K_INT_INVALID")
            continue
        effective_k = _round6_half_up(_clamp_k(float(effective_k_raw)))
        expected_k_int = int(floor(effective_k))
        if int(k_int_raw) != expected_k_int:
            codes.add("STRESS_TRANSFORM_K_INT_POLICY_VIOLATION")
            continue
        baseline_by_bucket[bucket] = {
            "bucket": bucket,
            "effective_K_before": effective_k,
            "K_int_before": int(k_int_raw),
        }

    normalized_operators: List[Dict[str, Any]] = []
    for row in operators_raw:
        normalized = _normalize_operator(row=row, default_by_turn=default_by_turn_norm)
        if normalized is None:
            codes.add("STRESS_TRANSFORM_OPERATOR_INVALID")
            continue
        normalized_operators.append(normalized)

    normalized_operators = sorted(
        normalized_operators,
        key=lambda operator: _operator_sort_key(
            operator=operator,
            precedence_index=precedence_index,
        ),
    )

    current_state_by_bucket: Dict[str, Dict[str, Any]] = {}
    for bucket in sorted(baseline_by_bucket.keys()):
        baseline_row = baseline_by_bucket[bucket]
        current_probabilities = _recompute_probabilities(
            k_int=int(baseline_row["K_int_before"]),
            checkpoint_draws_by_checkpoint=checkpoint_draws_by_checkpoint,
            codes=codes,
        )
        if current_probabilities is None:
            current_probabilities = {}
        current_state_by_bucket[bucket] = {
            "effective_K": float(baseline_row["effective_K_before"]),
            "K_int": int(baseline_row["K_int_before"]),
            "probabilities": current_probabilities,
        }

    operator_impacts: List[Dict[str, Any]] = []
    for operator_index, operator in enumerate(normalized_operators, start=1):
        op_name = str(operator.get("op") or "")
        bucket_impacts: List[Dict[str, Any]] = []

        for bucket in sorted(current_state_by_bucket.keys()):
            current_state = current_state_by_bucket[bucket]
            effective_k_before = float(current_state.get("effective_K") or 0.0)
            k_int_before = int(current_state.get("K_int") or 0)
            probabilities_before = current_state.get("probabilities") if isinstance(current_state.get("probabilities"), dict) else {}

            probabilities_before_rows = [
                {
                    "checkpoint": checkpoint,
                    "p_ge_1": _round6_half_up(float(probabilities_before.get(checkpoint) or 0.0)),
                }
                for checkpoint in _CHECKPOINTS
            ]

            effective_k_after = effective_k_before
            k_int_after = k_int_before
            probabilities_after = dict(probabilities_before)

            if op_name in {"TARGETED_REMOVAL", "HAND_DISRUPTION"}:
                effective_k_after = _round6_half_up(
                    _clamp_k(effective_k_before - float(operator.get("count") or 0.0))
                )
                k_int_after = int(floor(effective_k_after))
                recomputed = _recompute_probabilities(
                    k_int=k_int_after,
                    checkpoint_draws_by_checkpoint=checkpoint_draws_by_checkpoint,
                    codes=codes,
                )
                if recomputed is not None:
                    probabilities_after = recomputed
            elif op_name == "BOARD_WIPE":
                effective_k_after = _round6_half_up(
                    _clamp_k(effective_k_before * float(operator.get("surviving_engine_fraction") or 0.0))
                )
                k_int_after = int(floor(effective_k_after))
                recomputed = _recompute_probabilities(
                    k_int=k_int_after,
                    checkpoint_draws_by_checkpoint=checkpoint_draws_by_checkpoint,
                    codes=codes,
                )
                if recomputed is not None:
                    probabilities_after = recomputed
            elif op_name == "GRAVEYARD_HATE_WINDOW":
                effective_k_after = _round6_half_up(
                    _clamp_k(effective_k_before * float(operator.get("graveyard_penalty") or 0.0))
                )
                k_int_after = int(floor(effective_k_after))
                recomputed = _recompute_probabilities(
                    k_int=k_int_after,
                    checkpoint_draws_by_checkpoint=checkpoint_draws_by_checkpoint,
                    codes=codes,
                )
                if recomputed is not None:
                    probabilities_after = recomputed
            elif op_name == "STAX_TAX":
                inflation_factor = float(operator.get("inflation_factor") or 0.0)
                probabilities_after = {
                    checkpoint: _round6_half_up(
                        _clamp_probability(float(probabilities_before.get(checkpoint) or 0.0) * inflation_factor)
                    )
                    for checkpoint in _CHECKPOINTS
                }
            elif op_name == "WHEEL":
                wheel_penalty = float(operator.get("wheel_penalty") or 1.0)
                probabilities_after = {
                    checkpoint: _round6_half_up(
                        _clamp_probability(float(probabilities_before.get(checkpoint) or 0.0) * wheel_penalty)
                    )
                    for checkpoint in _CHECKPOINTS
                }
            elif op_name == "COMBAT_PRESSURE":
                pressure_penalty = float(operator.get("pressure_penalty") or 1.0)
                probabilities_after = {
                    checkpoint: _round6_half_up(
                        _clamp_probability(float(probabilities_before.get(checkpoint) or 0.0) * pressure_penalty)
                    )
                    for checkpoint in _CHECKPOINTS
                }
            else:
                codes.add("STRESS_TRANSFORM_OPERATOR_INVALID")

            probabilities_after_rows = [
                {
                    "checkpoint": checkpoint,
                    "p_ge_1": _round6_half_up(float(probabilities_after.get(checkpoint) or 0.0)),
                }
                for checkpoint in _CHECKPOINTS
            ]

            current_state["effective_K"] = float(effective_k_after)
            current_state["K_int"] = int(k_int_after)
            current_state["probabilities"] = dict(probabilities_after)

            bucket_impacts.append(
                {
                    "bucket": bucket,
                    "effective_K_before": float(effective_k_before),
                    "effective_K_after": float(effective_k_after),
                    "K_int_before": int(k_int_before),
                    "K_int_after": int(k_int_after),
                    "probabilities_before": probabilities_before_rows,
                    "probabilities_after": probabilities_after_rows,
                }
            )

        operator_impacts.append(
            {
                "operator_index": int(operator_index),
                "operator": operator,
                "bucket_impacts": bucket_impacts,
            }
        )

    stress_adjusted_effective_k = [
        {
            "bucket": bucket,
            "effective_K_before": float(baseline_by_bucket[bucket]["effective_K_before"]),
            "K_int_before": int(baseline_by_bucket[bucket]["K_int_before"]),
            "effective_K_after": float(current_state_by_bucket[bucket]["effective_K"]),
            "K_int_after": int(current_state_by_bucket[bucket]["K_int"]),
        }
        for bucket in sorted(current_state_by_bucket.keys())
    ]

    checkpoint_draws_rows = [
        {
            "checkpoint": checkpoint,
            "effective_n": float(checkpoint_draws_by_checkpoint.get(checkpoint, {}).get("effective_n") or 0.0),
            "n_int": int(checkpoint_draws_by_checkpoint.get(checkpoint, {}).get("n_int") or 0),
        }
        for checkpoint in _CHECKPOINTS
    ]

    stress_adjusted_probabilities_by_bucket = [
        {
            "bucket": bucket,
            "effective_K_after": float(current_state_by_bucket[bucket]["effective_K"]),
            "K_int_after": int(current_state_by_bucket[bucket]["K_int"]),
            "probabilities_by_checkpoint": [
                {
                    "checkpoint": checkpoint,
                    "effective_n": float(checkpoint_draws_by_checkpoint[checkpoint]["effective_n"]),
                    "n_int": int(checkpoint_draws_by_checkpoint[checkpoint]["n_int"]),
                    "p_ge_1": _round6_half_up(float(current_state_by_bucket[bucket]["probabilities"].get(checkpoint) or 0.0)),
                }
                for checkpoint in _CHECKPOINTS
                if checkpoint in checkpoint_draws_by_checkpoint
            ],
        }
        for bucket in sorted(current_state_by_bucket.keys())
    ]

    if len(codes.intersection(_ERROR_CODES)) > 0:
        return _base_payload(
            status="ERROR",
            reason_code=None,
            codes=sorted(codes),
            format_token=format_token,
            selected_model_id=selected_model_id,
            policy_version=policy_version,
            operators_applied=normalized_operators,
            checkpoint_draws=checkpoint_draws_rows,
            stress_adjusted_effective_k=stress_adjusted_effective_k,
            stress_adjusted_probabilities_by_bucket=stress_adjusted_probabilities_by_bucket,
            operator_impacts=operator_impacts,
        )

    return _base_payload(
        status="OK",
        reason_code=None,
        codes=[],
        format_token=format_token,
        selected_model_id=selected_model_id,
        policy_version=policy_version,
        operators_applied=normalized_operators,
        checkpoint_draws=checkpoint_draws_rows,
        stress_adjusted_effective_k=stress_adjusted_effective_k,
        stress_adjusted_probabilities_by_bucket=stress_adjusted_probabilities_by_bucket,
        operator_impacts=operator_impacts,
    )
