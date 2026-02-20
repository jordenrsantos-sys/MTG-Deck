from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Set, Tuple


RESILIENCE_MATH_ENGINE_V1_VERSION = "resilience_math_engine_v1"
_CHECKPOINTS: tuple[int, ...] = (7, 9, 10, 12)
_DECK_SIZE_N = 99.0

_ERROR_CODES = {
    "RESILIENCE_BASELINE_BUCKET_INVALID",
    "RESILIENCE_STRESS_BUCKET_INVALID",
    "RESILIENCE_BUCKET_ALIGNMENT_INVALID",
    "RESILIENCE_CHECKPOINT_INVALID",
    "RESILIENCE_PROBABILITY_INVALID",
    "RESILIENCE_OPERATOR_IMPACTS_INVALID",
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


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _mean(values: List[float]) -> float | None:
    if len(values) == 0:
        return None
    return float(sum(values) / float(len(values)))


def _safe_ratio(*, numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        if numerator <= 0.0:
            return 1.0
        return 0.0
    return _clamp01(float(numerator) / float(denominator))


def _default_metrics() -> Dict[str, Any]:
    return {
        "engine_continuity_after_removal": None,
        "rebuild_after_wipe": None,
        "graveyard_fragility_delta": None,
        "commander_fragility_delta": None,
    }


def _base_payload(
    *,
    status: str,
    reason_code: str | None,
    codes: List[str],
    format_token: str,
    commander_dependency: str | None,
    metrics: Dict[str, Any],
    bucket_metrics: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "version": RESILIENCE_MATH_ENGINE_V1_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": sorted(set(codes)),
        "format": format_token,
        "checkpoints": list(_CHECKPOINTS),
        "commander_dependency": commander_dependency,
        "metrics": metrics,
        "bucket_metrics": bucket_metrics,
    }


def _parse_checkpoint_probability_map(
    *,
    probability_rows: Any,
    codes: Set[str],
) -> Dict[int, float] | None:
    if not isinstance(probability_rows, list):
        codes.add("RESILIENCE_PROBABILITY_INVALID")
        return None

    out: Dict[int, float] = {}

    for row in probability_rows:
        if not isinstance(row, dict):
            codes.add("RESILIENCE_PROBABILITY_INVALID")
            continue

        checkpoint = row.get("checkpoint")
        probability_raw = row.get("p_ge_1")

        if not _is_nonnegative_int(checkpoint):
            codes.add("RESILIENCE_CHECKPOINT_INVALID")
            continue

        checkpoint_int = int(checkpoint)
        if checkpoint_int not in _CHECKPOINTS:
            codes.add("RESILIENCE_CHECKPOINT_INVALID")
            continue

        if not _is_number(probability_raw):
            codes.add("RESILIENCE_PROBABILITY_INVALID")
            continue

        if checkpoint_int in out:
            codes.add("RESILIENCE_CHECKPOINT_INVALID")
            continue

        out[checkpoint_int] = _round6_half_up(_clamp01(float(probability_raw)))

    if any(checkpoint not in out for checkpoint in _CHECKPOINTS):
        codes.add("RESILIENCE_CHECKPOINT_INVALID")
        return None

    return {
        checkpoint: out[checkpoint]
        for checkpoint in _CHECKPOINTS
    }


def _normalize_effective_k(
    *,
    value: Any,
    error_code: str,
    codes: Set[str],
) -> float | None:
    if not _is_number(value):
        codes.add(error_code)
        return None

    numeric = float(value)
    if numeric < 0.0 or numeric > _DECK_SIZE_N:
        codes.add(error_code)
        return None

    return _round6_half_up(numeric)


def _parse_baseline_by_bucket(
    *,
    baseline_rows: Any,
    codes: Set[str],
) -> Dict[str, Dict[str, Any]]:
    if not isinstance(baseline_rows, list):
        codes.add("RESILIENCE_BASELINE_BUCKET_INVALID")
        return {}

    baseline_by_bucket: Dict[str, Dict[str, Any]] = {}
    sorted_rows = sorted(
        [row for row in baseline_rows if isinstance(row, dict)],
        key=lambda row: str((row or {}).get("bucket") or ""),
    )

    if len(sorted_rows) != len(baseline_rows):
        codes.add("RESILIENCE_BASELINE_BUCKET_INVALID")

    for row in sorted_rows:
        bucket = _nonempty_str(row.get("bucket"))
        if bucket is None:
            codes.add("RESILIENCE_BASELINE_BUCKET_INVALID")
            continue

        if bucket in baseline_by_bucket:
            codes.add("RESILIENCE_BASELINE_BUCKET_INVALID")
            continue

        effective_k = _normalize_effective_k(
            value=row.get("effective_K"),
            error_code="RESILIENCE_BASELINE_BUCKET_INVALID",
            codes=codes,
        )
        probabilities = _parse_checkpoint_probability_map(
            probability_rows=row.get("probabilities_by_checkpoint"),
            codes=codes,
        )

        if effective_k is None or probabilities is None:
            continue

        baseline_by_bucket[bucket] = {
            "effective_K": effective_k,
            "probabilities": probabilities,
        }

    return baseline_by_bucket


def _parse_stress_by_bucket(
    *,
    stress_rows: Any,
    codes: Set[str],
) -> Dict[str, Dict[str, Any]]:
    if not isinstance(stress_rows, list):
        codes.add("RESILIENCE_STRESS_BUCKET_INVALID")
        return {}

    stress_by_bucket: Dict[str, Dict[str, Any]] = {}
    sorted_rows = sorted(
        [row for row in stress_rows if isinstance(row, dict)],
        key=lambda row: str((row or {}).get("bucket") or ""),
    )

    if len(sorted_rows) != len(stress_rows):
        codes.add("RESILIENCE_STRESS_BUCKET_INVALID")

    for row in sorted_rows:
        bucket = _nonempty_str(row.get("bucket"))
        if bucket is None:
            codes.add("RESILIENCE_STRESS_BUCKET_INVALID")
            continue

        if bucket in stress_by_bucket:
            codes.add("RESILIENCE_STRESS_BUCKET_INVALID")
            continue

        effective_k = _normalize_effective_k(
            value=row.get("effective_K_after"),
            error_code="RESILIENCE_STRESS_BUCKET_INVALID",
            codes=codes,
        )
        probabilities = _parse_checkpoint_probability_map(
            probability_rows=row.get("probabilities_by_checkpoint"),
            codes=codes,
        )

        if effective_k is None or probabilities is None:
            continue

        stress_by_bucket[bucket] = {
            "effective_K": effective_k,
            "probabilities": probabilities,
        }

    return stress_by_bucket


def _normalize_operator_entries(
    *,
    operator_impacts: Any,
    codes: Set[str],
) -> List[Dict[str, Any]]:
    if not isinstance(operator_impacts, list):
        return []

    entries: List[Dict[str, Any]] = []
    for row in operator_impacts:
        if not isinstance(row, dict):
            codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")
            continue

        operator_payload = row.get("operator") if isinstance(row.get("operator"), dict) else {}
        operator_name = _nonempty_str(operator_payload.get("op"))
        if operator_name is None:
            codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")
            continue

        operator_index_raw = row.get("operator_index")
        operator_index = int(operator_index_raw) if _is_nonnegative_int(operator_index_raw) else 0

        entries.append(
            {
                "operator_name": operator_name,
                "operator_index": operator_index,
                "bucket_impacts": row.get("bucket_impacts"),
            }
        )

    return sorted(
        entries,
        key=lambda entry: (
            int(entry.get("operator_index") or 0),
            str(entry.get("operator_name") or ""),
        ),
    )


def _collect_operator_k_ratios(
    *,
    operator_entries: List[Dict[str, Any]],
    operator_name: str,
    codes: Set[str],
) -> List[float]:
    first_before_by_bucket: Dict[str, float] = {}
    last_after_by_bucket: Dict[str, float] = {}
    operator_seen = False

    for entry in operator_entries:
        if str(entry.get("operator_name") or "") != operator_name:
            continue

        operator_seen = True
        bucket_impacts = entry.get("bucket_impacts")
        if not isinstance(bucket_impacts, list):
            codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")
            continue

        sorted_bucket_impacts = sorted(
            [row for row in bucket_impacts if isinstance(row, dict)],
            key=lambda row: str((row or {}).get("bucket") or ""),
        )
        if len(sorted_bucket_impacts) != len(bucket_impacts):
            codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")

        for impact in sorted_bucket_impacts:
            bucket = _nonempty_str(impact.get("bucket"))
            if bucket is None:
                codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")
                continue

            before_raw = impact.get("effective_K_before")
            after_raw = impact.get("effective_K_after")
            if not _is_number(before_raw) or not _is_number(after_raw):
                codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")
                continue

            before = float(before_raw)
            after = float(after_raw)
            if bucket not in first_before_by_bucket:
                first_before_by_bucket[bucket] = before
            last_after_by_bucket[bucket] = after

    ratios = [
        _safe_ratio(
            numerator=float(last_after_by_bucket[bucket]),
            denominator=float(first_before_by_bucket[bucket]),
        )
        for bucket in sorted(first_before_by_bucket.keys())
        if bucket in last_after_by_bucket
    ]

    if operator_seen and len(ratios) == 0:
        codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")

    return ratios


def _collect_operator_probability_deltas(
    *,
    operator_entries: List[Dict[str, Any]],
    operator_name: str,
    codes: Set[str],
) -> List[float]:
    first_before_by_bucket_checkpoint: Dict[Tuple[str, int], float] = {}
    last_after_by_bucket_checkpoint: Dict[Tuple[str, int], float] = {}
    operator_seen = False

    for entry in operator_entries:
        if str(entry.get("operator_name") or "") != operator_name:
            continue

        operator_seen = True
        bucket_impacts = entry.get("bucket_impacts")
        if not isinstance(bucket_impacts, list):
            codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")
            continue

        sorted_bucket_impacts = sorted(
            [row for row in bucket_impacts if isinstance(row, dict)],
            key=lambda row: str((row or {}).get("bucket") or ""),
        )
        if len(sorted_bucket_impacts) != len(bucket_impacts):
            codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")

        for impact in sorted_bucket_impacts:
            bucket = _nonempty_str(impact.get("bucket"))
            if bucket is None:
                codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")
                continue

            probabilities_before = _parse_checkpoint_probability_map(
                probability_rows=impact.get("probabilities_before"),
                codes=codes,
            )
            probabilities_after = _parse_checkpoint_probability_map(
                probability_rows=impact.get("probabilities_after"),
                codes=codes,
            )
            if probabilities_before is None or probabilities_after is None:
                continue

            for checkpoint in _CHECKPOINTS:
                key = (bucket, checkpoint)
                if key not in first_before_by_bucket_checkpoint:
                    first_before_by_bucket_checkpoint[key] = float(probabilities_before[checkpoint])
                last_after_by_bucket_checkpoint[key] = float(probabilities_after[checkpoint])

    deltas = [
        _clamp01(
            max(
                0.0,
                float(first_before_by_bucket_checkpoint[key]) - float(last_after_by_bucket_checkpoint[key]),
            )
        )
        for key in sorted(first_before_by_bucket_checkpoint.keys())
        if key in last_after_by_bucket_checkpoint
    ]

    if operator_seen and len(deltas) == 0:
        codes.add("RESILIENCE_OPERATOR_IMPACTS_INVALID")

    return deltas


def _extract_commander_dependency(engine_requirement_detection_v1_payload: Any) -> str | None:
    if not isinstance(engine_requirement_detection_v1_payload, dict):
        return None

    engine_requirements = engine_requirement_detection_v1_payload.get("engine_requirements_v1")
    if not isinstance(engine_requirements, dict):
        return None

    return _nonempty_str(engine_requirements.get("commander_dependent"))


def run_resilience_math_engine_v1(
    *,
    probability_checkpoint_layer_v1_payload: Any,
    stress_transform_engine_v1_payload: Any,
    engine_requirement_detection_v1_payload: Any = None,
) -> Dict[str, Any]:
    format_token = (
        _nonempty_str((probability_checkpoint_layer_v1_payload or {}).get("format"))
        or _nonempty_str((stress_transform_engine_v1_payload or {}).get("format"))
        or ""
    )

    if not isinstance(probability_checkpoint_layer_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="PROBABILITY_CHECKPOINT_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            commander_dependency=None,
            metrics=_default_metrics(),
            bucket_metrics=[],
        )

    baseline_status = _nonempty_str(probability_checkpoint_layer_v1_payload.get("status"))
    baseline_rows = probability_checkpoint_layer_v1_payload.get("probabilities_by_bucket")
    if baseline_status not in {"OK", "WARN"} or not isinstance(baseline_rows, list) or len(baseline_rows) == 0:
        return _base_payload(
            status="SKIP",
            reason_code="PROBABILITY_CHECKPOINT_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            commander_dependency=None,
            metrics=_default_metrics(),
            bucket_metrics=[],
        )

    if not isinstance(stress_transform_engine_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="STRESS_TRANSFORM_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            commander_dependency=None,
            metrics=_default_metrics(),
            bucket_metrics=[],
        )

    stress_status = _nonempty_str(stress_transform_engine_v1_payload.get("status"))
    stress_rows = stress_transform_engine_v1_payload.get("stress_adjusted_probabilities_by_bucket")
    if stress_status not in {"OK", "WARN"} or not isinstance(stress_rows, list) or len(stress_rows) == 0:
        return _base_payload(
            status="SKIP",
            reason_code="STRESS_TRANSFORM_UNAVAILABLE",
            codes=[],
            format_token=format_token,
            commander_dependency=None,
            metrics=_default_metrics(),
            bucket_metrics=[],
        )

    codes: Set[str] = set()

    baseline_by_bucket = _parse_baseline_by_bucket(
        baseline_rows=baseline_rows,
        codes=codes,
    )
    stress_by_bucket = _parse_stress_by_bucket(
        stress_rows=stress_rows,
        codes=codes,
    )

    baseline_buckets = set(baseline_by_bucket.keys())
    stress_buckets = set(stress_by_bucket.keys())

    if len(baseline_buckets) == 0:
        codes.add("RESILIENCE_BASELINE_BUCKET_INVALID")
    if len(stress_buckets) == 0:
        codes.add("RESILIENCE_STRESS_BUCKET_INVALID")

    if baseline_buckets != stress_buckets:
        codes.add("RESILIENCE_BUCKET_ALIGNMENT_INVALID")

    aligned_buckets = sorted(baseline_buckets.intersection(stress_buckets))

    bucket_metrics: List[Dict[str, Any]] = []
    continuity_fallback_ratios: List[float] = []

    for bucket in aligned_buckets:
        baseline_row = baseline_by_bucket[bucket]
        stress_row = stress_by_bucket[bucket]

        baseline_probabilities = baseline_row["probabilities"]
        stress_probabilities = stress_row["probabilities"]

        baseline_mean = _mean([float(baseline_probabilities[checkpoint]) for checkpoint in _CHECKPOINTS])
        stress_mean = _mean([float(stress_probabilities[checkpoint]) for checkpoint in _CHECKPOINTS])

        if baseline_mean is None or stress_mean is None:
            codes.add("RESILIENCE_PROBABILITY_INVALID")
            continue

        baseline_effective_k = float(baseline_row["effective_K"])
        stress_effective_k = float(stress_row["effective_K"])

        continuity_fallback_ratios.append(
            _safe_ratio(
                numerator=stress_effective_k,
                denominator=baseline_effective_k,
            )
        )

        bucket_metrics.append(
            {
                "bucket": bucket,
                "baseline_effective_K": _round6_half_up(baseline_effective_k),
                "stress_effective_K": _round6_half_up(stress_effective_k),
                "baseline_p_ge_1_mean": _round6_half_up(_clamp01(float(baseline_mean))),
                "stress_p_ge_1_mean": _round6_half_up(_clamp01(float(stress_mean))),
                "stress_delta_p_ge_1_mean": _round6_half_up(
                    _clamp01(max(0.0, float(baseline_mean) - float(stress_mean)))
                ),
            }
        )

    operator_entries = _normalize_operator_entries(
        operator_impacts=stress_transform_engine_v1_payload.get("operator_impacts"),
        codes=codes,
    )

    targeted_ratios = _collect_operator_k_ratios(
        operator_entries=operator_entries,
        operator_name="TARGETED_REMOVAL",
        codes=codes,
    )
    wipe_ratios = _collect_operator_k_ratios(
        operator_entries=operator_entries,
        operator_name="BOARD_WIPE",
        codes=codes,
    )
    graveyard_deltas = _collect_operator_probability_deltas(
        operator_entries=operator_entries,
        operator_name="GRAVEYARD_HATE_WINDOW",
        codes=codes,
    )

    metrics = _default_metrics()

    continuity_source = targeted_ratios if len(targeted_ratios) > 0 else continuity_fallback_ratios
    continuity_mean = _mean(continuity_source)
    if continuity_mean is not None:
        metrics["engine_continuity_after_removal"] = _round6_half_up(_clamp01(float(continuity_mean)))

    if len(wipe_ratios) > 0:
        wipe_mean = _mean(wipe_ratios)
        if wipe_mean is not None:
            metrics["rebuild_after_wipe"] = _round6_half_up(_clamp01(float(wipe_mean)))
    else:
        metrics["rebuild_after_wipe"] = 1.0

    if len(graveyard_deltas) > 0:
        graveyard_mean = _mean(graveyard_deltas)
        if graveyard_mean is not None:
            metrics["graveyard_fragility_delta"] = _round6_half_up(_clamp01(float(graveyard_mean)))
    else:
        metrics["graveyard_fragility_delta"] = 0.0

    commander_dependency = _extract_commander_dependency(engine_requirement_detection_v1_payload)
    if commander_dependency == "LOW":
        metrics["commander_fragility_delta"] = 0.0
    else:
        metrics["commander_fragility_delta"] = None
        codes.add("RESILIENCE_COMMANDER_FRAGILITY_UNAVAILABLE")

    status = "OK"
    if len(codes.intersection(_ERROR_CODES)) > 0:
        status = "ERROR"
    elif len(codes) > 0:
        status = "WARN"

    return _base_payload(
        status=status,
        reason_code=None,
        codes=sorted(codes),
        format_token=format_token,
        commander_dependency=commander_dependency,
        metrics=metrics,
        bucket_metrics=bucket_metrics,
    )
