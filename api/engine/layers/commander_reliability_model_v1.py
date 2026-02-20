from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Set


COMMANDER_RELIABILITY_MODEL_V1_VERSION = "commander_reliability_model_v1"
_CHECKPOINT_BY_TURN = {
    "t3": 9,
    "t4": 10,
    "t6": 12,
}
_TARGET_CHECKPOINTS = tuple(_CHECKPOINT_BY_TURN[turn] for turn in ("t3", "t4", "t6"))
_RAMP_BUCKET = "RAMP"
_PROTECTION_PRIMITIVE_IDS = {
    "HEXPROOF_PROTECTION",
    "INDESTRUCTIBLE_PROTECTION",
}
_ERROR_CODES = {
    "COMMANDER_RELIABILITY_CHECKPOINT_INVALID",
    "COMMANDER_RELIABILITY_PROBABILITY_INVALID",
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


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    cleaned = {
        token
        for token in (_nonempty_str(value) for value in values)
        if token is not None
    }
    return sorted(cleaned)


def _mean(values: List[float]) -> float | None:
    if len(values) == 0:
        return None
    return float(sum(values) / float(len(values)))


def _default_metrics() -> Dict[str, Any]:
    return {
        "cast_reliability_t3": None,
        "cast_reliability_t4": None,
        "cast_reliability_t6": None,
        "protection_coverage_proxy": None,
        "commander_fragility_delta": None,
    }


def _base_payload(
    *,
    status: str,
    reason_code: str | None,
    codes: List[str],
    commander_dependent: str | None,
    metrics: Dict[str, Any],
    notes: List[str],
) -> Dict[str, Any]:
    return {
        "version": COMMANDER_RELIABILITY_MODEL_V1_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": sorted(set(codes)),
        "commander_dependent": commander_dependent,
        "checkpoint_mapping": dict(_CHECKPOINT_BY_TURN),
        "metrics": metrics,
        "notes": sorted(set(notes)),
    }


def _extract_commander_dependency(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None

    requirements = payload.get("engine_requirements_v1")
    if not isinstance(requirements, dict):
        return None

    return _nonempty_str(requirements.get("commander_dependent"))


def _extract_ramp_probabilities_by_checkpoint(
    *,
    rows: Any,
    probability_row_key: str,
    codes: Set[str],
) -> Dict[int, float] | None:
    if not isinstance(rows, list):
        return None

    ramp_row = None
    for row in sorted([entry for entry in rows if isinstance(entry, dict)], key=lambda entry: str(entry.get("bucket") or "")):
        if _nonempty_str(row.get("bucket")) == _RAMP_BUCKET:
            ramp_row = row
            break

    if not isinstance(ramp_row, dict):
        return None

    probability_rows = ramp_row.get(probability_row_key)
    if not isinstance(probability_rows, list):
        codes.add("COMMANDER_RELIABILITY_PROBABILITY_INVALID")
        return None

    by_checkpoint: Dict[int, float] = {}
    for row in probability_rows:
        if not isinstance(row, dict):
            codes.add("COMMANDER_RELIABILITY_PROBABILITY_INVALID")
            continue

        checkpoint = row.get("checkpoint")
        probability_raw = row.get("p_ge_1")

        if not _is_nonnegative_int(checkpoint):
            codes.add("COMMANDER_RELIABILITY_CHECKPOINT_INVALID")
            continue

        checkpoint_int = int(checkpoint)
        if checkpoint_int not in _TARGET_CHECKPOINTS:
            continue

        if checkpoint_int in by_checkpoint:
            codes.add("COMMANDER_RELIABILITY_CHECKPOINT_INVALID")
            continue

        if not _is_number(probability_raw):
            codes.add("COMMANDER_RELIABILITY_PROBABILITY_INVALID")
            continue

        by_checkpoint[checkpoint_int] = _round6_half_up(_clamp01(float(probability_raw)))

    if any(checkpoint not in by_checkpoint for checkpoint in _TARGET_CHECKPOINTS):
        codes.add("COMMANDER_RELIABILITY_CHECKPOINT_INVALID")
        return None

    return {
        checkpoint: by_checkpoint[checkpoint]
        for checkpoint in _TARGET_CHECKPOINTS
    }


def _compute_protection_coverage_proxy(
    *,
    commander_slot_id: str,
    primitive_index_by_slot: Any,
    deck_slot_ids_playable: Any,
) -> float | None:
    if not isinstance(primitive_index_by_slot, dict) or not isinstance(deck_slot_ids_playable, list):
        return None

    slot_ids = [
        slot_id
        for slot_id in _clean_sorted_unique_strings(deck_slot_ids_playable)
        if slot_id != commander_slot_id
    ]
    eligible_slots = len(slot_ids)
    if eligible_slots <= 0:
        return None

    protected_slots = 0
    for slot_id in slot_ids:
        slot_primitives = set(_clean_sorted_unique_strings(primitive_index_by_slot.get(slot_id)))
        if len(slot_primitives.intersection(_PROTECTION_PRIMITIVE_IDS)) > 0:
            protected_slots += 1

    return _round6_half_up(_clamp01(float(protected_slots) / float(eligible_slots)))


def run_commander_reliability_model_v1(
    *,
    commander_slot_id: Any,
    probability_checkpoint_layer_v1_payload: Any,
    stress_transform_engine_v1_payload: Any,
    engine_requirement_detection_v1_payload: Any,
    primitive_index_by_slot: Any = None,
    deck_slot_ids_playable: Any = None,
) -> Dict[str, Any]:
    commander_slot = _nonempty_str(commander_slot_id)
    commander_dependent = _extract_commander_dependency(engine_requirement_detection_v1_payload)

    if commander_slot is None:
        return _base_payload(
            status="SKIP",
            reason_code="COMMANDER_SLOT_UNAVAILABLE",
            codes=[],
            commander_dependent=commander_dependent,
            metrics=_default_metrics(),
            notes=[],
        )

    if not isinstance(probability_checkpoint_layer_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="PROBABILITY_CHECKPOINT_UNAVAILABLE",
            codes=[],
            commander_dependent=commander_dependent,
            metrics=_default_metrics(),
            notes=[],
        )

    baseline_status = _nonempty_str(probability_checkpoint_layer_v1_payload.get("status"))
    baseline_rows = probability_checkpoint_layer_v1_payload.get("probabilities_by_bucket")
    if baseline_status not in {"OK", "WARN"} or not isinstance(baseline_rows, list) or len(baseline_rows) == 0:
        return _base_payload(
            status="SKIP",
            reason_code="PROBABILITY_CHECKPOINT_UNAVAILABLE",
            codes=[],
            commander_dependent=commander_dependent,
            metrics=_default_metrics(),
            notes=[],
        )

    if not isinstance(stress_transform_engine_v1_payload, dict):
        return _base_payload(
            status="SKIP",
            reason_code="STRESS_TRANSFORM_UNAVAILABLE",
            codes=[],
            commander_dependent=commander_dependent,
            metrics=_default_metrics(),
            notes=[],
        )

    stress_status = _nonempty_str(stress_transform_engine_v1_payload.get("status"))
    stress_rows = stress_transform_engine_v1_payload.get("stress_adjusted_probabilities_by_bucket")
    if stress_status not in {"OK", "WARN"} or not isinstance(stress_rows, list) or len(stress_rows) == 0:
        return _base_payload(
            status="SKIP",
            reason_code="STRESS_TRANSFORM_UNAVAILABLE",
            codes=[],
            commander_dependent=commander_dependent,
            metrics=_default_metrics(),
            notes=[],
        )

    codes: Set[str] = set()
    notes: Set[str] = set()
    metrics = _default_metrics()

    baseline_ramp = _extract_ramp_probabilities_by_checkpoint(
        rows=baseline_rows,
        probability_row_key="probabilities_by_checkpoint",
        codes=codes,
    )
    stress_ramp = _extract_ramp_probabilities_by_checkpoint(
        rows=stress_rows,
        probability_row_key="probabilities_by_checkpoint",
        codes=codes,
    )

    if baseline_ramp is None:
        codes.add("COMMANDER_RELIABILITY_RAMP_BUCKET_UNAVAILABLE")
        notes.add("RAMP bucket probabilities unavailable in baseline checkpoint payload.")
    else:
        metrics["cast_reliability_t3"] = float(baseline_ramp[_CHECKPOINT_BY_TURN["t3"]])
        metrics["cast_reliability_t4"] = float(baseline_ramp[_CHECKPOINT_BY_TURN["t4"]])
        metrics["cast_reliability_t6"] = float(baseline_ramp[_CHECKPOINT_BY_TURN["t6"]])

    protection_proxy = _compute_protection_coverage_proxy(
        commander_slot_id=commander_slot,
        primitive_index_by_slot=primitive_index_by_slot,
        deck_slot_ids_playable=deck_slot_ids_playable,
    )
    if protection_proxy is None:
        codes.add("COMMANDER_RELIABILITY_PROTECTION_PROXY_UNAVAILABLE")
        notes.add("Protection coverage proxy unavailable from primitive index/playable slot inputs.")
        metrics["protection_coverage_proxy"] = None
    else:
        metrics["protection_coverage_proxy"] = protection_proxy

    if commander_dependent == "LOW":
        metrics["commander_fragility_delta"] = 0.0
    elif baseline_ramp is not None and stress_ramp is not None:
        baseline_mean = _mean([float(baseline_ramp[checkpoint]) for checkpoint in _TARGET_CHECKPOINTS])
        stress_mean = _mean([float(stress_ramp[checkpoint]) for checkpoint in _TARGET_CHECKPOINTS])

        if baseline_mean is None or stress_mean is None:
            codes.add("COMMANDER_RELIABILITY_FRAGILITY_UNAVAILABLE")
            notes.add("Commander fragility delta unavailable due to missing checkpoint probabilities.")
            metrics["commander_fragility_delta"] = None
        else:
            metrics["commander_fragility_delta"] = _round6_half_up(
                _clamp01(max(0.0, float(baseline_mean) - float(stress_mean)))
            )
    else:
        codes.add("COMMANDER_RELIABILITY_FRAGILITY_UNAVAILABLE")
        notes.add("Commander fragility delta unavailable because RAMP bucket probabilities were missing.")
        metrics["commander_fragility_delta"] = None

    if commander_dependent == "HIGH" and metrics["protection_coverage_proxy"] is None:
        notes.add("Commander dependency is HIGH while protection coverage proxy is unavailable.")

    status = "OK"
    if len(codes.intersection(_ERROR_CODES)) > 0:
        status = "ERROR"
    elif len(codes) > 0:
        status = "WARN"

    return _base_payload(
        status=status,
        reason_code=None,
        codes=sorted(codes),
        commander_dependent=commander_dependent,
        metrics=metrics,
        notes=sorted(notes),
    )
