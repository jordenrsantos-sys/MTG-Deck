from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from math import floor
from typing import Any, Dict, List, Set


SUBSTITUTION_ENGINE_V1_VERSION = "substitution_engine_v1"


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


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    cleaned = {
        token
        for token in (_nonempty_str(item) for item in values)
        if token is not None
    }
    return sorted(cleaned)


def _normalize_slot_primitive_sets(
    *,
    primitive_index_by_slot: Dict[str, Any],
    deck_slot_ids_playable: List[str],
) -> Dict[str, Set[str]]:
    playable_slot_ids = _clean_sorted_unique_strings(deck_slot_ids_playable)
    primitive_sets_by_slot: Dict[str, Set[str]] = {}
    for slot_id in playable_slot_ids:
        primitives = _clean_sorted_unique_strings(primitive_index_by_slot.get(slot_id))
        primitive_sets_by_slot[slot_id] = set(primitives)
    return primitive_sets_by_slot


def _count_slots_with_any_primitives(
    *,
    primitive_sets_by_slot: Dict[str, Set[str]],
    primitives: List[str],
) -> int:
    primitive_set = set(primitives)
    if len(primitive_set) == 0:
        return 0

    count = 0
    for slot_id in sorted(primitive_sets_by_slot.keys(), key=lambda item: str(item)):
        slot_primitives = primitive_sets_by_slot.get(slot_id) or set()
        if len(slot_primitives.intersection(primitive_set)) > 0:
            count += 1
    return int(count)


def _count_slots_with_primitive(*, primitive_sets_by_slot: Dict[str, Set[str]], primitive: str) -> int:
    count = 0
    for slot_id in sorted(primitive_sets_by_slot.keys(), key=lambda item: str(item)):
        slot_primitives = primitive_sets_by_slot.get(slot_id) or set()
        if primitive in slot_primitives:
            count += 1
    return int(count)


def _skip_payload(*, reason_code: str, substitutions_version: str | None, format_token: str) -> Dict[str, Any]:
    return {
        "version": SUBSTITUTION_ENGINE_V1_VERSION,
        "status": "SKIP",
        "reason_code": reason_code,
        "codes": [],
        "substitutions_version": substitutions_version,
        "format": format_token,
        "buckets": [],
    }


def _extract_requirement_flags(engine_requirement_detection_v1_payload: Any) -> tuple[Dict[str, Any], bool]:
    if not isinstance(engine_requirement_detection_v1_payload, dict):
        return {}, False

    engine_requirements = engine_requirement_detection_v1_payload.get("engine_requirements_v1")
    if not isinstance(engine_requirements, dict):
        return {}, False

    return dict(engine_requirements), True


def run_substitution_engine_v1(
    *,
    primitive_index_by_slot: Any,
    deck_slot_ids_playable: Any,
    engine_requirement_detection_v1_payload: Any,
    format: Any,
    bucket_substitutions_payload: Any,
) -> Dict[str, Any]:
    format_token = _nonempty_str(format) or ""

    if not isinstance(bucket_substitutions_payload, dict):
        return _skip_payload(
            reason_code="BUCKET_SUBSTITUTIONS_UNAVAILABLE",
            substitutions_version=None,
            format_token=format_token,
        )

    substitutions_version = _nonempty_str(bucket_substitutions_payload.get("version"))
    format_defaults = bucket_substitutions_payload.get("format_defaults")
    if not isinstance(format_defaults, dict):
        return _skip_payload(
            reason_code="BUCKET_SUBSTITUTIONS_UNAVAILABLE",
            substitutions_version=substitutions_version,
            format_token=format_token,
        )

    if not isinstance(primitive_index_by_slot, dict) or not isinstance(deck_slot_ids_playable, list):
        return _skip_payload(
            reason_code="PRIMITIVE_INDEX_UNAVAILABLE",
            substitutions_version=substitutions_version,
            format_token=format_token,
        )

    format_entry = format_defaults.get(format_token)
    if not isinstance(format_entry, dict):
        format_entry = format_defaults.get(format_token.lower()) if isinstance(format_token, str) else None

    if not isinstance(format_entry, dict):
        return _skip_payload(
            reason_code="FORMAT_BUCKET_SUBSTITUTIONS_UNAVAILABLE",
            substitutions_version=substitutions_version,
            format_token=format_token,
        )

    buckets_payload = format_entry.get("buckets")
    if not isinstance(buckets_payload, dict):
        return _skip_payload(
            reason_code="FORMAT_BUCKET_SUBSTITUTIONS_UNAVAILABLE",
            substitutions_version=substitutions_version,
            format_token=format_token,
        )

    requirement_flags, has_requirement_flags = _extract_requirement_flags(engine_requirement_detection_v1_payload)

    codes: Set[str] = set()
    if not has_requirement_flags:
        codes.add("ENGINE_REQUIREMENTS_UNAVAILABLE")

    primitive_sets_by_slot = _normalize_slot_primitive_sets(
        primitive_index_by_slot=primitive_index_by_slot,
        deck_slot_ids_playable=deck_slot_ids_playable,
    )

    bucket_rows: List[Dict[str, Any]] = []

    for bucket_key_raw in sorted(buckets_payload.keys(), key=lambda item: str(item)):
        bucket_key = _nonempty_str(bucket_key_raw)
        if bucket_key is None:
            continue

        bucket_payload = buckets_payload.get(bucket_key_raw)
        if not isinstance(bucket_payload, dict):
            continue

        primary_primitives = _clean_sorted_unique_strings(bucket_payload.get("primary_primitives"))
        k_primary = _count_slots_with_any_primitives(
            primitive_sets_by_slot=primitive_sets_by_slot,
            primitives=primary_primitives,
        )

        base_rows_raw = bucket_payload.get("base_substitutions")
        base_rows = base_rows_raw if isinstance(base_rows_raw, list) else []

        conditional_rows_raw = bucket_payload.get("conditional_substitutions")
        conditional_rows = conditional_rows_raw if isinstance(conditional_rows_raw, list) else []

        active_rows: List[Dict[str, Any]] = []
        active_requirement_flags: Set[str] = set()

        for row in base_rows:
            if not isinstance(row, dict):
                continue
            primitive = _nonempty_str(row.get("primitive"))
            weight = row.get("weight")
            if primitive is None or isinstance(weight, bool) or not isinstance(weight, (int, float)):
                continue
            active_rows.append({"primitive": primitive, "weight": float(weight)})

        for conditional_row in conditional_rows:
            if not isinstance(conditional_row, dict):
                continue

            requirement_flag = _nonempty_str(conditional_row.get("requirement_flag"))
            if requirement_flag is None:
                continue

            requirement_value = requirement_flags.get(requirement_flag)
            if not isinstance(requirement_value, bool):
                codes.add("SUBSTITUTION_REQUIREMENT_FLAG_UNAVAILABLE")
                continue

            if requirement_value is not True:
                continue

            active_requirement_flags.add(requirement_flag)
            substitutions = conditional_row.get("substitutions")
            substitutions_rows = substitutions if isinstance(substitutions, list) else []
            for row in substitutions_rows:
                if not isinstance(row, dict):
                    continue
                primitive = _nonempty_str(row.get("primitive"))
                weight = row.get("weight")
                if primitive is None or isinstance(weight, bool) or not isinstance(weight, (int, float)):
                    continue
                active_rows.append({"primitive": primitive, "weight": float(weight)})

        aggregated_weights: Dict[str, float] = {}
        for row in active_rows:
            primitive = str(row.get("primitive") or "")
            if primitive == "":
                continue
            aggregated_weights[primitive] = float(aggregated_weights.get(primitive, 0.0)) + float(row.get("weight") or 0.0)

        substitution_terms: List[Dict[str, Any]] = []
        effective_k_raw = float(k_primary)

        for primitive in sorted(aggregated_weights.keys(), key=lambda item: str(item)):
            weight = float(aggregated_weights[primitive])
            k_substitute = _count_slots_with_primitive(
                primitive_sets_by_slot=primitive_sets_by_slot,
                primitive=primitive,
            )
            contribution = float(weight) * float(k_substitute)
            effective_k_raw += contribution

            substitution_terms.append(
                {
                    "primitive": primitive,
                    "weight": _round6_half_up(weight),
                    "k_substitute": int(k_substitute),
                    "contribution": _round6_half_up(contribution),
                }
            )

        effective_k = _round6_half_up(_clamp_to_deck_size(effective_k_raw))
        k_int = int(floor(effective_k))

        bucket_rows.append(
            {
                "bucket": bucket_key,
                "k_primary": int(k_primary),
                "effective_K": effective_k,
                "K_int": k_int,
                "active_requirement_flags": sorted(active_requirement_flags),
                "substitution_terms": substitution_terms,
            }
        )

    status = "OK"
    codes_sorted = sorted(codes)
    if len(codes_sorted) > 0:
        status = "WARN"

    return {
        "version": SUBSTITUTION_ENGINE_V1_VERSION,
        "status": status,
        "reason_code": None,
        "codes": codes_sorted,
        "substitutions_version": substitutions_version,
        "format": format_token,
        "buckets": bucket_rows,
    }
