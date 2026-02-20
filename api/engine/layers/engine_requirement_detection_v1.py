from __future__ import annotations

from typing import Any, Dict, List, Set

from api.engine.dependency_signatures_v1 import load_dependency_signatures_v1


ENGINE_REQUIREMENT_DETECTION_V1_VERSION = "engine_requirement_detection_v1"


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    cleaned = {
        token
        for token in (_nonempty_str(value) for value in values)
        if token is not None
    }
    return sorted(cleaned)


def _base_payload(*, status: str, reason_code: str | None) -> Dict[str, Any]:
    return {
        "version": ENGINE_REQUIREMENT_DETECTION_V1_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": [],
        "unknowns": [],
        "engine_requirements_v1": {},
    }


def _sorted_codes(codes: Set[str]) -> List[str]:
    return sorted({code for code in codes if isinstance(code, str) and code.strip() != ""})


def _build_commander_dependency(
    *,
    primitive_index_by_slot: Dict[str, Any],
    commander_slot_id: str | None,
    codes: Set[str],
) -> str:
    if commander_slot_id is None:
        codes.add("COMMANDER_SLOT_ID_MISSING")
        return "UNKNOWN"

    commander_primitives = set(_clean_sorted_unique_strings(primitive_index_by_slot.get(commander_slot_id)))
    if len(commander_primitives) == 0:
        return "LOW"

    for slot_id in sorted(primitive_index_by_slot.keys(), key=lambda item: str(item)):
        if slot_id == commander_slot_id:
            continue
        slot_primitives = set(_clean_sorted_unique_strings(primitive_index_by_slot.get(slot_id)))
        if len(commander_primitives.intersection(slot_primitives)) > 0:
            return "MED"

    return "LOW"


def run_engine_requirement_detection_v1(
    primitive_index_by_slot: Any,
    slot_ids_by_primitive: Any,
    commander_slot_id: Any = None,
) -> dict:
    if not isinstance(primitive_index_by_slot, dict) or len(primitive_index_by_slot) == 0:
        return _base_payload(status="SKIP", reason_code="PRIMITIVE_INDEX_UNAVAILABLE")

    if not isinstance(slot_ids_by_primitive, dict) or len(slot_ids_by_primitive) == 0:
        return _base_payload(status="SKIP", reason_code="PRIMITIVE_INDEX_UNAVAILABLE")

    dependency_signatures_payload = load_dependency_signatures_v1()
    signatures = (
        dependency_signatures_payload.get("signatures")
        if isinstance(dependency_signatures_payload.get("signatures"), dict)
        else {}
    )

    slot_ids_by_primitive_clean: Dict[str, List[str]] = {}
    for primitive_id_raw in sorted(slot_ids_by_primitive.keys(), key=lambda item: str(item)):
        primitive_id = _nonempty_str(primitive_id_raw)
        if primitive_id is None:
            continue
        slot_ids_by_primitive_clean[primitive_id] = _clean_sorted_unique_strings(slot_ids_by_primitive.get(primitive_id_raw))

    codes: Set[str] = {
        "ENGINE_REQ_MANA_HUNGRY_UNIMPLEMENTED",
        "ENGINE_REQ_PERMANENT_TYPE_UNIMPLEMENTED",
        "ENGINE_REQ_SHUFFLE_UNIMPLEMENTED",
    }

    unknown_primitive_ids: Set[str] = set()
    signature_flags: Dict[str, bool] = {}

    for signature_name in sorted(signatures.keys(), key=lambda item: str(item)):
        signature_payload = signatures.get(signature_name)
        required_primitives = (
            signature_payload.get("any_required_primitives")
            if isinstance(signature_payload, dict)
            else []
        )
        primitive_ids = _clean_sorted_unique_strings(required_primitives)

        matched = False
        for primitive_id in primitive_ids:
            primitive_slot_ids = slot_ids_by_primitive_clean.get(primitive_id, [])
            has_slots = len(primitive_slot_ids) >= 1
            if has_slots:
                matched = True

            if primitive_id.startswith("UNKNOWN_PRIMITIVE_ID::") or not has_slots:
                unknown_primitive_ids.add(primitive_id)

        signature_flags[signature_name] = matched

    if len(unknown_primitive_ids) > 0:
        codes.add("UNKNOWN_PRIMITIVE_ID_IN_SIGNATURES")

    commander_slot = _nonempty_str(commander_slot_id)
    commander_dependent = _build_commander_dependency(
        primitive_index_by_slot=primitive_index_by_slot,
        commander_slot_id=commander_slot,
        codes=codes,
    )

    unknowns: List[Dict[str, Any]] = []
    if len(unknown_primitive_ids) > 0:
        unknowns.append(
            {
                "code": "UNKNOWN_PRIMITIVE_ID_IN_SIGNATURES",
                "primitive_ids": sorted(unknown_primitive_ids),
            }
        )

    unknowns_sorted = sorted(
        unknowns,
        key=lambda entry: (
            str(entry.get("code") or ""),
            "|".join(_clean_sorted_unique_strings(entry.get("primitive_ids"))),
        ),
    )

    engine_requirements_v1: Dict[str, Any] = {
        key: bool(signature_flags[key])
        for key in sorted(signature_flags.keys(), key=lambda item: str(item))
    }
    engine_requirements_v1["commander_dependent"] = commander_dependent
    engine_requirements_v1["mana_hungry"] = False
    engine_requirements_v1["requires_shuffle"] = False
    engine_requirements_v1["requires_specific_permanent_type"] = sorted([])

    codes_sorted = _sorted_codes(codes)
    status = "OK" if len(codes_sorted) == 0 and len(unknowns_sorted) == 0 else "WARN"

    return {
        "version": ENGINE_REQUIREMENT_DETECTION_V1_VERSION,
        "status": status,
        "reason_code": None,
        "codes": codes_sorted,
        "unknowns": unknowns_sorted,
        "engine_requirements_v1": engine_requirements_v1,
    }
