from __future__ import annotations

from typing import Any, Dict, List


REQUIRED_EFFECTS_COVERAGE_V1_VERSION = "required_effects_coverage_v1"


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


def _base_payload(*, status: str, requirements_version: str, reason: str | None) -> Dict[str, Any]:
    return {
        "version": REQUIRED_EFFECTS_COVERAGE_V1_VERSION,
        "status": status,
        "reason": reason,
        "requirements_version": requirements_version,
        "coverage": [],
        "missing": [],
        "unknowns": [],
    }


def _extract_requirements(requirements_dict: Any) -> Dict[str, int]:
    if not isinstance(requirements_dict, dict):
        return {}

    raw_requirements = requirements_dict.get("requirements") if "requirements" in requirements_dict else requirements_dict
    if not isinstance(raw_requirements, dict):
        return {}

    normalized: Dict[str, int] = {}
    for primitive_key in sorted(raw_requirements.keys(), key=lambda item: str(item)):
        primitive_id = _nonempty_str(primitive_key)
        if primitive_id is None:
            continue

        raw_value = raw_requirements.get(primitive_key)
        if isinstance(raw_value, dict):
            raw_value = raw_value.get("min")

        if isinstance(raw_value, int) and not isinstance(raw_value, bool) and raw_value >= 0:
            normalized[primitive_id] = int(raw_value)

    return normalized


def _extract_taxonomy_primitive_ids(requirements_dict: Any) -> set[str]:
    if not isinstance(requirements_dict, dict):
        return set()

    raw = requirements_dict.get("taxonomy_primitive_ids")
    if not isinstance(raw, list):
        return set()

    return set(_clean_sorted_unique_strings(raw))


def _count_slots_with_primitive(
    *,
    primitive_index_by_slot: Dict[str, Any],
    playable_slot_ids: List[str],
    primitive_id: str,
) -> int:
    count = 0
    for slot_id in playable_slot_ids:
        slot_primitives = _clean_sorted_unique_strings(primitive_index_by_slot.get(slot_id))
        if primitive_id in set(slot_primitives):
            count += 1
    return int(count)


def _sort_unknowns(unknowns: List[Dict[str, str]]) -> List[Dict[str, str]]:
    unique_pairs = {
        (str(entry.get("code") or ""), str(entry.get("message") or ""))
        for entry in unknowns
        if isinstance(entry, dict)
    }
    return [
        {
            "code": code,
            "message": message,
        }
        for code, message in sorted(unique_pairs, key=lambda item: (item[0], item[1]))
        if code != "" and message != ""
    ]


def run_required_effects_coverage_v1(
    deck_slot_ids_playable: Any,
    primitive_index_by_slot: Any,
    format: str,
    requirements_dict: Any,
    requirements_version: Any,
) -> dict:
    _ = format

    requirements_version_token = _nonempty_str(requirements_version) or "required_effects_v1"

    if not isinstance(primitive_index_by_slot, dict) or not isinstance(deck_slot_ids_playable, list):
        return _base_payload(
            status="SKIP",
            requirements_version=requirements_version_token,
            reason="PRIMITIVE_INDEX_UNAVAILABLE",
        )

    playable_slot_ids = _clean_sorted_unique_strings(deck_slot_ids_playable)
    requirements = _extract_requirements(requirements_dict)
    taxonomy_primitive_ids = _extract_taxonomy_primitive_ids(requirements_dict)

    coverage: List[Dict[str, Any]] = []
    missing: List[Dict[str, Any]] = []
    unknowns: List[Dict[str, str]] = []

    for primitive_id in sorted(requirements.keys(), key=lambda item: str(item)):
        minimum = int(requirements[primitive_id])
        supported = primitive_id in taxonomy_primitive_ids

        if not supported:
            coverage.append(
                {
                    "primitive": primitive_id,
                    "min": minimum,
                    "count": None,
                    "supported": False,
                    "met": None,
                }
            )
            unknowns.append(
                {
                    "code": "REQUIRED_PRIMITIVE_UNSUPPORTED",
                    "message": (
                        f"Required primitive '{primitive_id}' is unsupported by runtime taxonomy coverage definitions."
                    ),
                }
            )
            continue

        count = _count_slots_with_primitive(
            primitive_index_by_slot=primitive_index_by_slot,
            playable_slot_ids=playable_slot_ids,
            primitive_id=primitive_id,
        )
        met = bool(count >= minimum)

        coverage.append(
            {
                "primitive": primitive_id,
                "min": minimum,
                "count": int(count),
                "supported": True,
                "met": met,
            }
        )

        if not met:
            missing.append(
                {
                    "primitive": primitive_id,
                    "min": minimum,
                    "count": int(count),
                }
            )

    coverage_sorted = sorted(coverage, key=lambda entry: str(entry.get("primitive") or ""))
    missing_sorted = sorted(missing, key=lambda entry: str(entry.get("primitive") or ""))
    unknowns_sorted = _sort_unknowns(unknowns)

    status = "OK"
    if len(missing_sorted) > 0 or len(unknowns_sorted) > 0:
        status = "WARN"

    return {
        "version": REQUIRED_EFFECTS_COVERAGE_V1_VERSION,
        "status": status,
        "reason": None,
        "requirements_version": requirements_version_token,
        "coverage": coverage_sorted,
        "missing": missing_sorted,
        "unknowns": unknowns_sorted,
    }
