from __future__ import annotations

from typing import Any, Dict, List


REDUNDANCY_INDEX_V1_VERSION = "redundancy_index_v1"


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _round6(value: float) -> float:
    return float(round(float(value), 6))


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    cleaned = {
        token
        for token in (_nonempty_str(value) for value in values)
        if token is not None
    }
    return sorted(cleaned)


def _skip_payload(reason: str) -> Dict[str, Any]:
    return {
        "version": REDUNDANCY_INDEX_V1_VERSION,
        "status": "SKIP",
        "reason": reason,
        "per_requirement": [],
        "summary": {
            "avg_redundancy_ratio": None,
            "low_redundancy_count": 0,
            "unsupported_count": 0,
        },
        "notes": [],
    }


def _extract_requirements(required_effects_coverage: Any) -> List[Dict[str, Any]]:
    if not isinstance(required_effects_coverage, dict):
        return []

    raw_coverage = required_effects_coverage.get("coverage")
    if not isinstance(raw_coverage, list):
        return []

    merged: Dict[str, Dict[str, Any]] = {}
    for entry in raw_coverage:
        if not isinstance(entry, dict):
            continue

        primitive_id = _nonempty_str(entry.get("primitive"))
        if primitive_id is None:
            continue

        minimum = entry.get("min")
        if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum < 0:
            continue

        supported_raw = entry.get("supported")
        if isinstance(supported_raw, bool):
            supported = supported_raw
        else:
            supported = entry.get("count") is not None

        existing = merged.get(primitive_id)
        if not isinstance(existing, dict):
            merged[primitive_id] = {
                "primitive": primitive_id,
                "min": int(minimum),
                "supported": bool(supported),
            }
            continue

        existing["min"] = max(int(existing.get("min") or 0), int(minimum))
        existing["supported"] = bool(existing.get("supported") is True or supported)

    return [merged[primitive_id] for primitive_id in sorted(merged.keys(), key=lambda item: str(item))]


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


def _redundancy_level(*, count: int | None, redundancy_ratio: float | None) -> str | None:
    if count is None:
        return None
    if count == 0:
        return "NONE"
    if redundancy_ratio is None:
        return None
    if redundancy_ratio < 1.0:
        return "LOW"
    if redundancy_ratio < 1.5:
        return "OK"
    return "HIGH"


def _sort_notes(notes: List[Dict[str, str]]) -> List[Dict[str, str]]:
    unique_pairs = {
        (str(entry.get("code") or ""), str(entry.get("message") or ""))
        for entry in notes
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


def run_redundancy_index_v1(
    required_effects_coverage: Any,
    primitive_index_by_slot: Any,
    deck_slot_ids_playable: Any,
) -> dict:
    if not isinstance(required_effects_coverage, dict):
        return _skip_payload("REQUIRED_EFFECTS_COVERAGE_MISSING")

    if not isinstance(primitive_index_by_slot, dict) or not isinstance(deck_slot_ids_playable, list):
        return _skip_payload("PRIMITIVE_INDEX_UNAVAILABLE")

    requirements = _extract_requirements(required_effects_coverage)
    playable_slot_ids = _clean_sorted_unique_strings(deck_slot_ids_playable)

    per_requirement: List[Dict[str, Any]] = []
    notes: List[Dict[str, str]] = []

    ratios_for_average: List[float] = []
    low_redundancy_count = 0
    unsupported_count = 0

    for requirement in requirements:
        primitive_id = str(requirement["primitive"])
        minimum = int(requirement["min"])
        supported = bool(requirement["supported"])

        if not supported:
            unsupported_count += 1
            per_requirement.append(
                {
                    "primitive": primitive_id,
                    "min": minimum,
                    "count": None,
                    "supported": False,
                    "redundancy_ratio": None,
                    "redundancy_level": None,
                }
            )
            notes.append(
                {
                    "code": "REDUNDANCY_PRIMITIVE_UNSUPPORTED",
                    "message": (
                        f"Primitive '{primitive_id}' is marked unsupported by required effects coverage definitions."
                    ),
                }
            )
            continue

        count = _count_slots_with_primitive(
            primitive_index_by_slot=primitive_index_by_slot,
            playable_slot_ids=playable_slot_ids,
            primitive_id=primitive_id,
        )

        redundancy_ratio = None
        if minimum > 0:
            redundancy_ratio = _round6(float(count) / float(minimum))
            ratios_for_average.append(redundancy_ratio)

        redundancy_level = _redundancy_level(count=count, redundancy_ratio=redundancy_ratio)

        if redundancy_ratio is not None and redundancy_ratio < 1.0:
            low_redundancy_count += 1
            notes.append(
                {
                    "code": "REDUNDANCY_BELOW_MIN",
                    "message": (
                        f"Primitive '{primitive_id}' is below minimum redundancy ({count}/{minimum})."
                    ),
                }
            )

        per_requirement.append(
            {
                "primitive": primitive_id,
                "min": minimum,
                "count": int(count),
                "supported": True,
                "redundancy_ratio": redundancy_ratio,
                "redundancy_level": redundancy_level,
            }
        )

    per_requirement_sorted = sorted(per_requirement, key=lambda entry: str(entry.get("primitive") or ""))
    notes_sorted = _sort_notes(notes)

    avg_redundancy_ratio = None
    if len(ratios_for_average) > 0:
        avg_redundancy_ratio = _round6(sum(ratios_for_average) / float(len(ratios_for_average)))

    status = "OK"
    if low_redundancy_count > 0 or unsupported_count > 0:
        status = "WARN"

    return {
        "version": REDUNDANCY_INDEX_V1_VERSION,
        "status": status,
        "reason": None,
        "per_requirement": per_requirement_sorted,
        "summary": {
            "avg_redundancy_ratio": avg_redundancy_ratio,
            "low_redundancy_count": int(low_redundancy_count),
            "unsupported_count": int(unsupported_count),
        },
        "notes": notes_sorted,
    }
