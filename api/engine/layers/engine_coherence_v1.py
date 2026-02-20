from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Set


ENGINE_COHERENCE_V1_VERSION = "engine_coherence_v1"


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


def _round6_half_up(value: float) -> float:
    return float(Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _sorted_codes(codes: Set[str]) -> List[str]:
    return sorted({code for code in codes if isinstance(code, str) and code.strip() != ""})


def _base_payload(*, status: str, reason_code: str | None) -> Dict[str, Any]:
    return {
        "version": ENGINE_COHERENCE_V1_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": [],
        "summary": {
            "playable_slots_total": 0,
            "non_dead_slots_total": 0,
            "dead_slots_total": 0,
            "dead_slot_ratio": 0.0,
            "primitive_concentration_index": 0.0,
            "overlap_score": 0.0,
        },
        "dead_slots": [],
        "top_primitive_concentration": [],
    }


def run_engine_coherence_v1(
    primitive_index_by_slot: Any,
    deck_slot_ids_playable: Any,
) -> dict:
    if not isinstance(primitive_index_by_slot, dict) or not isinstance(deck_slot_ids_playable, list):
        return _base_payload(status="SKIP", reason_code="PRIMITIVE_INDEX_UNAVAILABLE")

    playable_slot_ids = _clean_sorted_unique_strings(deck_slot_ids_playable)
    slot_primitives_by_slot: Dict[str, List[str]] = {
        slot_id: _clean_sorted_unique_strings(primitive_index_by_slot.get(slot_id))
        for slot_id in playable_slot_ids
    }

    dead_slots: List[Dict[str, Any]] = []
    non_dead_slot_ids: List[str] = []

    for slot_id in playable_slot_ids:
        primitives = slot_primitives_by_slot.get(slot_id, [])
        if len(primitives) == 0:
            dead_slots.append(
                {
                    "slot_id": slot_id,
                    "primitive_count": 0,
                    "primitives": [],
                }
            )
        else:
            non_dead_slot_ids.append(slot_id)

    primitive_coverage: Dict[str, int] = {}
    for slot_id in non_dead_slot_ids:
        for primitive_id in slot_primitives_by_slot.get(slot_id, []):
            primitive_coverage[primitive_id] = primitive_coverage.get(primitive_id, 0) + 1

    non_dead_slots_total = len(non_dead_slot_ids)
    top_primitive_concentration: List[Dict[str, Any]] = []

    if non_dead_slots_total > 0:
        top_primitive_concentration = [
            {
                "primitive": primitive_id,
                "slots_with_primitive": int(slot_count),
                "share": _round6_half_up(float(slot_count) / float(non_dead_slots_total)),
            }
            for primitive_id, slot_count in primitive_coverage.items()
        ]
        top_primitive_concentration = sorted(
            top_primitive_concentration,
            key=lambda entry: (-float(entry.get("share") or 0.0), str(entry.get("primitive") or "")),
        )[:8]

    primitive_concentration_index = (
        float(top_primitive_concentration[0].get("share") or 0.0)
        if len(top_primitive_concentration) > 0
        else 0.0
    )

    pair_scores: List[float] = []
    for left_idx in range(len(non_dead_slot_ids)):
        for right_idx in range(left_idx + 1, len(non_dead_slot_ids)):
            left_slot_id = non_dead_slot_ids[left_idx]
            right_slot_id = non_dead_slot_ids[right_idx]

            left_primitives = set(slot_primitives_by_slot.get(left_slot_id, []))
            right_primitives = set(slot_primitives_by_slot.get(right_slot_id, []))

            union_size = len(left_primitives.union(right_primitives))
            if union_size <= 0:
                pair_scores.append(0.0)
                continue

            shared_size = len(left_primitives.intersection(right_primitives))
            pair_scores.append(float(shared_size) / float(union_size))

    overlap_score = 0.0
    if len(pair_scores) > 0:
        overlap_score = _round6_half_up(sum(pair_scores) / float(len(pair_scores)))

    playable_slots_total = len(playable_slot_ids)
    dead_slots_total = len(dead_slots)
    dead_slot_ratio = 0.0
    if playable_slots_total > 0:
        dead_slot_ratio = _round6_half_up(float(dead_slots_total) / float(playable_slots_total))

    codes: Set[str] = set()
    if dead_slots_total > 0:
        codes.add("DEAD_SLOTS_PRESENT")

    codes_sorted = _sorted_codes(codes)

    return {
        "version": ENGINE_COHERENCE_V1_VERSION,
        "status": "WARN" if len(codes_sorted) > 0 else "OK",
        "reason_code": None,
        "codes": codes_sorted,
        "summary": {
            "playable_slots_total": int(playable_slots_total),
            "non_dead_slots_total": int(non_dead_slots_total),
            "dead_slots_total": int(dead_slots_total),
            "dead_slot_ratio": dead_slot_ratio,
            "primitive_concentration_index": _round6_half_up(primitive_concentration_index),
            "overlap_score": overlap_score,
        },
        "dead_slots": sorted(dead_slots, key=lambda entry: str(entry.get("slot_id") or "")),
        "top_primitive_concentration": top_primitive_concentration,
    }
