from __future__ import annotations

from typing import Dict, List


def _round_metric_6(value: float) -> float:
    return float(f"{value:.6f}")


def _sorted_unique_strings(values: List[str]) -> List[str]:
    clean_values = [value.strip() for value in values if isinstance(value, str) and value.strip() != ""]
    return sorted(set(clean_values))


def _slot_primitives(slot_id: str, primitive_index_by_slot: Dict[str, List[str]]) -> List[str]:
    raw = primitive_index_by_slot.get(slot_id)
    if not isinstance(raw, list):
        return []
    return _sorted_unique_strings([value for value in raw if isinstance(value, str)])


def build_structural_snapshot_v1(
    *,
    snapshot_id: str,
    taxonomy_version: str,
    ruleset_version: str,
    profile_id: str,
    bracket_id: str | None,
    commander_slot_id: str,
    deck_slot_ids: list[str],
    primitive_index_by_slot: dict[str, list[str]],
    required_primitives: list[str],
    basic_land_slot_ids: list[str],
) -> dict:
    commander_slot_id_clean = commander_slot_id.strip() if isinstance(commander_slot_id, str) else ""
    deck_slot_ids_clean = _sorted_unique_strings([slot_id for slot_id in deck_slot_ids if isinstance(slot_id, str)])
    required_primitives_v1 = _sorted_unique_strings([primitive for primitive in required_primitives if isinstance(primitive, str)])
    basic_land_slot_ids_clean = _sorted_unique_strings(
        [slot_id for slot_id in basic_land_slot_ids if isinstance(slot_id, str)]
    )

    all_relevant_slot_ids = _sorted_unique_strings(
        deck_slot_ids_clean + ([commander_slot_id_clean] if commander_slot_id_clean != "" else [])
    )

    primitives_by_slot: Dict[str, List[str]] = {}
    primitive_set_by_slot: Dict[str, set[str]] = {}
    for slot_id in all_relevant_slot_ids:
        slot_primitives = _slot_primitives(slot_id, primitive_index_by_slot)
        primitives_by_slot[slot_id] = slot_primitives
        primitive_set_by_slot[slot_id] = set(slot_primitives)

    primitive_counts_temp: Dict[str, int] = {}
    present_primitives_set: set[str] = set()
    for slot_id in deck_slot_ids_clean:
        for primitive_id in primitives_by_slot.get(slot_id, []):
            present_primitives_set.add(primitive_id)
            primitive_counts_temp[primitive_id] = primitive_counts_temp.get(primitive_id, 0) + 1

    present_primitives_v1 = sorted(present_primitives_set)
    primitive_counts_by_id = {
        primitive_id: primitive_counts_temp[primitive_id]
        for primitive_id in sorted(primitive_counts_temp.keys())
    }

    missing_primitives_v1 = sorted(
        [
            primitive_id
            for primitive_id in required_primitives_v1
            if primitive_id not in present_primitives_set
        ]
    )

    total_primitive_slot_assignments = int(sum(primitive_counts_by_id.values()))
    if total_primitive_slot_assignments <= 0:
        primitive_concentration_index_v1 = 0.0
    else:
        primitive_concentration_index_v1 = _round_metric_6(
            sum(
                (count / total_primitive_slot_assignments) * (count / total_primitive_slot_assignments)
                for count in primitive_counts_by_id.values()
            )
        )

    basic_land_slot_id_set = set(basic_land_slot_ids_clean)
    dead_slot_ids_v1 = sorted(
        [
            slot_id
            for slot_id in deck_slot_ids_clean
            if slot_id != commander_slot_id_clean
            and slot_id not in basic_land_slot_id_set
            and len(primitives_by_slot.get(slot_id, [])) == 0
        ]
    )

    commander_only_count = 0
    for primitive_id in required_primitives_v1:
        containing_slots = [
            slot_id
            for slot_id in all_relevant_slot_ids
            if primitive_id in primitive_set_by_slot.get(slot_id, set())
        ]
        if len(containing_slots) == 1 and containing_slots[0] == commander_slot_id_clean:
            commander_only_count += 1

    if len(required_primitives_v1) == 0:
        commander_dependency_signal_v1 = 0.0
    else:
        commander_dependency_signal_v1 = _round_metric_6(
            commander_only_count / len(required_primitives_v1)
        )

    top_primitives = [
        {"primitive_id": primitive_id, "count": count}
        for primitive_id, count in sorted(
            primitive_counts_by_id.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]

    structural_health_summary_v1 = {
        "dead_slot_count": len(dead_slot_ids_v1),
        "missing_required_count": len(missing_primitives_v1),
        "top_primitives": top_primitives,
    }

    return {
        "snapshot_id": str(snapshot_id),
        "taxonomy_version": str(taxonomy_version),
        "ruleset_version": str(ruleset_version),
        "profile_id": str(profile_id),
        "bracket_id": bracket_id if isinstance(bracket_id, str) else None,
        "required_primitives_v1": required_primitives_v1,
        "present_primitives_v1": present_primitives_v1,
        "missing_primitives_v1": missing_primitives_v1,
        "primitive_counts_by_id": primitive_counts_by_id,
        "primitive_concentration_index_v1": primitive_concentration_index_v1,
        "dead_slot_ids_v1": dead_slot_ids_v1,
        "commander_dependency_signal_v1": commander_dependency_signal_v1,
        "structural_health_summary_v1": structural_health_summary_v1,
    }
