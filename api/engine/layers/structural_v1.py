from typing import Any, Dict, List


def run_structural_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Structural reporting layer (structural_v1).
    Reporting only. Must preserve exact output + ordering.
    """

    deck_cards_canonical_input_order = state["deck_cards_canonical_input_order"]
    commander_canonical_slot = state["commander_canonical_slot"]
    primitive_index_by_slot = state["primitive_index_by_slot"]
    effective_generic_minimums = state["effective_generic_minimums"]

    deck_all_slot_ids = [entry.get("slot_id") for entry in deck_cards_canonical_input_order if isinstance(entry.get("slot_id"), str)]
    deck_playable_slot_ids = [
        entry.get("slot_id")
        for entry in deck_cards_canonical_input_order
        if isinstance(entry.get("slot_id"), str) and entry.get("status") == "PLAYABLE"
    ]
    commander_slots = 1 if commander_canonical_slot.get("input") else 0

    def count_primitives_for_slots(slot_ids: List[str]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for sid in slot_ids:
            for primitive in primitive_index_by_slot.get(sid, []):
                counts[primitive] = counts.get(primitive, 0) + 1
        return {k: counts[k] for k in sorted(counts.keys())}

    commander_only_slot_ids = ["C0"] if commander_slots == 1 else []
    commander_plus_deck_playable_slot_ids = commander_only_slot_ids + deck_playable_slot_ids

    primitive_counts_by_scope = {
        "commander_only": count_primitives_for_slots(commander_only_slot_ids),
        "deck_all_slots": count_primitives_for_slots(deck_all_slot_ids),
        "deck_playable_slots": count_primitives_for_slots(deck_playable_slot_ids),
        "commander_plus_deck_playable": count_primitives_for_slots(commander_plus_deck_playable_slot_ids),
    }
    primitive_counts_by_scope_totals = {
        "commander_slots": commander_slots,
        "deck_slots_total": len(deck_all_slot_ids),
        "deck_slots_playable_total": len(deck_playable_slot_ids),
        "combined_slots_total": commander_slots + len(deck_playable_slot_ids),
    }

    deck_playable_counts = primitive_counts_by_scope.get("deck_playable_slots", {})
    required_primitives_v0: List[Dict[str, Any]] = []
    for primitive in sorted(effective_generic_minimums.keys()):
        min_required = effective_generic_minimums[primitive]
        have_playable = int(deck_playable_counts.get(primitive, 0))
        deficit = max(min_required - have_playable, 0)
        required_primitives_v0.append(
            {
                "primitive": primitive,
                "min_required": min_required,
                "have_playable": have_playable,
                "deficit": deficit,
                "meets_minimum": have_playable >= min_required,
            }
        )

    overrepresentation_flags: List[Dict[str, Any]] = []
    for primitive in sorted(deck_playable_counts.keys()):
        min_required = effective_generic_minimums.get(primitive)
        if min_required is None:
            continue
        threshold = min_required * 2
        count = int(deck_playable_counts.get(primitive, 0))
        if count > threshold:
            overrepresentation_flags.append(
                {
                    "primitive": primitive,
                    "count": count,
                    "threshold": threshold,
                }
            )

    structural_coverage = {
        "required_primitives_v0": required_primitives_v0,
        "overrepresentation_flags": overrepresentation_flags,
    }

    commander_primitives_sorted = list(primitive_index_by_slot.get("C0", []))
    commander_primitives_set = set(commander_primitives_sorted)
    deck_slots_with_overlap = 0
    for sid in deck_playable_slot_ids:
        slot_primitives = primitive_index_by_slot.get(sid, [])
        if commander_primitives_set.intersection(slot_primitives):
            deck_slots_with_overlap += 1

    deck_playable_slots_total = len(deck_playable_slot_ids)
    overlap_ratio = round(deck_slots_with_overlap / max(deck_playable_slots_total, 1), 3)
    commander_dependency_signal = {
        "commander_primitives": commander_primitives_sorted,
        "deck_slots_with_overlap": deck_slots_with_overlap,
        "deck_playable_slots_total": deck_playable_slots_total,
        "overlap_ratio": overlap_ratio,
    }

    dead_slot_ids = [sid for sid in deck_playable_slot_ids if not primitive_index_by_slot.get(sid)]

    total_primitive_occurrences_playable = int(sum(deck_playable_counts.values()))
    if total_primitive_occurrences_playable == 0:
        primitive_concentration_index = 0.0
    else:
        primitive_concentration_index = round(
            sum(
                (count / total_primitive_occurrences_playable) * (count / total_primitive_occurrences_playable)
                for count in deck_playable_counts.values()
            ),
            4,
        )

    structural_snapshot_v1 = {
        "playable_deck_slots": deck_playable_slots_total,
        "unique_primitives_playable": len(deck_playable_counts),
        "total_primitive_occurrences_playable": total_primitive_occurrences_playable,
        "commander_overlap_ratio": overlap_ratio,
        "primitive_concentration_index": primitive_concentration_index,
    }

    state["primitive_counts_by_scope"] = primitive_counts_by_scope
    state["primitive_counts_by_scope_totals"] = primitive_counts_by_scope_totals
    state["structural_coverage"] = structural_coverage
    state["commander_dependency_signal"] = commander_dependency_signal
    state["dead_slot_ids"] = dead_slot_ids
    state["primitive_concentration_index"] = primitive_concentration_index
    state["structural_snapshot_v1"] = structural_snapshot_v1

    return state
