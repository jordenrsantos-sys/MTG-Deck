from typing import Any, Dict, List


def run_structural_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Legacy structural reporting layer (structural_v1).
    Deprecated: retained only for backward-compatibility diagnostics.
    Runtime downstream consumers must use structural_snapshot_v1.
    Reporting only. Must preserve exact output + ordering.
    """

    deck_cards_canonical_input_order = state["deck_cards_canonical_input_order"]
    commander_canonical_slot = state["commander_canonical_slot"]
    primitive_index_by_slot = state["primitive_index_by_slot"]

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

    state["primitive_counts_by_scope"] = primitive_counts_by_scope
    state["primitive_counts_by_scope_totals"] = primitive_counts_by_scope_totals

    return state
