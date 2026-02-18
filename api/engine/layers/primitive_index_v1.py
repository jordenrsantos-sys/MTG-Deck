from typing import Any, Dict, List


def run_primitive_index_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministic primitive index layer.
    Must preserve ordering exactly as pipeline_build currently emits it.
    """

    commander_resolved = state["commander_resolved"]
    primitive_overrides_by_oracle = state["primitive_overrides_by_oracle"]
    get_overridden_primitives_for_oracle = state["get_overridden_primitives_for_oracle"]
    deck_cards_canonical_input_order = state["deck_cards_canonical_input_order"]
    slot_primitives_source_by_slot_id = state["slot_primitives_source_by_slot_id"]
    canonical_slots_all = state["canonical_slots_all"]
    normalize_primitives_source = state["normalize_primitives_source"]

    commander_primitives_source = None
    if commander_resolved is not None:
        commander_primitives_source = (
            commander_resolved.get("primitives")
            if commander_resolved.get("primitives") is not None
            else commander_resolved.get("primitives_json")
        )

    if commander_resolved is not None and isinstance(commander_resolved.get("oracle_id"), str):
        commander_oracle_id_for_override = commander_resolved.get("oracle_id")
        if commander_oracle_id_for_override in primitive_overrides_by_oracle:
            commander_primitives_source = get_overridden_primitives_for_oracle(
                commander_oracle_id_for_override,
                commander_primitives_source,
            )

    for entry in deck_cards_canonical_input_order:
        slot_id = entry.get("slot_id")
        oracle_id = entry.get("resolved_oracle_id")
        if not isinstance(slot_id, str) or not isinstance(oracle_id, str):
            continue
        if entry.get("status") != "PLAYABLE":
            continue
        if oracle_id not in primitive_overrides_by_oracle:
            continue
        original_slot_source = slot_primitives_source_by_slot_id.get(slot_id)
        slot_primitives_source_by_slot_id[slot_id] = get_overridden_primitives_for_oracle(
            oracle_id,
            original_slot_source,
        )

    primitive_index_by_slot: Dict[str, List[str]] = {}
    primitive_to_slots_temp: Dict[str, List[str]] = {}
    for entry in canonical_slots_all:
        slot_id = entry.get("slot_id")
        if not isinstance(slot_id, str):
            continue
        if slot_id == "C0":
            source = commander_primitives_source
        else:
            source = slot_primitives_source_by_slot_id.get(slot_id)

        primitives_for_slot = normalize_primitives_source(source)
        primitive_index_by_slot[slot_id] = primitives_for_slot
        for primitive in primitives_for_slot:
            primitive_to_slots_temp.setdefault(primitive, []).append(slot_id)

    slot_ids_by_primitive = {
        primitive: primitive_to_slots_temp[primitive] for primitive in sorted(primitive_to_slots_temp.keys())
    }
    primitive_index_totals = {
        "total_slots": len(canonical_slots_all),
        "slots_with_primitives": sum(1 for vals in primitive_index_by_slot.values() if vals),
        "unique_primitives_total": len(slot_ids_by_primitive),
    }

    state["slot_primitives_source_by_slot_id"] = slot_primitives_source_by_slot_id
    state["primitive_index_by_slot"] = primitive_index_by_slot
    state["slot_ids_by_primitive"] = slot_ids_by_primitive
    state["primitive_index_totals"] = primitive_index_totals

    return state
