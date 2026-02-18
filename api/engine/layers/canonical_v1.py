import json
from typing import Any, Dict, List


def run_canonical_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pure deterministic canonical resolution layer.
    Must not mutate external globals.
    Must return updated state.
    """

    req = state["req"]
    unknowns = state["unknowns"]
    playable_cards_resolved = state["playable_cards_resolved"]
    resolved_oracle_id_queues = state["resolved_oracle_id_queues"]
    resolved_card_ci_queues = state["resolved_card_ci_queues"]
    resolved_format_status_queues = state["resolved_format_status_queues"]
    resolved_card_primitives_queues = state["resolved_card_primitives_queues"]
    commander_resolved = state["commander_resolved"]
    get_format_legality = state["get_format_legality"]
    make_slot_id = state["make_slot_id"]

    NONPLAYABLE_CODES = {"ILLEGAL_CARD", "COLOR_IDENTITY_VIOLATION", "DUPLICATE_CARD"}

    unknown_cards = sorted(
        [u["input"] for u in unknowns if u.get("code") == "UNKNOWN_CARD" and u.get("input")]
    )

    nonplayable_map: dict[str, set[str]] = {}
    for u in unknowns:
        code = u.get("code")
        name = u.get("input")
        if code in NONPLAYABLE_CODES and name:
            nonplayable_map.setdefault(name, set()).add(code)

    deck_cards_nonplayable = [
        {"name": name, "codes": sorted(list(codes))}
        for name, codes in sorted(nonplayable_map.items(), key=lambda x: x[0])
    ]

    deck_cards_playable = sorted([c["name"] for c in playable_cards_resolved if c.get("name")])

    # Canonical per-input-slot view (preserves submitted order, including duplicates)
    playable_remaining: Dict[str, int] = {}
    for c in playable_cards_resolved:
        n = c.get("name")
        if n:
            playable_remaining[n] = playable_remaining.get(n, 0) + 1

    unknown_remaining: Dict[str, int] = {}
    illegal_remaining: Dict[str, int] = {}
    ci_remaining: Dict[str, int] = {}
    duplicate_remaining: Dict[str, int] = {}
    for u in unknowns:
        code = u.get("code")
        name = u.get("input")
        if not name:
            continue
        if code == "UNKNOWN_CARD":
            unknown_remaining[name] = unknown_remaining.get(name, 0) + 1
        elif code == "ILLEGAL_CARD":
            illegal_remaining[name] = illegal_remaining.get(name, 0) + 1
        elif code == "COLOR_IDENTITY_VIOLATION":
            ci_remaining[name] = ci_remaining.get(name, 0) + 1
        elif code == "DUPLICATE_CARD":
            duplicate_remaining[name] = duplicate_remaining.get(name, 0) + 1

    def normalize_ci(value: Any) -> List[str]:
        if value is None:
            return []
        parsed = value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except (TypeError, ValueError):
                parsed = []
        if not isinstance(parsed, list):
            return []
        return sorted([c for c in parsed if isinstance(c, str)])

    commander_ci_sorted = normalize_ci((commander_resolved or {}).get("color_identity"))

    deck_cards_canonical_input_order: List[Dict[str, Any]] = []
    first_playable_slot_id_by_name: Dict[str, str] = {}
    slot_primitives_source_by_slot_id: Dict[str, Any] = {}
    for slot_idx, input_name in enumerate(req.cards):
        slot_id = make_slot_id("S", slot_idx)
        resolved_oracle_id = None
        slot_oracle_queue = resolved_oracle_id_queues.get(input_name)
        if slot_oracle_queue:
            resolved_oracle_id = slot_oracle_queue.pop(0)
        resolved_card_ci = []
        slot_ci_queue = resolved_card_ci_queues.get(input_name)
        if slot_ci_queue:
            resolved_card_ci = normalize_ci(slot_ci_queue.pop(0))
        resolved_format_status = "missing"
        slot_format_queue = resolved_format_status_queues.get(input_name)
        if slot_format_queue:
            resolved_format_status = slot_format_queue.pop(0)
        resolved_primitives_source = None
        slot_primitives_queue = resolved_card_primitives_queues.get(input_name)
        if slot_primitives_queue:
            resolved_primitives_source = slot_primitives_queue.pop(0)
        slot_primitives_source_by_slot_id[slot_id] = resolved_primitives_source

        if unknown_remaining.get(input_name, 0) > 0:
            unknown_remaining[input_name] -= 1
            deck_cards_canonical_input_order.append(
                {
                    "slot_id": slot_id,
                    "input": input_name,
                    "resolved_name": None,
                    "resolved_oracle_id": None,
                    "status": "UNKNOWN",
                    "codes": ["UNKNOWN_CARD"],
                    "ci_violation_detail": None,
                    "format_legality_detail": None,
                    "duplicate_detail": None,
                }
            )
            continue

        resolved_name = input_name
        slot_codes: List[str] = []

        if illegal_remaining.get(input_name, 0) > 0:
            illegal_remaining[input_name] -= 1
            slot_codes.append("ILLEGAL_CARD")
        if ci_remaining.get(input_name, 0) > 0:
            ci_remaining[input_name] -= 1
            slot_codes.append("COLOR_IDENTITY_VIOLATION")

        if slot_codes:
            ci_violation_detail = None
            if "COLOR_IDENTITY_VIOLATION" in slot_codes:
                ci_violation_detail = {
                    "commander_color_identity": commander_ci_sorted,
                    "card_color_identity": resolved_card_ci,
                }
            format_legality_detail = None
            if "ILLEGAL_CARD" in slot_codes:
                format_legality_detail = {
                    "format": req.format,
                    "status": resolved_format_status,
                }
            deck_cards_canonical_input_order.append(
                {
                    "slot_id": slot_id,
                    "input": input_name,
                    "resolved_name": resolved_name,
                    "resolved_oracle_id": resolved_oracle_id,
                    "status": "NONPLAYABLE",
                    "codes": sorted(slot_codes),
                    "ci_violation_detail": ci_violation_detail,
                    "format_legality_detail": format_legality_detail,
                    "duplicate_detail": None,
                }
            )
            continue

        if playable_remaining.get(input_name, 0) > 0:
            playable_remaining[input_name] -= 1
            deck_cards_canonical_input_order.append(
                {
                    "slot_id": slot_id,
                    "input": input_name,
                    "resolved_name": resolved_name,
                    "resolved_oracle_id": resolved_oracle_id,
                    "status": "PLAYABLE",
                    "codes": [],
                    "ci_violation_detail": None,
                    "format_legality_detail": None,
                    "duplicate_detail": None,
                }
            )
            if input_name not in first_playable_slot_id_by_name:
                first_playable_slot_id_by_name[input_name] = slot_id
            continue

        if duplicate_remaining.get(input_name, 0) > 0:
            duplicate_remaining[input_name] -= 1
            first_copy_slot_id = first_playable_slot_id_by_name.get(input_name)
            deck_cards_canonical_input_order.append(
                {
                    "slot_id": slot_id,
                    "input": input_name,
                    "resolved_name": resolved_name,
                    "resolved_oracle_id": resolved_oracle_id,
                    "status": "NONPLAYABLE",
                    "codes": ["DUPLICATE_CARD"],
                    "ci_violation_detail": None,
                    "format_legality_detail": None,
                    "duplicate_detail": {
                        "first_copy_slot_id": first_copy_slot_id,
                        "is_exempt_duplicate": False,
                    },
                }
            )
            continue

        deck_cards_canonical_input_order.append(
            {
                "slot_id": slot_id,
                "input": input_name,
                "resolved_name": resolved_name,
                "resolved_oracle_id": resolved_oracle_id,
                "status": "PLAYABLE",
                "codes": [],
                "ci_violation_detail": None,
                "format_legality_detail": None,
                "duplicate_detail": None,
            }
        )
        if input_name not in first_playable_slot_id_by_name:
            first_playable_slot_id_by_name[input_name] = slot_id
    playable_index_counter = 0
    nonplayable_index_counter = 0
    for entry in deck_cards_canonical_input_order:
        if entry.get("status") == "PLAYABLE":
            entry["playable_slot"] = True
            entry["playable_index"] = playable_index_counter
            entry["nonplayable_slot"] = False
            entry["nonplayable_index"] = None
            playable_index_counter += 1
        else:
            entry["playable_slot"] = False
            entry["playable_index"] = None
            entry["nonplayable_slot"] = True
            entry["nonplayable_index"] = nonplayable_index_counter
            nonplayable_index_counter += 1

        codes = entry.get("codes") or []
        if codes:
            refs = [{"code": code, "input": entry.get("input")} for code in codes]
            entry["unknown_refs"] = sorted(refs, key=lambda r: (r.get("code", ""), r.get("input", "")))
        else:
            entry["unknown_refs"] = []

    deck_cards_slot_ids_playable = [
        entry["slot_id"] for entry in deck_cards_canonical_input_order if entry.get("status") == "PLAYABLE"
    ]
    deck_cards_slot_ids_nonplayable = [
        entry["slot_id"] for entry in deck_cards_canonical_input_order if entry.get("status") != "PLAYABLE"
    ]
    deck_cards_unknowns_by_slot = {
        entry["slot_id"]: sorted(entry.get("codes") or []) for entry in deck_cards_canonical_input_order
    }

    commander_input_value = req.commander
    commander_resolved_name = (commander_resolved or {}).get("name")
    commander_codes = sorted(
        [
            u.get("code")
            for u in unknowns
            if u.get("input") == commander_input_value and isinstance(u.get("code"), str)
        ]
    )
    commander_unknown_refs = sorted(
        [{"code": code, "input": commander_input_value} for code in commander_codes],
        key=lambda r: (r.get("code", ""), r.get("input", "")),
    )

    if commander_resolved is None:
        commander_status = "UNKNOWN"
    elif commander_codes:
        commander_status = "NONPLAYABLE"
    else:
        commander_status = "PLAYABLE"

    commander_format_legality_detail = None
    if commander_status != "PLAYABLE" and commander_resolved is not None:
        _, commander_format_status = get_format_legality(commander_resolved, req.format)
        commander_format_legality_detail = {
            "format": req.format,
            "status": commander_format_status,
        }

    commander_canonical_slot = {
        "slot_id": "C0",
        "input": commander_input_value,
        "resolved_name": commander_resolved_name,
        "resolved_oracle_id": (commander_resolved or {}).get("oracle_id"),
        "status": commander_status,
        "codes": commander_codes,
        "unknown_refs": commander_unknown_refs,
        "ci_violation_detail": None,
        "format_legality_detail": commander_format_legality_detail,
        "duplicate_detail": None,
        "playable_slot": commander_status == "PLAYABLE",
        "playable_index": None,
        "nonplayable_slot": commander_status != "PLAYABLE",
        "nonplayable_index": None,
    }

    canonical_slots_all = [commander_canonical_slot] + deck_cards_canonical_input_order

    state["unknown_cards"] = unknown_cards
    state["deck_cards_nonplayable"] = deck_cards_nonplayable
    state["deck_cards_playable"] = deck_cards_playable
    state["deck_cards_canonical_input_order"] = deck_cards_canonical_input_order
    state["playable_index_counter"] = playable_index_counter
    state["nonplayable_index_counter"] = nonplayable_index_counter
    state["deck_cards_slot_ids_playable"] = deck_cards_slot_ids_playable
    state["deck_cards_slot_ids_nonplayable"] = deck_cards_slot_ids_nonplayable
    state["deck_cards_unknowns_by_slot"] = deck_cards_unknowns_by_slot
    state["commander_canonical_slot"] = commander_canonical_slot
    state["canonical_slots_all"] = canonical_slots_all
    state["slot_primitives_source_by_slot_id"] = slot_primitives_source_by_slot_id

    return state
