from typing import Any, Dict, List


def run_combo_candidate_v0(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Combo candidate layer (combo_candidate_v0).
    Builds structured combo candidate objects from skeleton cycles + typed edges.
    Must preserve exact output + ordering + hashing payloads.
    """

    combo_skeleton_components = state["combo_skeleton_components"]
    edge_key_norm = state["edge_key_norm"]
    edge_by_key_for_candidates = state["edge_by_key_for_candidates"]
    sorted_unique = state["sorted_unique"]
    slot_by_id = state["slot_by_id"]
    primitive_index_by_slot = state["primitive_index_by_slot"]
    stable_json_dumps = state["stable_json_dumps"]
    sha256_hex = state["sha256_hex"]
    combo_candidate_layer_version = state["combo_candidate_layer_version"]
    combo_candidate_ruleset_version = state["combo_candidate_ruleset_version"]
    combo_skeleton_hash_v1 = state["combo_skeleton_hash_v1"]
    graph_hash_v2 = state["graph_hash_v2"]

    combo_candidates_v0: List[Dict[str, Any]] = []
    for component in combo_skeleton_components:
        component_id = component.get("component_id")
        if not isinstance(component_id, str):
            continue
        for cycle in component.get("small_cycles", []):
            slot_ids = [s for s in (cycle.get("nodes") or []) if isinstance(s, str)]
            cycle_len = int(cycle.get("cycle_len", 0))
            if len(slot_ids) < 3:
                continue

            cycle_edge_keys: List[str] = []
            cycle_edges_missing: List[str] = []
            edges_payload: List[Dict[str, Any]] = []
            typed_edge_types_union_set: set[str] = set()

            for i in range(len(slot_ids)):
                a = slot_ids[i]
                b = slot_ids[(i + 1) % len(slot_ids)]
                edge_key = edge_key_norm(a, b)
                cycle_edge_keys.append(edge_key)
                edge_obj = edge_by_key_for_candidates.get(edge_key)
                if edge_obj is None:
                    cycle_edges_missing.append(edge_key)
                    continue

                typed_matches_compact = []
                typed_edge_types_for_edge_set: set[str] = set()
                for m in edge_obj.get("typed_matches", []):
                    edge_type = m.get("edge_type")
                    if isinstance(edge_type, str):
                        typed_edge_types_for_edge_set.add(edge_type)
                        typed_edge_types_union_set.add(edge_type)
                    typed_matches_compact.append(
                        {
                            "edge_type": edge_type,
                            "rule_index": m.get("rule_index"),
                            "matched_rule_version": m.get("matched_rule_version"),
                        }
                    )

                edges_payload.append(
                    {
                        "edge_key": edge_key,
                        "shared_primitives": sorted_unique(edge_obj.get("shared_primitives") or []),
                        "typed_edge_types": sorted_unique(typed_edge_types_for_edge_set),
                        "typed_matches": typed_matches_compact,
                    }
                )

            cards_payload = []
            primitives_union_set: set[str] = set()
            for sid in slot_ids:
                slot_entry = slot_by_id.get(sid) or {}
                primitives_for_slot = sorted(primitive_index_by_slot.get(sid, []))
                primitives_union_set.update(primitives_for_slot)
                cards_payload.append(
                    {
                        "slot_id": sid,
                        "name": slot_entry.get("resolved_name"),
                        "oracle_id": slot_entry.get("resolved_oracle_id"),
                        "status": slot_entry.get("status"),
                        "primitives": primitives_for_slot,
                    }
                )

            candidate_type = "CYCLE_LEN_4" if cycle_len == 4 else "CYCLE_LEN_3"
            candidate_id = f"CC{len(combo_candidates_v0)}"

            typed_edge_matches_compact = []
            for edge_key in cycle_edge_keys:
                edge_obj = edge_by_key_for_candidates.get(edge_key)
                matches_compact = []
                for m in (edge_obj or {}).get("typed_matches", []):
                    matches_compact.append(
                        {
                            "edge_type": m.get("edge_type"),
                            "rule_index": m.get("rule_index"),
                        }
                    )
                typed_edge_matches_compact.append(
                    {
                        "edge_key": edge_key,
                        "typed_matches": matches_compact,
                    }
                )

            candidate_fingerprint_payload_v1 = {
                "candidate_type": candidate_type,
                "cycle_len": cycle_len,
                "component_id": component_id,
                "slot_ids": slot_ids,
                "cards_compact": [
                    {
                        "slot_id": c.get("slot_id"),
                        "oracle_id": c.get("oracle_id"),
                    }
                    for c in cards_payload
                ],
                "cycle_edge_keys": cycle_edge_keys,
                "typed_edge_matches_compact": typed_edge_matches_compact,
            }
            candidate_hash_v1 = sha256_hex(stable_json_dumps(candidate_fingerprint_payload_v1))

            combo_candidates_v0.append(
                {
                    "candidate_id": candidate_id,
                    "candidate_type": candidate_type,
                    "cycle_len": cycle_len,
                    "component_id": component_id,
                    "slot_ids": slot_ids,
                    "cards": cards_payload,
                    "cycle_edge_keys": cycle_edge_keys,
                    "cycle_edges_missing": cycle_edges_missing,
                    "edges": edges_payload,
                    "primitives_union": sorted_unique(primitives_union_set),
                    "typed_edge_types_union": sorted_unique(typed_edge_types_union_set),
                    "commander_involved": "C0" in slot_ids,
                    "is_graph_consistent": len(cycle_edges_missing) == 0,
                    "candidate_hash_v1": candidate_hash_v1,
                }
            )

    combo_candidates_by_component_temp: Dict[str, List[str]] = {}
    combo_candidates_by_cycle_len = {"3": [], "4": []}
    for candidate in combo_candidates_v0:
        cid = candidate.get("candidate_id")
        comp_id = candidate.get("component_id")
        if isinstance(comp_id, str) and isinstance(cid, str):
            combo_candidates_by_component_temp.setdefault(comp_id, []).append(cid)
        if isinstance(cid, str):
            if candidate.get("cycle_len") == 3:
                combo_candidates_by_cycle_len["3"].append(cid)
            elif candidate.get("cycle_len") == 4:
                combo_candidates_by_cycle_len["4"].append(cid)

    combo_candidates_by_component = {
        comp_id: combo_candidates_by_component_temp[comp_id]
        for comp_id in sorted(combo_candidates_by_component_temp.keys())
    }

    combo_candidate_fingerprint_payload_v1 = {
        "combo_candidate_layer_version": combo_candidate_layer_version,
        "combo_candidate_ruleset_version": combo_candidate_ruleset_version,
        "combo_skeleton_hash_v1": combo_skeleton_hash_v1,
        "graph_hash_v2": graph_hash_v2,
        "candidates_compact": [
            {
                "candidate_id": c.get("candidate_id"),
                "candidate_hash_v1": c.get("candidate_hash_v1"),
            }
            for c in combo_candidates_v0
        ],
    }
    combo_candidates_hash_v1 = sha256_hex(stable_json_dumps(combo_candidate_fingerprint_payload_v1))

    state["combo_candidates_v0"] = combo_candidates_v0
    state["combo_candidates_by_component"] = combo_candidates_by_component
    state["combo_candidates_by_cycle_len"] = combo_candidates_by_cycle_len
    state["combo_candidate_fingerprint_payload_v1"] = combo_candidate_fingerprint_payload_v1
    state["combo_candidates_hash_v1"] = combo_candidates_hash_v1

    return state
