from typing import Any, Dict, List


def run_pathways_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pathways analysis layer (pathways_v1).
    Computes commander reachability, hubs, and bridge candidates.
    Must preserve exact output + ordering + hashing payloads.
    """

    node_order = state["node_order"]
    adj_simple = state["adj_simple"]
    graph_nodes = state["graph_nodes"]
    disruption_commander_risk = state["disruption_commander_risk"]
    graph_component_by_node = state["graph_component_by_node"]
    graph_components = state["graph_components"]
    pathways_layer_version = state["pathways_layer_version"]
    pathways_ruleset_version = state["pathways_ruleset_version"]
    graph_hash_v2 = state["graph_hash_v2"]
    stable_json_dumps = state["stable_json_dumps"]
    sha256_hex = state["sha256_hex"]

    commander_slot_id_pathways = "C0"
    commander_in_graph = commander_slot_id_pathways in node_order
    commander_playable = commander_in_graph
    distance_by_node: Dict[str, int] = {}
    if commander_playable:
        queue: List[str] = [commander_slot_id_pathways]
        distance_by_node[commander_slot_id_pathways] = 0
        while queue:
            current = queue.pop(0)
            current_distance = distance_by_node[current]
            for neighbor in sorted(adj_simple.get(current, [])):
                if neighbor not in distance_by_node:
                    distance_by_node[neighbor] = current_distance + 1
                    queue.append(neighbor)

    deck_node_ids_in_graph = [sid for sid in node_order if sid.startswith("S")]
    pathways_commander_distances: List[Dict[str, Any]] = []
    pathways_commander_reachable_slots: List[str] = []
    pathways_commander_unreachable_slots: List[str] = []
    for sid in deck_node_ids_in_graph:
        reachable = sid in distance_by_node
        distance_value = distance_by_node.get(sid) if reachable else None
        pathways_commander_distances.append(
            {
                "slot_id": sid,
                "distance": distance_value,
                "reachable": reachable,
            }
        )
        if reachable:
            pathways_commander_reachable_slots.append(sid)
        else:
            pathways_commander_unreachable_slots.append(sid)

    hubs_sorted = sorted(
        graph_nodes,
        key=lambda n: (
            -int(n.get("degree", 0)),
            -int(n.get("primitive_count", 0)),
            str(n.get("slot_id") or ""),
        ),
    )
    pathways_hubs = [
        {
            "slot_id": n.get("slot_id"),
            "node_type": n.get("node_type"),
            "degree": int(n.get("degree", 0)),
            "primitive_count": int(n.get("primitive_count", 0)),
            "resolved_name": n.get("resolved_name"),
            "resolved_oracle_id": n.get("resolved_oracle_id"),
        }
        for n in hubs_sorted[:10]
    ]

    pathways_commander_bridge_candidates: List[Dict[str, Any]] = []
    commander_isolated_for_pathways = bool(disruption_commander_risk.get("commander_is_isolated"))
    if commander_playable and (commander_isolated_for_pathways or len(pathways_commander_unreachable_slots) > 0):
        commander_component_id_for_pathways = graph_component_by_node.get(commander_slot_id_pathways)
        non_commander_components = []
        for comp in graph_components:
            comp_id = comp.get("component_id")
            comp_nodes = [n for n in (comp.get("nodes") or []) if isinstance(n, str)]
            if not isinstance(comp_id, str):
                continue
            if commander_slot_id_pathways in comp_nodes:
                continue
            non_commander_components.append(
                {
                    "component_id": comp_id,
                    "nodes": comp_nodes,
                    "nodes_total": len(comp_nodes),
                }
            )

        if non_commander_components:
            target_component = sorted(
                non_commander_components,
                key=lambda c: (-int(c.get("nodes_total", 0)), str(c.get("component_id") or "")),
            )[0]
            target_component_id = target_component["component_id"]
            target_nodes_set = set(target_component["nodes"])
            target_nodes = [n for n in graph_nodes if n.get("slot_id") in target_nodes_set]
            target_nodes_sorted = sorted(
                target_nodes,
                key=lambda n: (
                    -int(n.get("degree", 0)),
                    -int(n.get("primitive_count", 0)),
                    str(n.get("slot_id") or ""),
                ),
            )
            pathways_commander_bridge_candidates = [
                {
                    "slot_id": n.get("slot_id"),
                    "component_id": target_component_id,
                    "degree": int(n.get("degree", 0)),
                    "resolved_name": n.get("resolved_name"),
                    "resolved_oracle_id": n.get("resolved_oracle_id"),
                    "note": "High-degree node in largest non-commander component",
                }
                for n in target_nodes_sorted[:5]
            ]

    reachable_distances = [d for sid, d in distance_by_node.items() if sid.startswith("S")]
    max_distance_to_deck = max(reachable_distances) if reachable_distances else None
    pathways_totals = {
        "commander_in_graph": commander_in_graph,
        "commander_playable": commander_playable,
        "deck_nodes_total": len(deck_node_ids_in_graph),
        "reachable_deck_nodes_total": len(pathways_commander_reachable_slots),
        "unreachable_deck_nodes_total": len(pathways_commander_unreachable_slots),
        "max_distance_to_deck": max_distance_to_deck,
        "hubs_total": len(pathways_hubs),
        "bridge_candidates_total": len(pathways_commander_bridge_candidates),
    }

    pathways_fingerprint_payload_v1 = {
        "pathways_layer_version": pathways_layer_version,
        "pathways_ruleset_version": pathways_ruleset_version,
        "graph_hash_v2": graph_hash_v2,
        "distances_compact": [
            {
                "slot_id": row.get("slot_id"),
                "distance": row.get("distance"),
            }
            for row in pathways_commander_distances
        ],
        "hubs_compact": [
            {
                "slot_id": row.get("slot_id"),
                "degree": row.get("degree"),
            }
            for row in pathways_hubs
        ],
        "bridge_candidates_compact": [row.get("slot_id") for row in pathways_commander_bridge_candidates],
    }
    pathways_hash_v1 = sha256_hex(stable_json_dumps(pathways_fingerprint_payload_v1))

    state["commander_in_graph"] = commander_in_graph
    state["commander_playable"] = commander_playable
    state["distance_by_node"] = distance_by_node
    state["pathways_commander_distances"] = pathways_commander_distances
    state["pathways_commander_reachable_slots"] = pathways_commander_reachable_slots
    state["pathways_commander_unreachable_slots"] = pathways_commander_unreachable_slots
    state["pathways_hubs"] = pathways_hubs
    state["pathways_commander_bridge_candidates"] = pathways_commander_bridge_candidates
    state["pathways_totals"] = pathways_totals
    state["pathways_fingerprint_payload_v1"] = pathways_fingerprint_payload_v1
    state["pathways_hash_v1"] = pathways_hash_v1

    return state
