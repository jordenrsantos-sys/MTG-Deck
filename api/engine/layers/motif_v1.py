from typing import Any, Dict, List


def run_motif_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Motif detection layer (motif_v1).
    Reporting only. Must preserve exact output + ordering + hashing payloads.
    """

    graph_edges = state["graph_edges"]
    graph_nodes = state["graph_nodes"]
    graph_component_by_node = state["graph_component_by_node"]
    graph_components = state["graph_components"]
    graph_adjacency = state["graph_adjacency"]
    connected_components_total = state["connected_components_total"]
    isolated_nodes_total = state["isolated_nodes_total"]
    largest_component_id = state["largest_component_id"]
    largest_component_size = state["largest_component_size"]
    avg_degree = state["avg_degree"]
    max_degree = state["max_degree"]
    node_order = state["node_order"]
    order_by_node_order = state["order_by_node_order"]
    graph_hash_v2 = state["graph_hash_v2"]
    motif_layer_version = state["motif_layer_version"]
    motif_ruleset_version = state["motif_ruleset_version"]
    stable_json_dumps = state["stable_json_dumps"]
    sha256_hex = state["sha256_hex"]

    edge_by_key: Dict[str, Dict[str, Any]] = {}
    typed_matches_by_type_temp: Dict[str, List[str]] = {}
    typed_match_count_by_type_temp: Dict[str, int] = {}
    for edge in graph_edges:
        edge_key = f"{edge.get('a')}|{edge.get('b')}"
        edge_by_key[edge_key] = edge
        for typed_match in edge.get("typed_matches") or []:
            edge_type = typed_match.get("edge_type")
            if not isinstance(edge_type, str):
                continue
            typed_matches_by_type_temp.setdefault(edge_type, []).append(edge_key)
            typed_match_count_by_type_temp[edge_type] = typed_match_count_by_type_temp.get(edge_type, 0) + 1

    typed_matches_by_type = {
        edge_type: typed_matches_by_type_temp[edge_type]
        for edge_type in sorted(typed_matches_by_type_temp.keys())
    }
    node_type_by_slot = {
        n.get("slot_id"): n.get("node_type")
        for n in graph_nodes
        if isinstance(n.get("slot_id"), str)
    }
    component_id_by_slot = {
        sid: cid for sid, cid in graph_component_by_node.items() if isinstance(sid, str) and isinstance(cid, str)
    }
    component_nodes_by_id = {
        c.get("component_id"): list(c.get("nodes") or [])
        for c in graph_components
        if isinstance(c.get("component_id"), str)
    }

    motifs: List[Dict[str, Any]] = []
    for edge_type in sorted(typed_matches_by_type.keys()):
        edge_keys_seen: set[str] = set()
        edge_keys_for_type: List[str] = []
        slot_ids_seen: set[str] = set()
        slot_ids_unsorted: List[str] = []
        for edge_key in typed_matches_by_type[edge_type]:
            if edge_key not in edge_keys_seen:
                edge_keys_seen.add(edge_key)
                edge_keys_for_type.append(edge_key)
            edge = edge_by_key.get(edge_key) or {}
            a = edge.get("a")
            b = edge.get("b")
            for sid in [a, b]:
                if isinstance(sid, str) and sid not in slot_ids_seen:
                    slot_ids_seen.add(sid)
                    slot_ids_unsorted.append(sid)
        edge_keys_sorted = sorted(edge_keys_for_type)
        slot_ids_sorted = order_by_node_order(slot_ids_unsorted, node_order)
        count = typed_match_count_by_type_temp.get(edge_type, 0)
        motifs.append(
            {
                "motif_id": f"M0_{edge_type}",
                "motif_type": "EDGE_TYPE_PRESENT",
                "label": edge_type,
                "present": count > 0,
                "count": count,
                "evidence": {
                    "edge_keys": edge_keys_sorted,
                    "slot_ids": slot_ids_sorted,
                },
            }
        )

    commander_component_id = component_id_by_slot.get("C0")
    commander_component_nodes = component_nodes_by_id.get(commander_component_id, []) if commander_component_id else []
    deck_nodes_in_commander_component = [
        sid for sid in commander_component_nodes if node_type_by_slot.get(sid) == "DECK"
    ]
    commander_isolated = len(graph_adjacency.get("C0", [])) == 0
    motifs.append(
        {
            "motif_id": "M1_COMMANDER_CONNECTED_TO_DECK",
            "motif_type": "COMPONENT_RELATION",
            "label": "Commander Connected To Deck",
            "present": bool(commander_component_id and deck_nodes_in_commander_component),
            "count": len(deck_nodes_in_commander_component),
            "evidence": {
                "commander_component_id": commander_component_id,
                "deck_nodes_in_commander_component": deck_nodes_in_commander_component,
                "commander_isolated": commander_isolated,
            },
        }
    )

    motifs.append(
        {
            "motif_id": "M2_GRAPH_FRAGMENTED",
            "motif_type": "COMPONENT_STATS",
            "label": "Graph Fragmented",
            "present": connected_components_total > 1,
            "count": connected_components_total,
            "evidence": {
                "components": [
                    {
                        "component_id": c.get("component_id"),
                        "nodes_total": c.get("nodes_total"),
                    }
                    for c in graph_components
                ],
                "isolated_nodes_total": isolated_nodes_total,
                "largest_component_id": largest_component_id,
                "largest_component_size": largest_component_size,
            },
        }
    )

    motifs.append(
        {
            "motif_id": "M3_OVERLAP_DENSITY",
            "motif_type": "GRAPH_STATS",
            "label": "Primitive Overlap Density",
            "present": True,
            "count": len(graph_edges),
            "evidence": {
                "avg_degree": avg_degree,
                "max_degree": max_degree,
                "edges_total": len(graph_edges),
                "nodes_total": len(graph_nodes),
            },
        }
    )

    motifs.sort(key=lambda m: m.get("motif_id", ""))
    motifs_present_total = sum(1 for m in motifs if m.get("present") is True)
    typed_edge_types_total = len(typed_matches_by_type)
    typed_matches_total = sum(typed_match_count_by_type_temp.values())
    motif_totals = {
        "motifs_total": len(motifs),
        "motifs_present_total": motifs_present_total,
        "typed_edge_types_total": typed_edge_types_total,
        "typed_matches_total": typed_matches_total,
    }

    motifs_compact = []
    for motif in motifs:
        compact = {
            "motif_id": motif.get("motif_id"),
            "present": motif.get("present"),
            "count": motif.get("count"),
        }
        evidence = motif.get("evidence") or {}
        edge_keys = evidence.get("edge_keys")
        if isinstance(edge_keys, list):
            compact["edge_keys"] = list(edge_keys)
        motifs_compact.append(compact)

    motif_fingerprint_payload_v1 = {
        "motif_layer_version": motif_layer_version,
        "motif_ruleset_version": motif_ruleset_version,
        "graph_hash_v2": graph_hash_v2,
        "motifs_compact": motifs_compact,
    }
    motif_hash_v1 = sha256_hex(stable_json_dumps(motif_fingerprint_payload_v1))

    state["motifs"] = motifs
    state["motif_totals"] = motif_totals
    state["motif_fingerprint_payload_v1"] = motif_fingerprint_payload_v1
    state["motif_hash_v1"] = motif_hash_v1

    return state
