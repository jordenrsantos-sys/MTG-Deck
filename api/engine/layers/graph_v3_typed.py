from typing import Any, Dict, List


def run_graph_v3_typed(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Graph construction layer (graph_v3_typed).
    Builds nodes, edges, typed edges, components, and graph hashes.
    Must preserve exact output + ordering + hashing payloads.
    """

    req = state["req"]
    canonical_slots_all = state["canonical_slots_all"]
    primitive_index_by_slot = state["primitive_index_by_slot"]
    sorted_unique = state["sorted_unique"]
    engine_typed_edge_rule_toggle_by_index = state["engine_typed_edge_rule_toggle_by_index"]
    typed_edge_rules_v0 = state["typed_edge_rules_v0"]
    graph_typed_rules_version = state["graph_typed_rules_version"]
    graph_layer_version = state["graph_layer_version"]
    graph_ruleset_version = state["graph_ruleset_version"]
    stable_json_dumps = state["stable_json_dumps"]
    sha256_hex = state["sha256_hex"]

    graph_nodes: List[Dict[str, Any]] = []
    for entry in canonical_slots_all:
        if entry.get("status") != "PLAYABLE":
            continue
        slot_id = entry.get("slot_id")
        if not isinstance(slot_id, str):
            continue
        graph_nodes.append(
            {
                "slot_id": slot_id,
                "resolved_name": entry.get("resolved_name"),
                "resolved_oracle_id": entry.get("resolved_oracle_id"),
                "primitives": sorted_unique(primitive_index_by_slot.get(slot_id, [])),
                "node_type": "COMMANDER" if slot_id == "C0" else "DECK",
            }
        )

    graph_slot_ids = [n["slot_id"] for n in graph_nodes]
    primitives_by_graph_slot = {n["slot_id"]: set(n.get("primitives") or []) for n in graph_nodes}
    typed_rule_match_counts_before: Dict[int, int] = {i: 0 for i in range(len(typed_edge_rules_v0))}
    typed_rule_match_counts_after: Dict[int, int] = {i: 0 for i in range(len(typed_edge_rules_v0))}

    graph_edges: List[Dict[str, Any]] = []
    for i in range(len(graph_slot_ids)):
        for j in range(i + 1, len(graph_slot_ids)):
            sid_i = graph_slot_ids[i]
            sid_j = graph_slot_ids[j]
            shared = sorted_unique(primitives_by_graph_slot[sid_i].intersection(primitives_by_graph_slot[sid_j]))
            if not shared:
                continue
            a = sid_i if sid_i < sid_j else sid_j
            b = sid_j if sid_i < sid_j else sid_i
            reasons = [{"type": "SHARED_PRIMITIVE", "primitive": p} for p in shared]
            typed_matches: List[Dict[str, Any]] = []
            a_primitives = primitives_by_graph_slot[a]
            b_primitives = primitives_by_graph_slot[b]
            for rule_index, rule in enumerate(typed_edge_rules_v0):
                req_a = set(rule.get("requires_all_primitives_a", []))
                req_b = set(rule.get("requires_all_primitives_b", []))
                forward_match = req_a.issubset(a_primitives) and req_b.issubset(b_primitives)
                reverse_match = req_b.issubset(a_primitives) and req_a.issubset(b_primitives)
                if not (forward_match or reverse_match):
                    continue
                typed_rule_match_counts_before[rule_index] = typed_rule_match_counts_before.get(rule_index, 0) + 1
                if engine_typed_edge_rule_toggle_by_index.get(rule_index, True) is not True:
                    continue
                typed_matches.append(
                    {
                        "edge_type": rule["edge_type"],
                        "matched_rule_version": graph_typed_rules_version,
                        "rule_index": rule_index,
                        "a_primitives_used": sorted_unique(req_a),
                        "b_primitives_used": sorted_unique(req_b),
                        "reason": rule["reason_template"],
                    }
                )
                typed_rule_match_counts_after[rule_index] = typed_rule_match_counts_after.get(rule_index, 0) + 1
            graph_edges.append(
                {
                    "a": a,
                    "b": b,
                    "shared_primitives": shared,
                    "shared_primitives_count": len(shared),
                    "reasons": reasons,
                    "typed_matches": typed_matches,
                }
            )
    graph_edges.sort(key=lambda e: (e.get("a", ""), e.get("b", "")))

    graph_edge_index: Dict[str, Dict[str, Any]] = {}
    graph_adjacency: Dict[str, List[Dict[str, Any]]] = {sid: [] for sid in graph_slot_ids}
    for edge in graph_edges:
        a = edge["a"]
        b = edge["b"]
        shared = edge["shared_primitives"]
        shared_count = edge["shared_primitives_count"]
        graph_edge_index[f"{a}|{b}"] = {
            "shared_primitives_count": shared_count,
            "shared_primitives": shared,
        }
        graph_adjacency[a].append(
            {
                "neighbor": b,
                "shared_primitives": shared,
                "shared_primitives_count": shared_count,
            }
        )
        graph_adjacency[b].append(
            {
                "neighbor": a,
                "shared_primitives": shared,
                "shared_primitives_count": shared_count,
            }
        )
    for sid in graph_slot_ids:
        graph_adjacency[sid].sort(key=lambda n: n.get("neighbor", ""))

    graph_node_degrees: Dict[str, int] = {}
    for node in graph_nodes:
        sid = node["slot_id"]
        degree = len(graph_adjacency.get(sid, []))
        node["degree"] = degree
        node["primitive_count"] = len(node.get("primitives") or [])
        node["is_isolated"] = degree == 0
        graph_node_degrees[sid] = degree

    visited: set[str] = set()
    graph_components: List[Dict[str, Any]] = []
    graph_component_by_node: Dict[str, str] = {}
    for sid in graph_slot_ids:
        if sid in visited:
            continue
        queue = [sid]
        visited.add(sid)
        discovered: List[str] = []
        while queue:
            current = queue.pop(0)
            discovered.append(current)
            for neighbor_entry in graph_adjacency.get(current, []):
                neighbor = neighbor_entry.get("neighbor")
                if isinstance(neighbor, str) and neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)

        discovered_set = set(discovered)
        component_nodes = [nid for nid in graph_slot_ids if nid in discovered_set]
        component_id = f"G{len(graph_components)}"
        graph_components.append(
            {
                "component_id": component_id,
                "nodes": component_nodes,
                "nodes_total": len(component_nodes),
            }
        )
        for nid in component_nodes:
            graph_component_by_node[nid] = component_id

    connected_components_total = len(graph_components)
    isolated_nodes_total = sum(1 for sid in graph_slot_ids if len(graph_adjacency.get(sid, [])) == 0)
    max_degree = max((len(graph_adjacency.get(sid, [])) for sid in graph_slot_ids), default=0)
    avg_degree = round((2 * len(graph_edges)) / max(len(graph_nodes), 1), 3)

    if graph_components:
        largest_component_size = max(c["nodes_total"] for c in graph_components)
        largest_component_candidates = [c["component_id"] for c in graph_components if c["nodes_total"] == largest_component_size]
        largest_component_id = sorted(largest_component_candidates)[0]
    else:
        largest_component_size = 0
        largest_component_id = None

    graph_totals = {
        "connected_components_total": connected_components_total,
        "isolated_nodes_total": isolated_nodes_total,
        "max_degree": max_degree,
        "avg_degree": avg_degree,
        "largest_component_size": largest_component_size,
        "largest_component_id": largest_component_id,
    }

    graph_typed_edges_total = sum(1 for e in graph_edges if e.get("typed_matches"))
    typed_match_counts_temp: Dict[str, int] = {}
    typed_edges_by_type_temp: Dict[str, List[str]] = {}
    for edge in graph_edges:
        edge_key = f"{edge.get('a')}|{edge.get('b')}"
        for match in edge.get("typed_matches", []):
            edge_type = match.get("edge_type")
            if not isinstance(edge_type, str):
                continue
            typed_match_counts_temp[edge_type] = typed_match_counts_temp.get(edge_type, 0) + 1
            typed_edges_by_type_temp.setdefault(edge_type, []).append(edge_key)

    graph_typed_match_counts_by_type = {
        k: typed_match_counts_temp[k] for k in sorted(typed_match_counts_temp.keys())
    }
    graph_typed_edges_by_type = {
        k: typed_edges_by_type_temp[k] for k in sorted(typed_edges_by_type_temp.keys())
    }
    graph_rules_meta = {
        "graph_typed_rules_version": graph_typed_rules_version,
        "typed_rules_total": len(typed_edge_rules_v0),
        "graph_ruleset_version": graph_ruleset_version,
    }

    graph_fingerprint_payload_v1 = {
        "graph_layer_version": graph_layer_version,
        "graph_ruleset_version": graph_ruleset_version,
        "db_snapshot_id": req.db_snapshot_id,
        "format": req.format,
        "bracket_id": req.bracket_id,
        "profile_id": req.profile_id,
        "nodes_compact": [
            {
                "slot_id": n.get("slot_id"),
                "resolved_oracle_id": n.get("resolved_oracle_id"),
                "primitives": sorted(n.get("primitives") or []),
            }
            for n in graph_nodes
        ],
        "edges_compact": [
            {
                "a": e.get("a"),
                "b": e.get("b"),
                "shared_primitives": sorted(e.get("shared_primitives") or []),
            }
            for e in graph_edges
        ],
    }
    graph_hash_v1 = sha256_hex(stable_json_dumps(graph_fingerprint_payload_v1))
    graph_fingerprint_payload_v2 = {
        "graph_layer_version": graph_layer_version,
        "graph_ruleset_version": graph_ruleset_version,
        "db_snapshot_id": req.db_snapshot_id,
        "format": req.format,
        "bracket_id": req.bracket_id,
        "profile_id": req.profile_id,
        "nodes_compact": [
            {
                "slot_id": n.get("slot_id"),
                "resolved_oracle_id": n.get("resolved_oracle_id"),
                "primitives": sorted(n.get("primitives") or []),
            }
            for n in graph_nodes
        ],
        "edges_compact": [
            {
                "a": e.get("a"),
                "b": e.get("b"),
                "shared_primitives": sorted(e.get("shared_primitives") or []),
            }
            for e in graph_edges
        ],
        "typed_edges_compact": [
            {
                "a": e.get("a"),
                "b": e.get("b"),
                "typed_matches": [
                    {
                        "edge_type": m.get("edge_type"),
                        "rule_index": m.get("rule_index"),
                    }
                    for m in (e.get("typed_matches") or [])
                ],
            }
            for e in graph_edges
        ],
    }
    graph_hash_v2 = sha256_hex(stable_json_dumps(graph_fingerprint_payload_v2))

    node_order = list(graph_slot_ids)

    state["graph_nodes"] = graph_nodes
    state["typed_rule_match_counts_before"] = typed_rule_match_counts_before
    state["typed_rule_match_counts_after"] = typed_rule_match_counts_after
    state["graph_edges"] = graph_edges
    state["graph_edge_index"] = graph_edge_index
    state["graph_adjacency"] = graph_adjacency
    state["graph_node_degrees"] = graph_node_degrees
    state["graph_components"] = graph_components
    state["graph_component_by_node"] = graph_component_by_node
    state["graph_totals"] = graph_totals
    state["graph_typed_edges_total"] = graph_typed_edges_total
    state["graph_typed_match_counts_by_type"] = graph_typed_match_counts_by_type
    state["graph_typed_edges_by_type"] = graph_typed_edges_by_type
    state["graph_rules_meta"] = graph_rules_meta
    state["graph_fingerprint_payload_v1"] = graph_fingerprint_payload_v1
    state["graph_hash_v1"] = graph_hash_v1
    state["graph_fingerprint_payload_v2"] = graph_fingerprint_payload_v2
    state["graph_hash_v2"] = graph_hash_v2
    state["node_order"] = node_order

    return state
