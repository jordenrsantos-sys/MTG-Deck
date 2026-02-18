from typing import Any, Dict, List


def run_disruption_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Disruption analysis layer (disruption_v1).
    Computes articulation nodes, bridge edges, commander isolation signals.
    Must preserve exact output + ordering + hashing payloads.
    """

    graph_totals = state["graph_totals"]
    graph_nodes = state["graph_nodes"]
    graph_node_degrees = state["graph_node_degrees"]
    node_order = state["node_order"]
    adj_simple = state["adj_simple"]
    graph_component_by_node = state["graph_component_by_node"]
    graph_edges = state["graph_edges"]
    sorted_unique = state["sorted_unique"]
    make_slot_id = state["make_slot_id"]
    graph_components = state["graph_components"]
    disruption_layer_version = state["disruption_layer_version"]
    disruption_ruleset_version = state["disruption_ruleset_version"]
    graph_hash_v2 = state["graph_hash_v2"]
    stable_json_dumps = state["stable_json_dumps"]
    sha256_hex = state["sha256_hex"]

    def analyze_graph(node_ids: List[str], adjacency_map: Dict[str, List[str]]) -> Dict[str, int]:
        node_set = set(node_ids)
        visited_local: set[str] = set()
        components = 0
        largest_component_size_local = 0
        for sid in node_ids:
            if sid in visited_local:
                continue
            components += 1
            queue = [sid]
            visited_local.add(sid)
            comp_size = 0
            while queue:
                current = queue.pop(0)
                comp_size += 1
                neighbors = [n for n in adjacency_map.get(current, []) if n in node_set]
                for neighbor in neighbors:
                    if neighbor not in visited_local:
                        visited_local.add(neighbor)
                        queue.append(neighbor)
            if comp_size > largest_component_size_local:
                largest_component_size_local = comp_size

        isolated = 0
        for sid in node_ids:
            neighbors = [n for n in adjacency_map.get(sid, []) if n in node_set]
            if len(neighbors) == 0:
                isolated += 1

        return {
            "components": components,
            "isolated": isolated,
            "largest_component_size": largest_component_size_local,
        }

    baseline_components = int(graph_totals.get("connected_components_total", 0))
    node_type_by_slot_disruption = {
        n.get("slot_id"): n.get("node_type")
        for n in graph_nodes
        if isinstance(n.get("slot_id"), str)
    }

    disruption_articulation_nodes: List[Dict[str, Any]] = []
    disruption_node_impact: List[Dict[str, Any]] = []
    for sid in node_order:
        remaining_nodes = [nid for nid in node_order if nid != sid]
        remaining_set = set(remaining_nodes)
        adj_removed = {
            nid: [n for n in adj_simple.get(nid, []) if n in remaining_set and n != sid]
            for nid in remaining_nodes
        }
        analysis = analyze_graph(remaining_nodes, adj_removed)
        components_after = analysis["components"]
        delta_components = components_after - baseline_components

        disruption_node_impact.append(
            {
                "slot_id": sid,
                "node_type": node_type_by_slot_disruption.get(sid),
                "degree": int(graph_node_degrees.get(sid, 0)),
                "components_after_removal": components_after,
                "delta_components": delta_components,
                "isolated_nodes_after_removal": analysis["isolated"],
                "largest_component_size_after_removal": analysis["largest_component_size"],
            }
        )

        if components_after > baseline_components:
            disruption_articulation_nodes.append(
                {
                    "slot_id": sid,
                    "node_type": node_type_by_slot_disruption.get(sid),
                    "baseline_components": baseline_components,
                    "components_after_removal": components_after,
                    "delta_components": delta_components,
                    "affected_component_id": graph_component_by_node.get(sid),
                }
            )

    disruption_articulation_nodes.sort(
        key=lambda x: (-int(x.get("delta_components", 0)), str(x.get("slot_id") or ""))
    )

    disruption_bridge_edges: List[Dict[str, Any]] = []
    for edge in graph_edges:
        a = edge.get("a")
        b = edge.get("b")
        if not isinstance(a, str) or not isinstance(b, str):
            continue
        adj_removed = {nid: list(adj_simple.get(nid, [])) for nid in node_order}
        if a in adj_removed:
            adj_removed[a] = [n for n in adj_removed[a] if n != b]
        if b in adj_removed:
            adj_removed[b] = [n for n in adj_removed[b] if n != a]

        analysis = analyze_graph(node_order, adj_removed)
        components_after = analysis["components"]
        if components_after > baseline_components:
            typed_edge_types = sorted_unique(
                {
                    m.get("edge_type")
                    for m in (edge.get("typed_matches") or [])
                    if isinstance(m.get("edge_type"), str)
                }
            )
            disruption_bridge_edges.append(
                {
                    "edge_key": f"{a}|{b}",
                    "a": a,
                    "b": b,
                    "baseline_components": baseline_components,
                    "components_after_removal": components_after,
                    "delta_components": components_after - baseline_components,
                    "shared_primitives_count": edge.get("shared_primitives_count", 0),
                    "typed_edge_types": typed_edge_types,
                }
            )

    commander_slot_id = make_slot_id("C", 0)
    commander_is_playable = commander_slot_id in node_order
    commander_is_isolated = commander_is_playable and int(graph_node_degrees.get(commander_slot_id, 0)) == 0
    commander_component_id = graph_component_by_node.get(commander_slot_id) if commander_is_playable else None
    component_nodes = []
    if commander_component_id is not None:
        for comp in graph_components:
            if comp.get("component_id") == commander_component_id:
                component_nodes = [n for n in (comp.get("nodes") or []) if isinstance(n, str)]
                break
    deck_nodes_in_commander_component_count = sum(
        1 for n in component_nodes if node_type_by_slot_disruption.get(n) == "DECK"
    )
    articulation_slot_ids = {a.get("slot_id") for a in disruption_articulation_nodes if isinstance(a.get("slot_id"), str)}
    risk_flags: List[str] = []
    if commander_is_playable and commander_is_isolated:
        risk_flags.append("COMMANDER_ISOLATED")
    if commander_is_playable and commander_slot_id in articulation_slot_ids:
        risk_flags.append("COMMANDER_ARTICULATION")
    risk_flags = sorted(risk_flags)

    disruption_commander_risk = {
        "commander_slot_id": commander_slot_id,
        "commander_is_playable": commander_is_playable,
        "commander_is_isolated": commander_is_isolated,
        "commander_component_id": commander_component_id,
        "deck_nodes_in_commander_component": deck_nodes_in_commander_component_count,
        "risk_flags": risk_flags,
    }

    disruption_totals = {
        "baseline_components": baseline_components,
        "articulation_nodes_total": len(disruption_articulation_nodes),
        "bridge_edges_total": len(disruption_bridge_edges),
        "max_delta_components_node": max(
            (int(x.get("delta_components", 0)) for x in disruption_node_impact), default=0
        ),
        "max_delta_components_edge": max(
            (int(x.get("delta_components", 0)) for x in disruption_bridge_edges), default=0
        ),
        "commander_isolated": commander_is_isolated,
    }

    disruption_fingerprint_payload_v1 = {
        "disruption_layer_version": disruption_layer_version,
        "disruption_ruleset_version": disruption_ruleset_version,
        "graph_hash_v2": graph_hash_v2,
        "articulation_nodes_compact": [
            {
                "slot_id": x.get("slot_id"),
                "delta_components": x.get("delta_components"),
            }
            for x in disruption_articulation_nodes
        ],
        "bridge_edges_compact": [
            {
                "edge_key": x.get("edge_key"),
                "delta_components": x.get("delta_components"),
            }
            for x in disruption_bridge_edges
        ],
        "commander_risk_flags": risk_flags,
    }
    disruption_hash_v1 = sha256_hex(stable_json_dumps(disruption_fingerprint_payload_v1))

    state["disruption_articulation_nodes"] = disruption_articulation_nodes
    state["disruption_node_impact"] = disruption_node_impact
    state["disruption_bridge_edges"] = disruption_bridge_edges
    state["disruption_commander_risk"] = disruption_commander_risk
    state["disruption_totals"] = disruption_totals
    state["disruption_fingerprint_payload_v1"] = disruption_fingerprint_payload_v1
    state["disruption_hash_v1"] = disruption_hash_v1

    return state
