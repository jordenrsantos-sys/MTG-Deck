from typing import Any, Dict, List, Optional


def run_combo_skeleton_v0(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Combo skeleton layer (combo_skeleton_v0).
    Detects cycles/loop structure and emits bounded small cycles.
    Must preserve exact output + ordering + hashing payloads.
    """

    graph_components = state["graph_components"]
    adj_simple = state["adj_simple"]
    edge_key_norm = state["edge_key_norm"]
    combo_skeleton_bfs_node_cap = state["combo_skeleton_bfs_node_cap"]
    max_triangles = state["max_triangles"]
    max_4cycles = state["max_4cycles"]
    combo_skeleton_layer_version = state["combo_skeleton_layer_version"]
    combo_skeleton_ruleset_version = state["combo_skeleton_ruleset_version"]
    graph_hash_v2 = state["graph_hash_v2"]
    stable_json_dumps = state["stable_json_dumps"]
    sha256_hex = state["sha256_hex"]

    combo_skeleton_components: List[Dict[str, Any]] = []
    combo_small_cycles_emitted_total = 0
    combo_triangles_total = 0
    combo_cycles4_total = 0

    for component in graph_components:
        component_id = component.get("component_id")
        component_nodes = [n for n in (component.get("nodes") or []) if isinstance(n, str)]
        if not isinstance(component_id, str):
            continue

        adj_comp: Dict[str, List[str]] = {}
        node_set = set(component_nodes)
        for sid in component_nodes:
            neighbors = [n for n in adj_simple.get(sid, []) if n in node_set]
            adj_comp[sid] = sorted(neighbors)

        edges_comp: set[str] = set()
        for sid in component_nodes:
            for neighbor in adj_comp.get(sid, []):
                edges_comp.add(edge_key_norm(sid, neighbor))

        n_nodes = len(component_nodes)
        n_edges = len(edges_comp)
        cyclomatic_number = max(n_edges - n_nodes + 1, 0)
        has_cycle = cyclomatic_number > 0

        smallest_cycle_length: Optional[int] = None
        smallest_cycle_length_reason: Optional[str] = None
        if n_nodes > combo_skeleton_bfs_node_cap:
            smallest_cycle_length_reason = "SKIPPED_SIZE_CAP"
        else:
            for start in component_nodes:
                dist: Dict[str, int] = {start: 0}
                parent: Dict[str, Optional[str]] = {start: None}
                queue = [start]
                while queue:
                    u = queue.pop(0)
                    for v in adj_comp.get(u, []):
                        if v not in dist:
                            dist[v] = dist[u] + 1
                            parent[v] = u
                            queue.append(v)
                        elif parent.get(u) != v:
                            cycle_len = dist[u] + dist[v] + 1
                            if smallest_cycle_length is None or cycle_len < smallest_cycle_length:
                                smallest_cycle_length = cycle_len

        small_cycles: List[Dict[str, Any]] = []
        if n_nodes <= combo_skeleton_bfs_node_cap:
            # Triangles (bounded)
            triangle_count = 0
            for i in range(n_nodes):
                if triangle_count >= max_triangles:
                    break
                for j in range(i + 1, n_nodes):
                    if triangle_count >= max_triangles:
                        break
                    for k in range(j + 1, n_nodes):
                        a = component_nodes[i]
                        b = component_nodes[j]
                        c = component_nodes[k]
                        if (
                            edge_key_norm(a, b) in edges_comp
                            and edge_key_norm(b, c) in edges_comp
                            and edge_key_norm(a, c) in edges_comp
                        ):
                            small_cycles.append(
                                {
                                    "cycle_len": 3,
                                    "nodes": [a, b, c],
                                }
                            )
                            triangle_count += 1
                            if triangle_count >= max_triangles:
                                break

            # 4-cycles (bounded, canonical i-j-k-l-i pattern)
            cycle4_count = 0
            for i in range(n_nodes):
                if cycle4_count >= max_4cycles:
                    break
                for j in range(i + 1, n_nodes):
                    if cycle4_count >= max_4cycles:
                        break
                    for k in range(j + 1, n_nodes):
                        if cycle4_count >= max_4cycles:
                            break
                        for l in range(k + 1, n_nodes):
                            a = component_nodes[i]
                            b = component_nodes[j]
                            c = component_nodes[k]
                            d = component_nodes[l]
                            if (
                                edge_key_norm(a, b) in edges_comp
                                and edge_key_norm(b, c) in edges_comp
                                and edge_key_norm(c, d) in edges_comp
                                and edge_key_norm(d, a) in edges_comp
                            ):
                                small_cycles.append(
                                    {
                                        "cycle_len": 4,
                                        "nodes": [a, b, c, d],
                                    }
                                )
                                cycle4_count += 1
                                if cycle4_count >= max_4cycles:
                                    break
        small_cycles_total = len(small_cycles)
        combo_small_cycles_emitted_total += small_cycles_total
        combo_triangles_total += sum(1 for c in small_cycles if c.get("cycle_len") == 3)
        combo_cycles4_total += sum(1 for c in small_cycles if c.get("cycle_len") == 4)

        combo_skeleton_components.append(
            {
                "component_id": component_id,
                "nodes_total": n_nodes,
                "edges_total": n_edges,
                "has_cycle": has_cycle,
                "cyclomatic_number": cyclomatic_number,
                "smallest_cycle_length": smallest_cycle_length,
                "smallest_cycle_length_reason": smallest_cycle_length_reason,
                "small_cycles": small_cycles,
                "small_cycles_total": small_cycles_total,
            }
        )

    cycle_lengths_present = [
        c.get("smallest_cycle_length")
        for c in combo_skeleton_components
        if isinstance(c.get("smallest_cycle_length"), int)
    ]
    combo_skeleton_totals = {
        "components_total": len(combo_skeleton_components),
        "components_with_cycles_total": sum(1 for c in combo_skeleton_components if c.get("has_cycle") is True),
        "total_cyclomatic_number": sum(int(c.get("cyclomatic_number", 0)) for c in combo_skeleton_components),
        "min_smallest_cycle_length": min(cycle_lengths_present) if cycle_lengths_present else None,
        "triangles_total": combo_triangles_total,
        "cycles4_total": combo_cycles4_total,
        "small_cycles_emitted_total": combo_small_cycles_emitted_total,
    }

    combo_skeleton_fingerprint_payload_v1 = {
        "combo_skeleton_layer_version": combo_skeleton_layer_version,
        "combo_skeleton_ruleset_version": combo_skeleton_ruleset_version,
        "graph_hash_v2": graph_hash_v2,
        "components_compact": [
            {
                "component_id": c.get("component_id"),
                "has_cycle": c.get("has_cycle"),
                "cyclomatic_number": c.get("cyclomatic_number"),
                "smallest_cycle_length": c.get("smallest_cycle_length"),
                "small_cycles_compact": [
                    {
                        "cycle_len": sc.get("cycle_len"),
                        "nodes": sc.get("nodes"),
                    }
                    for sc in (c.get("small_cycles") or [])
                ],
            }
            for c in combo_skeleton_components
        ],
    }
    combo_skeleton_hash_v1 = sha256_hex(stable_json_dumps(combo_skeleton_fingerprint_payload_v1))

    state["combo_skeleton_components"] = combo_skeleton_components
    state["combo_skeleton_totals"] = combo_skeleton_totals
    state["combo_skeleton_fingerprint_payload_v1"] = combo_skeleton_fingerprint_payload_v1
    state["combo_skeleton_hash_v1"] = combo_skeleton_hash_v1

    return state
