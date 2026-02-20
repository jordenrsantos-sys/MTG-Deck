from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple


GRAPH_ANALYTICS_SUMMARY_V1_VERSION = "graph_analytics_summary_v1"

_TOP_PRIMITIVES_LIMIT = 12


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    cleaned = {
        token
        for token in (_nonempty_str(value) for value in values)
        if token is not None
    }
    return sorted(cleaned)


def _skip_payload(reason: str) -> Dict[str, Any]:
    return {
        "version": GRAPH_ANALYTICS_SUMMARY_V1_VERSION,
        "status": "SKIP",
        "reason": reason,
        "counts": {
            "nodes": 0,
            "edges": 0,
            "playable_nodes": 0,
        },
        "top_primitives_by_slot_coverage": [],
        "connectivity": {
            "avg_out_degree": 0.0,
            "avg_in_degree": 0.0,
            "max_out_degree": 0,
            "max_in_degree": 0,
        },
        "components": {
            "component_count": 0,
            "largest_component_nodes": 0,
            "largest_component_edges": 0,
        },
    }


def _node_id(node: Any) -> str | None:
    if not isinstance(node, dict):
        return None

    direct_id = _nonempty_str(node.get("id"))
    if direct_id is not None:
        return direct_id

    legacy_id = _nonempty_str(node.get("node_id"))
    if legacy_id is not None:
        return legacy_id

    return None


def _slot_id_from_node(node: Any) -> str | None:
    if not isinstance(node, dict):
        return None

    slot_id = _nonempty_str(node.get("slot_id"))
    if slot_id is not None:
        return slot_id

    node_id = _node_id(node)
    if node_id is not None and node_id.startswith("slot:"):
        suffix = _nonempty_str(node_id[5:])
        if suffix is not None:
            return suffix

    return None


def _edge_endpoints(edge: Any) -> tuple[str, str] | None:
    if not isinstance(edge, dict):
        return None

    src = _nonempty_str(edge.get("a"))
    dst = _nonempty_str(edge.get("b"))

    if src is None or dst is None:
        return None

    return src, dst


def _round6(value: float) -> float:
    return float(round(value, 6))


def run_graph_analytics_summary_v1(
    graph_v1: Any,
    primitive_index_by_slot: Any,
    deck_slot_ids_playable: Any,
    typed_graph_invariants: Any = None,
) -> Dict[str, Any]:
    if not isinstance(graph_v1, dict) or len(graph_v1) == 0:
        return _skip_payload("GRAPH_MISSING")

    if isinstance(typed_graph_invariants, dict):
        invariant_status = _nonempty_str(typed_graph_invariants.get("status"))
        if invariant_status == "ERROR":
            return _skip_payload("GRAPH_INVARIANTS_ERROR")

    bipartite = graph_v1.get("bipartite")
    if not isinstance(bipartite, dict):
        return _skip_payload("GRAPH_MALFORMED")

    nodes = bipartite.get("nodes")
    bipartite_edges = bipartite.get("edges")
    candidate_edges = graph_v1.get("candidate_edges")

    if not isinstance(nodes, list) or not isinstance(bipartite_edges, list) or not isinstance(candidate_edges, list):
        return _skip_payload("GRAPH_MALFORMED")

    playable_slots = _clean_sorted_unique_strings(deck_slot_ids_playable)
    playable_slot_set = set(playable_slots)

    playable_nodes = 0
    for node in nodes:
        slot_id = _slot_id_from_node(node)
        if slot_id is not None and slot_id in playable_slot_set:
            playable_nodes += 1

    primitive_coverage: Dict[str, int] = {}
    primitive_index = primitive_index_by_slot if isinstance(primitive_index_by_slot, dict) else {}
    for slot_id in playable_slots:
        raw_primitives = primitive_index.get(slot_id)
        slot_primitives = _clean_sorted_unique_strings(raw_primitives)
        for primitive_id in slot_primitives:
            primitive_coverage[primitive_id] = primitive_coverage.get(primitive_id, 0) + 1

    top_primitives = [
        {
            "primitive": primitive_id,
            "slots": slot_count,
        }
        for primitive_id, slot_count in sorted(
            primitive_coverage.items(),
            key=lambda entry: (-int(entry[1]), str(entry[0])),
        )[:_TOP_PRIMITIVES_LIMIT]
    ]

    all_node_ids = sorted({_node_id(node) for node in nodes if _node_id(node) is not None})
    node_id_set = set(all_node_ids)

    in_degree: Dict[str, int] = {node_id: 0 for node_id in all_node_ids}
    out_degree: Dict[str, int] = {node_id: 0 for node_id in all_node_ids}

    valid_edges: List[Tuple[str, str]] = []
    for edge in list(bipartite_edges) + list(candidate_edges):
        endpoints = _edge_endpoints(edge)
        if endpoints is None:
            continue

        src, dst = endpoints
        if src not in node_id_set or dst not in node_id_set:
            continue

        out_degree[src] = out_degree.get(src, 0) + 1
        in_degree[dst] = in_degree.get(dst, 0) + 1
        valid_edges.append((src, dst))

    node_count_for_degree = len(all_node_ids)
    if node_count_for_degree > 0:
        avg_out_degree = _round6(sum(out_degree.values()) / float(node_count_for_degree))
        avg_in_degree = _round6(sum(in_degree.values()) / float(node_count_for_degree))
    else:
        avg_out_degree = 0.0
        avg_in_degree = 0.0

    max_out_degree = max(out_degree.values(), default=0)
    max_in_degree = max(in_degree.values(), default=0)

    adjacency: Dict[str, Set[str]] = {node_id: set() for node_id in all_node_ids}
    for src, dst in valid_edges:
        adjacency[src].add(dst)
        adjacency[dst].add(src)

    component_count = 0
    largest_component_nodes = 0
    largest_component_edges = 0
    visited: Set[str] = set()

    for start_node in all_node_ids:
        if start_node in visited:
            continue

        component_count += 1
        queue: List[str] = [start_node]
        visited.add(start_node)
        component_nodes: List[str] = []

        while queue:
            current = queue.pop(0)
            component_nodes.append(current)

            for neighbor in sorted(adjacency.get(current, set())):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)

        component_node_set = set(component_nodes)
        component_edge_count = sum(
            1
            for src, dst in valid_edges
            if src in component_node_set and dst in component_node_set
        )

        component_node_count = len(component_nodes)
        if (
            component_node_count > largest_component_nodes
            or (
                component_node_count == largest_component_nodes
                and component_edge_count > largest_component_edges
            )
        ):
            largest_component_nodes = component_node_count
            largest_component_edges = component_edge_count

    return {
        "version": GRAPH_ANALYTICS_SUMMARY_V1_VERSION,
        "status": "OK",
        "reason": None,
        "counts": {
            "nodes": len(nodes),
            "edges": len(bipartite_edges) + len(candidate_edges),
            "playable_nodes": playable_nodes,
        },
        "top_primitives_by_slot_coverage": top_primitives,
        "connectivity": {
            "avg_out_degree": avg_out_degree,
            "avg_in_degree": avg_in_degree,
            "max_out_degree": max_out_degree,
            "max_in_degree": max_in_degree,
        },
        "components": {
            "component_count": component_count,
            "largest_component_nodes": largest_component_nodes,
            "largest_component_edges": largest_component_edges,
        },
    }
