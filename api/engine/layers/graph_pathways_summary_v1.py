from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple


GRAPH_PATHWAYS_SUMMARY_V1_VERSION = "graph_pathways_summary_v1"

_TOP_HUBS_LIMIT = 12
_TOP_EDGES_LIMIT = 15
_TOP_COMPONENTS_LIMIT = 6


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
        "version": GRAPH_PATHWAYS_SUMMARY_V1_VERSION,
        "status": "SKIP",
        "reason": reason,
        "top_hubs": [],
        "top_edges": [],
        "top_components": [],
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
        src = _nonempty_str(edge.get("src"))
        dst = _nonempty_str(edge.get("dst"))

    if src is None or dst is None:
        return None

    return src, dst


def _edge_weight(edge: Any) -> int | None:
    if not isinstance(edge, dict):
        return None

    raw = edge.get("weight")
    if isinstance(raw, int) and not isinstance(raw, bool):
        return int(raw)

    return None


def run_graph_pathways_summary_v1(
    graph_v1: Any,
    deck_slot_ids_playable: Any,
    typed_graph_invariants: Any = None,
    commander_slot_id: Any = None,
) -> Dict[str, Any]:
    if not isinstance(graph_v1, dict) or len(graph_v1) == 0:
        return _skip_payload("GRAPH_MISSING")

    if isinstance(typed_graph_invariants, dict):
        invariants_status = _nonempty_str(typed_graph_invariants.get("status"))
        if invariants_status == "ERROR":
            return _skip_payload("GRAPH_INVARIANTS_ERROR")

    bipartite = graph_v1.get("bipartite")
    if not isinstance(bipartite, dict):
        return _skip_payload("GRAPH_MALFORMED")

    nodes = bipartite.get("nodes")
    bipartite_edges = bipartite.get("edges")
    candidate_edges = graph_v1.get("candidate_edges")

    if not isinstance(nodes, list) or not isinstance(bipartite_edges, list) or not isinstance(candidate_edges, list):
        return _skip_payload("GRAPH_MALFORMED")

    playable_slot_ids = _clean_sorted_unique_strings(deck_slot_ids_playable)
    playable_slot_set = set(playable_slot_ids)
    commander_slot_token = _nonempty_str(commander_slot_id)

    node_id_by_slot: Dict[str, str] = {}
    for node in nodes:
        slot_id = _slot_id_from_node(node)
        node_id = _node_id(node)
        if slot_id is None or node_id is None:
            continue
        if slot_id not in playable_slot_set:
            continue

        existing = node_id_by_slot.get(slot_id)
        if existing is None or node_id < existing:
            node_id_by_slot[slot_id] = node_id

    slot_ids_in_graph = sorted(node_id_by_slot.keys())
    playable_node_id_set = set(node_id_by_slot.values())
    slot_id_by_node_id = {node_id: slot_id for slot_id, node_id in node_id_by_slot.items()}

    out_degree: Dict[str, int] = {slot_id: 0 for slot_id in slot_ids_in_graph}
    in_degree: Dict[str, int] = {slot_id: 0 for slot_id in slot_ids_in_graph}

    considered_edges: List[Dict[str, Any]] = []
    for edge in list(bipartite_edges) + list(candidate_edges):
        endpoints = _edge_endpoints(edge)
        if endpoints is None:
            continue

        src_node_id, dst_node_id = endpoints
        if src_node_id not in playable_node_id_set or dst_node_id not in playable_node_id_set:
            continue

        src_slot_id = slot_id_by_node_id.get(src_node_id)
        dst_slot_id = slot_id_by_node_id.get(dst_node_id)
        if src_slot_id is None or dst_slot_id is None:
            continue

        out_degree[src_slot_id] = out_degree.get(src_slot_id, 0) + 1
        in_degree[dst_slot_id] = in_degree.get(dst_slot_id, 0) + 1
        considered_edges.append(
            {
                "src": src_slot_id,
                "dst": dst_slot_id,
                "weight": _edge_weight(edge),
            }
        )

    top_hubs = sorted(
        [
            {
                "slot_id": slot_id,
                "degree_total": in_degree.get(slot_id, 0) + out_degree.get(slot_id, 0),
                "in_degree": in_degree.get(slot_id, 0),
                "out_degree": out_degree.get(slot_id, 0),
                "is_commander": bool(commander_slot_token is not None and slot_id == commander_slot_token),
            }
            for slot_id in slot_ids_in_graph
        ],
        key=lambda entry: (-int(entry.get("degree_total", 0)), str(entry.get("slot_id") or "")),
    )[:_TOP_HUBS_LIMIT]

    has_any_weight = any(edge.get("weight") is not None for edge in considered_edges)
    if has_any_weight:
        top_edges_sorted = sorted(
            considered_edges,
            key=lambda edge: (
                0 if edge.get("weight") is not None else 1,
                -(int(edge.get("weight")) if isinstance(edge.get("weight"), int) else 0),
                str(edge.get("src") or ""),
                str(edge.get("dst") or ""),
            ),
        )
    else:
        top_edges_sorted = sorted(
            considered_edges,
            key=lambda edge: (str(edge.get("src") or ""), str(edge.get("dst") or "")),
        )

    top_edges = [
        {
            "src": str(edge.get("src") or ""),
            "dst": str(edge.get("dst") or ""),
            "weight": edge.get("weight") if isinstance(edge.get("weight"), int) else None,
        }
        for edge in top_edges_sorted[:_TOP_EDGES_LIMIT]
    ]

    adjacency: Dict[str, Set[str]] = {slot_id: set() for slot_id in slot_ids_in_graph}
    for edge in considered_edges:
        src_slot_id = edge.get("src")
        dst_slot_id = edge.get("dst")
        if not isinstance(src_slot_id, str) or not isinstance(dst_slot_id, str):
            continue
        if src_slot_id not in adjacency or dst_slot_id not in adjacency:
            continue

        adjacency[src_slot_id].add(dst_slot_id)
        adjacency[dst_slot_id].add(src_slot_id)

    components_internal: List[Dict[str, Any]] = []
    visited: Set[str] = set()

    for start_slot_id in slot_ids_in_graph:
        if start_slot_id in visited:
            continue

        queue: List[str] = [start_slot_id]
        visited.add(start_slot_id)
        component_slots: List[str] = []

        while queue:
            current = queue.pop(0)
            component_slots.append(current)

            for neighbor in sorted(adjacency.get(current, set())):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)

        component_slot_set = set(component_slots)
        component_edge_count = sum(
            1
            for edge in considered_edges
            if edge.get("src") in component_slot_set and edge.get("dst") in component_slot_set
        )

        component_slots_sorted = sorted(component_slots)
        if len(component_slots_sorted) == 0:
            continue

        components_internal.append(
            {
                "node_count": len(component_slots_sorted),
                "edge_count": component_edge_count,
                "playable_nodes": len(component_slots_sorted),
                "smallest_slot_id": component_slots_sorted[0],
            }
        )

    components_ranked = sorted(
        components_internal,
        key=lambda entry: (-int(entry.get("node_count", 0)), str(entry.get("smallest_slot_id") or "")),
    )[:_TOP_COMPONENTS_LIMIT]

    top_components = [
        {
            "component_id": index + 1,
            "node_count": int(component.get("node_count", 0)),
            "edge_count": int(component.get("edge_count", 0)),
            "playable_nodes": int(component.get("playable_nodes", 0)),
        }
        for index, component in enumerate(components_ranked)
    ]

    return {
        "version": GRAPH_PATHWAYS_SUMMARY_V1_VERSION,
        "status": "OK",
        "reason": None,
        "top_hubs": top_hubs,
        "top_edges": top_edges,
        "top_components": top_components,
    }
