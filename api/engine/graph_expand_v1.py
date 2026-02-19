from __future__ import annotations

from typing import Any, Dict, List


DEFAULT_GRAPH_EXPAND_BOUNDS_V1 = {
    "MAX_PRIMS_PER_SLOT": 24,
    "MAX_SLOTS_PER_PRIM": 80,
    "MAX_CARD_CARD_EDGES_TOTAL": 5000,
}


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    clean = [value.strip() for value in values if isinstance(value, str) and value.strip() != ""]
    return sorted(set(clean))


def _normalize_bounds(bounds: Any) -> Dict[str, int]:
    bounds_obj = bounds if isinstance(bounds, dict) else {}
    out: Dict[str, int] = {}
    for key, default_value in DEFAULT_GRAPH_EXPAND_BOUNDS_V1.items():
        raw = bounds_obj.get(key)
        if isinstance(raw, bool):
            out[key] = default_value
            continue
        if isinstance(raw, int):
            out[key] = max(raw, 0)
            continue
        out[key] = default_value
    return out


def build_bipartite_graph_v1(deck_slot_ids, primitive_index_by_slot) -> Dict[str, Any]:
    slot_ids = _clean_sorted_unique_strings(deck_slot_ids)
    primitive_index = primitive_index_by_slot if isinstance(primitive_index_by_slot, dict) else {}

    slot_nodes = [{"id": f"slot:{slot_id}", "kind": "slot"} for slot_id in slot_ids]

    primitive_nodes = []
    edges = []
    primitive_ids_seen: set[str] = set()
    slots_with_primitives_total = 0
    max_primitives_per_slot = 0

    for slot_id in slot_ids:
        raw_primitives = primitive_index.get(slot_id)
        primitives = _clean_sorted_unique_strings(raw_primitives)
        if primitives:
            slots_with_primitives_total += 1
        if len(primitives) > max_primitives_per_slot:
            max_primitives_per_slot = len(primitives)

        for primitive_id in primitives:
            if primitive_id not in primitive_ids_seen:
                primitive_nodes.append({"id": f"prim:{primitive_id}", "kind": "primitive"})
                primitive_ids_seen.add(primitive_id)
            edges.append(
                {
                    "a": f"slot:{slot_id}",
                    "b": f"prim:{primitive_id}",
                    "kind": "has_primitive",
                }
            )

    nodes_sorted = sorted(slot_nodes + primitive_nodes, key=lambda node: (str(node.get("kind")), str(node.get("id"))))
    edges_sorted = sorted(
        edges,
        key=lambda edge: (str(edge.get("kind")), str(edge.get("a")), str(edge.get("b"))),
    )

    stats = {
        "slot_nodes_total": len(slot_nodes),
        "primitive_nodes_total": len(primitive_nodes),
        "bipartite_nodes_total": len(nodes_sorted),
        "bipartite_edges_total": len(edges_sorted),
        "slots_with_primitives_total": slots_with_primitives_total,
        "max_primitives_per_slot": max_primitives_per_slot,
    }

    return {
        "nodes": nodes_sorted,
        "edges": edges_sorted,
        "stats": stats,
    }


def expand_candidate_edges_v1(graph, bounds) -> Dict[str, Any]:
    graph_obj = graph if isinstance(graph, dict) else {}
    nodes = graph_obj.get("nodes") if isinstance(graph_obj.get("nodes"), list) else []
    edges = graph_obj.get("edges") if isinstance(graph_obj.get("edges"), list) else []
    limits = _normalize_bounds(bounds)

    max_prims_per_slot = limits["MAX_PRIMS_PER_SLOT"]
    max_slots_per_prim = limits["MAX_SLOTS_PER_PRIM"]
    max_card_card_edges_total = limits["MAX_CARD_CARD_EDGES_TOTAL"]

    slot_node_ids: set[str] = set()
    prim_node_ids: set[str] = set()
    for node in nodes:
        if not isinstance(node, dict):
            continue
        node_id = node.get("id")
        node_kind = node.get("kind")
        if not isinstance(node_id, str) or not isinstance(node_kind, str):
            continue
        if node_kind == "slot" and node_id.startswith("slot:"):
            slot_node_ids.add(node_id[5:])
        elif node_kind == "primitive" and node_id.startswith("prim:"):
            prim_node_ids.add(node_id[5:])

    slot_to_primitives_raw: Dict[str, set[str]] = {}
    n_bipartite_edges = 0
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        if edge.get("kind") != "has_primitive":
            continue

        node_a = edge.get("a")
        node_b = edge.get("b")
        if not isinstance(node_a, str) or not isinstance(node_b, str):
            continue
        if not node_a.startswith("slot:") or not node_b.startswith("prim:"):
            continue

        slot_id = node_a[5:].strip()
        primitive_id = node_b[5:].strip()
        if slot_id == "" or primitive_id == "":
            continue

        n_bipartite_edges += 1
        slot_to_primitives_raw.setdefault(slot_id, set()).add(primitive_id)

    slot_ids_sorted = sorted(slot_to_primitives_raw.keys())
    slot_to_primitives_capped: Dict[str, List[str]] = {}
    slots_capped_by_max_prims_total = 0

    for slot_id in slot_ids_sorted:
        primitives_sorted = sorted(slot_to_primitives_raw.get(slot_id, set()))

        primitives_capped = primitives_sorted[:max_prims_per_slot]
        if len(primitives_capped) < len(primitives_sorted):
            slots_capped_by_max_prims_total += 1

        slot_to_primitives_capped[slot_id] = primitives_capped

    primitive_to_slots_raw: Dict[str, List[str]] = {}
    for slot_id in slot_ids_sorted:
        for primitive_id in slot_to_primitives_capped.get(slot_id, []):
            primitive_to_slots_raw.setdefault(primitive_id, []).append(slot_id)

    primitive_to_slots_capped: Dict[str, List[str]] = {}
    primitives_capped_by_max_slots_total = 0

    for primitive_id in sorted(primitive_to_slots_raw.keys()):
        slots_sorted = sorted(set(primitive_to_slots_raw[primitive_id]))

        slots_capped = slots_sorted[:max_slots_per_prim]
        if len(slots_capped) < len(slots_sorted):
            primitives_capped_by_max_slots_total += 1

        if slots_capped:
            primitive_to_slots_capped[primitive_id] = slots_capped

    pair_to_shared_primitives: Dict[tuple[str, str], List[str]] = {}
    for primitive_id in sorted(primitive_to_slots_capped.keys()):
        slots = primitive_to_slots_capped[primitive_id]
        for left_idx in range(len(slots)):
            for right_idx in range(left_idx + 1, len(slots)):
                pair_key = (slots[left_idx], slots[right_idx])
                pair_to_shared_primitives.setdefault(pair_key, []).append(primitive_id)

    candidate_edges_full = []
    for slot_a, slot_b in sorted(pair_to_shared_primitives.keys()):
        shared_primitives = sorted(set(pair_to_shared_primitives[(slot_a, slot_b)]))
        candidate_edges_full.append(
            {
                "a": f"slot:{slot_a}",
                "b": f"slot:{slot_b}",
                "kind": "shared_primitive",
                "shared_primitives": shared_primitives,
            }
        )

    candidate_edges = candidate_edges_full[:max_card_card_edges_total]

    n_slot_nodes = len(sorted(slot_node_ids | set(slot_to_primitives_raw.keys())))
    n_prim_nodes = len(sorted(prim_node_ids | set(primitive_to_slots_raw.keys())))
    caps_hit = {
        "max_prims_per_slot": slots_capped_by_max_prims_total > 0,
        "max_slots_per_prim": primitives_capped_by_max_slots_total > 0,
        "max_edges_total": len(candidate_edges_full) > max_card_card_edges_total,
    }

    stats = {
        "n_slot_nodes": n_slot_nodes,
        "n_prim_nodes": n_prim_nodes,
        "n_bipartite_edges": n_bipartite_edges,
        "n_candidate_edges": len(candidate_edges),
        "caps_hit": caps_hit,
    }

    return {
        "candidate_edges": candidate_edges,
        "stats": stats,
    }
