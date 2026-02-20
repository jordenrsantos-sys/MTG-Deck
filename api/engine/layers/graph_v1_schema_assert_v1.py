from __future__ import annotations

from typing import Any, Dict, List, Set


GRAPH_V1_SCHEMA_ASSERT_V1_VERSION = "graph_v1_schema_assert_v1"

_REQUIRED_TOP_LEVEL_KEYS = (
    "bipartite",
    "candidate_edges",
    "bounds",
    "stats",
)
_ALLOWED_TOP_LEVEL_KEYS = set(_REQUIRED_TOP_LEVEL_KEYS)

_REQUIRED_BIPARTITE_KEYS = {"nodes", "edges", "stats"}
_REQUIRED_BOUNDS_KEYS = {
    "MAX_PRIMS_PER_SLOT",
    "MAX_SLOTS_PER_PRIM",
    "MAX_CARD_CARD_EDGES_TOTAL",
}
_REQUIRED_STATS_KEYS = {
    "n_slot_nodes",
    "n_prim_nodes",
    "n_bipartite_edges",
    "n_candidate_edges",
    "caps_hit",
}
_REQUIRED_CAPS_HIT_KEYS = {
    "max_prims_per_slot",
    "max_slots_per_prim",
    "max_edges_total",
}
_REQUIRED_BIPARTITE_STATS_KEYS = {
    "slot_nodes_total",
    "primitive_nodes_total",
    "bipartite_nodes_total",
    "bipartite_edges_total",
    "slots_with_primitives_total",
    "max_primitives_per_slot",
}

_NODE_REQUIRED_KEYS = {"id", "kind"}
_BIPARTITE_EDGE_REQUIRED_KEYS = {"a", "b", "kind"}
_CANDIDATE_EDGE_REQUIRED_KEYS = {"a", "b", "kind", "shared_primitives"}


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _is_nonempty_string_list(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    return all(_nonempty_str(item) is not None for item in value)


def _sorted_codes(codes: Set[str]) -> List[str]:
    return sorted({code for code in codes if isinstance(code, str) and code.strip() != ""})


def _is_non_negative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def run_graph_v1_schema_assert_v1(graph_v1_payload: dict) -> dict:
    codes: Set[str] = set()

    if not isinstance(graph_v1_payload, dict):
        return {
            "version": GRAPH_V1_SCHEMA_ASSERT_V1_VERSION,
            "status": "ERROR",
            "codes": ["GRAPH_V1_PAYLOAD_NOT_OBJECT"],
            "reason_code": "GRAPH_V1_SCHEMA_ASSERT_FAILED",
        }

    top_level_keys = set(graph_v1_payload.keys())
    for key in _REQUIRED_TOP_LEVEL_KEYS:
        if key not in top_level_keys:
            codes.add("GRAPH_V1_REQUIRED_TOP_LEVEL_KEY_MISSING")

    extra_top_level_keys = [key for key in top_level_keys if key not in _ALLOWED_TOP_LEVEL_KEYS]
    if len(extra_top_level_keys) > 0:
        codes.add("GRAPH_V1_TOP_LEVEL_EXTRA_KEYS")

    bipartite = graph_v1_payload.get("bipartite")
    if not isinstance(bipartite, dict):
        codes.add("GRAPH_V1_BIPARTITE_NOT_OBJECT")
        bipartite = {}

    bounds = graph_v1_payload.get("bounds")
    if not isinstance(bounds, dict):
        codes.add("GRAPH_V1_BOUNDS_NOT_OBJECT")
        bounds = {}

    if isinstance(bounds, dict):
        if set(bounds.keys()) != _REQUIRED_BOUNDS_KEYS:
            codes.add("GRAPH_V1_BOUNDS_SHAPE_INVALID")
        for key in _REQUIRED_BOUNDS_KEYS:
            if not _is_non_negative_int(bounds.get(key)):
                codes.add("GRAPH_V1_BOUNDS_VALUE_INVALID")

    stats = graph_v1_payload.get("stats")
    if not isinstance(stats, dict):
        codes.add("GRAPH_V1_STATS_NOT_OBJECT")
        stats = {}

    if isinstance(stats, dict):
        if set(stats.keys()) != _REQUIRED_STATS_KEYS:
            codes.add("GRAPH_V1_STATS_SHAPE_INVALID")

        for key in ("n_slot_nodes", "n_prim_nodes", "n_bipartite_edges", "n_candidate_edges"):
            if not _is_non_negative_int(stats.get(key)):
                codes.add("GRAPH_V1_STATS_VALUE_INVALID")

        caps_hit = stats.get("caps_hit")
        if not isinstance(caps_hit, dict):
            codes.add("GRAPH_V1_STATS_CAPS_HIT_NOT_OBJECT")
        else:
            if set(caps_hit.keys()) != _REQUIRED_CAPS_HIT_KEYS:
                codes.add("GRAPH_V1_STATS_CAPS_HIT_SHAPE_INVALID")
            for key in _REQUIRED_CAPS_HIT_KEYS:
                if not isinstance(caps_hit.get(key), bool):
                    codes.add("GRAPH_V1_STATS_CAPS_HIT_VALUE_INVALID")

    nodes = bipartite.get("nodes") if isinstance(bipartite.get("nodes"), list) else None
    if nodes is None:
        codes.add("GRAPH_V1_NODES_NOT_LIST")
        nodes = []

    bipartite_edges = bipartite.get("edges") if isinstance(bipartite.get("edges"), list) else None
    if bipartite_edges is None:
        codes.add("GRAPH_V1_BIPARTITE_EDGES_NOT_LIST")
        bipartite_edges = []

    if isinstance(bipartite, dict):
        if set(bipartite.keys()) != _REQUIRED_BIPARTITE_KEYS:
            codes.add("GRAPH_V1_BIPARTITE_SHAPE_INVALID")

        if not isinstance(bipartite.get("stats"), dict):
            codes.add("GRAPH_V1_BIPARTITE_STATS_NOT_OBJECT")
        else:
            bipartite_stats = bipartite.get("stats")
            if set(bipartite_stats.keys()) != _REQUIRED_BIPARTITE_STATS_KEYS:
                codes.add("GRAPH_V1_BIPARTITE_STATS_SHAPE_INVALID")
            for key in _REQUIRED_BIPARTITE_STATS_KEYS:
                if not _is_non_negative_int(bipartite_stats.get(key)):
                    codes.add("GRAPH_V1_BIPARTITE_STATS_VALUE_INVALID")

    candidate_edges = graph_v1_payload.get("candidate_edges")
    if not isinstance(candidate_edges, list):
        codes.add("GRAPH_V1_CANDIDATE_EDGES_NOT_LIST")
        candidate_edges = []

    for node in nodes:
        if not isinstance(node, dict):
            codes.add("GRAPH_V1_NODE_NOT_OBJECT")
            continue

        if set(node.keys()) != _NODE_REQUIRED_KEYS:
            codes.add("GRAPH_V1_NODE_SHAPE_INVALID")

        node_id = _nonempty_str(node.get("id"))
        node_kind = _nonempty_str(node.get("kind"))

        if node_id is None:
            codes.add("GRAPH_V1_NODE_ID_INVALID")
        if node_kind not in {"slot", "primitive"}:
            codes.add("GRAPH_V1_NODE_KIND_INVALID")

    for edge in bipartite_edges:
        if not isinstance(edge, dict):
            codes.add("GRAPH_V1_BIPARTITE_EDGE_NOT_OBJECT")
            continue

        if set(edge.keys()) != _BIPARTITE_EDGE_REQUIRED_KEYS:
            codes.add("GRAPH_V1_BIPARTITE_EDGE_SHAPE_INVALID")

        if _nonempty_str(edge.get("a")) is None or _nonempty_str(edge.get("b")) is None:
            codes.add("GRAPH_V1_BIPARTITE_EDGE_ENDPOINT_INVALID")

        if _nonempty_str(edge.get("kind")) != "has_primitive":
            codes.add("GRAPH_V1_BIPARTITE_EDGE_KIND_INVALID")

    for edge in candidate_edges:
        if not isinstance(edge, dict):
            codes.add("GRAPH_V1_CANDIDATE_EDGE_NOT_OBJECT")
            continue

        if set(edge.keys()) != _CANDIDATE_EDGE_REQUIRED_KEYS:
            codes.add("GRAPH_V1_CANDIDATE_EDGE_SHAPE_INVALID")

        if _nonempty_str(edge.get("a")) is None or _nonempty_str(edge.get("b")) is None:
            codes.add("GRAPH_V1_CANDIDATE_EDGE_ENDPOINT_INVALID")

        if _nonempty_str(edge.get("kind")) != "shared_primitive":
            codes.add("GRAPH_V1_CANDIDATE_EDGE_KIND_INVALID")

        shared_primitives = edge.get("shared_primitives")
        if not _is_nonempty_string_list(shared_primitives):
            codes.add("GRAPH_V1_CANDIDATE_EDGE_SHARED_PRIMITIVES_INVALID")
        else:
            normalized = [str(item) for item in shared_primitives]
            if normalized != sorted(set(normalized)):
                codes.add("GRAPH_V1_CANDIDATE_EDGE_SHARED_PRIMITIVES_ORDER_INVALID")

    if nodes != sorted(
        nodes,
        key=lambda node: (
            str(node.get("kind")) if isinstance(node, dict) else "",
            str(node.get("id")) if isinstance(node, dict) else "",
        ),
    ):
        codes.add("GRAPH_V1_NODE_ORDER_INVALID")

    if bipartite_edges != sorted(
        bipartite_edges,
        key=lambda edge: (
            str(edge.get("kind")) if isinstance(edge, dict) else "",
            str(edge.get("a")) if isinstance(edge, dict) else "",
            str(edge.get("b")) if isinstance(edge, dict) else "",
        ),
    ):
        codes.add("GRAPH_V1_BIPARTITE_EDGE_ORDER_INVALID")

    if candidate_edges != sorted(
        candidate_edges,
        key=lambda edge: (
            str(edge.get("kind")) if isinstance(edge, dict) else "",
            str(edge.get("a")) if isinstance(edge, dict) else "",
            str(edge.get("b")) if isinstance(edge, dict) else "",
        ),
    ):
        codes.add("GRAPH_V1_CANDIDATE_EDGE_ORDER_INVALID")

    sorted_codes = _sorted_codes(codes)
    return {
        "version": GRAPH_V1_SCHEMA_ASSERT_V1_VERSION,
        "status": "OK" if len(sorted_codes) == 0 else "ERROR",
        "codes": sorted_codes,
        "reason_code": None if len(sorted_codes) == 0 else "GRAPH_V1_SCHEMA_ASSERT_FAILED",
    }
