from __future__ import annotations

from typing import Any, Dict, List

from api.engine.constants import (
    GRAPH_EXPAND_V1_MAX_CARD_CARD_EDGES_TOTAL,
    GRAPH_EXPAND_V1_MAX_PRIMS_PER_SLOT,
    GRAPH_EXPAND_V1_MAX_SLOTS_PER_PRIM,
)


TYPED_GRAPH_INVARIANTS_V1_VERSION = "typed_graph_invariants_v1"

_REQUIRED_TOP_LEVEL_KEYS = (
    "bipartite",
    "candidate_edges",
    "bounds",
    "stats",
)


def _empty_checks(*, graph_present: bool) -> Dict[str, Any]:
    return {
        "graph_present": graph_present,
        "node_count": 0,
        "edge_count": 0,
        "duplicate_node_ids": 0,
        "dangling_edges": 0,
        "self_edges": 0,
        "invalid_node_refs": 0,
        "bounds_ok": True,
        "ordering_ok": True,
    }


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _add_error(errors: List[Dict[str, str]], *, code: str, message: str, path: str) -> None:
    errors.append(
        {
            "code": str(code),
            "message": str(message),
            "path": str(path),
        }
    )


def _error_sort_key(error: Dict[str, str]) -> tuple[str, str, str]:
    return (
        str(error.get("code") or ""),
        str(error.get("path") or ""),
        str(error.get("message") or ""),
    )


def _bound_value(bounds_obj: Dict[str, Any], key: str, default_value: int, errors: List[Dict[str, str]]) -> int:
    if key not in bounds_obj:
        return default_value

    raw = bounds_obj.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        _add_error(
            errors,
            code="GRAPH_BOUND_INVALID",
            message=f"Bounds field {key} must be a non-negative integer.",
            path=f"$.bounds.{key}",
        )
        return default_value

    return raw


def _collect_non_json_container_paths(value: Any, path: str) -> List[tuple[str, str]]:
    out: List[tuple[str, str]] = []

    if isinstance(value, (set, frozenset)):
        out.append((path, type(value).__name__))
        return out

    if isinstance(value, tuple):
        out.append((path, "tuple"))
        for idx, item in enumerate(value):
            out.extend(_collect_non_json_container_paths(item, f"{path}[{idx}]"))
        return out

    if isinstance(value, list):
        for idx, item in enumerate(value):
            out.extend(_collect_non_json_container_paths(item, f"{path}[{idx}]"))
        return out

    if isinstance(value, dict):
        for key in sorted(value.keys(), key=lambda item: str(item)):
            if isinstance(key, str):
                key_token = key
            else:
                key_token = repr(key)
            out.extend(_collect_non_json_container_paths(value.get(key), f"{path}.{key_token}"))

    return out


def _is_sorted_by(items: List[Any], key_fn) -> bool:
    if len(items) < 2:
        return True
    for idx in range(1, len(items)):
        if key_fn(items[idx - 1]) > key_fn(items[idx]):
            return False
    return True


def _validate_edge_refs(
    *,
    edges: List[Any],
    node_ids: set[str],
    path_prefix: str,
    errors: List[Dict[str, str]],
) -> tuple[int, int, int]:
    dangling_edges = 0
    self_edges = 0
    invalid_refs = 0

    for idx, edge in enumerate(edges):
        path = f"{path_prefix}[{idx}]"
        if not isinstance(edge, dict):
            invalid_refs += 1
            _add_error(
                errors,
                code="GRAPH_EDGE_INVALID_TYPE",
                message="Edge entry must be an object.",
                path=path,
            )
            continue

        a = _nonempty_str(edge.get("a"))
        b = _nonempty_str(edge.get("b"))
        if a is None or b is None:
            invalid_refs += 1
            _add_error(
                errors,
                code="GRAPH_EDGE_REF_INVALID",
                message="Edge must contain non-empty string refs for both 'a' and 'b'.",
                path=path,
            )
            continue

        if a == b:
            self_edges += 1
            _add_error(
                errors,
                code="GRAPH_SELF_EDGE",
                message=f"Edge self-reference detected for node id {a}.",
                path=path,
            )

        if a not in node_ids or b not in node_ids:
            dangling_edges += 1
            _add_error(
                errors,
                code="GRAPH_DANGLING_EDGE",
                message=f"Edge references unknown node ids: a={a}, b={b}.",
                path=path,
            )

    return dangling_edges, self_edges, invalid_refs


def run_typed_graph_invariants_v1(graph_v1: dict | None) -> Dict[str, Any]:
    checks = _empty_checks(graph_present=False)

    if not isinstance(graph_v1, dict) or len(graph_v1) == 0:
        return {
            "version": TYPED_GRAPH_INVARIANTS_V1_VERSION,
            "status": "SKIP",
            "errors": [],
            "checks": checks,
        }

    checks["graph_present"] = True
    errors: List[Dict[str, str]] = []

    for key in _REQUIRED_TOP_LEVEL_KEYS:
        if key not in graph_v1:
            _add_error(
                errors,
                code="GRAPH_TOP_LEVEL_KEY_MISSING",
                message=f"Required top-level key '{key}' is missing.",
                path=f"$.{key}",
            )

    bipartite_raw = graph_v1.get("bipartite")
    if not isinstance(bipartite_raw, dict):
        _add_error(
            errors,
            code="GRAPH_BIPARTITE_INVALID_TYPE",
            message="Top-level 'bipartite' must be an object.",
            path="$.bipartite",
        )
        bipartite: Dict[str, Any] = {}
    else:
        bipartite = bipartite_raw

    nodes_raw = bipartite.get("nodes")
    if not isinstance(nodes_raw, list):
        _add_error(
            errors,
            code="GRAPH_NODES_INVALID_TYPE",
            message="bipartite.nodes must be a list.",
            path="$.bipartite.nodes",
        )
        nodes: List[Any] = []
    else:
        nodes = nodes_raw

    bipartite_edges_raw = bipartite.get("edges")
    if not isinstance(bipartite_edges_raw, list):
        _add_error(
            errors,
            code="GRAPH_EDGES_INVALID_TYPE",
            message="bipartite.edges must be a list.",
            path="$.bipartite.edges",
        )
        bipartite_edges: List[Any] = []
    else:
        bipartite_edges = bipartite_edges_raw

    candidate_edges_raw = graph_v1.get("candidate_edges")
    if not isinstance(candidate_edges_raw, list):
        _add_error(
            errors,
            code="GRAPH_CANDIDATE_EDGES_INVALID_TYPE",
            message="candidate_edges must be a list.",
            path="$.candidate_edges",
        )
        candidate_edges: List[Any] = []
    else:
        candidate_edges = candidate_edges_raw

    stats_raw = graph_v1.get("stats")
    if not isinstance(stats_raw, dict):
        _add_error(
            errors,
            code="GRAPH_STATS_INVALID_TYPE",
            message="Top-level 'stats' must be an object.",
            path="$.stats",
        )

    bipartite_stats_raw = bipartite.get("stats")
    if not isinstance(bipartite_stats_raw, dict):
        _add_error(
            errors,
            code="GRAPH_BIPARTITE_STATS_INVALID_TYPE",
            message="bipartite.stats must be an object.",
            path="$.bipartite.stats",
        )

    checks["node_count"] = len(nodes)
    checks["edge_count"] = len(bipartite_edges) + len(candidate_edges)

    node_id_counts: Dict[str, int] = {}
    invalid_node_refs = 0
    for idx, node in enumerate(nodes):
        path = f"$.bipartite.nodes[{idx}]"
        if not isinstance(node, dict):
            invalid_node_refs += 1
            _add_error(
                errors,
                code="GRAPH_NODE_INVALID_TYPE",
                message="Node entry must be an object.",
                path=path,
            )
            continue

        node_id = _nonempty_str(node.get("id"))
        if node_id is None:
            invalid_node_refs += 1
            _add_error(
                errors,
                code="GRAPH_NODE_ID_INVALID",
                message="Node must contain a non-empty string 'id'.",
                path=f"{path}.id",
            )
            continue

        node_id_counts[node_id] = node_id_counts.get(node_id, 0) + 1

    duplicate_ids = sorted([node_id for node_id, count in node_id_counts.items() if count > 1])
    duplicate_node_ids_total = sum(node_id_counts[node_id] - 1 for node_id in duplicate_ids)
    checks["duplicate_node_ids"] = duplicate_node_ids_total
    if duplicate_ids:
        _add_error(
            errors,
            code="GRAPH_DUPLICATE_NODE_IDS",
            message=f"Duplicate node ids detected: {', '.join(duplicate_ids)}.",
            path="$.bipartite.nodes",
        )

    known_node_ids = set(node_id_counts.keys())

    bip_dangling, bip_self, bip_invalid = _validate_edge_refs(
        edges=bipartite_edges,
        node_ids=known_node_ids,
        path_prefix="$.bipartite.edges",
        errors=errors,
    )
    cand_dangling, cand_self, cand_invalid = _validate_edge_refs(
        edges=candidate_edges,
        node_ids=known_node_ids,
        path_prefix="$.candidate_edges",
        errors=errors,
    )

    checks["dangling_edges"] = bip_dangling + cand_dangling
    checks["self_edges"] = bip_self + cand_self
    checks["invalid_node_refs"] = invalid_node_refs + bip_invalid + cand_invalid

    bounds_raw = graph_v1.get("bounds")
    if not isinstance(bounds_raw, dict):
        _add_error(
            errors,
            code="GRAPH_BOUNDS_INVALID_TYPE",
            message="Top-level 'bounds' must be an object.",
            path="$.bounds",
        )
        bounds_obj: Dict[str, Any] = {}
    else:
        bounds_obj = bounds_raw

    max_prims_per_slot = _bound_value(
        bounds_obj,
        key="MAX_PRIMS_PER_SLOT",
        default_value=int(GRAPH_EXPAND_V1_MAX_PRIMS_PER_SLOT),
        errors=errors,
    )
    max_slots_per_prim = _bound_value(
        bounds_obj,
        key="MAX_SLOTS_PER_PRIM",
        default_value=int(GRAPH_EXPAND_V1_MAX_SLOTS_PER_PRIM),
        errors=errors,
    )
    max_card_card_edges_total = _bound_value(
        bounds_obj,
        key="MAX_CARD_CARD_EDGES_TOTAL",
        default_value=int(GRAPH_EXPAND_V1_MAX_CARD_CARD_EDGES_TOTAL),
        errors=errors,
    )

    slot_to_prim: Dict[str, set[str]] = {}
    prim_to_slot: Dict[str, set[str]] = {}
    for edge in bipartite_edges:
        if not isinstance(edge, dict):
            continue
        node_a = _nonempty_str(edge.get("a"))
        node_b = _nonempty_str(edge.get("b"))
        if node_a is None or node_b is None:
            continue
        if not node_a.startswith("slot:") or not node_b.startswith("prim:"):
            continue
        slot_to_prim.setdefault(node_a, set()).add(node_b)
        prim_to_slot.setdefault(node_b, set()).add(node_a)

    observed_max_prims_per_slot = max((len(items) for items in slot_to_prim.values()), default=0)
    observed_max_slots_per_prim = max((len(items) for items in prim_to_slot.values()), default=0)
    observed_card_card_edges_total = len(candidate_edges)

    bounds_ok = True
    if observed_max_prims_per_slot > max_prims_per_slot:
        bounds_ok = False
        _add_error(
            errors,
            code="GRAPH_BOUNDS_EXCEEDED",
            message=(
                "Observed max primitives-per-slot "
                f"({observed_max_prims_per_slot}) exceeds MAX_PRIMS_PER_SLOT ({max_prims_per_slot})."
            ),
            path="$.bipartite.edges",
        )

    if observed_max_slots_per_prim > max_slots_per_prim:
        bounds_ok = False
        _add_error(
            errors,
            code="GRAPH_BOUNDS_EXCEEDED",
            message=(
                "Observed max slots-per-primitive "
                f"({observed_max_slots_per_prim}) exceeds MAX_SLOTS_PER_PRIM ({max_slots_per_prim})."
            ),
            path="$.bipartite.edges",
        )

    if observed_card_card_edges_total > max_card_card_edges_total:
        bounds_ok = False
        _add_error(
            errors,
            code="GRAPH_BOUNDS_EXCEEDED",
            message=(
                "Observed candidate edge count "
                f"({observed_card_card_edges_total}) exceeds MAX_CARD_CARD_EDGES_TOTAL "
                f"({max_card_card_edges_total})."
            ),
            path="$.candidate_edges",
        )

    checks["bounds_ok"] = bounds_ok

    ordering_ok = True
    non_json_paths = _collect_non_json_container_paths(graph_v1, "$")
    for path, container_type in non_json_paths:
        ordering_ok = False
        _add_error(
            errors,
            code="GRAPH_NON_JSON_CONTAINER",
            message=f"Non-JSON container type detected: {container_type}.",
            path=path,
        )

    if nodes and all(isinstance(node, dict) for node in nodes):
        if not _is_sorted_by(nodes, key_fn=lambda node: (str(node.get("kind")), str(node.get("id")))):
            ordering_ok = False
            _add_error(
                errors,
                code="GRAPH_NODE_ORDER_UNSTABLE",
                message="bipartite.nodes must be sorted by (kind, id).",
                path="$.bipartite.nodes",
            )

    if bipartite_edges and all(isinstance(edge, dict) for edge in bipartite_edges):
        if not _is_sorted_by(
            bipartite_edges,
            key_fn=lambda edge: (str(edge.get("kind")), str(edge.get("a")), str(edge.get("b"))),
        ):
            ordering_ok = False
            _add_error(
                errors,
                code="GRAPH_EDGE_ORDER_UNSTABLE",
                message="bipartite.edges must be sorted by (kind, a, b).",
                path="$.bipartite.edges",
            )

    if candidate_edges and all(isinstance(edge, dict) for edge in candidate_edges):
        if not _is_sorted_by(
            candidate_edges,
            key_fn=lambda edge: (str(edge.get("kind")), str(edge.get("a")), str(edge.get("b"))),
        ):
            ordering_ok = False
            _add_error(
                errors,
                code="GRAPH_EDGE_ORDER_UNSTABLE",
                message="candidate_edges must be sorted by (kind, a, b).",
                path="$.candidate_edges",
            )

    checks["ordering_ok"] = ordering_ok

    errors_sorted = sorted(errors, key=_error_sort_key)
    return {
        "version": TYPED_GRAPH_INVARIANTS_V1_VERSION,
        "status": "OK" if len(errors_sorted) == 0 else "ERROR",
        "errors": errors_sorted,
        "checks": checks,
    }
