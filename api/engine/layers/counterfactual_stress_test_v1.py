from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from api.engine.constants_counterfactual import (
    COUNTERFACTUAL_ARTIFACT_RELIANCE_PRIMITIVE_ID,
    COUNTERFACTUAL_ENCHANTMENT_RELIANCE_PRIMITIVE_ID,
    COUNTERFACTUAL_GRAVEYARD_RELIANCE_PRIMITIVE_IDS,
)


COUNTERFACTUAL_STRESS_TEST_V1_VERSION = "counterfactual_stress_test_v1"

_TOP_HUB_SCENARIOS_LIMIT = 3


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def _round6(value: float) -> float:
    return float(round(float(value), 6))


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    cleaned = {
        token
        for token in (_nonempty_str(value) for value in values)
        if token is not None
    }
    return sorted(cleaned)


def _ordered_unique_nonempty_strings(values: Any) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()

    if not isinstance(values, list):
        return out

    for value in values:
        token = _nonempty_str(value)
        if token is None or token in seen:
            continue
        out.append(token)
        seen.add(token)

    return out


def _skip_payload(reason: str) -> Dict[str, Any]:
    return {
        "version": COUNTERFACTUAL_STRESS_TEST_V1_VERSION,
        "status": "SKIP",
        "reason": reason,
        "scenarios": [],
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


def _prepare_graph(
    graph_v1: Any,
    playable_slots: List[str],
) -> tuple[Dict[str, Set[str]], Dict[str, str]] | tuple[None, None]:
    if not isinstance(graph_v1, dict) or len(graph_v1) == 0:
        return None, None

    bipartite = graph_v1.get("bipartite")
    if not isinstance(bipartite, dict):
        return None, None

    nodes_raw = bipartite.get("nodes")
    bipartite_edges_raw = bipartite.get("edges")
    candidate_edges_raw = graph_v1.get("candidate_edges")

    if not isinstance(nodes_raw, list) or not isinstance(bipartite_edges_raw, list) or not isinstance(candidate_edges_raw, list):
        return None, None

    playable_slot_set = set(playable_slots)

    node_ids = sorted({_node_id(node) for node in nodes_raw if _node_id(node) is not None})
    node_id_set = set(node_ids)
    adjacency: Dict[str, Set[str]] = {node_id: set() for node_id in node_ids}

    slot_node_pairs: List[Tuple[str, str]] = []
    for node in nodes_raw:
        slot_id = _slot_id_from_node(node)
        node_id = _node_id(node)
        if slot_id is None or node_id is None:
            continue
        if slot_id not in playable_slot_set:
            continue
        slot_node_pairs.append((slot_id, node_id))

    slot_node_id_by_slot: Dict[str, str] = {}
    for slot_id, node_id in sorted(slot_node_pairs, key=lambda item: (item[0], item[1])):
        if slot_id not in slot_node_id_by_slot:
            slot_node_id_by_slot[slot_id] = node_id

    for slot_id in playable_slots:
        if slot_id in slot_node_id_by_slot:
            continue
        fallback_node_id = f"slot:{slot_id}"
        if fallback_node_id in node_id_set:
            slot_node_id_by_slot[slot_id] = fallback_node_id

    for edge in list(bipartite_edges_raw) + list(candidate_edges_raw):
        endpoints = _edge_endpoints(edge)
        if endpoints is None:
            continue

        src, dst = endpoints
        if src not in node_id_set or dst not in node_id_set:
            continue

        adjacency[src].add(dst)
        adjacency[dst].add(src)

    return adjacency, slot_node_id_by_slot


def _largest_playable_component_size(
    *,
    adjacency: Dict[str, Set[str]],
    playable_node_ids: Set[str],
    removed_slot_node_ids: Set[str],
) -> int:
    if len(playable_node_ids) == 0:
        return 0

    active_node_ids = {node_id for node_id in adjacency.keys() if node_id not in removed_slot_node_ids}
    active_playable_node_ids = {node_id for node_id in playable_node_ids if node_id in active_node_ids}
    if len(active_playable_node_ids) == 0:
        return 0

    visited: Set[str] = set()
    best = 0

    for start_node in sorted(active_node_ids):
        if start_node in visited:
            continue

        queue: List[str] = [start_node]
        visited.add(start_node)
        component_playable = 0

        while queue:
            current = queue.pop(0)
            if current in active_playable_node_ids:
                component_playable += 1

            for neighbor in sorted(adjacency.get(current, set())):
                if neighbor not in active_node_ids:
                    continue
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                queue.append(neighbor)

        if component_playable > best:
            best = component_playable

    return int(best)


def _build_notes(notes: List[Tuple[str, str]]) -> List[Dict[str, str]]:
    unique_notes = {
        (str(code), str(message))
        for code, message in notes
        if isinstance(code, str) and isinstance(message, str)
    }

    return [
        {
            "code": code,
            "message": message,
        }
        for code, message in sorted(unique_notes, key=lambda item: (item[0], item[1]))
    ]


def _top_hub_slot_ids(pathways: Any) -> List[str]:
    if not isinstance(pathways, dict):
        return []

    top_hubs_raw = pathways.get("top_hubs")
    if not isinstance(top_hubs_raw, list):
        return []

    hub_strength_by_slot: Dict[str, float] = {}
    for entry in top_hubs_raw:
        if not isinstance(entry, dict):
            continue
        slot_id = _nonempty_str(entry.get("slot_id"))
        if slot_id is None:
            continue

        degree_total = _number(entry.get("degree_total"))
        if degree_total is None:
            degree_total = 0.0

        prior = hub_strength_by_slot.get(slot_id)
        if prior is None or degree_total > prior:
            hub_strength_by_slot[slot_id] = degree_total

    ranked = sorted(
        hub_strength_by_slot.items(),
        key=lambda item: (-float(item[1]), str(item[0])),
    )
    return [slot_id for slot_id, _ in ranked[:_TOP_HUB_SCENARIOS_LIMIT]]


def _primitive_to_slots(
    primitive_index_by_slot: Dict[str, Any],
    playable_slots: List[str],
) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}

    for slot_id in playable_slots:
        slot_primitives = _clean_sorted_unique_strings(primitive_index_by_slot.get(slot_id))
        for primitive_id in slot_primitives:
            out.setdefault(primitive_id, set()).add(slot_id)

    return out


def _scenario_metrics(
    *,
    removed_slot_ids: Set[str],
    slot_node_id_by_slot: Dict[str, str],
    adjacency: Dict[str, Set[str]],
    playable_node_ids: Set[str],
    playable_nodes_before: int,
) -> Dict[str, Any]:
    removed_slot_node_ids = {
        slot_node_id_by_slot[slot_id]
        for slot_id in sorted(removed_slot_ids)
        if slot_id in slot_node_id_by_slot
    }

    playable_nodes_after = _largest_playable_component_size(
        adjacency=adjacency,
        playable_node_ids=playable_node_ids,
        removed_slot_node_ids=removed_slot_node_ids,
    )

    lost_nodes = max(0, int(playable_nodes_before) - int(playable_nodes_after))
    lost_fraction = _round6((float(lost_nodes) / float(playable_nodes_before)) if playable_nodes_before > 0 else 0.0)

    return {
        "playable_nodes_before": int(playable_nodes_before),
        "playable_nodes_after": int(playable_nodes_after),
        "lost_nodes": int(lost_nodes),
        "lost_fraction": float(lost_fraction),
    }


def run_counterfactual_stress_test_v1(
    graph_v1: Any,
    primitive_index_by_slot: Any,
    deck_slot_ids_playable: Any,
    typed_graph_invariants: Any = None,
    pathways: Any = None,
    commander_slot_id: Any = None,
) -> dict:
    if not isinstance(graph_v1, dict) or len(graph_v1) == 0:
        return _skip_payload("GRAPH_MISSING")

    if isinstance(typed_graph_invariants, dict):
        invariants_status = _nonempty_str(typed_graph_invariants.get("status"))
        if invariants_status == "ERROR":
            return _skip_payload("GRAPH_INVARIANTS_ERROR")

    if not isinstance(primitive_index_by_slot, dict) or not isinstance(deck_slot_ids_playable, list):
        return _skip_payload("PRIMITIVE_INDEX_UNAVAILABLE")

    playable_slots = _clean_sorted_unique_strings(deck_slot_ids_playable)
    adjacency, slot_node_id_by_slot = _prepare_graph(graph_v1=graph_v1, playable_slots=playable_slots)
    if adjacency is None or slot_node_id_by_slot is None:
        return _skip_payload("GRAPH_MALFORMED")

    playable_node_ids = set(slot_node_id_by_slot.values())
    playable_nodes_before = _largest_playable_component_size(
        adjacency=adjacency,
        playable_node_ids=playable_node_ids,
        removed_slot_node_ids=set(),
    )

    primitive_to_slots = _primitive_to_slots(primitive_index_by_slot=primitive_index_by_slot, playable_slots=playable_slots)
    playable_slot_set = set(playable_slots)

    scenarios: List[Dict[str, Any]] = []

    commander_slot_token = _nonempty_str(commander_slot_id)
    if commander_slot_token is not None:
        commander_notes: List[Tuple[str, str]] = []
        if commander_slot_token not in playable_slot_set:
            commander_notes.append(
                (
                    "COMMANDER_SLOT_NOT_PLAYABLE",
                    "Commander slot is not present in playable slots; scenario is no-op.",
                )
            )

        scenarios.append(
            {
                "scenario_id": "remove_commander_slot",
                "removed": {
                    "type": "slot",
                    "value": commander_slot_token,
                },
                "metrics": _scenario_metrics(
                    removed_slot_ids={commander_slot_token},
                    slot_node_id_by_slot=slot_node_id_by_slot,
                    adjacency=adjacency,
                    playable_node_ids=playable_node_ids,
                    playable_nodes_before=playable_nodes_before,
                ),
                "notes": _build_notes(commander_notes),
            }
        )

    hub_slot_ids = _top_hub_slot_ids(pathways)
    for index, hub_slot_id in enumerate(hub_slot_ids, start=1):
        hub_notes: List[Tuple[str, str]] = []
        if hub_slot_id not in playable_slot_set:
            hub_notes.append(
                (
                    "HUB_SLOT_NOT_PLAYABLE",
                    "Hub slot is not present in playable slots; scenario is no-op.",
                )
            )

        scenarios.append(
            {
                "scenario_id": f"remove_top_hub_{index}",
                "removed": {
                    "type": "slot",
                    "value": hub_slot_id,
                },
                "metrics": _scenario_metrics(
                    removed_slot_ids={hub_slot_id},
                    slot_node_id_by_slot=slot_node_id_by_slot,
                    adjacency=adjacency,
                    playable_node_ids=playable_node_ids,
                    playable_nodes_before=playable_nodes_before,
                ),
                "notes": _build_notes(hub_notes),
            }
        )

    for primitive_id in _ordered_unique_nonempty_strings(COUNTERFACTUAL_GRAVEYARD_RELIANCE_PRIMITIVE_IDS):
        primitive_notes: List[Tuple[str, str]] = []
        removed_slots = set(primitive_to_slots.get(primitive_id, set()))
        if len(removed_slots) == 0:
            primitive_notes.append(
                (
                    "PRIMITIVE_NOT_PRESENT_IN_PLAYABLE_SLOTS",
                    "Primitive does not appear in playable slots; scenario is no-op.",
                )
            )

        scenarios.append(
            {
                "scenario_id": f"remove_primitive_{primitive_id.lower()}",
                "removed": {
                    "type": "primitive",
                    "value": primitive_id,
                },
                "metrics": _scenario_metrics(
                    removed_slot_ids=removed_slots,
                    slot_node_id_by_slot=slot_node_id_by_slot,
                    adjacency=adjacency,
                    playable_node_ids=playable_node_ids,
                    playable_nodes_before=playable_nodes_before,
                ),
                "notes": _build_notes(primitive_notes),
            }
        )

    primitive_slot_scenarios = [
        ("artifact_reliance", _nonempty_str(COUNTERFACTUAL_ARTIFACT_RELIANCE_PRIMITIVE_ID)),
        ("enchantment_reliance", _nonempty_str(COUNTERFACTUAL_ENCHANTMENT_RELIANCE_PRIMITIVE_ID)),
    ]
    for category_label, primitive_id in primitive_slot_scenarios:
        primitive_notes: List[Tuple[str, str]] = []
        removed_value = primitive_id if primitive_id is not None else category_label
        removed_slots: Set[str] = set()

        if primitive_id is None:
            primitive_notes.append(
                (
                    "PRIMITIVE_ID_NOT_DEFINED",
                    "Primitive id is not defined for this scenario category; scenario is no-op.",
                )
            )
        else:
            removed_slots = set(primitive_to_slots.get(primitive_id, set()))
            if len(removed_slots) == 0:
                primitive_notes.append(
                    (
                        "PRIMITIVE_NOT_PRESENT_IN_PLAYABLE_SLOTS",
                        "Primitive does not appear in playable slots; scenario is no-op.",
                    )
                )

        scenarios.append(
            {
                "scenario_id": f"remove_primitive_{category_label}",
                "removed": {
                    "type": "primitive",
                    "value": removed_value,
                },
                "metrics": _scenario_metrics(
                    removed_slot_ids=removed_slots,
                    slot_node_id_by_slot=slot_node_id_by_slot,
                    adjacency=adjacency,
                    playable_node_ids=playable_node_ids,
                    playable_nodes_before=playable_nodes_before,
                ),
                "notes": _build_notes(primitive_notes),
            }
        )

    if len(scenarios) == 0:
        return _skip_payload("NO_SCENARIOS_AVAILABLE")

    return {
        "version": COUNTERFACTUAL_STRESS_TEST_V1_VERSION,
        "status": "OK",
        "reason": None,
        "scenarios": scenarios,
    }
