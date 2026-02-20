from __future__ import annotations

from typing import Any, Dict, List, Tuple

from api.engine.constants_disruption import (
    DISRUPTION_PRIMITIVE_IDS,
    DISRUPTION_PRIMITIVE_IDS_V1_VERSION,
)


DISRUPTION_SURFACE_V1_VERSION = "disruption_surface_v1"

_TOP_DISRUPTION_PRIMITIVES_LIMIT = 12
_HUB_MAPPING_LIMIT = 8
_TOP_COMPONENT_DISRUPTION_LIMIT = 5


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return int(value)
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


def _top_primitive_rows(
    primitive_counts: Dict[str, int],
    *,
    limit: int,
) -> List[Dict[str, int | str]]:
    return [
        {
            "primitive": primitive_id,
            "slots": slot_count,
        }
        for primitive_id, slot_count in sorted(
            primitive_counts.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )[:limit]
    ]


def _skip_payload(reason: str) -> Dict[str, Any]:
    return {
        "version": DISRUPTION_SURFACE_V1_VERSION,
        "status": "SKIP",
        "reason": reason,
        "definitions_version": DISRUPTION_PRIMITIVE_IDS_V1_VERSION,
        "totals": {
            "disruption_slots": 0,
            "disruption_primitives_hit": 0,
        },
        "top_disruption_primitives": [],
        "hub_mapping": [],
    }


def _build_slot_hits(
    primitive_index_by_slot: Dict[str, Any],
    playable_slots: List[str],
    disruption_primitive_set: set[str],
) -> Tuple[Dict[str, List[str]], Dict[str, int]]:
    slot_disruption_hits: Dict[str, List[str]] = {}
    primitive_counts: Dict[str, int] = {}

    for slot_id in playable_slots:
        primitives = _clean_sorted_unique_strings(primitive_index_by_slot.get(slot_id))
        disruption_hits = [primitive for primitive in primitives if primitive in disruption_primitive_set]
        if not disruption_hits:
            continue

        slot_disruption_hits[slot_id] = disruption_hits
        for primitive_id in disruption_hits:
            primitive_counts[primitive_id] = primitive_counts.get(primitive_id, 0) + 1

    return slot_disruption_hits, primitive_counts


def _build_hub_mapping(
    pathways_summary: Any,
    typed_graph_invariants: Any,
    playable_slots: List[str],
    slot_disruption_hits: Dict[str, List[str]],
) -> List[Dict[str, Any]]:
    invariants_status = _nonempty_str((typed_graph_invariants or {}).get("status")) if isinstance(typed_graph_invariants, dict) else None
    if invariants_status == "ERROR":
        return []

    if not isinstance(pathways_summary, dict):
        return []

    pathways_status = _nonempty_str(pathways_summary.get("status"))
    if pathways_status != "OK":
        return []

    top_hubs = pathways_summary.get("top_hubs")
    top_components = pathways_summary.get("top_components")
    if not isinstance(top_hubs, list) or not isinstance(top_components, list):
        return []

    playable_slot_set = set(playable_slots)

    component_slot_ids_by_id: Dict[int, List[str]] = {}
    for component in top_components:
        if not isinstance(component, dict):
            continue

        component_id = _nonnegative_int(component.get("component_id"))
        slot_ids_raw = component.get("slot_ids")
        if component_id is None or not isinstance(slot_ids_raw, list):
            continue

        slot_ids = [slot_id for slot_id in _clean_sorted_unique_strings(slot_ids_raw) if slot_id in playable_slot_set]
        component_slot_ids_by_id[component_id] = slot_ids

    if not component_slot_ids_by_id:
        return []

    hubs_normalized: List[Dict[str, Any]] = []
    for hub in top_hubs:
        if not isinstance(hub, dict):
            continue

        slot_id = _nonempty_str(hub.get("slot_id"))
        degree_total = _nonnegative_int(hub.get("degree_total"))
        component_id = _nonnegative_int(hub.get("component_id"))
        if slot_id is None or degree_total is None or component_id is None:
            continue

        component_slots = component_slot_ids_by_id.get(component_id)
        if not isinstance(component_slots, list):
            continue

        hubs_normalized.append(
            {
                "hub_slot_id": slot_id,
                "hub_degree_total": degree_total,
                "component_slot_ids": component_slots,
            }
        )

    hubs_ranked = sorted(
        hubs_normalized,
        key=lambda entry: (-int(entry.get("hub_degree_total", 0)), str(entry.get("hub_slot_id") or "")),
    )[:_HUB_MAPPING_LIMIT]

    hub_mapping: List[Dict[str, Any]] = []
    for hub in hubs_ranked:
        component_slot_ids = [
            slot_id
            for slot_id in _clean_sorted_unique_strings(hub.get("component_slot_ids"))
            if isinstance(slot_disruption_hits.get(slot_id), list)
        ]

        disruption_slots_in_component = len(component_slot_ids)

        primitive_counts: Dict[str, int] = {}
        for slot_id in component_slot_ids:
            for primitive_id in slot_disruption_hits.get(slot_id, []):
                primitive_counts[primitive_id] = primitive_counts.get(primitive_id, 0) + 1

        hub_mapping.append(
            {
                "hub_slot_id": str(hub.get("hub_slot_id") or ""),
                "hub_degree_total": int(hub.get("hub_degree_total") or 0),
                "disruption_slots_in_component": disruption_slots_in_component,
                "top_disruption_primitives_in_component": _top_primitive_rows(
                    primitive_counts,
                    limit=_TOP_COMPONENT_DISRUPTION_LIMIT,
                ),
            }
        )

    return sorted(
        hub_mapping,
        key=lambda entry: (-int(entry.get("hub_degree_total", 0)), str(entry.get("hub_slot_id") or "")),
    )


def run_disruption_surface_v1(
    primitive_index_by_slot: Any,
    deck_slot_ids_playable: Any,
    pathways_summary: Any = None,
    typed_graph_invariants: Any = None,
) -> Dict[str, Any]:
    disruption_primitives = _clean_sorted_unique_strings(DISRUPTION_PRIMITIVE_IDS)
    if not disruption_primitives:
        return _skip_payload("NO_DISRUPTION_PRIMITIVES_DEFINED")

    if not isinstance(primitive_index_by_slot, dict) or not isinstance(deck_slot_ids_playable, list):
        return _skip_payload("PRIMITIVE_INDEX_UNAVAILABLE")

    playable_slots = _clean_sorted_unique_strings(deck_slot_ids_playable)
    disruption_primitive_set = set(disruption_primitives)

    slot_disruption_hits, primitive_counts = _build_slot_hits(
        primitive_index_by_slot=primitive_index_by_slot,
        playable_slots=playable_slots,
        disruption_primitive_set=disruption_primitive_set,
    )

    disruption_slots = len(slot_disruption_hits)
    disruption_primitives_hit = len(primitive_counts)

    hub_mapping = _build_hub_mapping(
        pathways_summary=pathways_summary,
        typed_graph_invariants=typed_graph_invariants,
        playable_slots=playable_slots,
        slot_disruption_hits=slot_disruption_hits,
    )

    return {
        "version": DISRUPTION_SURFACE_V1_VERSION,
        "status": "OK",
        "reason": None,
        "definitions_version": DISRUPTION_PRIMITIVE_IDS_V1_VERSION,
        "totals": {
            "disruption_slots": disruption_slots,
            "disruption_primitives_hit": disruption_primitives_hit,
        },
        "top_disruption_primitives": _top_primitive_rows(
            primitive_counts,
            limit=_TOP_DISRUPTION_PRIMITIVES_LIMIT,
        ),
        "hub_mapping": hub_mapping,
    }
