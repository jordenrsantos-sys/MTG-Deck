from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from itertools import combinations
from typing import Any, Dict, List, Set, Tuple


PRIMITIVE_BRIDGE_EXPLORER_VERSION = "primitive_bridge_explorer_v1"

_MAX_CHAIN_HOPS = 3
_MAX_EVALUATED_CHAINS = 500
_MAX_LATENT_CLUSTERS = 120


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    return sorted(
        {
            token
            for token in (_nonempty_str(value) for value in values)
            if token is not None
        }
    )


def _round6_half_up(value: float) -> float:
    return float(Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _slot_id(token: Any) -> str | None:
    text = _nonempty_str(token)
    if text is None:
        return None
    if text.startswith("slot:"):
        suffix = _nonempty_str(text[5:])
        return suffix
    return text


def _normalize_primitive_index_by_slot(raw: Any) -> Dict[str, List[str]]:
    source = raw if isinstance(raw, dict) else {}
    normalized: Dict[str, List[str]] = {}
    for slot_key in sorted(source.keys(), key=lambda item: str(item)):
        slot_id = _nonempty_str(slot_key)
        if slot_id is None:
            continue
        normalized[slot_id] = _clean_sorted_unique_strings(source.get(slot_key))
    return normalized


def _normalize_slot_ids_by_primitive(
    raw: Any,
    *,
    primitive_index_by_slot: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    source = raw if isinstance(raw, dict) else {}
    normalized: Dict[str, Set[str]] = {}

    for primitive_key in sorted(source.keys(), key=lambda item: str(item)):
        primitive_id = _nonempty_str(primitive_key)
        if primitive_id is None:
            continue
        normalized.setdefault(primitive_id, set()).update(_clean_sorted_unique_strings(source.get(primitive_key)))

    for slot_id, primitives in primitive_index_by_slot.items():
        for primitive_id in primitives:
            normalized.setdefault(primitive_id, set()).add(slot_id)

    return {
        primitive_id: sorted(slot_ids)
        for primitive_id, slot_ids in sorted(normalized.items(), key=lambda item: item[0])
    }


def _build_slot_adjacency(
    graph_v1: Any,
    *,
    known_slot_ids: List[str],
) -> Dict[str, List[str]]:
    graph_payload = graph_v1 if isinstance(graph_v1, dict) else {}
    candidate_edges = graph_payload.get("candidate_edges") if isinstance(graph_payload.get("candidate_edges"), list) else []

    adjacency: Dict[str, Set[str]] = {slot_id: set() for slot_id in known_slot_ids}

    for edge in candidate_edges:
        if not isinstance(edge, dict):
            continue
        slot_a = _slot_id(edge.get("a"))
        slot_b = _slot_id(edge.get("b"))
        if slot_a is None or slot_b is None or slot_a == slot_b:
            continue
        adjacency.setdefault(slot_a, set()).add(slot_b)
        adjacency.setdefault(slot_b, set()).add(slot_a)

    return {
        slot_id: sorted(neighbors)
        for slot_id, neighbors in sorted(adjacency.items(), key=lambda item: item[0])
    }


def _build_primitive_adjacency(
    primitive_index_by_slot: Dict[str, List[str]],
    *,
    slot_adjacency: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    adjacency: Dict[str, Set[str]] = {}

    for slot_id in sorted(primitive_index_by_slot.keys()):
        slot_primitives = primitive_index_by_slot.get(slot_id, [])
        for primitive_id in slot_primitives:
            adjacency.setdefault(primitive_id, set())

        for left, right in combinations(slot_primitives, 2):
            if left == right:
                continue
            adjacency.setdefault(left, set()).add(right)
            adjacency.setdefault(right, set()).add(left)

    for slot_id in sorted(slot_adjacency.keys()):
        left_primitives = primitive_index_by_slot.get(slot_id, [])
        if len(left_primitives) == 0:
            continue

        for neighbor_slot_id in slot_adjacency.get(slot_id, []):
            right_primitives = primitive_index_by_slot.get(neighbor_slot_id, [])
            if len(right_primitives) == 0:
                continue
            for left in left_primitives:
                adjacency.setdefault(left, set())
                for right in right_primitives:
                    if left == right:
                        continue
                    adjacency.setdefault(right, set())
                    adjacency[left].add(right)
                    adjacency[right].add(left)

    return {
        primitive_id: sorted(neighbors)
        for primitive_id, neighbors in sorted(adjacency.items(), key=lambda item: item[0])
    }


def _commander_dependency_signal(metadata: Any) -> float:
    payload = metadata if isinstance(metadata, dict) else {}

    numeric_signal = payload.get("commander_dependency_signal_v1")
    if isinstance(numeric_signal, (int, float)) and not isinstance(numeric_signal, bool):
        return _clamp01(float(numeric_signal))

    requirements = payload.get("engine_requirements_v1") if isinstance(payload.get("engine_requirements_v1"), dict) else payload
    dependency_level = _nonempty_str(requirements.get("commander_dependent"))
    if dependency_level is None:
        return 0.5

    level = dependency_level.upper()
    if level == "LOW":
        return 0.0
    if level == "MED":
        return 0.5
    if level == "HIGH":
        return 1.0
    return 0.5


def _high_frequency_cutoff(primitive_counts: Dict[str, int]) -> int:
    if len(primitive_counts) == 0:
        return 999
    counts_sorted = sorted(max(0, int(count)) for count in primitive_counts.values())
    quartile_index = int((len(counts_sorted) - 1) * 0.75)
    quartile_value = counts_sorted[quartile_index]
    return max(3, int(quartile_value))


def _is_graveyard_reliant_primitive(primitive_id: str) -> bool:
    token = primitive_id.upper()
    markers = (
        "GRAVEYARD",
        "RECURSION",
        "REANIMATION",
        "SELF_MILL",
        "FLASHBACK",
    )
    return any(marker in token for marker in markers)


def _chain_scores(
    *,
    primitive_chain: List[str],
    slot_ids_by_primitive: Dict[str, List[str]],
    primitive_counts: Dict[str, int],
    primitive_concentration_index: float,
    commander_dependency_signal: float,
) -> Tuple[List[str], float, float, float, float]:
    slot_ids = sorted(
        {
            slot_id
            for primitive_id in primitive_chain
            for slot_id in slot_ids_by_primitive.get(primitive_id, [])
            if isinstance(slot_id, str) and slot_id.strip() != ""
        }
    )

    inverse_frequency_values: List[float] = []
    for primitive_id in primitive_chain:
        count = max(1, int(primitive_counts.get(primitive_id, 0)))
        inverse_frequency_values.append(1.0 / float(count))

    inverse_frequency_mean = (
        sum(inverse_frequency_values) / float(len(inverse_frequency_values))
        if len(inverse_frequency_values) > 0
        else 0.0
    )

    commander_irrelevance_weight = 1.0 - (0.75 * commander_dependency_signal)
    novelty_score = _clamp01(inverse_frequency_mean * (1.0 - primitive_concentration_index) * commander_irrelevance_weight * 2.0)

    support_slots = len(slot_ids)
    redundancy_score = _clamp01(float(support_slots) / float(max(1, len(primitive_chain) * 2)))

    single_slot_reliance = sum(1 for primitive_id in primitive_chain if int(primitive_counts.get(primitive_id, 0)) <= 1)
    single_slot_reliance_ratio = float(single_slot_reliance) / float(max(1, len(primitive_chain)))

    graveyard_reliance_ratio = float(
        sum(1 for primitive_id in primitive_chain if _is_graveyard_reliant_primitive(primitive_id))
    ) / float(max(1, len(primitive_chain)))

    vulnerability_score = _clamp01(
        (single_slot_reliance_ratio + graveyard_reliance_ratio + commander_dependency_signal) / 3.0
    )

    bridge_score = _clamp01(
        (novelty_score * 0.55)
        + ((1.0 - redundancy_score) * 0.20)
        + ((1.0 - vulnerability_score) * 0.25)
    )

    return (
        slot_ids,
        _round6_half_up(bridge_score),
        _round6_half_up(novelty_score),
        _round6_half_up(redundancy_score),
        _round6_half_up(vulnerability_score),
    )


def _classify_cluster_type(primitives: List[str]) -> str:
    joined = " ".join(primitives).upper()
    if any(marker in joined for marker in ("REPLACEMENT", "COPY", "CLONE")):
        return "replacement_abuse"
    if any(marker in joined for marker in ("LOCK", "STAX", "TAX", "DENIAL")):
        return "soft_lock"
    if any(marker in joined for marker in ("DOUBLE", "MULTIPLIER", "TREASURE", "MANA_RAMP")):
        return "mana_multiplier"
    if any(marker in joined for marker in ("RECURSION", "REANIMATION", "FLASHBACK")):
        return "recursive_engine"
    return "scaling_engine"


def _required_board_state_for_cluster(cluster_type: str) -> List[str]:
    lookup = {
        "recursive_engine": ["recursion_piece_online", "resource_in_graveyard"],
        "scaling_engine": ["repeatable_trigger_source", "payoff_piece_online"],
        "soft_lock": ["tax_or_denial_piece_online", "resource_upkeep_source"],
        "mana_multiplier": ["mana_engine_online", "mana_sink_available"],
        "replacement_abuse": ["replacement_effect_online", "event_source_online"],
    }
    return lookup.get(cluster_type, ["engine_piece_online"])


def _collect_latent_engine_clusters(
    *,
    primitive_adjacency: Dict[str, List[str]],
    slot_ids_by_primitive: Dict[str, List[str]],
    required_primitives_v0: List[str],
    commander_dependency_signal: float,
) -> List[Dict[str, Any]]:
    if commander_dependency_signal >= 0.95:
        return []

    required_set = set(required_primitives_v0)
    adjacency_sets = {
        primitive_id: set(neighbors)
        for primitive_id, neighbors in primitive_adjacency.items()
    }

    clusters: List[Dict[str, Any]] = []
    for primitive_triplet in combinations(sorted(primitive_adjacency.keys()), 3):
        if len(clusters) >= _MAX_LATENT_CLUSTERS:
            break

        p1, p2, p3 = primitive_triplet
        if p2 not in adjacency_sets.get(p1, set()):
            continue
        if p3 not in adjacency_sets.get(p1, set()):
            continue
        if p3 not in adjacency_sets.get(p2, set()):
            continue

        primitive_set = set(primitive_triplet)
        if len(required_set) > 0 and primitive_set.issubset(required_set):
            continue

        minimal_slot_set = sorted(
            {
                slot_id
                for primitive_id in primitive_triplet
                for slot_id in slot_ids_by_primitive.get(primitive_id, [])
                if isinstance(slot_id, str) and slot_id.strip() != ""
            }
        )
        if len(minimal_slot_set) == 0:
            continue

        cluster_type = _classify_cluster_type(list(primitive_triplet))
        required_board_state = _required_board_state_for_cluster(cluster_type)
        closure_potential = bool(len(minimal_slot_set) >= 3 and commander_dependency_signal < 0.75)

        clusters.append(
            {
                "cluster_type": cluster_type,
                "primitives": list(primitive_triplet),
                "required_board_state": sorted(required_board_state),
                "minimal_slot_set": minimal_slot_set,
                "closure_potential": closure_potential,
            }
        )

    return sorted(
        clusters,
        key=lambda entry: (
            str(entry.get("cluster_type") or ""),
            tuple(entry.get("primitives") or []),
            tuple(entry.get("minimal_slot_set") or []),
        ),
    )


def _cross_engine_overlap_score(
    *,
    bridge_clusters_v1: List[Dict[str, Any]],
    required_primitives_v0: List[str],
) -> float:
    required_set = set(required_primitives_v0)
    if len(required_set) == 0 or len(bridge_clusters_v1) == 0:
        return 0.0

    overlap_values: List[float] = []
    for row in bridge_clusters_v1:
        chain = row.get("primitive_chain") if isinstance(row.get("primitive_chain"), list) else []
        chain_set = {
            primitive
            for primitive in chain
            if isinstance(primitive, str) and primitive.strip() != ""
        }
        if len(chain_set) == 0:
            continue

        overlap_count = len(chain_set.intersection(required_set))
        if overlap_count == 0 or overlap_count == len(chain_set):
            continue

        overlap_values.append(float(overlap_count) / float(len(chain_set)))

    if len(overlap_values) == 0:
        return 0.0

    return _round6_half_up(sum(overlap_values) / float(len(overlap_values)))


def _structural_asymmetry_index(
    *,
    bridge_clusters_v1: List[Dict[str, Any]],
    bridge_amplification_bonus_v1: float,
) -> float:
    if len(bridge_clusters_v1) == 0:
        return 0.0

    base_values: List[float] = []
    for row in bridge_clusters_v1:
        bridge_score = row.get("bridge_score")
        redundancy_score = row.get("redundancy_score")
        if not isinstance(bridge_score, (int, float)) or isinstance(bridge_score, bool):
            continue
        if not isinstance(redundancy_score, (int, float)) or isinstance(redundancy_score, bool):
            continue
        base_values.append(_clamp01(float(bridge_score) * (1.0 - (0.5 * float(redundancy_score)))))

    if len(base_values) == 0:
        return 0.0

    base = sum(base_values) / float(len(base_values))
    adjusted = _clamp01(base * (1.0 + bridge_amplification_bonus_v1))
    return _round6_half_up(adjusted)


def run_primitive_bridge_explorer_v1(
    *,
    primitive_index_by_slot: Any,
    slot_ids_by_primitive: Any,
    graph_v1: Any,
    required_primitives_v0: Any = None,
    commander_dependency_metadata: Any = None,
    bridge_amplification_bonus_weight: Any = 0.0,
) -> Dict[str, Any]:
    primitive_index_clean = _normalize_primitive_index_by_slot(primitive_index_by_slot)

    if len(primitive_index_clean) == 0:
        return {
            "version": PRIMITIVE_BRIDGE_EXPLORER_VERSION,
            "status": "SKIP",
            "reason_code": "PRIMITIVE_INDEX_UNAVAILABLE",
            "codes": [],
            "bridge_clusters_v1": [],
            "latent_engine_clusters_v1": [],
            "cross_engine_overlap_score_v1": 0.0,
            "structural_asymmetry_index_v1": 0.0,
            "bridge_amplification_bonus_v1": 0.0,
            "bounds": {
                "max_chain_hops": _MAX_CHAIN_HOPS,
                "max_evaluated_chains": _MAX_EVALUATED_CHAINS,
                "evaluated_chain_candidates": 0,
            },
        }

    if not isinstance(graph_v1, dict):
        return {
            "version": PRIMITIVE_BRIDGE_EXPLORER_VERSION,
            "status": "SKIP",
            "reason_code": "GRAPH_V1_UNAVAILABLE",
            "codes": [],
            "bridge_clusters_v1": [],
            "latent_engine_clusters_v1": [],
            "cross_engine_overlap_score_v1": 0.0,
            "structural_asymmetry_index_v1": 0.0,
            "bridge_amplification_bonus_v1": 0.0,
            "bounds": {
                "max_chain_hops": _MAX_CHAIN_HOPS,
                "max_evaluated_chains": _MAX_EVALUATED_CHAINS,
                "evaluated_chain_candidates": 0,
            },
        }

    slot_ids_by_primitive_clean = _normalize_slot_ids_by_primitive(
        slot_ids_by_primitive,
        primitive_index_by_slot=primitive_index_clean,
    )

    required_primitives_clean = _clean_sorted_unique_strings(required_primitives_v0)
    commander_signal = _commander_dependency_signal(commander_dependency_metadata)

    if isinstance(bridge_amplification_bonus_weight, (int, float)) and not isinstance(bridge_amplification_bonus_weight, bool):
        bridge_amplification_bonus_v1 = _round6_half_up(_clamp01(float(bridge_amplification_bonus_weight)))
    else:
        bridge_amplification_bonus_v1 = 0.0

    slot_adjacency = _build_slot_adjacency(
        graph_v1,
        known_slot_ids=sorted(primitive_index_clean.keys()),
    )
    primitive_adjacency = _build_primitive_adjacency(
        primitive_index_clean,
        slot_adjacency=slot_adjacency,
    )

    primitive_counts = {
        primitive_id: len(slot_ids)
        for primitive_id, slot_ids in slot_ids_by_primitive_clean.items()
    }
    primitive_concentration_index = 0.0
    total_assignments = sum(max(0, count) for count in primitive_counts.values())
    if total_assignments > 0:
        primitive_concentration_index = _round6_half_up(
            sum(
                (float(count) / float(total_assignments)) * (float(count) / float(total_assignments))
                for count in primitive_counts.values()
                if count > 0
            )
        )

    high_frequency_cutoff = _high_frequency_cutoff(primitive_counts)

    bridge_clusters_by_set: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    evaluated_chain_candidates = 0
    cap_reached = False

    for start_primitive in sorted(primitive_adjacency.keys()):
        if cap_reached:
            break

        queue: List[List[str]] = [[start_primitive]]
        cursor = 0
        while cursor < len(queue):
            if cap_reached:
                break

            path = queue[cursor]
            cursor += 1
            hops = len(path) - 1
            if hops >= _MAX_CHAIN_HOPS:
                continue

            current = path[-1]
            for neighbor in primitive_adjacency.get(current, []):
                if neighbor in path:
                    continue

                next_path = path + [neighbor]
                next_hops = len(next_path) - 1

                if next_hops in {2, 3} and neighbor > start_primitive:
                    evaluated_chain_candidates += 1
                    if evaluated_chain_candidates > _MAX_EVALUATED_CHAINS:
                        cap_reached = True
                        break

                    intermediates = next_path[1:-1]
                    if len(intermediates) == 0 or len(intermediates) > 2:
                        continue

                    start_slots = set(slot_ids_by_primitive_clean.get(start_primitive, []))
                    end_slots = set(slot_ids_by_primitive_clean.get(neighbor, []))
                    if len(start_slots.intersection(end_slots)) > 1:
                        continue

                    if any(int(primitive_counts.get(primitive_id, 0)) >= high_frequency_cutoff for primitive_id in intermediates):
                        continue

                    primitive_set_key = tuple(sorted(set(next_path)))
                    slot_ids, bridge_score, novelty_score, redundancy_score, vulnerability_score = _chain_scores(
                        primitive_chain=next_path,
                        slot_ids_by_primitive=slot_ids_by_primitive_clean,
                        primitive_counts=primitive_counts,
                        primitive_concentration_index=primitive_concentration_index,
                        commander_dependency_signal=commander_signal,
                    )

                    candidate_payload = {
                        "primitive_chain": list(next_path),
                        "slot_ids": slot_ids,
                        "bridge_score": bridge_score,
                        "novelty_score": novelty_score,
                        "redundancy_score": redundancy_score,
                        "vulnerability_score": vulnerability_score,
                    }

                    existing = bridge_clusters_by_set.get(primitive_set_key)
                    if existing is None:
                        bridge_clusters_by_set[primitive_set_key] = candidate_payload
                    else:
                        existing_chain = tuple(existing.get("primitive_chain") or [])
                        candidate_chain = tuple(candidate_payload.get("primitive_chain") or [])
                        if candidate_chain < existing_chain:
                            bridge_clusters_by_set[primitive_set_key] = candidate_payload

                if next_hops < _MAX_CHAIN_HOPS:
                    queue.append(next_path)

    bridge_clusters_v1 = sorted(
        bridge_clusters_by_set.values(),
        key=lambda row: (
            -float(row.get("bridge_score") or 0.0),
            -float(row.get("novelty_score") or 0.0),
            tuple(row.get("primitive_chain") or []),
            tuple(row.get("slot_ids") or []),
        ),
    )

    latent_engine_clusters_v1 = _collect_latent_engine_clusters(
        primitive_adjacency=primitive_adjacency,
        slot_ids_by_primitive=slot_ids_by_primitive_clean,
        required_primitives_v0=required_primitives_clean,
        commander_dependency_signal=commander_signal,
    )

    cross_engine_overlap_score_v1 = _cross_engine_overlap_score(
        bridge_clusters_v1=bridge_clusters_v1,
        required_primitives_v0=required_primitives_clean,
    )

    structural_asymmetry_index_v1 = _structural_asymmetry_index(
        bridge_clusters_v1=bridge_clusters_v1,
        bridge_amplification_bonus_v1=bridge_amplification_bonus_v1,
    )

    status = "OK"
    codes: List[str] = []
    if len(bridge_clusters_v1) == 0 and len(latent_engine_clusters_v1) == 0:
        status = "WARN"
        codes = ["NO_BRIDGES_DETECTED"]

    primitive_edge_total = int(sum(len(neighbors) for neighbors in primitive_adjacency.values()) / 2)

    return {
        "version": PRIMITIVE_BRIDGE_EXPLORER_VERSION,
        "status": status,
        "reason_code": None,
        "codes": codes,
        "bridge_clusters_v1": bridge_clusters_v1,
        "latent_engine_clusters_v1": latent_engine_clusters_v1,
        "cross_engine_overlap_score_v1": _round6_half_up(cross_engine_overlap_score_v1),
        "structural_asymmetry_index_v1": structural_asymmetry_index_v1,
        "bridge_amplification_bonus_v1": bridge_amplification_bonus_v1,
        "bounds": {
            "max_chain_hops": _MAX_CHAIN_HOPS,
            "max_evaluated_chains": _MAX_EVALUATED_CHAINS,
            "evaluated_chain_candidates": min(evaluated_chain_candidates, _MAX_EVALUATED_CHAINS),
        },
        "stats": {
            "unique_primitives_total": len(primitive_adjacency),
            "primitive_edges_total": primitive_edge_total,
            "high_frequency_cutoff": high_frequency_cutoff,
            "commander_dependency_signal_v1": _round6_half_up(commander_signal),
        },
    }
