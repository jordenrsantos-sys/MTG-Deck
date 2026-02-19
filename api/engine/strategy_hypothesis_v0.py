from typing import Any, Dict, List

from api.engine.constants import TagsNotCompiledError
from api.engine.constants import GENERIC_MINIMUMS
from api.engine.layers.primitive_index_v1 import run_primitive_index_v1
from api.engine.strategy_packages_v0 import build_completion_packages_v0_1
from api.engine.utils import make_slot_id, normalize_primitives_source, sorted_unique
from engine.db import find_card_by_name, list_snapshots


def _round_metric(value: float) -> float:
    return float(f"{value:.6f}")


def _resolve_snapshot_id() -> str:
    snapshots = list_snapshots(limit=1)
    if snapshots and isinstance(snapshots[0].get("snapshot_id"), str):
        return snapshots[0]["snapshot_id"]
    raise ValueError("No local snapshot available")


def _cluster_primitives(observed_primitives: List[str]) -> Dict[str, List[str]]:
    def has_any(text: str, needles: List[str]) -> bool:
        upper = text.upper()
        return any(needle in upper for needle in needles)

    clusters = {
        "resource_engines": sorted_unique(
            [
                p
                for p in observed_primitives
                if has_any(p, ["RAMP", "MANA_FIX", "CARD_DRAW", "TREASURE", "RESOURCE"])
            ]
        ),
        "recursion_loops": sorted_unique(
            [
                p
                for p in observed_primitives
                if has_any(p, ["RECUR", "GRAVEYARD", "REANIMAT", "RETURN"]) 
            ]
        ),
        "sac_outlets": sorted_unique([p for p in observed_primitives if has_any(p, ["SAC"])]),
        "replacement_chains": sorted_unique(
            [p for p in observed_primitives if has_any(p, ["REPLACEMENT"])]
        ),
        "mana_multipliers": sorted_unique(
            [
                p
                for p in observed_primitives
                if has_any(p, ["MANA"]) and has_any(p, ["MULTIPLIER", "DOUBLE", "REDUCTION"])
            ]
        ),
    }
    return clusters


def _required_missing(primitives: List[str], primitive_frequency: Dict[str, int]) -> List[str]:
    relevant = [p for p in primitives if p in GENERIC_MINIMUMS]
    relevant_sorted = sorted_unique(relevant)
    missing: List[str] = []
    for primitive in relevant_sorted:
        have = int(primitive_frequency.get(primitive, 0))
        needed = int(GENERIC_MINIMUMS.get(primitive, 0))
        if have < needed:
            missing.append(primitive)
    return missing


def _completion_candidates(
    core_primitives: List[str],
    required_missing: List[str],
    primitive_frequency: Dict[str, int],
) -> List[str]:
    if required_missing:
        return sorted_unique(required_missing)

    extras = [
        primitive
        for primitive in sorted(
            primitive_frequency.keys(),
            key=lambda p: (-int(primitive_frequency.get(p, 0)), p),
        )
        if primitive not in core_primitives
    ]
    return extras[:3]


def _build_hypothesis(
    core_primitives: List[str],
    primitive_frequency: Dict[str, int],
    commander_dependency_estimate: float,
    primitive_concentration_projection: float,
    explanation: str,
) -> Dict[str, Any]:
    core_sorted = sorted_unique(core_primitives)
    required = _required_missing(core_sorted, primitive_frequency)
    completion = _completion_candidates(core_sorted, required, primitive_frequency)
    return {
        "hypothesis_id": "",
        "core_primitives": core_sorted,
        "required_primitives_missing": required,
        "candidate_completion_primitives": completion,
        "risk_projection": {
            "commander_dependency_estimate": _round_metric(commander_dependency_estimate),
            "primitive_concentration_projection": _round_metric(primitive_concentration_projection),
        },
        "explanation": explanation,
    }


def generate_strategy_hypotheses_v0(
    anchor_cards: list[str],
    commander: str | None,
    profile_id: str,
    bracket_id: str,
    max_packages_per_hypothesis: int = 5,
    max_cards_per_package: int = 4,
    validate_packages: bool = False,
) -> dict:
    snapshot_id = _resolve_snapshot_id()

    anchor_cards_clean = [
        card_name.strip()
        for card_name in (anchor_cards or [])
        if isinstance(card_name, str) and card_name.strip()
    ]
    anchor_cards_unique_sorted = sorted_unique(anchor_cards_clean)

    resolved_anchor_cards: List[Dict[str, Any]] = []
    unknown_anchor_cards: List[str] = []
    for card_name in anchor_cards_unique_sorted:
        resolved = find_card_by_name(snapshot_id, card_name)
        if resolved is None:
            unknown_anchor_cards.append(card_name)
            continue
        resolved_anchor_cards.append(resolved)

    commander_resolved = None
    if isinstance(commander, str) and commander.strip():
        commander_resolved = find_card_by_name(snapshot_id, commander.strip())

    deck_cards_canonical_input_order: List[Dict[str, Any]] = []
    canonical_slots_all: List[Dict[str, Any]] = []
    slot_primitives_source_by_slot_id: Dict[str, Any] = {}

    if commander_resolved is not None:
        canonical_slots_all.append(
            {
                "slot_id": "C0",
                "resolved_oracle_id": commander_resolved.get("oracle_id"),
                "status": "PLAYABLE",
            }
        )

    for idx, resolved in enumerate(resolved_anchor_cards):
        slot_id = make_slot_id("S", idx)
        oracle_id = resolved.get("oracle_id")
        deck_cards_canonical_input_order.append(
            {
                "slot_id": slot_id,
                "resolved_oracle_id": oracle_id,
                "status": "PLAYABLE",
            }
        )
        canonical_slots_all.append(
            {
                "slot_id": slot_id,
                "resolved_oracle_id": oracle_id,
                "status": "PLAYABLE",
            }
        )
        slot_primitives_source_by_slot_id[slot_id] = (
            resolved.get("primitives")
            if resolved.get("primitives") is not None
            else resolved.get("primitives_json")
        )

    primitive_state = {
        "commander_resolved": commander_resolved,
        "primitive_overrides_by_oracle": {},
        "get_overridden_primitives_for_oracle": (lambda _oracle_id, source: source),
        "deck_cards_canonical_input_order": deck_cards_canonical_input_order,
        "slot_primitives_source_by_slot_id": slot_primitives_source_by_slot_id,
        "canonical_slots_all": canonical_slots_all,
        "normalize_primitives_source": normalize_primitives_source,
    }
    primitive_state = run_primitive_index_v1(primitive_state)
    primitive_index_by_slot = primitive_state.get("primitive_index_by_slot") or {}
    slot_ids_by_primitive = primitive_state.get("slot_ids_by_primitive") or {}

    slot_name_by_id: Dict[str, str] = {}
    if commander_resolved is not None and isinstance(commander_resolved.get("name"), str):
        slot_name_by_id["C0"] = commander_resolved.get("name")
    for idx, resolved in enumerate(resolved_anchor_cards):
        slot_id = make_slot_id("S", idx)
        resolved_name = resolved.get("name")
        if isinstance(resolved_name, str):
            slot_name_by_id[slot_id] = resolved_name

    resolved_anchor_names_for_validation = sorted_unique(
        [
            name
            for name in [resolved.get("name") for resolved in resolved_anchor_cards]
            if isinstance(name, str)
        ]
    )

    anchor_slot_ids = [entry.get("slot_id") for entry in deck_cards_canonical_input_order if isinstance(entry.get("slot_id"), str)]
    anchor_slot_ids = [sid for sid in anchor_slot_ids if isinstance(sid, str)]

    primitive_frequency: Dict[str, int] = {}
    for slot_id in anchor_slot_ids:
        for primitive in primitive_index_by_slot.get(slot_id, []):
            if isinstance(primitive, str):
                primitive_frequency[primitive] = primitive_frequency.get(primitive, 0) + 1

    primitive_overlap_map: Dict[str, List[str]] = {}
    for i in range(len(anchor_slot_ids)):
        slot_a = anchor_slot_ids[i]
        primitives_a = primitive_index_by_slot.get(slot_a, [])
        primitives_a_set = set([p for p in primitives_a if isinstance(p, str)])
        for j in range(i + 1, len(anchor_slot_ids)):
            slot_b = anchor_slot_ids[j]
            primitives_b = primitive_index_by_slot.get(slot_b, [])
            shared = sorted_unique([p for p in primitives_b if p in primitives_a_set and isinstance(p, str)])
            if shared:
                primitive_overlap_map[f"{slot_a}|{slot_b}"] = shared

    observed_primitives = sorted(primitive_frequency.keys())
    clusters = _cluster_primitives(observed_primitives)

    cluster_scores = {
        cluster_name: sum(int(primitive_frequency.get(p, 0)) for p in cluster_primitives)
        for cluster_name, cluster_primitives in clusters.items()
    }
    cluster_names_by_score = sorted(
        cluster_scores.keys(),
        key=lambda name: (-int(cluster_scores.get(name, 0)), name),
    )

    commander_primitives = primitive_index_by_slot.get("C0", []) if commander_resolved is not None else []
    commander_primitive_set = set([p for p in commander_primitives if isinstance(p, str)])
    anchor_primitive_set = set(observed_primitives)
    all_origin_primitives = sorted_unique(list(commander_primitive_set.union(anchor_primitive_set)))

    if all_origin_primitives:
        commander_dependency_estimate = len(commander_primitive_set) / len(all_origin_primitives)
    else:
        commander_dependency_estimate = 0.0

    total_primitive_count = sum(int(v) for v in primitive_frequency.values())
    dominant_primitive_count = max([int(v) for v in primitive_frequency.values()], default=0)
    if total_primitive_count > 0:
        primitive_concentration_projection = dominant_primitive_count / total_primitive_count
    else:
        primitive_concentration_projection = 0.0

    hypotheses_raw: List[Dict[str, Any]] = []

    dominant_cluster_name = None
    dominant_cluster_primitives: List[str] = []
    for cluster_name in cluster_names_by_score:
        if int(cluster_scores.get(cluster_name, 0)) > 0:
            dominant_cluster_name = cluster_name
            dominant_cluster_primitives = list(clusters.get(cluster_name) or [])
            break

    if dominant_cluster_name is not None and dominant_cluster_primitives:
        hypotheses_raw.append(
            _build_hypothesis(
                core_primitives=dominant_cluster_primitives,
                primitive_frequency=primitive_frequency,
                commander_dependency_estimate=commander_dependency_estimate,
                primitive_concentration_projection=primitive_concentration_projection,
                explanation=(
                    "Maximize dominant primitive cluster "
                    f"{dominant_cluster_name} from anchor alignment."
                ),
            )
        )

    missing_global = sorted(
        [
            primitive
            for primitive in GENERIC_MINIMUMS.keys()
            if int(primitive_frequency.get(primitive, 0)) < int(GENERIC_MINIMUMS.get(primitive, 0))
        ]
    )
    top_anchor_primitives = [
        primitive
        for primitive in sorted(
            primitive_frequency.keys(),
            key=lambda p: (-int(primitive_frequency.get(p, 0)), p),
        )
    ]
    loop_core = sorted_unique(top_anchor_primitives[:3])
    if loop_core or missing_global:
        hypotheses_raw.append(
            _build_hypothesis(
                core_primitives=loop_core,
                primitive_frequency=primitive_frequency,
                commander_dependency_estimate=commander_dependency_estimate,
                primitive_concentration_projection=primitive_concentration_projection,
                explanation=(
                    "Close resource loop by covering missing primitive minimums "
                    "from current anchor profile."
                ),
            )
        )

    nonempty_clusters = [
        name
        for name in cluster_names_by_score
        if int(cluster_scores.get(name, 0)) > 0 and clusters.get(name)
    ]
    if len(nonempty_clusters) >= 2:
        hybrid_core = sorted_unique(
            list(clusters.get(nonempty_clusters[0]) or [])
            + list(clusters.get(nonempty_clusters[1]) or [])
        )
        hypotheses_raw.append(
            _build_hypothesis(
                core_primitives=hybrid_core,
                primitive_frequency=primitive_frequency,
                commander_dependency_estimate=commander_dependency_estimate,
                primitive_concentration_projection=primitive_concentration_projection,
                explanation=(
                    "Hybridize top two primitive clusters "
                    f"{nonempty_clusters[0]} and {nonempty_clusters[1]} for balanced coverage."
                ),
            )
        )

    if not hypotheses_raw:
        hypotheses_raw.append(
            _build_hypothesis(
                core_primitives=[],
                primitive_frequency=primitive_frequency,
                commander_dependency_estimate=commander_dependency_estimate,
                primitive_concentration_projection=primitive_concentration_projection,
                explanation="No resolved anchor primitives available for strategy synthesis.",
            )
        )

    unique_hypotheses: Dict[tuple, Dict[str, Any]] = {}
    for hypothesis in hypotheses_raw:
        key = (
            tuple(hypothesis.get("core_primitives") or []),
            tuple(hypothesis.get("required_primitives_missing") or []),
            tuple(hypothesis.get("candidate_completion_primitives") or []),
            str(hypothesis.get("explanation") or ""),
        )
        if key not in unique_hypotheses:
            unique_hypotheses[key] = hypothesis

    hypotheses = list(unique_hypotheses.values())
    hypotheses.sort(
        key=lambda h: (
            -sum(int(primitive_frequency.get(p, 0)) for p in (h.get("core_primitives") or [])),
            tuple(h.get("core_primitives") or []),
            str(h.get("explanation") or ""),
        )
    )

    bounded = hypotheses[:3]
    for idx, hypothesis in enumerate(bounded):
        hypothesis["hypothesis_id"] = f"H{idx}"
        try:
            hypothesis["completion_packages_v0_1"] = build_completion_packages_v0_1(
                snapshot_id=snapshot_id,
                hypothesis=hypothesis,
                primitive_frequency=primitive_frequency,
                slot_ids_by_primitive=slot_ids_by_primitive,
                primitive_index_by_slot=primitive_index_by_slot,
                slot_name_by_id=slot_name_by_id,
                max_packages_per_hypothesis=max_packages_per_hypothesis,
                max_cards_per_package=max_cards_per_package,
                validate_packages=validate_packages,
                commander=commander,
                anchor_cards_for_validation=resolved_anchor_names_for_validation,
                profile_id=profile_id,
                bracket_id=bracket_id,
            )
        except TagsNotCompiledError as exc:
            unknown = exc.to_unknown()
            return {
                "status": "TAGS_NOT_COMPILED",
                "db_snapshot_id": snapshot_id,
                "profile_id": profile_id,
                "bracket_id": bracket_id,
                "commander": commander,
                "anchor_cards": anchor_cards_unique_sorted,
                "unknown_anchor_cards": sorted_unique(unknown_anchor_cards),
                "primitive_frequency": {
                    key: primitive_frequency[key] for key in sorted(primitive_frequency.keys())
                },
                "primitive_overlap_map": {
                    key: primitive_overlap_map[key] for key in sorted(primitive_overlap_map.keys())
                },
                "strategy_hypotheses_v0": bounded,
                "unknowns": [unknown],
            }

    return {
        "db_snapshot_id": snapshot_id,
        "profile_id": profile_id,
        "bracket_id": bracket_id,
        "commander": commander,
        "anchor_cards": anchor_cards_unique_sorted,
        "unknown_anchor_cards": sorted_unique(unknown_anchor_cards),
        "primitive_frequency": {
            key: primitive_frequency[key] for key in sorted(primitive_frequency.keys())
        },
        "primitive_overlap_map": {
            key: primitive_overlap_map[key] for key in sorted(primitive_overlap_map.keys())
        },
        "strategy_hypotheses_v0": bounded,
    }
