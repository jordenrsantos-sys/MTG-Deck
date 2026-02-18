from typing import Any, Dict, List, Tuple

_BUCKETS_V2 = ("ramp", "draw", "interaction", "protection", "wincon")

_ENGINE_GROUPS_V2: List[Tuple[str, List[str], int]] = [
    ("recursion", ["RECUR", "REANIMAT", "GRAVEYARD", "RETURN"], 2),
    ("sac_outlet", ["SAC"], 1),
    ("self_mill", ["SELF_MILL", "MILL"], 1),
    ("token_production", ["TOKEN"], 2),
    ("mana_accel", ["RAMP", "MANA_FIX", "TREASURE", "MANA"], 3),
    ("draw_engine", ["CARD_DRAW", "DRAW", "ADVANTAGE", "LOOT"], 3),
]


def _round_metric(value: float) -> float:
    return float(f"{value:.6f}")


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _to_str_set(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    out: set[str] = set()
    for item in value:
        if isinstance(item, str) and item.strip() != "":
            out.add(item)
    return out


def _required_structural_completion(structural_coverage: Dict[str, Any]) -> float:
    required = structural_coverage.get("required_primitives_v0")
    required = required if isinstance(required, list) else []
    required_total = len(required)
    if required_total <= 0:
        return 0.0
    required_met = sum(
        1
        for row in required
        if isinstance(row, dict) and row.get("meets_minimum") is True
    )
    return _clamp(float(required_met) / float(required_total))


def _targets_completion(category_counts: Dict[str, Any], targets: Dict[str, Any]) -> float:
    met = 0
    for bucket in _BUCKETS_V2:
        target_value = targets.get(f"{bucket}_target")
        have_value = category_counts.get(bucket)
        target_int = int(target_value) if isinstance(target_value, int) else 0
        have_int = int(have_value) if isinstance(have_value, int) else 0
        if target_int <= 0:
            continue
        if have_int >= target_int:
            met += 1
    return _clamp(float(met) / float(len(_BUCKETS_V2)))


def _cohesion_score(primitive_frequency: Dict[str, Any], primitive_concentration_index: float) -> float:
    freq_map: Dict[str, int] = {
        key: int(value)
        for key, value in primitive_frequency.items()
        if isinstance(key, str) and isinstance(value, int) and value > 0
    }
    if not freq_map:
        return 0.0

    counts_desc = sorted(freq_map.values(), reverse=True)
    total = int(sum(counts_desc))
    if total <= 0:
        return 0.0

    dominant_share = float(counts_desc[0]) / float(total)
    top3_share = float(sum(counts_desc[:3])) / float(total)

    base = _clamp((0.45 * dominant_share) + (0.55 * top3_share) + 0.1)
    overconcentration_penalty = _clamp((float(primitive_concentration_index) - 0.3) / 0.4)

    return _clamp(base - (0.35 * overconcentration_penalty))


def _overlap_ratio(deck_primitive_set: set[str], reference_set: set[str]) -> float:
    if not reference_set:
        return 0.0
    overlap = len([p for p in deck_primitive_set if p in reference_set])
    return _clamp(float(overlap) / float(len(reference_set)))


def _group_count(primitive_frequency: Dict[str, int], needles: List[str]) -> int:
    total = 0
    for primitive, count in primitive_frequency.items():
        primitive_upper = primitive.upper()
        if any(needle in primitive_upper for needle in needles):
            total += int(count)
    return total


def _engine_density_score(primitive_frequency: Dict[str, int]) -> tuple[float, Dict[str, float]]:
    if not primitive_frequency:
        return 0.0, {group_name: 0.0 for group_name, _, _ in _ENGINE_GROUPS_V2}

    group_scores: Dict[str, float] = {}
    for group_name, keywords, threshold in _ENGINE_GROUPS_V2:
        group_total = _group_count(primitive_frequency=primitive_frequency, needles=keywords)
        threshold_safe = max(1, int(threshold))
        group_scores[group_name] = _clamp(float(group_total) / float(threshold_safe))

    mean_score = sum(group_scores.values()) / float(len(_ENGINE_GROUPS_V2))
    return _clamp(mean_score), group_scores


def _winpath_presence_score(result: Dict[str, Any], engine_density_score: float) -> float:
    combo_candidates_total = result.get("combo_candidates_v0_total")
    if isinstance(combo_candidates_total, int):
        has_combo_signal = combo_candidates_total > 0
    else:
        has_combo_signal = isinstance(result.get("combo_candidates_v0"), list) and len(result.get("combo_candidates_v0")) > 0

    if engine_density_score >= 0.6:
        scaling_engine_signal = 1.0
    elif engine_density_score >= 0.4:
        scaling_engine_signal = 0.5
    else:
        scaling_engine_signal = 0.0

    motifs = result.get("motifs")
    motifs = motifs if isinstance(motifs, list) else []
    has_resource_denial_signal = False
    for motif in motifs:
        if not isinstance(motif, dict):
            continue
        motif_id = motif.get("motif_id")
        label = motif.get("label")
        motif_text = " ".join([
            motif_id if isinstance(motif_id, str) else "",
            label if isinstance(label, str) else "",
        ]).upper()
        if ("DENIAL" in motif_text) or ("STAX" in motif_text) or ("TAX" in motif_text):
            has_resource_denial_signal = True
            break

    score = (
        (0.6 * (1.0 if has_combo_signal else 0.0))
        + (0.3 * scaling_engine_signal)
        + (0.1 * (1.0 if has_resource_denial_signal else 0.0))
    )
    return _clamp(score)


def _vulnerability_penalty(result: Dict[str, Any]) -> tuple[float, float, float]:
    commander_dependency_signal = result.get("commander_dependency_signal")
    commander_dependency_signal = commander_dependency_signal if isinstance(commander_dependency_signal, dict) else {}
    overlap_ratio = float(commander_dependency_signal.get("overlap_ratio") or 0.0)

    dead_slot_ids = result.get("dead_slot_ids")
    dead_slot_ids = dead_slot_ids if isinstance(dead_slot_ids, list) else []
    dead_slot_ratio = _clamp(float(len(dead_slot_ids)) / 12.0)

    penalty = _clamp((0.7 * overlap_ratio) + (0.3 * dead_slot_ratio))
    return penalty, overlap_ratio, dead_slot_ratio


def score_deck_v2(state: dict, context: dict) -> dict:
    """
    Deterministic strategic score for builder selection/refinement.
    Must not mutate engine outputs.
    Returns components + total_score.
    """

    state_obj = state if isinstance(state, dict) else {}
    context_obj = context if isinstance(context, dict) else {}

    result = _as_dict(state_obj.get("result"))
    structural_coverage = _as_dict(result.get("structural_coverage"))

    category_counts = _as_dict(context_obj.get("category_counts"))
    targets = _as_dict(context_obj.get("targets"))
    primitive_frequency_raw = _as_dict(context_obj.get("primitive_frequency"))

    primitive_frequency: Dict[str, int] = {
        key: int(value)
        for key, value in primitive_frequency_raw.items()
        if isinstance(key, str) and isinstance(value, int) and value > 0
    }

    deck_primitive_set = set(primitive_frequency.keys())
    commander_primitives_set = _to_str_set(context_obj.get("commander_primitives"))
    anchor_primitives_set = _to_str_set(context_obj.get("anchor_primitives"))

    structural_required_score = _required_structural_completion(structural_coverage)
    targets_score = _targets_completion(category_counts=category_counts, targets=targets)
    structural_completion_score = _clamp((0.7 * structural_required_score) + (0.3 * targets_score))

    primitive_concentration_index = float(result.get("primitive_concentration_index") or 0.0)
    cohesion_score = _cohesion_score(
        primitive_frequency=primitive_frequency,
        primitive_concentration_index=primitive_concentration_index,
    )

    commander_alignment_score = _overlap_ratio(deck_primitive_set=deck_primitive_set, reference_set=commander_primitives_set)
    anchor_alignment_score = _overlap_ratio(deck_primitive_set=deck_primitive_set, reference_set=anchor_primitives_set)

    engine_density_score, engine_density_breakdown = _engine_density_score(primitive_frequency=primitive_frequency)
    winpath_presence_score = _winpath_presence_score(result=result, engine_density_score=engine_density_score)

    vulnerability_penalty, commander_overlap_ratio, dead_slot_ratio = _vulnerability_penalty(result=result)

    positive_score = (
        (0.30 * structural_completion_score)
        + (0.20 * cohesion_score)
        + (0.15 * commander_alignment_score)
        + (0.10 * anchor_alignment_score)
        + (0.15 * engine_density_score)
        + (0.10 * winpath_presence_score)
    )

    total_score_v2 = positive_score - (0.10 * vulnerability_penalty)

    return {
        "total_score_v2": _round_metric(total_score_v2),
        "components": {
            "structural_completion_score": _round_metric(structural_completion_score),
            "cohesion_score": _round_metric(cohesion_score),
            "commander_alignment_score": _round_metric(commander_alignment_score),
            "anchor_alignment_score": _round_metric(anchor_alignment_score),
            "engine_density_score": _round_metric(engine_density_score),
            "engine_density_breakdown": {
                group_name: _round_metric(group_score)
                for group_name, group_score in engine_density_breakdown.items()
            },
            "winpath_presence_score": _round_metric(winpath_presence_score),
            "vulnerability_penalty": _round_metric(vulnerability_penalty),
            "commander_overlap_ratio": _round_metric(commander_overlap_ratio),
            "dead_slot_ratio": _round_metric(dead_slot_ratio),
            "primitive_concentration_index": _round_metric(primitive_concentration_index),
            "structural_required_primitives_score": _round_metric(structural_required_score),
            "targets_completion_score": _round_metric(targets_score),
        },
    }
