from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

from api.engine.candidate_ranking_v1 import rank_candidates_v1
from api.engine.candidate_selection_v0 import (
    get_candidate_pool_v0,
    is_singleton_exempt_card,
    normalize_color_identity,
)
from api.engine.constants import BASIC_NAMES, GENERIC_MINIMUMS
from api.engine.pipeline_build import run_build_pipeline
from api.engine.scoring_v0 import score_deck_v0
from api.engine.scoring_v2 import score_deck_v2
from api.engine.utils import normalize_primitives_source, sorted_unique
from engine.db import find_card_by_name, is_legal_commander_card, is_legal_in_format, list_snapshots
from engine.game_changers import bracket_floor_from_count, detect_game_changers


DEFAULT_COMPLETION_TARGETS_V0 = {
    "land_count_target": 36,
    "ramp_target": max(int(GENERIC_MINIMUMS.get("RAMP_MANA", 8)), 8),
    "draw_target": max(int(GENERIC_MINIMUMS.get("CARD_DRAW", 8)), 8),
    "interaction_target": int(GENERIC_MINIMUMS.get("REMOVAL_SINGLE", 8)) + int(GENERIC_MINIMUMS.get("BOARD_WIPE", 2)),
    "protection_target": max(int(GENERIC_MINIMUMS.get("PROTECTION", 3)), 3),
    "wincon_target": 6,
}

PROFILE_COMPLETION_TARGETS_V0 = {
    "default": dict(DEFAULT_COMPLETION_TARGETS_V0),
}

REFINEMENT_REPLACEMENT_TOP_K_V0_1 = 10

_COLOR_TO_BASIC = {
    "W": "Plains",
    "U": "Island",
    "B": "Swamp",
    "R": "Mountain",
    "G": "Forest",
}


def _round_metric(value: float) -> float:
    return float(f"{value:.6f}")


def _resolve_snapshot_id(explicit_snapshot_id: str | None = None) -> str:
    if isinstance(explicit_snapshot_id, str) and explicit_snapshot_id.strip():
        return explicit_snapshot_id.strip()
    snapshots = list_snapshots(limit=1)
    if snapshots and isinstance(snapshots[0].get("snapshot_id"), str):
        return snapshots[0]["snapshot_id"]
    raise ValueError("No local snapshot available")


def _stable_unique_preserve_order(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        clean = value.strip()
        if clean == "":
            continue
        if clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _card_meta(card: Dict[str, Any]) -> Dict[str, Any]:
    primitives_source = card.get("primitives") if card.get("primitives") is not None else card.get("primitives_json")
    return {
        "name": card.get("name") if isinstance(card.get("name"), str) else None,
        "oracle_id": card.get("oracle_id") if isinstance(card.get("oracle_id"), str) else None,
        "type_line": card.get("type_line") if isinstance(card.get("type_line"), str) else "",
        "mana_cost": card.get("mana_cost") if isinstance(card.get("mana_cost"), str) else "",
        "primitives": normalize_primitives_source(primitives_source),
        "color_identity": normalize_color_identity(card.get("color_identity")),
        "legalities": card.get("legalities") if isinstance(card.get("legalities"), dict) else {},
    }


def _is_land(meta: Dict[str, Any]) -> bool:
    type_line = meta.get("type_line") if isinstance(meta.get("type_line"), str) else ""
    return "land" in type_line.lower()


def _primitive_matches_need(primitive: str, need_name: str) -> bool:
    upper = primitive.upper()
    if need_name == "ramp":
        return ("RAMP" in upper) or ("MANA_FIX" in upper) or ("TREASURE" in upper)
    if need_name == "draw":
        return ("DRAW" in upper) or ("ADVANTAGE" in upper) or ("LOOT" in upper)
    if need_name == "interaction":
        return ("REMOVAL" in upper) or ("COUNTER" in upper) or ("BOARD_WIPE" in upper) or ("DISRUPT" in upper)
    if need_name == "protection":
        return ("PROTECTION" in upper) or ("HEXPROOF" in upper) or ("WARD" in upper) or ("INDESTRUCT" in upper)
    if need_name == "wincon":
        return (
            ("WIN" in upper)
            or ("FINISH" in upper)
            or ("TOKEN_PRODUCTION" in upper)
            or ("COMBAT" in upper)
            or ("DAMAGE" in upper)
            or ("STORM" in upper)
        )
    return False


def _need_counts(deck_cards: List[str], catalog: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    primitive_frequency: Dict[str, int] = {}
    category_counts = {
        "land": 0,
        "ramp": 0,
        "draw": 0,
        "interaction": 0,
        "protection": 0,
        "wincon": 0,
    }

    for card_name in deck_cards:
        meta = catalog.get(card_name)
        if not isinstance(meta, dict):
            continue

        if _is_land(meta):
            category_counts["land"] += 1

        card_primitives = [p for p in (meta.get("primitives") or []) if isinstance(p, str)]
        for primitive in card_primitives:
            primitive_frequency[primitive] = primitive_frequency.get(primitive, 0) + 1

        for need_name in ("ramp", "draw", "interaction", "protection", "wincon"):
            if any(_primitive_matches_need(primitive, need_name) for primitive in card_primitives):
                category_counts[need_name] += 1

    return {
        "primitive_frequency": primitive_frequency,
        "category_counts": category_counts,
    }


def _missing_generic_primitives(primitive_frequency: Dict[str, int]) -> List[str]:
    missing = []
    for primitive in sorted(GENERIC_MINIMUMS.keys()):
        have = int(primitive_frequency.get(primitive, 0))
        need = int(GENERIC_MINIMUMS.get(primitive, 0))
        if have < need:
            missing.append(primitive)
    return missing


def _missing_targets_by_bucket(category_counts: Dict[str, int], targets: Dict[str, int]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for bucket in ("ramp", "draw", "interaction", "protection", "wincon"):
        target_value = int(targets.get(f"{bucket}_target", 0))
        have_value = int(category_counts.get(bucket, 0))
        out[bucket] = max(target_value - have_value, 0)
    return out


def _candidate_meta_from_pool_row(candidate: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": candidate.get("name") if isinstance(candidate.get("name"), str) else None,
        "oracle_id": candidate.get("oracle_id"),
        "type_line": candidate.get("type_line") if isinstance(candidate.get("type_line"), str) else "",
        "mana_cost": candidate.get("mana_cost") if isinstance(candidate.get("mana_cost"), str) else "",
        "primitives": [p for p in (candidate.get("primitives") or []) if isinstance(p, str)],
        "color_identity": [c for c in (candidate.get("color_identity") or []) if isinstance(c, str)],
        "legalities": candidate.get("legalities") if isinstance(candidate.get("legalities"), dict) else {},
    }


def _gc_remaining_for_ranking(deck_cards: List[str], commander_name: str, bracket_id: str, gc_set: set[str]) -> int | None:
    if bracket_id != "B3":
        return None
    current_gc_count = _gc_count_with_set(deck_cards=deck_cards, commander_name=commander_name, gc_set=gc_set)
    return max(0, 3 - int(current_gc_count))


def _build_score_context_v2(
    primitive_frequency: Dict[str, int],
    category_counts: Dict[str, int],
    targets: Dict[str, int],
    commander_primitives_set: set[str],
    anchor_primitives_set: set[str],
) -> Dict[str, Any]:
    return {
        "primitive_frequency": {
            key: int(primitive_frequency.get(key, 0))
            for key in sorted(primitive_frequency.keys())
            if isinstance(key, str)
        },
        "category_counts": {
            key: int(category_counts.get(key, 0))
            for key in ("land", "ramp", "draw", "interaction", "protection", "wincon")
        },
        "targets": {
            key: int(targets.get(key, 0))
            for key in (
                "land_count_target",
                "ramp_target",
                "draw_target",
                "interaction_target",
                "protection_target",
                "wincon_target",
            )
        },
        "commander_primitives": sorted(commander_primitives_set),
        "anchor_primitives": sorted(anchor_primitives_set),
    }


def _build_estimated_score_state_v2(
    primitive_frequency: Dict[str, int],
    commander_overlap_ratio: float,
) -> Dict[str, Any]:
    return {
        "result": {
            "structural_coverage": {"required_primitives_v0": []},
            "primitive_concentration_index": _estimate_primitive_concentration(primitive_frequency),
            "commander_dependency_signal": {
                "overlap_ratio": _round_metric(commander_overlap_ratio),
            },
            "dead_slot_ids": [],
            "combo_candidates_v0_total": 0,
            "motifs": [],
        }
    }


def _rank_candidates_for_builder_v1(
    candidate_pool: List[Dict[str, Any]],
    deck_cards: List[str],
    commander_ci: List[str],
    bracket_id: str,
    commander_name: str,
    gc_set: set[str],
    missing_primitives: List[str],
    primitive_frequency: Dict[str, int],
    category_counts: Dict[str, int],
    targets: Dict[str, int],
    commander_primitives_set: set[str],
    anchor_primitives_set: set[str],
    core_primitives_set: set[str],
    exclude_names: set[str] | None = None,
) -> List[Dict[str, Any]]:
    excluded = exclude_names if isinstance(exclude_names, set) else set()
    candidate_rows: List[Dict[str, Any]] = []
    for candidate in candidate_pool:
        if not isinstance(candidate, dict):
            continue

        candidate_meta = _candidate_meta_from_pool_row(candidate)
        name = candidate_meta.get("name")
        if not isinstance(name, str):
            continue
        if name in excluded:
            continue

        if not _can_add_card(
            card_name=name,
            card_meta=candidate_meta,
            deck_cards=deck_cards,
            commander_ci=commander_ci,
            bracket_id=bracket_id,
            commander_name=commander_name,
            gc_set=gc_set,
        ):
            continue

        candidate_rows.append(
            {
                "name": name,
                "slot_id": None,
                "primitives": list(candidate_meta.get("primitives") or []),
                "is_game_changer": name in gc_set,
                "meta": candidate_meta,
            }
        )

    missing_targets = {
        "missing_primitives": list(missing_primitives),
        "missing_by_bucket": _missing_targets_by_bucket(category_counts=category_counts, targets=targets),
    }
    ranked = rank_candidates_v1(
        candidates=candidate_rows,
        deck_state={
            "commander_primitives": sorted(commander_primitives_set),
            "anchor_primitives": sorted(anchor_primitives_set),
            "deck_primitives": sorted(primitive_frequency.keys()),
        },
        hypothesis={
            "core_primitives": sorted(core_primitives_set),
        },
        missing_targets=missing_targets,
        gc_remaining=_gc_remaining_for_ranking(
            deck_cards=deck_cards,
            commander_name=commander_name,
            bracket_id=bracket_id,
            gc_set=gc_set,
        ),
    )
    return ranked


def _targets_for_profile(profile_id: str, desired_noncommander: int) -> Dict[str, int]:
    selected = PROFILE_COMPLETION_TARGETS_V0.get(profile_id)
    if not isinstance(selected, dict):
        selected = PROFILE_COMPLETION_TARGETS_V0["default"]

    out = {
        "land_count_target": int(selected.get("land_count_target", DEFAULT_COMPLETION_TARGETS_V0["land_count_target"])),
        "ramp_target": int(selected.get("ramp_target", DEFAULT_COMPLETION_TARGETS_V0["ramp_target"])),
        "draw_target": int(selected.get("draw_target", DEFAULT_COMPLETION_TARGETS_V0["draw_target"])),
        "interaction_target": int(selected.get("interaction_target", DEFAULT_COMPLETION_TARGETS_V0["interaction_target"])),
        "protection_target": int(selected.get("protection_target", DEFAULT_COMPLETION_TARGETS_V0["protection_target"])),
        "wincon_target": int(selected.get("wincon_target", DEFAULT_COMPLETION_TARGETS_V0["wincon_target"])),
    }
    out["land_count_target"] = max(0, min(out["land_count_target"], desired_noncommander))
    return out


def _next_need_priority(category_counts: Dict[str, int], targets: Dict[str, int]) -> str:
    if int(category_counts.get("land", 0)) < int(targets.get("land_count_target", 0)):
        return "land"
    if int(category_counts.get("ramp", 0)) < int(targets.get("ramp_target", 0)):
        return "ramp"
    if int(category_counts.get("draw", 0)) < int(targets.get("draw_target", 0)):
        return "draw"
    if int(category_counts.get("interaction", 0)) < int(targets.get("interaction_target", 0)):
        return "interaction"
    if int(category_counts.get("protection", 0)) < int(targets.get("protection_target", 0)):
        return "protection"
    if int(category_counts.get("wincon", 0)) < int(targets.get("wincon_target", 0)):
        return "wincon"
    return "redundancy"


def _build_basic_cycle(commander_ci: List[str], deck_cards: List[str], catalog: Dict[str, Dict[str, Any]]) -> List[str]:
    colors = [c for c in commander_ci if c in _COLOR_TO_BASIC]
    if not colors:
        return ["Mountain"]

    pip_counts = {color: 0 for color in colors}
    for card_name in deck_cards:
        meta = catalog.get(card_name)
        if not isinstance(meta, dict):
            continue
        mana_cost = meta.get("mana_cost") if isinstance(meta.get("mana_cost"), str) else ""
        for color in colors:
            pip_counts[color] += mana_cost.upper().count("{" + color + "}")

    ordered_colors = sorted(colors, key=lambda c: (-pip_counts.get(c, 0), c))
    basics = [_COLOR_TO_BASIC[color] for color in ordered_colors if _COLOR_TO_BASIC[color] in BASIC_NAMES]
    if not basics:
        basics = ["Mountain"]
    return basics


def _current_gc_count(deck_cards: List[str], commander_name: str) -> int:
    found, count = detect_game_changers(
        playable_names=[name for name in deck_cards if isinstance(name, str)],
        commander_name=commander_name,
        gc_set=set(),
    )
    _ = found
    return int(count)


def _gc_count_with_set(deck_cards: List[str], commander_name: str, gc_set: set[str]) -> int:
    found, count = detect_game_changers(
        playable_names=[name for name in deck_cards if isinstance(name, str)],
        commander_name=commander_name,
        gc_set=gc_set,
    )
    _ = found
    return int(count)


def _can_add_card(
    card_name: str,
    card_meta: Dict[str, Any],
    deck_cards: List[str],
    commander_ci: List[str],
    bracket_id: str,
    commander_name: str,
    gc_set: set[str],
) -> bool:
    if not isinstance(card_name, str):
        return False
    if card_name == commander_name:
        return False

    if not isinstance(card_meta, dict):
        return False

    if not set(card_meta.get("color_identity") or []).issubset(set(commander_ci)):
        return False

    exempt = is_singleton_exempt_card(card_name, card_meta.get("type_line"))
    if (not exempt) and (card_name in set(deck_cards)):
        return False

    if bracket_id == "B3" and card_name in gc_set:
        projected = _gc_count_with_set(deck_cards + [card_name], commander_name, gc_set)
        if bracket_floor_from_count(projected) == "B4":
            return False

    return True


def _build_req(snapshot_id: str, commander: str, cards: List[str], profile_id: str, bracket_id: str) -> Any:
    return SimpleNamespace(
        db_snapshot_id=snapshot_id,
        profile_id=profile_id,
        bracket_id=bracket_id,
        format="commander",
        commander=commander,
        cards=list(cards),
        engine_patches_v0=[],
    )


def _run_build(snapshot_id: str, commander: str, cards: List[str], profile_id: str, bracket_id: str) -> Dict[str, Any]:
    req = _build_req(
        snapshot_id=snapshot_id,
        commander=commander,
        cards=cards,
        profile_id=profile_id,
        bracket_id=bracket_id,
    )
    return run_build_pipeline(req=req, conn=None, repo_root_path=None)


def _internal_score_v0(category_counts: Dict[str, int], targets: Dict[str, int], dead_slots_count: int = 0) -> Dict[str, Any]:
    ratios = {}
    for key, target_key in (
        ("land", "land_count_target"),
        ("ramp", "ramp_target"),
        ("draw", "draw_target"),
        ("interaction", "interaction_target"),
        ("protection", "protection_target"),
        ("wincon", "wincon_target"),
    ):
        target = max(1, int(targets.get(target_key, 1)))
        have = int(category_counts.get(key, 0))
        ratios[key] = min(float(have) / float(target), 1.0)

    mean_ratio = sum(ratios.values()) / float(len(ratios))
    score_total = mean_ratio - (0.01 * float(dead_slots_count))
    return {
        "score_total": _round_metric(score_total),
        "components": {
            "coverage_mean_ratio": _round_metric(mean_ratio),
            "dead_slot_penalty": _round_metric(0.01 * float(dead_slots_count)),
        },
    }


def _estimate_primitive_concentration(primitive_frequency: Dict[str, int]) -> float:
    total_primitive_occurrences = int(sum(int(v) for v in primitive_frequency.values()))
    if total_primitive_occurrences <= 0:
        return 0.0
    concentration = sum(
        (float(int(v)) / float(total_primitive_occurrences)) * (float(int(v)) / float(total_primitive_occurrences))
        for v in primitive_frequency.values()
    )
    return _round_metric(concentration)


def _estimate_commander_overlap_ratio(
    deck_cards: List[str],
    card_catalog: Dict[str, Dict[str, Any]],
    commander_primitives_set: set[str],
) -> float:
    if not deck_cards:
        return 0.0

    overlap_count = 0
    for card_name in deck_cards:
        meta = card_catalog.get(card_name)
        if not isinstance(meta, dict):
            continue
        primitives = [p for p in (meta.get("primitives") or []) if isinstance(p, str)]
        if commander_primitives_set.intersection(primitives):
            overlap_count += 1

    ratio = float(overlap_count) / float(max(len(deck_cards), 1))
    return _round_metric(ratio)


def _extract_dead_card_names(build_output: Dict[str, Any]) -> List[str]:
    result = build_output.get("result") if isinstance(build_output, dict) else {}
    result = result if isinstance(result, dict) else {}
    dead_slot_ids = set([sid for sid in (result.get("dead_slot_ids") or []) if isinstance(sid, str)])
    canonical = result.get("deck_cards_canonical_input_order") if isinstance(result.get("deck_cards_canonical_input_order"), list) else []

    dead_names: List[str] = []
    for entry in canonical:
        if not isinstance(entry, dict):
            continue
        slot_id = entry.get("slot_id")
        if not isinstance(slot_id, str) or slot_id not in dead_slot_ids:
            continue
        if entry.get("status") != "PLAYABLE":
            continue
        name = entry.get("resolved_name") or entry.get("input")
        if isinstance(name, str):
            dead_names.append(name)

    return sorted_unique(dead_names)


def _evaluate_deck_state_v0_1(
    deck_cards: List[str],
    card_catalog: Dict[str, Dict[str, Any]],
    targets: Dict[str, int],
    snapshot_id: str,
    commander_name: str,
    profile_id: str,
    bracket_id: str,
    validate_each_refine_iter: bool,
    commander_primitives_set: set[str],
    anchor_primitives_set: set[str],
) -> Dict[str, Any]:
    if validate_each_refine_iter:
        build_output = _run_build(
            snapshot_id=snapshot_id,
            commander=commander_name,
            cards=deck_cards,
            profile_id=profile_id,
            bracket_id=bracket_id,
        )
        result = build_output.get("result") if isinstance(build_output, dict) else {}
        result = result if isinstance(result, dict) else {}
        dead_slot_ids = result.get("dead_slot_ids") if isinstance(result.get("dead_slot_ids"), list) else []
        dead_slot_ids_count = len(dead_slot_ids)
        concentration = float(result.get("primitive_concentration_index") or 0.0)
        commander_dependency_signal = (
            result.get("commander_dependency_signal")
            if isinstance(result.get("commander_dependency_signal"), dict)
            else {}
        )
        commander_overlap_ratio = float(commander_dependency_signal.get("overlap_ratio") or 0.0)
        score_obj = score_deck_v0(build_output)
        need_data = _need_counts(deck_cards=deck_cards, catalog=card_catalog)
        score_obj_v2 = score_deck_v2(
            state=build_output,
            context=_build_score_context_v2(
                primitive_frequency=need_data["primitive_frequency"],
                category_counts=need_data["category_counts"],
                targets=targets,
                commander_primitives_set=commander_primitives_set,
                anchor_primitives_set=anchor_primitives_set,
            ),
        )
        score_components_v2 = (
            score_obj_v2.get("components")
            if isinstance(score_obj_v2.get("components"), dict)
            else {}
        )

        return {
            "score_total": float(score_obj.get("score_total") or 0.0),
            "score_v0": score_obj,
            "score_v2": score_obj_v2,
            "total_score_v2": float(score_obj_v2.get("total_score_v2") or 0.0),
            "dead_slot_ids_count": dead_slot_ids_count,
            "dead_card_names": _extract_dead_card_names(build_output),
            "primitive_concentration_index": concentration,
            "commander_overlap_ratio": commander_overlap_ratio,
            "engine_density_score": float(score_components_v2.get("engine_density_score") or 0.0),
            "build_output": build_output,
            "build_hash_v1": build_output.get("build_hash_v1") if isinstance(build_output, dict) else None,
        }

    need_data = _need_counts(deck_cards=deck_cards, catalog=card_catalog)
    primitive_frequency = need_data["primitive_frequency"]
    category_counts = need_data["category_counts"]
    score_obj = _internal_score_v0(
        category_counts=category_counts,
        targets=targets,
        dead_slots_count=0,
    )
    commander_overlap_ratio = _estimate_commander_overlap_ratio(
        deck_cards=deck_cards,
        card_catalog=card_catalog,
        commander_primitives_set=commander_primitives_set,
    )
    score_obj_v2 = score_deck_v2(
        state=_build_estimated_score_state_v2(
            primitive_frequency=primitive_frequency,
            commander_overlap_ratio=commander_overlap_ratio,
        ),
        context=_build_score_context_v2(
            primitive_frequency=primitive_frequency,
            category_counts=category_counts,
            targets=targets,
            commander_primitives_set=commander_primitives_set,
            anchor_primitives_set=anchor_primitives_set,
        ),
    )
    score_components_v2 = (
        score_obj_v2.get("components")
        if isinstance(score_obj_v2.get("components"), dict)
        else {}
    )

    dead_card_names = sorted([
        card_name
        for card_name in deck_cards
        if isinstance(card_catalog.get(card_name), dict)
        and not _is_land(card_catalog.get(card_name) or {})
        and len((card_catalog.get(card_name) or {}).get("primitives") or []) == 0
    ])

    return {
        "score_total": float(score_obj.get("score_total") or 0.0),
        "score_v0": score_obj,
        "score_v2": score_obj_v2,
        "total_score_v2": float(score_obj_v2.get("total_score_v2") or 0.0),
        "dead_slot_ids_count": len(dead_card_names),
        "dead_card_names": dead_card_names,
        "primitive_concentration_index": _estimate_primitive_concentration(primitive_frequency),
        "commander_overlap_ratio": commander_overlap_ratio,
        "engine_density_score": float(score_components_v2.get("engine_density_score") or 0.0),
        "build_output": None,
        "build_hash_v1": None,
    }


def _card_role_compression_value(meta: Dict[str, Any], missing_primitives: List[str]) -> int:
    primitives = [p for p in (meta.get("primitives") or []) if isinstance(p, str)]
    primitive_set = set(primitives)

    need_buckets = [
        need_name
        for need_name in ("ramp", "draw", "interaction", "protection", "wincon")
        if any(_primitive_matches_need(p, need_name) for p in primitive_set)
    ]
    missing_covered = [p for p in missing_primitives if p in primitive_set]
    return int(len(need_buckets) + len(missing_covered))


def _rank_cut_candidates_v0_1(
    deck_cards: List[str],
    card_catalog: Dict[str, Dict[str, Any]],
    dead_card_names: List[str],
    missing_primitives: List[str],
    targets: Dict[str, int],
    category_counts: Dict[str, int],
    locked_cards: set[str],
    allow_anchor_swaps: bool,
) -> List[str]:
    dead_set = set(dead_card_names)
    land_too_high = int(category_counts.get("land", 0)) > int(targets.get("land_count_target", 0))

    ranked: List[Tuple[Tuple[Any, ...], str]] = []
    for card_name in sorted(deck_cards):
        meta = card_catalog.get(card_name)
        if not isinstance(meta, dict):
            continue

        if card_name in locked_cards and not allow_anchor_swaps:
            continue
        if is_singleton_exempt_card(card_name, meta.get("type_line")):
            continue

        primitives = [p for p in (meta.get("primitives") or []) if isinstance(p, str)]
        primitive_set = set(primitives)
        contributes_required = any(p in GENERIC_MINIMUMS for p in primitive_set)
        role_compression = _card_role_compression_value(meta, missing_primitives)
        is_land = _is_land(meta)

        if land_too_high:
            land_rank = 0 if is_land else 1
        else:
            land_rank = 0 if (not is_land) else 1

        rank_key = (
            0 if card_name in dead_set else 1,
            0 if (not contributes_required) else 1,
            int(role_compression),
            int(land_rank),
            card_name,
        )
        ranked.append((rank_key, card_name))

    ranked.sort(key=lambda item: item[0])
    return [card_name for _, card_name in ranked]


def _weakest_need_bucket_v0_1(category_counts: Dict[str, int], targets: Dict[str, int]) -> str:
    ordered = ["ramp", "draw", "interaction", "protection", "wincon"]
    deficits: List[Tuple[int, int, str]] = []
    for idx, bucket in enumerate(ordered):
        target_key = f"{bucket}_target"
        deficit = int(targets.get(target_key, 0)) - int(category_counts.get(bucket, 0))
        deficits.append((deficit, -idx, bucket))
    deficits.sort(reverse=True)
    if deficits and deficits[0][0] > 0:
        return deficits[0][2]
    return "redundancy"


def _rank_replacements_for_cut_v0_1(
    candidate_pool: List[Dict[str, Any]],
    cut_name: str,
    deck_without_cut: List[str],
    commander_ci: List[str],
    bracket_id: str,
    commander_name: str,
    gc_set: set[str],
    missing_primitives: List[str],
    weakest_bucket: str,
    core_primitives_set: set[str],
    commander_primitives_set: set[str],
    anchor_primitives_set: set[str],
    primitive_frequency_without: Dict[str, int],
    targets: Dict[str, int],
    category_counts_without: Dict[str, int],
) -> List[Dict[str, Any]]:
    _ = weakest_bucket

    candidate_rows: List[Dict[str, Any]] = []

    for candidate in candidate_pool:
        name = candidate.get("name")
        if not isinstance(name, str):
            continue
        if name == cut_name:
            continue

        candidate_meta = _candidate_meta_from_pool_row(candidate)

        if not _can_add_card(
            card_name=name,
            card_meta=candidate_meta,
            deck_cards=deck_without_cut,
            commander_ci=commander_ci,
            bracket_id=bracket_id,
            commander_name=commander_name,
            gc_set=gc_set,
        ):
            continue

        candidate_rows.append(
            {
                "name": name,
                "slot_id": None,
                "primitives": list(candidate_meta.get("primitives") or []),
                "is_game_changer": name in gc_set,
                "meta": candidate_meta,
            }
        )

    ranked = rank_candidates_v1(
        candidates=candidate_rows,
        deck_state={
            "commander_primitives": sorted(commander_primitives_set),
            "anchor_primitives": sorted(anchor_primitives_set),
            "deck_primitives": sorted(primitive_frequency_without.keys()),
        },
        hypothesis={
            "core_primitives": sorted(core_primitives_set),
        },
        missing_targets={
            "missing_primitives": list(missing_primitives),
            "missing_by_bucket": _missing_targets_by_bucket(
                category_counts=category_counts_without,
                targets=targets,
            ),
        },
        gc_remaining=_gc_remaining_for_ranking(
            deck_cards=deck_without_cut,
            commander_name=commander_name,
            bracket_id=bracket_id,
            gc_set=gc_set,
        ),
    )

    return [
        {
            "name": row.get("name"),
            "meta": row.get("meta") if isinstance(row.get("meta"), dict) else {},
            "ranking_signals_v1": row.get("ranking_signals_v1") if isinstance(row.get("ranking_signals_v1"), dict) else {},
        }
        for row in ranked
        if isinstance(row.get("name"), str)
    ]


def _remove_one_card(deck_cards: List[str], card_name: str) -> List[str]:
    removed = False
    out: List[str] = []
    for value in deck_cards:
        if (not removed) and value == card_name:
            removed = True
            continue
        out.append(value)
    return out


def _sort_deck_cards_for_refine(deck_cards: List[str]) -> List[str]:
    return sorted(deck_cards, key=lambda name: (str(name).lower(), str(name)))


def _is_metrics_improvement(candidate: Dict[str, Any], baseline: Dict[str, Any]) -> bool:
    candidate_score = float(candidate.get("total_score_v2") or 0.0)
    baseline_score = float(baseline.get("total_score_v2") or 0.0)
    if candidate_score > baseline_score:
        return True
    if candidate_score < baseline_score:
        return False

    candidate_secondary = (
        int(candidate.get("dead_slot_ids_count") or 0),
        float(candidate.get("commander_overlap_ratio") or 0.0),
        -float(candidate.get("engine_density_score") or 0.0),
    )
    baseline_secondary = (
        int(baseline.get("dead_slot_ids_count") or 0),
        float(baseline.get("commander_overlap_ratio") or 0.0),
        -float(baseline.get("engine_density_score") or 0.0),
    )
    return candidate_secondary < baseline_secondary


def generate_deck_completion_v0(
    commander: str,
    anchors: List[str],
    profile_id: str,
    bracket_id: str,
    max_iters: int = 40,
    target_deck_size: int = 100,
    seed_package: Dict[str, Any] | None = None,
    validate_each_iter: bool = True,
    db_snapshot_id: str | None = None,
    refine: bool = False,
    max_refine_iters: int = 30,
    swap_batch_size: int = 8,
    validate_each_refine_iter: bool = True,
) -> Dict[str, Any]:
    try:
        snapshot_id = _resolve_snapshot_id(db_snapshot_id)
    except Exception as exc:
        return {
            "status": "ERROR",
            "deck_complete_v0": {
                "inputs": {
                    "commander": commander,
                    "anchors": anchors,
                    "profile_id": profile_id,
                    "bracket_id": bracket_id,
                    "max_iters": max_iters,
                    "target_deck_size": target_deck_size,
                    "seed_package": seed_package,
                    "validate_each_iter": validate_each_iter,
                },
                "final_deck": {"commander": commander, "cards": []},
                "build_report": {},
                "iterations": [],
                "explanation": {
                    "plan_summary": "Snapshot resolution failed for deterministic deck completion.",
                    "why_these_cards": [],
                    "structural_gaps_remaining": [str(exc)],
                },
            },
        }

    if not isinstance(commander, str) or commander.strip() == "":
        return {
            "status": "ERROR",
            "deck_complete_v0": {
                "inputs": {
                    "commander": commander,
                    "anchors": anchors,
                    "profile_id": profile_id,
                    "bracket_id": bracket_id,
                    "max_iters": max_iters,
                    "target_deck_size": target_deck_size,
                    "seed_package": seed_package,
                    "validate_each_iter": validate_each_iter,
                },
                "final_deck": {"commander": commander, "cards": []},
                "build_report": {},
                "iterations": [],
                "explanation": {
                    "plan_summary": "Commander is required.",
                    "why_these_cards": [],
                    "structural_gaps_remaining": ["MISSING_COMMANDER"],
                },
            },
        }

    commander_name = commander.strip()
    commander_card = find_card_by_name(snapshot_id, commander_name)
    if commander_card is None:
        return {
            "status": "ERROR",
            "deck_complete_v0": {
                "inputs": {
                    "commander": commander,
                    "anchors": anchors,
                    "profile_id": profile_id,
                    "bracket_id": bracket_id,
                    "max_iters": max_iters,
                    "target_deck_size": target_deck_size,
                    "seed_package": seed_package,
                    "validate_each_iter": validate_each_iter,
                },
                "final_deck": {"commander": commander_name, "cards": []},
                "build_report": {},
                "iterations": [],
                "explanation": {
                    "plan_summary": "Commander unknown in local snapshot.",
                    "why_these_cards": [],
                    "structural_gaps_remaining": ["UNKNOWN_COMMANDER"],
                },
            },
        }

    commander_legal, _ = is_legal_commander_card(commander_card)
    format_legal, _ = is_legal_in_format(commander_card, "commander")
    if (not commander_legal) or (not format_legal):
        return {
            "status": "ERROR",
            "deck_complete_v0": {
                "inputs": {
                    "commander": commander,
                    "anchors": anchors,
                    "profile_id": profile_id,
                    "bracket_id": bracket_id,
                    "max_iters": max_iters,
                    "target_deck_size": target_deck_size,
                    "seed_package": seed_package,
                    "validate_each_iter": validate_each_iter,
                },
                "final_deck": {"commander": commander_name, "cards": []},
                "build_report": {},
                "iterations": [],
                "explanation": {
                    "plan_summary": "Commander failed legality checks.",
                    "why_these_cards": [],
                    "structural_gaps_remaining": ["ILLEGAL_COMMANDER"],
                },
            },
        }

    commander_meta = _card_meta(commander_card)
    commander_ci = [c for c in commander_meta.get("color_identity") or [] if isinstance(c, str)]
    commander_oracle_id = commander_meta.get("oracle_id") or ""
    commander_primitives_set = set(
        [p for p in (commander_meta.get("primitives") or []) if isinstance(p, str)]
    )

    desired_noncommander = max(int(target_deck_size) - 1, 0)
    if desired_noncommander <= 0:
        return {
            "status": "ERROR",
            "deck_complete_v0": {
                "inputs": {
                    "commander": commander,
                    "anchors": anchors,
                    "profile_id": profile_id,
                    "bracket_id": bracket_id,
                    "max_iters": max_iters,
                    "target_deck_size": target_deck_size,
                    "seed_package": seed_package,
                    "validate_each_iter": validate_each_iter,
                },
                "final_deck": {"commander": commander_name, "cards": []},
                "build_report": {},
                "iterations": [],
                "explanation": {
                    "plan_summary": "Target deck size must be at least 1.",
                    "why_these_cards": [],
                    "structural_gaps_remaining": ["INVALID_TARGET_DECK_SIZE"],
                },
            },
        }

    anchor_input = _stable_unique_preserve_order(anchors or [])
    seed_cards_raw = []
    if isinstance(seed_package, dict):
        raw_cards = seed_package.get("cards")
        if isinstance(raw_cards, list):
            seed_cards_raw = _stable_unique_preserve_order(raw_cards)

    seed_ordered = _stable_unique_preserve_order(anchor_input + seed_cards_raw)

    deck_cards: List[str] = []
    card_catalog: Dict[str, Dict[str, Any]] = {}
    unknown_inputs: List[str] = []
    rejected_inputs: List[str] = []

    for card_name in seed_ordered:
        resolved = find_card_by_name(snapshot_id, card_name)
        if resolved is None:
            unknown_inputs.append(card_name)
            continue
        meta = _card_meta(resolved)
        name = meta.get("name")
        if not isinstance(name, str):
            continue

        legal, _ = is_legal_in_format(meta, "commander")
        if not legal:
            rejected_inputs.append(f"{name}:ILLEGAL_CARD")
            continue

        if not set(meta.get("color_identity") or []).issubset(set(commander_ci)):
            rejected_inputs.append(f"{name}:COLOR_IDENTITY_VIOLATION")
            continue

        if name == commander_name or meta.get("oracle_id") == commander_oracle_id:
            rejected_inputs.append(f"{name}:COMMANDER_DUPLICATE")
            continue

        if (not is_singleton_exempt_card(name, meta.get("type_line"))) and (name in set(deck_cards)):
            rejected_inputs.append(f"{name}:DUPLICATE_CARD")
            continue

        card_catalog[name] = meta
        deck_cards.append(name)

    if unknown_inputs:
        return {
            "status": "ERROR",
            "deck_complete_v0": {
                "inputs": {
                    "commander": commander_name,
                    "anchors": anchor_input,
                    "profile_id": profile_id,
                    "bracket_id": bracket_id,
                    "max_iters": int(max_iters),
                    "target_deck_size": int(target_deck_size),
                    "seed_package": seed_package,
                    "validate_each_iter": bool(validate_each_iter),
                    "db_snapshot_id": snapshot_id,
                },
                "final_deck": {"commander": commander_name, "cards": deck_cards},
                "build_report": {},
                "iterations": [],
                "explanation": {
                    "plan_summary": "Unknown card inputs require adjudication.",
                    "why_these_cards": [],
                    "structural_gaps_remaining": sorted_unique([f"UNKNOWN_CARD:{name}" for name in unknown_inputs]),
                },
            },
        }

    targets = _targets_for_profile(profile_id=profile_id, desired_noncommander=desired_noncommander)
    max_iters_safe = max(0, int(max_iters))

    anchor_primitives: List[str] = []
    for name in anchor_input:
        meta = card_catalog.get(name)
        if isinstance(meta, dict):
            anchor_primitives.extend([p for p in (meta.get("primitives") or []) if isinstance(p, str)])
    anchor_primitives_freq: Dict[str, int] = {}
    for primitive in anchor_primitives:
        anchor_primitives_freq[primitive] = anchor_primitives_freq.get(primitive, 0) + 1
    core_primitives = [
        p
        for p in sorted(anchor_primitives_freq.keys(), key=lambda x: (-anchor_primitives_freq.get(x, 0), x))[:6]
    ]
    core_primitives_set = set(core_primitives)
    anchor_primitives_set = set(anchor_primitives)

    iterations: List[Dict[str, Any]] = []
    iter_index = 0

    gc_set = set()
    try:
        from api.engine.constants import GAME_CHANGERS_SET as _GC_SET

        gc_set = set(_GC_SET)
    except Exception:
        gc_set = set()

    while len(deck_cards) < desired_noncommander and iter_index < max_iters_safe:
        need_data = _need_counts(deck_cards=deck_cards, catalog=card_catalog)
        primitive_frequency = need_data["primitive_frequency"]
        category_counts = need_data["category_counts"]

        missing_primitives = _missing_generic_primitives(primitive_frequency)

        candidate_pool = get_candidate_pool_v0(
            snapshot_id=snapshot_id,
            primitives_needed=missing_primitives,
            commander_name=commander_name,
            commander_oracle_id=commander_oracle_id,
            commander_ci=commander_ci,
            format_name="commander",
            bracket_id=bracket_id,
            current_cards=deck_cards,
        )

        ranked_candidates = _rank_candidates_for_builder_v1(
            candidate_pool=candidate_pool,
            deck_cards=deck_cards,
            commander_ci=commander_ci,
            bracket_id=bracket_id,
            commander_name=commander_name,
            gc_set=gc_set,
            missing_primitives=missing_primitives,
            primitive_frequency=primitive_frequency,
            category_counts=category_counts,
            targets=targets,
            commander_primitives_set=commander_primitives_set,
            anchor_primitives_set=anchor_primitives_set,
            core_primitives_set=core_primitives_set,
        )
        chosen = ranked_candidates[0] if ranked_candidates else None

        if chosen is None:
            break

        chosen_name = chosen["name"]
        chosen_meta = chosen["meta"]
        card_catalog[chosen_name] = chosen_meta
        deck_cards.append(chosen_name)
        iter_counts = _need_counts(deck_cards=deck_cards, catalog=card_catalog)

        iter_record: Dict[str, Any] = {
            "iter_id": f"I{iter_index}",
            "iter": iter_index,
            "iter_type": "build",
            "added": [chosen_name],
            "removed": [],
            "deck_size": 1 + len(deck_cards),
            "candidate_ranking_signals_v1": chosen.get("ranking_signals_v1") if isinstance(chosen.get("ranking_signals_v1"), dict) else None,
        }

        if validate_each_iter:
            build_output = _run_build(
                snapshot_id=snapshot_id,
                commander=commander_name,
                cards=deck_cards,
                profile_id=profile_id,
                bracket_id=bracket_id,
            )
            iter_record["score_v0"] = score_deck_v0(build_output)
            iter_record["score_v2"] = score_deck_v2(
                state=build_output,
                context=_build_score_context_v2(
                    primitive_frequency=iter_counts["primitive_frequency"],
                    category_counts=iter_counts["category_counts"],
                    targets=targets,
                    commander_primitives_set=commander_primitives_set,
                    anchor_primitives_set=anchor_primitives_set,
                ),
            )
            iter_record["build_hash_v1"] = build_output.get("build_hash_v1")
        else:
            iter_record["score_v0"] = _internal_score_v0(
                category_counts=iter_counts["category_counts"],
                targets=targets,
                dead_slots_count=0,
            )
            iter_commander_overlap_ratio = _estimate_commander_overlap_ratio(
                deck_cards=deck_cards,
                card_catalog=card_catalog,
                commander_primitives_set=commander_primitives_set,
            )
            iter_record["score_v2"] = score_deck_v2(
                state=_build_estimated_score_state_v2(
                    primitive_frequency=iter_counts["primitive_frequency"],
                    commander_overlap_ratio=iter_commander_overlap_ratio,
                ),
                context=_build_score_context_v2(
                    primitive_frequency=iter_counts["primitive_frequency"],
                    category_counts=iter_counts["category_counts"],
                    targets=targets,
                    commander_primitives_set=commander_primitives_set,
                    anchor_primitives_set=anchor_primitives_set,
                ),
            )

        iterations.append(iter_record)
        iter_index += 1

    basic_cycle = _build_basic_cycle(commander_ci=commander_ci, deck_cards=deck_cards, catalog=card_catalog)
    basic_idx = 0

    def _add_basic_card() -> bool:
        nonlocal basic_idx
        if not basic_cycle:
            return False
        basic_name = basic_cycle[basic_idx % len(basic_cycle)]
        basic_idx += 1
        resolved = find_card_by_name(snapshot_id, basic_name)
        if resolved is None:
            return False
        meta = _card_meta(resolved)
        if not _can_add_card(
            card_name=basic_name,
            card_meta=meta,
            deck_cards=deck_cards,
            commander_ci=commander_ci,
            bracket_id=bracket_id,
            commander_name=commander_name,
            gc_set=gc_set,
        ):
            return False
        card_catalog[basic_name] = meta
        deck_cards.append(basic_name)
        return True

    while len(deck_cards) < desired_noncommander:
        need_data = _need_counts(deck_cards=deck_cards, catalog=card_catalog)
        primitive_frequency = need_data["primitive_frequency"]
        category_counts = need_data["category_counts"]
        missing_primitives = _missing_generic_primitives(primitive_frequency)
        need_land = int(category_counts.get("land", 0)) < int(targets.get("land_count_target", 0))
        if need_land:
            if not _add_basic_card():
                break
            continue

        fallback_pool = get_candidate_pool_v0(
            snapshot_id=snapshot_id,
            primitives_needed=[],
            commander_name=commander_name,
            commander_oracle_id=commander_oracle_id,
            commander_ci=commander_ci,
            format_name="commander",
            bracket_id=bracket_id,
            current_cards=deck_cards,
        )

        ranked_fallback = _rank_candidates_for_builder_v1(
            candidate_pool=fallback_pool,
            deck_cards=deck_cards,
            commander_ci=commander_ci,
            bracket_id=bracket_id,
            commander_name=commander_name,
            gc_set=gc_set,
            missing_primitives=missing_primitives,
            primitive_frequency=primitive_frequency,
            category_counts=category_counts,
            targets=targets,
            commander_primitives_set=commander_primitives_set,
            anchor_primitives_set=anchor_primitives_set,
            core_primitives_set=core_primitives_set,
        )

        picked = None
        if ranked_fallback:
            first = ranked_fallback[0]
            picked_name = first.get("name")
            picked_meta = first.get("meta") if isinstance(first.get("meta"), dict) else None
            if isinstance(picked_name, str) and isinstance(picked_meta, dict):
                picked = (picked_name, picked_meta)

        if picked is None:
            if not _add_basic_card():
                break
            continue

        picked_name, picked_meta = picked
        deck_cards.append(picked_name)
        card_catalog[picked_name] = picked_meta

    final_build = _run_build(
        snapshot_id=snapshot_id,
        commander=commander_name,
        cards=deck_cards,
        profile_id=profile_id,
        bracket_id=bracket_id,
    )

    final_result = final_build.get("result") if isinstance(final_build, dict) else {}
    final_result = final_result if isinstance(final_result, dict) else {}

    is_exact_target = len(deck_cards) == desired_noncommander
    final_status = final_build.get("status") if isinstance(final_build, dict) else None
    success_statuses = {"OK", "OK_WITH_UNKNOWNS"}
    is_status_ok = final_status in success_statuses

    refinement_obj: Dict[str, Any] | None = None
    if bool(refine):
        max_refine_iters_safe = max(0, int(max_refine_iters))
        swap_batch_size_safe = max(1, int(swap_batch_size))
        allow_anchor_swaps = False
        locked_cards = set(anchor_input)

        baseline_eval = _evaluate_deck_state_v0_1(
            deck_cards=deck_cards,
            card_catalog=card_catalog,
            targets=targets,
            snapshot_id=snapshot_id,
            commander_name=commander_name,
            profile_id=profile_id,
            bracket_id=bracket_id,
            validate_each_refine_iter=bool(validate_each_refine_iter),
            commander_primitives_set=commander_primitives_set,
            anchor_primitives_set=anchor_primitives_set,
        )

        refinement_obj = {
            "enabled": True,
            "iters_run": 0,
            "best_score_v0": _round_metric(float(baseline_eval.get("score_total") or 0.0)),
            "best_score_v2": _round_metric(float(baseline_eval.get("total_score_v2") or 0.0)),
            "accepted_swaps": 0,
            "rejected_swaps": 0,
        }

        if is_exact_target and is_status_ok and max_refine_iters_safe > 0:
            best_deck_cards = list(deck_cards)
            best_eval = dict(baseline_eval)
            accepted_swaps = 0
            rejected_swaps = 0
            iters_run = 0

            while iters_run < max_refine_iters_safe:
                iters_run += 1
                need_data_best = _need_counts(deck_cards=best_deck_cards, catalog=card_catalog)
                primitive_frequency_best = need_data_best["primitive_frequency"]
                category_counts_best = need_data_best["category_counts"]
                missing_primitives_best = _missing_generic_primitives(primitive_frequency_best)

                cut_candidates = _rank_cut_candidates_v0_1(
                    deck_cards=best_deck_cards,
                    card_catalog=card_catalog,
                    dead_card_names=best_eval.get("dead_card_names") or [],
                    missing_primitives=missing_primitives_best,
                    targets=targets,
                    category_counts=category_counts_best,
                    locked_cards=locked_cards,
                    allow_anchor_swaps=allow_anchor_swaps,
                )
                cut_batch = cut_candidates[:swap_batch_size_safe]

                improvement_found = False

                for cut_name in cut_batch:
                    deck_without_cut = _remove_one_card(best_deck_cards, cut_name)
                    need_data_without = _need_counts(deck_cards=deck_without_cut, catalog=card_catalog)
                    primitive_frequency_without = need_data_without["primitive_frequency"]
                    category_counts_without = need_data_without["category_counts"]
                    missing_without = _missing_generic_primitives(primitive_frequency_without)
                    weakest_bucket = _weakest_need_bucket_v0_1(
                        category_counts=category_counts_without,
                        targets=targets,
                    )

                    candidate_pool = get_candidate_pool_v0(
                        snapshot_id=snapshot_id,
                        primitives_needed=missing_without,
                        commander_name=commander_name,
                        commander_oracle_id=commander_oracle_id,
                        commander_ci=commander_ci,
                        format_name="commander",
                        bracket_id=bracket_id,
                        current_cards=deck_without_cut,
                    )

                    replacement_candidates = _rank_replacements_for_cut_v0_1(
                        candidate_pool=candidate_pool,
                        cut_name=cut_name,
                        deck_without_cut=deck_without_cut,
                        commander_ci=commander_ci,
                        bracket_id=bracket_id,
                        commander_name=commander_name,
                        gc_set=gc_set,
                        missing_primitives=missing_without,
                        weakest_bucket=weakest_bucket,
                        core_primitives_set=core_primitives_set,
                        commander_primitives_set=commander_primitives_set,
                        anchor_primitives_set=anchor_primitives_set,
                        primitive_frequency_without=primitive_frequency_without,
                        targets=targets,
                        category_counts_without=category_counts_without,
                    )
                    replacement_candidates = replacement_candidates[:REFINEMENT_REPLACEMENT_TOP_K_V0_1]

                    for replacement in replacement_candidates:
                        replacement_name = replacement.get("name")
                        replacement_meta = replacement.get("meta") if isinstance(replacement.get("meta"), dict) else {}
                        if not isinstance(replacement_name, str):
                            continue

                        proposal_cards = _sort_deck_cards_for_refine(deck_without_cut + [replacement_name])
                        proposal_eval = _evaluate_deck_state_v0_1(
                            deck_cards=proposal_cards,
                            card_catalog={**card_catalog, replacement_name: replacement_meta},
                            targets=targets,
                            snapshot_id=snapshot_id,
                            commander_name=commander_name,
                            profile_id=profile_id,
                            bracket_id=bracket_id,
                            validate_each_refine_iter=bool(validate_each_refine_iter),
                            commander_primitives_set=commander_primitives_set,
                            anchor_primitives_set=anchor_primitives_set,
                        )

                        if _is_metrics_improvement(proposal_eval, best_eval):
                            best_deck_cards = proposal_cards
                            card_catalog[replacement_name] = replacement_meta
                            best_eval = proposal_eval
                            accepted_swaps += 1

                            iter_record = {
                                "iter_id": f"I{iter_index}",
                                "iter": iter_index,
                                "iter_type": "refine",
                                "swap": {"out": cut_name, "in": replacement_name},
                                "added": [replacement_name],
                                "removed": [cut_name],
                                "deck_size": 1 + len(best_deck_cards),
                                "score_v0": proposal_eval.get("score_v0") or {"score_total": _round_metric(float(proposal_eval.get("score_total") or 0.0))},
                                "score_v2": proposal_eval.get("score_v2") or {"total_score_v2": _round_metric(float(proposal_eval.get("total_score_v2") or 0.0))},
                                "candidate_ranking_signals_v1": replacement.get("ranking_signals_v1") if isinstance(replacement.get("ranking_signals_v1"), dict) else None,
                            }
                            if bool(validate_each_refine_iter):
                                iter_record["build_hash_v1"] = proposal_eval.get("build_hash_v1")

                            iterations.append(iter_record)
                            iter_index += 1
                            improvement_found = True
                            break

                        rejected_swaps += 1

                    if improvement_found:
                        break

                if not improvement_found:
                    break

            refinement_obj = {
                "enabled": True,
                "iters_run": iters_run,
                "best_score_v0": _round_metric(float(best_eval.get("score_total") or 0.0)),
                "best_score_v2": _round_metric(float(best_eval.get("total_score_v2") or 0.0)),
                "accepted_swaps": accepted_swaps,
                "rejected_swaps": rejected_swaps,
            }

            deck_cards = list(best_deck_cards)
            if bool(validate_each_refine_iter) and isinstance(best_eval.get("build_output"), dict):
                final_build = best_eval.get("build_output")
            else:
                final_build = _run_build(
                    snapshot_id=snapshot_id,
                    commander=commander_name,
                    cards=deck_cards,
                    profile_id=profile_id,
                    bracket_id=bracket_id,
                )

            final_result = final_build.get("result") if isinstance(final_build, dict) else {}
            final_result = final_result if isinstance(final_result, dict) else {}
            is_exact_target = len(deck_cards) == desired_noncommander
            final_status = final_build.get("status") if isinstance(final_build, dict) else None
            is_status_ok = final_status in success_statuses

    final_needs = final_result.get("needs") if isinstance(final_result.get("needs"), list) else []
    status = "OK" if is_exact_target and is_status_ok else "ERROR"

    why_these_cards = _stable_unique_preserve_order(deck_cards[:25])
    structural_gaps_remaining = [
        str(item.get("primitive"))
        for item in final_needs
        if isinstance(item, dict) and isinstance(item.get("primitive"), str)
    ]

    explanation_summary = (
        "Deterministic completion used legality + color identity filters, primitive-target guidance, "
        "and basic-land fallback to reach target deck size."
    )

    if bool(refine):
        explanation_summary = (
            "Deterministic completion used legality + color identity filters, primitive-target guidance, "
            "basic-land fallback, and bounded deterministic swap refinement."
        )

    if not is_exact_target:
        structural_gaps_remaining = structural_gaps_remaining + ["TARGET_SIZE_NOT_REACHED"]

    if rejected_inputs:
        structural_gaps_remaining = structural_gaps_remaining + sorted_unique(rejected_inputs)

    deck_complete_payload: Dict[str, Any] = {
        "inputs": {
            "commander": commander_name,
            "anchors": anchor_input,
            "profile_id": profile_id,
            "bracket_id": bracket_id,
            "max_iters": max_iters_safe,
            "target_deck_size": int(target_deck_size),
            "seed_package": seed_package if isinstance(seed_package, dict) else None,
            "validate_each_iter": bool(validate_each_iter),
            "db_snapshot_id": snapshot_id,
            "refine": bool(refine),
            "max_refine_iters": int(max_refine_iters),
            "swap_batch_size": int(swap_batch_size),
            "validate_each_refine_iter": bool(validate_each_refine_iter),
        },
        "final_deck": {
            "commander": commander_name,
            "cards": deck_cards,
        },
        "build_report": final_build,
        "iterations": iterations,
        "explanation": {
            "plan_summary": explanation_summary,
            "why_these_cards": why_these_cards,
            "structural_gaps_remaining": structural_gaps_remaining,
        },
    }

    if bool(refine) and isinstance(refinement_obj, dict):
        deck_complete_payload["refinement"] = refinement_obj

    return {
        "status": status,
        "deck_complete_v0": deck_complete_payload,
    }
