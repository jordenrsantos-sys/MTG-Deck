from __future__ import annotations

from time import perf_counter
from typing import Any, Dict, List, Set, Tuple

from api.engine.bracket_gc_enforcement_v1 import UNKNOWN_BRACKET_RULES, would_violate_gc_limit_v1
from api.engine.candidate_pool_v1 import get_candidate_pool_v1
from api.engine.color_identity_constraints_v1 import (
    COLOR_IDENTITY_UNAVAILABLE,
    UNKNOWN_COLOR_IDENTITY,
    get_commander_color_identity_v1,
    is_card_color_legal_v1,
)
from api.engine.utils import normalize_primitives_source, slot_sort_key


VERSION = "deck_tune_engine_v1"

_TOP_CUT_LIMIT = 10
_TOP_ADD_LIMIT = 50
_MAX_SWAP_EVALUATIONS = 500
PROTECT_TOP_K_CARDS_V1 = 8
MIN_TOTAL_SCORE_DELTA_V1 = 0.01
REQUIRE_PRIMITIVE_COVERAGE_WHEN_MISSING_REQUIRED_V1 = True

_PROTECTION_PRIMITIVES = (
    "PROTECTION",
    "PROTECTION_COMBAT",
    "PROTECTION_STACK",
    "INDESTRUCTIBLE",
    "HEXPROOF",
    "WARD",
)


def _nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return ""


def _coerce_positive_int(value: Any, *, default: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return int(default)
    if int(value) < 1:
        return int(default)
    return int(value)


def _coerce_nonnegative_float(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    numeric = float(value)
    if numeric < 0.0:
        return 0.0
    return numeric


def _coerce_float(value: Any, *, default: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return float(default)
    return float(value)


def _coerce_nonnegative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    if int(value) < 0:
        return 0
    return int(value)


def _coerce_int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return int(default)
    return int(value)


def _round6(value: float) -> float:
    return float(f"{float(value):.6f}")


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    cleaned = {
        token
        for token in (_nonempty_str(value) for value in values)
        if token != ""
    }
    return sorted(cleaned)


def _normalize_card_list(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    out: List[str] = []
    for value in values:
        token = _nonempty_str(value)
        if token == "":
            continue
        out.append(token)
    return out


def _remove_one_card(cards: List[str], card_name: str) -> List[str]:
    out: List[str] = []
    removed = False
    for value in cards:
        if (not removed) and value == card_name:
            removed = True
            continue
        out.append(value)
    return out


def _baseline_result_payload(baseline_build_result: Any) -> Dict[str, Any]:
    payload = baseline_build_result if isinstance(baseline_build_result, dict) else {}
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    return result


def _extract_structural_snapshot(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    return (
        result_payload.get("structural_snapshot_v1")
        if isinstance(result_payload.get("structural_snapshot_v1"), dict)
        else {}
    )


def _extract_primitive_counts_by_id(structural_snapshot: Dict[str, Any]) -> Dict[str, int]:
    raw = structural_snapshot.get("primitive_counts_by_id")
    if not isinstance(raw, dict):
        return {}

    out: Dict[str, int] = {}
    for key in sorted(raw.keys(), key=lambda token: str(token)):
        primitive = _nonempty_str(key)
        if primitive == "":
            continue
        value = raw.get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            continue
        if int(value) < 0:
            continue
        out[primitive] = int(value)
    return out


def _extract_missing_primitives(
    *,
    result_payload: Dict[str, Any],
    structural_snapshot: Dict[str, Any],
    primitive_counts_by_id: Dict[str, int],
) -> List[str]:
    required_v0 = _clean_sorted_unique_strings(result_payload.get("required_primitives_v0"))
    if len(required_v0) > 0:
        return [
            primitive
            for primitive in required_v0
            if int(primitive_counts_by_id.get(primitive, 0)) <= 0
        ]

    return _clean_sorted_unique_strings(structural_snapshot.get("missing_primitives_v1"))


def _extract_redundancy_primitives(*, result_payload: Dict[str, Any], level: str) -> List[str]:
    redundancy_payload = (
        result_payload.get("redundancy_index_v1")
        if isinstance(result_payload.get("redundancy_index_v1"), dict)
        else {}
    )
    rows = redundancy_payload.get("per_requirement") if isinstance(redundancy_payload.get("per_requirement"), list) else []

    out: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        primitive = _nonempty_str(row.get("primitive"))
        if primitive == "":
            continue
        row_level = _nonempty_str(row.get("redundancy_level"))
        supported = row.get("supported") if isinstance(row.get("supported"), bool) else False
        if (not supported) or row_level != level:
            continue
        out.append(primitive)
    return sorted(set(out))


def _needs_protection_candidates(result_payload: Dict[str, Any]) -> bool:
    resilience_payload = (
        result_payload.get("resilience_math_engine_v1")
        if isinstance(result_payload.get("resilience_math_engine_v1"), dict)
        else {}
    )
    resilience_metrics = resilience_payload.get("metrics") if isinstance(resilience_payload.get("metrics"), dict) else {}

    continuity = resilience_metrics.get("engine_continuity_after_removal")
    rebuild = resilience_metrics.get("rebuild_after_wipe")

    if isinstance(continuity, (int, float)) and not isinstance(continuity, bool) and float(continuity) < 0.7:
        return True
    if isinstance(rebuild, (int, float)) and not isinstance(rebuild, bool) and float(rebuild) < 0.6:
        return True

    commander_payload = (
        result_payload.get("commander_reliability_model_v1")
        if isinstance(result_payload.get("commander_reliability_model_v1"), dict)
        else {}
    )
    commander_metrics = commander_payload.get("metrics") if isinstance(commander_payload.get("metrics"), dict) else {}
    protection_proxy = commander_metrics.get("protection_coverage_proxy")
    if isinstance(protection_proxy, (int, float)) and not isinstance(protection_proxy, bool):
        return float(protection_proxy) < 0.2

    return False


def _slot_contribution_score(slot_primitives: List[str], primitive_counts_by_id: Dict[str, int]) -> float:
    if len(slot_primitives) == 0:
        return 0.0

    contribution = 0.0
    for primitive in slot_primitives:
        denominator = max(int(primitive_counts_by_id.get(primitive, 0)), 1)
        contribution += 1.0 / float(denominator)
    return _round6(contribution)


def _extract_cut_candidates(
    *,
    canonical_deck_input: Dict[str, Any],
    result_payload: Dict[str, Any],
    primitive_counts_by_id: Dict[str, int],
    high_redundancy_primitives: Set[str],
) -> List[Dict[str, Any]]:
    dead_slot_ids = set(_clean_sorted_unique_strings(_extract_structural_snapshot(result_payload).get("dead_slot_ids_v1")))

    primitive_index_by_slot = (
        result_payload.get("primitive_index_by_slot")
        if isinstance(result_payload.get("primitive_index_by_slot"), dict)
        else {}
    )

    canonical_rows_raw = (
        result_payload.get("deck_cards_canonical_input_order")
        if isinstance(result_payload.get("deck_cards_canonical_input_order"), list)
        else []
    )

    canonical_rows = [row for row in canonical_rows_raw if isinstance(row, dict)]
    canonical_rows = sorted(
        canonical_rows,
        key=lambda row: (
            slot_sort_key(_nonempty_str(row.get("slot_id"))),
            _nonempty_str(row.get("resolved_oracle_id") or row.get("oracle_id")),
            _nonempty_str(row.get("resolved_name") or row.get("input")),
        ),
    )

    candidates: List[Dict[str, Any]] = []

    for row in canonical_rows:
        status = _nonempty_str(row.get("status"))
        if status.upper() != "PLAYABLE":
            continue

        slot_id = _nonempty_str(row.get("slot_id"))
        if slot_id == "" or slot_id.startswith("C"):
            continue

        card_name = _nonempty_str(row.get("resolved_name") or row.get("input"))
        if card_name == "":
            continue

        oracle_id = _nonempty_str(row.get("resolved_oracle_id") or row.get("oracle_id"))
        if oracle_id == "":
            oracle_id = f"NAME::{card_name}"

        slot_primitives = normalize_primitives_source(primitive_index_by_slot.get(slot_id))
        slot_primitives = sorted(set(slot_primitives))

        contribution_score = _slot_contribution_score(slot_primitives, primitive_counts_by_id)
        redundancy_excess_count = len([primitive for primitive in slot_primitives if primitive in high_redundancy_primitives])

        negative_impact_score = 0.0
        if slot_id in dead_slot_ids:
            negative_impact_score += 100.0
        if len(slot_primitives) == 0:
            negative_impact_score += 25.0
        negative_impact_score += float(redundancy_excess_count) * 10.0
        negative_impact_score += max(0.0, 5.0 - (contribution_score * 5.0))

        candidates.append(
            {
                "slot_id": slot_id,
                "card_name": card_name,
                "oracle_id": oracle_id,
                "slot_primitives": slot_primitives,
                "is_dead_slot": slot_id in dead_slot_ids,
                "contribution_score": contribution_score,
                "redundancy_excess_count": int(redundancy_excess_count),
                "negative_impact_score": _round6(negative_impact_score),
                "cut_score_v1": _round6(negative_impact_score),
            }
        )

    if len(candidates) > 0:
        return sorted(
            candidates,
            key=lambda row: (
                -float(row.get("negative_impact_score") or 0.0),
                str(row.get("oracle_id") or ""),
                str(row.get("card_name") or ""),
                str(row.get("slot_id") or ""),
            ),
        )

    cards_fallback = _normalize_card_list(canonical_deck_input.get("cards"))
    fallback_candidates: List[Dict[str, Any]] = []
    for idx, card_name in enumerate(cards_fallback):
        slot_id = f"S{idx}"
        fallback_candidates.append(
            {
                "slot_id": slot_id,
                "card_name": card_name,
                "oracle_id": f"NAME::{card_name}",
                "slot_primitives": [],
                "is_dead_slot": False,
                "contribution_score": 0.0,
                "redundancy_excess_count": 0,
                "negative_impact_score": 5.0,
                "cut_score_v1": 5.0,
            }
        )
    return fallback_candidates


def _cut_protection_rank_key(row: Any) -> Tuple[float, str, str]:
    if not isinstance(row, dict):
        return (0.0, "", "")
    return (
        -_coerce_nonnegative_float(row.get("contribution_score")),
        _nonempty_str(row.get("oracle_id")),
        _nonempty_str(row.get("card_name")),
    )


def _cut_candidate_identity_key(row: Any) -> str:
    if not isinstance(row, dict):
        return ""
    oracle_id = _nonempty_str(row.get("oracle_id"))
    if oracle_id != "":
        return f"OID::{oracle_id}"
    card_name = _nonempty_str(row.get("card_name"))
    if card_name != "":
        return f"NAME::{card_name}"
    slot_id = _nonempty_str(row.get("slot_id"))
    if slot_id != "":
        return f"SLOT::{slot_id}"
    return ""


def _resolve_protect_top_k_from_engine_patches(canonical_payload: Dict[str, Any]) -> int:
    protect_top_k_cards = max(int(PROTECT_TOP_K_CARDS_V1), 0)

    raw_patches = canonical_payload.get("engine_patches_v0") if isinstance(canonical_payload, dict) else None
    patches = raw_patches if isinstance(raw_patches, list) else []

    for patch in patches:
        if not isinstance(patch, dict):
            continue
        if _nonempty_str(patch.get("patch_type")) != "tune_config_v1":
            continue

        enabled_flag = patch.get("enabled")
        if isinstance(enabled_flag, bool) and not enabled_flag:
            continue

        protect_top_k_raw = patch.get("protect_top_k")
        if protect_top_k_raw is None:
            payload = patch.get("payload") if isinstance(patch.get("payload"), dict) else {}
            protect_top_k_raw = payload.get("protect_top_k")

        if isinstance(protect_top_k_raw, bool) or not isinstance(protect_top_k_raw, int):
            continue

        protect_top_k_cards = max(int(protect_top_k_raw), 0)

    return int(protect_top_k_cards)


def _partition_protected_cut_candidates(
    cut_candidates: List[Dict[str, Any]],
    *,
    protect_top_k_cards: int = PROTECT_TOP_K_CARDS_V1,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    non_dead_rows = [
        row
        for row in cut_candidates
        if isinstance(row, dict) and (not bool(row.get("is_dead_slot")))
    ]
    ranked_non_dead = sorted(non_dead_rows, key=_cut_protection_rank_key)

    protected_cut_keys = {
        key
        for key in (
            _cut_candidate_identity_key(row)
            for row in ranked_non_dead[: max(int(protect_top_k_cards), 0)]
        )
        if key != ""
    }

    eligible: List[Dict[str, Any]] = []
    protected: List[Dict[str, Any]] = []
    for row in cut_candidates:
        if not isinstance(row, dict):
            continue
        out = dict(row)
        key = _cut_candidate_identity_key(out)
        is_dead_slot = bool(out.get("is_dead_slot"))
        is_protected = (not is_dead_slot) and key in protected_cut_keys
        out["protected_cut_v1"] = bool(is_protected)
        if is_protected:
            protected.append(out)
        else:
            eligible.append(out)

    return eligible, protected


def _build_baseline_summary(
    *,
    baseline_build_result: Dict[str, Any],
    result_payload: Dict[str, Any],
    structural_snapshot: Dict[str, Any],
    profile_id: str,
    bracket_id: str,
    mulligan_model_id: str,
) -> Dict[str, Any]:
    structural_health = (
        structural_snapshot.get("structural_health_summary_v1")
        if isinstance(structural_snapshot.get("structural_health_summary_v1"), dict)
        else {}
    )

    dead_slot_ids = _clean_sorted_unique_strings(structural_snapshot.get("dead_slot_ids_v1"))
    missing_primitives = _clean_sorted_unique_strings(structural_snapshot.get("missing_primitives_v1"))

    bracket_payload = (
        result_payload.get("profile_bracket_enforcement_v1")
        if isinstance(result_payload.get("profile_bracket_enforcement_v1"), dict)
        else {}
    )
    bracket_counts = bracket_payload.get("counts") if isinstance(bracket_payload.get("counts"), dict) else {}

    coherence_payload = result_payload.get("engine_coherence_v1") if isinstance(result_payload.get("engine_coherence_v1"), dict) else {}
    coherence_metrics = coherence_payload.get("metrics") if isinstance(coherence_payload.get("metrics"), dict) else {}

    return {
        "build_status": _nonempty_str(baseline_build_result.get("status")),
        "deck_size_total": (
            int(baseline_build_result.get("deck_size_total"))
            if isinstance(baseline_build_result.get("deck_size_total"), int)
            and not isinstance(baseline_build_result.get("deck_size_total"), bool)
            else 0
        ),
        "dead_slot_count_v1": (
            int(structural_health.get("dead_slot_count"))
            if isinstance(structural_health.get("dead_slot_count"), int)
            and not isinstance(structural_health.get("dead_slot_count"), bool)
            else len(dead_slot_ids)
        ),
        "missing_required_count_v1": (
            int(structural_health.get("missing_required_count"))
            if isinstance(structural_health.get("missing_required_count"), int)
            and not isinstance(structural_health.get("missing_required_count"), bool)
            else len(missing_primitives)
        ),
        "primitive_concentration_index_v1": _coerce_nonnegative_float(
            structural_snapshot.get("primitive_concentration_index_v1")
        ),
        "overlap_score_v1": _coerce_nonnegative_float(coherence_metrics.get("overlap_score")),
        "game_changers_in_deck": (
            int(bracket_counts.get("game_changers_in_deck"))
            if isinstance(bracket_counts.get("game_changers_in_deck"), int)
            and not isinstance(bracket_counts.get("game_changers_in_deck"), bool)
            else 0
        ),
        "profile_id": profile_id,
        "bracket_id": bracket_id,
        "mulligan_model_id": mulligan_model_id,
    }


def _coverage_count(*, primitive_counts: Dict[str, int], target_primitives: Set[str]) -> int:
    return int(sum(1 for primitive in target_primitives if int(primitive_counts.get(primitive, 0)) > 0))


def _missing_required_count(*, primitive_counts: Dict[str, int], required_missing_primitives: Set[str]) -> int:
    return int(sum(1 for primitive in required_missing_primitives if int(primitive_counts.get(primitive, 0)) <= 0))


def _copy_counts_with_cut(
    *,
    primitive_counts_by_id: Dict[str, int],
    cut_primitives: List[str],
) -> Dict[str, int]:
    counts = {primitive: int(value) for primitive, value in primitive_counts_by_id.items()}
    for primitive in cut_primitives:
        current = int(counts.get(primitive, 0))
        counts[primitive] = max(current - 1, 0)
    return counts


def _copy_counts_with_add(
    *,
    primitive_counts: Dict[str, int],
    add_primitives: List[str],
) -> Dict[str, int]:
    counts = {primitive: int(value) for primitive, value in primitive_counts.items()}
    for primitive in add_primitives:
        counts[primitive] = int(counts.get(primitive, 0)) + 1
    return counts


def _evaluate_swap_pairs(
    *,
    db_snapshot_id: str,
    bracket_id: str,
    deck_cards: List[str],
    commander_color_set: Set[str],
    cut_candidates: List[Dict[str, Any]],
    add_candidates: List[Dict[str, Any]],
    primitive_counts_by_id: Dict[str, int],
    target_primitives: Set[str],
    required_missing_primitives: Set[str],
    protection_primitives_enabled: bool,
    collect_dev_metrics: bool,
    swap_filter_metrics_out: Any = None,
) -> Tuple[List[Dict[str, Any]], int, float]:
    swaps: List[Dict[str, Any]] = []
    swap_evaluations_total = 0
    swap_eval_ms_total = 0.0
    swaps_filtered_minbar_count = 0

    protection_primitive_set = set(_PROTECTION_PRIMITIVES)
    require_primitive_coverage_when_missing = bool(
        REQUIRE_PRIMITIVE_COVERAGE_WHEN_MISSING_REQUIRED_V1 and len(required_missing_primitives) > 0
    )

    top_cuts = cut_candidates[:_TOP_CUT_LIMIT]
    top_adds = add_candidates[:_TOP_ADD_LIMIT]

    for cut in top_cuts:
        cut_name = _nonempty_str(cut.get("card_name"))
        if cut_name == "":
            continue

        cut_oracle_id = _nonempty_str(cut.get("oracle_id"))
        cut_primitives = _clean_sorted_unique_strings(cut.get("slot_primitives"))
        cut_contribution = _coerce_nonnegative_float(cut.get("contribution_score"))
        cut_is_dead_slot = bool(cut.get("is_dead_slot"))
        cut_redundancy_excess_count = int(cut.get("redundancy_excess_count") or 0)

        deck_without_cut = _remove_one_card(deck_cards, cut_name)
        counts_without_cut = _copy_counts_with_cut(
            primitive_counts_by_id=primitive_counts_by_id,
            cut_primitives=cut_primitives,
        )

        baseline_coverage = _coverage_count(
            primitive_counts=counts_without_cut,
            target_primitives=target_primitives,
        )
        baseline_missing_required_count = _missing_required_count(
            primitive_counts=counts_without_cut,
            required_missing_primitives=required_missing_primitives,
        )

        for add in top_adds:
            if swap_evaluations_total >= _MAX_SWAP_EVALUATIONS:
                if isinstance(swap_filter_metrics_out, dict):
                    swap_filter_metrics_out["swaps_filtered_minbar_count"] = int(swaps_filtered_minbar_count)
                return swaps, swap_evaluations_total, _round6(swap_eval_ms_total)

            evaluation_started_at = perf_counter() if collect_dev_metrics else 0.0
            try:
                swap_evaluations_total += 1

                add_name = _nonempty_str(add.get("name"))
                if add_name == "" or add_name == cut_name:
                    continue

                add_oracle_id = _nonempty_str(add.get("oracle_id"))
                add_primitives = _clean_sorted_unique_strings(add.get("primitive_ids_v1"))

                color_verdict = is_card_color_legal_v1(
                    card_name=add_name,
                    commander_color_set=commander_color_set,
                    db_snapshot_id=db_snapshot_id,
                )
                if color_verdict != True:  # noqa: E712 - explicit bool check for deterministic tri-state API
                    continue

                gc_verdict = would_violate_gc_limit_v1(
                    candidate_card=add_name,
                    current_cards=deck_without_cut,
                    bracket_id=bracket_id,
                    db_snapshot_id=db_snapshot_id,
                )
                if gc_verdict == UNKNOWN_BRACKET_RULES:
                    continue
                if gc_verdict is True:
                    continue

                counts_after_swap = _copy_counts_with_add(
                    primitive_counts=counts_without_cut,
                    add_primitives=add_primitives,
                )
                coverage_after = _coverage_count(
                    primitive_counts=counts_after_swap,
                    target_primitives=target_primitives,
                )
                primitive_coverage_delta = int(coverage_after - baseline_coverage)
                missing_required_after = _missing_required_count(
                    primitive_counts=counts_after_swap,
                    required_missing_primitives=required_missing_primitives,
                )
                missing_required_count_delta = int(
                    baseline_missing_required_count - missing_required_after
                )

                add_contribution = _slot_contribution_score(add_primitives, counts_without_cut)
                dead_slot_delta = 0
                if cut_is_dead_slot:
                    dead_slot_delta += 1
                if len(add_primitives) == 0:
                    dead_slot_delta -= 1

                coherence_delta = float(dead_slot_delta) + float(add_contribution - cut_contribution)
                total_score_delta = float(coherence_delta) + float(primitive_coverage_delta)

                if total_score_delta < float(MIN_TOTAL_SCORE_DELTA_V1):
                    swaps_filtered_minbar_count += 1
                    continue

                if require_primitive_coverage_when_missing and int(primitive_coverage_delta) < 1:
                    swaps_filtered_minbar_count += 1
                    continue

                reasons: List[str] = []
                if cut_is_dead_slot:
                    reasons.append("CUT_DEAD_SLOT")
                if cut_redundancy_excess_count > 0:
                    reasons.append("CUT_REDUNDANCY_EXCESS")
                if primitive_coverage_delta > 0:
                    reasons.append("ADD_PRIMITIVE_COVERAGE")
                if protection_primitives_enabled and len(protection_primitive_set.intersection(set(add_primitives))) > 0:
                    reasons.append("ADD_PROTECTION_SUPPORT")
                reasons.append("GC_COMPLIANCE_PRESERVED")

                swaps.append(
                    {
                        "cut_name": cut_name,
                        "add_name": add_name,
                        "cut_oracle_id": cut_oracle_id,
                        "add_oracle_id": add_oracle_id,
                        "reasons_v1": sorted(set(reasons)),
                        "delta_summary_v1": {
                            "total_score_delta_v1": _round6(total_score_delta),
                            "coherence_delta_v1": _round6(coherence_delta),
                            "primitive_coverage_delta_v1": int(primitive_coverage_delta),
                            "missing_required_count_delta_v1": int(missing_required_count_delta),
                            "gc_compliance_preserved_v1": True,
                        },
                    }
                )
            finally:
                if collect_dev_metrics:
                    swap_eval_ms_total += max((perf_counter() - evaluation_started_at) * 1000.0, 0.0)

    if isinstance(swap_filter_metrics_out, dict):
        swap_filter_metrics_out["swaps_filtered_minbar_count"] = int(swaps_filtered_minbar_count)
    return swaps, swap_evaluations_total, _round6(swap_eval_ms_total)


def _attach_dev_metrics(
    *,
    payload: Dict[str, Any],
    collect_dev_metrics: bool,
    candidate_pool_ms: float,
    candidate_pool_count: int,
    swap_eval_count: int,
    swap_eval_ms_total: float,
    candidate_pool_breakdown_v1: Any = None,
    swap_selection_summary_v1: Any = None,
) -> Dict[str, Any]:
    if not collect_dev_metrics:
        return payload

    breakdown_payload = candidate_pool_breakdown_v1 if isinstance(candidate_pool_breakdown_v1, dict) else {}

    payload["dev_metrics_v1"] = {
        "candidate_pool_ms": _round6(_coerce_nonnegative_float(candidate_pool_ms)),
        "candidate_pool_count": max(int(candidate_pool_count), 0),
        "candidate_pool_breakdown_v1": {
            "sql_query_ms": _round6(_coerce_nonnegative_float(breakdown_payload.get("sql_query_ms"))),
            "python_filter_ms": _round6(_coerce_nonnegative_float(breakdown_payload.get("python_filter_ms"))),
            "color_check_ms": _round6(_coerce_nonnegative_float(breakdown_payload.get("color_check_ms"))),
            "gc_check_ms": _round6(_coerce_nonnegative_float(breakdown_payload.get("gc_check_ms"))),
            "total_candidates_seen": _coerce_nonnegative_int(breakdown_payload.get("total_candidates_seen")),
            "total_candidates_returned": _coerce_nonnegative_int(breakdown_payload.get("total_candidates_returned")),
        },
        "swap_eval_count": max(int(swap_eval_count), 0),
        "swap_eval_ms_total": _round6(_coerce_nonnegative_float(swap_eval_ms_total)),
        "evaluation_cap_hit": bool(max(int(swap_eval_count), 0) >= _MAX_SWAP_EVALUATIONS),
        "raw_pair_count": _coerce_nonnegative_int(
            (
                swap_selection_summary_v1.get("raw_pair_count")
                if isinstance(swap_selection_summary_v1, dict)
                else 0
            )
        ),
        "unique_add_count": _coerce_nonnegative_int(
            (
                swap_selection_summary_v1.get("unique_add_count")
                if isinstance(swap_selection_summary_v1, dict)
                else 0
            )
        ),
        "unique_cut_count": _coerce_nonnegative_int(
            (
                swap_selection_summary_v1.get("unique_cut_count")
                if isinstance(swap_selection_summary_v1, dict)
                else 0
            )
        ),
        "selected_count": _coerce_nonnegative_int(
            (
                swap_selection_summary_v1.get("selected_count")
                if isinstance(swap_selection_summary_v1, dict)
                else 0
            )
        ),
        "protected_cut_count": _coerce_nonnegative_int(
            (
                swap_selection_summary_v1.get("protected_cut_count")
                if isinstance(swap_selection_summary_v1, dict)
                else 0
            )
        ),
        "protected_cut_names_top10": (
            [
                token
                for token in (
                    _nonempty_str(value)
                    for value in (
                        swap_selection_summary_v1.get("protected_cut_names_top10")
                        if isinstance(swap_selection_summary_v1, dict)
                        and isinstance(swap_selection_summary_v1.get("protected_cut_names_top10"), list)
                        else []
                    )
                )
                if token != ""
            ][:10]
        ),
        "swaps_filtered_protected_count": _coerce_nonnegative_int(
            (
                swap_selection_summary_v1.get("swaps_filtered_protected_count")
                if isinstance(swap_selection_summary_v1, dict)
                else 0
            )
        ),
        "swaps_filtered_minbar_count": _coerce_nonnegative_int(
            (
                swap_selection_summary_v1.get("swaps_filtered_minbar_count")
                if isinstance(swap_selection_summary_v1, dict)
                else 0
            )
        ),
    }
    return payload


def _swap_delta_payload(row: Any) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    return row.get("delta_summary_v1") if isinstance(row.get("delta_summary_v1"), dict) else {}


def _swap_ordering_key(row: Any) -> Tuple[float, float, int, int, str, str, str, str]:
    delta = _swap_delta_payload(row)
    return (
        -_coerce_float(delta.get("total_score_delta_v1"), default=0.0),
        -_coerce_float(delta.get("coherence_delta_v1"), default=0.0),
        -_coerce_int(delta.get("primitive_coverage_delta_v1"), default=0),
        -_coerce_int(delta.get("missing_required_count_delta_v1"), default=0),
        _nonempty_str(row.get("cut_oracle_id") if isinstance(row, dict) else ""),
        _nonempty_str(row.get("cut_name") if isinstance(row, dict) else ""),
        _nonempty_str(row.get("add_oracle_id") if isinstance(row, dict) else ""),
        _nonempty_str(row.get("add_name") if isinstance(row, dict) else ""),
    )


def _swap_add_identity_key(row: Any) -> str:
    add_oracle_id = _nonempty_str(row.get("add_oracle_id") if isinstance(row, dict) else "")
    if add_oracle_id != "":
        return f"OID::{add_oracle_id}"
    add_name = _nonempty_str(row.get("add_name") if isinstance(row, dict) else "")
    if add_name != "":
        return f"NAME::{add_name}"
    return ""


def _swap_cut_identity_key(row: Any) -> str:
    cut_oracle_id = _nonempty_str(row.get("cut_oracle_id") if isinstance(row, dict) else "")
    if cut_oracle_id != "":
        return f"OID::{cut_oracle_id}"
    cut_name = _nonempty_str(row.get("cut_name") if isinstance(row, dict) else "")
    if cut_name != "":
        return f"NAME::{cut_name}"
    return ""


def _select_unique_swaps(
    *,
    candidate_swaps: List[Dict[str, Any]],
    max_swaps: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    valid_swaps: List[Dict[str, Any]] = []
    for row in candidate_swaps:
        if not isinstance(row, dict):
            continue
        if _nonempty_str(row.get("cut_name")) == "":
            continue
        if _nonempty_str(row.get("add_name")) == "":
            continue
        if _swap_add_identity_key(row) == "":
            continue
        if _swap_cut_identity_key(row) == "":
            continue
        valid_swaps.append(row)

    best_per_add: Dict[str, Dict[str, Any]] = {}
    for row in valid_swaps:
        add_key = _swap_add_identity_key(row)
        existing = best_per_add.get(add_key)
        if existing is None or _swap_ordering_key(row) < _swap_ordering_key(existing):
            best_per_add[add_key] = row

    collapsed_swaps = sorted(best_per_add.values(), key=_swap_ordering_key)

    used_add_keys: Set[str] = set()
    used_cut_keys: Set[str] = set()
    selected: List[Dict[str, Any]] = []

    max_swaps_clean = max(int(max_swaps), 0)
    for row in collapsed_swaps:
        if len(selected) >= max_swaps_clean:
            break
        add_key = _swap_add_identity_key(row)
        cut_key = _swap_cut_identity_key(row)
        if add_key in used_add_keys:
            continue
        if cut_key in used_cut_keys:
            continue
        used_add_keys.add(add_key)
        used_cut_keys.add(cut_key)
        selected.append(row)

    summary = {
        "raw_pair_count": int(len(candidate_swaps)),
        "unique_add_count": int(len(collapsed_swaps)),
        "unique_cut_count": int(len({_swap_cut_identity_key(row) for row in collapsed_swaps})),
        "selected_count": int(len(selected)),
    }
    return selected, summary


def run_deck_tune_engine_v1(
    *,
    canonical_deck_input: Any,
    baseline_build_result: Any,
    db_snapshot_id: str,
    bracket_id: str,
    profile_id: str,
    mulligan_model_id: str,
    max_swaps: int,
    collect_dev_metrics: bool = False,
) -> Dict[str, Any]:
    canonical_payload = canonical_deck_input if isinstance(canonical_deck_input, dict) else {}
    baseline_payload = baseline_build_result if isinstance(baseline_build_result, dict) else {}
    result_payload = _baseline_result_payload(baseline_payload)
    structural_snapshot = _extract_structural_snapshot(result_payload)
    primitive_counts_by_id = _extract_primitive_counts_by_id(structural_snapshot)

    profile_id_clean = _nonempty_str(profile_id)
    bracket_id_clean = _nonempty_str(bracket_id)
    mulligan_model_id_clean = _nonempty_str(mulligan_model_id)
    max_swaps_clean = _coerce_positive_int(max_swaps, default=5)

    baseline_summary_v1 = _build_baseline_summary(
        baseline_build_result=baseline_payload,
        result_payload=result_payload,
        structural_snapshot=structural_snapshot,
        profile_id=profile_id_clean,
        bracket_id=bracket_id_clean,
        mulligan_model_id=mulligan_model_id_clean,
    )

    baseline_status = _nonempty_str(baseline_payload.get("status"))
    if baseline_status not in {"OK", "WARN"}:
        return _attach_dev_metrics(
            payload={
                "version": VERSION,
                "status": "SKIP",
                "codes": ["BASELINE_BUILD_UNAVAILABLE"],
                "baseline_summary_v1": baseline_summary_v1,
                "recommended_swaps_v1": [],
                "evaluation_summary_v1": {
                    "cuts_considered": 0,
                    "adds_considered": 0,
                    "swap_evaluations_total": 0,
                },
            },
            collect_dev_metrics=collect_dev_metrics,
            candidate_pool_ms=0.0,
            candidate_pool_count=0,
            swap_eval_count=0,
            swap_eval_ms_total=0.0,
        )

    commander_name = _nonempty_str(canonical_payload.get("commander"))
    if commander_name == "":
        return _attach_dev_metrics(
            payload={
                "version": VERSION,
                "status": "SKIP",
                "codes": ["COMMANDER_MISSING"],
                "baseline_summary_v1": baseline_summary_v1,
                "recommended_swaps_v1": [],
                "evaluation_summary_v1": {
                    "cuts_considered": 0,
                    "adds_considered": 0,
                    "swap_evaluations_total": 0,
                },
            },
            collect_dev_metrics=collect_dev_metrics,
            candidate_pool_ms=0.0,
            candidate_pool_count=0,
            swap_eval_count=0,
            swap_eval_ms_total=0.0,
        )

    commander_color_identity = get_commander_color_identity_v1(
        db_snapshot_id=db_snapshot_id,
        commander_name=commander_name,
    )
    if commander_color_identity == COLOR_IDENTITY_UNAVAILABLE:
        return _attach_dev_metrics(
            payload={
                "version": VERSION,
                "status": "WARN",
                "codes": [COLOR_IDENTITY_UNAVAILABLE],
                "baseline_summary_v1": baseline_summary_v1,
                "recommended_swaps_v1": [],
                "evaluation_summary_v1": {
                    "cuts_considered": 0,
                    "adds_considered": 0,
                    "swap_evaluations_total": 0,
                },
            },
            collect_dev_metrics=collect_dev_metrics,
            candidate_pool_ms=0.0,
            candidate_pool_count=0,
            swap_eval_count=0,
            swap_eval_ms_total=0.0,
        )
    if not isinstance(commander_color_identity, set):
        return _attach_dev_metrics(
            payload={
                "version": VERSION,
                "status": "WARN",
                "codes": [UNKNOWN_COLOR_IDENTITY],
                "baseline_summary_v1": baseline_summary_v1,
                "recommended_swaps_v1": [],
                "evaluation_summary_v1": {
                    "cuts_considered": 0,
                    "adds_considered": 0,
                    "swap_evaluations_total": 0,
                },
            },
            collect_dev_metrics=collect_dev_metrics,
            candidate_pool_ms=0.0,
            candidate_pool_count=0,
            swap_eval_count=0,
            swap_eval_ms_total=0.0,
        )

    high_redundancy_primitives = set(
        _extract_redundancy_primitives(result_payload=result_payload, level="HIGH")
    )
    low_redundancy_primitives = _extract_redundancy_primitives(result_payload=result_payload, level="LOW")
    missing_primitives = _extract_missing_primitives(
        result_payload=result_payload,
        structural_snapshot=structural_snapshot,
        primitive_counts_by_id=primitive_counts_by_id,
    )

    protection_enabled = _needs_protection_candidates(result_payload)
    protection_primitives = list(_PROTECTION_PRIMITIVES) if protection_enabled else []

    include_primitives = sorted(
        set(missing_primitives).union(set(low_redundancy_primitives)).union(set(protection_primitives))
    )

    protect_top_k_cards = _resolve_protect_top_k_from_engine_patches(canonical_payload)

    cut_candidates = _extract_cut_candidates(
        canonical_deck_input=canonical_payload,
        result_payload=result_payload,
        primitive_counts_by_id=primitive_counts_by_id,
        high_redundancy_primitives=high_redundancy_primitives,
    )
    eligible_cut_candidates, protected_cut_candidates = _partition_protected_cut_candidates(
        cut_candidates,
        protect_top_k_cards=protect_top_k_cards,
    )

    deck_cards = _normalize_card_list(canonical_payload.get("cards"))
    exclude_card_names = [commander_name] + list(deck_cards)

    candidate_pool_breakdown_v1: Dict[str, Any] = {}
    candidate_pool_started_at = perf_counter() if collect_dev_metrics else 0.0
    add_candidates = get_candidate_pool_v1(
        db_snapshot_id=db_snapshot_id,
        include_primitives=include_primitives,
        exclude_card_names=exclude_card_names,
        commander_color_set=commander_color_identity,
        bracket_id=bracket_id_clean,
        limit=_TOP_ADD_LIMIT,
        dev_metrics_out=candidate_pool_breakdown_v1 if collect_dev_metrics else None,
    )
    candidate_pool_ms = (
        _round6(max((perf_counter() - candidate_pool_started_at) * 1000.0, 0.0))
        if collect_dev_metrics
        else 0.0
    )

    add_candidates_dedup: List[Dict[str, Any]] = []
    seen_add_keys: Set[Tuple[str, str]] = set()
    for row in add_candidates:
        if not isinstance(row, dict):
            continue
        key = (_nonempty_str(row.get("oracle_id")), _nonempty_str(row.get("name")))
        if key in seen_add_keys:
            continue
        seen_add_keys.add(key)
        add_candidates_dedup.append(row)

    swap_filter_metrics: Dict[str, Any] = {}
    candidate_swaps, swap_evaluations_total, swap_eval_ms_total = _evaluate_swap_pairs(
        db_snapshot_id=db_snapshot_id,
        bracket_id=bracket_id_clean,
        deck_cards=deck_cards,
        commander_color_set=commander_color_identity,
        cut_candidates=eligible_cut_candidates,
        add_candidates=add_candidates_dedup,
        primitive_counts_by_id=primitive_counts_by_id,
        target_primitives=set(include_primitives),
        required_missing_primitives=set(missing_primitives),
        protection_primitives_enabled=protection_enabled,
        collect_dev_metrics=collect_dev_metrics,
        swap_filter_metrics_out=swap_filter_metrics,
    )

    selected_swaps, swap_selection_summary = _select_unique_swaps(
        candidate_swaps=candidate_swaps,
        max_swaps=max_swaps_clean,
    )

    protected_cut_names_top10: List[str] = []
    protected_seen: Set[str] = set()
    for row in sorted(protected_cut_candidates, key=_cut_protection_rank_key):
        card_name = _nonempty_str(row.get("card_name"))
        if card_name == "" or card_name in protected_seen:
            continue
        protected_seen.add(card_name)
        protected_cut_names_top10.append(card_name)
        if len(protected_cut_names_top10) >= 10:
            break

    top_add_count = min(len(add_candidates_dedup), _TOP_ADD_LIMIT)
    top_all_cuts_count = min(len(cut_candidates), _TOP_CUT_LIMIT)
    top_eligible_cuts_count = min(len(eligible_cut_candidates), _TOP_CUT_LIMIT)
    swap_selection_summary["protected_cut_count"] = int(len(protected_cut_candidates))
    swap_selection_summary["protected_cut_names_top10"] = protected_cut_names_top10
    swap_selection_summary["swaps_filtered_protected_count"] = int(
        max((top_all_cuts_count - top_eligible_cuts_count) * top_add_count, 0)
    )
    swap_selection_summary["swaps_filtered_minbar_count"] = _coerce_nonnegative_int(
        swap_filter_metrics.get("swaps_filtered_minbar_count")
    )

    recommended_swaps_v1 = [
        {
            "cut_name": _nonempty_str(row.get("cut_name")),
            "add_name": _nonempty_str(row.get("add_name")),
            "reasons_v1": _clean_sorted_unique_strings(row.get("reasons_v1")),
            "delta_summary_v1": {
                "total_score_delta_v1": _coerce_nonnegative_float(
                    (
                        row.get("delta_summary_v1")
                        if isinstance(row.get("delta_summary_v1"), dict)
                        else {}
                    ).get("total_score_delta_v1")
                ),
                "coherence_delta_v1": float(
                    (
                        row.get("delta_summary_v1")
                        if isinstance(row.get("delta_summary_v1"), dict)
                        else {}
                    ).get("coherence_delta_v1")
                    or 0.0
                ),
                "primitive_coverage_delta_v1": int(
                    (
                        row.get("delta_summary_v1")
                        if isinstance(row.get("delta_summary_v1"), dict)
                        else {}
                    ).get("primitive_coverage_delta_v1")
                    or 0
                ),
                "gc_compliance_preserved_v1": bool(
                    (
                        row.get("delta_summary_v1")
                        if isinstance(row.get("delta_summary_v1"), dict)
                        else {}
                    ).get("gc_compliance_preserved_v1")
                ),
            },
        }
        for row in selected_swaps
        if _nonempty_str(row.get("cut_name")) != "" and _nonempty_str(row.get("add_name")) != ""
    ]

    status = "OK" if len(recommended_swaps_v1) > 0 else "WARN"

    return _attach_dev_metrics(
        payload={
            "version": VERSION,
            "status": status,
            "codes": [],
            "baseline_summary_v1": baseline_summary_v1,
            "recommended_swaps_v1": recommended_swaps_v1,
            "evaluation_summary_v1": {
                "cuts_considered": min(len(eligible_cut_candidates), _TOP_CUT_LIMIT),
                "adds_considered": min(len(add_candidates_dedup), _TOP_ADD_LIMIT),
                "swap_evaluations_total": int(swap_evaluations_total),
            },
        },
        collect_dev_metrics=collect_dev_metrics,
        candidate_pool_ms=candidate_pool_ms,
        candidate_pool_count=len(add_candidates_dedup),
        swap_eval_count=int(swap_evaluations_total),
        swap_eval_ms_total=swap_eval_ms_total,
        candidate_pool_breakdown_v1=candidate_pool_breakdown_v1,
        swap_selection_summary_v1=swap_selection_summary,
    )
