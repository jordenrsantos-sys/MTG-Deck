from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from api.engine.candidate_pool_v1 import get_candidate_pool_v1
from api.engine.color_identity_constraints_v1 import (
    COLOR_IDENTITY_UNAVAILABLE,
    UNKNOWN_COLOR_IDENTITY,
    get_commander_color_identity_union_v1,
)
from api.engine.constants import BASIC_NAMES, GENERIC_MINIMUMS, SNOW_BASIC_NAMES
from api.engine.utils import normalize_primitives_source

VERSION = "deck_complete_engine_v1"

_COLOR_ORDER = ("W", "U", "B", "R", "G")
_COLOR_TO_BASIC = {
    "W": "Plains",
    "U": "Island",
    "B": "Swamp",
    "R": "Mountain",
    "G": "Forest",
}
_SINGLETON_EXEMPT_NAMES = set(BASIC_NAMES).union(set(SNOW_BASIC_NAMES)).union({"Wastes"})

_PROTECTION_PRIMITIVES = (
    "PROTECTION",
    "PROTECTION_COMBAT",
    "PROTECTION_STACK",
    "INDESTRUCTIBLE",
    "HEXPROOF",
    "WARD",
)

_INTERACTION_TOKENS = (
    "INTERACTION",
    "REMOVAL",
    "COUNTER",
    "BOARD_WIPE",
    "DISRUPT",
)

_PROTECTION_TOKENS = (
    "PROTECTION",
    "HEXPROOF",
    "WARD",
    "INDESTRUCTIBLE",
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


def _normalize_commander_name_list(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    out: List[str] = []
    seen: Set[str] = set()
    for value in values:
        token = _nonempty_str(value)
        if token == "":
            continue
        token_key = token.casefold()
        if token_key in seen:
            continue
        seen.add(token_key)
        out.append(token)
    return out


def _normalize_commander_colors(values: Any) -> Set[str]:
    if not isinstance(values, (set, list, tuple)):
        return set()
    out: Set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        token = value.strip().upper()
        if token in _COLOR_ORDER:
            out.add(token)
    return out


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


def _extract_missing_required_primitives(
    *,
    result_payload: Dict[str, Any],
    structural_snapshot: Dict[str, Any],
    primitive_counts_by_id: Dict[str, int],
) -> List[str]:
    missing_from_structural = _clean_sorted_unique_strings(structural_snapshot.get("missing_primitives_v1"))
    required_v0 = _clean_sorted_unique_strings(result_payload.get("required_primitives_v0"))
    missing_from_required = [
        primitive
        for primitive in required_v0
        if int(primitive_counts_by_id.get(primitive, 0)) <= 0
    ]
    return sorted(set(missing_from_structural).union(set(missing_from_required)))


def _extract_low_redundancy_primitives(result_payload: Dict[str, Any]) -> List[str]:
    redundancy_payload = (
        result_payload.get("redundancy_index_v1")
        if isinstance(result_payload.get("redundancy_index_v1"), dict)
        else {}
    )
    per_requirement = (
        redundancy_payload.get("per_requirement")
        if isinstance(redundancy_payload.get("per_requirement"), list)
        else []
    )

    out: List[str] = []
    for row in per_requirement:
        if not isinstance(row, dict):
            continue
        primitive = _nonempty_str(row.get("primitive"))
        if primitive == "":
            continue
        supported = bool(row.get("supported"))
        redundancy_level = _nonempty_str(row.get("redundancy_level"))
        if (not supported) or redundancy_level not in {"LOW", "NONE"}:
            continue
        out.append(primitive)
    return sorted(set(out))


def _needs_commander_support(result_payload: Dict[str, Any]) -> bool:
    commander_payload = (
        result_payload.get("commander_reliability_model_v1")
        if isinstance(result_payload.get("commander_reliability_model_v1"), dict)
        else {}
    )
    commander_dependent = bool(commander_payload.get("commander_dependent"))

    metrics = commander_payload.get("metrics") if isinstance(commander_payload.get("metrics"), dict) else {}
    protection_proxy = metrics.get("protection_coverage_proxy")
    protection_proxy_value = (
        float(protection_proxy)
        if isinstance(protection_proxy, (int, float)) and not isinstance(protection_proxy, bool)
        else 1.0
    )

    return commander_dependent or protection_proxy_value < 0.2


def _category_count(primitive_counts_by_id: Dict[str, int], tokens: Tuple[str, ...]) -> int:
    total = 0
    token_upper = tuple(token.upper() for token in tokens)
    for primitive, count in primitive_counts_by_id.items():
        primitive_upper = primitive.upper()
        if any(token in primitive_upper for token in token_upper):
            total += int(count)
    return int(total)


def _extract_interaction_protection_needs(primitive_counts_by_id: Dict[str, int]) -> List[str]:
    interaction_target = int(GENERIC_MINIMUMS.get("REMOVAL_SINGLE", 8)) + int(GENERIC_MINIMUMS.get("BOARD_WIPE", 2))
    protection_target = max(int(GENERIC_MINIMUMS.get("PROTECTION", 3)), 3)

    interaction_count = _category_count(primitive_counts_by_id, _INTERACTION_TOKENS)
    protection_count = _category_count(primitive_counts_by_id, _PROTECTION_TOKENS)

    needs: List[str] = []
    if interaction_count < interaction_target:
        needs.extend(["REMOVAL_SINGLE", "BOARD_WIPE", "STACK_COUNTERSPELL"])
    if protection_count < protection_target:
        needs.extend(list(_PROTECTION_PRIMITIVES))

    return sorted(set(_clean_sorted_unique_strings(needs)))


def _is_singleton_exempt_name(card_name: str) -> bool:
    return card_name in _SINGLETON_EXEMPT_NAMES


def _attach_dev_metrics(
    payload: Dict[str, Any],
    *,
    collect_dev_metrics: bool,
    stop_reason_v1: str,
    nonland_added_count: int,
    land_fill_needed: int,
    land_fill_applied: int,
    candidate_pool_last_returned: int,
    candidate_pool_filtered_illegal_count: int | None,
) -> Dict[str, Any]:
    if not collect_dev_metrics:
        return payload

    metrics: Dict[str, Any] = {
        "stop_reason_v1": _nonempty_str(stop_reason_v1),
        "nonland_added_count": int(max(nonland_added_count, 0)),
        "land_fill_needed": int(max(land_fill_needed, 0)),
        "land_fill_applied": int(max(land_fill_applied, 0)),
        "candidate_pool_last_returned": int(max(candidate_pool_last_returned, 0)),
    }
    if (
        isinstance(candidate_pool_filtered_illegal_count, int)
        and not isinstance(candidate_pool_filtered_illegal_count, bool)
        and int(candidate_pool_filtered_illegal_count) >= 0
    ):
        metrics["candidate_pool_filtered_illegal_count"] = int(candidate_pool_filtered_illegal_count)

    payload["dev_metrics_v1"] = metrics
    return payload


def _pick_round_additions(
    *,
    round_reason: str,
    include_primitives: List[str],
    db_snapshot_id: str,
    bracket_id: str,
    commander_names: List[str],
    commander_color_set: Set[str],
    current_cards: List[str],
    max_to_add: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    diagnostics: Dict[str, Any] = {
        "pool_called": False,
        "candidate_pool_returned_count": 0,
        "candidate_pool_filtered_illegal_count": None,
    }

    if max_to_add <= 0:
        return [], diagnostics

    include_primitives_clean = sorted(set(_clean_sorted_unique_strings(include_primitives)))
    if len(include_primitives_clean) == 0:
        return [], diagnostics

    candidate_limit = max(200, max_to_add * 20)
    candidate_pool_dev_metrics: Dict[str, Any] = {}
    candidate_pool = get_candidate_pool_v1(
        db_snapshot_id=db_snapshot_id,
        include_primitives=include_primitives_clean,
        exclude_card_names=list(commander_names) + list(current_cards),
        commander_color_set=commander_color_set,
        bracket_id=bracket_id,
        limit=candidate_limit,
        dev_metrics_out=candidate_pool_dev_metrics,
    )
    diagnostics["pool_called"] = True
    diagnostics["candidate_pool_returned_count"] = int(len(candidate_pool))
    filtered_illegal_count = candidate_pool_dev_metrics.get("filtered_illegal_count_v1")
    if isinstance(filtered_illegal_count, int) and not isinstance(filtered_illegal_count, bool):
        diagnostics["candidate_pool_filtered_illegal_count"] = int(filtered_illegal_count)

    include_set = set(include_primitives_clean)
    seen_names = set(current_cards)
    additions: List[Dict[str, Any]] = []

    for row in candidate_pool:
        if len(additions) >= max_to_add:
            break
        if not isinstance(row, dict):
            continue

        name = _nonempty_str(row.get("name"))
        if name == "":
            continue
        if (not _is_singleton_exempt_name(name)) and name in seen_names:
            continue

        primitive_ids = normalize_primitives_source(row.get("primitive_ids_v1"))
        primitives_added = sorted({primitive for primitive in primitive_ids if primitive in include_set})

        additions.append(
            {
                "name": name,
                "reasons_v1": sorted({"COMPLETE_TO_TARGET_SIZE", round_reason}),
                "primitives_added_v1": primitives_added,
                "_primitive_ids": primitive_ids,
            }
        )
        if not _is_singleton_exempt_name(name):
            seen_names.add(name)

    return additions, diagnostics


def _apply_primitive_counts(primitive_counts_by_id: Dict[str, int], primitive_ids: List[str]) -> None:
    for primitive in primitive_ids:
        primitive_clean = _nonempty_str(primitive)
        if primitive_clean == "":
            continue
        primitive_counts_by_id[primitive_clean] = int(primitive_counts_by_id.get(primitive_clean, 0)) + 1


def _build_land_fill_sequence(
    *,
    commander_color_set: Set[str],
    slots_needed: int,
) -> List[str]:
    if slots_needed <= 0:
        return []

    colors = [color for color in _COLOR_ORDER if color in commander_color_set]
    out: List[str] = []
    if len(colors) == 0:
        return ["Wastes"] * int(slots_needed)

    base = int(slots_needed) // len(colors)
    remainder = int(slots_needed) % len(colors)

    per_color_counts: Dict[str, int] = {
        color: int(base)
        for color in colors
    }
    for color in colors:
        if remainder <= 0:
            break
        per_color_counts[color] = int(per_color_counts.get(color, 0)) + 1
        remainder -= 1

    for color in colors:
        basic_name = _COLOR_TO_BASIC[color]
        copies = int(per_color_counts.get(color, 0))
        if copies <= 0:
            continue
        out.extend([basic_name] * copies)

    return out


def _build_completed_decklist_text(commander_names: List[str], deck_cards: List[str]) -> str:
    lines: List[str] = ["Commander"]
    for commander_name in commander_names:
        token = _nonempty_str(commander_name)
        if token == "":
            continue
        lines.append(f"1 {token}")
    lines.append("Deck")
    for card_name in deck_cards:
        token = _nonempty_str(card_name)
        if token == "":
            continue
        lines.append(f"1 {token}")
    return "\n".join(lines)


def run_deck_complete_engine_v1(
    *,
    canonical_deck_input: Any,
    baseline_build_result: Any,
    db_snapshot_id: str,
    bracket_id: str,
    profile_id: str,
    mulligan_model_id: str,
    target_deck_size: int,
    max_adds: int,
    allow_basic_lands: bool,
    land_target_mode: str,
    collect_dev_metrics: bool = False,
) -> Dict[str, Any]:
    canonical_payload = canonical_deck_input if isinstance(canonical_deck_input, dict) else {}
    baseline_payload = baseline_build_result if isinstance(baseline_build_result, dict) else {}
    result_payload = baseline_payload.get("result") if isinstance(baseline_payload.get("result"), dict) else {}
    structural_snapshot = _extract_structural_snapshot(result_payload)

    profile_id_clean = _nonempty_str(profile_id)
    bracket_id_clean = _nonempty_str(bracket_id)
    mulligan_model_id_clean = _nonempty_str(mulligan_model_id)

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
        return {
            "version": VERSION,
            "status": "SKIP",
            "codes": ["BASELINE_BUILD_UNAVAILABLE"],
            "baseline_summary_v1": baseline_summary_v1,
            "added_cards_v1": [],
            "completed_decklist_text_v1": "",
        }

    commander_name = _nonempty_str(canonical_payload.get("commander"))
    commander_names = _normalize_commander_name_list(canonical_payload.get("commander_list_v1"))
    if commander_name != "" and commander_name.casefold() not in {token.casefold() for token in commander_names}:
        commander_names.insert(0, commander_name)

    if len(commander_names) == 0:
        return {
            "version": VERSION,
            "status": "SKIP",
            "codes": ["COMMANDER_MISSING"],
            "baseline_summary_v1": baseline_summary_v1,
            "added_cards_v1": [],
            "completed_decklist_text_v1": "",
        }

    deck_cards = _normalize_card_list(canonical_payload.get("cards"))
    target_deck_size_clean = _coerce_positive_int(target_deck_size, default=100)
    current_total = len(commander_names) + len(deck_cards)
    slots_needed = max(target_deck_size_clean - current_total, 0)

    commander_color_identity = get_commander_color_identity_union_v1(
        db_snapshot_id=db_snapshot_id,
        commander_names=commander_names,
    )
    commander_color_identity_warn_code = ""
    if commander_color_identity == COLOR_IDENTITY_UNAVAILABLE:
        commander_color_identity_warn_code = COLOR_IDENTITY_UNAVAILABLE
        commander_colors = set()
    elif not isinstance(commander_color_identity, set):
        commander_color_identity_warn_code = UNKNOWN_COLOR_IDENTITY
        commander_colors = set()
    else:
        commander_colors = _normalize_commander_colors(commander_color_identity)
    max_adds_clean = _coerce_positive_int(max_adds, default=30)

    if slots_needed <= 0:
        return _attach_dev_metrics({
            "version": VERSION,
            "status": "OK",
            "codes": [],
            "baseline_summary_v1": baseline_summary_v1,
            "added_cards_v1": [],
            "completed_decklist_text_v1": _build_completed_decklist_text(commander_names, deck_cards),
        },
            collect_dev_metrics=bool(collect_dev_metrics),
            stop_reason_v1="OK_REACHED_TARGET",
            nonland_added_count=0,
            land_fill_needed=0,
            land_fill_applied=0,
            candidate_pool_last_returned=0,
            candidate_pool_filtered_illegal_count=None,
        )

    add_budget = min(slots_needed, max_adds_clean)

    primitive_counts_by_id = _extract_primitive_counts_by_id(structural_snapshot)

    round_required_needs = _extract_missing_required_primitives(
        result_payload=result_payload,
        structural_snapshot=structural_snapshot,
        primitive_counts_by_id=primitive_counts_by_id,
    )

    round_redundancy_needs = _extract_low_redundancy_primitives(result_payload)
    if _needs_commander_support(result_payload):
        round_redundancy_needs = sorted(set(round_redundancy_needs).union(set(_PROTECTION_PRIMITIVES)))

    round_interaction_needs = _extract_interaction_protection_needs(primitive_counts_by_id)

    rounds = [
        ("ADD_REQUIRED_COVERAGE", round_required_needs),
        ("ADD_REDUNDANCY_SUPPORT", round_redundancy_needs),
        ("ADD_INTERACTION_OR_PROTECTION", round_interaction_needs),
    ]

    remaining_budget = int(add_budget)
    working_cards = list(deck_cards)
    added_cards: List[Dict[str, Any]] = []
    nonland_added_count = 0
    nonland_pool_attempted = False
    candidate_pool_empty_seen = False
    candidate_pool_last_returned = 0
    candidate_pool_filtered_illegal_count: int | None = None

    for round_reason, include_primitives in rounds:
        if remaining_budget <= 0:
            break
        additions_for_round, diagnostics = _pick_round_additions(
            round_reason=round_reason,
            include_primitives=include_primitives,
            db_snapshot_id=db_snapshot_id,
            bracket_id=bracket_id_clean,
            commander_names=commander_names,
            commander_color_set=commander_colors,
            current_cards=working_cards,
            max_to_add=remaining_budget,
        )

        if bool(diagnostics.get("pool_called")):
            nonland_pool_attempted = True
            candidate_pool_last_returned = int(diagnostics.get("candidate_pool_returned_count") or 0)
            if candidate_pool_last_returned <= 0:
                candidate_pool_empty_seen = True
            filtered_illegal = diagnostics.get("candidate_pool_filtered_illegal_count")
            if isinstance(filtered_illegal, int) and not isinstance(filtered_illegal, bool):
                candidate_pool_filtered_illegal_count = int(filtered_illegal)

        for row in additions_for_round:
            if remaining_budget <= 0:
                break
            name = _nonempty_str(row.get("name"))
            if name == "":
                continue
            if (not _is_singleton_exempt_name(name)) and name in working_cards:
                continue
            primitive_ids = normalize_primitives_source(row.get("_primitive_ids"))
            _apply_primitive_counts(primitive_counts_by_id, primitive_ids)

            working_cards.append(name)
            added_cards.append(
                {
                    "name": name,
                    "reasons_v1": _clean_sorted_unique_strings(row.get("reasons_v1")),
                    "primitives_added_v1": _clean_sorted_unique_strings(row.get("primitives_added_v1")),
                }
            )
            remaining_budget -= 1
            nonland_added_count += 1

    allow_basic_lands_clean = bool(allow_basic_lands)
    land_target_mode_clean = _nonempty_str(land_target_mode).upper()
    if land_target_mode_clean == "":
        land_target_mode_clean = "AUTO"
    auto_land_fill_enabled = allow_basic_lands_clean and land_target_mode_clean == "AUTO"

    land_fill_needed = max(target_deck_size_clean - (len(commander_names) + len(working_cards)), 0)
    land_fill_applied = 0
    if land_fill_needed > 0 and auto_land_fill_enabled:
        land_fill_names = _build_land_fill_sequence(
            commander_color_set=commander_colors,
            slots_needed=land_fill_needed,
        )

        for land_name in land_fill_names:
            card_name = _nonempty_str(land_name)
            if card_name == "":
                continue

            working_cards.append(card_name)
            added_cards.append(
                {
                    "name": card_name,
                    "reasons_v1": ["ADD_BASIC_LAND_FILL_AUTO", "COMPLETE_TO_TARGET_SIZE"],
                    "primitives_added_v1": [],
                }
            )
            land_fill_applied += 1

    target_reached = (len(commander_names) + len(working_cards)) >= target_deck_size_clean
    status = "OK" if target_reached else "WARN"

    stop_reason_v1 = "OK_REACHED_TARGET"
    if target_reached and land_fill_applied > 0:
        stop_reason_v1 = "LAND_FILL_APPLIED"

    codes: List[str] = []
    if not target_reached:
        if commander_color_identity_warn_code != "":
            codes.append(commander_color_identity_warn_code)
        if nonland_pool_attempted and candidate_pool_empty_seen:
            codes.append("CANDIDATE_POOL_EMPTY")
        if not allow_basic_lands_clean and int(land_fill_needed) > 0:
            codes.append("BASIC_LANDS_DISALLOWED")
        elif allow_basic_lands_clean and land_target_mode_clean != "AUTO" and int(land_fill_needed) > 0:
            codes.append("LAND_MODE_DISABLED")

        if int(land_fill_applied) <= 0 and auto_land_fill_enabled and int(land_fill_needed) > 0:
            codes.append("LAND_FILL_FAILED")
        if int(max_adds_clean) < int(slots_needed) and int(nonland_added_count) >= int(add_budget):
            codes.append("MAX_ADDS_REACHED_BEFORE_TARGET")
        codes.append("TARGET_SIZE_NOT_REACHED")

        if "BASIC_LANDS_DISALLOWED" in codes:
            stop_reason_v1 = "BASIC_LANDS_DISALLOWED"
        elif "MAX_ADDS_REACHED_BEFORE_TARGET" in codes:
            stop_reason_v1 = "MAX_ADDS_REACHED_BEFORE_TARGET"
        elif "CANDIDATE_POOL_EMPTY" in codes:
            stop_reason_v1 = "CANDIDATE_POOL_EMPTY"
        elif "LAND_MODE_DISABLED" in codes:
            stop_reason_v1 = "LAND_MODE_DISABLED"
        else:
            stop_reason_v1 = "FILL_FAILED"

    return _attach_dev_metrics({
        "version": VERSION,
        "status": status,
        "codes": sorted(set(codes)),
        "baseline_summary_v1": baseline_summary_v1,
        "added_cards_v1": added_cards,
        "completed_decklist_text_v1": _build_completed_decklist_text(commander_names, working_cards),
    },
        collect_dev_metrics=bool(collect_dev_metrics),
        stop_reason_v1=stop_reason_v1,
        nonland_added_count=nonland_added_count,
        land_fill_needed=land_fill_needed,
        land_fill_applied=land_fill_applied,
        candidate_pool_last_returned=candidate_pool_last_returned,
        candidate_pool_filtered_illegal_count=candidate_pool_filtered_illegal_count,
    )
