from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from api.engine.candidate_pool_v1 import get_candidate_pool_v1
from api.engine.color_identity_constraints_v1 import (
    COLOR_IDENTITY_UNAVAILABLE,
    UNKNOWN_COLOR_IDENTITY,
    get_commander_color_identity_v1,
    is_card_color_legal_v1,
)
from api.engine.constants import BASIC_NAMES, GAME_CHANGERS_SET, GENERIC_MINIMUMS, SNOW_BASIC_NAMES
from api.engine.utils import normalize_primitives_source
from engine.db import connect as cards_db_connect

VERSION = "deck_complete_engine_v1"

_COLOR_ORDER = ("W", "U", "B", "R", "G")
_COLOR_TO_BASIC = {
    "W": "Plains",
    "U": "Island",
    "B": "Swamp",
    "R": "Mountain",
    "G": "Forest",
}
_COLOR_TO_SNOW_BASIC = {
    "W": "Snow-Covered Plains",
    "U": "Snow-Covered Island",
    "B": "Snow-Covered Swamp",
    "R": "Snow-Covered Mountain",
    "G": "Snow-Covered Forest",
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


def _pick_round_additions(
    *,
    round_reason: str,
    include_primitives: List[str],
    db_snapshot_id: str,
    bracket_id: str,
    commander_name: str,
    commander_color_set: Set[str],
    current_cards: List[str],
    max_to_add: int,
) -> List[Dict[str, Any]]:
    if max_to_add <= 0:
        return []
    include_primitives_clean = sorted(set(_clean_sorted_unique_strings(include_primitives)))
    if len(include_primitives_clean) == 0:
        return []

    candidate_limit = max(200, max_to_add * 20)
    candidate_pool = get_candidate_pool_v1(
        db_snapshot_id=db_snapshot_id,
        include_primitives=include_primitives_clean,
        exclude_card_names=[commander_name] + list(current_cards),
        commander_color_set=commander_color_set,
        bracket_id=bracket_id,
        limit=candidate_limit,
    )

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

    return additions


def _apply_primitive_counts(primitive_counts_by_id: Dict[str, int], primitive_ids: List[str]) -> None:
    for primitive in primitive_ids:
        primitive_clean = _nonempty_str(primitive)
        if primitive_clean == "":
            continue
        primitive_counts_by_id[primitive_clean] = int(primitive_counts_by_id.get(primitive_clean, 0)) + 1


def _resolve_existing_land_name_map(db_snapshot_id: str, candidate_names: List[str]) -> Dict[str, str]:
    snapshot_id = _nonempty_str(db_snapshot_id)
    if snapshot_id == "":
        return {}

    name_keys = sorted({name.lower() for name in candidate_names if isinstance(name, str) and name.strip() != ""})
    if len(name_keys) == 0:
        return {}

    placeholders = ",".join("?" for _ in name_keys)
    rows = []
    with cards_db_connect() as con:
        rows = con.execute(
            (
                "SELECT name "
                "FROM cards "
                "WHERE snapshot_id = ? "
                f"AND LOWER(name) IN ({placeholders}) "
                "ORDER BY oracle_id ASC, name ASC"
            ),
            (snapshot_id, *name_keys),
        ).fetchall()

    out: Dict[str, str] = {}
    for row in rows:
        name = row["name"] if isinstance(row["name"], str) else ""
        if name == "":
            continue
        key = name.lower()
        if key in out:
            continue
        out[key] = name
    return out


def _build_land_fill_sequence(
    *,
    db_snapshot_id: str,
    commander_color_set: Set[str],
    current_cards: List[str],
    slots_needed: int,
) -> List[str]:
    if slots_needed <= 0:
        return []

    colors = [color for color in _COLOR_ORDER if color in commander_color_set]
    preferred_name_keys: List[str] = []
    for color in colors:
        preferred_name_keys.append(_COLOR_TO_BASIC[color])
        preferred_name_keys.append(_COLOR_TO_SNOW_BASIC[color])
    preferred_name_keys.append("Wastes")

    resolved_names = _resolve_existing_land_name_map(db_snapshot_id=db_snapshot_id, candidate_names=preferred_name_keys)

    counts: Dict[str, int] = {}
    for name in current_cards:
        clean = _nonempty_str(name)
        if clean == "":
            continue
        counts[clean] = int(counts.get(clean, 0)) + 1

    cycle: List[str] = []
    for color in colors:
        regular_default = _COLOR_TO_BASIC[color]
        snow_default = _COLOR_TO_SNOW_BASIC[color]
        regular_name = resolved_names.get(regular_default.lower(), regular_default)
        snow_name = resolved_names.get(snow_default.lower(), snow_default)

        regular_count = int(counts.get(regular_name, 0))
        snow_count = int(counts.get(snow_name, 0))
        cycle.append(snow_name if snow_count > regular_count else regular_name)

    if len(cycle) == 0:
        cycle = [resolved_names.get("wastes", "Wastes")]

    out: List[str] = []
    while len(out) < slots_needed:
        out.append(cycle[len(out) % len(cycle)])
    return out


def _build_completed_decklist_text(commander_name: str, deck_cards: List[str]) -> str:
    lines: List[str] = ["Commander", f"1 {commander_name}", "Deck"]
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
    if commander_name == "":
        return {
            "version": VERSION,
            "status": "SKIP",
            "codes": ["COMMANDER_MISSING"],
            "baseline_summary_v1": baseline_summary_v1,
            "added_cards_v1": [],
            "completed_decklist_text_v1": "",
        }

    commander_color_identity = get_commander_color_identity_v1(
        db_snapshot_id=db_snapshot_id,
        commander_name=commander_name,
    )
    if commander_color_identity == COLOR_IDENTITY_UNAVAILABLE:
        return {
            "version": VERSION,
            "status": "WARN",
            "codes": [COLOR_IDENTITY_UNAVAILABLE],
            "baseline_summary_v1": baseline_summary_v1,
            "added_cards_v1": [],
            "completed_decklist_text_v1": _build_completed_decklist_text(commander_name, _normalize_card_list(canonical_payload.get("cards"))),
        }
    if not isinstance(commander_color_identity, set):
        return {
            "version": VERSION,
            "status": "WARN",
            "codes": [UNKNOWN_COLOR_IDENTITY],
            "baseline_summary_v1": baseline_summary_v1,
            "added_cards_v1": [],
            "completed_decklist_text_v1": _build_completed_decklist_text(commander_name, _normalize_card_list(canonical_payload.get("cards"))),
        }

    commander_colors = _normalize_commander_colors(commander_color_identity)

    deck_cards = _normalize_card_list(canonical_payload.get("cards"))
    target_deck_size_clean = _coerce_positive_int(target_deck_size, default=100)
    max_adds_clean = _coerce_positive_int(max_adds, default=30)

    current_total = 1 + len(deck_cards)
    slots_needed = max(target_deck_size_clean - current_total, 0)
    add_budget = min(slots_needed, max_adds_clean)

    if add_budget <= 0:
        return {
            "version": VERSION,
            "status": "OK",
            "codes": [],
            "baseline_summary_v1": baseline_summary_v1,
            "added_cards_v1": [],
            "completed_decklist_text_v1": _build_completed_decklist_text(commander_name, deck_cards),
        }

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

    for round_reason, include_primitives in rounds:
        if remaining_budget <= 0:
            break
        additions_for_round = _pick_round_additions(
            round_reason=round_reason,
            include_primitives=include_primitives,
            db_snapshot_id=db_snapshot_id,
            bracket_id=bracket_id_clean,
            commander_name=commander_name,
            commander_color_set=commander_colors,
            current_cards=working_cards,
            max_to_add=remaining_budget,
        )

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

    land_mode = _nonempty_str(land_target_mode).upper()
    if remaining_budget > 0 and bool(allow_basic_lands) and land_mode == "AUTO":
        land_fill_names = _build_land_fill_sequence(
            db_snapshot_id=db_snapshot_id,
            commander_color_set=commander_colors,
            current_cards=working_cards,
            slots_needed=remaining_budget,
        )

        for land_name in land_fill_names:
            if remaining_budget <= 0:
                break
            card_name = _nonempty_str(land_name)
            if card_name == "":
                continue

            color_legality = is_card_color_legal_v1(
                card_name=card_name,
                commander_color_set=commander_colors,
                db_snapshot_id=db_snapshot_id,
            )
            if color_legality is not True:
                continue

            if card_name in GAME_CHANGERS_SET:
                continue

            if (not _is_singleton_exempt_name(card_name)) and card_name in working_cards:
                continue

            working_cards.append(card_name)
            added_cards.append(
                {
                    "name": card_name,
                    "reasons_v1": ["ADD_BASIC_LAND_FILL_AUTO", "COMPLETE_TO_TARGET_SIZE"],
                    "primitives_added_v1": [],
                }
            )
            remaining_budget -= 1

    target_reached = (1 + len(working_cards)) >= target_deck_size_clean
    status = "OK" if target_reached else "WARN"

    codes: List[str] = []
    if not target_reached:
        if int(max_adds_clean) < int(slots_needed):
            codes.append("MAX_ADDS_REACHED")
        codes.append("TARGET_SIZE_NOT_REACHED")

    return {
        "version": VERSION,
        "status": status,
        "codes": sorted(set(codes)),
        "baseline_summary_v1": baseline_summary_v1,
        "added_cards_v1": added_cards,
        "completed_decklist_text_v1": _build_completed_decklist_text(commander_name, working_cards),
    }
