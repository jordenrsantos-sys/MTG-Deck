from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from api.engine.candidate_pool_v1 import get_candidate_pool_v1
from api.engine.color_identity_constraints_v1 import (
    COLOR_IDENTITY_UNAVAILABLE,
    UNKNOWN_COLOR_IDENTITY,
    get_commander_color_identity_union_v1,
)
from api.engine.constants import BASIC_NAMES, GENERIC_MINIMUMS, SNOW_BASIC_NAMES
from api.engine.layers.combo_enabler_reasons_v1 import attach_combo_enabler_reasons_v1
# v1.7.2 Stage 1 — deck-combo insight surfaces (detected_combos_v1 +
# missing_partners_v1). Independent of combo_enabler_reasons_v1; runs
# AFTER it so both surfaces see the final added-cards list.
from api.engine.layers.deck_combo_insights_v1 import compute_deck_combo_insights_v1
# v1.7.3 Stage 2 — proactive combo completion. Runs AFTER primitive-
# coverage backfill but BEFORE combo_enabler so the proactive adds
# get COMBO_ENABLER chips naturally + deck_combo_insights then
# reclassifies the pair as detected_combos (no longer missing).
from api.engine.layers.proactive_combo_completion_v1 import (
    PROACTIVE_COMBO_REASON_CODE,
    propose_proactive_combo_partners_v1,
)
# v1.7.5 — bracket-combo compliance check. Consumes detected_combos_v1
# (already populated by deck_combo_insights_v1) and emits violations_v1
# entries when bracket_id ∈ {B1, B2} and any 2-card combo is present in
# the final deck. Closes the gap where /deck/complete_v1 previously
# returned status:OK with zero violations even for bracket-illegal decks.
from api.engine.layers.complete_bracket_violations_v1 import (
    compute_complete_bracket_violations_v1,
)
# Phase 2.1a — deck theme classifier. Reads the brain's themes_v1_5 +
# typal_themes_v1_6 + signal vocabulary + confidence bands (all BYTE-IDENTICAL),
# evaluates them against the post-completion deck's primitive index, returns
# top-N classified themes for the UI to surface as a DeckThemesPanel.
from api.engine.layers.deck_theme_classifier_v1 import (
    classify_deck_themes_v1,
    compute_primitive_index_from_card_names,
    compute_subtype_counts_from_card_names,
)
# Phase 2.1b — THEME_SYNERGY reasons on added cards. Decorates the
# added_cards_v1 array's reasons_v1 with THEME_SYNERGY:<theme_id> entries
# attributing each card to the theme(s) it strengthens.
from api.engine.layers.theme_synergy_reasons_v1 import attach_theme_synergy_reasons_v1
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


def _face_variants(card_name: str) -> Set[str]:
    """Return all face-name variants of a card name.

    Scryfall stores DFCs / Adventures with a '// '-joined canonical name
    ('Decadent Dragon // Expensive Taste'). User-imported decks often
    contain only one face. Treat the full name AND each face as the same
    physical card so the singleton-dedupe at the completion step doesn't
    add a second copy under a different surface name.
    """
    variants: Set[str] = {card_name}
    if " // " in card_name:
        for face in card_name.split(" // "):
            face = face.strip()
            if face:
                variants.add(face)
    return variants


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


def _backfill_added_cards_from_diff(
    added_cards: List[Dict[str, Any]],
    deck_cards: List[str],
    working_cards: List[str],
) -> List[Dict[str, Any]]:
    """v1.5 Stage 3 — belt-and-suspenders backfill of added_cards_v1.

    If the engine grew `working_cards` beyond `deck_cards` but the
    accumulator `added_cards` is shorter than the growth delta, this
    helper backfills the missing entries via a multiset diff against
    the original input cards. Used as defense-in-depth so any current
    or future code path that grows the deck without explicit
    `added_cards.append(...)` updates can't cause downstream consumers
    (AddedCardsPanel, /deck/complete_v1 contract test) to render empty
    or break the length-equals-growth invariant.

    Preserves byte-identical behavior when `len(added_cards) ==
    (len(working_cards) - len(deck_cards))` (the common, already-correct
    path through the rounds + land_fill accumulator). Only synthesizes
    new entries when the accumulator under-counts; never overrides or
    deletes existing entries.

    Reason code for synthesized entries: `auto_completion_target_size`
    (v1.2 vocabulary). The exact code path that grew the deck without
    explicit accumulator tracking isn't recoverable from the final
    state, so the placeholder communicates "engine added this to reach
    target_deck_size during completion."
    """
    if not isinstance(added_cards, list):
        return []
    if not isinstance(deck_cards, list) or not isinstance(working_cards, list):
        return list(added_cards)

    growth_delta = len(working_cards) - len(deck_cards)
    if growth_delta <= 0 or len(added_cards) >= growth_delta:
        return list(added_cards)

    before_counts: Dict[str, int] = {}
    for name in deck_cards:
        key = _nonempty_str(name)
        if key == "":
            continue
        before_counts[key] = before_counts.get(key, 0) + 1

    after_counts: Dict[str, int] = {}
    for name in working_cards:
        key = _nonempty_str(name)
        if key == "":
            continue
        after_counts[key] = after_counts.get(key, 0) + 1

    represented: Dict[str, int] = {}
    for entry in added_cards:
        if not isinstance(entry, dict):
            continue
        name_value = entry.get("name")
        key = _nonempty_str(name_value) if isinstance(name_value, str) else ""
        if key == "":
            continue
        represented[key] = represented.get(key, 0) + 1

    backfilled: List[Dict[str, Any]] = list(added_cards)
    # Deterministic ordering: iterate after_counts keys in sorted order so
    # synthesized entries are reproducible across runs (HARD: determinism).
    for name in sorted(after_counts.keys()):
        after_n = after_counts[name]
        before_n = before_counts.get(name, 0)
        added_n = after_n - before_n
        if added_n <= 0:
            continue
        already_logged = represented.get(name, 0)
        missing = added_n - already_logged
        for _ in range(missing):
            backfilled.append(
                {
                    "name": name,
                    "reasons_v1": ["auto_completion_target_size"],
                    "primitives_added_v1": [],
                }
            )
    return backfilled


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
    # v1.2 Stage 1: accept OK_WITH_UNKNOWNS as a valid baseline status. Live-
    # retest 2026-05-10 confirmed /build returns this status for imported
    # decks (Archidekt Shelob 1010839) with some unresolved card names —
    # treating it as "BASELINE_BUILD_UNAVAILABLE" caused /deck/complete_v1
    # to return empty added_cards_v1 + empty completed_decklist_text_v1
    # even when the underlying baseline was structurally valid. The engine's
    # completion logic doesn't depend on every card resolving cleanly; the
    # canonical_deck_input + structural_snapshot it consumes are independent
    # of the unknown-card surface. Acceptable baseline statuses now mirror
    # /build's success-with-warnings vocabulary.
    if baseline_status not in {"OK", "WARN", "OK_WITH_UNKNOWNS"}:
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
    # Face-aware dedup set: 'Decadent Dragon // Expensive Taste' contributes
    # all three variants ({full, 'Decadent Dragon', 'Expensive Taste'}) so a
    # candidate offering 'Decadent Dragon' is recognized as a duplicate.
    working_card_variants: Set[str] = set()
    for _existing in working_cards:
        if isinstance(_existing, str) and _existing.strip():
            working_card_variants |= _face_variants(_existing)
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
            candidate_variants = _face_variants(name)
            if (not _is_singleton_exempt_name(name)) and (
                candidate_variants & working_card_variants
            ):
                continue
            primitive_ids = normalize_primitives_source(row.get("_primitive_ids"))
            _apply_primitive_counts(primitive_counts_by_id, primitive_ids)

            working_cards.append(name)
            working_card_variants |= candidate_variants
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

    # v1.5 Stage 3: belt-and-suspenders backfill — if the engine grew
    # working_cards beyond deck_cards but the added_cards accumulator
    # is shorter than the growth delta, synthesize entries via multiset
    # diff with the v1.2 vocabulary reason "auto_completion_target_size".
    # No-op when the accumulator is already correct (the common, byte-
    # identical path through rounds + land_fill).
    added_cards_final = _backfill_added_cards_from_diff(added_cards, deck_cards, working_cards)

    # v1.7.3 Stage 2 — proactive combo completion. Bracket-gated
    # (B1/B2 → 0; B3 → 1; B4 → 2; B5 → 3). For each proposal, append
    # a row to added_cards_final AND extend working_cards so the
    # downstream combo_enabler_reasons_v1 + compute_deck_combo_insights_v1
    # see the partner as present. Side-effect-free against any frozen
    # data (no bracket policy / combo pack / GC pool / primitive
    # coverage scoring writes).
    proactive_proposals = propose_proactive_combo_partners_v1(
        db_snapshot_id=db_snapshot_id,
        commander_names=commander_names,
        deck_cards=deck_cards,
        current_added_cards_v1=added_cards_final,
        bracket_id=bracket_id_clean,
        commander_color_identity=commander_color_identity,
    )
    for proposal in proactive_proposals:
        partner_name = proposal.get("partner_card_name")
        if not isinstance(partner_name, str) or partner_name == "":
            continue
        added_cards_final.append({
            "name": partner_name,
            "reasons_v1": [PROACTIVE_COMBO_REASON_CODE],
            "primitives_added_v1": [],
        })
        working_cards.append(partner_name)

    added_cards_final = attach_combo_enabler_reasons_v1(
        db_snapshot_id=db_snapshot_id,
        commander_names=commander_names,
        deck_cards=working_cards,
        added_cards_v1=added_cards_final,
    )

    # v1.7.2 Stage 1 — compute the deck's combo insight surfaces against
    # the FINAL completed deck (commander + working_cards, where
    # working_cards already includes the engine's adds). Cheap pure-Python
    # index lookups; no network, no DB writes.
    combo_insights = compute_deck_combo_insights_v1(
        db_snapshot_id=db_snapshot_id,
        commander_names=commander_names,
        deck_cards_after_completion=working_cards,
    )

    # v1.7.5 — bracket-combo compliance. Consumes detected_combos_v1 above.
    # When bracket is B1/B2 and any 2-card combo is detected, emit
    # violations_v1 entries and downgrade response.status to
    # BRACKET_VIOLATION so the UI surfaces the issue clearly.
    _bracket_violations_payload = compute_complete_bracket_violations_v1(
        bracket_id=bracket_id_clean,
        detected_combos_v1=combo_insights.get("detected_combos_v1") or [],
    )
    _violations_v1 = _bracket_violations_payload.get("violations_v1") or []
    _deck_status_override = _bracket_violations_payload.get("deck_status_override")
    _final_status = _deck_status_override if _deck_status_override else status

    # Phase 2.1a — theme classifier. Builds primitive_index_by_slot inline
    # from working_cards via db_cards (the Complete engine doesn't share
    # the build pipeline's pre-computed primitive index). Identifies the
    # deck's dominant themes (TRIBAL_GOBLINS, THEME_CONTROL, etc.) using
    # the brain's 41+78 theme classification system in BYTE-IDENTICAL data
    # files. Output `deck_themes_v1` lets the UI surface a DeckThemesPanel
    # AND lets v1.7.5+ stages annotate added_cards with THEME_SYNERGY
    # reasons attributing each card to the dominant theme it strengthens.
    _all_deck_cards: List[str] = list(commander_names) + list(working_cards)
    _primitive_index_for_themes = compute_primitive_index_from_card_names(
        db_snapshot_id, _all_deck_cards
    )
    _subtype_counts_for_themes = compute_subtype_counts_from_card_names(
        db_snapshot_id, _all_deck_cards
    )
    _deck_themes_v1 = classify_deck_themes_v1(
        primitive_index_by_slot=_primitive_index_for_themes,
        deck_subtype_counts=_subtype_counts_for_themes,
    )

    # Phase 2.1b — annotate added cards with THEME_SYNERGY:<theme_id> reasons.
    # Mutates added_cards_final in place; chain-safe with prior reason layers
    # (proactive_combo_completion, combo_enabler). When deck_themes_v1 is empty
    # (no classified themes), this is a no-op.
    added_cards_final = attach_theme_synergy_reasons_v1(
        added_cards_v1=added_cards_final,
        deck_themes_v1=_deck_themes_v1,
        db_snapshot_id=db_snapshot_id,
    )

    return _attach_dev_metrics({
        "version": VERSION,
        "status": _final_status,
        "codes": sorted(set(codes)),
        "baseline_summary_v1": baseline_summary_v1,
        "added_cards_v1": added_cards_final,
        "completed_decklist_text_v1": _build_completed_decklist_text(commander_names, working_cards),
        "detected_combos_v1": combo_insights.get("detected_combos_v1") or [],
        "missing_partners_v1": combo_insights.get("missing_partners_v1") or [],
        "violations_v1": _violations_v1,
        "deck_themes_v1": _deck_themes_v1,
    },
        collect_dev_metrics=bool(collect_dev_metrics),
        stop_reason_v1=stop_reason_v1,
        nonland_added_count=nonland_added_count,
        land_fill_needed=land_fill_needed,
        land_fill_applied=land_fill_applied,
        candidate_pool_last_returned=candidate_pool_last_returned,
        candidate_pool_filtered_illegal_count=candidate_pool_filtered_illegal_count,
    )
