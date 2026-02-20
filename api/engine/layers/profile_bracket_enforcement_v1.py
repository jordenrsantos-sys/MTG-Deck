from __future__ import annotations

from typing import Any, Dict, List

from api.engine.bracket_gc_limits import resolve_gc_limits
from api.engine.bracket_rules_v2 import resolve_bracket_rules_v2
from api.engine.constants import GAME_CHANGERS_SET, GAME_CHANGERS_VERSION
from api.engine.two_card_combos import detect_two_card_combos
from engine.db_tags import get_deck_tag_count


PROFILE_BRACKET_ENFORCEMENT_V1_VERSION = "profile_bracket_enforcement_v1_3"

_TRACKED_CATEGORIES = (
    "mass_land_denial",
    "extra_turn_chains",
    "two_card_combos",
)

_MASS_LAND_DENIAL_TAG_ID = "mass_land_denial"
_EXTRA_TURN_TAG_ID = "extra_turn"

_MISSING_SUPPORT_UNKNOWN_BY_CATEGORY = {
    "mass_land_denial": "MISSING_SUPPORT_MASS_LAND_DENIAL",
    "extra_turn_chains": "MISSING_SUPPORT_EXTRA_TURN_CHAINS",
    "two_card_combos": "MISSING_SUPPORT_TWO_CARD_COMBOS",
}

_DISALLOW_VIOLATION_CODE_BY_CATEGORY = {
    "mass_land_denial": "MASS_LAND_DENIAL_DISALLOWED",
    "extra_turn_chains": "EXTRA_TURN_CHAINS_DISALLOWED",
    "two_card_combos": "TWO_CARD_COMBOS_DISALLOWED",
}

# Deterministic, local definition bundles pinned by version.
# Keep this validation-only: no recommendations and no runtime inference.
_KNOWN_BRACKET_DEFINITION_VERSIONS = {"bracket_v0"}

_PROFILE_DEFINITIONS_BY_VERSION: Dict[str, Dict[str, Dict[str, Any]]] = {
    "profile_defaults_v1_10": {
        "default": {},
        "focused": {},
        "B3_default": {},
    },
}


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _clean_str_list(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    out: List[str] = []
    for item in values:
        token = _nonempty_str(item)
        if token is not None:
            out.append(token)
    return out


def _add_unknown(unknowns: List[Dict[str, str]], *, code: str, message: str) -> None:
    unknowns.append(
        {
            "code": str(code),
            "message": str(message),
        }
    )


def _add_violation(
    violations: List[Dict[str, Any]],
    *,
    code: str,
    message: str,
    category: str | None = None,
    card: str | None = None,
    limit: int | None = None,
    actual: int | None = None,
) -> None:
    violations.append(
        {
            "code": str(code),
            "message": str(message),
            "category": category,
            "card": card,
            "limit": limit,
            "actual": actual,
        }
    )


def _violation_sort_key(entry: Dict[str, Any]) -> tuple[str, str]:
    return (
        str(entry.get("code") or ""),
        str(entry.get("category") or ""),
    )


def _unknown_sort_key(entry: Dict[str, Any]) -> tuple[str]:
    return (str(entry.get("code") or ""),)


def _status_for(violations: List[Dict[str, Any]], unknowns: List[Dict[str, str]]) -> str:
    if violations:
        return "ERROR"
    if unknowns:
        return "WARN"
    return "OK"


def _game_changers_count(deck_cards: List[str], commander: str | None) -> int:
    combined = list(deck_cards)
    if commander is not None:
        combined.append(commander)
    return sum(1 for card_name in combined if card_name in GAME_CHANGERS_SET)


def _resolve_category_support_counts(
    deck_cards: List[str],
    commander: str | None,
    primitive_index_by_slot: Any,
    deck_slot_ids_playable: Any,
) -> Dict[str, Dict[str, Any]]:
    _ = commander
    category_support = {
        category: {
            "supported": False,
            "count": None,
        }
        for category in _TRACKED_CATEGORIES
    }

    if isinstance(primitive_index_by_slot, dict) and isinstance(deck_slot_ids_playable, list):
        category_support["mass_land_denial"] = {
            "supported": True,
            "count": get_deck_tag_count(
                primitive_index_by_slot=primitive_index_by_slot,
                deck_slot_ids=deck_slot_ids_playable,
                tag_id=_MASS_LAND_DENIAL_TAG_ID,
            ),
        }
        category_support["extra_turn_chains"] = {
            "supported": True,
            "count": get_deck_tag_count(
                primitive_index_by_slot=primitive_index_by_slot,
                deck_slot_ids=deck_slot_ids_playable,
                tag_id=_EXTRA_TURN_TAG_ID,
            ),
        }

    try:
        combo_detection = detect_two_card_combos(deck_cards)
        combo_count = combo_detection.get("count") if isinstance(combo_detection, dict) else None
        if isinstance(combo_count, int) and not isinstance(combo_count, bool) and combo_count >= 0:
            category_support["two_card_combos"] = {
                "supported": True,
                "count": combo_count,
            }
    except RuntimeError:
        pass

    return category_support


def _normalize_category_support(raw_support: Any, category: str) -> Dict[str, Any]:
    entry = raw_support.get(category) if isinstance(raw_support, dict) else None
    supported = False
    count: int | None = None

    if isinstance(entry, dict) and isinstance(entry.get("supported"), bool) and entry.get("supported"):
        candidate_count = entry.get("count")
        if isinstance(candidate_count, int) and not isinstance(candidate_count, bool) and candidate_count >= 0:
            supported = True
            count = int(candidate_count)

    return {
        "supported": supported,
        "count": count,
    }


def run_profile_bracket_enforcement_v1(
    deck_cards: list[str],
    commander: str,
    profile_id: str,
    bracket_id: str,
    game_changers_version: str,
    bracket_definition_version: str,
    profile_definition_version: str,
    primitive_index_by_slot: dict[str, list[str]] | None = None,
    deck_slot_ids_playable: list[str] | None = None,
) -> dict:
    deck_cards_clean = _clean_str_list(deck_cards)
    commander_clean = _nonempty_str(commander)

    profile_id_clean = _nonempty_str(profile_id) or ""
    bracket_id_clean = _nonempty_str(bracket_id) or ""
    game_changers_version_clean = _nonempty_str(game_changers_version) or ""
    bracket_definition_version_clean = _nonempty_str(bracket_definition_version) or ""
    profile_definition_version_clean = _nonempty_str(profile_definition_version) or ""

    deck_size_total = len(deck_cards_clean) + (1 if commander_clean is not None else 0)

    unknowns: List[Dict[str, str]] = []
    violations: List[Dict[str, Any]] = []

    if bracket_definition_version_clean not in _KNOWN_BRACKET_DEFINITION_VERSIONS:
        _add_unknown(
            unknowns,
            code="BRACKET_DEFINITION_VERSION_UNKNOWN",
            message=(
                "Cannot evaluate bracket constraints because bracket_definition_version is unrecognized."
            ),
        )

    profile_defs = _PROFILE_DEFINITIONS_BY_VERSION.get(profile_definition_version_clean)
    if not isinstance(profile_defs, dict):
        _add_unknown(
            unknowns,
            code="PROFILE_DEFINITION_VERSION_UNKNOWN",
            message=(
                "Cannot evaluate profile constraints because profile_definition_version is unrecognized."
            ),
        )

    gc_min, gc_max, gc_limits_version, bracket_unknown_for_gc = resolve_gc_limits(bracket_id_clean)

    bracket_rules, bracket_rules_version, bracket_unknown_for_rules = resolve_bracket_rules_v2(bracket_id_clean)
    bracket_unknown = bracket_unknown_for_gc or bracket_unknown_for_rules
    if bracket_unknown:
        _add_unknown(
            unknowns,
            code="UNKNOWN_BRACKET",
            message=(
                "Bracket id is not recognized; Game Changer limits are treated as unlimited."
            ),
        )

    if game_changers_version_clean != GAME_CHANGERS_VERSION:
        _add_unknown(
            unknowns,
            code="GAME_CHANGERS_VERSION_MISMATCH",
            message=(
                "Cannot evaluate Game Changers constraints because requested game_changers_version "
                "does not match local compiled version."
            ),
        )

    if not isinstance(GAME_CHANGERS_SET, set):
        _add_unknown(
            unknowns,
            code="GAME_CHANGERS_DEFINITION_INVALID",
            message="Cannot evaluate Game Changers constraints because local list is invalid.",
        )
    elif game_changers_version_clean == GAME_CHANGERS_VERSION and GAME_CHANGERS_VERSION == "gc_missing":
        _add_unknown(
            unknowns,
            code="GAME_CHANGERS_DEFINITION_MISSING",
            message="Cannot evaluate Game Changers constraints because local list is missing.",
        )

    if isinstance(profile_defs, dict) and profile_id_clean not in profile_defs:
        _add_unknown(
            unknowns,
            code="PROFILE_ID_UNKNOWN",
            message="Cannot evaluate profile constraints because profile_id is unrecognized.",
        )

    game_changers_in_deck = 0
    if isinstance(GAME_CHANGERS_SET, set):
        game_changers_in_deck = _game_changers_count(deck_cards_clean, commander_clean)

    if gc_min is not None and game_changers_in_deck < gc_min:
        _add_violation(
            violations,
            code="GAME_CHANGER_MIN_NOT_MET",
            message="Deck is below the minimum required Game Changers for this bracket.",
            category="game_changers",
            card=None,
            limit=gc_min,
            actual=game_changers_in_deck,
        )

    if gc_max is not None and game_changers_in_deck > gc_max:
        _add_violation(
            violations,
            code="GAME_CHANGER_MAX_EXCEEDED",
            message="Deck exceeds the maximum allowed Game Changers for this bracket.",
            category="game_changers",
            card=None,
            limit=gc_max,
            actual=game_changers_in_deck,
        )

    raw_category_support = _resolve_category_support_counts(
        deck_cards_clean,
        commander_clean,
        primitive_index_by_slot,
        deck_slot_ids_playable,
    )
    category_results: Dict[str, Dict[str, Any]] = {}
    for category in _TRACKED_CATEGORIES:
        support_info = _normalize_category_support(raw_category_support, category)
        supported = bool(support_info.get("supported"))
        count_value = support_info.get("count") if supported else None

        policy_value = None
        if not bracket_unknown and isinstance(bracket_rules, dict):
            policy_candidate = bracket_rules.get(category)
            if isinstance(policy_candidate, str):
                policy_value = policy_candidate

        category_results[category] = {
            "policy": policy_value,
            "count": count_value,
            "supported": supported,
        }

        if not supported:
            _add_unknown(
                unknowns,
                code=_MISSING_SUPPORT_UNKNOWN_BY_CATEGORY[category],
                message=(
                    "Deterministic tag/index support for this category is unavailable; count was not computed."
                ),
            )
            continue

        if policy_value == "DISALLOW" and isinstance(count_value, int) and count_value > 0:
            _add_violation(
                violations,
                code=_DISALLOW_VIOLATION_CODE_BY_CATEGORY[category],
                message="Bracket policy disallows this category when deterministic count is non-zero.",
                category=category,
            )

    violations_sorted = sorted(violations, key=_violation_sort_key)
    unknowns_sorted = sorted(unknowns, key=_unknown_sort_key)

    return {
        "version": PROFILE_BRACKET_ENFORCEMENT_V1_VERSION,
        "profile_id": profile_id_clean,
        "bracket_id": bracket_id_clean,
        "profile_definition_version": profile_definition_version_clean,
        "bracket_definition_version": bracket_definition_version_clean,
        "game_changers_version": game_changers_version_clean,
        "gc_limits_version": gc_limits_version,
        "bracket_rules_version": bracket_rules_version,
        "limits": {
            "min": gc_min,
            "max": gc_max,
        },
        "category_results": category_results,
        "status": _status_for(violations_sorted, unknowns_sorted),
        "counts": {
            "deck_size_total": deck_size_total,
            "game_changers_in_deck": game_changers_in_deck,
        },
        "violations": violations_sorted,
        "unknowns": unknowns_sorted,
    }
