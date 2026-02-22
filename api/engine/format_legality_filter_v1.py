from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Tuple

VERSION = "format_legality_filter_v1"

NON_DECK_TYPE_KEYWORDS_V1 = [
    "Token",
    "Emblem",
    "Scheme",
    "Conspiracy",
    "Plane",
    "Phenomenon",
    "Dungeon",
]

LEGALITY_SOURCE_COLUMNS_V1 = (
    "legal_commander",
    "is_legal_commander",
    "commander_legal",
    "commander_eligible",
    "can_be_commander",
    "legalities",
    "legalities_json",
)

TYPE_SOURCE_COLUMNS_V1 = (
    "layout",
    "type_line",
    "card_type",
)

_TRUE_LEGALITY_TOKENS_V1 = {
    "1",
    "allowed",
    "eligible",
    "legal",
    "ok",
    "true",
    "yes",
    "y",
}

_FALSE_LEGALITY_TOKENS_V1 = {
    "0",
    "banned",
    "forbidden",
    "illegal",
    "ineligible",
    "not_legal",
    "restricted",
    "false",
    "no",
    "n",
}

_NON_DECK_LAYOUT_HINTS_V1 = tuple(keyword.casefold() for keyword in NON_DECK_TYPE_KEYWORDS_V1)


def _nonempty_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    token = value.strip()
    if token == "":
        return None
    return token


def _normalize_column_set(columns: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for value in columns:
        if not isinstance(value, str):
            continue
        token = value.strip()
        if token == "":
            continue
        out.add(token)
    return out


def select_filter_columns_v1(cards_table_columns: Iterable[str]) -> List[str]:
    normalized = _normalize_column_set(cards_table_columns)
    out: List[str] = []
    for column in LEGALITY_SOURCE_COLUMNS_V1:
        if column in normalized:
            out.append(column)
    for column in TYPE_SOURCE_COLUMNS_V1:
        if column in normalized and column not in out:
            out.append(column)
    return out


def legality_filter_available_v1(cards_table_columns: Iterable[str]) -> bool:
    return len(select_filter_columns_v1(cards_table_columns)) > 0


def _coerce_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    if isinstance(value, str):
        token = value.strip().casefold()
        if token in _TRUE_LEGALITY_TOKENS_V1:
            return True
        if token in _FALSE_LEGALITY_TOKENS_V1:
            return False
    return None


def _parse_legalities_map(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return {}
    token = raw.strip()
    if token == "":
        return {}
    try:
        parsed = json.loads(token)
    except (TypeError, ValueError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _format_lookup_keys(format_name: str) -> Tuple[str, ...]:
    token = format_name.casefold()
    if token == "":
        return ("commander",)
    if token == "commander":
        return (
            "commander",
            "commander_legal",
            "legal_commander",
            "is_legal_commander",
            "commander_eligible",
            "can_be_commander",
        )
    compact = token.replace(" ", "").replace("_", "")
    return (token, compact)


def _resolve_legality_from_row(card_row: Dict[str, Any], format_name: str) -> Tuple[bool, bool | None]:
    lookup_keys = _format_lookup_keys(format_name)
    source_seen = False

    for column in LEGALITY_SOURCE_COLUMNS_V1:
        if column not in card_row:
            continue

        source_seen = True

        raw_value = card_row.get(column)

        if column in {
            "legal_commander",
            "is_legal_commander",
            "commander_legal",
            "commander_eligible",
            "can_be_commander",
        }:
            parsed_bool = _coerce_bool(raw_value)
            if parsed_bool is not None:
                return True, parsed_bool
            continue

        legalities_map = _parse_legalities_map(raw_value)
        if len(legalities_map) == 0:
            continue

        for key in lookup_keys:
            if key not in legalities_map:
                continue
            parsed_bool = _coerce_bool(legalities_map.get(key))
            if parsed_bool is not None:
                return True, parsed_bool

    if source_seen:
        return True, None

    return False, None


def _is_non_deck_layout(value: Any) -> bool:
    token = _nonempty_str(value)
    if token is None:
        return False
    lowered = token.casefold()
    return any(hint in lowered for hint in _NON_DECK_LAYOUT_HINTS_V1)


def _is_non_deck_type(value: Any) -> bool:
    token = _nonempty_str(value)
    if token is None:
        return False
    lowered = token.casefold()
    return any(keyword.casefold() in lowered for keyword in NON_DECK_TYPE_KEYWORDS_V1)


def is_deck_legal_card_v1(card_row: dict, format: str) -> tuple[bool, str | None]:
    if not isinstance(card_row, dict):
        return True, "LEG_FILTER_UNAVAILABLE"

    format_name = _nonempty_str(format) or "commander"

    legality_available, legality_allowed = _resolve_legality_from_row(card_row, format_name)
    if legality_available:
        if legality_allowed is True:
            return True, None
        return False, "ILLEGAL_BY_LEGALITIES"

    type_source_available = any(column in card_row for column in TYPE_SOURCE_COLUMNS_V1)
    if type_source_available:
        if _is_non_deck_layout(card_row.get("layout")):
            return False, "ILLEGAL_BY_TYPE"
        if _is_non_deck_type(card_row.get("type_line")):
            return False, "ILLEGAL_BY_TYPE"
        if _is_non_deck_type(card_row.get("card_type")):
            return False, "ILLEGAL_BY_TYPE"
        return True, None

    return True, "LEG_FILTER_UNAVAILABLE"
