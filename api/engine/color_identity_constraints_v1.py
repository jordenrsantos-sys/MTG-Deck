from __future__ import annotations

import json
from typing import Any, Iterable, Set

from engine.db import connect as cards_db_connect

VERSION = "color_identity_constraints_v1"
COLOR_IDENTITY_UNAVAILABLE = "COLOR_IDENTITY_UNAVAILABLE"
UNKNOWN_COLOR_IDENTITY = "UNKNOWN_COLOR_IDENTITY"

_ALLOWED_COLORS = frozenset({"W", "U", "B", "R", "G"})


def _normalize_color_set(value: Any) -> Set[str]:
    if isinstance(value, set):
        items: Iterable[Any] = value
    elif isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = value
    else:
        return set()

    out: Set[str] = set()
    for item in items:
        if not isinstance(item, str):
            continue
        token = item.strip().upper()
        if token in _ALLOWED_COLORS:
            out.add(token)
    return out


def _parse_color_identity_field(raw: Any) -> tuple[bool, Set[str]]:
    if isinstance(raw, list):
        parsed = raw
    elif isinstance(raw, str):
        stripped = raw.strip()
        if stripped == "":
            return False, set()
        try:
            parsed = json.loads(stripped)
        except (TypeError, ValueError):
            return False, set()
        if not isinstance(parsed, list):
            return False, set()
    else:
        return False, set()

    return True, _normalize_color_set(parsed)


def _fetch_color_identity(db_snapshot_id: str, card_name: str) -> tuple[bool, Set[str]]:
    snapshot_id = db_snapshot_id.strip() if isinstance(db_snapshot_id, str) else ""
    name = card_name.strip() if isinstance(card_name, str) else ""
    if snapshot_id == "" or name == "":
        return False, set()

    with cards_db_connect() as con:
        row = con.execute(
            """
            SELECT color_identity
            FROM cards
            WHERE snapshot_id = ?
              AND LOWER(name) = LOWER(?)
            ORDER BY oracle_id ASC, name ASC
            LIMIT 1
            """,
            (snapshot_id, name),
        ).fetchone()

    if row is None:
        return False, set()

    color_identity_raw = row[0] if isinstance(row, (tuple, list)) else row["color_identity"]
    return _parse_color_identity_field(color_identity_raw)


def get_commander_color_identity_v1(db_snapshot_id: str, commander_name: str) -> Set[str] | str:
    available, commander_colors = _fetch_color_identity(db_snapshot_id=db_snapshot_id, card_name=commander_name)
    if not available:
        return COLOR_IDENTITY_UNAVAILABLE
    return set(commander_colors)


def is_card_color_legal_v1(card_name: str, commander_color_set: Set[str], db_snapshot_id: str) -> bool | str:
    commander_colors = _normalize_color_set(commander_color_set)
    available, card_colors = _fetch_color_identity(db_snapshot_id=db_snapshot_id, card_name=card_name)
    if not available:
        return UNKNOWN_COLOR_IDENTITY
    return card_colors.issubset(commander_colors)
