from __future__ import annotations

from typing import Any, List

from api.engine.bracket_gc_limits import resolve_gc_limits
from api.engine.constants import GAME_CHANGERS_SET
from engine.db import connect as cards_db_connect

VERSION = "bracket_gc_enforcement_v1"
UNKNOWN_BRACKET_RULES = "UNKNOWN_BRACKET_RULES"


def _clean_card_names(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    out: List[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        token = value.strip()
        if token == "":
            continue
        out.append(token)
    return sorted(out)


def _snapshot_exists(snapshot_id: str) -> bool:
    token = snapshot_id.strip() if isinstance(snapshot_id, str) else ""
    if token == "":
        return False

    with cards_db_connect() as con:
        row = con.execute(
            "SELECT 1 FROM snapshots WHERE snapshot_id = ? LIMIT 1",
            (token,),
        ).fetchone()
    return row is not None


def count_game_changers_v1(card_names: List[str], db_snapshot_id: str) -> int:
    if not _snapshot_exists(db_snapshot_id):
        return 0

    card_names_clean = _clean_card_names(card_names)
    return sum(1 for name in card_names_clean if name in GAME_CHANGERS_SET)


def would_violate_gc_limit_v1(
    candidate_card: str,
    current_cards: List[str],
    bracket_id: str,
    db_snapshot_id: str,
) -> bool | str:
    candidate = candidate_card.strip() if isinstance(candidate_card, str) else ""
    if candidate == "":
        return False

    if candidate not in GAME_CHANGERS_SET:
        return False

    if not _snapshot_exists(db_snapshot_id):
        return False

    bracket_token = bracket_id.strip() if isinstance(bracket_id, str) else ""
    try:
        _, max_allowed, _, unknown_flag = resolve_gc_limits(bracket_token)
    except RuntimeError:
        return UNKNOWN_BRACKET_RULES

    if unknown_flag:
        return UNKNOWN_BRACKET_RULES

    if max_allowed is None:
        return False

    projected_cards = _clean_card_names(current_cards) + [candidate]
    projected_count = count_game_changers_v1(projected_cards, db_snapshot_id=db_snapshot_id)
    return projected_count > int(max_allowed)
