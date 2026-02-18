import json
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

DB_PATH = Path(r"E:\mtg-engine\data\mtg.sqlite")

def connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con

def snapshot_exists(snapshot_id: str) -> bool:
    with connect() as con:
        row = con.execute(
            "SELECT 1 FROM snapshots WHERE snapshot_id = ? LIMIT 1",
            (snapshot_id,)
        ).fetchone()
        return row is not None

def find_card_by_name(snapshot_id: str, name: str) -> Optional[Dict[str, Any]]:
    with connect() as con:
        row = con.execute(
            "SELECT oracle_id, name, mana_cost, cmc, type_line, oracle_text, colors, color_identity, legalities_json, primitives_json "
            "FROM cards WHERE snapshot_id = ? AND LOWER(name) = LOWER(?) LIMIT 1",
            (snapshot_id, name)
        ).fetchone()
        card = dict(row) if row else None
        if card is not None:
            card["legalities"] = json.loads(card.get("legalities_json") or "{}")
            card["primitives"] = json.loads(card.get("primitives_json") or "[]")
        return card

def list_snapshots(limit: int = 20):
    with connect() as con:
        rows = con.execute(
            "SELECT snapshot_id, created_at, source, scryfall_bulk_updated_at "
            "FROM snapshots ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

def suggest_card_names(snapshot_id: str, query: str, limit: int = 5) -> list[str]:
    q = (query or "").strip().lower()
    if not q:
        return []

    # Deterministic: prefix first, then contains
    with connect() as con:
        prefix = con.execute(
            "SELECT name FROM cards "
            "WHERE snapshot_id = ? AND LOWER(name) LIKE ? "
            "ORDER BY name ASC LIMIT ?",
            (snapshot_id, q + "%", limit),
        ).fetchall()

        names = [r["name"] for r in prefix]

        if len(names) < limit:
            remaining = limit - len(names)
            contains = con.execute(
                "SELECT name FROM cards "
                "WHERE snapshot_id = ? AND LOWER(name) LIKE ? AND LOWER(name) NOT LIKE ? "
                "ORDER BY name ASC LIMIT ?",
                (snapshot_id, "%" + q + "%", q + "%", remaining),
            ).fetchall()
            names.extend([r["name"] for r in contains])

        return names

def is_legal_in_format(card: dict, fmt: str) -> tuple[bool, str]:
    legalities = card.get("legalities") or {}
    status = legalities.get(fmt)

    if status == "legal":
        return True, "legal"

    if status in ("banned", "not_legal"):
        return False, f"{card.get('name', 'Card')} is {status} in {fmt}"

    if status == "restricted":
        return False, f"{card.get('name', 'Card')} is restricted in {fmt}"

    return False, f"{card.get('name', 'Card')} legality unknown in {fmt}"

def is_legal_commander_card(card: Dict[str, Any]) -> tuple[bool, str]:
    """
    Deterministic Commander legality:
    - Legendary Creature (modern)
    - OR old 'Legend' type (older templating)
    - OR oracle text says it can be your commander
    """
    type_line = (card.get("type_line") or "").lower()
    oracle_text = (card.get("oracle_text") or "").lower()

    tl = type_line.replace("—", "-")

    if "legendary" in tl and "creature" in tl:
        return True, "OK_LEGENDARY_CREATURE"

    if "legend" in tl and "creature" in tl:
        return True, "OK_LEGEND_CREATURE"

    if "can be your commander" in oracle_text:
        return True, "OK_TEXT_ALLOWS_COMMANDER"

    ok, reason = is_legal_in_format(card, "commander")
    if not ok:
        return False, reason

    return False, "NOT_A_COMMANDER"


def commander_legality(snapshot_id: str, commander_name: str) -> tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Returns:
      (is_legal, reason_code, resolved_card_dict_or_none)
    """
    card = find_card_by_name(snapshot_id, commander_name)
    if card is None:
        return False, "UNKNOWN_COMMANDER", None

    ok, reason = is_legal_commander_card(card)
    if not ok:
        return False, "ILLEGAL_COMMANDER", card

    return True, reason, card

