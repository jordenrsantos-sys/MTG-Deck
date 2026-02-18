from typing import Any, Dict, List

from engine.db import connect as cards_db_connect, find_card_by_name


def get_format_legality(card: dict, fmt: str) -> tuple[bool, str]:
    legalities = card.get("legalities") or {}
    status = legalities.get(fmt)
    if status == "legal":
        return True, "legal"
    if status is None:
        return False, "missing"
    return False, status


def resolve_commander_by_name(conn, snapshot_id: str, name: str):
    _ = conn
    return find_card_by_name(snapshot_id, name)


def resolve_deck_cards_by_inputs(conn, snapshot_id: str, inputs: List[str]) -> List[Dict[str, Any]]:
    _ = conn
    resolved: List[Dict[str, Any]] = []
    for name in inputs:
        card = find_card_by_name(snapshot_id, name)
        if isinstance(card, dict):
            resolved.append(card)
    return resolved


def lookup_cards_by_oracle_ids(conn, snapshot_id: str, oracle_ids: set[str]) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    oracle_ids_unique = sorted(set(oid for oid in oracle_ids if isinstance(oid, str) and oid != ""))
    if not oracle_ids_unique:
        return lookup

    placeholders = ",".join(["?"] * len(oracle_ids_unique))
    query = (
        "SELECT oracle_id, name, type_line, mana_cost, oracle_text "
        f"FROM cards WHERE snapshot_id = ? AND oracle_id IN ({placeholders})"
    )

    try:
        if conn is None:
            with cards_db_connect() as local_con:
                rows = local_con.execute(query, [snapshot_id, *oracle_ids_unique]).fetchall()
        else:
            rows = conn.execute(query, [snapshot_id, *oracle_ids_unique]).fetchall()
    except Exception:
        return lookup

    for row in rows:
        row_dict = dict(row)
        oracle_id = row_dict.get("oracle_id")
        if not isinstance(oracle_id, str):
            continue
        lookup[oracle_id] = {
            "name": row_dict.get("name") if isinstance(row_dict.get("name"), str) else None,
            "type_line": row_dict.get("type_line") if isinstance(row_dict.get("type_line"), str) else None,
            "mana_cost": row_dict.get("mana_cost") if isinstance(row_dict.get("mana_cost"), str) else None,
            "oracle_text": row_dict.get("oracle_text") if isinstance(row_dict.get("oracle_text"), str) else None,
        }

    return lookup
