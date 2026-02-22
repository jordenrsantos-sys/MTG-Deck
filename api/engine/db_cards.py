from typing import Any, Dict, List

from api.engine.constants import assert_runtime_no_oracle_text

from engine.db import connect as cards_db_connect, find_card_by_name


_DEFAULT_CARD_LOOKUP_FIELDS = [
    "oracle_id",
    "name",
    "type_line",
    "mana_cost",
    "cmc",
    "colors",
    "color_identity",
]

_ALLOWED_CARD_LOOKUP_FIELDS = {
    "oracle_id",
    "name",
    "type_line",
    "mana_cost",
    "cmc",
    "colors",
    "color_identity",
    "produced_mana",
    "keywords",
    "legalities_json",
    "primitives_json",
}


def list_cards_table_columns(*, table_name: str = "cards") -> List[str]:
    safe_table_name = table_name.strip() if isinstance(table_name, str) else "cards"
    if safe_table_name == "":
        safe_table_name = "cards"

    # Keep introspection deterministic and safe for SQLite identifiers.
    if not all(ch.isalnum() or ch == "_" for ch in safe_table_name):
        return []

    with cards_db_connect() as con:
        try:
            rows = con.execute(f"PRAGMA table_info({safe_table_name})").fetchall()
        except Exception:
            return []

    out: List[str] = []
    for row in rows:
        row_dict = dict(row)
        col = row_dict.get("name")
        if not isinstance(col, str):
            continue
        token = col.strip()
        if token == "":
            continue
        out.append(token)
    return sorted(set(out))


def _normalize_requested_fields(requested_fields: List[str] | None) -> List[str]:
    if requested_fields is None:
        requested = list(_DEFAULT_CARD_LOOKUP_FIELDS)
    else:
        requested = [field for field in requested_fields if isinstance(field, str) and field != ""]

    requested_set = set(requested)
    if "oracle_text" in requested_set:
        assert_runtime_no_oracle_text(
            "db_cards.lookup_cards_by_oracle_ids requested forbidden field oracle_text"
        )

    safe_fields = [field for field in requested if field in _ALLOWED_CARD_LOOKUP_FIELDS]
    if "oracle_id" not in safe_fields:
        safe_fields.insert(0, "oracle_id")
    return safe_fields


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


def lookup_cards_by_oracle_ids(
    conn,
    snapshot_id: str,
    oracle_ids: set[str],
    requested_fields: List[str] | None = None,
) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    oracle_ids_unique = sorted(set(oid for oid in oracle_ids if isinstance(oid, str) and oid != ""))
    if not oracle_ids_unique:
        return lookup

    select_fields = _normalize_requested_fields(requested_fields=requested_fields)
    placeholders = ",".join(["?"] * len(oracle_ids_unique))
    query = (
        f"SELECT {', '.join(select_fields)} "
        f"FROM cards WHERE snapshot_id = ? AND oracle_id IN ({placeholders})"
    )

    try:
        if conn is None:
            local_con = cards_db_connect()
            try:
                rows = local_con.execute(query, [snapshot_id, *oracle_ids_unique]).fetchall()
            finally:
                local_con.close()
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
            field: row_dict.get(field)
            for field in select_fields
            if field in row_dict
        }

    return lookup
