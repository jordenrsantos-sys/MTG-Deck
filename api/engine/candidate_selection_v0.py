import json
from typing import Any, Dict, List

from api.engine.constants import GAME_CHANGERS_SET, SINGLETON_EXEMPT_NAMES
from api.engine.utils import normalize_primitives_source, sorted_unique
from engine.db import connect as cards_db_connect, is_legal_in_format
from engine.game_changers import bracket_floor_from_count, detect_game_changers


def normalize_color_identity(value: Any) -> List[str]:
    if isinstance(value, list):
        return [c for c in value if isinstance(c, str)]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return []
        if isinstance(parsed, list):
            return [c for c in parsed if isinstance(c, str)]
    return []


def normalize_legalities(value: Any, fallback_json: Any = None) -> Dict[str, str]:
    def _clean_mapping(mapping: Any) -> Dict[str, str]:
        out: Dict[str, str] = {}
        if not isinstance(mapping, dict):
            return out
        try:
            items = list(mapping.items())
        except Exception:
            return out
        for key, item_value in items:
            if isinstance(key, str) and isinstance(item_value, str):
                out[key] = item_value
        return out

    if isinstance(value, dict):
        return _clean_mapping(value)
    source = fallback_json if fallback_json is not None else value
    if isinstance(source, str):
        try:
            parsed = json.loads(source)
        except (TypeError, ValueError):
            return {}
        return _clean_mapping(parsed)
    return {}


def is_singleton_exempt_card(name: str, type_line: str | None) -> bool:
    if name in SINGLETON_EXEMPT_NAMES:
        return True
    return isinstance(type_line, str) and "basic land" in type_line.lower()


def ci_compatible(commander_ci: List[str], card_ci: List[str]) -> bool:
    return set(card_ci).issubset(set(commander_ci))


def build_card_row(row: Dict[str, Any]) -> Dict[str, Any]:
    name = row.get("name")
    oracle_id = row.get("oracle_id")
    type_line = row.get("type_line")
    mana_cost = row.get("mana_cost")
    primitives = normalize_primitives_source(row.get("primitives_json"))
    legalities = normalize_legalities(row.get("legalities"), fallback_json=row.get("legalities_json"))
    color_identity = normalize_color_identity(row.get("color_identity"))

    return {
        "name": name if isinstance(name, str) else None,
        "oracle_id": oracle_id if isinstance(oracle_id, str) else None,
        "type_line": type_line if isinstance(type_line, str) else None,
        "mana_cost": mana_cost if isinstance(mana_cost, str) else None,
        "primitives": primitives,
        "legalities": legalities,
        "color_identity": color_identity,
    }


def query_candidate_rows(snapshot_id: str, primitives_needed: List[str], limit: int = 4000) -> List[Dict[str, Any]]:
    primitives_needed_sorted = sorted_unique([p for p in primitives_needed if isinstance(p, str)])

    if primitives_needed_sorted:
        predicate = " OR ".join(["LOWER(primitives_json) LIKE LOWER(?)"] * len(primitives_needed_sorted))
        params: List[Any] = [snapshot_id]
        params.extend([f'%"{primitive}"%' for primitive in primitives_needed_sorted])
        params.append(int(limit))
        query = (
            "SELECT name, oracle_id, mana_cost, type_line, color_identity, legalities_json, primitives_json "
            "FROM cards "
            "WHERE snapshot_id = ? AND (" + predicate + ") "
            "ORDER BY name ASC "
            "LIMIT ?"
        )
    else:
        params = [snapshot_id, int(limit)]
        query = (
            "SELECT name, oracle_id, mana_cost, type_line, color_identity, legalities_json, primitives_json "
            "FROM cards "
            "WHERE snapshot_id = ? "
            "ORDER BY name ASC "
            "LIMIT ?"
        )

    try:
        with cards_db_connect() as con:
            rows = con.execute(query, params).fetchall()
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    seen_names: set[str] = set()
    for row in rows:
        card = build_card_row(dict(row))
        name = card.get("name")
        if not isinstance(name, str) or name == "":
            continue
        if name in seen_names:
            continue
        seen_names.add(name)
        out.append(card)
    return out


def filter_candidate_rows(
    cards: List[Dict[str, Any]],
    commander_name: str,
    commander_oracle_id: str,
    commander_ci: List[str],
    format_name: str,
    bracket_id: str,
    current_cards: List[str],
) -> List[Dict[str, Any]]:
    current_cards_sorted = [c for c in current_cards if isinstance(c, str)]
    current_cards_set = set(current_cards_sorted)

    current_gc_found, current_gc_count = detect_game_changers(
        playable_names=current_cards_sorted,
        commander_name=commander_name,
        gc_set=GAME_CHANGERS_SET,
    )
    _ = current_gc_found

    filtered: List[Dict[str, Any]] = []
    for card in cards:
        name = card.get("name")
        oracle_id = card.get("oracle_id")
        type_line = card.get("type_line")

        if not isinstance(name, str):
            continue
        if not isinstance(oracle_id, str):
            continue
        if name == commander_name:
            continue
        if oracle_id == commander_oracle_id:
            continue

        exempt = is_singleton_exempt_card(name, type_line)
        if (not exempt) and (name in current_cards_set):
            continue

        if not ci_compatible(commander_ci, card.get("color_identity") or []):
            continue

        legal, _ = is_legal_in_format(card, format_name)
        if not legal:
            continue

        if bracket_id == "B3" and name in GAME_CHANGERS_SET:
            projected_count = current_gc_count + 1
            projected_floor = bracket_floor_from_count(projected_count)
            if projected_floor == "B4":
                continue

        filtered.append(card)

    filtered.sort(key=lambda c: str(c.get("name") or ""))
    return filtered


def get_candidate_pool_v0(
    snapshot_id: str,
    primitives_needed: List[str],
    commander_name: str,
    commander_oracle_id: str,
    commander_ci: List[str],
    format_name: str,
    bracket_id: str,
    current_cards: List[str],
) -> List[Dict[str, Any]]:
    candidates = query_candidate_rows(snapshot_id=snapshot_id, primitives_needed=primitives_needed, limit=4000)
    filtered = filter_candidate_rows(
        cards=candidates,
        commander_name=commander_name,
        commander_oracle_id=commander_oracle_id,
        commander_ci=commander_ci,
        format_name=format_name,
        bracket_id=bracket_id,
        current_cards=current_cards,
    )

    if filtered:
        return filtered

    fallback = query_candidate_rows(snapshot_id=snapshot_id, primitives_needed=[], limit=4000)
    return filter_candidate_rows(
        cards=fallback,
        commander_name=commander_name,
        commander_oracle_id=commander_oracle_id,
        commander_ci=commander_ci,
        format_name=format_name,
        bracket_id=bracket_id,
        current_cards=current_cards,
    )
