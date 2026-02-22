from __future__ import annotations

import json
import os
import sqlite3
from time import perf_counter
from typing import Any, Dict, List, Optional, Set, Tuple

from api.engine.bracket_gc_enforcement_v1 import UNKNOWN_BRACKET_RULES
from api.engine.bracket_gc_limits import resolve_gc_limits
from api.engine.constants import GAME_CHANGERS_SET
from api.engine.db_cards import list_cards_table_columns
from api.engine.format_legality_filter_v1 import (
    is_deck_legal_card_v1,
    legality_filter_available_v1,
    select_filter_columns_v1,
)
from api.engine.utils import normalize_primitives_source
from engine.db import connect as cards_db_connect

VERSION = "candidate_pool_v1"
_ALLOWED_COLORS = frozenset({"W", "U", "B", "R", "G"})


def _clean_nonempty_strings(values: Any) -> List[str]:
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
    return out


def _normalize_exclude_name_set(values: Any) -> Set[str]:
    out: Set[str] = set()
    for value in _clean_nonempty_strings(values):
        out.add(value.lower())
    return out


def _normalize_commander_colors(values: Set[str]) -> Set[str]:
    if not isinstance(values, (set, list, tuple)):
        return set()

    out: Set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        token = value.strip().upper()
        if token in {"W", "U", "B", "R", "G"}:
            out.add(token)
    return out


def _normalize_limit(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 2000
    if value < 1:
        return 1
    return value


def _round6(value: float) -> float:
    return float(f"{float(value):.6f}")


def _normalize_cards_table_columns(values: Any) -> List[str]:
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
    return sorted(set(out))


def _top5_sorted_unique_names(values: List[str]) -> List[str]:
    names = [token for token in _clean_nonempty_strings(values)]
    return sorted(set(names), key=lambda name: (name.casefold(), name))[:5]


def _is_dev_metrics_enabled() -> bool:
    return os.getenv("MTG_ENGINE_DEV_METRICS") == "1"


def _normalize_color_set(values: Any) -> Set[str]:
    if not isinstance(values, (set, list, tuple)):
        return set()

    out: Set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        token = value.strip().upper()
        if token in _ALLOWED_COLORS:
            out.add(token)
    return out


def _parse_color_identity(raw: Any) -> Tuple[bool, Set[str]]:
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


def _chunk(values: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [values]
    return [values[idx : idx + size] for idx in range(0, len(values), size)]


def _build_name_color_cache(db_snapshot_id: str, rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Tuple[bool, Set[str]]], float]:
    snapshot_id = db_snapshot_id.strip() if isinstance(db_snapshot_id, str) else ""
    if snapshot_id == "":
        return {}, 0.0

    name_keys = sorted(
        {
            name.lower()
            for row in rows
            for name in [row.get("name")]
            if isinstance(name, str) and name != ""
        }
    )
    if len(name_keys) == 0:
        return {}, 0.0

    out: Dict[str, Tuple[bool, Set[str]]] = {}
    sql_started_at = perf_counter()
    with cards_db_connect() as con:
        for name_chunk in _chunk(name_keys, 900):
            placeholders = ",".join("?" for _ in name_chunk)
            rows_for_chunk = con.execute(
                (
                    "SELECT name, color_identity "
                    "FROM cards "
                    "WHERE snapshot_id = ? "
                    f"AND LOWER(name) IN ({placeholders}) "
                    "ORDER BY oracle_id ASC, name ASC"
                ),
                (snapshot_id, *name_chunk),
            ).fetchall()
            for row in rows_for_chunk:
                name = row["name"] if isinstance(row["name"], str) else ""
                if name == "":
                    continue
                key = name.lower()
                if key in out:
                    continue
                out[key] = _parse_color_identity(row["color_identity"])

    sql_query_ms = _round6(max((perf_counter() - sql_started_at) * 1000.0, 0.0))
    return out, sql_query_ms


def _snapshot_exists(db_snapshot_id: str) -> bool:
    snapshot_id = db_snapshot_id.strip() if isinstance(db_snapshot_id, str) else ""
    if snapshot_id == "":
        return False

    with cards_db_connect() as con:
        try:
            row = con.execute(
                "SELECT 1 FROM snapshots WHERE snapshot_id = ? LIMIT 1",
                (snapshot_id,),
            ).fetchone()
        except sqlite3.OperationalError:
            return False
    return row is not None


def _build_gc_filter_context(*, db_snapshot_id: str, bracket_id: str, current_cards: List[str]) -> Dict[str, Any]:
    current_gc_count = sum(1 for name in current_cards if name in GAME_CHANGERS_SET)
    snapshot_available = _snapshot_exists(db_snapshot_id)
    if not snapshot_available:
        return {
            "snapshot_available": False,
            "unknown_bracket_rules": False,
            "max_allowed": None,
            "current_gc_count": int(current_gc_count),
        }

    bracket_token = bracket_id.strip() if isinstance(bracket_id, str) else ""
    try:
        _, max_allowed, _, unknown_flag = resolve_gc_limits(bracket_token)
    except RuntimeError:
        unknown_flag = True
        max_allowed = None

    return {
        "snapshot_available": True,
        "unknown_bracket_rules": bool(unknown_flag),
        "max_allowed": max_allowed if isinstance(max_allowed, int) or max_allowed is None else None,
        "current_gc_count": int(current_gc_count),
    }


def _passes_gc_constraint(card_name: str, gc_context: Dict[str, Any]) -> bool | str:
    if card_name not in GAME_CHANGERS_SET:
        return True

    if not bool(gc_context.get("snapshot_available")):
        return True

    if bool(gc_context.get("unknown_bracket_rules")):
        return UNKNOWN_BRACKET_RULES

    max_allowed = gc_context.get("max_allowed")
    if max_allowed is None:
        return True

    projected_count = int(gc_context.get("current_gc_count") or 0) + 1
    return projected_count <= int(max_allowed)


def _query_snapshot_cards(
    *,
    db_snapshot_id: str,
    exclude_names_lower: Set[str],
    include_primitives_set: Set[str],
    select_columns: List[str],
) -> Tuple[List[Dict[str, Any]], float]:
    snapshot_id = db_snapshot_id.strip() if isinstance(db_snapshot_id, str) else ""
    if snapshot_id == "":
        return [], 0.0

    required_columns = ["oracle_id", "name", "color_identity", "primitives_json"]
    select_columns_clean: List[str] = []
    seen_select_columns: Set[str] = set()
    for value in required_columns + list(select_columns):
        if not isinstance(value, str):
            continue
        token = value.strip()
        if token == "":
            continue
        if not all(ch.isalnum() or ch == "_" for ch in token):
            continue
        if token in seen_select_columns:
            continue
        seen_select_columns.add(token)
        select_columns_clean.append(token)

    if len(select_columns_clean) == 0:
        select_columns_clean = list(required_columns)

    select_sql = ", ".join(select_columns_clean)

    where_clauses: List[str] = ["snapshot_id = ?"]
    params: List[Any] = [snapshot_id]

    exclude_names_sorted = sorted(exclude_names_lower)
    if len(exclude_names_sorted) > 0:
        placeholders = ",".join("?" for _ in exclude_names_sorted)
        where_clauses.append(f"LOWER(name) NOT IN ({placeholders})")
        params.extend(exclude_names_sorted)

    base_where_clauses = list(where_clauses)
    base_params = list(params)

    include_primitives_sorted = sorted(include_primitives_set)
    include_filter_enabled = len(include_primitives_sorted) > 0
    if include_filter_enabled:
        primitive_placeholders = ",".join("?" for _ in include_primitives_sorted)
        where_clauses.append("json_valid(primitives_json) = 1")
        where_clauses.append(
            (
                "EXISTS ("
                "SELECT 1 FROM json_each(primitives_json) AS primitive "
                f"WHERE primitive.value IN ({primitive_placeholders})"
                ")"
            )
        )
        params.extend(include_primitives_sorted)

    sql = (
        f"SELECT {select_sql} "
        f"FROM cards WHERE {' AND '.join(where_clauses)} "
        "ORDER BY oracle_id ASC, name ASC"
    )

    sql_started_at = perf_counter()

    fallback_sql = (
        f"SELECT {select_sql} "
        f"FROM cards WHERE {' AND '.join(base_where_clauses)} "
        "ORDER BY oracle_id ASC, name ASC"
    )
    fallback_params = tuple(base_params)

    with cards_db_connect() as con:
        try:
            rows = con.execute(sql, tuple(params)).fetchall()
        except sqlite3.OperationalError:
            if not include_filter_enabled:
                raise
            rows = con.execute(fallback_sql, fallback_params).fetchall()

    sql_query_ms = _round6(max((perf_counter() - sql_started_at) * 1000.0, 0.0))
    return [dict(row) for row in rows], sql_query_ms


def _primitive_match_score(card_primitives: List[str], include_primitives: Set[str]) -> int:
    if len(include_primitives) == 0:
        return 0
    return sum(1 for primitive in card_primitives if primitive in include_primitives)


def get_candidate_pool_v1(
    db_snapshot_id: str,
    include_primitives: Optional[List[str]],
    exclude_card_names: List[str],
    commander_color_set: Set[str],
    bracket_id: str,
    limit: int = 2000,
    dev_metrics_out: Optional[Dict[str, Any]] = None,
    format: str = "commander",
) -> List[Dict[str, Any]]:
    include_primitives_clean = sorted(set(_clean_nonempty_strings(include_primitives)))
    include_primitives_set = set(include_primitives_clean)
    exclude_names_lower = _normalize_exclude_name_set(exclude_card_names)
    commander_colors = _normalize_commander_colors(commander_color_set)
    limit_clean = _normalize_limit(limit)
    dev_metrics_enabled = _is_dev_metrics_enabled()
    format_clean = format.strip() if isinstance(format, str) and format.strip() != "" else "commander"
    cards_table_columns = _normalize_cards_table_columns(list_cards_table_columns())
    legality_filter_available = legality_filter_available_v1(cards_table_columns)

    query_columns = ["oracle_id", "name", "color_identity", "primitives_json"]
    for column in select_filter_columns_v1(cards_table_columns):
        if column in query_columns:
            continue
        query_columns.append(column)

    current_cards = sorted(_clean_nonempty_strings(exclude_card_names))
    gc_context = _build_gc_filter_context(
        db_snapshot_id=db_snapshot_id,
        bracket_id=bracket_id,
        current_cards=current_cards,
    )

    rows, sql_query_ms = _query_snapshot_cards(
        db_snapshot_id=db_snapshot_id,
        exclude_names_lower=exclude_names_lower,
        include_primitives_set=include_primitives_set,
        select_columns=query_columns,
    )

    total_candidates_seen = len(rows)
    filtered_illegal_names: List[str] = []
    if legality_filter_available:
        legal_rows: List[Dict[str, Any]] = []
        for row in rows:
            allowed, _ = is_deck_legal_card_v1(row, format_clean)
            if allowed:
                legal_rows.append(row)
                continue
            name = row.get("name")
            if isinstance(name, str) and name != "":
                filtered_illegal_names.append(name)
        rows = legal_rows

    filtered_illegal_count = len(filtered_illegal_names)
    filtered_illegal_examples_top5 = _top5_sorted_unique_names(filtered_illegal_names)

    color_cache, color_cache_sql_ms = _build_name_color_cache(db_snapshot_id, rows)
    sql_query_ms = _round6(sql_query_ms + color_cache_sql_ms)

    python_filter_ms = 0.0
    color_check_ms = 0.0
    gc_check_ms = 0.0

    out: List[Dict[str, Any]] = []
    for row in rows:
        filter_started_at = perf_counter() if dev_metrics_enabled else 0.0
        oracle_id = row.get("oracle_id")
        name = row.get("name")
        if not isinstance(oracle_id, str) or oracle_id == "":
            if dev_metrics_enabled:
                python_filter_ms += max((perf_counter() - filter_started_at) * 1000.0, 0.0)
            continue
        if not isinstance(name, str) or name == "":
            if dev_metrics_enabled:
                python_filter_ms += max((perf_counter() - filter_started_at) * 1000.0, 0.0)
            continue

        if name.lower() in exclude_names_lower:
            if dev_metrics_enabled:
                python_filter_ms += max((perf_counter() - filter_started_at) * 1000.0, 0.0)
            continue

        if dev_metrics_enabled:
            python_filter_ms += max((perf_counter() - filter_started_at) * 1000.0, 0.0)

        color_started_at = perf_counter() if dev_metrics_enabled else 0.0
        color_available, card_colors = color_cache.get(name.lower(), (False, set()))
        color_legal = color_available and card_colors.issubset(commander_colors)
        if dev_metrics_enabled:
            color_check_ms += max((perf_counter() - color_started_at) * 1000.0, 0.0)
        if not color_available:
            continue
        if not color_legal:
            continue

        gc_started_at = perf_counter() if dev_metrics_enabled else 0.0
        gc_violation = _passes_gc_constraint(name, gc_context)
        if dev_metrics_enabled:
            gc_check_ms += max((perf_counter() - gc_started_at) * 1000.0, 0.0)
        if gc_violation == UNKNOWN_BRACKET_RULES:
            continue
        if gc_violation is not True:
            continue

        score_started_at = perf_counter() if dev_metrics_enabled else 0.0
        primitive_ids = normalize_primitives_source(row.get("primitives_json"))
        score = _primitive_match_score(primitive_ids, include_primitives_set)

        if len(include_primitives_set) > 0 and score <= 0:
            if dev_metrics_enabled:
                python_filter_ms += max((perf_counter() - score_started_at) * 1000.0, 0.0)
            continue

        if dev_metrics_enabled:
            python_filter_ms += max((perf_counter() - score_started_at) * 1000.0, 0.0)

        out.append(
            {
                "oracle_id": oracle_id,
                "name": name,
                "primitive_ids_v1": list(primitive_ids),
                "primitive_match_score_v1": int(score),
                "is_game_changer_v1": name in GAME_CHANGERS_SET,
            }
        )

    out.sort(
        key=lambda row: (
            -int(row.get("primitive_match_score_v1", 0)),
            str(row.get("oracle_id") or ""),
            str(row.get("name") or ""),
        )
    )

    limited = out[:limit_clean]

    if dev_metrics_enabled and isinstance(dev_metrics_out, dict):
        dev_metrics_out.clear()
        dev_metrics_out.update(
            {
                "sql_query_ms": _round6(sql_query_ms),
                "python_filter_ms": _round6(python_filter_ms),
                "color_check_ms": _round6(color_check_ms),
                "gc_check_ms": _round6(gc_check_ms),
                "total_candidates_seen": int(total_candidates_seen),
                "total_candidates_returned": int(len(limited)),
                "legality_filter_available_v1": bool(legality_filter_available),
                "filtered_illegal_count_v1": int(filtered_illegal_count),
                "filtered_illegal_examples_top5_v1": filtered_illegal_examples_top5,
                "cards_table_columns_v1": list(cards_table_columns),
            }
        )

    return limited
