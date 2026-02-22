from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple

from engine.db import connect as cards_db_connect

from api.engine.decklist_parse_v1 import normalize_decklist_name


DECKLIST_RESOLVE_VERSION = "decklist_resolve_v1"

_ALIAS_TABLE_CANDIDATES = (
    "card_aliases",
    "card_name_aliases",
    "name_aliases",
    "aliases",
)
_ALIAS_COLUMN_CANDIDATES = (
    "alias_name",
    "alias",
    "name_alias",
    "lookup_name",
)
_ALIAS_ORACLE_COLUMN_CANDIDATES = (
    "oracle_id",
    "card_oracle_id",
)
_ALIAS_NAME_COLUMN_CANDIDATES = (
    "name",
    "card_name",
    "canonical_name",
)
_ALIAS_SNAPSHOT_COLUMN_CANDIDATES = (
    "snapshot_id",
    "db_snapshot_id",
)
_DFC_DELIMITER_VARIANTS_RE = re.compile(r"\s*/{1,2}\s*")


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _safe_positive_int(value: Any, *, default: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return int(default)
    if value < 1:
        return int(default)
    return int(value)


def _safe_line_no(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    if value < 0:
        return 0
    return int(value)


def _make_candidate(*, oracle_id: str, name: str) -> Dict[str, str]:
    return {
        "oracle_id": oracle_id,
        "name": name,
    }


def _sort_candidates(candidates: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
    dedup: Dict[Tuple[str, str], Dict[str, str]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        oracle_id = _nonempty_str(candidate.get("oracle_id"))
        name = _nonempty_str(candidate.get("name"))
        if oracle_id is None or name is None:
            continue
        dedup[(oracle_id, name)] = {
            "oracle_id": oracle_id,
            "name": name,
        }

    return [
        dedup[key]
        for key in sorted(
            dedup.keys(),
            key=lambda item: (item[0], item[1].casefold(), item[1]),
        )
    ]


def _list_table_names(con) -> List[str]:
    rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name ASC"
    ).fetchall()

    out: List[str] = []
    for row in rows:
        row_dict = dict(row)
        name = _nonempty_str(row_dict.get("name"))
        if name is None:
            continue
        out.append(name)
    return out


def _table_columns(con, table_name: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    columns: List[str] = []
    for row in rows:
        row_dict = dict(row)
        name = _nonempty_str(row_dict.get("name"))
        if name is None:
            continue
        columns.append(name)
    return columns


def _pick_first(columns: List[str], candidates: Tuple[str, ...], *, exclude: set[str] | None = None) -> str | None:
    exclude_set = exclude or set()
    for candidate in candidates:
        if candidate in columns and candidate not in exclude_set:
            return candidate
    return None


def _split_dfc_faces_from_card_name(card_name: str) -> Tuple[str, str] | None:
    if "//" not in card_name:
        return None
    parts = card_name.split("//")
    if len(parts) != 2:
        return None
    face_a = _nonempty_str(parts[0])
    face_b = _nonempty_str(parts[1])
    if face_a is None or face_b is None:
        return None
    return face_a, face_b


def _normalize_dfc_combined_name(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    token = value.strip()
    if token == "":
        return None
    parts = _DFC_DELIMITER_VARIANTS_RE.split(token)
    if len(parts) != 2:
        return None
    face_a_norm = normalize_decklist_name(parts[0])
    face_b_norm = normalize_decklist_name(parts[1])
    if face_a_norm is None or face_b_norm is None:
        return None
    return f"{face_a_norm}//{face_b_norm}"


def _load_cards_index(con, db_snapshot_id: str) -> Tuple[
    Dict[str, Dict[str, str]],
    Dict[str, List[Dict[str, str]]],
    Dict[str, List[Dict[str, str]]],
    Dict[str, List[Dict[str, str]]],
    Dict[str, List[Dict[str, str]]],
    Dict[str, List[Dict[str, str]]],
]:
    rows = con.execute(
        """
        SELECT oracle_id, name
        FROM cards
        WHERE snapshot_id = ?
        ORDER BY LOWER(name) ASC, name ASC, oracle_id ASC
        """,
        (db_snapshot_id,),
    ).fetchall()

    cards_by_oracle: Dict[str, Dict[str, str]] = {}
    exact_index: Dict[str, List[Dict[str, str]]] = {}
    normalized_index: Dict[str, List[Dict[str, str]]] = {}
    dfc_combined_normalized_index: Dict[str, List[Dict[str, str]]] = {}
    dfc_face_exact_index: Dict[str, List[Dict[str, str]]] = {}
    dfc_face_normalized_index: Dict[str, List[Dict[str, str]]] = {}

    for row in rows:
        row_dict = dict(row)
        oracle_id = _nonempty_str(row_dict.get("oracle_id"))
        name = _nonempty_str(row_dict.get("name"))
        if oracle_id is None or name is None:
            continue

        candidate = _make_candidate(oracle_id=oracle_id, name=name)
        cards_by_oracle[oracle_id] = candidate

        exact_key = name.casefold()
        exact_index.setdefault(exact_key, []).append(candidate)

        normalized_key = normalize_decklist_name(name)
        if normalized_key is not None:
            normalized_index.setdefault(normalized_key, []).append(candidate)

        dfc_faces = _split_dfc_faces_from_card_name(name)
        if dfc_faces is None:
            continue

        combined_norm = _normalize_dfc_combined_name(name)
        if combined_norm is not None:
            dfc_combined_normalized_index.setdefault(combined_norm, []).append(candidate)

        for face_name in dfc_faces:
            dfc_face_exact_index.setdefault(face_name.casefold(), []).append(candidate)
            face_name_norm = normalize_decklist_name(face_name)
            if face_name_norm is not None:
                dfc_face_normalized_index.setdefault(face_name_norm, []).append(candidate)

    for exact_key in list(exact_index.keys()):
        exact_index[exact_key] = _sort_candidates(exact_index[exact_key])
    for normalized_key in list(normalized_index.keys()):
        normalized_index[normalized_key] = _sort_candidates(normalized_index[normalized_key])
    for combined_key in list(dfc_combined_normalized_index.keys()):
        dfc_combined_normalized_index[combined_key] = _sort_candidates(dfc_combined_normalized_index[combined_key])
    for face_exact_key in list(dfc_face_exact_index.keys()):
        dfc_face_exact_index[face_exact_key] = _sort_candidates(dfc_face_exact_index[face_exact_key])
    for face_norm_key in list(dfc_face_normalized_index.keys()):
        dfc_face_normalized_index[face_norm_key] = _sort_candidates(dfc_face_normalized_index[face_norm_key])

    return (
        cards_by_oracle,
        exact_index,
        normalized_index,
        dfc_combined_normalized_index,
        dfc_face_exact_index,
        dfc_face_normalized_index,
    )


def _load_alias_index(
    con,
    *,
    db_snapshot_id: str,
    cards_by_oracle: Dict[str, Dict[str, str]],
    exact_index: Dict[str, List[Dict[str, str]]],
) -> Dict[str, List[Dict[str, str]]]:
    alias_index: Dict[str, List[Dict[str, str]]] = {}

    table_names = _list_table_names(con)
    table_name_set = set(table_names)

    alias_tables = [table_name for table_name in _ALIAS_TABLE_CANDIDATES if table_name in table_name_set]
    alias_tables.extend(
        table_name
        for table_name in table_names
        if "alias" in table_name.casefold() and table_name not in alias_tables
    )

    for table_name in alias_tables:
        columns = _table_columns(con, table_name)
        if len(columns) == 0:
            continue

        alias_column = _pick_first(columns, _ALIAS_COLUMN_CANDIDATES)
        if alias_column is None:
            continue

        oracle_column = _pick_first(columns, _ALIAS_ORACLE_COLUMN_CANDIDATES)
        name_column = _pick_first(columns, _ALIAS_NAME_COLUMN_CANDIDATES, exclude={alias_column})
        snapshot_column = _pick_first(columns, _ALIAS_SNAPSHOT_COLUMN_CANDIDATES)

        if oracle_column is None and name_column is None:
            continue

        select_columns = [alias_column]
        if oracle_column is not None:
            select_columns.append(oracle_column)
        if name_column is not None:
            select_columns.append(name_column)

        order_columns = [alias_column]
        if oracle_column is not None:
            order_columns.append(oracle_column)
        if name_column is not None:
            order_columns.append(name_column)

        if snapshot_column is not None:
            query = (
                f"SELECT {', '.join(select_columns)} "
                f"FROM {table_name} "
                f"WHERE {snapshot_column} = ? "
                f"ORDER BY {', '.join(order_columns)}"
            )
            rows = con.execute(query, (db_snapshot_id,)).fetchall()
        else:
            query = (
                f"SELECT {', '.join(select_columns)} "
                f"FROM {table_name} "
                f"ORDER BY {', '.join(order_columns)}"
            )
            rows = con.execute(query).fetchall()

        for row in rows:
            row_dict = dict(row)
            alias_value = _nonempty_str(row_dict.get(alias_column))
            alias_norm = normalize_decklist_name(alias_value)
            if alias_norm is None:
                continue

            candidates_raw: List[Dict[str, str]] = []

            if oracle_column is not None:
                oracle_id = _nonempty_str(row_dict.get(oracle_column))
                if oracle_id is not None and oracle_id in cards_by_oracle:
                    candidates_raw.append(cards_by_oracle[oracle_id])

            if name_column is not None:
                name_value = _nonempty_str(row_dict.get(name_column))
                if name_value is not None:
                    candidates_raw.extend(exact_index.get(name_value.casefold(), []))

            candidates_sorted = _sort_candidates(candidates_raw)
            if len(candidates_sorted) == 0:
                continue

            alias_index.setdefault(alias_norm, []).extend(candidates_sorted)

    for alias_norm in list(alias_index.keys()):
        alias_index[alias_norm] = _sort_candidates(alias_index[alias_norm])

    return alias_index


def _lookup_candidates(
    *,
    name_raw: str,
    name_norm: str,
    exact_index: Dict[str, List[Dict[str, str]]],
    normalized_index: Dict[str, List[Dict[str, str]]],
    alias_index: Dict[str, List[Dict[str, str]]],
) -> List[Dict[str, str]]:
    candidates = exact_index.get(name_raw.casefold(), [])
    if len(candidates) == 0:
        candidates = normalized_index.get(name_norm, [])
    if len(candidates) == 0:
        candidates = alias_index.get(name_norm, [])
    return candidates


def _lookup_dfc_fallback_candidates(
    *,
    name_raw: str,
    name_norm: str,
    exact_index: Dict[str, List[Dict[str, str]]],
    normalized_index: Dict[str, List[Dict[str, str]]],
    dfc_combined_normalized_index: Dict[str, List[Dict[str, str]]],
    dfc_face_exact_index: Dict[str, List[Dict[str, str]]],
    dfc_face_normalized_index: Dict[str, List[Dict[str, str]]],
) -> List[Dict[str, str]]:
    full_exact = exact_index.get(name_raw.casefold(), [])
    if len(full_exact) > 0:
        return full_exact

    face_exact = dfc_face_exact_index.get(name_raw.casefold(), [])
    if len(face_exact) == 1:
        return face_exact
    if len(face_exact) > 1:
        return face_exact

    combined_norm = _normalize_dfc_combined_name(name_raw)
    normalized_full_candidates = normalized_index.get(name_norm, [])
    if combined_norm is not None:
        normalized_full_candidates = _sort_candidates(
            list(normalized_full_candidates) + list(dfc_combined_normalized_index.get(combined_norm, []))
        )
    if len(normalized_full_candidates) > 0:
        return normalized_full_candidates

    return dfc_face_normalized_index.get(name_norm, [])


def _normalize_name_overrides_v1_for_resolution(name_overrides_v1: Any) -> Dict[str, Dict[str, str]]:
    rows = name_overrides_v1 if isinstance(name_overrides_v1, list) else []

    normalized_rows: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        name_raw = _nonempty_str(row.get("name_raw"))
        if name_raw is None:
            continue

        name_norm = normalize_decklist_name(name_raw)
        if name_norm is None:
            continue

        resolved_oracle_id = _nonempty_str(row.get("resolved_oracle_id"))
        resolved_name = _nonempty_str(row.get("resolved_name"))
        if resolved_oracle_id is None and resolved_name is None:
            continue

        normalized_row: Dict[str, str] = {
            "name_raw": name_raw,
            "name_norm": name_norm,
        }
        if resolved_oracle_id is not None:
            normalized_row["resolved_oracle_id"] = resolved_oracle_id
        elif resolved_name is not None:
            normalized_row["resolved_name"] = resolved_name

        normalized_rows.append(normalized_row)

    normalized_rows = sorted(
        normalized_rows,
        key=lambda row: (
            str(row.get("name_norm") or ""),
            str(row.get("name_raw") or "").casefold(),
            str(row.get("name_raw") or ""),
            str(row.get("resolved_oracle_id") or ""),
            str(row.get("resolved_name") or "").casefold(),
            str(row.get("resolved_name") or ""),
        ),
    )

    overrides_by_name_norm: Dict[str, Dict[str, str]] = {}
    for row in normalized_rows:
        name_norm = row.get("name_norm")
        if not isinstance(name_norm, str) or name_norm in overrides_by_name_norm:
            continue
        overrides_by_name_norm[name_norm] = dict(row)

    return overrides_by_name_norm


def _resolve_override_candidates(
    *,
    override_row: Dict[str, str],
    cards_by_oracle: Dict[str, Dict[str, str]],
    exact_index: Dict[str, List[Dict[str, str]]],
    normalized_index: Dict[str, List[Dict[str, str]]],
    alias_index: Dict[str, List[Dict[str, str]]],
) -> List[Dict[str, str]]:
    override_oracle_id = _nonempty_str(override_row.get("resolved_oracle_id"))
    if override_oracle_id is not None:
        candidate = cards_by_oracle.get(override_oracle_id)
        if isinstance(candidate, dict):
            return [candidate]
        return []

    override_name = _nonempty_str(override_row.get("resolved_name"))
    if override_name is None:
        return []

    override_name_norm = normalize_decklist_name(override_name)
    if override_name_norm is None:
        return []

    return _lookup_candidates(
        name_raw=override_name,
        name_norm=override_name_norm,
        exact_index=exact_index,
        normalized_index=normalized_index,
        alias_index=alias_index,
    )


def _unknown_row(
    *,
    name_raw: str,
    name_norm: str,
    count: int,
    line_no: int,
    reason_code: str,
    candidates: List[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "name_raw": name_raw,
        "name_norm": name_norm,
        "count": int(count),
        "line_no": int(line_no),
        "reason_code": reason_code,
        "candidates": candidates,
    }


def resolve_parsed_decklist(parsed: Any, db_snapshot_id: str, name_overrides_v1: Any = None) -> Dict[str, Any]:
    parsed_items = parsed.get("items") if isinstance(parsed, dict) and isinstance(parsed.get("items"), list) else []

    con = cards_db_connect()
    try:
        (
            cards_by_oracle,
            exact_index,
            normalized_index,
            dfc_combined_normalized_index,
            dfc_face_exact_index,
            dfc_face_normalized_index,
        ) = _load_cards_index(con, db_snapshot_id)
        alias_index = _load_alias_index(
            con,
            db_snapshot_id=db_snapshot_id,
            cards_by_oracle=cards_by_oracle,
            exact_index=exact_index,
        )
    finally:
        con.close()

    overrides_by_name_norm = _normalize_name_overrides_v1_for_resolution(name_overrides_v1)

    resolved_cards: List[Dict[str, Any]] = []
    unknowns: List[Dict[str, Any]] = []

    for raw_item in parsed_items:
        if not isinstance(raw_item, dict):
            continue

        name_raw = _nonempty_str(raw_item.get("name_raw"))
        name_norm = _nonempty_str(raw_item.get("name_norm"))
        if name_raw is None and name_norm is None:
            continue

        if name_raw is None and name_norm is not None:
            name_raw = name_norm
        if name_norm is None and name_raw is not None:
            name_norm = normalize_decklist_name(name_raw)

        if name_raw is None or name_norm is None:
            continue

        count = _safe_positive_int(raw_item.get("count"), default=1)
        line_no = _safe_line_no(raw_item.get("line_no"))

        candidates = _lookup_candidates(
            name_raw=name_raw,
            name_norm=name_norm,
            exact_index=exact_index,
            normalized_index=normalized_index,
            alias_index=alias_index,
        )
        if len(candidates) == 0:
            candidates = _lookup_dfc_fallback_candidates(
                name_raw=name_raw,
                name_norm=name_norm,
                exact_index=exact_index,
                normalized_index=normalized_index,
                dfc_combined_normalized_index=dfc_combined_normalized_index,
                dfc_face_exact_index=dfc_face_exact_index,
                dfc_face_normalized_index=dfc_face_normalized_index,
            )

        if len(candidates) != 1 and name_norm in overrides_by_name_norm:
            override_row = overrides_by_name_norm[name_norm]
            override_candidates = _resolve_override_candidates(
                override_row=override_row,
                cards_by_oracle=cards_by_oracle,
                exact_index=exact_index,
                normalized_index=normalized_index,
                alias_index=alias_index,
            )
            if len(override_candidates) == 1:
                candidate = override_candidates[0]
                resolved_cards.append(
                    {
                        "oracle_id": candidate["oracle_id"],
                        "name": candidate["name"],
                        "count": int(count),
                        "source_line_no": int(line_no),
                    }
                )
                continue

            fallback_candidates = candidates if len(candidates) > 0 else override_candidates
            unknowns.append(
                _unknown_row(
                    name_raw=name_raw,
                    name_norm=name_norm,
                    count=count,
                    line_no=line_no,
                    reason_code="OVERRIDE_INVALID",
                    candidates=_sort_candidates(fallback_candidates),
                )
            )
            continue

        if len(candidates) == 1:
            candidate = candidates[0]
            resolved_cards.append(
                {
                    "oracle_id": candidate["oracle_id"],
                    "name": candidate["name"],
                    "count": int(count),
                    "source_line_no": int(line_no),
                }
            )
            continue

        if len(candidates) == 0:
            unknowns.append(
                _unknown_row(
                    name_raw=name_raw,
                    name_norm=name_norm,
                    count=count,
                    line_no=line_no,
                    reason_code="CARD_NOT_FOUND",
                    candidates=[],
                )
            )
            continue

        unknowns.append(
            _unknown_row(
                name_raw=name_raw,
                name_norm=name_norm,
                count=count,
                line_no=line_no,
                reason_code="CARD_NAME_AMBIGUOUS",
                candidates=_sort_candidates(candidates),
            )
        )

    return {
        "version": DECKLIST_RESOLVE_VERSION,
        "status": "UNKNOWN_PRESENT" if len(unknowns) > 0 else "OK",
        "db_snapshot_id": db_snapshot_id,
        "resolved_cards": resolved_cards,
        "unknowns": unknowns,
    }
