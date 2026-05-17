"""
card_search_v1 — Pillar A.2 AI-facing card search endpoint layer.

Rich-filter card lookup over the snapshot's cards + primitive_to_cards inverted
index. Returns wide annotated pools (never pre-ranked) per the creativity
envelope rule in DESIGN_DECISIONS.md.

Architectural rules served:
  - 1.1 Creativity envelope: sort_by is structural (name|cmc|color_identity_size),
    NEVER a quality score. AI ranks by its own criteria.
  - 1.2 Speed budget: <500ms warm. Uses the primitive_to_cards inverted index
    for primitive filters (avoids per-row JSON parse). Color/legality filters
    do JSON parse but only on the candidate set.
  - 1.4 Honest signal: matched_count = total before pagination; agents see
    when their filter is too narrow.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional, Tuple


CARD_SEARCH_VERSION = "card_search_v1.0"

# Hard cap on returned rows regardless of caller's `limit`. Protects against
# accidentally returning the entire 36k-card snapshot.
_MAX_LIMIT = 500


def search_cards_v1(
    *,
    db_snapshot_id: str,
    taxonomy_version: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    offset: int = 0,
    sort_by: str = "name",
    include: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Run the card search and return v1.0 response shape.

    See ENGINE_API_GUIDE.md /card/search_v1 for filter semantics. Never raises —
    invalid filter values are surfaced in `warnings`.
    """
    warnings: List[Dict[str, str]] = []
    filters = filters or {}
    include = include or ["primitives", "type_line", "cmc", "color_identity"]

    # Clamp limit/offset
    try:
        limit_int = max(0, min(int(limit), _MAX_LIMIT))
    except (TypeError, ValueError):
        limit_int = 100
    try:
        offset_int = max(0, int(offset))
    except (TypeError, ValueError):
        offset_int = 0

    # Resolve taxonomy if not provided
    if taxonomy_version is None:
        taxonomy_version = _resolve_taxonomy(db_snapshot_id, warnings)

    # Build candidate oracle_id set via primitive filters (uses inverted index)
    primitives_any = _clean_str_list(filters.get("primitives_any"))
    primitives_all = _clean_str_list(filters.get("primitives_all"))
    primitives_none = _clean_str_list(filters.get("primitives_none"))

    candidate_oids: Optional[set] = None
    try:
        from engine.db import connect as cards_db_connect
    except Exception as exc:
        warnings.append({"code": "DB_IMPORT_FAILED", "message": f"{exc.__class__.__name__}: {exc}"})
        return _empty_response(warnings)

    try:
        with cards_db_connect() as con:
            # Apply primitive filters via primitive_to_cards
            if primitives_any:
                candidate_oids = _oids_with_primitives_any(
                    con, db_snapshot_id, taxonomy_version, primitives_any
                )
            if primitives_all:
                all_set = _oids_with_primitives_all(
                    con, db_snapshot_id, taxonomy_version, primitives_all
                )
                candidate_oids = all_set if candidate_oids is None else (candidate_oids & all_set)
            if primitives_none:
                exclude_set = _oids_with_primitives_any(
                    con, db_snapshot_id, taxonomy_version, primitives_none
                )
                if candidate_oids is not None:
                    candidate_oids = candidate_oids - exclude_set
                else:
                    # Need a starting universe; defer to post-fetch filter
                    pass

            # Build the cards-table query
            sql_parts = [
                "SELECT name, oracle_id, cmc, type_line, color_identity, mana_cost, primitives_json",
                "FROM cards WHERE snapshot_id = ?",
            ]
            params: List[Any] = [db_snapshot_id]

            if candidate_oids is not None:
                if not candidate_oids:
                    return _empty_response(warnings)
                # IN clause for oracle_id
                oids_list = list(candidate_oids)
                # SQLite has 999-param limit by default; chunk if needed
                if len(oids_list) <= 950:
                    placeholders = ",".join("?" for _ in oids_list)
                    sql_parts.append(f"AND oracle_id IN ({placeholders})")
                    params.extend(oids_list)
                else:
                    # Fall back to no SQL filter; we'll Python-filter after
                    pass

            # cmc bounds
            cmc_min = filters.get("cmc_min")
            cmc_max = filters.get("cmc_max")
            if isinstance(cmc_min, (int, float)):
                sql_parts.append("AND cmc >= ?")
                params.append(float(cmc_min))
            if isinstance(cmc_max, (int, float)):
                sql_parts.append("AND cmc <= ?")
                params.append(float(cmc_max))

            # type_any (matches before the "—"; LIKE search)
            type_any = _clean_str_list(filters.get("type_any"))
            if type_any:
                or_clauses = " OR ".join(["type_line LIKE ?" for _ in type_any])
                sql_parts.append(f"AND ({or_clauses})")
                params.extend([f"%{t}%" for t in type_any])

            # subtype_any (matches after the "—"; LIKE search)
            subtype_any = _clean_str_list(filters.get("subtypes_any"))
            if subtype_any:
                or_clauses = " OR ".join(["type_line LIKE ?" for _ in subtype_any])
                sql_parts.append(f"AND ({or_clauses})")
                params.extend([f"%{s}%" for s in subtype_any])

            # name_contains
            name_contains = filters.get("name_contains")
            if isinstance(name_contains, str) and name_contains.strip():
                sql_parts.append("AND name LIKE ?")
                params.append(f"%{name_contains.strip()}%")

            sql = " ".join(sql_parts)
            rows = con.execute(sql, params).fetchall()

            # Apply Python-side filters that don't fit cleanly in SQL
            results = _apply_python_filters(rows, filters, primitives_none if candidate_oids is None else None, warnings)

            # Sorting
            results = _sort_results(results, sort_by)

            matched_count = len(results)
            page = results[offset_int : offset_int + limit_int]

            # Decorate with annotations from include list
            decorated = _decorate(page, include, db_snapshot_id, taxonomy_version, con)

            return {
                "version": CARD_SEARCH_VERSION,
                "db_snapshot_id": db_snapshot_id,
                "taxonomy_version": taxonomy_version,
                "matched_count": matched_count,
                "returned_count": len(decorated),
                "offset": offset_int,
                "limit": limit_int,
                "sort_by": sort_by,
                "results": decorated,
                "warnings": warnings,
            }
    except Exception as exc:
        warnings.append({"code": "SEARCH_FAILED", "message": f"{exc.__class__.__name__}: {exc}"})
        return _empty_response(warnings)


# ============================================================
# Helpers
# ============================================================


def _empty_response(warnings: List[Dict[str, str]]) -> Dict[str, Any]:
    return {
        "version": CARD_SEARCH_VERSION,
        "db_snapshot_id": "",
        "taxonomy_version": None,
        "matched_count": 0,
        "returned_count": 0,
        "offset": 0,
        "limit": 0,
        "sort_by": "name",
        "results": [],
        "warnings": warnings,
    }


def _resolve_taxonomy(db_snapshot_id: str, warnings: List[Dict[str, str]]) -> Optional[str]:
    try:
        from engine.db import connect as cards_db_connect
        from api.engine.version_resolve_v1 import resolve_runtime_taxonomy_version
        with cards_db_connect() as con:
            return resolve_runtime_taxonomy_version(
                snapshot_id=db_snapshot_id, requested=None, db=con
            )
    except Exception as exc:
        warnings.append({"code": "TAXONOMY_RESOLVE_FAILED", "message": str(exc)})
        return None


def _clean_str_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    return [v.strip() for v in value if isinstance(v, str) and v.strip()]


def _oids_with_primitives_any(
    con, snapshot_id: str, taxonomy_version: Optional[str], primitives: List[str]
) -> set:
    if not primitives or not taxonomy_version:
        return set()
    placeholders = ",".join("?" for _ in primitives)
    sql = (
        "SELECT DISTINCT oracle_id FROM primitive_to_cards "
        "WHERE snapshot_id = ? AND taxonomy_version = ? AND primitive_id IN (" + placeholders + ")"
    )
    params: List[Any] = [snapshot_id, taxonomy_version] + primitives
    return {r[0] for r in con.execute(sql, params).fetchall() if r and r[0]}


def _oids_with_primitives_all(
    con, snapshot_id: str, taxonomy_version: Optional[str], primitives: List[str]
) -> set:
    """Cards that have ALL of the given primitives (intersection)."""
    if not primitives or not taxonomy_version:
        return set()
    result: Optional[set] = None
    for prim in primitives:
        sql = (
            "SELECT DISTINCT oracle_id FROM primitive_to_cards "
            "WHERE snapshot_id = ? AND taxonomy_version = ? AND primitive_id = ?"
        )
        oids = {r[0] for r in con.execute(sql, [snapshot_id, taxonomy_version, prim]).fetchall() if r and r[0]}
        result = oids if result is None else (result & oids)
        if not result:
            return set()
    return result or set()


def _apply_python_filters(
    rows: Iterable[Any],
    filters: Dict[str, Any],
    primitives_none_fallback: Optional[List[str]],
    warnings: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """Apply filters that didn't fit in SQL: color_identity_subset_of,
    color_identity_includes_all, and the primitives_none fallback when
    SQL-side filtering wasn't applied."""
    color_subset = filters.get("color_identity_subset_of")
    color_includes_all = filters.get("color_identity_includes_all")
    color_subset_set = _color_set(color_subset)
    color_includes_set = _color_set(color_includes_all)

    out: List[Dict[str, Any]] = []
    for row in rows:
        d = dict(row) if hasattr(row, "keys") else dict(row)
        ci = _parse_color_identity(d.get("color_identity"))
        d["_color_identity_parsed"] = ci
        if color_subset_set is not None:
            if not ci.issubset(color_subset_set):
                continue
        if color_includes_set is not None:
            if not color_includes_set.issubset(ci):
                continue
        out.append(d)
    return out


def _color_set(value: Any) -> Optional[set]:
    if not isinstance(value, list):
        return None
    s: set = set()
    for v in value:
        if isinstance(v, str) and v.strip():
            s.add(v.strip().upper())
    return s


def _parse_color_identity(value: Any) -> set:
    if isinstance(value, list):
        return {c.upper() for c in value if isinstance(c, str) and c}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return {c.upper() for c in parsed if isinstance(c, str) and c}
        except Exception:
            pass
        return {c.strip().upper() for c in value.split(",") if c.strip()}
    return set()


def _sort_results(results: List[Dict[str, Any]], sort_by: str) -> List[Dict[str, Any]]:
    """Sort by structural keys only — never a quality score (creativity envelope)."""
    if sort_by == "cmc":
        return sorted(results, key=lambda r: (r.get("cmc") or 0, r.get("name", "")))
    if sort_by == "color_identity_size":
        return sorted(results, key=lambda r: (len(r.get("_color_identity_parsed", set())), r.get("name", "")))
    # Default: name
    return sorted(results, key=lambda r: r.get("name", ""))


def _decorate(
    rows: List[Dict[str, Any]],
    include: List[str],
    snapshot_id: str,
    taxonomy_version: Optional[str],
    con,
) -> List[Dict[str, Any]]:
    """Attach requested annotations to each row."""
    # Bulk-fetch primitives for the returned set (efficient single query)
    primitives_by_oid: Dict[str, List[str]] = {}
    if "primitives" in include and taxonomy_version and rows:
        oids = [r["oracle_id"] for r in rows if r.get("oracle_id")]
        if oids:
            # Chunk to stay under SQLite param limit
            for chunk_start in range(0, len(oids), 900):
                chunk = oids[chunk_start : chunk_start + 900]
                placeholders = ",".join("?" for _ in chunk)
                sql = (
                    "SELECT oracle_id, primitive_id FROM primitive_to_cards "
                    "WHERE snapshot_id = ? AND taxonomy_version = ? AND oracle_id IN (" + placeholders + ")"
                )
                for oid, prim in con.execute(sql, [snapshot_id, taxonomy_version] + chunk).fetchall():
                    primitives_by_oid.setdefault(oid, []).append(prim)

    decorated: List[Dict[str, Any]] = []
    for r in rows:
        out: Dict[str, Any] = {
            "oracle_id": r.get("oracle_id"),
            "name": r.get("name"),
        }
        if "type_line" in include:
            out["type_line"] = r.get("type_line")
        if "cmc" in include:
            out["cmc"] = r.get("cmc")
        if "color_identity" in include:
            out["color_identity"] = sorted(list(r.get("_color_identity_parsed", set())))
        if "mana_cost" in include:
            out["mana_cost"] = r.get("mana_cost")
        if "primitives" in include:
            out["primitives"] = sorted(primitives_by_oid.get(r.get("oracle_id"), []))
        decorated.append(out)
    return decorated
