import json
import sqlite3
from typing import Any, Dict, List


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def _supports_json1(con: sqlite3.Connection) -> bool:
    try:
        con.execute("SELECT json_valid('[]')").fetchone()
        return True
    except sqlite3.OperationalError:
        return False


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS primitive_defs_v0 (
          primitive_id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          description TEXT NOT NULL,
          category TEXT NOT NULL,
          is_engine_primitive INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS primitive_rules_v0 (
          rule_id TEXT PRIMARY KEY,
          primitive_id TEXT NOT NULL,
          rule_type TEXT NOT NULL,
          pattern TEXT NOT NULL,
          weight REAL NOT NULL DEFAULT 1.0,
          priority INTEGER NOT NULL DEFAULT 100,
          notes TEXT,
          enabled INTEGER NOT NULL DEFAULT 1,
          ruleset_version TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS card_primitive_tags_v0 (
          oracle_id TEXT NOT NULL,
          card_name TEXT NOT NULL,
          primitive_id TEXT NOT NULL,
          ruleset_version TEXT NOT NULL,
          confidence REAL NOT NULL,
          evidence_json TEXT NOT NULL,
          PRIMARY KEY (oracle_id, primitive_id, ruleset_version)
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_cpt_v0_primitive ON card_primitive_tags_v0(primitive_id, ruleset_version)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_cpt_v0_oracle ON card_primitive_tags_v0(oracle_id, ruleset_version)"
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS primitive_tag_runs_v0 (
          run_id TEXT PRIMARY KEY,
          db_snapshot_id TEXT NOT NULL,
          ruleset_version TEXT NOT NULL,
          cards_processed INTEGER NOT NULL,
          tags_emitted INTEGER NOT NULL,
          unknowns_emitted INTEGER NOT NULL,
          run_hash_v1 TEXT NOT NULL,
          created_at TEXT
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_primitive_tag_runs_v0_snapshot ON primitive_tag_runs_v0(db_snapshot_id, created_at)"
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS primitive_tag_unknowns_v0 (
          oracle_id TEXT NOT NULL,
          card_name TEXT NOT NULL,
          reason TEXT NOT NULL,
          details_json TEXT NOT NULL,
          ruleset_version TEXT NOT NULL,
          PRIMARY KEY (oracle_id, reason, ruleset_version)
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_primitive_tag_unknowns_v0_ruleset ON primitive_tag_unknowns_v0(ruleset_version, reason)"
    )


def _normalize_ci(value: Any) -> List[str]:
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


def _ci_allowed(card_ci_raw: Any, requested_ci: list[str] | None) -> bool:
    requested = [c for c in (requested_ci or []) if isinstance(c, str)]
    if not requested:
        return True
    card_ci = _normalize_ci(card_ci_raw)
    return set(card_ci).issubset(set(requested))


def _latest_snapshot_for_ruleset(con: sqlite3.Connection, ruleset_version: str) -> str | None:
    try:
        row = con.execute(
            """
            SELECT db_snapshot_id
            FROM primitive_tag_runs_v0
            WHERE ruleset_version = ?
            ORDER BY created_at DESC, run_id DESC
            LIMIT 1
            """,
            (ruleset_version,),
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    if row is not None and isinstance(row[0], str) and row[0] != "":
        return row[0]

    try:
        snapshot_row = con.execute(
            "SELECT snapshot_id FROM snapshots ORDER BY created_at DESC, snapshot_id DESC LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError:
        snapshot_row = None
    if snapshot_row is not None and isinstance(snapshot_row[0], str) and snapshot_row[0] != "":
        return snapshot_row[0]

    return None


def _latest_ruleset_version(con: sqlite3.Connection) -> str | None:
    try:
        row = con.execute(
            """
            SELECT ruleset_version
            FROM primitive_tag_runs_v0
            ORDER BY created_at DESC, run_id DESC
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    if row is not None and isinstance(row[0], str) and row[0] != "":
        return row[0]
    return None


def get_cards_for_primitive_v0(
    db_path: str,
    ruleset_version: str,
    primitive_id: str,
    limit: int = 200,
) -> list[dict]:
    """
    Returns [{oracle_id, name, confidence}] ordered deterministically:
    confidence desc, name asc, oracle_id asc
    """

    limit_safe = max(1, int(limit))
    with _connect(db_path) as con:
        try:
            rows = con.execute(
                """
                SELECT
                  oracle_id,
                  card_name,
                  confidence
                FROM card_primitive_tags_v0
                WHERE ruleset_version = ?
                  AND primitive_id = ?
                ORDER BY confidence DESC, card_name ASC, oracle_id ASC
                LIMIT ?
                """,
                (ruleset_version, primitive_id, limit_safe),
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []

    return [
        {
            "oracle_id": row["oracle_id"],
            "name": row["card_name"],
            "confidence": float(row["confidence"]),
        }
        for row in rows
    ]


def get_candidates_for_primitives_v0(
    db_path: str,
    ruleset_version: str,
    primitive_ids: list[str],
    color_identity: list[str] | None,
    exclude_oracle_ids: set[str] | None,
    limit_per_primitive: int = 200,
) -> dict[str, list[dict]]:
    primitive_ids_clean = sorted(set([
        primitive
        for primitive in (primitive_ids or [])
        if isinstance(primitive, str) and primitive.strip() != ""
    ]))
    exclude_ids = set([
        oid
        for oid in (exclude_oracle_ids or set())
        if isinstance(oid, str) and oid != ""
    ])
    limit_safe = max(1, int(limit_per_primitive))
    requested_ci = sorted(set([
        color
        for color in (color_identity or [])
        if isinstance(color, str) and color != ""
    ]))

    out: Dict[str, List[Dict[str, Any]]] = {}

    with _connect(db_path) as con:
        snapshot_id = _latest_snapshot_for_ruleset(con=con, ruleset_version=ruleset_version)
        json1_available = _supports_json1(con)

        for primitive_id in primitive_ids_clean:
            applied_sql_ci_filter = bool(snapshot_id is not None and requested_ci and json1_available)
            if snapshot_id is not None:
                sql_params: List[Any] = [snapshot_id, ruleset_version, primitive_id]
                ci_clause = ""
                # Use SQL color-identity filtering when JSON1 is available.
                if applied_sql_ci_filter:
                    placeholders = ",".join(["?"] * len(requested_ci))
                    ci_clause = (
                        " AND NOT EXISTS ("
                        "   SELECT 1 "
                        "   FROM json_each(COALESCE(c.color_identity, '[]')) card_ci "
                        f"   WHERE card_ci.value NOT IN ({placeholders})"
                        " )"
                    )
                    sql_params.extend(requested_ci)

                sql_params.append(limit_safe * 4)
                query = (
                    "SELECT "
                    "  t.oracle_id, "
                    "  t.card_name, "
                    "  t.confidence, "
                    "  c.color_identity AS card_color_identity "
                    "FROM card_primitive_tags_v0 t "
                    "LEFT JOIN cards c "
                    "  ON c.snapshot_id = ? "
                    " AND c.oracle_id = t.oracle_id "
                    "WHERE t.ruleset_version = ? "
                    "  AND t.primitive_id = ?"
                    f"{ci_clause} "
                    "ORDER BY t.confidence DESC, t.card_name ASC, t.oracle_id ASC "
                    "LIMIT ?"
                )

                try:
                    rows = con.execute(query, tuple(sql_params)).fetchall()
                except sqlite3.OperationalError:
                    try:
                        rows = con.execute(
                            """
                            SELECT
                              t.oracle_id,
                              t.card_name,
                              t.confidence,
                              c.color_identity AS card_color_identity
                            FROM card_primitive_tags_v0 t
                            LEFT JOIN cards c
                              ON c.snapshot_id = ?
                             AND c.oracle_id = t.oracle_id
                            WHERE t.ruleset_version = ?
                              AND t.primitive_id = ?
                            ORDER BY t.confidence DESC, t.card_name ASC, t.oracle_id ASC
                            LIMIT ?
                            """,
                            (snapshot_id, ruleset_version, primitive_id, limit_safe * 4),
                        ).fetchall()
                    except sqlite3.OperationalError:
                        rows = []
            else:
                # No snapshot context available; omit color-identity filtering in v0.
                try:
                    rows = con.execute(
                        """
                        SELECT
                          oracle_id,
                          card_name,
                          confidence,
                          NULL AS card_color_identity
                        FROM card_primitive_tags_v0
                        WHERE ruleset_version = ?
                          AND primitive_id = ?
                        ORDER BY confidence DESC, card_name ASC, oracle_id ASC
                        LIMIT ?
                        """,
                        (ruleset_version, primitive_id, limit_safe * 4),
                    ).fetchall()
                except sqlite3.OperationalError:
                    rows = []

            candidates: List[Dict[str, Any]] = []
            for row in rows:
                oracle_id = row["oracle_id"]
                if not isinstance(oracle_id, str) or oracle_id in exclude_ids:
                    continue
                if requested_ci and not applied_sql_ci_filter:
                    card_ci_raw = row["card_color_identity"] if "card_color_identity" in row.keys() else None
                    if not _ci_allowed(card_ci_raw=card_ci_raw, requested_ci=requested_ci):
                        continue

                candidates.append(
                    {
                        "oracle_id": oracle_id,
                        "name": row["card_name"],
                        "confidence": float(row["confidence"]),
                    }
                )
                if len(candidates) >= limit_safe:
                    break

            out[primitive_id] = candidates

    return out


def get_primitive_tag_index_status_v0(db_path: str, db_snapshot_id: str | None) -> dict:
    with _connect(db_path) as con:
        try:
            ruleset_rows = con.execute(
                "SELECT DISTINCT ruleset_version FROM primitive_tag_runs_v0 ORDER BY ruleset_version ASC"
            ).fetchall()
        except sqlite3.OperationalError:
            ruleset_rows = []
        available_ruleset_versions = [
            row["ruleset_version"]
            for row in ruleset_rows
            if isinstance(row["ruleset_version"], str)
        ]

        try:
            latest_global_row = con.execute(
                """
                SELECT
                  run_id,
                  db_snapshot_id,
                  ruleset_version,
                  cards_processed,
                  tags_emitted,
                  unknowns_emitted,
                  run_hash_v1,
                  created_at
                FROM primitive_tag_runs_v0
                ORDER BY created_at DESC, run_id DESC
                LIMIT 1
                """
            ).fetchone()
        except sqlite3.OperationalError:
            latest_global_row = None

        latest_for_snapshot_row = None
        if isinstance(db_snapshot_id, str) and db_snapshot_id != "":
            try:
                latest_for_snapshot_row = con.execute(
                    """
                    SELECT
                      run_id,
                      db_snapshot_id,
                      ruleset_version,
                      cards_processed,
                      tags_emitted,
                      unknowns_emitted,
                      run_hash_v1,
                      created_at
                    FROM primitive_tag_runs_v0
                    WHERE db_snapshot_id = ?
                    ORDER BY created_at DESC, run_id DESC
                    LIMIT 1
                    """,
                    (db_snapshot_id,),
                ).fetchone()
            except sqlite3.OperationalError:
                latest_for_snapshot_row = None

    return {
        "db_snapshot_id": db_snapshot_id,
        "available_ruleset_versions": available_ruleset_versions,
        "latest_run_global": dict(latest_global_row) if latest_global_row is not None else None,
        "latest_run_for_snapshot": dict(latest_for_snapshot_row) if latest_for_snapshot_row is not None else None,
    }


def resolve_ruleset_version_v0(db_path: str, requested_ruleset_version: str | None) -> str | None:
    if isinstance(requested_ruleset_version, str) and requested_ruleset_version.strip() != "":
        return requested_ruleset_version.strip()
    with _connect(db_path) as con:
        return _latest_ruleset_version(con)
