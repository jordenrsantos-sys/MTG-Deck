from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Tuple

from engine.db import connect as cards_db_connect
from engine.db_tags import ensure_tag_tables


def _normalize_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned if cleaned != "" else None


def _connect_for_resolution(db: Any) -> Tuple[sqlite3.Connection, bool]:
    if isinstance(db, sqlite3.Connection):
        return db, False

    if isinstance(db, (str, Path)):
        con = sqlite3.connect(str(db))
        con.row_factory = sqlite3.Row
        return con, True

    con = cards_db_connect()
    return con, True


def resolve_runtime_taxonomy_version(snapshot_id: str, requested: Any = None, db: Any = None) -> str | None:
    snapshot_id_clean = _normalize_str(snapshot_id)
    if snapshot_id_clean is None:
        return None

    requested_clean = _normalize_str(requested)
    if requested_clean is not None:
        return requested_clean

    con, should_close = _connect_for_resolution(db)
    try:
        ensure_tag_tables(con)
        row = con.execute(
            """
            SELECT taxonomy_version
            FROM card_tags
            WHERE snapshot_id = ?
            GROUP BY taxonomy_version
            ORDER BY taxonomy_version DESC
            LIMIT 1
            """,
            (snapshot_id_clean,),
        ).fetchone()
    except Exception:
        return None
    finally:
        if should_close:
            con.close()

    if row and isinstance(row[0], str) and row[0].strip() != "":
        return row[0]
    return None


def resolve_runtime_ruleset_version(
    snapshot_id: str,
    taxonomy_version: str | None,
    requested: Any = None,
    db: Any = None,
) -> str | None:
    snapshot_id_clean = _normalize_str(snapshot_id)
    taxonomy_version_clean = _normalize_str(taxonomy_version)
    if snapshot_id_clean is None or taxonomy_version_clean is None:
        return None

    requested_clean = _normalize_str(requested)
    if requested_clean is not None:
        return requested_clean

    con, should_close = _connect_for_resolution(db)
    try:
        ensure_tag_tables(con)
        row = con.execute(
            """
            SELECT ruleset_version
            FROM card_tags
            WHERE snapshot_id = ?
              AND taxonomy_version = ?
            GROUP BY ruleset_version
            ORDER BY ruleset_version DESC
            LIMIT 1
            """,
            (snapshot_id_clean, taxonomy_version_clean),
        ).fetchone()
    except Exception:
        return None
    finally:
        if should_close:
            con.close()

    if row and isinstance(row[0], str) and row[0].strip() != "":
        return row[0]
    return None
