from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Tuple

from engine.db import connect as cards_db_connect
from engine.db import snapshot_exists

SNAPSHOT_PREFLIGHT_V1_VERSION = "snapshot_preflight_v1"

_REQUIRED_SNAPSHOTS_COLUMNS = (
    "snapshot_id",
    "created_at",
    "source",
    "scryfall_bulk_uri",
    "scryfall_bulk_updated_at",
    "manifest_json",
)


def _normalize_snapshot_id(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _connect_for_preflight(db: Any) -> Tuple[sqlite3.Connection, bool]:
    if isinstance(db, sqlite3.Connection):
        return db, False

    if isinstance(db, (str, Path)):
        con = sqlite3.connect(str(db))
        con.row_factory = sqlite3.Row
        return con, True

    con = cards_db_connect()
    return con, True


def _snapshot_schema_ok(con: sqlite3.Connection) -> bool:
    try:
        rows = con.execute("PRAGMA table_info(snapshots)").fetchall()
    except sqlite3.Error:
        return False

    column_names = set()
    for row in rows:
        if isinstance(row, sqlite3.Row):
            name_value = row["name"] if "name" in row.keys() else None
            if isinstance(name_value, str):
                column_names.add(name_value)
            continue

        if isinstance(row, (tuple, list)) and len(row) > 1 and isinstance(row[1], str):
            column_names.add(row[1])

    return all(required in column_names for required in _REQUIRED_SNAPSHOTS_COLUMNS)


def _snapshot_exists_via_connection(con: sqlite3.Connection, snapshot_id: str) -> bool:
    if snapshot_id == "":
        return False

    try:
        row = con.execute(
            "SELECT 1 FROM snapshots WHERE snapshot_id = ? LIMIT 1",
            (snapshot_id,),
        ).fetchone()
    except sqlite3.Error:
        return False

    return row is not None


def _read_manifest_raw(con: sqlite3.Connection, snapshot_id: str) -> Any:
    try:
        row = con.execute(
            "SELECT manifest_json FROM snapshots WHERE snapshot_id = ? LIMIT 1",
            (snapshot_id,),
        ).fetchone()
    except sqlite3.Error:
        return None

    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        return row["manifest_json"] if "manifest_json" in row.keys() else None
    if isinstance(row, (tuple, list)) and len(row) > 0:
        return row[0]
    return None


def _manifest_present(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def _tags_compiled_from_manifest(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return False

    if not isinstance(parsed, dict):
        return False
    return bool(parsed.get("tags_compiled"))


def run_snapshot_preflight_v1(db: Any, snapshot_id: Any) -> Dict[str, Any]:
    snapshot_id_clean = _normalize_snapshot_id(snapshot_id)

    con, should_close = _connect_for_preflight(db)
    try:
        schema_ok = _snapshot_schema_ok(con)

        if isinstance(db, sqlite3.Connection):
            snapshot_exists_check = _snapshot_exists_via_connection(con=con, snapshot_id=snapshot_id_clean)
        else:
            snapshot_exists_check = snapshot_exists(snapshot_id_clean) if schema_ok else False

        manifest_raw = _read_manifest_raw(con=con, snapshot_id=snapshot_id_clean) if snapshot_exists_check else None
        manifest_present = _manifest_present(manifest_raw)

        tags_compiled: bool | None
        if snapshot_exists_check and manifest_present:
            tags_compiled = _tags_compiled_from_manifest(manifest_raw)
        elif snapshot_exists_check:
            tags_compiled = None
        else:
            tags_compiled = None

        checks: Dict[str, Any] = {
            "snapshot_exists": snapshot_exists_check,
            "manifest_present": manifest_present,
            "tags_compiled": tags_compiled,
            "schema_ok": schema_ok,
        }

        errors: list[dict[str, str]] = []
        if not schema_ok:
            errors.append(
                {
                    "code": "SNAPSHOTS_SCHEMA_INVALID",
                    "message": "Snapshots schema is missing required columns.",
                }
            )
        if not snapshot_exists_check:
            errors.append(
                {
                    "code": "SNAPSHOT_NOT_FOUND",
                    "message": "Snapshot ID not found in local DB.",
                }
            )
        if snapshot_exists_check and not manifest_present:
            errors.append(
                {
                    "code": "SNAPSHOT_MANIFEST_MISSING",
                    "message": "Snapshot manifest_json is missing or empty.",
                }
            )
        if snapshot_exists_check and manifest_present and tags_compiled is False:
            errors.append(
                {
                    "code": "SNAPSHOT_TAGS_NOT_COMPILED",
                    "message": "Snapshot manifest_json indicates tags_compiled is not truthy.",
                }
            )

        return {
            "version": SNAPSHOT_PREFLIGHT_V1_VERSION,
            "snapshot_id": snapshot_id_clean,
            "status": "OK" if len(errors) == 0 else "ERROR",
            "errors": errors,
            "checks": checks,
        }
    finally:
        if should_close:
            con.close()
