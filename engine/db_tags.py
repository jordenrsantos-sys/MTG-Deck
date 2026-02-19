from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List


SQLITE_IN_BATCH_SIZE = 900


class TagSnapshotMissingError(RuntimeError):
    def __init__(self, snapshot_id: str, taxonomy_version: str, missing_oracle_ids: List[str]):
        self.snapshot_id = snapshot_id
        self.taxonomy_version = taxonomy_version
        self.missing_oracle_ids = list(missing_oracle_ids)
        preview = ", ".join(self.missing_oracle_ids[:10])
        suffix = "..." if len(self.missing_oracle_ids) > 10 else ""
        super().__init__(
            "Tags not compiled for snapshot/taxonomy_version. "
            "Run snapshot_build.tag_snapshot. "
            f"snapshot_id={snapshot_id} taxonomy_version={taxonomy_version} "
            f"missing_oracle_ids=[{preview}{suffix}]"
        )


def ensure_tag_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS card_tags (
          oracle_id TEXT NOT NULL,
          snapshot_id TEXT NOT NULL,
          taxonomy_version TEXT NOT NULL,
          ruleset_version TEXT NOT NULL,
          primitive_ids_json TEXT NOT NULL,
          equiv_class_ids_json TEXT NOT NULL,
          facets_json TEXT NOT NULL,
          evidence_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY (oracle_id, snapshot_id, taxonomy_version)
        );

        CREATE TABLE IF NOT EXISTS unknowns_queue (
          id INTEGER PRIMARY KEY,
          oracle_id TEXT,
          snapshot_id TEXT,
          taxonomy_version TEXT,
          rule_id TEXT,
          reason TEXT,
          snippet TEXT,
          created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS patches_applied (
          id INTEGER PRIMARY KEY,
          oracle_id TEXT,
          snapshot_id TEXT,
          taxonomy_version TEXT,
          patch_pack_version TEXT,
          patch_json TEXT,
          created_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_card_tags_lookup
          ON card_tags(snapshot_id, taxonomy_version, oracle_id);
        """
    )


def _parse_json(raw: Any) -> Any:
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return None
    return raw


def _to_str_list(value: Any) -> List[str]:
    parsed = _parse_json(value)
    if not isinstance(parsed, list):
        return []

    out: List[str] = []
    seen: set[str] = set()
    for item in parsed:
        if not isinstance(item, str):
            continue
        if item == "" or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _to_dict(value: Any) -> Dict[str, Any]:
    parsed = _parse_json(value)
    if isinstance(parsed, dict):
        return parsed
    return {}


def _to_evidence_dict(value: Any) -> Dict[str, Any]:
    parsed = _parse_json(value)
    if isinstance(parsed, dict):
        return parsed
    if isinstance(parsed, list):
        return {
            "matches": [item for item in parsed if isinstance(item, dict)],
        }
    return {}


def _decode_row(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "primitive_ids": _to_str_list(row_dict.get("primitive_ids_json")),
        "equiv_ids": _to_str_list(row_dict.get("equiv_class_ids_json")),
        "facets": _to_dict(row_dict.get("facets_json")),
        "evidence": _to_evidence_dict(row_dict.get("evidence_json")),
        "ruleset_version": row_dict.get("ruleset_version") if isinstance(row_dict.get("ruleset_version"), str) else None,
    }


def get_card_tags(
    conn: sqlite3.Connection,
    oracle_id: str,
    snapshot_id: str,
    taxonomy_version: str,
) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT
          oracle_id,
          ruleset_version,
          primitive_ids_json,
          equiv_class_ids_json,
          facets_json,
          evidence_json
        FROM card_tags
        WHERE oracle_id = ? AND snapshot_id = ? AND taxonomy_version = ?
        LIMIT 1
        """,
        (oracle_id, snapshot_id, taxonomy_version),
    ).fetchone()

    if row is None:
        raise TagSnapshotMissingError(
            snapshot_id=snapshot_id,
            taxonomy_version=taxonomy_version,
            missing_oracle_ids=[oracle_id],
        )

    row_dict = dict(row) if isinstance(row, sqlite3.Row) else {
        "oracle_id": row[0],
        "ruleset_version": row[1],
        "primitive_ids_json": row[2],
        "equiv_class_ids_json": row[3],
        "facets_json": row[4],
        "evidence_json": row[5],
    }
    return _decode_row(row_dict)


def bulk_get_card_tags(
    conn: sqlite3.Connection,
    oracle_ids: List[str],
    snapshot_id: str,
    taxonomy_version: str,
) -> Dict[str, Dict[str, Any]]:
    oracle_ids_clean = sorted({oid for oid in oracle_ids if isinstance(oid, str) and oid.strip() != ""})
    if not oracle_ids_clean:
        return {}

    found: Dict[str, Dict[str, Any]] = {}
    for start in range(0, len(oracle_ids_clean), SQLITE_IN_BATCH_SIZE):
        batch = oracle_ids_clean[start : start + SQLITE_IN_BATCH_SIZE]
        placeholders = ",".join(["?"] * len(batch))
        rows = conn.execute(
            f"""
            SELECT
              oracle_id,
              ruleset_version,
              primitive_ids_json,
              equiv_class_ids_json,
              facets_json,
              evidence_json
            FROM card_tags
            WHERE snapshot_id = ?
              AND taxonomy_version = ?
              AND oracle_id IN ({placeholders})
            ORDER BY oracle_id ASC
            """,
            (snapshot_id, taxonomy_version, *batch),
        ).fetchall()

        for row in rows:
            row_dict = dict(row) if isinstance(row, sqlite3.Row) else {
                "oracle_id": row[0],
                "ruleset_version": row[1],
                "primitive_ids_json": row[2],
                "equiv_class_ids_json": row[3],
                "facets_json": row[4],
                "evidence_json": row[5],
            }
            oracle_id = row_dict.get("oracle_id")
            if not isinstance(oracle_id, str) or oracle_id == "":
                continue
            found[oracle_id] = _decode_row(row_dict)

    missing = [oid for oid in oracle_ids_clean if oid not in found]
    if missing:
        raise TagSnapshotMissingError(
            snapshot_id=snapshot_id,
            taxonomy_version=taxonomy_version,
            missing_oracle_ids=missing,
        )

    return {oid: found[oid] for oid in oracle_ids_clean}
