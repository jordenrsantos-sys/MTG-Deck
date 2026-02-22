from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Tuple

from engine.db import connect


def _json_list(raw: Any) -> List[str]:
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
    else:
        parsed = raw

    if not isinstance(parsed, list):
        return []
    return sorted({item for item in parsed if isinstance(item, str) and item != ""})


def _table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return row is not None


def ensure_runtime_tag_indices(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_card_tags_lookup
          ON card_tags(snapshot_id, taxonomy_version, oracle_id);

        CREATE TABLE IF NOT EXISTS primitive_to_cards (
          primitive_id TEXT NOT NULL,
          oracle_id TEXT NOT NULL,
          snapshot_id TEXT NOT NULL,
          taxonomy_version TEXT NOT NULL,
          PRIMARY KEY (primitive_id, oracle_id, snapshot_id, taxonomy_version)
        );

        CREATE TABLE IF NOT EXISTS equiv_to_cards (
          equiv_id TEXT NOT NULL,
          oracle_id TEXT NOT NULL,
          snapshot_id TEXT NOT NULL,
          taxonomy_version TEXT NOT NULL,
          PRIMARY KEY (equiv_id, oracle_id, snapshot_id, taxonomy_version)
        );

        CREATE INDEX IF NOT EXISTS idx_primitive_to_cards_lookup
          ON primitive_to_cards(snapshot_id, taxonomy_version, primitive_id);

        CREATE INDEX IF NOT EXISTS idx_equiv_to_cards_lookup
          ON equiv_to_cards(snapshot_id, taxonomy_version, equiv_id);
        """
    )

    if _table_exists(con, "primitive_to_cards"):
        con.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_primitive_to_cards_oracle_lookup
              ON primitive_to_cards(snapshot_id, taxonomy_version, oracle_id);

            CREATE INDEX IF NOT EXISTS idx_primitive_to_cards_primitive_oracle
              ON primitive_to_cards(snapshot_id, taxonomy_version, primitive_id, oracle_id);
            """
        )

    if _table_exists(con, "cards"):
        con.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_cards_snapshot_name_oracle
              ON cards(snapshot_id, name, oracle_id);

            CREATE INDEX IF NOT EXISTS idx_cards_snapshot_name_lower
              ON cards(snapshot_id, LOWER(name));

            CREATE INDEX IF NOT EXISTS idx_cards_oracle_id
              ON cards(oracle_id);
            """
        )


def rebuild_inverted_indices(
    con: sqlite3.Connection,
    snapshot_id: str,
    taxonomy_version: str,
) -> Dict[str, int]:
    con.execute(
        "DELETE FROM primitive_to_cards WHERE snapshot_id = ? AND taxonomy_version = ?",
        (snapshot_id, taxonomy_version),
    )
    con.execute(
        "DELETE FROM equiv_to_cards WHERE snapshot_id = ? AND taxonomy_version = ?",
        (snapshot_id, taxonomy_version),
    )

    rows = con.execute(
        """
        SELECT oracle_id, primitive_ids_json, equiv_class_ids_json
        FROM card_tags
        WHERE snapshot_id = ? AND taxonomy_version = ?
        ORDER BY oracle_id ASC
        """,
        (snapshot_id, taxonomy_version),
    ).fetchall()

    primitive_rows: List[Tuple[str, str, str, str]] = []
    equiv_rows: List[Tuple[str, str, str, str]] = []

    for row in rows:
        row_dict = dict(row)
        oracle_id = row_dict.get("oracle_id")
        if not isinstance(oracle_id, str) or oracle_id == "":
            continue

        primitive_ids = _json_list(row_dict.get("primitive_ids_json"))
        equiv_ids = _json_list(row_dict.get("equiv_class_ids_json"))

        for primitive_id in primitive_ids:
            primitive_rows.append((primitive_id, oracle_id, snapshot_id, taxonomy_version))
        for equiv_id in equiv_ids:
            equiv_rows.append((equiv_id, oracle_id, snapshot_id, taxonomy_version))

    primitive_rows = sorted(
        set(primitive_rows),
        key=lambda row: (row[0], row[1], row[2], row[3]),
    )
    equiv_rows = sorted(
        set(equiv_rows),
        key=lambda row: (row[0], row[1], row[2], row[3]),
    )

    if primitive_rows:
        con.executemany(
            """
            INSERT OR REPLACE INTO primitive_to_cards (
              primitive_id,
              oracle_id,
              snapshot_id,
              taxonomy_version
            ) VALUES (?, ?, ?, ?)
            """,
            primitive_rows,
        )

    if equiv_rows:
        con.executemany(
            """
            INSERT OR REPLACE INTO equiv_to_cards (
              equiv_id,
              oracle_id,
              snapshot_id,
              taxonomy_version
            ) VALUES (?, ?, ?, ?)
            """,
            equiv_rows,
        )

    return {
        "card_tags_scanned": len(rows),
        "primitive_to_cards_rows": len(primitive_rows),
        "equiv_to_cards_rows": len(equiv_rows),
    }


def build_indices(snapshot_id: str, taxonomy_version: str) -> Dict[str, Any]:
    with connect() as con:
        ensure_runtime_tag_indices(con)
        summary = rebuild_inverted_indices(
            con=con,
            snapshot_id=snapshot_id,
            taxonomy_version=taxonomy_version,
        )
        con.commit()

    return {
        "snapshot_id": snapshot_id,
        "taxonomy_version": taxonomy_version,
        "indices_built": True,
        **summary,
    }
