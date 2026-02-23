from __future__ import annotations

import sqlite3
from pathlib import Path

from snapshot_build.migrate_cards_image_columns import ensure_cards_image_columns


def _create_legacy_cards_db(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(
            """
            CREATE TABLE cards (
              snapshot_id TEXT NOT NULL,
              oracle_id TEXT NOT NULL,
              name TEXT NOT NULL,
              PRIMARY KEY (snapshot_id, oracle_id)
            );
            """
        )
        con.commit()
    finally:
        con.close()


def test_ensure_cards_image_columns_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite"
    _create_legacy_cards_db(db_path)

    first = ensure_cards_image_columns(db_path=db_path)
    second = ensure_cards_image_columns(db_path=db_path)

    assert sorted(first["added_columns"]) == ["card_faces_json", "image_status", "image_uris_json"]
    assert second["added_columns"] == []

    con = sqlite3.connect(str(db_path))
    try:
        columns = [row[1] for row in con.execute("PRAGMA table_info(cards)").fetchall()]
    finally:
        con.close()

    assert "image_uris_json" in columns
    assert "card_faces_json" in columns
    assert "image_status" in columns
