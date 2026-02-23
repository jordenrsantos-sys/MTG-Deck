from __future__ import annotations

import sqlite3
from pathlib import Path

from snapshot_build.migrate_card_images_table import ensure_card_images_table


def _create_cards_only_db(db_path: Path) -> None:
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


def test_ensure_card_images_table_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "card_images.sqlite"
    _create_cards_only_db(db_path)

    first = ensure_card_images_table(db_path=db_path)
    second = ensure_card_images_table(db_path=db_path)

    assert first["created_table"] is True
    assert second["created_table"] is False

    con = sqlite3.connect(str(db_path))
    try:
        columns = [row[1] for row in con.execute("PRAGMA table_info(card_images)").fetchall()]
    finally:
        con.close()

    assert "oracle_id" in columns
    assert "img_normal_uri" in columns
    assert "img_small_uri" in columns
    assert "img_source" in columns
    assert "img_enriched_at" in columns
    assert "img_bulk_version" in columns
