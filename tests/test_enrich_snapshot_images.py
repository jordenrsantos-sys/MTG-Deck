from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from snapshot_build.enrich_snapshot_images import enrich_snapshot_images


SNAPSHOT_ID = "snap_enrich"
ORACLE_ID_IN_BULK = "11111111-1111-1111-1111-111111111111"
ORACLE_ID_MISSING_FROM_BULK = "22222222-2222-2222-2222-222222222222"
ORACLE_ID_BULK_NOT_IN_DB = "33333333-3333-3333-3333-333333333333"


def _create_cards_db(db_path: Path) -> None:
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
        con.executemany(
            """
            INSERT INTO cards (snapshot_id, oracle_id, name)
            VALUES (?, ?, ?)
            """,
            [
                (SNAPSHOT_ID, ORACLE_ID_IN_BULK, "Card In Bulk"),
                (SNAPSHOT_ID, ORACLE_ID_MISSING_FROM_BULK, "Card Missing From Bulk"),
                ("snap_other", ORACLE_ID_IN_BULK, "Card In Other Snapshot"),
            ],
        )
        con.commit()
    finally:
        con.close()


def _write_bulk_json(path: Path) -> None:
    payload = [
        {
            "id": "print-2",
            "oracle_id": ORACLE_ID_IN_BULK,
            "released_at": "2024-01-01",
            "set": "bbb",
            "collector_number": "2",
            "lang": "en",
            "image_uris": {
                "normal": "https://img.example/new-normal.jpg",
                "small": "https://img.example/new-small.jpg",
            },
        },
        {
            "id": "print-1",
            "oracle_id": ORACLE_ID_IN_BULK,
            "released_at": "2023-01-01",
            "set": "aaa",
            "collector_number": "1",
            "lang": "en",
            "image_uris": {
                "normal": "https://img.example/old-normal.jpg",
                "small": "https://img.example/old-small.jpg",
            },
            "card_faces": [
                {
                    "name": "Face A",
                    "image_uris": {
                        "normal": "https://img.example/face-a-normal.jpg",
                    },
                }
            ],
        },
        {
            "id": "print-3",
            "oracle_id": ORACLE_ID_BULK_NOT_IN_DB,
            "released_at": "2022-01-01",
            "set": "ccc",
            "collector_number": "3",
            "lang": "en",
            "image_uris": {
                "normal": "https://img.example/extra-normal.jpg",
                "small": "https://img.example/extra-small.jpg",
            },
        },
    ]
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_enrich_snapshot_images_updates_snapshot_rows_deterministically(tmp_path: Path) -> None:
    db_path = tmp_path / "cards.sqlite"
    _create_cards_db(db_path)

    bulk_path = tmp_path / "default-cards.json"
    _write_bulk_json(bulk_path)

    summary = enrich_snapshot_images(
        db_path=db_path,
        snapshot_id=SNAPSHOT_ID,
        bulk_json_path=bulk_path,
    )

    assert summary["updated_rows"] == 1
    assert summary["missing_oracle_ids"] == 1
    assert summary["bulk_oracle_ids_not_in_db"] == 1

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        target = con.execute(
            """
            SELECT image_uris_json, card_faces_json
            FROM cards
            WHERE snapshot_id = ? AND oracle_id = ?
            """,
            (SNAPSHOT_ID, ORACLE_ID_IN_BULK),
        ).fetchone()
        other = con.execute(
            """
            SELECT image_uris_json, card_faces_json
            FROM cards
            WHERE snapshot_id = ? AND oracle_id = ?
            """,
            ("snap_other", ORACLE_ID_IN_BULK),
        ).fetchone()
    finally:
        con.close()

    assert target is not None
    assert other is not None

    image_uris = json.loads(target["image_uris_json"])
    assert image_uris["normal"] == "https://img.example/old-normal.jpg"
    assert image_uris["small"] == "https://img.example/old-small.jpg"

    card_faces = json.loads(target["card_faces_json"])
    assert isinstance(card_faces, list)
    assert card_faces[0]["name"] == "Face A"

    assert other["image_uris_json"] is None
    assert other["card_faces_json"] is None
