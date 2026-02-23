from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from snapshot_build.enrich_images_from_scryfall_bulk import enrich_images_from_scryfall_bulk

SNAPSHOT_ID = "SNAP_STEP4A"
ORACLE_ID_DIRECT = "11111111-1111-1111-1111-111111111111"
ORACLE_ID_FACE_FALLBACK = "22222222-2222-2222-2222-222222222222"
ORACLE_ID_NOT_IN_BULK = "33333333-3333-3333-3333-333333333333"
ORACLE_ID_BULK_NOT_IN_DB = "44444444-4444-4444-4444-444444444444"


def _create_fixture_db(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(
            """
            CREATE TABLE snapshots (
              snapshot_id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              source TEXT NOT NULL,
              scryfall_bulk_uri TEXT NOT NULL,
              scryfall_bulk_updated_at TEXT,
              manifest_json TEXT NOT NULL
            );

            CREATE TABLE cards (
              snapshot_id TEXT NOT NULL,
              oracle_id TEXT NOT NULL,
              name TEXT NOT NULL,
              PRIMARY KEY (snapshot_id, oracle_id)
            );
            """
        )

        con.execute(
            """
            INSERT INTO snapshots (
              snapshot_id,
              created_at,
              source,
              scryfall_bulk_uri,
              scryfall_bulk_updated_at,
              manifest_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                SNAPSHOT_ID,
                "2026-02-22T00:00:00+00:00",
                "unit-test",
                "local://bulk/default-cards",
                "2026-02-22T00:00:00+00:00",
                json.dumps({"tags_compiled": True}),
            ),
        )

        con.executemany(
            """
            INSERT INTO cards (snapshot_id, oracle_id, name)
            VALUES (?, ?, ?)
            """,
            [
                (SNAPSHOT_ID, ORACLE_ID_DIRECT, "Direct URI Card"),
                (SNAPSHOT_ID, ORACLE_ID_FACE_FALLBACK, "Face Fallback Card"),
                (SNAPSHOT_ID, ORACLE_ID_NOT_IN_BULK, "Not In Bulk Card"),
            ],
        )
        con.commit()
    finally:
        con.close()


def _write_bulk_json(path: Path) -> None:
    payload = [
        {
            "id": "print-1",
            "oracle_id": ORACLE_ID_DIRECT,
            "image_uris": {
                "normal": "https://img.example/direct-normal.jpg",
                "small": "https://img.example/direct-small.jpg",
            },
        },
        {
            "id": "print-2",
            "oracle_id": ORACLE_ID_FACE_FALLBACK,
            "card_faces": [
                {
                    "name": "Front Face",
                    "image_uris": {
                        "normal": "https://img.example/face-normal.jpg",
                        "small": "https://img.example/face-small.jpg",
                    },
                },
                {
                    "name": "Back Face",
                    "image_uris": {
                        "normal": "https://img.example/back-normal.jpg",
                        "small": "https://img.example/back-small.jpg",
                    },
                },
            ],
        },
        {
            "id": "print-3",
            "oracle_id": ORACLE_ID_BULK_NOT_IN_DB,
            "image_uris": {
                "normal": "https://img.example/extra-normal.jpg",
                "small": "https://img.example/extra-small.jpg",
            },
        },
    ]
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_enrich_images_from_scryfall_bulk_upserts_card_images_and_writes_report(tmp_path: Path) -> None:
    db_path = tmp_path / "cards.sqlite"
    logs_dir = tmp_path / "logs"
    bulk_path = tmp_path / "default-cards.json"

    _create_fixture_db(db_path)
    _write_bulk_json(bulk_path)

    report = enrich_images_from_scryfall_bulk(
        db_path=db_path,
        bulk_json_path=bulk_path,
        bulk_version="bulk-test-v1",
        logs_dir=logs_dir,
    )

    assert report["db_snapshot_id"] == SNAPSHOT_ID
    assert report["rows_upserted"] == 2
    assert report["oracle_ids_matched_to_db"] == 2
    assert report["enriched_count"] == 2
    assert report["missing_image_uris_count"] == 0
    assert report["unmatched_oracle_ids_in_db_count"] == 1
    assert report["bulk_oracle_ids_not_in_db_count"] == 1

    report_path = Path(report["report_path"])
    assert report_path.is_file()
    assert report_path.name == f"image_enrich_report_{SNAPSHOT_ID}.json"

    report_from_file = json.loads(report_path.read_text(encoding="utf-8"))
    assert report_from_file["rows_upserted"] == 2
    assert report_from_file["bulk_version"] == "bulk-test-v1"

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT oracle_id, img_normal_uri, img_small_uri, img_source, img_bulk_version
            FROM card_images
            ORDER BY oracle_id ASC
            """
        ).fetchall()
    finally:
        con.close()

    assert len(rows) == 2
    by_oracle = {str(row["oracle_id"]): dict(row) for row in rows}

    assert by_oracle[ORACLE_ID_DIRECT]["img_normal_uri"] == "https://img.example/direct-normal.jpg"
    assert by_oracle[ORACLE_ID_DIRECT]["img_small_uri"] == "https://img.example/direct-small.jpg"
    assert by_oracle[ORACLE_ID_FACE_FALLBACK]["img_normal_uri"] == "https://img.example/face-normal.jpg"
    assert by_oracle[ORACLE_ID_FACE_FALLBACK]["img_small_uri"] == "https://img.example/face-small.jpg"
    assert by_oracle[ORACLE_ID_DIRECT]["img_source"] == "scryfall_bulk_default_cards"
    assert by_oracle[ORACLE_ID_DIRECT]["img_bulk_version"] == "bulk-test-v1"


def test_enrich_images_from_scryfall_bulk_errors_when_bulk_file_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "cards.sqlite"
    _create_fixture_db(db_path)

    with pytest.raises(RuntimeError, match="Bulk JSON file not found"):
        enrich_images_from_scryfall_bulk(
            db_path=db_path,
            bulk_json_path=tmp_path / "missing-default-cards.json",
        )


def test_enrich_images_from_scryfall_bulk_errors_when_required_cards_table_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "cards.sqlite"
    bulk_path = tmp_path / "default-cards.json"

    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE snapshots (
              snapshot_id TEXT PRIMARY KEY,
              created_at TEXT NOT NULL,
              source TEXT NOT NULL,
              scryfall_bulk_uri TEXT NOT NULL,
              scryfall_bulk_updated_at TEXT,
              manifest_json TEXT NOT NULL
            )
            """
        )
        con.commit()
    finally:
        con.close()

    _write_bulk_json(bulk_path)

    with pytest.raises(RuntimeError, match="cards table not found"):
        enrich_images_from_scryfall_bulk(
            db_path=db_path,
            bulk_json_path=bulk_path,
        )


def test_enrich_images_from_scryfall_bulk_zero_rows_requires_allow_empty(tmp_path: Path) -> None:
    db_path = tmp_path / "cards.sqlite"
    bulk_path = tmp_path / "default-cards.json"

    _create_fixture_db(db_path)

    bulk_payload = [
        {
            "id": "print-x",
            "oracle_id": ORACLE_ID_BULK_NOT_IN_DB,
            "image_uris": {
                "normal": "https://img.example/extra-normal.jpg",
                "small": "https://img.example/extra-small.jpg",
            },
        }
    ]
    bulk_path.write_text(json.dumps(bulk_payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="No rows were updated in card_images"):
        enrich_images_from_scryfall_bulk(
            db_path=db_path,
            bulk_json_path=bulk_path,
        )

    report = enrich_images_from_scryfall_bulk(
        db_path=db_path,
        bulk_json_path=bulk_path,
        allow_empty=True,
    )
    assert report["rows_upserted"] == 0
