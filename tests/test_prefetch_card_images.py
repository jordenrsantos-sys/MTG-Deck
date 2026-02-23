from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from snapshot_build.prefetch_card_images import _parse_sizes_csv, prefetch_card_images


SNAPSHOT_ID = "snap_prefetch_test"
ORACLE_ID_1 = "11111111-1111-1111-1111-111111111111"
ORACLE_ID_2 = "22222222-2222-2222-2222-222222222222"


def _create_fixture_db(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(
            """
            CREATE TABLE cards (
              snapshot_id TEXT NOT NULL,
              oracle_id TEXT NOT NULL,
              image_uris_json TEXT
            );
            """
        )
        con.executemany(
            """
            INSERT INTO cards (snapshot_id, oracle_id, image_uris_json)
            VALUES (?, ?, ?)
            """,
            [
                (
                    SNAPSHOT_ID,
                    ORACLE_ID_1,
                    json.dumps(
                        {
                            "normal": "https://img.example/oracle1-normal.jpg",
                            "small": "https://img.example/oracle1-small.jpg",
                        }
                    ),
                ),
                (
                    SNAPSHOT_ID,
                    ORACLE_ID_2,
                    json.dumps(
                        {
                            "normal": "https://img.example/oracle2-normal.jpg",
                            "small": "https://img.example/oracle2-small.jpg",
                        }
                    ),
                ),
            ],
        )
        con.commit()
    finally:
        con.close()


def _create_fixture_db_with_card_images(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(
            """
            CREATE TABLE cards (
              snapshot_id TEXT NOT NULL,
              oracle_id TEXT NOT NULL
            );

            CREATE TABLE card_images (
              oracle_id TEXT PRIMARY KEY,
              img_normal_uri TEXT,
              img_small_uri TEXT,
              img_source TEXT NOT NULL,
              img_enriched_at TEXT NOT NULL,
              img_bulk_version TEXT NOT NULL
            );
            """
        )
        con.executemany(
            """
            INSERT INTO cards (snapshot_id, oracle_id)
            VALUES (?, ?)
            """,
            [
                (SNAPSHOT_ID, ORACLE_ID_1),
                (SNAPSHOT_ID, ORACLE_ID_2),
            ],
        )
        con.executemany(
            """
            INSERT INTO card_images (
              oracle_id,
              img_normal_uri,
              img_small_uri,
              img_source,
              img_enriched_at,
              img_bulk_version
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    ORACLE_ID_1,
                    "https://img.example/table-normal-1.jpg",
                    "https://img.example/table-small-1.jpg",
                    "scryfall_bulk_default_cards",
                    "2026-02-23T00:00:00+00:00",
                    "bulk-v1",
                ),
                (
                    ORACLE_ID_2,
                    "https://img.example/table-normal-2.jpg",
                    "https://img.example/table-small-2.jpg",
                    "scryfall_bulk_default_cards",
                    "2026-02-23T00:00:00+00:00",
                    "bulk-v1",
                ),
            ],
        )
        con.commit()
    finally:
        con.close()


def test_parse_sizes_csv_dedupes_and_ignores_empty_segments() -> None:
    assert _parse_sizes_csv("normal,small,normal,,small") == ["normal", "small"]


def test_prefetch_full_snapshot_multiple_sizes_with_resume_and_progress(tmp_path: Path) -> None:
    db_path = tmp_path / "cards.sqlite"
    _create_fixture_db(db_path)

    out_dir = tmp_path / "card_images"
    preexisting_path = out_dir / "small" / f"{ORACLE_ID_1}.jpg"
    preexisting_path.parent.mkdir(parents=True, exist_ok=True)
    preexisting_path.write_bytes(b"already-cached")

    def fake_download(url: str, timeout_seconds: float) -> bytes:
        _ = timeout_seconds
        return f"payload:{url}".encode("utf-8")

    summary = prefetch_card_images(
        db_path=db_path,
        snapshot_id=SNAPSHOT_ID,
        out_dir=out_dir,
        sizes=["normal", "small"],
        limit=0,
        workers=4,
        resume=True,
        progress_every=1,
        rate_limit_per_sec=1000.0,
        timeout_seconds=1.0,
        download_fn=fake_download,
    )

    assert summary["planned_cards"] == 2
    assert summary["planned_images"] == 4
    assert summary["downloaded"] == 3
    assert summary["skipped"] == 1
    assert summary["failed"] == 0

    assert preexisting_path.read_bytes() == b"already-cached"

    expected_downloaded_paths = [
        out_dir / "normal" / f"{ORACLE_ID_1}.jpg",
        out_dir / "normal" / f"{ORACLE_ID_2}.jpg",
        out_dir / "small" / f"{ORACLE_ID_2}.jpg",
    ]
    for path in expected_downloaded_paths:
        assert path.is_file()


def test_prefetch_limit_applies_to_cards_not_image_tasks(tmp_path: Path) -> None:
    db_path = tmp_path / "cards.sqlite"
    _create_fixture_db(db_path)

    out_dir = tmp_path / "card_images"

    def fake_download(url: str, timeout_seconds: float) -> bytes:
        _ = timeout_seconds
        return f"payload:{url}".encode("utf-8")

    summary = prefetch_card_images(
        db_path=db_path,
        snapshot_id=SNAPSHOT_ID,
        out_dir=out_dir,
        sizes=["normal", "small"],
        limit=1,
        workers=2,
        resume=True,
        progress_every=0,
        rate_limit_per_sec=1000.0,
        timeout_seconds=1.0,
        download_fn=fake_download,
    )

    assert summary["planned_cards"] == 1
    assert summary["planned_images"] == 2
    assert summary["downloaded"] == 2
    assert summary["skipped"] == 0
    assert summary["failed"] == 0


def test_prefetch_uses_card_images_source_when_requested(tmp_path: Path) -> None:
    db_path = tmp_path / "cards.sqlite"
    _create_fixture_db_with_card_images(db_path)

    out_dir = tmp_path / "card_images"

    def fake_download(url: str, timeout_seconds: float) -> bytes:
        _ = timeout_seconds
        return f"payload:{url}".encode("utf-8")

    summary = prefetch_card_images(
        db_path=db_path,
        snapshot_id=SNAPSHOT_ID,
        out_dir=out_dir,
        sizes=["normal", "small"],
        limit=0,
        workers=2,
        resume=True,
        progress_every=0,
        rate_limit_per_sec=1000.0,
        timeout_seconds=1.0,
        source_mode="card_images",
        download_fn=fake_download,
    )

    assert summary["source_mode"] == "card_images"
    assert summary["planned_cards"] == 2
    assert summary["planned_images"] == 4
    assert summary["downloaded"] == 4
    assert summary["failed"] == 0

    assert (out_dir / "normal" / f"{ORACLE_ID_1}.jpg").is_file()
    assert (out_dir / "small" / f"{ORACLE_ID_2}.jpg").is_file()


def test_prefetch_uses_payload_extension_when_it_differs_from_url(tmp_path: Path) -> None:
    db_path = tmp_path / "cards.sqlite"
    _create_fixture_db_with_card_images(db_path)

    out_dir = tmp_path / "card_images"
    png_payload = b"\x89PNG\r\n\x1a\n" + b"fakepng"

    summary = prefetch_card_images(
        db_path=db_path,
        snapshot_id=SNAPSHOT_ID,
        out_dir=out_dir,
        sizes=["normal"],
        limit=1,
        workers=1,
        resume=False,
        progress_every=0,
        rate_limit_per_sec=1000.0,
        timeout_seconds=1.0,
        source_mode="card_images",
        download_fn=lambda _url, _timeout: png_payload,
    )

    assert summary["downloaded"] == 1
    assert (out_dir / "normal" / f"{ORACLE_ID_1}.png").is_file()
    assert not (out_dir / "normal" / f"{ORACLE_ID_1}.jpg").exists()
