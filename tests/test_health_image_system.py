from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

try:
    from fastapi.testclient import TestClient
    from api.main import app

    _IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - environment-dependent dependency loading
    TestClient = None
    app = None
    _IMPORT_ERROR = exc


def _create_fixture_db(db_path: Path, oracle_id: str, snapshot_id: str) -> None:
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
                snapshot_id,
                "2026-02-25T00:00:00+00:00",
                "tests",
                "local://fixture",
                "2026-02-25T00:00:00+00:00",
                '{"tags_compiled":1}',
            ),
        )
        con.execute(
            "INSERT INTO cards (snapshot_id, oracle_id, name) VALUES (?, ?, ?)",
            (snapshot_id, oracle_id, "Fixture Card"),
        )
        con.execute(
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
            (
                oracle_id,
                "https://example.invalid/fixture.jpg",
                None,
                "seed",
                "2026-02-25T00:00:00+00:00",
                snapshot_id,
            ),
        )
        con.commit()
    finally:
        con.close()


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_health_reports_image_system_fields(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    oracle_id = "123e4567-e89b-12d3-a456-426614174001"
    snapshot_id = "HEALTH_TEST_SNAPSHOT"

    db_path = tmp_path / "mtg.sqlite"
    cache_dir = tmp_path / "image_cache"
    _create_fixture_db(db_path, oracle_id=oracle_id, snapshot_id=snapshot_id)

    sample_image = cache_dir / "normal" / f"{oracle_id}.jpg"
    sample_image.parent.mkdir(parents=True, exist_ok=True)
    sample_image.write_bytes(b"\xff\xd8\xff\xd9")

    monkeypatch.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    monkeypatch.setenv("MTG_IMAGE_CACHE_DIR", str(cache_dir))

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()

    assert payload.get("image_cache_dir") == str(cache_dir.resolve())
    assert payload.get("image_cache_exists") is True
    assert payload.get("card_images_schema_ok") is True
    assert isinstance(payload.get("cached_image_count"), int)
    assert payload.get("cached_image_count") >= 1

    sample_test = payload.get("image_sample_test") if isinstance(payload.get("image_sample_test"), dict) else {}
    assert sample_test.get("oracle_id") == oracle_id
    assert sample_test.get("exists_on_disk") is True
