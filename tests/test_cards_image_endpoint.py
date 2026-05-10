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


SNAPSHOT_ID = "TEST_SNAPSHOT_0001"
ORACLE_ID = "123e4567-e89b-12d3-a456-426614174000"


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
              image_uris_json TEXT,
              card_faces_json TEXT,
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
                SNAPSHOT_ID,
                "2026-02-25T00:00:00+00:00",
                "tests",
                "local://fixture",
                "2026-02-25T00:00:00+00:00",
                '{"tags_compiled":1}',
            ),
        )
        con.execute(
            "INSERT INTO cards (snapshot_id, oracle_id, name) VALUES (?, ?, ?)",
            (SNAPSHOT_ID, ORACLE_ID, "Fixture Card"),
        )
        con.commit()
    finally:
        con.close()


def _update_cards_image_uris_json(db_path: Path, *, image_uris_json: str) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            "UPDATE cards SET image_uris_json = ? WHERE snapshot_id = ? AND oracle_id = ?",
            (image_uris_json, SNAPSHOT_ID, ORACLE_ID),
        )
        con.commit()
    finally:
        con.close()


def _insert_card_images_uri(db_path: Path, *, uri: str) -> None:
    con = sqlite3.connect(str(db_path))
    try:
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
            ON CONFLICT(oracle_id) DO UPDATE SET
              img_normal_uri = excluded.img_normal_uri,
              img_small_uri = excluded.img_small_uri,
              img_source = excluded.img_source,
              img_enriched_at = excluded.img_enriched_at,
              img_bulk_version = excluded.img_bulk_version
            """,
            (
                ORACLE_ID,
                uri,
                None,
                "seed",
                "2026-02-25T00:00:00+00:00",
                SNAPSHOT_ID,
            ),
        )
        con.commit()
    finally:
        con.close()


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_missing_returns_404_json(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "mtg.sqlite"
    cache_dir = tmp_path / "image_cache"
    _create_fixture_db(db_path)

    monkeypatch.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    monkeypatch.setenv("MTG_IMAGE_CACHE_DIR", str(cache_dir))

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/cards/image/{ORACLE_ID}", params={"size": "normal"})

    assert response.status_code == 404
    assert response.json().get("code") == "IMAGE_URI_MISSING"
    assert response.json().get("oracle_id") == ORACLE_ID


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_present_returns_200_with_jpeg_headers(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "mtg.sqlite"
    cache_dir = tmp_path / "image_cache"
    _create_fixture_db(db_path)
    monkeypatch.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    monkeypatch.setenv("MTG_IMAGE_CACHE_DIR", str(cache_dir))

    image_path = cache_dir / "normal" / f"{ORACLE_ID}.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"\xff\xd8\xff\xd9")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/cards/image/{ORACLE_ID}", params={"size": "normal"})

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith("image/jpeg")
    assert response.headers.get("cache-control") == "public, max-age=31536000"
    assert response.content == b"\xff\xd8\xff\xd9"


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_missing_cache_auto_fetches_and_persists(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "mtg.sqlite"
    cache_dir = tmp_path / "image_cache"
    _create_fixture_db(db_path)
    _insert_card_images_uri(db_path, uri="https://example.invalid/fixture.jpg")

    monkeypatch.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    monkeypatch.setenv("MTG_IMAGE_CACHE_DIR", str(cache_dir))
    monkeypatch.setattr("engine.image_runtime._download_image_bytes", lambda _uri, _timeout: b"\xff\xd8\xff\xd9")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/cards/image/{ORACLE_ID}", params={"size": "normal"})

    assert response.status_code == 200
    assert response.content == b"\xff\xd8\xff\xd9"

    cached_path = cache_dir / "normal" / f"{ORACLE_ID}.jpg"
    assert cached_path.is_file()

    con = sqlite3.connect(str(db_path))
    try:
        row = con.execute(
            "SELECT img_normal_uri, img_source, img_bulk_version FROM card_images WHERE oracle_id = ?",
            (ORACLE_ID,),
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    assert row[0] == "https://example.invalid/fixture.jpg"
    assert row[1] == "scryfall"
    assert row[2] == SNAPSHOT_ID


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_falls_back_to_cards_image_uris_when_card_images_row_missing(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "mtg.sqlite"
    cache_dir = tmp_path / "image_cache"
    _create_fixture_db(db_path)
    _update_cards_image_uris_json(
        db_path,
        image_uris_json='{"normal":"https://example.invalid/from-cards-normal.jpg"}',
    )

    monkeypatch.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    monkeypatch.setenv("MTG_IMAGE_CACHE_DIR", str(cache_dir))
    monkeypatch.setattr("engine.image_runtime._download_image_bytes", lambda _uri, _timeout: b"\xff\xd8\xff\xd9")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/cards/image/{ORACLE_ID}", params={"size": "normal"})

    assert response.status_code == 200
    assert response.content == b"\xff\xd8\xff\xd9"

    cached_path = cache_dir / "normal" / f"{ORACLE_ID}.jpg"
    assert cached_path.is_file()

    con = sqlite3.connect(str(db_path))
    try:
        row = con.execute(
            "SELECT img_normal_uri FROM card_images WHERE oracle_id = ?",
            (ORACLE_ID,),
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    assert row[0] == "https://example.invalid/from-cards-normal.jpg"


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_download_failure_returns_502_json(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "mtg.sqlite"
    cache_dir = tmp_path / "image_cache"
    _create_fixture_db(db_path)
    _insert_card_images_uri(db_path, uri="https://example.invalid/fixture.jpg")

    monkeypatch.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    monkeypatch.setenv("MTG_IMAGE_CACHE_DIR", str(cache_dir))

    def _raise_fetch_error(_uri: str, _timeout: float) -> bytes:
        raise RuntimeError("download failed")

    monkeypatch.setattr("engine.image_runtime._download_image_bytes", _raise_fetch_error)

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/cards/image/{ORACLE_ID}", params={"size": "normal"})

    assert response.status_code == 502
    assert response.json().get("code") == "IMAGE_FETCH_FAILED"


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_rejects_invalid_size(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "mtg.sqlite"
    cache_dir = tmp_path / "image_cache"
    _create_fixture_db(db_path)

    monkeypatch.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    monkeypatch.setenv("MTG_IMAGE_CACHE_DIR", str(cache_dir))

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/cards/image/{ORACLE_ID}", params={"size": "gigantic"})

    assert response.status_code == 400


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_rejects_invalid_oracle_id(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "mtg.sqlite"
    cache_dir = tmp_path / "image_cache"
    _create_fixture_db(db_path)

    monkeypatch.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    monkeypatch.setenv("MTG_IMAGE_CACHE_DIR", str(cache_dir))

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/cards/image/not-a-uuid", params={"size": "normal"})

    assert response.status_code == 400
