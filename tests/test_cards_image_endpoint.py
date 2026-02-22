from __future__ import annotations

import pytest

try:
    from fastapi.testclient import TestClient
    from api.main import app

    _IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - environment-dependent dependency loading
    TestClient = None
    app = None
    _IMPORT_ERROR = exc


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_missing_returns_404_json(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MTG_ENGINE_IMAGE_CACHE_DIR", str(tmp_path))
    oracle_id = "123e4567-e89b-12d3-a456-426614174000"

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/cards/image/{oracle_id}", params={"size": "normal"})

    assert response.status_code == 404
    assert response.json() == {
        "status": "MISSING_IMAGE",
        "oracle_id": oracle_id,
        "size": "normal",
    }


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_present_returns_200_with_jpeg_headers(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MTG_ENGINE_IMAGE_CACHE_DIR", str(tmp_path))
    oracle_id = "123e4567-e89b-12d3-a456-426614174000"

    image_path = tmp_path / "normal" / f"{oracle_id}.jpg"
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(b"\xff\xd8\xff\xd9")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/cards/image/{oracle_id}", params={"size": "normal"})

    assert response.status_code == 200
    assert response.headers.get("content-type", "").startswith("image/jpeg")
    assert response.headers.get("cache-control") == "public, max-age=31536000"
    assert response.content == b"\xff\xd8\xff\xd9"


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_rejects_invalid_size(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MTG_ENGINE_IMAGE_CACHE_DIR", str(tmp_path))
    oracle_id = "123e4567-e89b-12d3-a456-426614174000"

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(f"/cards/image/{oracle_id}", params={"size": "gigantic"})

    assert response.status_code == 400


@pytest.mark.skipif(_IMPORT_ERROR is not None, reason="FastAPI integration dependencies unavailable")
def test_cards_image_rejects_invalid_oracle_id(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MTG_ENGINE_IMAGE_CACHE_DIR", str(tmp_path))

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/cards/image/not-a-uuid", params={"size": "normal"})

    assert response.status_code == 400
