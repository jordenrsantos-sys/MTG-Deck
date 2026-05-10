from __future__ import annotations

from pathlib import Path

from engine.db import resolve_image_cache_dir


def test_resolve_image_cache_dir_prefers_mtg_image_cache_dir(tmp_path, monkeypatch) -> None:
    preferred_dir = tmp_path / "preferred_cache"
    monkeypatch.setenv("MTG_IMAGE_CACHE_DIR", str(preferred_dir))
    monkeypatch.delenv("MTG_ENGINE_IMAGE_CACHE_DIR", raising=False)

    resolved = Path(resolve_image_cache_dir())

    assert resolved == preferred_dir.resolve()
    assert resolved.is_dir()


def test_resolve_image_cache_dir_uses_legacy_env_when_new_env_missing(tmp_path, monkeypatch) -> None:
    legacy_dir = tmp_path / "legacy_cache"
    monkeypatch.delenv("MTG_IMAGE_CACHE_DIR", raising=False)
    monkeypatch.setenv("MTG_ENGINE_IMAGE_CACHE_DIR", str(legacy_dir))

    resolved = Path(resolve_image_cache_dir())

    assert resolved == legacy_dir.resolve()
    assert resolved.is_dir()
