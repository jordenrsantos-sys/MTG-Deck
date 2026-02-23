from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Sequence
from uuid import UUID

IMAGE_CACHE_ALLOWED_SIZES: Sequence[str] = ("normal", "small")
IMAGE_CACHE_SIZE_DIR_BY_SIZE: Dict[str, str] = {
    "normal": "normal",
    "small": "small",
}
IMAGE_CACHE_EXTENSIONS_PREFERRED: Sequence[str] = ("jpg", "jpeg", "png", "webp")
_IMAGE_CACHE_EXTENSIONS_SET = set(IMAGE_CACHE_EXTENSIONS_PREFERRED)


def _nonempty_str(value: object) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return ""


def normalize_image_size(size: object) -> str:
    normalized = _nonempty_str(size).lower()
    if normalized not in IMAGE_CACHE_SIZE_DIR_BY_SIZE:
        raise ValueError(f"Invalid image size: {size}")
    return normalized


def normalize_image_extension(ext: object) -> str:
    normalized = _nonempty_str(ext).lower().lstrip(".")
    if normalized not in _IMAGE_CACHE_EXTENSIONS_SET:
        raise ValueError(f"Invalid image extension: {ext}")
    return normalized


def normalize_oracle_id(oracle_id: object) -> str:
    token = _nonempty_str(oracle_id)
    if token == "":
        raise ValueError("Invalid oracle_id.")
    try:
        return str(UUID(token))
    except Exception as exc:
        raise ValueError("Invalid oracle_id.") from exc


def _resolve_cache_root(cache_root: object) -> Path:
    token = _nonempty_str(cache_root)
    if token == "":
        raise ValueError("Invalid cache_root.")

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return candidate.resolve()


def image_relpath(oracle_id: str, size: str, ext: str) -> str:
    normalized_oracle_id = normalize_oracle_id(oracle_id)
    normalized_size = normalize_image_size(size)
    normalized_ext = normalize_image_extension(ext)
    size_dir = IMAGE_CACHE_SIZE_DIR_BY_SIZE[normalized_size]
    return f"{size_dir}/{normalized_oracle_id}.{normalized_ext}"


def resolve_local_image_path(cache_root: str, oracle_id: str, size: str) -> Optional[str]:
    cache_root_path = _resolve_cache_root(cache_root)
    normalized_oracle_id = normalize_oracle_id(oracle_id)
    normalized_size = normalize_image_size(size)

    for extension in IMAGE_CACHE_EXTENSIONS_PREFERRED:
        candidate = (cache_root_path / image_relpath(normalized_oracle_id, normalized_size, extension)).resolve()
        try:
            candidate.relative_to(cache_root_path)
        except ValueError as exc:
            raise ValueError("Invalid image cache path.") from exc

        if candidate.is_file():
            return str(candidate)
    return None


def ensure_size_dir(cache_root: str, size: str) -> str:
    cache_root_path = _resolve_cache_root(cache_root)
    normalized_size = normalize_image_size(size)

    target_dir = (cache_root_path / IMAGE_CACHE_SIZE_DIR_BY_SIZE[normalized_size]).resolve()
    try:
        target_dir.relative_to(cache_root_path)
    except ValueError as exc:
        raise ValueError("Invalid image cache path.") from exc

    target_dir.mkdir(parents=True, exist_ok=True)
    return str(target_dir)
