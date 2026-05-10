from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict

from engine.image_cache_contract import normalize_image_size, normalize_oracle_id

CARD_IMAGES_URI_COLUMN_BY_SIZE: Dict[str, str] = {
    "normal": "img_normal_uri",
    "small": "img_small_uri",
}
IMAGE_FETCH_SOURCE = "scryfall"
DEFAULT_IMAGE_FETCH_TIMEOUT_SECONDS = 20.0
IMAGE_URI_PREFERENCE_BY_SIZE: Dict[str, tuple[str, ...]] = {
    "normal": ("normal", "large", "png", "small", "art_crop"),
    "small": ("small", "normal", "large", "png", "art_crop"),
}
_UTC_NOW = datetime.now


def _nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return ""


def _utc_now_iso() -> str:
    return _UTC_NOW(timezone.utc).isoformat()


def _resolve_cache_root(cache_dir: Any) -> Path:
    token = _nonempty_str(cache_dir)
    if token == "":
        raise ValueError("Invalid image cache directory.")

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    candidate = candidate.resolve()
    candidate.mkdir(parents=True, exist_ok=True)
    return candidate


def build_cached_image_path(*, cache_dir: str, oracle_id: str, size: str) -> Path:
    normalized_oracle_id = normalize_oracle_id(oracle_id)
    normalized_size = normalize_image_size(size)
    if normalized_size not in CARD_IMAGES_URI_COLUMN_BY_SIZE:
        raise ValueError(f"Unsupported image size: {normalized_size}")

    cache_root = _resolve_cache_root(cache_dir)
    candidate = (cache_root / normalized_size / f"{normalized_oracle_id}.jpg").resolve()
    try:
        candidate.relative_to(cache_root)
    except ValueError as exc:
        raise ValueError("Invalid image cache path.") from exc
    return candidate


def _load_card_image_uri(*, con: sqlite3.Connection, oracle_id: str, size: str) -> str:
    uri_column = CARD_IMAGES_URI_COLUMN_BY_SIZE[size]
    row = con.execute(
        """
        SELECT img_normal_uri, img_small_uri
        FROM card_images
        WHERE oracle_id = ?
        LIMIT 1
        """,
        (oracle_id,),
    ).fetchone()

    if row is None:
        return ""

    if isinstance(row, sqlite3.Row):
        value = row[uri_column] if uri_column in row.keys() else None
        return _nonempty_str(value)

    if isinstance(row, (tuple, list)):
        if uri_column == "img_normal_uri":
            value = row[0] if len(row) > 0 else None
        else:
            value = row[1] if len(row) > 1 else None
        return _nonempty_str(value)

    return ""


def _parse_json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _extract_uri_from_image_uris(image_uris: Dict[str, Any], size: str) -> str:
    for key in IMAGE_URI_PREFERENCE_BY_SIZE.get(size, (size, "normal", "small")):
        value = image_uris.get(key)
        token = _nonempty_str(value)
        if token != "":
            return token
    return ""


def _extract_uri_from_card_faces(card_faces: list[Any], size: str) -> str:
    for face in card_faces:
        if not isinstance(face, dict):
            continue
        image_uris = face.get("image_uris") if isinstance(face.get("image_uris"), dict) else {}
        uri = _extract_uri_from_image_uris(image_uris, size)
        if uri != "":
            return uri
    return ""


def _load_cards_table_image_uri(*, con: sqlite3.Connection, oracle_id: str, size: str) -> str:
    try:
        rows = con.execute(
            """
            SELECT image_uris_json, card_faces_json
            FROM cards
            WHERE oracle_id = ?
              AND (
                (image_uris_json IS NOT NULL AND LENGTH(TRIM(image_uris_json)) > 0)
                OR (card_faces_json IS NOT NULL AND LENGTH(TRIM(card_faces_json)) > 0)
              )
            ORDER BY snapshot_id DESC
            LIMIT 64
            """,
            (oracle_id,),
        ).fetchall()
    except sqlite3.Error:
        return ""

    for row in rows:
        if isinstance(row, sqlite3.Row):
            image_uris_json = row["image_uris_json"] if "image_uris_json" in row.keys() else None
            card_faces_json = row["card_faces_json"] if "card_faces_json" in row.keys() else None
        elif isinstance(row, (tuple, list)):
            image_uris_json = row[0] if len(row) > 0 else None
            card_faces_json = row[1] if len(row) > 1 else None
        else:
            image_uris_json = None
            card_faces_json = None

        image_uris = _parse_json_object(image_uris_json)
        uri = _extract_uri_from_image_uris(image_uris, size)
        if uri != "":
            return uri

        card_faces = _parse_json_list(card_faces_json)
        uri = _extract_uri_from_card_faces(card_faces, size)
        if uri != "":
            return uri

    return ""


def _load_existing_row_uris(*, con: sqlite3.Connection, oracle_id: str) -> tuple[str, str]:
    row = con.execute(
        """
        SELECT img_normal_uri, img_small_uri
        FROM card_images
        WHERE oracle_id = ?
        LIMIT 1
        """,
        (oracle_id,),
    ).fetchone()

    if row is None:
        return "", ""

    if isinstance(row, sqlite3.Row):
        normal_value = row["img_normal_uri"] if "img_normal_uri" in row.keys() else None
        small_value = row["img_small_uri"] if "img_small_uri" in row.keys() else None
        return _nonempty_str(normal_value), _nonempty_str(small_value)

    if isinstance(row, (tuple, list)):
        normal_value = row[0] if len(row) > 0 else None
        small_value = row[1] if len(row) > 1 else None
        return _nonempty_str(normal_value), _nonempty_str(small_value)

    return "", ""


def _upsert_card_image_row(
    *,
    con: sqlite3.Connection,
    oracle_id: str,
    size: str,
    image_uri: str,
    current_snapshot_id: str,
) -> None:
    existing_normal_uri, existing_small_uri = _load_existing_row_uris(con=con, oracle_id=oracle_id)
    if size == "normal":
        img_normal_uri = image_uri
        img_small_uri = existing_small_uri or None
    else:
        img_normal_uri = existing_normal_uri or None
        img_small_uri = image_uri

    bulk_version = _nonempty_str(current_snapshot_id) or "unknown_snapshot"

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
            oracle_id,
            img_normal_uri,
            img_small_uri,
            IMAGE_FETCH_SOURCE,
            _utc_now_iso(),
            bulk_version,
        ),
    )


def _download_image_bytes(image_uri: str, timeout_seconds: float) -> bytes:
    request = urllib.request.Request(image_uri, headers={"User-Agent": "mtg-engine-image-fetch/1"})
    with urllib.request.urlopen(request, timeout=max(float(timeout_seconds), 1.0)) as response:
        payload = response.read()

    if not isinstance(payload, (bytes, bytearray)) or len(payload) == 0:
        raise RuntimeError("Image download returned empty payload")
    return bytes(payload)


def ensure_card_image_cached(
    *,
    con: sqlite3.Connection,
    cache_dir: str,
    oracle_id: str,
    size: str,
    current_snapshot_id: str,
    timeout_seconds: float = DEFAULT_IMAGE_FETCH_TIMEOUT_SECONDS,
    download_bytes_fn: Callable[[str, float], bytes] | None = None,
) -> Dict[str, Any]:
    normalized_oracle_id = normalize_oracle_id(oracle_id)
    normalized_size = normalize_image_size(size)
    if normalized_size not in CARD_IMAGES_URI_COLUMN_BY_SIZE:
        raise ValueError(f"Unsupported image size: {normalized_size}")

    target_path = build_cached_image_path(
        cache_dir=cache_dir,
        oracle_id=normalized_oracle_id,
        size=normalized_size,
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)

    if target_path.is_file():
        return {
            "status": "CACHE_HIT",
            "oracle_id": normalized_oracle_id,
            "size": normalized_size,
            "image_path": str(target_path),
        }

    image_uri = _load_card_image_uri(
        con=con,
        oracle_id=normalized_oracle_id,
        size=normalized_size,
    )
    image_uri_source = "card_images"
    if image_uri == "":
        image_uri = _load_cards_table_image_uri(
            con=con,
            oracle_id=normalized_oracle_id,
            size=normalized_size,
        )
        if image_uri != "":
            image_uri_source = "cards"

    if image_uri == "":
        return {
            "status": "IMAGE_URI_MISSING",
            "oracle_id": normalized_oracle_id,
            "size": normalized_size,
            "image_path": str(target_path),
        }

    downloader = download_bytes_fn or _download_image_bytes
    temp_path = target_path.with_suffix(f"{target_path.suffix}.tmp")

    try:
        payload = downloader(image_uri, float(timeout_seconds))
        temp_path.write_bytes(payload)
        temp_path.replace(target_path)

        _upsert_card_image_row(
            con=con,
            oracle_id=normalized_oracle_id,
            size=normalized_size,
            image_uri=image_uri,
            current_snapshot_id=current_snapshot_id,
        )
        con.commit()
    except (OSError, sqlite3.Error, RuntimeError, urllib.error.URLError, urllib.error.HTTPError) as exc:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
        return {
            "status": "IMAGE_FETCH_FAILED",
            "oracle_id": normalized_oracle_id,
            "size": normalized_size,
            "image_path": str(target_path),
            "error": str(exc),
        }
    except Exception as exc:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
        return {
            "status": "IMAGE_FETCH_FAILED",
            "oracle_id": normalized_oracle_id,
            "size": normalized_size,
            "image_path": str(target_path),
            "error": str(exc),
        }

    return {
        "status": "IMAGE_FETCHED",
        "oracle_id": normalized_oracle_id,
        "size": normalized_size,
        "image_path": str(target_path),
        "image_uri": image_uri,
        "image_uri_source": image_uri_source,
    }
