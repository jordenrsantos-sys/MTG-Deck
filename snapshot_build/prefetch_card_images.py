from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, Tuple
from uuid import UUID

from engine.db import resolve_db_path

IMAGE_SIZE_EXTENSIONS: Dict[str, str] = {
    "small": ".jpg",
    "normal": ".jpg",
    "large": ".jpg",
    "png": ".png",
    "art_crop": ".jpg",
    "border_crop": ".jpg",
}

IMAGE_URI_COLUMNS: Sequence[str] = (
    "image_uri",
    "image_url",
    "art_uri",
    "art_url",
    "image_uris_json",
    "card_faces_json",
)

DEFAULT_LIMIT = 500
DEFAULT_RATE_LIMIT_PER_SEC = 10.0
DEFAULT_TIMEOUT_SECONDS = 20.0


def _nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
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


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def _normalize_size(raw_size: Any) -> str:
    token = _nonempty_str(raw_size).lower()
    if token not in IMAGE_SIZE_EXTENSIONS:
        allowed = ", ".join(sorted(IMAGE_SIZE_EXTENSIONS.keys()))
        raise RuntimeError(f"Invalid --size '{raw_size}'. Allowed: {allowed}")
    return token


def _normalize_oracle_id(raw_oracle_id: Any) -> str:
    token = _nonempty_str(raw_oracle_id)
    if token == "":
        return ""
    try:
        return str(UUID(token))
    except Exception:
        return ""


def _resolve_db_path_from_cli(raw_db_path: Any) -> Path:
    token = _nonempty_str(raw_db_path)
    if token == "":
        return resolve_db_path()

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    if not candidate.is_file():
        raise RuntimeError(f"Database file not found: {candidate}")
    return candidate


def _resolve_output_dir(raw_output_dir: Any) -> Path:
    token = _nonempty_str(raw_output_dir)
    if token == "":
        raise RuntimeError("--out is required")

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return candidate.resolve()


def _load_cards_columns(con: sqlite3.Connection) -> List[str]:
    rows = con.execute("PRAGMA table_info(cards)").fetchall()
    columns: List[str] = []
    for row in rows:
        row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
        name = row_dict.get("name")
        if isinstance(name, str) and name != "":
            columns.append(name)
    return sorted(set(columns))


def _extract_image_url_for_size(row: Dict[str, Any], size: str) -> str:
    if size == "art_crop":
        direct_keys = ["art_uri", "art_url", "image_uri", "image_url"]
    else:
        direct_keys = ["image_uri", "image_url", "art_uri", "art_url"]

    for key in direct_keys:
        value = row.get(key)
        if isinstance(value, str) and value.strip() != "":
            return value.strip()

    image_uris = _parse_json_object(row.get("image_uris_json"))
    image_uri_value = image_uris.get(size)
    if isinstance(image_uri_value, str) and image_uri_value.strip() != "":
        return image_uri_value.strip()

    card_faces = _parse_json_list(row.get("card_faces_json"))
    for face in card_faces:
        if not isinstance(face, dict):
            continue
        face_image_uris = face.get("image_uris") if isinstance(face.get("image_uris"), dict) else {}
        face_image_uri_value = face_image_uris.get(size)
        if isinstance(face_image_uri_value, str) and face_image_uri_value.strip() != "":
            return face_image_uri_value.strip()

    return ""


def _load_prefetch_plan(
    *,
    db_path: Path,
    snapshot_id: str,
    size: str,
    limit: int,
) -> List[Tuple[str, str]]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        columns = _load_cards_columns(con)
        available_image_columns = sorted(set(columns).intersection(IMAGE_URI_COLUMNS))
        if len(available_image_columns) == 0:
            raise RuntimeError(
                "cards table does not contain image URI fields (image_uri/image_uris_json/card_faces_json). "
                "Reimport snapshot with image URI metadata before running prefetch."
            )

        select_fields = ["oracle_id", *available_image_columns]
        select_clause = ", ".join(select_fields)
        rows = con.execute(
            f"""
            SELECT {select_clause}
            FROM cards
            WHERE snapshot_id = ?
            ORDER BY oracle_id ASC
            """,
            (snapshot_id,),
        ).fetchall()
    finally:
        con.close()

    plan: List[Tuple[str, str]] = []
    seen_oracle_ids: set[str] = set()
    limit_total = limit if limit > 0 else 0

    for row in rows:
        row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
        oracle_id = _normalize_oracle_id(row_dict.get("oracle_id"))
        if oracle_id == "" or oracle_id in seen_oracle_ids:
            continue

        image_url = _extract_image_url_for_size(row_dict, size=size)
        if image_url == "":
            continue

        seen_oracle_ids.add(oracle_id)
        plan.append((oracle_id, image_url))

        if limit_total > 0 and len(plan) >= limit_total:
            break

    if len(plan) == 0:
        raise RuntimeError(
            "No image URLs found for selected snapshot/size in local cards metadata. "
            "Reimport snapshot with image URI fields, then rerun prefetch."
        )

    return plan


def _target_image_path(*, out_dir: Path, size: str, oracle_id: str) -> Path:
    extension = IMAGE_SIZE_EXTENSIONS[size]
    target = (out_dir / size / f"{oracle_id}{extension}").resolve()
    return target


def _write_bytes_atomically(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("wb") as file_obj:
        file_obj.write(payload)
    os.replace(temp_path, path)


def _download_bytes(url: str, timeout_seconds: float) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "mtg-engine-image-prefetch/1.0"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return response.read()


def prefetch_card_images(
    *,
    db_path: Path,
    snapshot_id: str,
    out_dir: Path,
    size: str,
    limit: int,
    rate_limit_per_sec: float,
    timeout_seconds: float,
    download_fn: Callable[[str, float], bytes] = _download_bytes,
) -> Dict[str, int]:
    normalized_snapshot_id = _nonempty_str(snapshot_id)
    if normalized_snapshot_id == "":
        raise RuntimeError("--snapshot_id is required")

    normalized_size = _normalize_size(size)
    target_out_dir = out_dir.resolve()
    plan = _load_prefetch_plan(
        db_path=db_path,
        snapshot_id=normalized_snapshot_id,
        size=normalized_size,
        limit=limit,
    )

    safe_rate_limit = max(float(rate_limit_per_sec), 0.1)
    min_interval_seconds = 1.0 / safe_rate_limit
    last_fetch_started = 0.0

    downloaded = 0
    skipped = 0
    failed = 0

    print(
        "UPDATE MODE: prefetching image binaries from URLs already stored in local cards metadata; "
        "runtime endpoints remain local-cache-only."
    )
    print(
        f"Prefetch plan | snapshot_id={normalized_snapshot_id} size={normalized_size} "
        f"rows={len(plan)} out={target_out_dir}"
    )

    for index, (oracle_id, image_url) in enumerate(plan, start=1):
        target_path = _target_image_path(out_dir=target_out_dir, size=normalized_size, oracle_id=oracle_id)

        if target_path.is_file():
            skipped += 1
            print(f"[{index}/{len(plan)}] SKIP {oracle_id} (already cached)")
            continue

        now = time.perf_counter()
        if last_fetch_started > 0.0:
            remaining = min_interval_seconds - (now - last_fetch_started)
            if remaining > 0.0:
                time.sleep(remaining)
        last_fetch_started = time.perf_counter()

        url_lower = image_url.lower()
        if not (url_lower.startswith("http://") or url_lower.startswith("https://")):
            failed += 1
            print(f"[{index}/{len(plan)}] FAIL {oracle_id} (unsupported URL scheme)")
            continue

        try:
            payload = download_fn(image_url, timeout_seconds)
            if not isinstance(payload, (bytes, bytearray)) or len(payload) == 0:
                raise RuntimeError("empty payload")
            _write_bytes_atomically(target_path, bytes(payload))
            downloaded += 1
            print(f"[{index}/{len(plan)}] OK {oracle_id} -> {target_path}")
        except Exception as exc:
            failed += 1
            print(f"[{index}/{len(plan)}] FAIL {oracle_id} ({exc})")

    summary = {
        "planned": len(plan),
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
    }
    print(
        "Summary | "
        f"planned={summary['planned']} downloaded={summary['downloaded']} "
        f"skipped={summary['skipped']} failed={summary['failed']}"
    )
    return summary


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update-mode local card image prefetch from DB-stored URLs")
    parser.add_argument("--db", default="", help="Path to SQLite DB (defaults to MTG_ENGINE_DB_PATH / ./data/mtg.sqlite)")
    parser.add_argument("--snapshot_id", required=True, help="Snapshot ID to read cards from")
    parser.add_argument("--out", default="./data/card_images", help="Output cache root directory")
    parser.add_argument("--size", default="normal", help="Image size folder (small|normal|large|png|art_crop|border_crop)")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max cards to prefetch (<=0 means no limit)")
    parser.add_argument("--rate_limit", type=float, default=DEFAULT_RATE_LIMIT_PER_SEC, help="Max downloads per second")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Per-request timeout in seconds")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        db_path = _resolve_db_path_from_cli(args.db)
        out_dir = _resolve_output_dir(args.out)
        summary = prefetch_card_images(
            db_path=db_path,
            snapshot_id=args.snapshot_id,
            out_dir=out_dir,
            size=args.size,
            limit=int(args.limit),
            rate_limit_per_sec=float(args.rate_limit),
            timeout_seconds=float(args.timeout),
        )
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        return 2
    except Exception as exc:
        print(f"ERROR: unexpected failure: {exc}")
        return 2

    return 0 if summary.get("failed", 0) == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
