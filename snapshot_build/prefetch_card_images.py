from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, Tuple

from engine.db import resolve_db_path
from engine.image_cache_contract import (
    IMAGE_CACHE_ALLOWED_SIZES,
    IMAGE_CACHE_EXTENSIONS_PREFERRED,
    ensure_size_dir,
    image_relpath,
    normalize_image_extension,
    normalize_image_size,
    normalize_oracle_id,
    resolve_local_image_path,
)

IMAGE_URI_COLUMNS: Sequence[str] = (
    "image_uri",
    "image_url",
    "art_uri",
    "art_url",
    "image_uris_json",
    "card_faces_json",
)

CARD_IMAGES_URI_BY_SIZE: Dict[str, str] = {
    "normal": "img_normal_uri",
    "small": "img_small_uri",
}

PREFETCH_SOURCE_MODES = {"auto", "card_images", "legacy"}

DEFAULT_LIMIT = 0
DEFAULT_RATE_LIMIT_PER_SEC = 10.0
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_WORKERS = 4
DEFAULT_PROGRESS_EVERY = 100


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
    try:
        return normalize_image_size(raw_size)
    except ValueError:
        allowed = ", ".join(sorted(IMAGE_CACHE_ALLOWED_SIZES))
        raise RuntimeError(f"Invalid image size '{raw_size}'. Allowed: {allowed}")


def _parse_sizes_csv(raw_sizes: Any) -> List[str]:
    token = _nonempty_str(raw_sizes)
    if token == "":
        token = "normal"

    parsed: List[str] = []
    seen: set[str] = set()
    for raw_part in token.split(","):
        part_token = _nonempty_str(raw_part)
        if part_token == "":
            continue
        normalized = _normalize_size(part_token)
        if normalized in seen:
            continue
        seen.add(normalized)
        parsed.append(normalized)

    if len(parsed) == 0:
        raise RuntimeError("--sizes must include at least one valid size")
    return parsed


def _normalize_sizes_argument(raw_sizes: Any) -> List[str]:
    if isinstance(raw_sizes, str):
        return _parse_sizes_csv(raw_sizes)
    if isinstance(raw_sizes, (list, tuple)):
        joined = ",".join(_nonempty_str(value) for value in raw_sizes)
        return _parse_sizes_csv(joined)
    raise RuntimeError("Invalid sizes argument")


def _normalize_oracle_id(raw_oracle_id: Any) -> str:
    if _nonempty_str(raw_oracle_id) == "":
        return ""
    try:
        return normalize_oracle_id(raw_oracle_id)
    except ValueError:
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


def _table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return row is not None


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

    if size == "art_crop":
        direct_keys = ["art_uri", "art_url", "image_uri", "image_url"]
    else:
        direct_keys = ["image_uri", "image_url", "art_uri", "art_url"]

    for key in direct_keys:
        value = row.get(key)
        if isinstance(value, str) and value.strip() != "":
            return value.strip()

    return ""


def _load_prefetch_plan_legacy(
    *,
    db_path: Path,
    snapshot_id: str,
    sizes: Sequence[str],
    limit: int,
) -> List[Tuple[str, str, str]]:
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

    plan: List[Tuple[str, str, str]] = []
    seen_oracle_ids: set[str] = set()
    limit_total = limit if limit > 0 else 0
    cards_selected = 0

    for row in rows:
        row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
        oracle_id = _normalize_oracle_id(row_dict.get("oracle_id"))
        if oracle_id == "" or oracle_id in seen_oracle_ids:
            continue

        card_plan_rows: List[Tuple[str, str, str]] = []
        for size in sizes:
            image_url = _extract_image_url_for_size(row_dict, size=size)
            if image_url == "":
                continue
            card_plan_rows.append((oracle_id, size, image_url))

        if len(card_plan_rows) == 0:
            continue

        seen_oracle_ids.add(oracle_id)
        plan.extend(card_plan_rows)
        cards_selected += 1

        if limit_total > 0 and cards_selected >= limit_total:
            break

    if len(plan) == 0:
        selected_sizes = ",".join(sizes)
        raise RuntimeError(
            f"No image URLs found for selected snapshot/sizes ({selected_sizes}) in local cards metadata. "
            "Reimport snapshot with image URI fields, then rerun prefetch."
        )

    return plan


def _load_prefetch_plan_from_card_images(
    *,
    db_path: Path,
    snapshot_id: str,
    sizes: Sequence[str],
    limit: int,
) -> List[Tuple[str, str, str]]:
    unsupported_sizes = [size for size in sizes if size not in CARD_IMAGES_URI_BY_SIZE]
    if unsupported_sizes:
        raise RuntimeError(
            "card_images source only supports sizes normal/small; unsupported: "
            + ",".join(sorted(set(unsupported_sizes)))
        )

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        if not _table_exists(con, "cards"):
            raise RuntimeError("cards table not found in target DB")
        if not _table_exists(con, "card_images"):
            raise RuntimeError(
                "card_images table not found. Run image enrichment first: "
                "python -m snapshot_build.enrich_images_from_scryfall_bulk"
            )

        rows = con.execute(
            """
            SELECT DISTINCT
              c.oracle_id AS oracle_id,
              ci.img_normal_uri AS img_normal_uri,
              ci.img_small_uri AS img_small_uri
            FROM cards c
            LEFT JOIN card_images ci
              ON ci.oracle_id = c.oracle_id
            WHERE c.snapshot_id = ?
            ORDER BY c.oracle_id ASC
            """,
            (snapshot_id,),
        ).fetchall()
    finally:
        con.close()

    plan: List[Tuple[str, str, str]] = []
    limit_total = limit if limit > 0 else 0
    cards_selected = 0

    for row in rows:
        row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
        oracle_id = _normalize_oracle_id(row_dict.get("oracle_id"))
        if oracle_id == "":
            continue

        card_plan_rows: List[Tuple[str, str, str]] = []
        for size in sizes:
            column_name = CARD_IMAGES_URI_BY_SIZE[size]
            image_url = _nonempty_str(row_dict.get(column_name))
            if image_url == "":
                continue
            card_plan_rows.append((oracle_id, size, image_url))

        if len(card_plan_rows) == 0:
            continue

        plan.extend(card_plan_rows)
        cards_selected += 1

        if limit_total > 0 and cards_selected >= limit_total:
            break

    if len(plan) == 0:
        selected_sizes = ",".join(sizes)
        raise RuntimeError(
            f"No image URLs found in card_images for selected snapshot/sizes ({selected_sizes}). "
            "Run enrichment first and confirm card_images contains normal/small URIs."
        )

    return plan


def _infer_extension_from_payload(payload: bytes) -> str | None:
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if len(payload) >= 12 and payload.startswith(b"RIFF") and payload[8:12] == b"WEBP":
        return "webp"
    if payload.startswith(b"\xff\xd8\xff"):
        return "jpg"
    return None


def _infer_extension_from_url(url: str) -> str | None:
    parsed = urllib.parse.urlparse(url)
    suffix = Path(parsed.path).suffix.lstrip(".").lower()
    if suffix == "":
        return None

    try:
        return normalize_image_extension(suffix)
    except ValueError:
        return None


def _resolve_target_extension(*, image_url: str, payload: bytes) -> str:
    extension_from_payload = _infer_extension_from_payload(payload)
    if isinstance(extension_from_payload, str):
        return extension_from_payload

    extension_from_url = _infer_extension_from_url(image_url)
    if isinstance(extension_from_url, str):
        return extension_from_url

    return IMAGE_CACHE_EXTENSIONS_PREFERRED[0]


def _target_image_path(*, out_dir: Path, size: str, oracle_id: str, extension: str) -> Path:
    relpath = image_relpath(oracle_id=oracle_id, size=size, ext=extension)
    target = (out_dir / relpath).resolve()
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


class _GlobalRateLimiter:
    def __init__(self, rate_limit_per_sec: float):
        safe_rate = max(float(rate_limit_per_sec), 0.1)
        self._min_interval_seconds = 1.0 / safe_rate
        self._next_allowed_time = 0.0
        self._lock = threading.Lock()

    def wait_turn(self) -> None:
        while True:
            with self._lock:
                now = time.perf_counter()
                wait_seconds = self._next_allowed_time - now
                if wait_seconds <= 0.0:
                    self._next_allowed_time = now + self._min_interval_seconds
                    return
            time.sleep(wait_seconds)


def _download_plan_item(
    *,
    out_dir: Path,
    oracle_id: str,
    size: str,
    image_url: str,
    resume: bool,
    timeout_seconds: float,
    rate_limiter: _GlobalRateLimiter,
    download_fn: Callable[[str, float], bytes],
) -> Tuple[str, str]:
    if resume:
        existing_path = resolve_local_image_path(
            cache_root=str(out_dir),
            oracle_id=oracle_id,
            size=size,
        )
        if isinstance(existing_path, str) and existing_path != "":
            return "SKIP", "already cached"

    url_lower = image_url.lower()
    if not (url_lower.startswith("http://") or url_lower.startswith("https://")):
        return "FAIL", "unsupported URL scheme"

    try:
        rate_limiter.wait_turn()
        payload = download_fn(image_url, timeout_seconds)
        if not isinstance(payload, (bytes, bytearray)) or len(payload) == 0:
            raise RuntimeError("empty payload")

        normalized_payload = bytes(payload)
        extension = _resolve_target_extension(image_url=image_url, payload=normalized_payload)
        ensure_size_dir(cache_root=str(out_dir), size=size)
        target_path = _target_image_path(
            out_dir=out_dir,
            size=size,
            oracle_id=oracle_id,
            extension=extension,
        )
        _write_bytes_atomically(target_path, bytes(payload))
        return "OK", str(target_path)
    except Exception as exc:
        return "FAIL", str(exc)


def prefetch_card_images(
    *,
    db_path: Path,
    snapshot_id: str,
    out_dir: Path,
    sizes: Sequence[str],
    limit: int,
    workers: int,
    resume: bool,
    progress_every: int,
    rate_limit_per_sec: float,
    timeout_seconds: float,
    source_mode: str = "auto",
    download_fn: Callable[[str, float], bytes] = _download_bytes,
) -> Dict[str, Any]:
    normalized_snapshot_id = _nonempty_str(snapshot_id)
    if normalized_snapshot_id == "":
        raise RuntimeError("--snapshot_id is required")

    normalized_sizes = _normalize_sizes_argument(sizes)
    target_out_dir = out_dir.resolve()
    normalized_source_mode = _nonempty_str(source_mode).lower() or "auto"
    if normalized_source_mode not in PREFETCH_SOURCE_MODES:
        allowed = ",".join(sorted(PREFETCH_SOURCE_MODES))
        raise RuntimeError(f"Invalid source mode '{source_mode}'. Allowed: {allowed}")

    selected_source_mode = normalized_source_mode
    if normalized_source_mode == "card_images":
        plan = _load_prefetch_plan_from_card_images(
            db_path=db_path,
            snapshot_id=normalized_snapshot_id,
            sizes=normalized_sizes,
            limit=limit,
        )
    elif normalized_source_mode == "legacy":
        plan = _load_prefetch_plan_legacy(
            db_path=db_path,
            snapshot_id=normalized_snapshot_id,
            sizes=normalized_sizes,
            limit=limit,
        )
    else:
        selected_source_mode = "card_images"
        try:
            plan = _load_prefetch_plan_from_card_images(
                db_path=db_path,
                snapshot_id=normalized_snapshot_id,
                sizes=normalized_sizes,
                limit=limit,
            )
        except RuntimeError:
            selected_source_mode = "legacy"
            plan = _load_prefetch_plan_legacy(
                db_path=db_path,
                snapshot_id=normalized_snapshot_id,
                sizes=normalized_sizes,
                limit=limit,
            )
    planned_cards = len({oracle_id for oracle_id, _, _ in plan})
    safe_workers = max(int(workers), 1)
    safe_progress_every = max(int(progress_every), 0)

    downloaded = 0
    skipped = 0
    failed = 0

    print(
        "UPDATE MODE: prefetching image binaries from URLs already stored in local cards metadata; "
        "runtime endpoints remain local-cache-only."
    )
    print(
        f"Prefetch plan | snapshot_id={normalized_snapshot_id} source={selected_source_mode} sizes={','.join(normalized_sizes)} "
        f"cards={planned_cards} images={len(plan)} out={target_out_dir} workers={safe_workers} resume={resume}"
    )

    rate_limiter = _GlobalRateLimiter(rate_limit_per_sec)

    def _run_plan_item(item: Tuple[str, str, str]) -> Tuple[str, str, str, str]:
        oracle_id, size, image_url = item
        status, details = _download_plan_item(
            out_dir=target_out_dir,
            oracle_id=oracle_id,
            size=size,
            image_url=image_url,
            resume=resume,
            timeout_seconds=timeout_seconds,
            rate_limiter=rate_limiter,
            download_fn=download_fn,
        )
        return status, oracle_id, size, details

    with ThreadPoolExecutor(max_workers=safe_workers) as executor:
        completed_cards = 0
        last_oracle_id = ""
        for index, result in enumerate(executor.map(_run_plan_item, plan), start=1):
            status, oracle_id, size, details = result

            if oracle_id != last_oracle_id:
                completed_cards += 1
                last_oracle_id = oracle_id

            if status == "OK":
                downloaded += 1
                print(f"[{index}/{len(plan)}] OK {oracle_id} [{size}] -> {details}")
            elif status == "SKIP":
                skipped += 1
                print(f"[{index}/{len(plan)}] SKIP {oracle_id} [{size}] ({details})")
            else:
                failed += 1
                print(f"[{index}/{len(plan)}] FAIL {oracle_id} [{size}] ({details})")

            if safe_progress_every > 0 and (
                completed_cards % safe_progress_every == 0 or index == len(plan)
            ):
                print(
                    "Progress | "
                    f"cards={completed_cards}/{planned_cards} images={index}/{len(plan)} "
                    f"downloaded={downloaded} skipped={skipped} failed={failed}"
                )

    summary = {
        "planned": len(plan),
        "planned_images": len(plan),
        "planned_cards": planned_cards,
        "source_mode": selected_source_mode,
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
    parser.add_argument(
        "--source",
        default="auto",
        choices=sorted(PREFETCH_SOURCE_MODES),
        help="Image URL source mode: auto (prefer card_images), card_images, or legacy cards metadata",
    )
    parser.add_argument("--out", default="./data/card_images", help="Output cache root directory")
    parser.add_argument("--sizes", default="normal", help="Comma-separated image sizes (normal,small)")
    parser.add_argument("--size", default="", help=argparse.SUPPRESS)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max cards to prefetch (<=0 means no limit; default: full snapshot)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Concurrent download workers")
    resume_group = parser.add_mutually_exclusive_group()
    resume_group.add_argument("--resume", dest="resume", action="store_true", help="Skip files already cached (default)")
    resume_group.add_argument("--no-resume", dest="resume", action="store_false", help="Redownload even if cached")
    parser.set_defaults(resume=True)
    parser.add_argument(
        "--progress",
        nargs="?",
        const=DEFAULT_PROGRESS_EVERY,
        type=int,
        default=0,
        help="Print progress every N processed cards (use --progress for default cadence)",
    )
    parser.add_argument("--rate_limit", type=float, default=DEFAULT_RATE_LIMIT_PER_SEC, help="Max downloads per second")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="Per-request timeout in seconds")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        db_path = _resolve_db_path_from_cli(args.db)
        out_dir = _resolve_output_dir(args.out)
        sizes_token = _nonempty_str(args.sizes)
        size_alias = _nonempty_str(args.size)
        if size_alias != "" and sizes_token.lower() in {"", "normal"}:
            sizes_token = size_alias

        summary = prefetch_card_images(
            db_path=db_path,
            snapshot_id=args.snapshot_id,
            out_dir=out_dir,
            sizes=_parse_sizes_csv(sizes_token),
            limit=int(args.limit),
            workers=int(args.workers),
            resume=bool(args.resume),
            progress_every=int(args.progress),
            rate_limit_per_sec=float(args.rate_limit),
            timeout_seconds=float(args.timeout),
            source_mode=args.source,
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
