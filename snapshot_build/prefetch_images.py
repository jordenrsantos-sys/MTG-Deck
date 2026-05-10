from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Sequence

from engine.db import resolve_db_path, resolve_image_cache_dir
from engine.image_cache_contract import normalize_image_size, normalize_oracle_id
from engine.image_runtime import ensure_card_image_cached
from snapshot_build.migrate_card_images_table import ensure_card_images_table


def _nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
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
    return candidate.resolve()


def _normalize_size(raw_size: Any) -> str:
    token = _nonempty_str(raw_size)
    if token == "":
        token = "normal"

    try:
        normalized = normalize_image_size(token)
    except ValueError as exc:
        raise RuntimeError("--size must be one of: normal, small") from exc

    if normalized not in {"normal", "small"}:
        raise RuntimeError("--size must be one of: normal, small")
    return normalized


def _load_snapshot_oracle_ids(*, con: sqlite3.Connection, snapshot_id: str) -> List[str]:
    rows = con.execute(
        """
        SELECT DISTINCT oracle_id
        FROM cards
        WHERE snapshot_id = ?
        ORDER BY oracle_id ASC
        """,
        (snapshot_id,),
    ).fetchall()

    oracle_ids: List[str] = []
    for row in rows:
        try:
            row_dict = dict(row)
            oracle_id_raw = row_dict.get("oracle_id")
        except Exception:
            oracle_id_raw = row[0] if isinstance(row, (tuple, list)) and len(row) > 0 else ""

        oracle_id_token = _nonempty_str(oracle_id_raw)
        if oracle_id_token == "":
            continue

        try:
            oracle_ids.append(normalize_oracle_id(oracle_id_token))
        except ValueError:
            continue

    return oracle_ids


def prefetch_snapshot_images(*, db_path: Path, snapshot_id: str, size: str) -> Dict[str, Any]:
    snapshot_id_clean = _nonempty_str(snapshot_id)
    if snapshot_id_clean == "":
        raise RuntimeError("--snapshot_id is required")

    size_clean = _normalize_size(size)
    cache_dir = resolve_image_cache_dir()

    ensure_card_images_table(db_path=db_path)

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        oracle_ids = _load_snapshot_oracle_ids(con=con, snapshot_id=snapshot_id_clean)

        total = len(oracle_ids)
        cache_hits = 0
        fetched = 0
        missing_uri = 0
        failed = 0

        print(
            "Prefetch start | "
            f"snapshot_id={snapshot_id_clean} size={size_clean} cards={total} cache_dir={cache_dir}"
        )

        for index, oracle_id in enumerate(oracle_ids, start=1):
            result = ensure_card_image_cached(
                con=con,
                cache_dir=cache_dir,
                oracle_id=oracle_id,
                size=size_clean,
                current_snapshot_id=snapshot_id_clean,
            )
            status = _nonempty_str(result.get("status"))

            if status == "CACHE_HIT":
                cache_hits += 1
            elif status == "IMAGE_FETCHED":
                fetched += 1
            elif status == "IMAGE_URI_MISSING":
                missing_uri += 1
            elif status == "IMAGE_FETCH_FAILED":
                failed += 1
                print(
                    "FAIL | "
                    f"oracle_id={oracle_id} size={size_clean} error={_nonempty_str(result.get('error')) or '(unknown)'}"
                )
            else:
                failed += 1
                print(f"FAIL | oracle_id={oracle_id} size={size_clean} error=unexpected_status:{status}")

            if index % 500 == 0 or index == total:
                print(
                    "Progress | "
                    f"{index}/{total} cache_hits={cache_hits} fetched={fetched} "
                    f"missing_uri={missing_uri} failed={failed}"
                )

        stored_total = cache_hits + fetched
        print(f"Prefetch complete: {stored_total} images stored")

        return {
            "snapshot_id": snapshot_id_clean,
            "size": size_clean,
            "total": total,
            "cache_hits": cache_hits,
            "fetched": fetched,
            "missing_uri": missing_uri,
            "failed": failed,
            "stored_total": stored_total,
            "cache_dir": cache_dir,
            "db_path": str(db_path),
        }
    finally:
        con.close()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prefetch snapshot images into local runtime cache")
    parser.add_argument("--db", default="", help="Path to SQLite DB (defaults to MTG_ENGINE_DB_PATH / ./data/mtg.sqlite)")
    parser.add_argument("--snapshot_id", required=True, help="Snapshot ID to prefetch")
    parser.add_argument("--size", default="normal", help="Image size to prefetch (normal|small)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        db_path = _resolve_db_path_from_cli(args.db)
        summary = prefetch_snapshot_images(
            db_path=db_path,
            snapshot_id=args.snapshot_id,
            size=args.size,
        )
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        return 2
    except Exception as exc:
        print(f"ERROR: unexpected failure: {exc}")
        return 2

    return 0 if int(summary.get("failed", 0)) == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
