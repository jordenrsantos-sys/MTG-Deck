from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple
from uuid import UUID

from engine.determinism import stable_json_dumps
from snapshot_build.migrate_card_images_table import ensure_card_images_table

IMG_SOURCE = "scryfall_bulk_default_cards"
DEFAULT_REPORT_SAMPLE_SIZE = 10
REPO_ROOT = Path(__file__).resolve().parents[1]


def _nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return ""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_oracle_id(raw_oracle_id: Any) -> str:
    token = _nonempty_str(raw_oracle_id)
    if token == "":
        return ""
    try:
        return str(UUID(token))
    except Exception:
        return ""


def _resolve_db_path(raw_db_path: Any) -> Path:
    token = _nonempty_str(raw_db_path)
    if token == "":
        raise RuntimeError("--db is required")

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    if not candidate.is_file():
        raise RuntimeError(f"Database file not found: {candidate}")
    return candidate


def _resolve_bulk_json_path(raw_bulk_json: Any) -> Path:
    token = _nonempty_str(raw_bulk_json)
    if token == "":
        raise RuntimeError("--bulk-json is required")

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    if not candidate.is_file():
        raise RuntimeError(f"Bulk JSON file not found: {candidate}")
    return candidate


def _resolve_logs_dir(raw_logs_dir: Any) -> Path:
    token = _nonempty_str(raw_logs_dir)
    if token == "":
        return (REPO_ROOT / "logs").resolve()

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return candidate.resolve()


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _load_table_columns(con: sqlite3.Connection, table_name: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    columns: List[str] = []
    for row in rows:
        row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
        name = row_dict.get("name")
        if isinstance(name, str) and name != "":
            columns.append(name)
    return sorted(set(columns))


def _load_bulk_rows(bulk_json_path: Path) -> List[Any]:
    payload = json.loads(bulk_json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError("Bulk JSON must be a top-level list")
    return payload


def _extract_uri(image_uris: Any, key: str) -> str | None:
    if not isinstance(image_uris, dict):
        return None
    value = image_uris.get(key)
    token = _nonempty_str(value)
    return token if token != "" else None


def _extract_normal_small_uris(card_row: Dict[str, Any]) -> Tuple[str | None, str | None]:
    image_uris = card_row.get("image_uris") if isinstance(card_row.get("image_uris"), dict) else {}
    normal_uri = _extract_uri(image_uris, "normal")
    small_uri = _extract_uri(image_uris, "small")

    card_faces = card_row.get("card_faces") if isinstance(card_row.get("card_faces"), list) else []
    face0 = card_faces[0] if len(card_faces) > 0 and isinstance(card_faces[0], dict) else {}
    face0_uris = face0.get("image_uris") if isinstance(face0.get("image_uris"), dict) else {}

    if normal_uri is None:
        normal_uri = _extract_uri(face0_uris, "normal")
    if small_uri is None:
        small_uri = _extract_uri(face0_uris, "small")

    return normal_uri, small_uri


def _build_bulk_image_map(*, bulk_rows: Sequence[Any], limit: int) -> Dict[str, Dict[str, Any]]:
    by_oracle_id: Dict[str, Dict[str, Any]] = {}
    max_rows = int(limit)

    scanned_rows = 0
    for raw_row in bulk_rows:
        if max_rows > 0 and scanned_rows >= max_rows:
            break
        scanned_rows += 1

        if not isinstance(raw_row, dict):
            continue

        oracle_id = _normalize_oracle_id(raw_row.get("oracle_id"))
        if oracle_id == "":
            continue

        normal_uri, small_uri = _extract_normal_small_uris(raw_row)
        score = int(normal_uri is not None) + int(small_uri is not None)

        existing = by_oracle_id.get(oracle_id)
        if existing is None or score > int(existing.get("score", -1)):
            by_oracle_id[oracle_id] = {
                "img_normal_uri": normal_uri,
                "img_small_uri": small_uri,
                "score": score,
            }

    return by_oracle_id


def _load_db_oracle_ids(con: sqlite3.Connection) -> List[str]:
    rows = con.execute(
        """
        SELECT DISTINCT oracle_id
        FROM cards
        WHERE oracle_id IS NOT NULL
          AND TRIM(oracle_id) <> ''
        ORDER BY oracle_id ASC
        """
    ).fetchall()
    return [str(row[0]) for row in rows if isinstance(row[0], str) and row[0] != ""]


def _resolve_report_snapshot_id(con: sqlite3.Connection) -> str:
    if not _table_exists(con, "snapshots"):
        return "unknown_snapshot"

    row = con.execute(
        """
        SELECT snapshot_id
        FROM snapshots
        ORDER BY created_at DESC, snapshot_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return "unknown_snapshot"

    snapshot_id = _nonempty_str(row[0])
    return snapshot_id if snapshot_id != "" else "unknown_snapshot"


def _update_snapshot_manifest(
    *,
    con: sqlite3.Connection,
    snapshot_id: str,
    img_bulk_version: str,
    img_enriched_at: str,
    rows_upserted: int,
) -> bool:
    if snapshot_id == "unknown_snapshot" or not _table_exists(con, "snapshots"):
        return False

    row = con.execute(
        "SELECT manifest_json FROM snapshots WHERE snapshot_id = ? LIMIT 1",
        (snapshot_id,),
    ).fetchone()
    if row is None:
        return False

    manifest_raw = row[0]
    manifest: Dict[str, Any] = {}
    if isinstance(manifest_raw, str) and manifest_raw.strip() != "":
        try:
            parsed = json.loads(manifest_raw)
            if isinstance(parsed, dict):
                manifest = parsed
        except Exception:
            manifest = {}

    manifest["image_enrichment_v1"] = {
        "img_source": IMG_SOURCE,
        "img_bulk_version": img_bulk_version,
        "img_enriched_at": img_enriched_at,
        "rows_upserted": int(rows_upserted),
    }

    con.execute(
        "UPDATE snapshots SET manifest_json = ? WHERE snapshot_id = ?",
        (stable_json_dumps(manifest), snapshot_id),
    )
    return True


def enrich_images_from_scryfall_bulk(
    *,
    db_path: Path,
    bulk_json_path: Path,
    bulk_version: str | None = None,
    limit: int = 0,
    allow_empty: bool = False,
    logs_dir: Path | None = None,
) -> Dict[str, Any]:
    if not bulk_json_path.is_file():
        raise RuntimeError(f"Bulk JSON file not found: {bulk_json_path}")

    migration_summary = ensure_card_images_table(db_path=db_path)

    resolved_bulk_version = _nonempty_str(bulk_version)
    if resolved_bulk_version == "":
        resolved_bulk_version = _sha256_file(bulk_json_path)

    bulk_rows = _load_bulk_rows(bulk_json_path)
    image_payload_by_oracle_id = _build_bulk_image_map(bulk_rows=bulk_rows, limit=int(limit))

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        if not _table_exists(con, "cards"):
            raise RuntimeError("cards table not found in target DB")

        cards_columns = set(_load_table_columns(con, "cards"))
        if "oracle_id" not in cards_columns:
            raise RuntimeError("cards table missing required oracle_id column")

        db_oracle_ids = _load_db_oracle_ids(con)
        db_oracle_id_set = set(db_oracle_ids)
        bulk_oracle_id_set = set(image_payload_by_oracle_id.keys())

        matched_oracle_ids = sorted(db_oracle_id_set.intersection(bulk_oracle_id_set))
        unmatched_oracle_ids_in_db = sorted(db_oracle_id_set.difference(bulk_oracle_id_set))
        bulk_oracle_ids_not_in_db = sorted(bulk_oracle_id_set.difference(db_oracle_id_set))

        img_enriched_at = _utc_now_iso()
        rows_upserted = 0
        enriched_count = 0
        missing_image_uris_count = 0

        for oracle_id in matched_oracle_ids:
            payload = image_payload_by_oracle_id[oracle_id]
            img_normal_uri = payload.get("img_normal_uri") if isinstance(payload.get("img_normal_uri"), str) else None
            img_small_uri = payload.get("img_small_uri") if isinstance(payload.get("img_small_uri"), str) else None

            if img_normal_uri is None and img_small_uri is None:
                missing_image_uris_count += 1
            else:
                enriched_count += 1

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
                    IMG_SOURCE,
                    img_enriched_at,
                    resolved_bulk_version,
                ),
            )
            rows_upserted += 1

        if rows_upserted == 0 and not allow_empty:
            raise RuntimeError(
                "No rows were updated in card_images. Use --allow-empty to accept zero-row enrichment."
            )

        db_snapshot_id = _resolve_report_snapshot_id(con)
        manifest_updated = _update_snapshot_manifest(
            con=con,
            snapshot_id=db_snapshot_id,
            img_bulk_version=resolved_bulk_version,
            img_enriched_at=img_enriched_at,
            rows_upserted=rows_upserted,
        )
        con.commit()
    finally:
        con.close()

    report_root = logs_dir if isinstance(logs_dir, Path) else (REPO_ROOT / "logs")
    report_root.mkdir(parents=True, exist_ok=True)
    safe_snapshot_token = db_snapshot_id.replace("/", "_").replace("\\", "_")
    report_path = report_root / f"image_enrich_report_{safe_snapshot_token}.json"

    report = {
        "db_path": str(db_path),
        "db_snapshot_id": db_snapshot_id,
        "bulk_json_path": str(bulk_json_path),
        "bulk_version": resolved_bulk_version,
        "img_source": IMG_SOURCE,
        "img_enriched_at": img_enriched_at,
        "limit": int(limit),
        "allow_empty": bool(allow_empty),
        "total_cards_scanned": min(len(bulk_rows), int(limit)) if int(limit) > 0 else len(bulk_rows),
        "bulk_oracle_ids_scanned": len(image_payload_by_oracle_id),
        "oracle_ids_in_db": len(db_oracle_ids),
        "oracle_ids_matched_to_db": len(matched_oracle_ids),
        "rows_upserted": rows_upserted,
        "enriched_count": enriched_count,
        "missing_image_uris_count": missing_image_uris_count,
        "unmatched_oracle_ids_in_db_count": len(unmatched_oracle_ids_in_db),
        "unmatched_oracle_ids_in_db_sample": unmatched_oracle_ids_in_db[:DEFAULT_REPORT_SAMPLE_SIZE],
        "bulk_oracle_ids_not_in_db_count": len(bulk_oracle_ids_not_in_db),
        "bulk_oracle_ids_not_in_db_sample": bulk_oracle_ids_not_in_db[:DEFAULT_REPORT_SAMPLE_SIZE],
        "card_images_table_created": bool(migration_summary.get("created_table")),
        "manifest_updated": manifest_updated,
        "report_path": str(report_path),
    }

    report_path.write_text(stable_json_dumps(report), encoding="utf-8")
    return report


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enrich local card_images table from a local Scryfall Default Cards bulk JSON file")
    parser.add_argument("--db", required=True, help="Path to SQLite DB")
    parser.add_argument("--bulk-json", required=True, dest="bulk_json", help="Path to local default-cards bulk JSON")
    parser.add_argument("--bulk-version", default="", dest="bulk_version", help="Optional bulk version token; defaults to SHA256 of --bulk-json")
    parser.add_argument("--limit", type=int, default=0, help="Optional max number of bulk rows to scan (<=0 means full file)")
    parser.add_argument("--allow-empty", action="store_true", help="Allow successful exit when zero rows are updated")
    parser.add_argument("--logs-dir", default="", help="Optional report output directory (default: ./logs)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        db_path = _resolve_db_path(args.db)
        bulk_json_path = _resolve_bulk_json_path(args.bulk_json)
        logs_dir = _resolve_logs_dir(args.logs_dir)
        report = enrich_images_from_scryfall_bulk(
            db_path=db_path,
            bulk_json_path=bulk_json_path,
            bulk_version=args.bulk_version,
            limit=int(args.limit),
            allow_empty=bool(args.allow_empty),
            logs_dir=logs_dir,
        )
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        return 2
    except Exception as exc:
        print(f"ERROR: unexpected failure: {exc}")
        return 2

    print(stable_json_dumps(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
