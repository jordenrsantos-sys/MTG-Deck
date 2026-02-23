from __future__ import annotations

import argparse
import json
import sqlite3
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple
from uuid import UUID

from engine.determinism import stable_json_dumps
from snapshot_build.migrate_cards_image_columns import ensure_cards_image_columns

DEFAULT_BULK_INDEX_URL = "https://api.scryfall.com/bulk-data"
DEFAULT_BULK_TYPE = "default_cards"
HTTP_HEADERS = {
    "User-Agent": "mtg-engine/1.0 (local-update-mode)",
    "Accept": "application/json",
}


def _nonempty_str(value: Any) -> str:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return ""


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
        raise RuntimeError("--bulk_json is required")

    candidate = Path(token).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return candidate


def _fetch_json(url: str, timeout_seconds: float = 60.0) -> Any:
    request = urllib.request.Request(url, headers=HTTP_HEADERS)
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        payload = response.read()
    return json.loads(payload.decode("utf-8"))


def _download_file(url: str, out_path: Path, timeout_seconds: float = 180.0) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url, headers=HTTP_HEADERS)
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        with out_path.open("wb") as file_obj:
            file_obj.write(response.read())


def download_default_cards_bulk_json(*, out_path: Path) -> Dict[str, Any]:
    index_payload = _fetch_json(DEFAULT_BULK_INDEX_URL)
    rows = index_payload.get("data") if isinstance(index_payload, dict) else []
    if not isinstance(rows, list):
        rows = []

    selected_row: Dict[str, Any] | None = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _nonempty_str(row.get("type")) == DEFAULT_BULK_TYPE:
            selected_row = row
            break

    if selected_row is None:
        raise RuntimeError("Scryfall bulk-data index did not include default_cards")

    download_uri = _nonempty_str(selected_row.get("download_uri"))
    if download_uri == "":
        raise RuntimeError("default_cards entry missing download_uri")

    _download_file(download_uri, out_path=out_path)
    return {
        "bulk_type": DEFAULT_BULK_TYPE,
        "download_uri": download_uri,
        "updated_at": _nonempty_str(selected_row.get("updated_at")),
        "out_path": str(out_path),
    }


def _sanitize_card_faces(raw_card_faces: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_card_faces, list):
        return []

    sanitized: List[Dict[str, Any]] = []
    for row in raw_card_faces:
        if isinstance(row, dict):
            sanitized.append(row)
    return sanitized


def _collector_sort_key(value: Any) -> str:
    token = _nonempty_str(value)
    if token == "":
        return "~"

    digits = ""
    suffix = ""
    for ch in token:
        if ch.isdigit() and suffix == "":
            digits += ch
        else:
            suffix += ch
    if digits == "":
        return f"~{token.lower()}"
    return f"{int(digits):08d}{suffix.lower()}"


def _extract_image_payload(card_row: Dict[str, Any]) -> Dict[str, Any] | None:
    image_uris = card_row.get("image_uris") if isinstance(card_row.get("image_uris"), dict) else {}
    card_faces = _sanitize_card_faces(card_row.get("card_faces"))
    image_status = _nonempty_str(card_row.get("image_status"))

    image_uris_json = stable_json_dumps(image_uris) if len(image_uris) > 0 else None
    card_faces_json = stable_json_dumps(card_faces) if len(card_faces) > 0 else None
    image_status_value = image_status if image_status != "" else None

    if image_uris_json is None and card_faces_json is None and image_status_value is None:
        return None

    return {
        "image_uris_json": image_uris_json,
        "card_faces_json": card_faces_json,
        "image_status": image_status_value,
        "rank": (
            0 if image_uris_json is not None else 1,
            0 if card_faces_json is not None else 1,
            _nonempty_str(card_row.get("released_at")) or "9999-99-99",
            _nonempty_str(card_row.get("set")) or "~",
            _collector_sort_key(card_row.get("collector_number")),
            _nonempty_str(card_row.get("lang")) or "~",
            _nonempty_str(card_row.get("id")) or "~",
        ),
    }


def _build_bulk_image_map(*, bulk_json_path: Path) -> Dict[str, Dict[str, Any]]:
    if not bulk_json_path.is_file():
        raise RuntimeError(f"Bulk JSON file not found: {bulk_json_path}")

    payload = json.loads(bulk_json_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError("Bulk JSON must be a top-level list")

    by_oracle_id: Dict[str, Dict[str, Any]] = {}

    for card_row in payload:
        if not isinstance(card_row, dict):
            continue

        oracle_id = _normalize_oracle_id(card_row.get("oracle_id"))
        if oracle_id == "":
            continue

        image_payload = _extract_image_payload(card_row)
        if image_payload is None:
            continue

        existing = by_oracle_id.get(oracle_id)
        if existing is None or image_payload["rank"] < existing["rank"]:
            by_oracle_id[oracle_id] = image_payload

    return by_oracle_id


def _load_snapshot_oracle_ids(*, con: sqlite3.Connection, snapshot_id: str) -> List[str]:
    rows = con.execute(
        """
        SELECT oracle_id
        FROM cards
        WHERE snapshot_id = ?
        ORDER BY oracle_id ASC
        """,
        (snapshot_id,),
    ).fetchall()
    return [str(row[0]) for row in rows if isinstance(row[0], str) and row[0] != ""]


def _load_cards_columns(*, con: sqlite3.Connection) -> List[str]:
    rows = con.execute("PRAGMA table_info(cards)").fetchall()
    columns: List[str] = []
    for row in rows:
        row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
        name = row_dict.get("name")
        if isinstance(name, str) and name != "":
            columns.append(name)
    return sorted(set(columns))


def _load_all_oracle_ids(*, con: sqlite3.Connection) -> List[str]:
    rows = con.execute(
        """
        SELECT oracle_id
        FROM cards
        ORDER BY oracle_id ASC
        """
    ).fetchall()
    return [str(row[0]) for row in rows if isinstance(row[0], str) and row[0] != ""]


def enrich_snapshot_images(
    *,
    db_path: Path,
    snapshot_id: str,
    bulk_json_path: Path,
) -> Dict[str, Any]:
    normalized_snapshot_id = _nonempty_str(snapshot_id)
    if normalized_snapshot_id == "":
        raise RuntimeError("--snapshot_id is required")

    ensure_cards_image_columns(db_path=db_path)
    image_payload_by_oracle = _build_bulk_image_map(bulk_json_path=bulk_json_path)
    bulk_oracle_ids = set(image_payload_by_oracle.keys())

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        cards_columns = set(_load_cards_columns(con=con))
        if "oracle_id" not in cards_columns:
            raise RuntimeError("cards table missing required oracle_id column")

        snapshot_scoped = "snapshot_id" in cards_columns
        if snapshot_scoped:
            snapshot_oracle_ids = _load_snapshot_oracle_ids(con=con, snapshot_id=normalized_snapshot_id)
        else:
            snapshot_oracle_ids = _load_all_oracle_ids(con=con)

        snapshot_oracle_id_set = set(snapshot_oracle_ids)

        to_update = sorted(snapshot_oracle_id_set.intersection(bulk_oracle_ids))
        missing_oracle_ids = sorted(snapshot_oracle_id_set.difference(bulk_oracle_ids))
        bulk_oracle_ids_not_in_db = sorted(bulk_oracle_ids.difference(snapshot_oracle_id_set))

        updated_rows = 0
        for oracle_id in to_update:
            payload = image_payload_by_oracle[oracle_id]
            if snapshot_scoped:
                con.execute(
                    """
                    UPDATE cards
                    SET image_uris_json = ?,
                        card_faces_json = ?,
                        image_status = ?
                    WHERE snapshot_id = ?
                      AND oracle_id = ?
                    """,
                    (
                        payload.get("image_uris_json"),
                        payload.get("card_faces_json"),
                        payload.get("image_status"),
                        normalized_snapshot_id,
                        oracle_id,
                    ),
                )
            else:
                con.execute(
                    """
                    UPDATE cards
                    SET image_uris_json = ?,
                        card_faces_json = ?,
                        image_status = ?
                    WHERE oracle_id = ?
                    """,
                    (
                        payload.get("image_uris_json"),
                        payload.get("card_faces_json"),
                        payload.get("image_status"),
                        oracle_id,
                    ),
                )
            updated_rows += 1

        con.commit()
    finally:
        con.close()

    return {
        "db_path": str(db_path),
        "snapshot_id": normalized_snapshot_id,
        "bulk_json": str(bulk_json_path),
        "snapshot_scoped_cards": snapshot_scoped,
        "updated_rows": updated_rows,
        "missing_oracle_ids": len(missing_oracle_ids),
        "bulk_oracle_ids_not_in_db": len(bulk_oracle_ids_not_in_db),
        "missing_oracle_ids_sample": missing_oracle_ids[:10],
        "bulk_oracle_ids_not_in_db_sample": bulk_oracle_ids_not_in_db[:10],
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Enrich snapshot card rows with image metadata from Scryfall default-cards bulk JSON")
    parser.add_argument("--db", required=True, help="Path to SQLite DB")
    parser.add_argument("--snapshot_id", required=True, help="Snapshot ID to enrich")
    parser.add_argument("--bulk_json", required=True, help="Path to local default-cards bulk JSON file")
    parser.add_argument(
        "--download_bulk_if_missing",
        action="store_true",
        help="If --bulk_json file is missing, download latest default-cards bulk JSON first",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        db_path = _resolve_db_path(args.db)
        bulk_json_path = _resolve_bulk_json_path(args.bulk_json)

        download_summary: Dict[str, Any] | None = None
        if args.download_bulk_if_missing and not bulk_json_path.is_file():
            download_summary = download_default_cards_bulk_json(out_path=bulk_json_path)
            print(
                "Downloaded default-cards bulk | "
                f"out={download_summary.get('out_path')} updated_at={download_summary.get('updated_at')}"
            )

        summary = enrich_snapshot_images(
            db_path=db_path,
            snapshot_id=args.snapshot_id,
            bulk_json_path=bulk_json_path,
        )
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        return 2
    except Exception as exc:
        print(f"ERROR: unexpected failure: {exc}")
        return 2

    print(
        "Enrich summary | "
        f"snapshot_id={summary['snapshot_id']} updated_rows={summary['updated_rows']} "
        f"missing_oracle_ids={summary['missing_oracle_ids']} bulk_oracle_ids_not_in_db={summary['bulk_oracle_ids_not_in_db']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
