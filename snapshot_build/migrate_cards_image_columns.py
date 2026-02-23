from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Sequence

from engine.db import resolve_db_path

REQUIRED_IMAGE_COLUMNS: Sequence[tuple[str, str]] = (
    ("image_uris_json", "TEXT"),
    ("card_faces_json", "TEXT"),
    ("image_status", "TEXT"),
)


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
    return candidate


def _load_cards_columns(con: sqlite3.Connection) -> List[str]:
    rows = con.execute("PRAGMA table_info(cards)").fetchall()
    columns: List[str] = []
    for row in rows:
        row_dict = dict(row) if isinstance(row, sqlite3.Row) else {}
        name = row_dict.get("name")
        if isinstance(name, str) and name != "":
            columns.append(name)
    return sorted(set(columns))


def ensure_cards_image_columns(*, db_path: Path) -> Dict[str, Any]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        table_exists = con.execute(
            "SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = 'cards'"
        ).fetchone()
        if not table_exists or int(table_exists[0]) <= 0:
            raise RuntimeError("cards table not found in target DB")

        existing_columns = set(_load_cards_columns(con))
        added_columns: List[str] = []

        for column_name, column_type in REQUIRED_IMAGE_COLUMNS:
            if column_name in existing_columns:
                continue
            con.execute(f"ALTER TABLE cards ADD COLUMN {column_name} {column_type}")
            added_columns.append(column_name)
            existing_columns.add(column_name)

        con.commit()

        return {
            "db_path": str(db_path),
            "added_columns": added_columns,
            "cards_columns": sorted(existing_columns),
        }
    finally:
        con.close()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ensure cards image metadata columns exist")
    parser.add_argument("--db", default="", help="Path to SQLite DB (defaults to MTG_ENGINE_DB_PATH / ./data/mtg.sqlite)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        db_path = _resolve_db_path_from_cli(args.db)
        summary = ensure_cards_image_columns(db_path=db_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        return 2
    except Exception as exc:
        print(f"ERROR: unexpected failure: {exc}")
        return 2

    added_columns = summary.get("added_columns") if isinstance(summary.get("added_columns"), list) else []
    print(f"cards image columns ready | db={summary.get('db_path')} added={len(added_columns)} columns={added_columns}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
