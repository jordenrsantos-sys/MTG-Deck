from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Sequence

from engine.db import resolve_db_path

REQUIRED_CARD_IMAGES_COLUMNS: Sequence[str] = (
    "oracle_id",
    "img_normal_uri",
    "img_small_uri",
    "img_source",
    "img_enriched_at",
    "img_bulk_version",
)

_CARD_IMAGES_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS card_images (
  oracle_id TEXT PRIMARY KEY,
  img_normal_uri TEXT,
  img_small_uri TEXT,
  img_source TEXT NOT NULL,
  img_enriched_at TEXT NOT NULL,
  img_bulk_version TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_card_images_bulk_version
  ON card_images(img_bulk_version);
"""


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


def ensure_card_images_table(*, db_path: Path) -> Dict[str, Any]:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        if not _table_exists(con, "cards"):
            raise RuntimeError("cards table not found in target DB")

        created_table = False
        if not _table_exists(con, "card_images"):
            con.executescript(_CARD_IMAGES_TABLE_DDL)
            created_table = True
        else:
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_card_images_bulk_version ON card_images(img_bulk_version)"
            )

        columns = _load_table_columns(con, "card_images")
        missing_columns = [column for column in REQUIRED_CARD_IMAGES_COLUMNS if column not in columns]
        if missing_columns:
            raise RuntimeError(
                "card_images table missing required columns: "
                + ", ".join(missing_columns)
            )

        con.commit()
        return {
            "db_path": str(db_path),
            "created_table": created_table,
            "card_images_columns": columns,
        }
    finally:
        con.close()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ensure card_images table exists for image enrichment metadata")
    parser.add_argument("--db", default="", help="Path to SQLite DB (defaults to MTG_ENGINE_DB_PATH / ./data/mtg.sqlite)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        db_path = _resolve_db_path_from_cli(args.db)
        summary = ensure_card_images_table(db_path=db_path)
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        return 2
    except Exception as exc:
        print(f"ERROR: unexpected failure: {exc}")
        return 2

    print(
        "card_images table ready | "
        f"db={summary.get('db_path')} created_table={summary.get('created_table')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
