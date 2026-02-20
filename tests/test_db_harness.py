from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

TEST_SNAPSHOT_ID = "TEST_SNAPSHOT_0001"
TEST_SNAPSHOT_CREATED_AT = "2026-01-01T00:00:00+00:00"
TEST_SNAPSHOT_SOURCE = "unittest_hermetic"
TEST_SCRYFALL_BULK_URI = "local://unittest/scryfall_bulk/default_cards"
TEST_SCRYFALL_BULK_UPDATED_AT = "2026-01-01T00:00:00+00:00"


def _schema_sql_path() -> Path:
    return Path(__file__).resolve().parents[1] / "schemas" / "schema.sql"


def _stable_manifest_json() -> str:
    return json.dumps({"tags_compiled": 1}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def create_temp_sqlite_db(tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = (tmp_dir / "mtg_test.sqlite").resolve()

    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(_schema_sql_path().read_text(encoding="utf-8"))
        con.execute(
            """
            INSERT OR REPLACE INTO snapshots (
              snapshot_id,
              created_at,
              source,
              scryfall_bulk_uri,
              scryfall_bulk_updated_at,
              manifest_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                TEST_SNAPSHOT_ID,
                TEST_SNAPSHOT_CREATED_AT,
                TEST_SNAPSHOT_SOURCE,
                TEST_SCRYFALL_BULK_URI,
                TEST_SCRYFALL_BULK_UPDATED_AT,
                _stable_manifest_json(),
            ),
        )
        con.commit()
    finally:
        con.close()

    return db_path


@contextmanager
def set_env_db_path(db_path: Path) -> Iterator[None]:
    previous = os.environ.get("MTG_ENGINE_DB_PATH")
    os.environ["MTG_ENGINE_DB_PATH"] = str(db_path.resolve())
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("MTG_ENGINE_DB_PATH", None)
        else:
            os.environ["MTG_ENGINE_DB_PATH"] = previous
