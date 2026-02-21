from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


DECKLIST_FIXTURE_SNAPSHOT_ID = "DECKLIST_TEST_SNAPSHOT"


def _fixture_sql_path() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "decklist_resolve_v1_fixture.sql"


def create_decklist_fixture_db(tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = (tmp_dir / "decklist_fixture.sqlite").resolve()

    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(_fixture_sql_path().read_text(encoding="utf-8"))
        con.commit()
    finally:
        con.close()

    return db_path


@contextmanager
def set_decklist_fixture_env(db_path: Path) -> Iterator[None]:
    previous = os.environ.get("MTG_ENGINE_DB_PATH")
    os.environ["MTG_ENGINE_DB_PATH"] = str(db_path.resolve())
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("MTG_ENGINE_DB_PATH", None)
        else:
            os.environ["MTG_ENGINE_DB_PATH"] = previous
