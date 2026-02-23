from __future__ import annotations

import sqlite3
from pathlib import Path

import engine.db as cards_db


def _create_valid_sqlite_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    try:
        con.execute("CREATE TABLE IF NOT EXISTS healthcheck (id INTEGER PRIMARY KEY)")
        con.commit()
    finally:
        con.close()


def test_resolve_db_path_prefers_valid_external_db_over_zero_byte_repo_local(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_local_db = (tmp_path / "mtg.sqlite").resolve()
    repo_local_db.write_bytes(b"")

    external_db = (tmp_path / "external.sqlite").resolve()
    _create_valid_sqlite_db(external_db)

    monkeypatch.delenv("MTG_ENGINE_DB_PATH", raising=False)
    monkeypatch.setattr(cards_db, "DB_PATH", repo_local_db)
    monkeypatch.setattr(cards_db, "EXTERNAL_DB_PATH", external_db)

    resolved = cards_db.resolve_db_path()

    assert resolved == external_db
