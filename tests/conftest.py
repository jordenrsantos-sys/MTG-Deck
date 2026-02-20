from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

TEST_SNAPSHOT_ID = "TEST_SNAPSHOT_0001"
TEST_SNAPSHOT_CREATED_AT = "2026-01-01T00:00:00+00:00"
TEST_SNAPSHOT_SOURCE = "pytest_hermetic"
TEST_SCRYFALL_BULK_URI = "local://pytest/scryfall_bulk/default_cards"
TEST_SCRYFALL_BULK_UPDATED_AT = "2026-01-01T00:00:00+00:00"
TEST_MANIFEST_JSON = '{"tags_compiled":1,"fixture":"mtg_test_db_path"}'


def _schema_sql_path() -> Path:
    return Path(__file__).resolve().parents[1] / "schemas" / "schema.sql"


@pytest.fixture
def mtg_test_db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "mtg_test.sqlite"

    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(_schema_sql_path().read_text(encoding="utf-8"))
        con.execute(
            """
            INSERT INTO snapshots (
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
                TEST_MANIFEST_JSON,
            ),
        )
        con.commit()
    finally:
        con.close()

    monkeypatch.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    yield db_path


@pytest.fixture(autouse=True)
def _use_mtg_test_db_path(mtg_test_db_path: Path) -> None:
    _ = mtg_test_db_path
