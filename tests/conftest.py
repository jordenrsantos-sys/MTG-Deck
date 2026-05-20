from __future__ import annotations

import os
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


def _is_valid_sqlite_db(path: str) -> bool:
    try:
        if not path:
            return False
        if not os.path.isfile(path):
            return False
        with open(path, "rb") as f:
            header = f.read(16)
        return header.startswith(b"SQLite format 3")
    except Exception:
        return False


@pytest.fixture
def mtg_test_db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    env_db_path = os.getenv("MTG_ENGINE_DB_PATH", "")
    if _is_valid_sqlite_db(env_db_path):
        # Decklist/guardrails tests intentionally set MTG_ENGINE_DB_PATH
        # to their own fixture DBs. Do not override if a valid SQLite DB
        # is already set via environment.
        yield Path(env_db_path)
        return

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


@pytest.fixture(autouse=True)
def _disable_llm_layer_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pillar D iteration 2 added the LLM reasoning layer. By default
    pytest runs MUST NOT make real Anthropic API calls — developer
    machines have ANTHROPIC_API_KEY in the shell env, and silent
    network calls during the test suite are both expensive and a
    reproducibility hazard.

    This autouse fixture sets MTG_ENGINE_DISABLE_LLM=1 for every test.
    Tests that explicitly want to exercise the LLM path (Phase B2 / C2 /
    D2 unit tests) can re-enable it by patching the env var inside the
    test body or by using `enable_llm_layer` below.
    """
    monkeypatch.setenv("MTG_ENGINE_DISABLE_LLM", "1")
    # Force a clean singleton so a stale cached client doesn't carry
    # over a previous test's availability state.
    try:
        from api.engine.layers.agent_llm_client_v1 import reset_default_client_for_tests
        reset_default_client_for_tests()
    except Exception:
        pass


@pytest.fixture
def enable_llm_layer(monkeypatch: pytest.MonkeyPatch):
    """Re-enables the LLM layer for a single test. Pair with a mocked
    `call_with_budget` — this fixture does NOT permit real network calls.
    Sets ANTHROPIC_API_KEY to a dummy and unsets the kill switch so
    `is_available()` returns True.
    """
    monkeypatch.delenv("MTG_ENGINE_DISABLE_LLM", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-fixture")
    try:
        from api.engine.layers.agent_llm_client_v1 import reset_default_client_for_tests
        reset_default_client_for_tests()
    except Exception:
        pass
    yield
