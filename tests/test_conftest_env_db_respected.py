from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

SQLITE_HEADER_PREFIX = b"SQLite format 3"


def _create_sqlite_db(path: Path) -> None:
    con = sqlite3.connect(str(path))
    try:
        con.execute("CREATE TABLE fixture_guard (id INTEGER PRIMARY KEY)")
        con.commit()
    finally:
        con.close()


def _read_header(path: Path) -> bytes:
    with path.open("rb") as handle:
        return handle.read(16)


@pytest.fixture(scope="class")
def _valid_env_db_path(tmp_path_factory: pytest.TempPathFactory):
    base_dir = tmp_path_factory.mktemp("conftest_env_valid")
    db_path = (base_dir / "preset_fixture.sqlite").resolve()
    _create_sqlite_db(db_path)

    env = pytest.MonkeyPatch()
    env.setenv("MTG_ENGINE_DB_PATH", str(db_path))
    try:
        yield db_path
    finally:
        env.undo()


@pytest.mark.usefixtures("_valid_env_db_path")
class TestConftestRespectsValidEnvDb:
    def test_valid_env_db_path_is_not_overwritten(self, _valid_env_db_path: Path) -> None:
        env_value = os.environ.get("MTG_ENGINE_DB_PATH")
        assert env_value == str(_valid_env_db_path)
        assert _read_header(_valid_env_db_path).startswith(SQLITE_HEADER_PREFIX)


@pytest.fixture(scope="class")
def _unset_env_db_path():
    env = pytest.MonkeyPatch()
    env.delenv("MTG_ENGINE_DB_PATH", raising=False)
    try:
        yield
    finally:
        env.undo()


@pytest.mark.usefixtures("_unset_env_db_path")
class TestConftestCreatesDbWhenEnvMissing:
    def test_unset_env_creates_valid_pytest_db(self) -> None:
        env_value = os.environ.get("MTG_ENGINE_DB_PATH")
        assert isinstance(env_value, str) and env_value != ""

        db_path = Path(env_value)
        assert db_path.is_file()
        assert _read_header(db_path).startswith(SQLITE_HEADER_PREFIX)


@pytest.fixture(scope="class")
def _invalid_env_db_path(tmp_path_factory: pytest.TempPathFactory):
    base_dir = tmp_path_factory.mktemp("conftest_env_invalid")
    invalid_path = (base_dir / "not_sqlite.sqlite").resolve()
    invalid_path.write_text("not a sqlite database", encoding="utf-8")

    env = pytest.MonkeyPatch()
    env.setenv("MTG_ENGINE_DB_PATH", str(invalid_path))
    try:
        yield invalid_path
    finally:
        env.undo()


@pytest.mark.usefixtures("_invalid_env_db_path")
class TestConftestReplacesInvalidEnvDb:
    def test_invalid_env_db_is_replaced_with_valid_pytest_db(self, _invalid_env_db_path: Path) -> None:
        env_value = os.environ.get("MTG_ENGINE_DB_PATH")
        assert isinstance(env_value, str) and env_value != ""

        db_path = Path(env_value).resolve()
        assert db_path != _invalid_env_db_path
        assert db_path.is_file()
        assert _read_header(db_path).startswith(SQLITE_HEADER_PREFIX)
