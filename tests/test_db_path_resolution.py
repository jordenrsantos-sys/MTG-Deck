from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import engine.db as cards_db


def _create_valid_sqlite_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    try:
        con.execute("CREATE TABLE IF NOT EXISTS healthcheck (id INTEGER PRIMARY KEY)")
        con.commit()
    finally:
        con.close()


class DbPathResolutionTests(unittest.TestCase):
    def test_env_override_works(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "override.sqlite"
            _create_valid_sqlite_db(db_path)

            with patch.dict(os.environ, {"MTG_ENGINE_DB_PATH": str(db_path)}, clear=False):
                resolved = cards_db.resolve_db_path()

        self.assertEqual(resolved, db_path.resolve())

    def test_project_relative_path_works(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            db_path = (repo_root / "data" / "mtg.sqlite").resolve()
            external_missing = (repo_root / ".." / "data" / "mtg.sqlite").resolve()
            _create_valid_sqlite_db(db_path)

            with (
                patch.object(cards_db, "REPO_ROOT", repo_root),
                patch.object(cards_db, "DB_PATH", db_path),
                patch.object(cards_db, "EXTERNAL_DB_PATH", external_missing),
                patch.dict(os.environ, {}, clear=False),
            ):
                os.environ.pop("MTG_ENGINE_DB_PATH", None)
                resolved = cards_db.resolve_db_path()

        self.assertEqual(resolved, db_path)

    def test_missing_db_raises_runtime_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            missing_path = (repo_root / "data" / "mtg.sqlite").resolve()
            missing_external = (repo_root / ".." / "data" / "mtg.sqlite").resolve()

            with (
                patch.object(cards_db, "REPO_ROOT", repo_root),
                patch.object(cards_db, "DB_PATH", missing_path),
                patch.object(cards_db, "EXTERNAL_DB_PATH", missing_external),
                patch.dict(os.environ, {}, clear=False),
            ):
                os.environ.pop("MTG_ENGINE_DB_PATH", None)
                with self.assertRaises(RuntimeError) as err:
                    cards_db.resolve_db_path()

        message = str(err.exception)
        self.assertIn("No valid MTG engine SQLite database found", message)
        self.assertIn(str(missing_external), message)
        self.assertIn(str(missing_path), message)
        self.assertIn("MTG_ENGINE_DB_PATH", message)


if __name__ == "__main__":
    unittest.main()
