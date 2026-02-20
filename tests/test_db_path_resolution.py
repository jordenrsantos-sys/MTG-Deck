from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import engine.db as cards_db


class DbPathResolutionTests(unittest.TestCase):
    def test_env_override_works(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "override.sqlite"
            db_path.write_bytes(b"")

            with patch.dict(os.environ, {"MTG_ENGINE_DB_PATH": str(db_path)}, clear=False):
                resolved = cards_db.resolve_db_path()

        self.assertEqual(resolved, db_path.resolve())

    def test_project_relative_path_works(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            db_path = (repo_root / "data" / "mtg.sqlite").resolve()
            db_path.parent.mkdir(parents=True, exist_ok=True)
            db_path.write_bytes(b"")

            with (
                patch.object(cards_db, "REPO_ROOT", repo_root),
                patch.object(cards_db, "DB_PATH", db_path),
                patch.dict(os.environ, {}, clear=False),
            ):
                os.environ.pop("MTG_ENGINE_DB_PATH", None)
                resolved = cards_db.resolve_db_path()

        self.assertEqual(resolved, db_path)

    def test_missing_db_raises_runtime_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            missing_path = (repo_root / "data" / "mtg.sqlite").resolve()

            with (
                patch.object(cards_db, "REPO_ROOT", repo_root),
                patch.object(cards_db, "DB_PATH", missing_path),
                patch.dict(os.environ, {}, clear=False),
            ):
                os.environ.pop("MTG_ENGINE_DB_PATH", None)
                with self.assertRaises(RuntimeError) as err:
                    cards_db.resolve_db_path()

        message = str(err.exception)
        self.assertIn(str(missing_path), message)
        self.assertIn("MTG_ENGINE_DB_PATH", message)
        self.assertIn("./data/mtg.sqlite", message)


if __name__ == "__main__":
    unittest.main()
