"""Pytest for engine/db.MtgDbUnavailable typed exception — Phase 6 Stage 6 Stage 3.

Verifies the new typed exception:
- Class exists + carries reason_code + path attributes.
- Raised by `connect()` when MTG_ENGINE_DB_PATH is set to a non-existent file
  AND no fallback DB is reachable.
- Subclasses Exception (catchable by API endpoints).
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


class MtgDbUnavailableTests(unittest.TestCase):
    def test_exception_class_exists_and_is_exception_subclass(self) -> None:
        from engine.db import MtgDbUnavailable
        self.assertTrue(issubclass(MtgDbUnavailable, Exception))

    def test_exception_carries_reason_code_attribute(self) -> None:
        from engine.db import MtgDbUnavailable
        exc = MtgDbUnavailable(reason_code="mtg_db_path_unresolvable", message="not found", path="/nope")
        self.assertEqual(exc.reason_code, "mtg_db_path_unresolvable")
        self.assertEqual(exc.path, "/nope")
        self.assertIn("not found", str(exc))

    def test_default_message_falls_through_to_reason_code(self) -> None:
        from engine.db import MtgDbUnavailable
        exc = MtgDbUnavailable(reason_code="cards_table_missing")
        self.assertEqual(str(exc), "cards_table_missing")

    def test_connect_raises_mtg_db_unavailable_when_env_path_invalid_and_no_fallback(self) -> None:
        """Mock resolve_db_path to raise RuntimeError; verify connect() converts it."""
        from engine import db as engine_db
        from engine.db import MtgDbUnavailable

        with mock.patch.object(engine_db, "resolve_db_path", side_effect=RuntimeError("no DB found")):
            with self.assertRaises(MtgDbUnavailable) as ctx:
                engine_db.connect()
            self.assertEqual(ctx.exception.reason_code, "mtg_db_path_unresolvable")
            self.assertIn("no DB found", str(ctx.exception))

    def test_connect_succeeds_when_db_path_resolves_normally(self) -> None:
        """Sanity: the new exception path doesn't break the OK case."""
        from engine import db as engine_db
        # If a real DB is present, connect() should return a sqlite3.Connection.
        # If not (sandbox), the test falls through gracefully — only fails when
        # the OK path is broken AND a real DB exists.
        try:
            con = engine_db.connect()
            con.close()
        except engine_db.MtgDbUnavailable:
            # Sandbox without DB — skip rather than fail (this test guards against
            # regression in the OK path; the sandbox case is exercised above).
            self.skipTest("No mtg.sqlite available in this environment")


if __name__ == "__main__":
    unittest.main()
