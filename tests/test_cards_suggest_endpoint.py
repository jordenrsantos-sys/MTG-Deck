from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.decklist_fixture_harness import (
    DECKLIST_FIXTURE_SNAPSHOT_ID,
    create_decklist_fixture_db,
    set_decklist_fixture_env,
)

try:
    from fastapi.testclient import TestClient
    from api.main import app

    _IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - environment-dependent dependency loading
    TestClient = None
    app = None
    _IMPORT_ERROR = exc


class CardsSuggestEndpointTests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if _IMPORT_ERROR is not None:
            return

        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = create_decklist_fixture_db(Path(cls._tmp_dir_ctx.name))
        cls._db_env_ctx = set_decklist_fixture_env(db_path)
        cls._db_env_ctx.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls._db_env_ctx is not None:
                cls._db_env_ctx.__exit__(None, None, None)
                cls._db_env_ctx = None
        finally:
            if cls._tmp_dir_ctx is not None:
                cls._tmp_dir_ctx.cleanup()
                cls._tmp_dir_ctx = None
            super().tearDownClass()

    def test_suggest_short_query_returns_empty_results(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get(
                "/cards/suggest",
                params={
                    "q": "s",
                    "snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                    "limit": 7,
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("query"), "s")
        self.assertEqual(body.get("snapshot_id"), DECKLIST_FIXTURE_SNAPSHOT_ID)
        self.assertEqual(body.get("limit"), 7)
        self.assertEqual(body.get("results"), [])

    def test_suggest_with_snapshot_returns_expected_fields(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get(
                "/cards/suggest",
                params={
                    "q": "ring",
                    "snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                    "limit": 20,
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("query"), "ring")
        self.assertEqual(body.get("snapshot_id"), DECKLIST_FIXTURE_SNAPSHOT_ID)
        self.assertEqual(body.get("limit"), 20)

        rows = body.get("results")
        self.assertIsInstance(rows, list)
        self.assertGreaterEqual(len(rows), 1)

        names: list[str] = []
        for row in rows:
            self.assertIsInstance(row, dict)
            self.assertIn("oracle_id", row)
            self.assertIn("name", row)
            self.assertIn("mana_cost", row)
            self.assertIn("type_line", row)
            self.assertIn("image_uri", row)

            if isinstance(row.get("name"), str):
                names.append(row["name"])

        self.assertIn("Sol Ring", names)

    def test_suggest_without_snapshot_uses_latest_snapshot_id(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get(
                "/cards/suggest",
                params={
                    "q": "sol",
                    "limit": 999,
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()

        # Endpoint clamps limit to max 20.
        self.assertEqual(body.get("limit"), 20)
        self.assertEqual(body.get("snapshot_id"), DECKLIST_FIXTURE_SNAPSHOT_ID)

        rows = body.get("results")
        self.assertIsInstance(rows, list)
        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0].get("name"), "Sol Ring")


if __name__ == "__main__":
    unittest.main()
