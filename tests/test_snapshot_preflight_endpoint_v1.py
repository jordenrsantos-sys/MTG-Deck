from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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


class SnapshotPreflightEndpointV1Tests(unittest.TestCase):
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

    def test_snapshot_preflight_endpoint_returns_runner_payload_exactly(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        expected_payload = {
            "version": "snapshot_preflight_v1",
            "snapshot_id": "SNAP_123",
            "status": "ERROR",
            "errors": [
                {
                    "code": "SNAPSHOT_NOT_FOUND",
                    "message": "Snapshot ID not found in local DB.",
                }
            ],
            "checks": {
                "snapshot_exists": False,
                "manifest_present": False,
                "tags_compiled": None,
                "schema_ok": True,
                "card_images_schema_ok": True,
            },
        }

        with (
            patch("api.main.run_snapshot_preflight_v1", return_value=expected_payload) as mocked_run_preflight,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.get("/snapshot/preflight/SNAP_123")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected_payload)
        mocked_run_preflight.assert_called_once()
        call_kwargs = mocked_run_preflight.call_args.kwargs
        self.assertEqual(call_kwargs.get("snapshot_id"), "SNAP_123")
        self.assertIsInstance(call_kwargs.get("db"), sqlite3.Connection)

    def test_snapshot_preflight_endpoint_with_fixture_snapshot_returns_expected_shape(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get(f"/snapshot/preflight/{DECKLIST_FIXTURE_SNAPSHOT_ID}")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("version"), "snapshot_preflight_v1")
        self.assertEqual(body.get("snapshot_id"), DECKLIST_FIXTURE_SNAPSHOT_ID)
        self.assertIn(body.get("status"), ["OK", "ERROR"])
        self.assertIsInstance(body.get("errors"), list)
        error_codes = [
            row.get("code")
            for row in body.get("errors")
            if isinstance(row, dict) and isinstance(row.get("code"), str)
        ]
        self.assertNotIn("SNAPSHOT_NOT_FOUND", error_codes)

        checks = body.get("checks") if isinstance(body.get("checks"), dict) else {}
        self.assertIn("snapshot_exists", checks)
        self.assertIn("manifest_present", checks)
        self.assertIn("tags_compiled", checks)
        self.assertIn("schema_ok", checks)
        self.assertIn("card_images_schema_ok", checks)


if __name__ == "__main__":
    unittest.main()
