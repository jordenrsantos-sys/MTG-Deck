"""Mega-task v5 Phase 2: /snapshots/active endpoint tests.

Validates the auto-default snapshot id endpoint the UI fetches on mount.
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient
    from api.main import app

    _IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - environment-dependent
    TestClient = None
    app = None
    _IMPORT_ERROR = exc


class SnapshotsActiveEndpointTests(unittest.TestCase):
    def setUp(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")

    def test_returns_latest_snapshot_id_when_available(self) -> None:
        with patch("api.main._latest_snapshot_id", return_value="20260217_190902_tagpass_20260222"):
            with TestClient(app) as client:
                response = client.get("/snapshots/active")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body, {"snapshot_id": "20260217_190902_tagpass_20260222"})

    def test_returns_empty_string_when_no_snapshots(self) -> None:
        with patch("api.main._latest_snapshot_id", return_value=""):
            with TestClient(app) as client:
                response = client.get("/snapshots/active")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), {"snapshot_id": ""})


if __name__ == "__main__":
    unittest.main()
