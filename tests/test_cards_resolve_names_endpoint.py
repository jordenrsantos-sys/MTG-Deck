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


class CardsResolveNamesEndpointTests(unittest.TestCase):
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

    def test_resolves_exact_case_insensitive_trim_and_preserves_input_order(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post(
                "/cards/resolve_names",
                json={
                    "snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                    "names": ["  arCanE signet  ", "SOL RING", " krenko, mob boss "],
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "OK")
        self.assertEqual(body.get("snapshot_id"), DECKLIST_FIXTURE_SNAPSHOT_ID)

        rows = body.get("results")
        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 3)
        self.assertEqual(
            [row.get("input") for row in rows],
            ["arCanE signet", "SOL RING", "krenko, mob boss"],
        )
        self.assertEqual(
            [row.get("name") for row in rows],
            ["Arcane Signet", "Sol Ring", "Krenko, Mob Boss"],
        )
        self.assertEqual(body.get("missing"), [])

    def test_duplicate_inputs_repeat_rows_in_input_order(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post(
                "/cards/resolve_names",
                json={
                    "snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                    "names": ["Sol Ring", "sol ring", " Sol Ring "],
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        rows = body.get("results")
        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 3)

        self.assertEqual(
            [row.get("input") for row in rows],
            ["Sol Ring", "sol ring", "Sol Ring"],
        )
        self.assertEqual([row.get("name") for row in rows], ["Sol Ring", "Sol Ring", "Sol Ring"])
        self.assertEqual(
            [row.get("oracle_id") for row in rows],
            ["ORA_RING_001", "ORA_RING_001", "ORA_RING_001"],
        )
        self.assertEqual(body.get("missing"), [])

    def test_missing_names_are_returned_in_missing_array(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post(
                "/cards/resolve_names",
                json={
                    "snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                    "names": ["Sol Ring", "Missing Card", "Arcane Signet", "Ghost Card"],
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        rows = body.get("results")
        self.assertIsInstance(rows, list)
        self.assertEqual(
            [row.get("name") for row in rows],
            ["Sol Ring", "Arcane Signet"],
        )
        self.assertEqual(body.get("missing"), ["Missing Card", "Ghost Card"])

    def test_rejects_more_than_200_names(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        names = [f"Card {index}" for index in range(201)]
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post(
                "/cards/resolve_names",
                json={
                    "snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                    "names": names,
                },
            )

        self.assertEqual(response.status_code, 400)
        body = response.json()
        self.assertIn("at most 200", str(body.get("detail", "")))

    def test_tie_break_prefers_stable_name_then_oracle_order(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        with TestClient(app, raise_server_exceptions=False) as client:
            first = client.post(
                "/cards/resolve_names",
                json={
                    "snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                    "names": ["twin name"],
                },
            )
            second = client.post(
                "/cards/resolve_names",
                json={
                    "snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                    "names": ["twin name"],
                },
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        first_body = first.json()
        second_body = second.json()
        self.assertEqual(first_body, second_body)

        rows = first_body.get("results")
        self.assertIsInstance(rows, list)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("name"), "Twin Name")
        self.assertEqual(rows[0].get("oracle_id"), "ORA_AMB_001")
        self.assertEqual(first_body.get("missing"), [])


if __name__ == "__main__":
    unittest.main()
