from __future__ import annotations

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


class DeckCompleteEndpointV1Tests(unittest.TestCase):
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

    def test_complete_unknowns_block_build_and_complete_engine(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": "Unknown Card Name",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "target_deck_size": 100,
            "max_adds": 30,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
        }

        with (
            patch("api.main.run_build_pipeline") as mocked_run_build,
            patch("api.main.run_deck_complete_engine_v1") as mocked_run_complete,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "UNKNOWN_PRESENT")
        self.assertEqual(body.get("added_cards_v1"), [])
        self.assertIsInstance(body.get("unknowns"), list)
        mocked_run_build.assert_not_called()
        mocked_run_complete.assert_not_called()

    def test_complete_happy_path_invokes_build_then_complete_engine(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": """
Commander
1 Krenko, Mob Boss
Deck
1 Sol Ring
1 Arcane Signet
""",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "target_deck_size": 100,
            "max_adds": 30,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
        }

        mocked_build_payload = {
            "status": "OK",
            "deck_size_total": 3,
            "result": {},
        }
        mocked_complete_payload = {
            "version": "deck_complete_engine_v1",
            "status": "OK",
            "baseline_summary_v1": {
                "build_status": "OK",
                "deck_size_total": 3,
            },
            "added_cards_v1": [
                {
                    "name": "Plains",
                    "reasons_v1": ["ADD_BASIC_LAND_FILL_AUTO", "COMPLETE_TO_TARGET_SIZE"],
                    "primitives_added_v1": [],
                }
            ],
            "completed_decklist_text_v1": "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet\n1 Plains",
        }

        with (
            patch("api.main.run_build_pipeline", return_value=mocked_build_payload) as mocked_run_build,
            patch("api.main.run_deck_complete_engine_v1", return_value=mocked_complete_payload) as mocked_run_complete,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "OK")
        self.assertEqual(body.get("complete_engine_version"), "deck_complete_engine_v1")
        self.assertIsInstance(body.get("added_cards_v1"), list)
        self.assertEqual(body.get("added_cards_v1")[0].get("name"), "Plains")
        self.assertIn("Commander", body.get("completed_decklist_text_v1", ""))

        mocked_run_build.assert_called_once()
        mocked_run_complete.assert_called_once()


if __name__ == "__main__":
    unittest.main()
