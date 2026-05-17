"""v1.2 Stage 1 regression — `added_cards_v1` population fix.

Before v1.2, two parallel guards (engine layer `deck_complete_engine_v1.py`
+ wrapper `api/main.py`) rejected the `OK_WITH_UNKNOWNS` baseline status
with BASELINE_BUILD_UNAVAILABLE, returning empty added_cards_v1 even though
the engine had already completed the deck. This module covers:

  1. `OK_WITH_UNKNOWNS` is now accepted by the wrapper (no early return).
  2. When the wrapper proceeds, the engine's added_cards_v1 array
     length equals the deck's growth (target_deck_size - input cards).
  3. Each added_cards_v1 entry preserves the {name, reasons_v1,
     primitives_added_v1} shape — no field drift.

Per canonical Shelob 1010839 fixture: 78-card import → 100-card completion
gives 22 additions with non-empty reasons_v1. The tests below mirror that
end-to-end behavior via the FastAPI TestClient.
"""
from __future__ import annotations

import os
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
except Exception as exc:  # pragma: no cover
    TestClient = None
    app = None
    _IMPORT_ERROR = exc


class DeckCompleteAddedCardsV1PopulationTests(unittest.TestCase):
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

    def test_ok_with_unknowns_baseline_no_longer_rejected_by_wrapper_guard(self) -> None:
        """The wrapper's gate at api/main.py must accept OK_WITH_UNKNOWNS
        (in addition to OK and WARN). Pre-fix the wrapper rejected with
        BASELINE_BUILD_UNAVAILABLE and returned empty added_cards_v1.

        Mocks the engine to confirm wrapper passes through rather than
        short-circuiting — i.e. exercises only the wrapper-side guard fix.
        """
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "target_deck_size": 100,
            "max_adds": 200,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
            "commander": "Krenko, Mob Boss",
        }

        mocked_build_payload = {
            "status": "OK_WITH_UNKNOWNS",
            "deck_size_total": 3,
            "result": {},
        }
        mocked_complete_payload = {
            "version": "deck_complete_engine_v1",
            "status": "OK",
            "baseline_summary_v1": {
                "build_status": "OK_WITH_UNKNOWNS",
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
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "0"}, clear=False),
            patch("api.main.run_build_pipeline", return_value=mocked_build_payload) as mocked_run_build,
            patch("api.main.run_deck_complete_engine_v1", return_value=mocked_complete_payload) as mocked_run_complete,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        # Pre-fix: status="ERROR" codes=["BASELINE_BUILD_UNAVAILABLE"]
        # Post-fix: wrapper proceeds, engine returns "OK"
        self.assertEqual(body.get("status"), "OK")
        self.assertNotIn("BASELINE_BUILD_UNAVAILABLE", body.get("codes_v1", []))
        self.assertEqual(body.get("baseline_build_status_v1"), "OK_WITH_UNKNOWNS")
        self.assertEqual(len(body.get("added_cards_v1") or []), 1)
        mocked_run_build.assert_called_once()
        mocked_run_complete.assert_called_once()

    def test_added_cards_v1_length_matches_deck_growth_when_engine_populates(self) -> None:
        """Wrapper's pass-through preserves added_cards_v1 length-equals-growth
        invariant. Pre-fix: empty added_cards_v1 even when completed deck grew.
        Post-fix: each engine addition surfaces with matching name/reasons.
        """
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "target_deck_size": 100,
            "max_adds": 200,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
            "commander": "Krenko, Mob Boss",
        }

        # Simulate the engine adding 22 cards (the Shelob 1010839 canonical
        # growth from 78 input → 100 completion).
        added_cards = [
            {
                "name": f"BasicFill{i:02d}",
                "reasons_v1": ["ADD_BASIC_LAND_FILL_AUTO", "COMPLETE_TO_TARGET_SIZE"],
                "primitives_added_v1": [],
            }
            for i in range(22)
        ]
        completed_lines = [
            "Commander", "1 Krenko, Mob Boss", "Deck",
            "1 Sol Ring", "1 Arcane Signet",
            *(f"1 BasicFill{i:02d}" for i in range(22)),
        ]
        mocked_build_payload = {
            "status": "OK",
            "deck_size_total": 3,
            "result": {},
        }
        mocked_complete_payload = {
            "version": "deck_complete_engine_v1",
            "status": "OK",
            "baseline_summary_v1": {"build_status": "OK", "deck_size_total": 3},
            "added_cards_v1": added_cards,
            "completed_decklist_text_v1": "\n".join(completed_lines),
        }

        with (
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "0"}, clear=False),
            patch("api.main.run_build_pipeline", return_value=mocked_build_payload),
            patch("api.main.run_deck_complete_engine_v1", return_value=mocked_complete_payload),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "OK")
        surfaced = body.get("added_cards_v1") or []
        self.assertEqual(len(surfaced), 22, "added_cards_v1 length must match engine output 1:1")

        # Count completed lines that are not Commander/Deck banners,
        # and check additions == post - pre.
        text = body.get("completed_decklist_text_v1", "")
        card_lines = [
            ln for ln in text.split("\n")
            if ln.strip() and ln.strip() not in ("Commander", "Deck")
        ]
        # input cards: 1 (Krenko) + 2 (Sol Ring, Arcane Signet) = 3 lines
        # completed: 3 + 22 = 25 lines
        # growth: 22 == len(added_cards_v1) ✓
        self.assertEqual(len(card_lines) - 3, len(surfaced))

    def test_added_cards_v1_entry_shape_byte_identical_with_pydantic_model(self) -> None:
        """Each `added_cards_v1` entry must shape exactly as
        DeckCompleteAddedCardV1: {name, reasons_v1, primitives_added_v1}.
        No extra keys (Pydantic ConfigDict(extra="forbid")).
        """
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "target_deck_size": 100,
            "max_adds": 200,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
            "commander": "Krenko, Mob Boss",
        }
        mocked_complete_payload = {
            "version": "deck_complete_engine_v1",
            "status": "OK",
            "baseline_summary_v1": {"build_status": "OK", "deck_size_total": 2},
            "added_cards_v1": [
                {
                    "name": "Sol Ring",
                    "reasons_v1": ["ADD_PRIMITIVE_COVERAGE"],
                    "primitives_added_v1": ["RAMP_MANA"],
                },
                {
                    "name": "Mountain",
                    "reasons_v1": ["ADD_BASIC_LAND_FILL_AUTO", "COMPLETE_TO_TARGET_SIZE"],
                    "primitives_added_v1": [],
                },
            ],
            "completed_decklist_text_v1": "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Mountain",
        }

        with (
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "0"}, clear=False),
            patch("api.main.run_build_pipeline", return_value={"status": "OK", "deck_size_total": 2, "result": {}}),
            patch("api.main.run_deck_complete_engine_v1", return_value=mocked_complete_payload),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        surfaced = body.get("added_cards_v1") or []
        self.assertEqual(len(surfaced), 2)
        for entry in surfaced:
            # Exact key set — Pydantic extra="forbid" enforces this server-side.
            self.assertEqual(set(entry.keys()), {"name", "reasons_v1", "primitives_added_v1"})
            self.assertIsInstance(entry["name"], str)
            self.assertIsInstance(entry["reasons_v1"], list)
            self.assertIsInstance(entry["primitives_added_v1"], list)
            self.assertTrue(len(entry["reasons_v1"]) > 0, "reasons_v1 must be non-empty when engine added the card")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
