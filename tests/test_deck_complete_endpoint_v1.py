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
            "max_adds": 200,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
        }

        with (
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "0"}, clear=False),
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

    def test_complete_rejects_empty_raw_decklist_text(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": "   ",
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

        with (
            patch("api.main.run_build_pipeline") as mocked_run_build,
            patch("api.main.run_deck_complete_engine_v1") as mocked_run_complete,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 422)
        body = response.json() if isinstance(response.json(), dict) else {}
        self.assertEqual(body.get("detail"), "raw_decklist_text missing.")
        mocked_run_build.assert_not_called()
        mocked_run_complete.assert_not_called()

    def test_complete_rejects_wrapped_endpoint_payload_with_hint(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": " ",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "target_deck_size": 100,
            "max_adds": 200,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
            "commander": "Krenko, Mob Boss",
            "endpoint_payload": {
                "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                "raw_decklist_text": "1 Sol Ring",
                "format": "commander",
                "profile_id": "focused",
                "bracket_id": "B2",
                "mulligan_model_id": "NORMAL",
                "target_deck_size": 100,
                "max_adds": 200,
                "allow_basic_lands": True,
                "land_target_mode": "AUTO",
                "commander": "Krenko, Mob Boss",
            },
        }

        with (
            patch("api.main.run_build_pipeline") as mocked_run_build,
            patch("api.main.run_deck_complete_engine_v1") as mocked_run_complete,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 422)
        body = response.json() if isinstance(response.json(), dict) else {}
        self.assertEqual(
            body.get("detail"),
            "raw_decklist_text missing. Did you wrap the payload in endpoint_payload?",
        )
        mocked_run_build.assert_not_called()
        mocked_run_complete.assert_not_called()

    def test_complete_rejects_empty_ingest_with_structured_detail(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": "Commander\nDeck\n",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "target_deck_size": 100,
            "max_adds": 200,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
        }

        with (
            patch("api.main.run_build_pipeline") as mocked_run_build,
            patch("api.main.run_deck_complete_engine_v1") as mocked_run_complete,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 422)
        body = response.json() if isinstance(response.json(), dict) else {}
        detail = body.get("detail") if isinstance(body.get("detail"), dict) else {}
        self.assertEqual(detail.get("code"), "EMPTY_INGEST")
        self.assertEqual(detail.get("message"), "No deck lines parsed from raw_decklist_text")
        self.assertEqual(detail.get("raw_first120"), "Commander\nDeck\n")

        parse_totals = detail.get("parse_totals") if isinstance(detail.get("parse_totals"), dict) else {}
        self.assertEqual(parse_totals.get("items_total"), 0)
        self.assertEqual(parse_totals.get("card_count_total"), 0)

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
            "max_adds": 200,
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
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "0"}, clear=False),
            patch("api.main.run_build_pipeline", return_value=mocked_build_payload) as mocked_run_build,
            patch("api.main.run_deck_complete_engine_v1", return_value=mocked_complete_payload) as mocked_run_complete,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "OK")
        self.assertEqual(body.get("codes_v1"), [])
        self.assertEqual(body.get("baseline_build_status_v1"), "OK")
        self.assertEqual(body.get("baseline_unknowns_v1"), [])
        self.assertIsNone(body.get("snapshot_preflight_v1"))
        self.assertEqual(body.get("complete_engine_version"), "deck_complete_engine_v1")
        self.assertIsInstance(body.get("added_cards_v1"), list)
        self.assertEqual(body.get("added_cards_v1")[0].get("name"), "Plains")
        self.assertIn("Commander", body.get("completed_decklist_text_v1", ""))

        mocked_run_build.assert_called_once()
        mocked_run_complete.assert_called_once()

    def test_complete_dev_metrics_include_stop_reason_when_flag_enabled(self) -> None:
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
            "max_adds": 200,
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
                    "name": "Mountain",
                    "reasons_v1": ["ADD_BASIC_LAND_FILL_AUTO", "COMPLETE_TO_TARGET_SIZE"],
                    "primitives_added_v1": [],
                }
            ],
            "completed_decklist_text_v1": "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet\n1 Mountain",
            "dev_metrics_v1": {
                "stop_reason_v1": "LAND_FILL_APPLIED",
                "nonland_added_count": 0,
                "land_fill_needed": 97,
                "land_fill_applied": 97,
                "candidate_pool_last_returned": 0,
            },
        }

        with (
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "1"}, clear=False),
            patch("api.main.run_build_pipeline", return_value=mocked_build_payload),
            patch("api.main.run_deck_complete_engine_v1", return_value=mocked_complete_payload),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIsInstance(body.get("dev_metrics_v1"), dict)
        metrics = body.get("dev_metrics_v1")
        self.assertEqual(metrics.get("stop_reason_v1"), "LAND_FILL_APPLIED")
        self.assertEqual(metrics.get("land_fill_applied"), 97)
        self.assertEqual(metrics.get("land_fill_needed"), 97)
        self.assertIn("baseline_build_ms", metrics)
        self.assertIn("total_ms", metrics)

    def test_complete_response_carries_game_changers_v1(self) -> None:
        """The response surfaces a flat sorted list of card names from the
        submitted deck that match the engine's Game Changers userlist. The
        UI uses this to badge AddedCardRow / deck overview rows.

        The fixture DB does not contain any cards that are also on the
        production GC userlist, so we patch ``api.main.GAME_CHANGERS_SET``
        to include the fixture-deck cards. The detection logic itself is
        independent of which set is configured."""
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
            "max_adds": 200,
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
            "baseline_summary_v1": {"build_status": "OK", "deck_size_total": 3},
            "added_cards_v1": [],
            "completed_decklist_text_v1": (
                "Commander\n1 Krenko, Mob Boss\nDeck\n"
                "1 Sol Ring\n1 Arcane Signet"
            ),
        }

        # Patch the module-level GAME_CHANGERS_SET so the test is not
        # coupled to the production userlist contents.
        with (
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "0"}, clear=False),
            patch("api.main.GAME_CHANGERS_SET", {"Sol Ring", "Krenko, Mob Boss"}),
            patch("api.main.run_build_pipeline", return_value=mocked_build_payload),
            patch("api.main.run_deck_complete_engine_v1", return_value=mocked_complete_payload),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("game_changers_v1", body)
        gc = body.get("game_changers_v1")
        self.assertIsInstance(gc, list)
        # Sol Ring (deck) + Krenko, Mob Boss (commander) both surface in
        # the response since both are in the patched GC set; Arcane Signet
        # is not in the set so it must be absent.
        self.assertIn("Sol Ring", gc)
        self.assertIn("Krenko, Mob Boss", gc)
        self.assertNotIn("Arcane Signet", gc)
        # Sorted output preserves the engine's deterministic ordering.
        self.assertEqual(gc, sorted(gc))

    def test_complete_response_game_changers_v1_empty_for_no_match_deck(self) -> None:
        """When the deck has no GC overlap, the field is an empty list."""
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": """
Commander
1 Krenko, Mob Boss
Deck
1 Arcane Signet
""",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "target_deck_size": 100,
            "max_adds": 200,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
        }

        mocked_build_payload = {"status": "OK", "deck_size_total": 2, "result": {}}
        mocked_complete_payload = {
            "version": "deck_complete_engine_v1",
            "status": "OK",
            "baseline_summary_v1": {"build_status": "OK", "deck_size_total": 2},
            "added_cards_v1": [],
            "completed_decklist_text_v1": (
                "Commander\n1 Krenko, Mob Boss\nDeck\n1 Arcane Signet"
            ),
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
        self.assertEqual(body.get("game_changers_v1"), [])

    def test_complete_warn_codes_promote_to_violations(self) -> None:
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
            "max_adds": 200,
            "allow_basic_lands": False,
            "land_target_mode": "AUTO",
        }

        mocked_build_payload = {
            "status": "OK",
            "deck_size_total": 3,
            "result": {},
        }
        mocked_complete_payload = {
            "version": "deck_complete_engine_v1",
            "status": "WARN",
            "codes": ["BASIC_LANDS_DISALLOWED", "CANDIDATE_POOL_EMPTY", "TARGET_SIZE_NOT_REACHED"],
            "baseline_summary_v1": {
                "build_status": "OK",
                "deck_size_total": 3,
            },
            "added_cards_v1": [],
            "completed_decklist_text_v1": "Commander\n1 Krenko, Mob Boss\nDeck\n1 Sol Ring\n1 Arcane Signet",
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
        self.assertEqual(body.get("status"), "WARN")
        self.assertEqual(
            body.get("codes_v1"),
            ["BASIC_LANDS_DISALLOWED", "CANDIDATE_POOL_EMPTY", "TARGET_SIZE_NOT_REACHED"],
        )
        violations = body.get("violations_v1") if isinstance(body.get("violations_v1"), list) else []
        violation_codes = sorted(
            {
                row.get("code")
                for row in violations
                if isinstance(row, dict) and isinstance(row.get("code"), str)
            }
        )
        self.assertIn("BASIC_LANDS_DISALLOWED", violation_codes)
        self.assertIn("CANDIDATE_POOL_EMPTY", violation_codes)
        self.assertIn("TARGET_SIZE_NOT_REACHED", violation_codes)

    def test_complete_returns_error_when_baseline_build_unavailable(self) -> None:
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
            "max_adds": 200,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
        }

        mocked_build_payload = {
            "status": "SKIP",
            "unknowns": [
                {
                    "code": "SNAPSHOT_PREFLIGHT_FAILED",
                    "message": "manifest missing",
                }
            ],
            "result": {
                "snapshot_preflight_v1": {
                    "tags_compiled": False,
                    "manifest_present": False,
                    "card_images_schema_valid": False,
                }
            },
        }

        with (
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "0"}, clear=False),
            patch("api.main.run_build_pipeline", return_value=mocked_build_payload) as mocked_run_build,
            patch("api.main.run_deck_complete_engine_v1") as mocked_run_complete,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/complete_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "ERROR")
        self.assertEqual(body.get("codes_v1"), ["BASELINE_BUILD_UNAVAILABLE"])
        self.assertEqual(body.get("baseline_build_status_v1"), "SKIP")
        self.assertEqual(body.get("baseline_unknowns_v1"), mocked_build_payload["unknowns"])
        self.assertEqual(body.get("snapshot_preflight_v1"), mocked_build_payload["result"]["snapshot_preflight_v1"])
        mocked_run_build.assert_called_once()
        mocked_run_complete.assert_not_called()


class DeckCompleteOkPathAddedCardsV1BackfillTests(unittest.TestCase):
    """v1.5 Stage 3 — OK baseline_status path: added_cards_v1 length must
    match deck growth even when the engine's accumulator under-counts.

    The defensive backfill in `_backfill_added_cards_from_diff` ensures
    that any code path which grows working_cards beyond deck_cards
    synthesizes missing added_cards_v1 entries (via multiset diff) so
    the length-equals-growth invariant always holds.

    Tests exercise the engine at the layer below the FastAPI wrapper:
    they call `run_deck_complete_engine_v1` directly with constructed
    canonical_deck_input + baseline_build_result payloads, asserting
    the response shape matches the invariant.
    """

    def setUp(self) -> None:
        super().setUp()
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")
        from api.engine.deck_complete_engine_v1 import run_deck_complete_engine_v1

        self._run_engine = run_deck_complete_engine_v1

    def _ok_baseline(self) -> dict:
        return {
            "status": "OK",
            "deck_size_total": 0,
            "result": {
                "structural_snapshot_v1": {
                    "dead_slot_ids_v1": [],
                    "missing_primitives_v1": [],
                },
            },
        }

    def test_ok_baseline_path_growth_equals_added_cards_length(self) -> None:
        # Input: 3-card deck under target. Engine adds basic lands to reach 100.
        canonical = {
            "commander": "Krenko, Mob Boss",
            "commander_list_v1": ["Krenko, Mob Boss"],
            "cards": ["Sol Ring", "Arcane Signet", "Mountain"],
        }
        response = self._run_engine(
            canonical_deck_input=canonical,
            baseline_build_result=self._ok_baseline(),
            db_snapshot_id="test",
            bracket_id="B2",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            target_deck_size=100,
            max_adds=200,
            allow_basic_lands=True,
            land_target_mode="AUTO",
            collect_dev_metrics=False,
        )

        added = response.get("added_cards_v1") or []
        completed_text = response.get("completed_decklist_text_v1") or ""
        completed_card_lines = [
            ln for ln in completed_text.split("\n")
            if ln.strip() and ln.strip() not in ("Commander", "Deck")
        ]
        # input cards = 3 (Sol Ring + Arcane Signet + Mountain) + commander = 4
        # completed = 100; growth = 100 - 4 = 96.
        growth_delta = len(completed_card_lines) - (len(canonical["cards"]) + 1)
        # Length-equals-growth invariant: added_cards_v1.length == growth_delta.
        self.assertEqual(len(added), growth_delta)
        self.assertGreater(len(added), 0, "Engine added 0 cards but deck grew")

    def test_ok_baseline_path_determinism_same_inputs_same_outputs(self) -> None:
        # Determinism: invoking the engine twice with the same inputs returns
        # byte-identical added_cards_v1 + completed_decklist_text_v1.
        canonical = {
            "commander": "Krenko, Mob Boss",
            "commander_list_v1": ["Krenko, Mob Boss"],
            "cards": ["Sol Ring", "Arcane Signet", "Mountain"],
        }
        kwargs = dict(
            canonical_deck_input=canonical,
            baseline_build_result=self._ok_baseline(),
            db_snapshot_id="test",
            bracket_id="B2",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            target_deck_size=100,
            max_adds=200,
            allow_basic_lands=True,
            land_target_mode="AUTO",
            collect_dev_metrics=False,
        )
        run1 = self._run_engine(**kwargs)
        run2 = self._run_engine(**kwargs)
        self.assertEqual(run1.get("added_cards_v1"), run2.get("added_cards_v1"))
        self.assertEqual(
            run1.get("completed_decklist_text_v1"),
            run2.get("completed_decklist_text_v1"),
        )

    def test_ok_baseline_path_reason_codes_from_v12_vocabulary(self) -> None:
        # All added_cards_v1 entries carry non-empty reasons_v1 lists; each
        # reason is from the v1.2 vocabulary (existing engine codes OR the
        # v1.5 backfill placeholder `auto_completion_target_size`).
        canonical = {
            "commander": "Krenko, Mob Boss",
            "commander_list_v1": ["Krenko, Mob Boss"],
            "cards": ["Sol Ring", "Arcane Signet", "Mountain"],
        }
        response = self._run_engine(
            canonical_deck_input=canonical,
            baseline_build_result=self._ok_baseline(),
            db_snapshot_id="test",
            bracket_id="B2",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            target_deck_size=100,
            max_adds=200,
            allow_basic_lands=True,
            land_target_mode="AUTO",
            collect_dev_metrics=False,
        )
        # Known engine reason codes (rounds + land_fill paths) + the v1.5
        # backfill placeholder. Test asserts each entry has non-empty
        # reasons_v1 and that at least one expected vocabulary code appears.
        known_codes = {
            "ADD_BASIC_LAND_FILL_AUTO",
            "COMPLETE_TO_TARGET_SIZE",
            "ADD_REQUIRED_COVERAGE",
            "ADD_REDUNDANCY_SUPPORT",
            "ADD_INTERACTION_OR_PROTECTION",
            "auto_completion_target_size",
            # v1.2-listed vocabulary aliases (spec body):
            "basic_land_fill",
            "land_target_completion",
            "primitive_coverage_fill",
        }
        added = response.get("added_cards_v1") or []
        self.assertGreater(len(added), 0)
        for entry in added:
            reasons = entry.get("reasons_v1") or []
            self.assertGreater(len(reasons), 0, f"entry {entry.get('name')!r} has empty reasons_v1")
            # At least one reason code in the known v1.2 vocabulary.
            overlap = set(reasons) & known_codes
            self.assertGreater(
                len(overlap),
                0,
                f"entry {entry.get('name')!r} reasons_v1 {reasons} have no known v1.2 code",
            )

    def test_backfill_helper_synthesizes_missing_entries(self) -> None:
        # Unit-test the helper directly: passing under-counted added_cards
        # alongside grown working_cards produces synthesized entries with
        # the placeholder reason. Byte-identical pass-through when accumulator
        # is already correct.
        from api.engine.deck_complete_engine_v1 import _backfill_added_cards_from_diff

        # Case 1: accumulator already correct → no backfill.
        deck_cards = ["A", "B"]
        working_cards = ["A", "B", "C", "D"]
        existing = [
            {"name": "C", "reasons_v1": ["ENGINE_REASON_1"], "primitives_added_v1": []},
            {"name": "D", "reasons_v1": ["ENGINE_REASON_2"], "primitives_added_v1": []},
        ]
        out = _backfill_added_cards_from_diff(existing, deck_cards, working_cards)
        self.assertEqual(out, existing)

        # Case 2: accumulator under-counts → backfill synthesizes the missing.
        out2 = _backfill_added_cards_from_diff([], deck_cards, working_cards)
        self.assertEqual(len(out2), 2)
        names = sorted(e["name"] for e in out2)
        self.assertEqual(names, ["C", "D"])
        for entry in out2:
            self.assertEqual(entry["reasons_v1"], ["auto_completion_target_size"])
            self.assertEqual(entry["primitives_added_v1"], [])

        # Case 3: no growth → no synthesis.
        out3 = _backfill_added_cards_from_diff([], deck_cards, deck_cards)
        self.assertEqual(out3, [])

        # Case 4: partial accumulator → fill the gap deterministically.
        out4 = _backfill_added_cards_from_diff(
            [{"name": "C", "reasons_v1": ["ENGINE_REASON_1"], "primitives_added_v1": []}],
            deck_cards,
            working_cards,
        )
        self.assertEqual(len(out4), 2)
        # D was missing → synthesized with placeholder reason.
        d_entry = next(e for e in out4 if e["name"] == "D")
        self.assertEqual(d_entry["reasons_v1"], ["auto_completion_target_size"])


if __name__ == "__main__":
    unittest.main()
