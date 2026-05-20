"""Phase A smoke tests for /agent/build_deck_v1.

The endpoint stub returns commander + 99 Wastes regardless of inputs (beyond
basic validation). These tests pin the request/response contract so Phases B-D
can swap in the real selection algorithm without breaking the wire shape.
"""
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
except Exception as exc:  # pragma: no cover - environment-dependent
    TestClient = None
    app = None
    _IMPORT_ERROR = exc


COMMANDER = "Edgar Markov"


class AgentBuildDeckV1PhaseATests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory | None = None
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

    def _post(self, **overrides):
        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "commander": COMMANDER,
            "bracket": "B3",
            "theme_hints": [],
            "must_include_cards": [],
        }
        payload.update(overrides)
        with TestClient(app, raise_server_exceptions=False) as client:
            return client.post("/agent/build_deck_v1", json=payload)

    def test_smoke_returns_100_cards_with_commander_and_99_basics(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        response = self._post()
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()

        self.assertEqual(body["version"], "agent_build_deck_v1.0")
        self.assertEqual(body["status"], "OK")
        self.assertEqual(len(body["deck"]), 100, "must be commander + 99")
        self.assertEqual(body["deck"][0]["card_name"], COMMANDER)
        self.assertEqual(body["deck"][0]["source"], "user_intent")
        # Phase A stub fills with Wastes.
        basic_count = sum(1 for c in body["deck"][1:] if c["card_name"] == "Wastes")
        self.assertEqual(basic_count, 99)
        # Every card has a non-empty reason (rule 1.3 audit).
        for card in body["deck"]:
            self.assertTrue(isinstance(card.get("reason"), str) and card["reason"].strip())

    def test_summary_reports_bracket_and_must_include_total(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        must_include = ["Vito, Thorn of the Dusk Rose", "Bloodthirsty Conqueror"]
        response = self._post(must_include_cards=must_include, bracket="B3")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        summary = body["summary"]
        self.assertEqual(summary["bracket_placement"], "B3")
        metrics = summary["creativity_envelope_metrics"]
        self.assertEqual(metrics["user_picks_total"], 2)
        # Phase A stub doesn't yet include the picks; metric reports the
        # ratio honestly (0/2). Phases C-D will lift user_picks_present to 2.
        self.assertEqual(metrics["user_picks_present"], 0)

    def test_invalid_bracket_returns_failed_with_warning(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        response = self._post(bracket="B9")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["status"], "FAILED")
        self.assertEqual(body["deck"], [])
        codes = {w["code"] for w in body["warnings"]}
        self.assertIn("INVALID_BRACKET", codes)

    def test_missing_commander_returns_failed(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        response = self._post(commander="   ")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["status"], "FAILED")
        codes = {w["code"] for w in body["warnings"]}
        self.assertIn("MISSING_COMMANDER", codes)

    def test_warnings_contain_phase_a_stub_marker(self) -> None:
        """Until Phases B-D land, every successful build should announce
        itself as a stub so downstream consumers don't mistake it for the
        real selection algorithm."""
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        response = self._post()
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        codes = {w["code"] for w in body["warnings"]}
        self.assertIn("PHASE_A_STUB", codes)


if __name__ == "__main__":
    unittest.main()
