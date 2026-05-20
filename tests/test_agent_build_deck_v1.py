"""Endpoint contract tests for /agent/build_deck_v1.

Validates the request/response contract end-to-end through the FastAPI route,
mocking upstream agent_endpoints_v1 functions so tests don't pay the cost of
loading and vectorizing the 13K-deck corpus against the small fixture DB.

Phase B/C live-layer behavior is covered by:
  - tests/test_agent_build_deck_v1_phase_b.py — `_build_candidate_pool` w/ mocks
  - tests/test_agent_build_deck_v1_phase_c.py — `_select_deck` slot + combo logic

The 5-test-case live validation sweep against a real snapshot lands in Phase F.
"""
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
except Exception as exc:  # pragma: no cover - environment-dependent
    TestClient = None
    app = None
    _IMPORT_ERROR = exc


COMMANDER = "Edgar Markov"


def _archetype_brief_stub() -> dict:
    return {
        "version": "archetype_brief_v1.0",
        "commander": COMMANDER,
        "commander_oracle_id": "edgar-oracle-id",
        "color_identity": ["B", "R", "W"],
        "corpus_deck_count": 25,
        "common_archetypes": [{"name": "Vampire Tribal Aristocrats", "frequency": 0.6, "deck_count": 15}],
        "bracket_distribution": {"B3": 0.5, "B4": 0.3},
        "staple_cards": [
            {"name": "Sol Ring", "usage_pct": 0.92},
            {"name": "Command Tower", "usage_pct": 0.88},
        ],
        "warnings": [],
    }


def _theme_top_cards_stub() -> dict:
    # Enough candidates (~50) to satisfy non-land slot targets.
    results = []
    for i in range(60):
        results.append({
            "oracle_id": f"vampire-{i}",
            "name": f"Mock Vampire {i:02d}",
            "type_line": "Creature — Vampire",
            "cmc": (i % 5) + 1,
            "primitives": ["TRIBAL_PAYOFFS"],
            "theme_signal_count": 1 + (i % 3),
        })
    return {
        "version": "theme_top_cards_v1.0",
        "theme_id": "TYPAL_VAMPIRES",
        "subtype": "Vampire",
        "primitives_used_for_match": ["TRIBAL_PAYOFFS"],
        "matched_count": 60,
        "returned_count": len(results),
        "results": results,
        "warnings": [],
    }


def _find_card_stub(snapshot_id: str, name: str):
    return None  # No user must-includes in these tests; not exercised here.


class AgentBuildDeckV1ContractTests(unittest.TestCase):
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

    def _post(self, *, with_upstream_mocks: bool = True, **overrides):
        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "commander": COMMANDER,
            "bracket": "B3",
            "theme_hints": ["TYPAL_VAMPIRES"],
            "must_include_cards": [],
        }
        payload.update(overrides)

        from api.engine.layers import agent_endpoints_v1 as ae
        from engine import db as engine_db
        if with_upstream_mocks:
            mocks = [
                patch.object(ae, "compute_archetype_brief_v1", return_value=_archetype_brief_stub()),
                patch.object(ae, "compute_theme_top_cards_v1", return_value=_theme_top_cards_stub()),
                patch.object(engine_db, "find_card_by_name", side_effect=_find_card_stub),
            ]
            for m in mocks:
                m.start()
            try:
                with TestClient(app, raise_server_exceptions=False) as client:
                    return client.post("/agent/build_deck_v1", json=payload)
            finally:
                for m in mocks:
                    m.stop()
        else:
            with TestClient(app, raise_server_exceptions=False) as client:
                return client.post("/agent/build_deck_v1", json=payload)

    def test_smoke_returns_100_cards_with_commander_first(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        response = self._post()
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()

        self.assertEqual(body["version"], "agent_build_deck_v1.0")
        self.assertEqual(body["status"], "OK")
        self.assertEqual(len(body["deck"]), 100, f"want 100, got {len(body['deck'])}")
        self.assertEqual(body["deck"][0]["card_name"], COMMANDER)
        self.assertEqual(body["deck"][0]["source"], "user_intent")
        # Every card has a non-empty reason (rule 1.3 audit).
        for card in body["deck"]:
            self.assertTrue(isinstance(card.get("reason"), str) and card["reason"].strip())

    def test_summary_contains_creativity_envelope_metrics(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        response = self._post(must_include_cards=["Vito, Thorn of the Dusk Rose"], bracket="B3")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        summary = body["summary"]
        self.assertEqual(summary["bracket_placement"], "B3")
        metrics = summary["creativity_envelope_metrics"]
        self.assertEqual(metrics["user_picks_total"], 1)
        # find_card returns None for everything in stub → must_includes_dropped
        self.assertIn("Vito, Thorn of the Dusk Rose", metrics.get("must_includes_dropped", []) +
                      metrics.get("must_includes_resolved", []))

    def test_invalid_bracket_returns_failed_with_warning(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        response = self._post(with_upstream_mocks=False, bracket="B9")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["status"], "FAILED")
        self.assertEqual(body["deck"], [])
        codes = {w["code"] for w in body["warnings"]}
        self.assertIn("INVALID_BRACKET", codes)

    def test_missing_commander_returns_failed(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        response = self._post(with_upstream_mocks=False, commander="   ")
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["status"], "FAILED")
        codes = {w["code"] for w in body["warnings"]}
        self.assertIn("MISSING_COMMANDER", codes)

    def test_summary_reports_endpoint_call_count_and_timings(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")
        response = self._post()
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        summary = body["summary"]
        self.assertGreaterEqual(summary["endpoint_call_count"], 2,
                                "expected at least 1 brief + 1 theme call")
        self.assertIn("phase_timings_ms", summary)
        timings = summary["phase_timings_ms"]
        self.assertIn("pool", timings)
        self.assertIn("select", timings)


if __name__ == "__main__":
    unittest.main()
