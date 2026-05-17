"""Integration pytest for v1.7 Stage 5 Deliverable B — bracket-aware GC
production wire-in.

Stage 4 added the engine layer + a convenience wrapper
(`run_deck_tune_with_bracket_aware_recommendations_v1`) but did NOT wire
it into the FastAPI `/deck/tune_v1` route (api/main.py was on the v1.7
HARD-safety BYTE-IDENTICAL list through Stage 4). Stage 5 substitutes
the wrapper at the production call site.

This test exercises the ROUTE-LEVEL wire-up — submits a request to
`/deck/tune_v1` via FastAPI's TestClient against a B3 deck with zero
current GCs, then asserts the response carries at least one
BRACKET_AWARE_GC entry in `recommended_swaps_v1[*].reasons_v1`. The
Stage 4 wrapper test verified the engine-level contract; this test
verifies the wrapper is reachable from the live HTTP boundary.

Fixture: the guardrails fixture DB has Niv-Mizzet, Parun (UR commander)
plus three real GCs (Rhystic Study, Cyclonic Rift, Force of Will), all
blue and thus legal under UR. The deck submitted has zero GCs so the
bracket cap leaves room.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient
    from api.main import app
    _IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover
    TestClient = None  # type: ignore
    app = None  # type: ignore
    _IMPORT_ERROR = exc

from tests.guardrails_fixture_harness import (
    GUARDRAILS_FIXTURE_SNAPSHOT_ID,
    create_guardrails_fixture_db,
    set_guardrails_fixture_env,
)


BRACKET_AWARE_GC_REASON_PREFIX = "BRACKET_AWARE_GC:"


class DeckTuneV1EmitsBracketAwareGcTests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if _IMPORT_ERROR is not None:
            return
        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = create_guardrails_fixture_db(Path(cls._tmp_dir_ctx.name))
        cls._db_env_ctx = set_guardrails_fixture_env(db_path)
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

    def test_deck_tune_v1_response_contains_bracket_aware_gc_entries(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        client = TestClient(app)
        request_body = {
            "db_snapshot_id": GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": (
                "Commander\n1 Niv-Mizzet, Parun\n"
                "Deck\n1 Arcane Signet\n1 Mystery Card\n1 Plain Utility"
            ),
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B3",
            "mulligan_model_id": "NORMAL",
            "commander": "Niv-Mizzet, Parun",
            "max_swaps": 10,
            "engine_patches_v0": [],
        }

        response = client.post("/deck/tune_v1", json=request_body)
        self.assertEqual(
            response.status_code, 200,
            f"Expected 200; got {response.status_code}. body={response.text[:500]}",
        )
        payload = response.json()
        swaps = payload.get("recommended_swaps_v1")
        self.assertIsInstance(swaps, list, f"recommended_swaps_v1 must be a list; got {type(swaps).__name__}")

        bracket_aware_entries: list[tuple[dict, str]] = []
        for swap in swaps or []:
            if not isinstance(swap, dict):
                continue
            reasons = swap.get("reasons_v1") or []
            for r in reasons:
                if isinstance(r, str) and r.startswith(BRACKET_AWARE_GC_REASON_PREFIX):
                    bracket_aware_entries.append((swap, r))

        self.assertGreaterEqual(
            len(bracket_aware_entries), 1,
            f"Expected ≥1 BRACKET_AWARE_GC entry in /deck/tune_v1 response for B3 deck with zero GCs; "
            f"got {len(bracket_aware_entries)}. "
            f"recommended_swaps_v1={[s.get('reasons_v1') for s in (swaps or [])[:5]]}",
        )


if __name__ == "__main__":
    unittest.main()
