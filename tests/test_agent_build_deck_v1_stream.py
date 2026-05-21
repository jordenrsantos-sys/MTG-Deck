"""Mega-task v5 Phase 3: /agent/build_deck_v1/stream SSE endpoint tests.

Verifies progress events are emitted at every phase boundary and that the
final "complete" event carries the full response payload. Mocks the heavy
LLM/corpus calls so the test doesn't pay live API cost.

Also includes unit tests for the _emit_progress helper + the
compute_agent_build_deck_v1 progress_callback integration directly (without
going through HTTP) so a future refactor that breaks the callback wiring
gets caught by a fast unit test.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

from tests.decklist_fixture_harness import (
    DECKLIST_FIXTURE_SNAPSHOT_ID,
    create_decklist_fixture_db,
    set_decklist_fixture_env,
)

try:
    from fastapi.testclient import TestClient
    from api.main import app
    from api.engine.layers.agent_build_deck_v1 import (
        compute_agent_build_deck_v1,
        _emit_progress,
    )

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
    results = []
    for i in range(60):
        results.append({
            "card_name": f"Stub Vampire #{i}",
            "oracle_id": f"oid-{i}",
            "primitives": ["typal-vampire"],
            "type_line": "Creature — Vampire",
            "mana_cost": "{2}{B}",
            "cmc": 3,
            "frequency_in_corpus": 0.1,
            "rarity": "uncommon",
        })
    return {
        "version": "theme_top_cards_v1.0",
        "theme_id": "TYPAL_VAMPIRES",
        "color_identity": ["B", "R", "W"],
        "results": results,
        "warnings": [],
    }


def _find_card_stub(name: str, *_a, **_kw) -> Dict[str, Any]:
    if name == COMMANDER:
        return {"name": COMMANDER, "oracle_id": "edgar-oracle-id", "color_identity": ["B", "R", "W"]}
    return {"name": name, "oracle_id": f"oid-{name}", "color_identity": []}


def _strength_check_stub(*_a, **_kw) -> dict:
    return {
        "version": "deck_strength_check_v1.0",
        "summary": {"bracket_signal": "B3", "mean_similarity": 0.7, "nearest_neighbors_count": 5},
        "warnings": [],
    }


def _analyze_stub(*_a, **_kw) -> dict:
    return {
        "version": "deck_analyze_v1.0",
        "themes_classified": [{"theme_id": "TYPAL_VAMPIRES", "confidence": 0.8}],
        "warnings": [],
    }


class EmitProgressHelperTests(unittest.TestCase):
    def test_emit_with_none_callback_is_no_op(self) -> None:
        # Should not raise; should not need any of the params.
        _emit_progress(None, phase="x", status="started", t_start=0.0)

    def test_emit_populates_standard_fields(self) -> None:
        captured: List[dict] = []
        llm_metrics = {"calls": [{"cost_usd": 0.1}, {"cost_usd": 0.05}]}
        call_counter = {"calls": 3}
        _emit_progress(
            captured.append,
            phase="intent_interpreter",
            status="started",
            t_start=0.0,
            llm_metrics=llm_metrics,
            call_counter=call_counter,
        )
        self.assertEqual(len(captured), 1)
        ev = captured[0]
        self.assertEqual(ev["phase"], "intent_interpreter")
        self.assertEqual(ev["status"], "started")
        self.assertIn("elapsed_s", ev)
        self.assertAlmostEqual(ev["cost_usd"], 0.15, places=3)
        self.assertEqual(ev["calls_so_far"], 3)

    def test_emit_includes_extra_payload(self) -> None:
        captured: List[dict] = []
        _emit_progress(
            captured.append,
            phase="complete",
            status="completed",
            t_start=0.0,
            extra={"response": {"version": "v1", "deck": []}},
        )
        self.assertIn("response", captured[0])
        self.assertEqual(captured[0]["response"]["version"], "v1")

    def test_emit_swallows_callback_errors(self) -> None:
        def boom(_event):
            raise RuntimeError("intentional")
        # Must not raise — progress reporting cannot break the build.
        _emit_progress(boom, phase="x", status="started", t_start=0.0)


class ProgressCallbackIntegrationTests(unittest.TestCase):
    """Tests that compute_agent_build_deck_v1 emits at every phase boundary."""

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

    def setUp(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")

    def test_progress_callback_emits_all_phase_boundaries(self) -> None:
        captured: List[dict] = []
        from api.engine.layers import agent_endpoints_v1 as ae
        from api.engine.layers import agent_llm_client_v1 as llm_client_mod
        from api.engine.layers import deck_analyze_v1 as da
        from api.engine.layers import deck_strength_check_v1 as sc
        from engine import db as engine_db

        class _UnavailableLLMClient:
            model = None

            def is_available(self):
                return False

            def unavailable_reason(self):
                return "Test fixture forces LLM-unavailable mode."

        mocks = [
            patch.object(ae, "compute_archetype_brief_v1", return_value=_archetype_brief_stub()),
            patch.object(ae, "compute_theme_top_cards_v1", return_value=_theme_top_cards_stub()),
            patch.object(engine_db, "find_card_by_name", side_effect=_find_card_stub),
            patch.object(da, "compute_deck_analyze_v1", side_effect=_analyze_stub),
            patch.object(sc, "compute_deck_strength_check_v1", side_effect=_strength_check_stub),
            patch.object(llm_client_mod, "get_default_client", return_value=_UnavailableLLMClient()),
        ]
        for m in mocks:
            m.start()
        try:
            response = compute_agent_build_deck_v1(
                db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
                commander=COMMANDER,
                bracket="B3",
                theme_hints=["TYPAL_VAMPIRES"],
                must_include_cards=[],
                progress_callback=captured.append,
            )
        finally:
            for m in mocks:
                m.stop()

        phases = [ev["phase"] for ev in captured]
        # The "complete" event must always fire.
        self.assertEqual(phases[-1], "complete")
        # Core deterministic phases must fire regardless of LLM availability:
        self.assertIn("candidate_pool", phases)
        self.assertIn("select_deck", phases)
        self.assertIn("validate_swap", phases)
        self.assertIn("structural_safety_net", phases)
        self.assertIn("mana_base", phases)
        self.assertIn("card_advantage", phases)
        # Every non-complete phase emits both started + completed:
        for phase in ("candidate_pool", "select_deck", "validate_swap", "structural_safety_net",
                      "mana_base", "card_advantage"):
            statuses = [ev["status"] for ev in captured if ev["phase"] == phase]
            self.assertIn("started", statuses, f"phase {phase} missing 'started'")
            self.assertIn("completed", statuses, f"phase {phase} missing 'completed'")
        # The final complete event carries the full response.
        complete_event = captured[-1]
        self.assertIn("response", complete_event)
        self.assertEqual(complete_event["response"]["version"], response["version"])
        self.assertEqual(complete_event["response"]["status"], response["status"])

    def test_progress_callback_emits_on_invalid_bracket_early_return(self) -> None:
        captured: List[dict] = []
        compute_agent_build_deck_v1(
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            commander=COMMANDER,
            bracket="B9_INVALID",
            theme_hints=[],
            must_include_cards=[],
            progress_callback=captured.append,
        )
        # Even on early-return, a "complete" event must fire so the SSE
        # client knows the stream is done.
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0]["phase"], "complete")
        self.assertEqual(captured[0]["response"]["status"], "FAILED")
        self.assertEqual(captured[0]["response"]["warnings"][0]["code"], "INVALID_BRACKET")

    def test_progress_callback_emits_on_missing_commander_early_return(self) -> None:
        captured: List[dict] = []
        compute_agent_build_deck_v1(
            db_snapshot_id=DECKLIST_FIXTURE_SNAPSHOT_ID,
            commander="",
            bracket="B3",
            theme_hints=[],
            must_include_cards=[],
            progress_callback=captured.append,
        )
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0]["phase"], "complete")
        self.assertEqual(captured[0]["response"]["warnings"][0]["code"], "MISSING_COMMANDER")


class StreamEndpointTests(unittest.TestCase):
    """End-to-end TestClient verification of the SSE endpoint."""

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

    def setUp(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration unavailable: {_IMPORT_ERROR}")

    def _parse_sse_events(self, body_text: str) -> List[Dict[str, Any]]:
        """Parse the SSE wire format into a list of {event, data} dicts."""
        # sse-starlette uses \r\n line endings + \r\n\r\n block separator;
        # also skips comment lines starting with ":" (e.g. ": ping - ...").
        normalized = body_text.replace("\r\n", "\n")
        events: List[Dict[str, Any]] = []
        for block in normalized.split("\n\n"):
            event_type = "message"
            data_lines: List[str] = []
            for line in block.split("\n"):
                if not line:
                    continue
                if line.startswith(":"):
                    # SSE comment / keep-alive — skip.
                    continue
                if line.startswith("event:"):
                    event_type = line[len("event:"):].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[len("data:"):].strip())
            if data_lines:
                try:
                    data = json.loads("\n".join(data_lines))
                except json.JSONDecodeError:
                    data = {"raw": "\n".join(data_lines)}
                events.append({"event": event_type, "data": data})
        return events

    def _mocks_for_fast_build(self):
        """Mocks that make the full build complete in ~3s without burning the LLM budget."""
        from api.engine.layers import agent_endpoints_v1 as ae
        from api.engine.layers import agent_llm_client_v1 as llm_client_mod
        from api.engine.layers import deck_analyze_v1 as da
        from api.engine.layers import deck_strength_check_v1 as sc
        from engine import db as engine_db

        class _UnavailableLLMClient:
            model = None

            def is_available(self):
                return False

            def unavailable_reason(self):
                return "Test fixture forces LLM-unavailable mode."

        return [
            patch.object(ae, "compute_archetype_brief_v1", return_value=_archetype_brief_stub()),
            patch.object(ae, "compute_theme_top_cards_v1", return_value=_theme_top_cards_stub()),
            patch.object(engine_db, "find_card_by_name", side_effect=_find_card_stub),
            patch.object(da, "compute_deck_analyze_v1", side_effect=_analyze_stub),
            patch.object(sc, "compute_deck_strength_check_v1", side_effect=_strength_check_stub),
            patch.object(llm_client_mod, "get_default_client", return_value=_UnavailableLLMClient()),
        ]

    def test_stream_endpoint_returns_event_stream_content_type(self) -> None:
        mocks = self._mocks_for_fast_build()
        for m in mocks:
            m.start()
        try:
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.post(
                    "/agent/build_deck_v1/stream",
                    json={
                        "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                        "commander": COMMANDER,
                        "bracket": "B3",
                        "theme_hints": ["TYPAL_VAMPIRES"],
                        "must_include_cards": [],
                    },
                )
            self.assertEqual(response.status_code, 200, response.text)
            # sse-starlette uses Content-Type: text/event-stream
            ct = response.headers.get("content-type", "")
            self.assertIn("text/event-stream", ct.lower())
        finally:
            for m in mocks:
                m.stop()

    def test_stream_endpoint_emits_progress_then_complete(self) -> None:
        mocks = self._mocks_for_fast_build()
        for m in mocks:
            m.start()
        try:
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.post(
                    "/agent/build_deck_v1/stream",
                    json={
                        "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                        "commander": COMMANDER,
                        "bracket": "B3",
                        "theme_hints": ["TYPAL_VAMPIRES"],
                        "must_include_cards": [],
                    },
                )
            events = self._parse_sse_events(response.text)
            # Must have at least: started events for the deterministic phases
            # + a final "complete" event with response payload.
            self.assertGreaterEqual(len(events), 2, f"want >=2 events, got {len(events)}")
            phases = [(e["data"].get("phase"), e["data"].get("status")) for e in events]
            self.assertIn(("complete", "completed"), phases)
            # The complete event must carry the full response.
            complete_ev = next(e for e in events if e["data"].get("phase") == "complete")
            self.assertIn("response", complete_ev["data"])
            self.assertEqual(complete_ev["data"]["response"]["version"], "agent_build_deck_v1.0")
            # All event types should be "progress" (or "error" — none in this case).
            self.assertTrue(all(e["event"] in ("progress", "error", "message") for e in events))

        finally:
            for m in mocks:
                m.stop()

    def test_non_streaming_endpoint_still_works(self) -> None:
        # Backward-compat: the non-streaming endpoint behavior is unchanged.
        mocks = self._mocks_for_fast_build()
        for m in mocks:
            m.start()
        try:
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.post(
                    "/agent/build_deck_v1",
                    json={
                        "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                        "commander": COMMANDER,
                        "bracket": "B3",
                        "theme_hints": ["TYPAL_VAMPIRES"],
                        "must_include_cards": [],
                    },
                )
            self.assertEqual(response.status_code, 200, response.text)
            body = response.json()
            self.assertEqual(body["status"], "OK")
            self.assertEqual(body["version"], "agent_build_deck_v1.0")
            self.assertEqual(len(body["deck"]), 100)
        finally:
            for m in mocks:
                m.stop()


if __name__ == "__main__":
    unittest.main()
