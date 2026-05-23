"""Mega-task v6 Phase 1: end-to-end regression test for the SSE streaming endpoint.

The v5 unit tests in `test_agent_build_deck_v1_stream.py` use FastAPI's
``TestClient`` which delivers the response body as a single string after the
generator finishes. That hides any streaming/wire-format issue that only
manifests when bytes are consumed incrementally.

This test uses ``httpx.AsyncClient`` + ``httpx.ASGITransport`` to actually
iterate the response in chunks (mirroring what a browser's
``fetch + ReadableStream.getReader()`` does) and parses each chunk with
the SSE parser logic that's a bit-for-bit equivalent of
``useBuildStreaming.ts::_parseSseBuffer``.

If this test passes, the server emits a valid SSE wire format that any
cross-origin streaming consumer (including the browser) can parse end to
end, AND emits the expected ``{"phase": "complete"}`` event with a
non-empty response payload.

Catches the future regression class: any change to ``main.py``'s
``_run_build`` / ``event_generator`` / ``EventSourceResponse`` wiring that
would prevent the browser from receiving phase events.
"""
from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import patch

try:
    import httpx
    from api.main import app
    from tests.decklist_fixture_harness import (
        DECKLIST_FIXTURE_SNAPSHOT_ID,
        create_decklist_fixture_db,
        set_decklist_fixture_env,
    )
    _IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - environment-dependent
    httpx = None  # type: ignore[assignment]
    app = None
    DECKLIST_FIXTURE_SNAPSHOT_ID = ""
    _IMPORT_ERROR = exc


COMMANDER = "Edgar Markov"


def _parse_sse_buffer(buffer: str) -> Tuple[List[Dict[str, Any]], str]:
    """Bit-for-bit equivalent of useBuildStreaming.ts::_parseSseBuffer.

    Keeping the two parsers locked in lockstep is the whole point of this
    test: if the wire format ever drifts (e.g., separator changes from
    \\r\\n to \\n, or data: lines get split across multiple data: lines
    in a way the UI parser doesn't expect), the assertion below catches
    it before it ships to the browser.
    """
    events: List[Dict[str, Any]] = []
    normalized = buffer.replace("\r\n", "\n")
    parts = normalized.split("\n\n")
    remaining = parts.pop() if parts else ""
    for block in parts:
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
                continue
            events.append({"event": event_type, "data": data})
    return events, remaining


def _archetype_brief_stub() -> dict:
    return {
        "version": "archetype_brief_v1.0",
        "commander": COMMANDER,
        "commander_oracle_id": "edgar-oracle-id",
        "color_identity": ["B", "R", "W"],
        "corpus_deck_count": 25,
        "common_archetypes": [
            {"name": "Vampire Tribal Aristocrats", "frequency": 0.6, "deck_count": 15}
        ],
        "bracket_distribution": {"B3": 0.5, "B4": 0.3},
        "staple_cards": [
            {"name": "Sol Ring", "usage_pct": 0.92},
            {"name": "Command Tower", "usage_pct": 0.88},
        ],
        "warnings": [],
    }


def _theme_top_cards_stub() -> dict:
    return {
        "version": "theme_top_cards_v1.0",
        "theme_id": "TYPAL_VAMPIRES",
        "color_identity": ["B", "R", "W"],
        "results": [
            {
                "card_name": f"Stub Vampire #{i}",
                "oracle_id": f"oid-{i}",
                "primitives": ["typal-vampire"],
                "type_line": "Creature — Vampire",
                "mana_cost": "{2}{B}",
                "cmc": 3,
                "frequency_in_corpus": 0.1,
                "rarity": "uncommon",
            }
            for i in range(60)
        ],
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


def _mocks_for_fast_build():
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


async def _stream_to_completion(
    base_url: str,
    payload: Dict[str, Any],
    *,
    origin: str = "http://localhost:5173",
) -> Tuple[int, Dict[str, str], List[Dict[str, Any]]]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url=base_url) as client:
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "Origin": origin,
        }
        async with client.stream(
            "POST",
            "/agent/build_deck_v1/stream",
            json=payload,
            headers=headers,
        ) as response:
            events: List[Dict[str, Any]] = []
            buffer = ""
            async for chunk in response.aiter_text():
                buffer += chunk
                new_events, buffer = _parse_sse_buffer(buffer)
                events.extend(new_events)
            return response.status_code, dict(response.headers), events


class StreamE2ETests(unittest.TestCase):
    """End-to-end regression: server emits a SSE wire format the UI parser
    can consume incrementally, including the final complete event with a
    full response payload."""

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
            self.skipTest(f"integration deps unavailable: {_IMPORT_ERROR}")

    def _run(self, payload, origin: str = "http://localhost:5173"):
        return asyncio.run(_stream_to_completion("http://test", payload, origin=origin))

    def test_streaming_endpoint_delivers_events_incrementally_with_complete_payload(self) -> None:
        mocks = _mocks_for_fast_build()
        for m in mocks:
            m.start()
        try:
            status, headers, events = self._run({
                "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                "commander": COMMANDER,
                "bracket": "B3",
                "theme_hints": ["TYPAL_VAMPIRES"],
                "must_include_cards": [],
            })
        finally:
            for m in mocks:
                m.stop()

        self.assertEqual(status, 200)
        ct = headers.get("content-type", "")
        self.assertIn("text/event-stream", ct.lower())

        # >=6 progress events covering deterministic phases (per kickoff).
        progress_phases = {
            (e["data"].get("phase"), e["data"].get("status"))
            for e in events
            if e["event"] != "error"
        }
        self.assertGreaterEqual(
            len(progress_phases),
            6,
            f"want >=6 distinct (phase,status) tuples, got {len(progress_phases)}: "
            f"{sorted(progress_phases)}",
        )

        # Final "complete" event with a real response payload.
        complete_events = [e for e in events if e["data"].get("phase") == "complete"]
        self.assertTrue(complete_events, "no complete event emitted")
        last_complete = complete_events[-1]
        self.assertEqual(last_complete["data"].get("status"), "completed")
        resp = last_complete["data"].get("response")
        self.assertIsInstance(resp, dict, "complete event missing response payload")
        self.assertEqual(resp.get("version"), "agent_build_deck_v1.0")

    def test_streaming_cors_headers_match_allowlist_origin(self) -> None:
        """The dev allowlist must include localhost:5173 + 127.0.0.1:5173/5174.
        Verifies the CORS access-control-allow-origin matches the request
        Origin so the browser actually delivers the body to the JS layer."""
        mocks = _mocks_for_fast_build()
        for m in mocks:
            m.start()
        try:
            status, headers, _events = self._run({
                "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
                "commander": COMMANDER,
                "bracket": "B3",
                "theme_hints": ["TYPAL_VAMPIRES"],
                "must_include_cards": [],
            }, origin="http://localhost:5173")
        finally:
            for m in mocks:
                m.stop()

        self.assertEqual(status, 200)
        # CORSMiddleware echoes the request Origin when allowed; without
        # this header the browser drops the response body silently.
        self.assertEqual(
            headers.get("access-control-allow-origin"),
            "http://localhost:5173",
        )


if __name__ == "__main__":  # pragma: no cover
    sys.exit(unittest.main())
