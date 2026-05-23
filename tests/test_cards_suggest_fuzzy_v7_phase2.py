"""
test_cards_suggest_fuzzy_v7_phase2 — Mega-task v7 Phase 2.

Exercises the `/cards/suggest` endpoint's new `fuzzy=true` parameter,
which falls back to difflib edit-distance matching when prefix +
substring matching yields zero results. The fuzzy fallback exists to
handle typos in the AIBuildView commander typeahead (e.g.,
"Edgar Makrov" → "Edgar Markov") that pre-v7 cascaded into total deck
failure via BRIEF_NO_CORPUS_ENTRIES_FOR_COMMANDER.

These tests query the live snapshot; they skip cleanly when the real DB
is unavailable (CI, hermetic dev boxes). Hermetic conftest fixture
swap-in is bypassed via the same MTG_ENGINE_DB_PATH override pattern
used by test_candidate_pool_fill_rate.
"""
from __future__ import annotations

import os
import unittest
from pathlib import Path
from typing import Optional


SNAPSHOT_ID = "20260217_190902_tagpass_20260222"


def _discover_real_db_path() -> Optional[Path]:
    candidates = []
    env_path = os.environ.get("MTG_ENGINE_DB_PATH", "")
    if env_path and Path(env_path).is_file() and Path(env_path).stat().st_size > 100_000_000:
        candidates.append(Path(env_path))
    try:
        from engine.db import resolve_db_path
        p = resolve_db_path()
        if p:
            candidates.append(Path(p))
    except Exception:
        pass
    repo_root = Path(__file__).resolve().parents[1]
    candidates.append(repo_root / "engine" / "data" / "mtg.sqlite")
    candidates.append(repo_root.parent / "data" / "mtg.sqlite")
    for c in candidates:
        try:
            if c.is_file() and c.stat().st_size > 100_000_000:
                return c
        except Exception:
            continue
    return None


_REAL_DB_PATH = _discover_real_db_path()


def _snapshot_available() -> bool:
    if _REAL_DB_PATH is None:
        return False
    import sqlite3
    try:
        con = sqlite3.connect(f"file:{_REAL_DB_PATH}?mode=ro", uri=True)
        try:
            row = con.execute(
                "SELECT 1 FROM snapshots WHERE snapshot_id = ?", (SNAPSHOT_ID,)
            ).fetchone()
            return bool(row)
        finally:
            con.close()
    except Exception:
        return False


@unittest.skipUnless(_snapshot_available(), f"snapshot {SNAPSHOT_ID} not available")
class CardsSuggestFuzzyTests(unittest.TestCase):
    """v7 Phase 2: fuzzy=true fallback recovers from typos."""

    def setUp(self) -> None:
        self._saved_db_env = os.environ.get("MTG_ENGINE_DB_PATH", "")
        assert _REAL_DB_PATH is not None
        os.environ["MTG_ENGINE_DB_PATH"] = str(_REAL_DB_PATH)
        from fastapi.testclient import TestClient
        from api.main import app
        self.client = TestClient(app)

    def tearDown(self) -> None:
        if self._saved_db_env:
            os.environ["MTG_ENGINE_DB_PATH"] = self._saved_db_env
        else:
            os.environ.pop("MTG_ENGINE_DB_PATH", None)

    def test_exact_typeahead_returns_edgar_markov_for_edgar(self) -> None:
        r = self.client.get(
            "/cards/suggest",
            params={"q": "edgar", "snapshot_id": SNAPSHOT_ID,
                    "commander_only": "true", "limit": 5},
        )
        self.assertEqual(r.status_code, 200, r.text)
        names = [x.get("name") for x in r.json().get("results", [])]
        self.assertIn("Edgar Markov", names)

    def test_fuzzy_recovers_edgar_makrov_typo(self) -> None:
        # Pre-v7 the typo "Edgar Makrov" returned zero results and the
        # AIBuildView shipped that empty value to the build endpoint,
        # which then cascaded into BRIEF_NO_CORPUS_ENTRIES_FOR_COMMANDER
        # and a 99-Wastes deck.
        r = self.client.get(
            "/cards/suggest",
            params={"q": "edgar makrov", "snapshot_id": SNAPSHOT_ID,
                    "commander_only": "true", "fuzzy": "true", "limit": 5},
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertTrue(body.get("fuzzy_active"))
        names = [x.get("name") for x in body.get("results", [])]
        self.assertIn(
            "Edgar Markov", names,
            f"Fuzzy fallback did not surface Edgar Markov; got {names}",
        )
        # The fuzzy match should be tagged so the UI can render
        # "Did you mean: ..." rather than treating it as a direct hit.
        fuzzy_rows = [x for x in body.get("results", []) if x.get("fuzzy_match")]
        self.assertTrue(
            any(x.get("name") == "Edgar Markov" for x in fuzzy_rows),
            "Edgar Markov result should carry fuzzy_match=True",
        )

    def test_fuzzy_not_active_without_param(self) -> None:
        # Without fuzzy=true a typo returns 0 results — preserve the
        # existing default behavior.
        r = self.client.get(
            "/cards/suggest",
            params={"q": "edgar makrov", "snapshot_id": SNAPSHOT_ID,
                    "commander_only": "true", "limit": 5},
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertFalse(body.get("fuzzy_active"))
        self.assertEqual(body.get("results", []), [])

    def test_fuzzy_skipped_when_exact_results_exist(self) -> None:
        # When the exact query returns non-empty, fuzzy must NOT inject
        # extra results (the user clearly meant the exact match they typed).
        r = self.client.get(
            "/cards/suggest",
            params={"q": "edgar", "snapshot_id": SNAPSHOT_ID,
                    "commander_only": "true", "fuzzy": "true", "limit": 5},
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        # All returned rows should be deterministic matches (no fuzzy_match
        # flag) since the prefix search yielded results.
        fuzzy_rows = [x for x in body.get("results", []) if x.get("fuzzy_match")]
        self.assertEqual(
            fuzzy_rows, [],
            "Fuzzy should not inject when exact match returned results",
        )

    def test_response_shape_is_backward_compatible(self) -> None:
        # The fuzzy_active field is new in v7; legacy callers that don't
        # opt in should see the same shape minus that flag (we set it to
        # False, which preserves the documented contract).
        r = self.client.get(
            "/cards/suggest",
            params={"q": "lightning", "snapshot_id": SNAPSHOT_ID, "limit": 3},
        )
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        for key in ("query", "snapshot_id", "limit", "results", "fuzzy_active"):
            self.assertIn(key, body)
        self.assertIsInstance(body["results"], list)


if __name__ == "__main__":
    unittest.main()
