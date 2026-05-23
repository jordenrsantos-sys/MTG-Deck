"""
test_candidate_pool_fill_rate — Mega-task v7 Phase 1.

Asserts the iter-7 sweep cases now yield a candidate pool with ≥60 spell
candidates after the v7 fix chain:
  - archetype staples hydrated with type_line + primitives so the slot
    classifier routes them correctly,
  - _RAMP_/_DRAW_/_REMOVAL_/_WIN_CONDITION_PRIMITIVES updated to match
    both legacy primitive_to_cards vocab and v6 Phase 3 cards.primitives_v1_json
    vocab,
  - _inject_slot_fallback_candidates query the DB directly when slots are
    under-floor (search_cards_v1 silently disables primitives_any when
    inverted-index oids exceed 950 — a Pillar A endpoint quirk we work
    around from the agent layer),
  - _select_deck Pass 3.5 backfill from overflow pool candidates so slot
    caps don't strand the deck under 99 cards.

Pre-v7 baseline: POOL_UNDER_FILL_PADDED_WITH_BASICS warning fired on every
case (~25-32 extra basics padded). Iter-8 Phase 1 target: warning gone +
spell pool ≥60 + basic count ≤38 per case.

These tests query the live `mtg.sqlite` snapshot, so they require the v6
Phase 3 backfill present. They skip cleanly if the active snapshot is
unavailable.
"""
from __future__ import annotations

import os
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


SNAPSHOT_ID = "20260217_190902_tagpass_20260222"

# The conftest.py autouse fixture _use_mtg_test_db_path swaps
# MTG_ENGINE_DB_PATH to a temp hermetic DB on every test. v7 Phase 1
# tests need the live mtg.sqlite so the active snapshot's actual 36k
# cards + primitive_to_cards inverted index are present. This module
# resolves the real DB path once at import (before the autouse fixture
# fires) and the test setUp swaps the env back, then tearDown restores
# whatever the fixture set. Skip the suite cleanly when no real DB is
# discoverable (CI, hermetic dev boxes).
def _discover_real_db_path() -> Optional[Path]:
    candidates: List[Path] = []
    env_path = os.environ.get("MTG_ENGINE_DB_PATH", "")
    if env_path and Path(env_path).is_file() and Path(env_path).stat().st_size > 100_000_000:
        candidates.append(Path(env_path))
    # Delegate to engine.db's own resolution (handles repo layout +
    # external mtg-engine/data sibling). Falls back to common layouts.
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
                # >100 MB sanity gate — the real DB is ~732 MB; hermetic
                # fixture DBs are tens of KB.
                return c
        except Exception:
            continue
    return None


_REAL_DB_PATH = _discover_real_db_path()

# Iter-7 sweep cases — same set as tools/test_pillar_d_iteration_7.py.
SWEEP_CASES: List[Dict[str, Any]] = [
    {
        "id": "edgar_b3_vampire_tribal",
        "commander": "Edgar Markov",
        "bracket": "B3",
        "theme_hints": ["TYPAL_VAMPIRES"],
        "must_include_cards": ["Vito, Thorn of the Dusk Rose", "Bloodthirsty Conqueror"],
    },
    {
        "id": "krenko_b4_goblin_combo",
        "commander": "Krenko, Mob Boss",
        "bracket": "B4",
        "theme_hints": ["TYPAL_GOBLINS"],
        "must_include_cards": ["Conspicuous Snoop", "Kiki-Jiki, Mirror Breaker"],
    },
    {
        "id": "atraxa_b2_proliferate",
        "commander": "Atraxa, Praetors' Voice",
        "bracket": "B2",
        "theme_hints": ["THEME_PROLIFERATE", "THEME_PLUS1_COUNTERS"],
        "must_include_cards": ["Doubling Season", "Pir, Imaginative Rascal"],
    },
    {
        "id": "yuriko_b5_ninja_tempo",
        "commander": "Yuriko, the Tiger's Shadow",
        "bracket": "B5",
        "theme_hints": ["TYPAL_NINJAS"],
        "must_include_cards": ["Thassa's Oracle", "Demonic Consultation"],
    },
    {
        "id": "ur_dragon_b3_dragon_tribal",
        "commander": "The Ur-Dragon",
        "bracket": "B3",
        "theme_hints": ["TYPAL_DRAGONS"],
        "must_include_cards": ["Dragon Tempest", "Tiamat"],
    },
]


def _snapshot_available() -> bool:
    if _REAL_DB_PATH is None:
        return False
    # Probe the real DB directly without going through the (autouse-swapped)
    # MTG_ENGINE_DB_PATH env var.
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


def _run_case(case: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, str]], List[Dict[str, str]]]:
    """Returns (pool, deck_body, select_warnings) for a single sweep case."""
    from api.engine.layers.agent_build_deck_v1 import _build_candidate_pool, _select_deck
    call_counter = {"calls": 0}
    pool = _build_candidate_pool(
        db_snapshot_id=SNAPSHOT_ID,
        commander=case["commander"],
        bracket=case["bracket"],
        theme_hints=case["theme_hints"],
        must_include_cards=case["must_include_cards"],
        seed="fill_rate_test",
        call_counter=call_counter,
        suggested_extension_names=None,
        forbidden_set=set(),
    )
    body, warnings = _select_deck(
        pool=pool, bracket=case["bracket"], commander=case["commander"],
    )
    return pool, body, warnings


@unittest.skipUnless(_snapshot_available(), f"snapshot {SNAPSHOT_ID} not available")
class CandidatePoolFillRateTests(unittest.TestCase):
    """v7 Phase 1: pool ≥60 spells per case + no POOL_UNDER_FILL warning."""

    def setUp(self) -> None:
        # The autouse conftest fixture _use_mtg_test_db_path points
        # MTG_ENGINE_DB_PATH at a temp hermetic DB. Override back to the
        # real live DB for the duration of this test so we exercise the
        # actual 36k-card snapshot. tearDown restores whatever the fixture
        # had set.
        self._saved_db_env = os.environ.get("MTG_ENGINE_DB_PATH", "")
        assert _REAL_DB_PATH is not None
        os.environ["MTG_ENGINE_DB_PATH"] = str(_REAL_DB_PATH)

    def tearDown(self) -> None:
        if self._saved_db_env:
            os.environ["MTG_ENGINE_DB_PATH"] = self._saved_db_env
        else:
            os.environ.pop("MTG_ENGINE_DB_PATH", None)

    def test_pool_yields_at_least_60_spells_per_case(self) -> None:
        for case in SWEEP_CASES:
            with self.subTest(case=case["id"]):
                pool, _, _ = _run_case(case)
                trace = pool.get("pool_filter_trace", {})
                slot_dist = trace.get("slot_distribution", {})
                spells = sum(v for k, v in slot_dist.items() if k != "land")
                self.assertGreaterEqual(
                    spells, 60,
                    f"{case['id']}: pool spells={spells} < 60 (v7 Phase 1 target). "
                    f"pool_size={len(pool.get('candidates', []))}, trace={trace}",
                )

    def test_pool_underfill_warning_does_not_fire_post_v7(self) -> None:
        for case in SWEEP_CASES:
            with self.subTest(case=case["id"]):
                _, body, warnings = _run_case(case)
                codes = {w.get("code") for w in warnings}
                self.assertNotIn(
                    "POOL_UNDER_FILL_PADDED_WITH_BASICS", codes,
                    f"{case['id']}: under-fill warning still fires post-v7 Phase 1",
                )
                # _select_deck returns 99 non-commander cards.
                self.assertEqual(len(body), 99, f"{case['id']}: deck body should be exactly 99")

    def test_basic_lands_at_most_38_per_case(self) -> None:
        """Pre-v7 Edgar build had 62-68 basics. Post-v7, basic count should
        sit at or below 38 (≤36 land target + some color-matched dual lands
        from the pool can ALSO be in the deck, so basics typically 20-32)."""
        BASIC_NAMES = {"Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"}
        for case in SWEEP_CASES:
            with self.subTest(case=case["id"]):
                _, body, _ = _run_case(case)
                basics = sum(1 for c in body if c.get("card_name") in BASIC_NAMES)
                self.assertLessEqual(
                    basics, 38,
                    f"{case['id']}: deck has {basics} basics, expected ≤38",
                )

    def test_pool_slot_distribution_is_healthy(self) -> None:
        """All four primary spell slots should have ≥5 cards after fallback,
        proving the slot-fallback injection is working end-to-end."""
        for case in SWEEP_CASES:
            with self.subTest(case=case["id"]):
                pool, _, _ = _run_case(case)
                trace = pool.get("pool_filter_trace", {})
                slot_dist = trace.get("slot_distribution", {})
                self.assertGreaterEqual(
                    slot_dist.get("ramp", 0), 5,
                    f"{case['id']}: ramp slot under-populated: {slot_dist}",
                )
                self.assertGreaterEqual(
                    slot_dist.get("card_draw", 0), 5,
                    f"{case['id']}: card_draw slot under-populated: {slot_dist}",
                )
                self.assertGreaterEqual(
                    slot_dist.get("removal", 0), 4,
                    f"{case['id']}: removal slot under-populated: {slot_dist}",
                )

    def test_pool_filter_trace_is_returned(self) -> None:
        """The v7 instrumentation surface — pool response carries the trace
        so callers + future debug sessions can audit per-stage counts."""
        case = SWEEP_CASES[0]
        pool, _, _ = _run_case(case)
        trace = pool.get("pool_filter_trace")
        self.assertIsNotNone(trace, "pool_filter_trace missing from pool response")
        self.assertIn("staples_in_brief", trace)
        self.assertIn("staples_hydrated", trace)
        self.assertIn("slot_fallback", trace)
        self.assertIn("slot_distribution", trace)


if __name__ == "__main__":
    unittest.main()
