"""v1.7 Stage 4 — Bracket-aware proactive game-changer recommendations.

The bracket policy data (`api/engine/data/brackets/gc_limits_v1.json`)
encodes per-bracket min/max GC counts:
    B1: {min: 0, max: 0}     — game-changers disallowed
    B2: {min: 0, max: 0}     — game-changers disallowed
    B3: {min: 1, max: 3}     — 1-3 GCs allowed
    B4: {min: null, max: 5}  — up to 5 GCs allowed
    B5: {min: 6, max: null}  — ≥6 GCs (unlimited up)

Stage 2 wired GC ENFORCEMENT (capping above the max). Stage 4 adds
proactive GC RECOMMENDATION: when the bracket allows GCs but the
deck has none, the engine emits BRACKET_AWARE_GC entries in the
Power Tune Upgrade pipeline's `recommended_swaps_v1` so the user
sees concrete add candidates calibrated to the chosen bracket.

The BRACKET_AWARE_GC reason is encoded as a tagged string
`"BRACKET_AWARE_GC:<json>"` so it survives api/main.py's strict
`reasons_v1: List[str]` Pydantic filter (HARD safety BYTE-IDENTICAL).

Expected scaling: B3 ≥1, B4 ≥2, B5 ≥3, B1/B2 == 0.

Fixture: guardrails DB carries 3 real GC cards (Rhystic Study,
Cyclonic Rift, Force of Will) — all blue, all legal under the
Niv-Mizzet, Parun (UR) commander used by the existing harness.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from api.engine.layers.bracket_aware_recommendations_v1 import (
    run_deck_tune_with_bracket_aware_recommendations_v1,
)
from tests.guardrails_fixture_harness import (
    GUARDRAILS_FIXTURE_SNAPSHOT_ID,
    create_guardrails_fixture_db,
    set_guardrails_fixture_env,
)


BRACKET_AWARE_GC_REASON_PREFIX = "BRACKET_AWARE_GC:"


class BracketAwareGcRecommendationsTests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
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

    def _canonical_input(self, *, bracket_id: str, cards: list[str]) -> dict:
        return {
            "db_snapshot_id": GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            "profile_id": "focused",
            "bracket_id": bracket_id,
            "format": "commander",
            "commander": "Niv-Mizzet, Parun",
            "cards": list(cards),
            "engine_patches_v0": [],
        }

    def _baseline_build_result(self) -> dict:
        return {
            "status": "OK",
            "deck_size_total": 4,
            "result": {
                "deck_cards_canonical_input_order": [
                    {"slot_id": "C0", "resolved_name": "Niv-Mizzet, Parun",
                     "resolved_oracle_id": "ORA_CMDR_001", "status": "PLAYABLE"},
                    {"slot_id": "S0", "resolved_name": "Arcane Signet",
                     "resolved_oracle_id": "ORA_CAN_020", "status": "PLAYABLE"},
                    {"slot_id": "S1", "resolved_name": "Mystery Card",
                     "resolved_oracle_id": "ORA_CAN_060", "status": "PLAYABLE"},
                    {"slot_id": "S2", "resolved_name": "Plain Utility",
                     "resolved_oracle_id": "ORA_CAN_070", "status": "PLAYABLE"},
                ],
                "primitive_index_by_slot": {
                    "C0": ["COMMANDER_ENGINE"],
                    "S0": ["RAMP_MANA"],
                    "S1": ["RAMP_MANA"],
                    "S2": [],
                },
                "structural_snapshot_v1": {
                    "dead_slot_ids_v1": ["S2"],
                    "missing_primitives_v1": ["CARD_DRAW"],
                    "primitive_counts_by_id": {"RAMP_MANA": 2},
                    "primitive_concentration_index_v1": 1.0,
                    "structural_health_summary_v1": {
                        "dead_slot_count": 1, "missing_required_count": 1,
                    },
                },
                "required_primitives_v0": ["RAMP_MANA", "CARD_DRAW"],
                "redundancy_index_v1": {"per_requirement": [
                    {"primitive": "RAMP_MANA", "min": 1, "count": 2,
                     "supported": True, "redundancy_ratio": 2.0,
                     "redundancy_level": "HIGH"},
                    {"primitive": "CARD_DRAW", "min": 1, "count": 0,
                     "supported": True, "redundancy_ratio": 0.0,
                     "redundancy_level": "LOW"},
                ]},
                "resilience_math_engine_v1": {"metrics": {"engine_continuity_after_removal": 0.5, "rebuild_after_wipe": 0.5}},
                "engine_coherence_v1": {"metrics": {"overlap_score": 0.1}},
                "profile_bracket_enforcement_v1": {"counts": {"game_changers_in_deck": 0}},
            },
        }

    def _run(self, bracket_id: str) -> list[dict]:
        payload = run_deck_tune_with_bracket_aware_recommendations_v1(
            canonical_deck_input=self._canonical_input(
                bracket_id=bracket_id,
                cards=["Arcane Signet", "Mystery Card", "Plain Utility"],
            ),
            baseline_build_result=self._baseline_build_result(),
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id=bracket_id,
            profile_id="focused",
            mulligan_model_id="NORMAL",
            max_swaps=10,
        )
        swaps = payload.get("recommended_swaps_v1") or []
        return [s for s in swaps if isinstance(s, dict)]

    def _count_bracket_aware_gc_entries(self, swaps: list[dict]) -> list[dict]:
        out = []
        for swap in swaps:
            reasons = swap.get("reasons_v1") or []
            for r in reasons:
                if isinstance(r, str) and r.startswith(BRACKET_AWARE_GC_REASON_PREFIX):
                    payload = json.loads(r[len(BRACKET_AWARE_GC_REASON_PREFIX):])
                    out.append({"swap": swap, "payload": payload})
        return out

    def test_b3_zero_current_gcs_proposes_at_least_one(self) -> None:
        swaps = self._run("B3")
        entries = self._count_bracket_aware_gc_entries(swaps)
        self.assertGreaterEqual(
            len(entries), 1,
            f"Expected ≥1 BRACKET_AWARE_GC entry for B3; got {len(entries)}. "
            f"swaps={[(s.get('cut_name'), s.get('add_name'), s.get('reasons_v1')) for s in swaps]}",
        )
        first = entries[0]["payload"]
        self.assertIn("recommended_gc_oracle_id", first)
        self.assertIn("recommended_gc_name", first)
        self.assertEqual(first.get("current_deck_gc_count"), 0)
        self.assertEqual(first.get("bracket_max_gc"), 3)

    def test_b4_zero_current_gcs_proposes_at_least_two(self) -> None:
        swaps = self._run("B4")
        entries = self._count_bracket_aware_gc_entries(swaps)
        self.assertGreaterEqual(
            len(entries), 2,
            f"Expected ≥2 BRACKET_AWARE_GC entries for B4; got {len(entries)}. "
            f"reasons sampled: {[swap.get('reasons_v1') for swap in swaps[:5]]}",
        )
        # Per-bracket payload sentinel: bracket_max_gc==5 for B4 (per gc_limits_v1.json).
        self.assertEqual(entries[0]["payload"].get("bracket_max_gc"), 5)

    def test_b5_zero_current_gcs_proposes_at_least_three(self) -> None:
        swaps = self._run("B5")
        entries = self._count_bracket_aware_gc_entries(swaps)
        self.assertGreaterEqual(
            len(entries), 3,
            f"Expected ≥3 BRACKET_AWARE_GC entries for B5; got {len(entries)}.",
        )
        # B5 max is unlimited (null) — payload should signal None.
        self.assertIsNone(entries[0]["payload"].get("bracket_max_gc"))

    def test_b1_emits_zero_proactive_gcs(self) -> None:
        swaps = self._run("B1")
        entries = self._count_bracket_aware_gc_entries(swaps)
        self.assertEqual(
            len(entries), 0,
            f"B1 disallows GCs (max=0); expected 0 BRACKET_AWARE_GC entries; got {len(entries)}",
        )

    def test_b2_emits_zero_proactive_gcs(self) -> None:
        swaps = self._run("B2")
        entries = self._count_bracket_aware_gc_entries(swaps)
        self.assertEqual(
            len(entries), 0,
            f"B2 disallows GCs (max=0); expected 0 BRACKET_AWARE_GC entries; got {len(entries)}",
        )


if __name__ == "__main__":
    unittest.main()
