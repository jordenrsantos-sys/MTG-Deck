"""v1.7 Stage 3 — Power Tune multi-swap regression test.

Asserts that with `max_swaps=5` and a deck possessing multiple weak
slots eligible for cuts, the Power Tune engine emits AT LEAST 3 swaps.

Pre-fix behavior (user-observed regression): the engine returned only
1 swap because `_partition_protected_cut_candidates` unconditionally
protects the top `PROTECT_TOP_K_CARDS_V1 = 8` non-dead cuts. With a
small fixture deck of 3 non-commander slots (2 non-dead RAMP_MANA +
1 dead utility), both non-dead slots fall within the absolute top-8
protection window, leaving only the dead slot eligible for cutting.
1 eligible cut → ≤ 1 swap.

Post-fix behavior: the protect-top-k cap is bounded so that at least
`min_eligible_floor` non-dead slots remain eligible. For
production-size 100-card decks (~95 non-dead slots), behavior is
unchanged (8 < 95 - floor). For small fixtures, the cap opens up.

The "≥3" assertion is a defensible lower bound: with 3 unique cut
candidates and 3 unique add candidates from the curated pool, the
post-collapse / dedup-by-add / dedup-by-cut math yields up to 3
selected swaps (one per unique add, one per unique cut). The full
`max_swaps=5` may not always be reached because the engine honestly
caps below 5 when the unique-add or unique-cut pool runs out — but
≥ 3 proves multi-swap emission works.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from api.engine.deck_tune_engine_v1 import run_deck_tune_engine_v1
from tests.guardrails_fixture_harness import (
    GUARDRAILS_FIXTURE_SNAPSHOT_ID,
    create_guardrails_fixture_db,
    set_guardrails_fixture_env,
)


class PowerTuneMultiSwapTests(unittest.TestCase):
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

    def _canonical_input(self, cards: list[str]) -> dict:
        return {
            "db_snapshot_id": GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            "profile_id": "focused",
            "bracket_id": "B3",
            "format": "commander",
            "commander": "Niv-Mizzet, Parun",
            "cards": list(cards),
            "engine_patches_v0": [],
        }

    def _baseline_build_result(self) -> dict:
        """Baseline result mirrors the canonical multi-weak-slot scenario:
        3 non-commander slots (S0/S1 = RAMP_MANA HIGH redundancy excess,
        S2 = dead utility slot), CARD_DRAW required + missing entirely.
        """
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
                        "dead_slot_count": 1,
                        "missing_required_count": 1,
                    },
                },
                "required_primitives_v0": ["RAMP_MANA", "CARD_DRAW"],
                "redundancy_index_v1": {
                    "per_requirement": [
                        {"primitive": "RAMP_MANA", "min": 1, "count": 2,
                         "supported": True, "redundancy_ratio": 2.0,
                         "redundancy_level": "HIGH"},
                        {"primitive": "CARD_DRAW", "min": 1, "count": 0,
                         "supported": True, "redundancy_ratio": 0.0,
                         "redundancy_level": "LOW"},
                    ]
                },
                "resilience_math_engine_v1": {
                    "metrics": {"engine_continuity_after_removal": 0.5,
                                "rebuild_after_wipe": 0.5}
                },
                "engine_coherence_v1": {"metrics": {"overlap_score": 0.1}},
                "profile_bracket_enforcement_v1": {
                    "counts": {"game_changers_in_deck": 0}
                },
            },
        }

    def test_power_tune_emits_at_least_three_swaps_with_max_swaps_five(self) -> None:
        payload = run_deck_tune_engine_v1(
            canonical_deck_input=self._canonical_input(
                cards=["Arcane Signet", "Mystery Card", "Plain Utility"]
            ),
            baseline_build_result=self._baseline_build_result(),
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B3",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            max_swaps=5,
        )
        swaps = payload.get("recommended_swaps_v1") or []
        eval_summary = payload.get("evaluation_summary_v1") or {}
        self.assertGreaterEqual(
            len(swaps), 3,
            f"Expected at least 3 swaps with max_swaps=5; got {len(swaps)}. "
            f"evaluation_summary_v1={eval_summary!r}; "
            f"swaps={[(s.get('cut_name'), s.get('add_name')) for s in swaps]}",
        )

    def test_max_swaps_one_still_caps_at_one(self) -> None:
        """Regression guard: the protect-top-k cap relaxation must NOT
        allow more swaps than max_swaps. With max_swaps=1, ≤1 swap.
        """
        payload = run_deck_tune_engine_v1(
            canonical_deck_input=self._canonical_input(
                cards=["Arcane Signet", "Mystery Card", "Plain Utility"]
            ),
            baseline_build_result=self._baseline_build_result(),
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B3",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            max_swaps=1,
        )
        swaps = payload.get("recommended_swaps_v1") or []
        self.assertLessEqual(len(swaps), 1)


if __name__ == "__main__":
    unittest.main()
