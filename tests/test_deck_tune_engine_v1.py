from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.bracket_gc_enforcement_v1 import would_violate_gc_limit_v1
from api.engine.color_identity_constraints_v1 import get_commander_color_identity_v1, is_card_color_legal_v1
from api.engine.deck_tune_engine_v1 import VERSION, run_deck_tune_engine_v1
from api.engine.constants import GAME_CHANGERS_SET
from tests.guardrails_fixture_harness import (
    GUARDRAILS_FIXTURE_SNAPSHOT_ID,
    create_guardrails_fixture_db,
    set_guardrails_fixture_env,
)


def _remove_one(cards: list[str], card_name: str) -> list[str]:
    out: list[str] = []
    removed = False
    for value in cards:
        if (not removed) and value == card_name:
            removed = True
            continue
        out.append(value)
    return out


class DeckTuneEngineV1Tests(unittest.TestCase):
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

    def _canonical_input(self, cards: list[str], engine_patches_v0: list[dict] | None = None) -> dict:
        return {
            "db_snapshot_id": GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            "profile_id": "focused",
            "bracket_id": "B3",
            "format": "commander",
            "commander": "Niv-Mizzet, Parun",
            "cards": list(cards),
            "engine_patches_v0": [dict(row) for row in (engine_patches_v0 or []) if isinstance(row, dict)],
        }

    def _baseline_build_result(self) -> dict:
        return {
            "status": "OK",
            "deck_size_total": 4,
            "result": {
                "deck_cards_canonical_input_order": [
                    {
                        "slot_id": "C0",
                        "resolved_name": "Niv-Mizzet, Parun",
                        "resolved_oracle_id": "ORA_CMDR_001",
                        "status": "PLAYABLE",
                    },
                    {
                        "slot_id": "S0",
                        "resolved_name": "Arcane Signet",
                        "resolved_oracle_id": "ORA_CAN_020",
                        "status": "PLAYABLE",
                    },
                    {
                        "slot_id": "S1",
                        "resolved_name": "Mystery Card",
                        "resolved_oracle_id": "ORA_CAN_060",
                        "status": "PLAYABLE",
                    },
                    {
                        "slot_id": "S2",
                        "resolved_name": "Plain Utility",
                        "resolved_oracle_id": "ORA_CAN_070",
                        "status": "PLAYABLE",
                    },
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
                    "primitive_counts_by_id": {
                        "RAMP_MANA": 2,
                    },
                    "primitive_concentration_index_v1": 1.0,
                    "structural_health_summary_v1": {
                        "dead_slot_count": 1,
                        "missing_required_count": 1,
                    },
                },
                "required_primitives_v0": ["RAMP_MANA", "CARD_DRAW"],
                "redundancy_index_v1": {
                    "per_requirement": [
                        {
                            "primitive": "RAMP_MANA",
                            "min": 1,
                            "count": 2,
                            "supported": True,
                            "redundancy_ratio": 2.0,
                            "redundancy_level": "HIGH",
                        },
                        {
                            "primitive": "CARD_DRAW",
                            "min": 1,
                            "count": 0,
                            "supported": True,
                            "redundancy_ratio": 0.0,
                            "redundancy_level": "LOW",
                        },
                    ]
                },
                "resilience_math_engine_v1": {
                    "metrics": {
                        "engine_continuity_after_removal": 0.5,
                        "rebuild_after_wipe": 0.5,
                    }
                },
                "engine_coherence_v1": {
                    "metrics": {
                        "overlap_score": 0.1,
                    }
                },
                "profile_bracket_enforcement_v1": {
                    "counts": {
                        "game_changers_in_deck": 0,
                    }
                },
            },
        }

    def _swap_candidate(
        self,
        *,
        cut_name: str,
        add_name: str,
        cut_oracle_id: str,
        add_oracle_id: str,
        total: float,
        coherence: float,
        coverage_delta: int,
        missing_required_delta: int = 0,
    ) -> dict:
        return {
            "cut_name": cut_name,
            "add_name": add_name,
            "cut_oracle_id": cut_oracle_id,
            "add_oracle_id": add_oracle_id,
            "reasons_v1": ["ADD_PRIMITIVE_COVERAGE"],
            "delta_summary_v1": {
                "total_score_delta_v1": total,
                "coherence_delta_v1": coherence,
                "primitive_coverage_delta_v1": coverage_delta,
                "missing_required_count_delta_v1": missing_required_delta,
                "gc_compliance_preserved_v1": True,
            },
        }

    def test_deterministic_repeat_same_input(self) -> None:
        canonical = self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"])
        baseline = self._baseline_build_result()

        first = run_deck_tune_engine_v1(
            canonical_deck_input=canonical,
            baseline_build_result=baseline,
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B3",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            max_swaps=5,
        )
        second = run_deck_tune_engine_v1(
            canonical_deck_input=canonical,
            baseline_build_result=baseline,
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B3",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            max_swaps=5,
        )

        self.assertEqual(VERSION, "deck_tune_engine_v1")
        self.assertEqual(first, second)

    def test_max_swaps_respected(self) -> None:
        payload = run_deck_tune_engine_v1(
            canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
            baseline_build_result=self._baseline_build_result(),
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B3",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            max_swaps=1,
        )

        swaps = payload.get("recommended_swaps_v1") if isinstance(payload.get("recommended_swaps_v1"), list) else []
        self.assertLessEqual(len(swaps), 1)

    def test_recommended_adds_never_violate_color_identity(self) -> None:
        payload = run_deck_tune_engine_v1(
            canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
            baseline_build_result=self._baseline_build_result(),
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B3",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            max_swaps=5,
        )

        commander_colors = get_commander_color_identity_v1(
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            commander_name="Niv-Mizzet, Parun",
        )
        self.assertIsInstance(commander_colors, set)

        swaps = payload.get("recommended_swaps_v1") if isinstance(payload.get("recommended_swaps_v1"), list) else []
        for swap in swaps:
            self.assertIsInstance(swap, dict)
            add_name = swap.get("add_name")
            self.assertIsInstance(add_name, str)
            self.assertNotEqual(add_name, "Cultivate")

            legal = is_card_color_legal_v1(
                card_name=add_name,
                commander_color_set=commander_colors,
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            )
            self.assertIs(legal, True)

    def test_recommended_adds_preserve_gc_limits(self) -> None:
        gc_names = sorted([name for name in GAME_CHANGERS_SET if isinstance(name, str)])
        if len(gc_names) < 3:
            self.skipTest("Need at least 3 local game changers for GC limit tests.")

        cards = ["Arcane Signet", "Mystery Card", "Plain Utility", gc_names[0], gc_names[1], gc_names[2]]
        payload = run_deck_tune_engine_v1(
            canonical_deck_input=self._canonical_input(cards=cards),
            baseline_build_result=self._baseline_build_result(),
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B3",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            max_swaps=5,
        )

        swaps = payload.get("recommended_swaps_v1") if isinstance(payload.get("recommended_swaps_v1"), list) else []
        for swap in swaps:
            self.assertIsInstance(swap, dict)
            cut_name = swap.get("cut_name") if isinstance(swap.get("cut_name"), str) else ""
            add_name = swap.get("add_name") if isinstance(swap.get("add_name"), str) else ""
            deck_without_cut = _remove_one(cards, cut_name)
            gc_verdict = would_violate_gc_limit_v1(
                candidate_card=add_name,
                current_cards=deck_without_cut,
                bracket_id="B3",
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            )
            self.assertIs(gc_verdict, False)

            delta = swap.get("delta_summary_v1") if isinstance(swap.get("delta_summary_v1"), dict) else {}
            self.assertIs(delta.get("gc_compliance_preserved_v1"), True)

    def test_unique_add_constraint(self) -> None:
        mocked_swaps = [
            self._swap_candidate(
                cut_name="Plain Utility",
                add_name="Staff of Completion",
                cut_oracle_id="ORA_CAN_070",
                add_oracle_id="ORA_ADD_STAFF",
                total=1.2,
                coherence=0.2,
                coverage_delta=1,
            ),
            self._swap_candidate(
                cut_name="Mystery Card",
                add_name="Staff of Completion",
                cut_oracle_id="ORA_CAN_060",
                add_oracle_id="ORA_ADD_STAFF",
                total=1.6,
                coherence=0.1,
                coverage_delta=1,
            ),
            self._swap_candidate(
                cut_name="Arcane Signet",
                add_name="Ponder",
                cut_oracle_id="ORA_CAN_020",
                add_oracle_id="ORA_ADD_PONDER",
                total=1.1,
                coherence=0.1,
                coverage_delta=1,
            ),
        ]

        with patch(
            "api.engine.deck_tune_engine_v1._evaluate_swap_pairs",
            return_value=(mocked_swaps, len(mocked_swaps), 0.0),
        ):
            payload = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=5,
            )

        swaps = payload.get("recommended_swaps_v1") if isinstance(payload.get("recommended_swaps_v1"), list) else []
        add_names = [row.get("add_name") for row in swaps if isinstance(row, dict)]
        self.assertEqual(len(add_names), len(set(add_names)))

    def test_best_cut_chosen_per_add(self) -> None:
        mocked_swaps = [
            self._swap_candidate(
                cut_name="Plain Utility",
                add_name="Staff of Completion",
                cut_oracle_id="ORA_CAN_070",
                add_oracle_id="ORA_ADD_STAFF",
                total=2.0,
                coherence=0.3,
                coverage_delta=1,
            ),
            self._swap_candidate(
                cut_name="Mystery Card",
                add_name="Staff of Completion",
                cut_oracle_id="ORA_CAN_060",
                add_oracle_id="ORA_ADD_STAFF",
                total=2.5,
                coherence=0.1,
                coverage_delta=1,
            ),
        ]

        with patch(
            "api.engine.deck_tune_engine_v1._evaluate_swap_pairs",
            return_value=(mocked_swaps, len(mocked_swaps), 0.0),
        ):
            payload = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=5,
            )

        swaps = payload.get("recommended_swaps_v1") if isinstance(payload.get("recommended_swaps_v1"), list) else []
        staff_rows = [
            row
            for row in swaps
            if isinstance(row, dict) and row.get("add_name") == "Staff of Completion"
        ]
        self.assertEqual(len(staff_rows), 1)
        self.assertEqual(staff_rows[0].get("cut_name"), "Mystery Card")

    def test_unique_cut_constraint(self) -> None:
        mocked_swaps = [
            self._swap_candidate(
                cut_name="Mystery Card",
                add_name="Staff of Completion",
                cut_oracle_id="ORA_CAN_060",
                add_oracle_id="ORA_ADD_STAFF",
                total=2.1,
                coherence=0.1,
                coverage_delta=1,
            ),
            self._swap_candidate(
                cut_name="Mystery Card",
                add_name="Ponder",
                cut_oracle_id="ORA_CAN_060",
                add_oracle_id="ORA_ADD_PONDER",
                total=2.0,
                coherence=0.1,
                coverage_delta=1,
            ),
            self._swap_candidate(
                cut_name="Plain Utility",
                add_name="Opt",
                cut_oracle_id="ORA_CAN_070",
                add_oracle_id="ORA_ADD_OPT",
                total=1.4,
                coherence=0.1,
                coverage_delta=1,
            ),
        ]

        with patch(
            "api.engine.deck_tune_engine_v1._evaluate_swap_pairs",
            return_value=(mocked_swaps, len(mocked_swaps), 0.0),
        ):
            payload = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=5,
            )

        swaps = payload.get("recommended_swaps_v1") if isinstance(payload.get("recommended_swaps_v1"), list) else []
        cut_names = [row.get("cut_name") for row in swaps if isinstance(row, dict)]
        self.assertEqual(len(cut_names), len(set(cut_names)))

    def test_dev_metrics_report_collapsed_unique_selection_counts(self) -> None:
        mocked_swaps = [
            self._swap_candidate(
                cut_name="Arcane Signet",
                add_name="Staff of Completion",
                cut_oracle_id="ORA_CAN_020",
                add_oracle_id="ORA_ADD_STAFF",
                total=3.0,
                coherence=0.2,
                coverage_delta=1,
            ),
            self._swap_candidate(
                cut_name="Mystery Card",
                add_name="Staff of Completion",
                cut_oracle_id="ORA_CAN_060",
                add_oracle_id="ORA_ADD_STAFF",
                total=2.0,
                coherence=0.2,
                coverage_delta=1,
            ),
            self._swap_candidate(
                cut_name="Arcane Signet",
                add_name="Ponder",
                cut_oracle_id="ORA_CAN_020",
                add_oracle_id="ORA_ADD_PONDER",
                total=2.5,
                coherence=0.1,
                coverage_delta=1,
            ),
            self._swap_candidate(
                cut_name="Plain Utility",
                add_name="Opt",
                cut_oracle_id="ORA_CAN_070",
                add_oracle_id="ORA_ADD_OPT",
                total=1.0,
                coherence=0.1,
                coverage_delta=1,
            ),
        ]

        with patch(
            "api.engine.deck_tune_engine_v1._evaluate_swap_pairs",
            return_value=(mocked_swaps, len(mocked_swaps), 0.0),
        ):
            payload = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=5,
                collect_dev_metrics=True,
            )

        metrics = payload.get("dev_metrics_v1") if isinstance(payload.get("dev_metrics_v1"), dict) else {}
        self.assertEqual(int(metrics.get("raw_pair_count") or 0), 4)
        self.assertEqual(int(metrics.get("unique_add_count") or 0), 3)
        self.assertEqual(int(metrics.get("unique_cut_count") or 0), 2)
        self.assertEqual(int(metrics.get("selected_count") or 0), 2)

    def test_top_k_protected_excludes_those_cuts(self) -> None:
        cut_candidates = [
            {
                "slot_id": f"S{idx}",
                "card_name": f"Cut Card {idx + 1}",
                "oracle_id": f"ORA_CUT_{idx + 1:03d}",
                "slot_primitives": ["RAMP_MANA"],
                "is_dead_slot": False,
                "contribution_score": float(20 - idx),
                "redundancy_excess_count": 0,
                "negative_impact_score": float(20 - idx),
                "cut_score_v1": float(20 - idx),
            }
            for idx in range(10)
        ]
        expected_protected = {f"Cut Card {idx}" for idx in range(1, 9)}
        expected_eligible = {"Cut Card 9", "Cut Card 10"}

        def _fake_evaluate_swap_pairs(*, cut_candidates, **kwargs):
            cut_names = [
                row.get("card_name")
                for row in cut_candidates
                if isinstance(row, dict) and isinstance(row.get("card_name"), str)
            ]
            self.assertEqual(set(cut_names), expected_eligible)
            self.assertTrue(all(name not in expected_protected for name in cut_names))
            return (
                [
                    self._swap_candidate(
                        cut_name="Cut Card 9",
                        add_name="Opt",
                        cut_oracle_id="ORA_CUT_009",
                        add_oracle_id="ORA_CAN_030",
                        total=1.0,
                        coherence=0.0,
                        coverage_delta=1,
                    )
                ],
                1,
                0.0,
            )

        with (
            patch("api.engine.deck_tune_engine_v1._extract_cut_candidates", return_value=cut_candidates),
            patch(
                "api.engine.deck_tune_engine_v1.get_candidate_pool_v1",
                return_value=[
                    {
                        "name": "Opt",
                        "oracle_id": "ORA_CAN_030",
                        "primitive_ids_v1": ["CARD_DRAW"],
                    }
                ],
            ),
            patch(
                "api.engine.deck_tune_engine_v1._evaluate_swap_pairs",
                side_effect=_fake_evaluate_swap_pairs,
            ),
        ):
            payload = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=2,
            )

        swaps = payload.get("recommended_swaps_v1") if isinstance(payload.get("recommended_swaps_v1"), list) else []
        cut_names = [row.get("cut_name") for row in swaps if isinstance(row, dict)]
        self.assertTrue(all(name in expected_eligible for name in cut_names))
        self.assertTrue(all(name not in expected_protected for name in cut_names))

    def test_protect_top_k_patch_override_applies(self) -> None:
        cut_candidates = [
            {
                "slot_id": "S0",
                "card_name": "High Keep Card",
                "oracle_id": "ORA_KEEP_HIGH",
                "slot_primitives": ["RAMP_MANA"],
                "is_dead_slot": False,
                "contribution_score": 5.0,
                "redundancy_excess_count": 0,
                "negative_impact_score": 10.0,
                "cut_score_v1": 10.0,
            },
            {
                "slot_id": "S1",
                "card_name": "Mid Value Card",
                "oracle_id": "ORA_KEEP_MID",
                "slot_primitives": ["RAMP_MANA"],
                "is_dead_slot": False,
                "contribution_score": 3.0,
                "redundancy_excess_count": 0,
                "negative_impact_score": 9.0,
                "cut_score_v1": 9.0,
            },
            {
                "slot_id": "S2",
                "card_name": "Low Value Card",
                "oracle_id": "ORA_KEEP_LOW",
                "slot_primitives": ["RAMP_MANA"],
                "is_dead_slot": False,
                "contribution_score": 1.0,
                "redundancy_excess_count": 0,
                "negative_impact_score": 8.0,
                "cut_score_v1": 8.0,
            },
        ]
        captured_cut_batches: list[list[str]] = []

        def _fake_evaluate_swap_pairs(*, cut_candidates, **kwargs):
            cut_names = [
                row.get("card_name")
                for row in cut_candidates
                if isinstance(row, dict) and isinstance(row.get("card_name"), str)
            ]
            captured_cut_batches.append(cut_names)
            if len(cut_names) == 0:
                return ([], 0, 0.0)
            return (
                [
                    self._swap_candidate(
                        cut_name=cut_names[0],
                        add_name="Opt",
                        cut_oracle_id="ORA_KEEP_MID",
                        add_oracle_id="ORA_CAN_030",
                        total=1.0,
                        coherence=0.0,
                        coverage_delta=1,
                    )
                ],
                1,
                0.0,
            )

        with (
            patch("api.engine.deck_tune_engine_v1._extract_cut_candidates", return_value=cut_candidates),
            patch(
                "api.engine.deck_tune_engine_v1.get_candidate_pool_v1",
                return_value=[
                    {
                        "name": "Opt",
                        "oracle_id": "ORA_CAN_030",
                        "primitive_ids_v1": ["CARD_DRAW"],
                    }
                ],
            ),
            patch(
                "api.engine.deck_tune_engine_v1._evaluate_swap_pairs",
                side_effect=_fake_evaluate_swap_pairs,
            ),
        ):
            payload_default = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["High Keep Card", "Mid Value Card", "Low Value Card"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=2,
            )
            payload_override = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(
                    cards=["High Keep Card", "Mid Value Card", "Low Value Card"],
                    engine_patches_v0=[{"patch_type": "tune_config_v1", "protect_top_k": 1}],
                ),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=2,
            )

        self.assertGreaterEqual(len(captured_cut_batches), 2)
        self.assertEqual(captured_cut_batches[0], [])
        self.assertEqual(set(captured_cut_batches[1]), {"Mid Value Card", "Low Value Card"})

        swaps_default = (
            payload_default.get("recommended_swaps_v1") if isinstance(payload_default.get("recommended_swaps_v1"), list) else []
        )
        swaps_override = (
            payload_override.get("recommended_swaps_v1")
            if isinstance(payload_override.get("recommended_swaps_v1"), list)
            else []
        )
        self.assertEqual(swaps_default, [])
        self.assertEqual(len(swaps_override), 1)
        self.assertIn(swaps_override[0].get("cut_name"), {"Mid Value Card", "Low Value Card"})

    def test_min_bar_removes_bad_swap(self) -> None:
        with patch(
            "api.engine.deck_tune_engine_v1.get_candidate_pool_v1",
            return_value=[
                {
                    "name": "Island",
                    "oracle_id": "ORA_CAN_100",
                    "primitive_ids_v1": [],
                }
            ],
        ):
            payload = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=5,
                collect_dev_metrics=True,
            )

        swaps = payload.get("recommended_swaps_v1") if isinstance(payload.get("recommended_swaps_v1"), list) else []
        self.assertEqual(swaps, [])
        metrics = payload.get("dev_metrics_v1") if isinstance(payload.get("dev_metrics_v1"), dict) else {}
        self.assertGreaterEqual(int(metrics.get("swaps_filtered_minbar_count") or 0), 1)

    def test_add_omitted_when_only_protected_or_bad_cuts_exist(self) -> None:
        cut_candidates = [
            {
                "slot_id": f"S{idx}",
                "card_name": f"Candidate Cut {idx + 1}",
                "oracle_id": f"ORA_CUT_BAD_{idx + 1:03d}",
                "slot_primitives": [],
                "is_dead_slot": False,
                "contribution_score": float(20 - idx),
                "redundancy_excess_count": 0,
                "negative_impact_score": float(20 - idx),
                "cut_score_v1": float(20 - idx),
            }
            for idx in range(9)
        ]

        with (
            patch("api.engine.deck_tune_engine_v1._extract_cut_candidates", return_value=cut_candidates),
            patch(
                "api.engine.deck_tune_engine_v1.get_candidate_pool_v1",
                return_value=[
                    {
                        "name": "Island",
                        "oracle_id": "ORA_CAN_100",
                        "primitive_ids_v1": [],
                    }
                ],
            ),
        ):
            payload = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=3,
                collect_dev_metrics=True,
            )

        self.assertEqual(payload.get("status"), "WARN")
        swaps = payload.get("recommended_swaps_v1") if isinstance(payload.get("recommended_swaps_v1"), list) else []
        self.assertEqual(swaps, [])
        metrics = payload.get("dev_metrics_v1") if isinstance(payload.get("dev_metrics_v1"), dict) else {}
        self.assertEqual(int(metrics.get("protected_cut_count") or 0), 8)
        self.assertGreaterEqual(int(metrics.get("swaps_filtered_minbar_count") or 0), 1)
        self.assertEqual(int(metrics.get("selected_count") or 0), 0)

    def test_determinism_repeat_for_collapsed_unique_selection(self) -> None:
        mocked_swaps = [
            self._swap_candidate(
                cut_name="Plain Utility",
                add_name="Staff of Completion",
                cut_oracle_id="ORA_CAN_070",
                add_oracle_id="ORA_ADD_STAFF",
                total=2.0,
                coherence=0.2,
                coverage_delta=1,
                missing_required_delta=0,
            ),
            self._swap_candidate(
                cut_name="Mystery Card",
                add_name="Staff of Completion",
                cut_oracle_id="ORA_CAN_060",
                add_oracle_id="ORA_ADD_STAFF",
                total=2.0,
                coherence=0.2,
                coverage_delta=1,
                missing_required_delta=1,
            ),
            self._swap_candidate(
                cut_name="Arcane Signet",
                add_name="Ponder",
                cut_oracle_id="ORA_CAN_020",
                add_oracle_id="ORA_ADD_PONDER",
                total=1.9,
                coherence=0.2,
                coverage_delta=1,
                missing_required_delta=0,
            ),
        ]

        with patch(
            "api.engine.deck_tune_engine_v1._evaluate_swap_pairs",
            return_value=(mocked_swaps, len(mocked_swaps), 0.0),
        ):
            first = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=3,
            )
            second = run_deck_tune_engine_v1(
                canonical_deck_input=self._canonical_input(cards=["Arcane Signet", "Mystery Card", "Plain Utility"]),
                baseline_build_result=self._baseline_build_result(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B3",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                max_swaps=3,
            )

        first_swaps = first.get("recommended_swaps_v1") if isinstance(first.get("recommended_swaps_v1"), list) else []
        second_swaps = second.get("recommended_swaps_v1") if isinstance(second.get("recommended_swaps_v1"), list) else []
        self.assertEqual(first_swaps, second_swaps)


if __name__ == "__main__":
    unittest.main()
