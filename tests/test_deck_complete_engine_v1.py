from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.deck_complete_engine_v1 import VERSION, run_deck_complete_engine_v1
from tests.guardrails_fixture_harness import (
    GUARDRAILS_FIXTURE_SNAPSHOT_ID,
    create_guardrails_fixture_db,
    set_guardrails_fixture_env,
)


class DeckCompleteEngineV1Tests(unittest.TestCase):
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

    def _baseline_payload(self) -> dict:
        return {
            "status": "OK",
            "deck_size_total": 3,
            "result": {
                "structural_snapshot_v1": {
                    "primitive_counts_by_id": {
                        "RAMP_MANA": 1,
                        "CARD_DRAW": 0,
                        "REMOVAL_SINGLE": 0,
                        "PROTECTION": 0,
                    },
                    "missing_primitives_v1": ["CARD_DRAW", "REMOVAL_SINGLE", "PROTECTION"],
                    "dead_slot_ids_v1": [],
                    "primitive_concentration_index_v1": 0.5,
                },
                "profile_bracket_enforcement_v1": {
                    "counts": {
                        "game_changers_in_deck": 0,
                    }
                },
                "engine_coherence_v1": {
                    "metrics": {
                        "overlap_score": 0.0,
                    }
                },
                "redundancy_index_v1": {
                    "per_requirement": [
                        {
                            "primitive": "CARD_DRAW",
                            "supported": True,
                            "redundancy_level": "LOW",
                        }
                    ]
                },
                "commander_reliability_model_v1": {
                    "commander_dependent": True,
                    "metrics": {
                        "protection_coverage_proxy": 0.0,
                    },
                },
            },
        }

    def _canonical_payload(self, *, cards: list[str]) -> dict:
        return {
            "db_snapshot_id": GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            "profile_id": "focused",
            "bracket_id": "B2",
            "format": "commander",
            "commander": "Niv-Mizzet, Parun",
            "cards": list(cards),
            "engine_patches_v0": [],
        }

    def test_completes_to_100_deterministically(self) -> None:
        kwargs = {
            "canonical_deck_input": self._canonical_payload(cards=["Arcane Signet", "Opt", "Rhystic Study"]),
            "baseline_build_result": self._baseline_payload(),
            "db_snapshot_id": GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            "bracket_id": "B2",
            "profile_id": "focused",
            "mulligan_model_id": "NORMAL",
            "target_deck_size": 100,
            "max_adds": 200,
            "allow_basic_lands": True,
            "land_target_mode": "AUTO",
        }

        first = run_deck_complete_engine_v1(**kwargs)
        second = run_deck_complete_engine_v1(**kwargs)

        self.assertEqual(VERSION, "deck_complete_engine_v1")
        self.assertEqual(first, second)
        self.assertEqual(
            first.get("completed_decklist_text_v1"),
            second.get("completed_decklist_text_v1"),
        )
        self.assertEqual(first.get("status"), "OK")

        decklist_lines = (
            first.get("completed_decklist_text_v1").splitlines()
            if isinstance(first.get("completed_decklist_text_v1"), str)
            else []
        )
        self.assertIn("Deck", decklist_lines)
        deck_start = decklist_lines.index("Deck") + 1
        deck_rows = decklist_lines[deck_start:]
        self.assertEqual(len(deck_rows), 99)

        added_rows = first.get("added_cards_v1") if isinstance(first.get("added_cards_v1"), list) else []
        land_rows = [
            row
            for row in added_rows
            if isinstance(row, dict)
            and "ADD_BASIC_LAND_FILL_AUTO" in (
                row.get("reasons_v1") if isinstance(row.get("reasons_v1"), list) else []
            )
        ]
        self.assertGreater(len(land_rows), 0)

    def test_respects_color_identity(self) -> None:
        out = run_deck_complete_engine_v1(
            canonical_deck_input=self._canonical_payload(cards=["Arcane Signet"]),
            baseline_build_result=self._baseline_payload(),
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B2",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            target_deck_size=20,
            max_adds=30,
            allow_basic_lands=True,
            land_target_mode="AUTO",
        )

        names = [row.get("name") for row in (out.get("added_cards_v1") if isinstance(out.get("added_cards_v1"), list) else [])]
        self.assertNotIn("Cultivate", names)

    def test_respects_gc_limits(self) -> None:
        with patch("api.engine.candidate_pool_v1.GAME_CHANGERS_SET", {"Hybrid Engine Piece"}):
            out = run_deck_complete_engine_v1(
                canonical_deck_input=self._canonical_payload(cards=["Arcane Signet"]),
                baseline_build_result=self._baseline_payload(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B2",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                target_deck_size=8,
                max_adds=10,
                allow_basic_lands=False,
                land_target_mode="AUTO",
            )

        names = [row.get("name") for row in (out.get("added_cards_v1") if isinstance(out.get("added_cards_v1"), list) else [])]
        self.assertNotIn("Hybrid Engine Piece", names)

    def test_land_fill_uses_deterministic_ur_distribution(self) -> None:
        baseline_payload = {
            "status": "OK",
            "deck_size_total": 3,
            "result": {
                "structural_snapshot_v1": {
                    "primitive_counts_by_id": {
                        "REMOVAL_SINGLE": 9,
                        "BOARD_WIPE": 3,
                        "PROTECTION": 4,
                    },
                    "missing_primitives_v1": [],
                    "dead_slot_ids_v1": [],
                    "primitive_concentration_index_v1": 0.2,
                },
                "profile_bracket_enforcement_v1": {"counts": {"game_changers_in_deck": 0}},
                "engine_coherence_v1": {"metrics": {"overlap_score": 0.0}},
                "redundancy_index_v1": {"per_requirement": []},
                "commander_reliability_model_v1": {
                    "commander_dependent": False,
                    "metrics": {"protection_coverage_proxy": 0.5},
                },
            },
        }

        out = run_deck_complete_engine_v1(
            canonical_deck_input=self._canonical_payload(cards=["Arcane Signet", "Snow-Covered Island"]),
            baseline_build_result=baseline_payload,
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B2",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            target_deck_size=12,
            max_adds=20,
            allow_basic_lands=True,
            land_target_mode="AUTO",
        )

        added_rows = out.get("added_cards_v1") if isinstance(out.get("added_cards_v1"), list) else []
        added_names = [row.get("name") for row in added_rows if isinstance(row, dict)]
        self.assertEqual(added_names.count("Island"), 5)
        self.assertEqual(added_names.count("Mountain"), 4)
        self.assertNotIn("Snow-Covered Island", added_names)
        self.assertNotIn("Snow-Covered Mountain", added_names)

    def test_mono_red_land_fill_uses_only_mountains(self) -> None:
        baseline_payload = {
            "status": "OK",
            "deck_size_total": 4,
            "result": {
                "structural_snapshot_v1": {
                    "primitive_counts_by_id": {
                        "REMOVAL_SINGLE": 9,
                        "BOARD_WIPE": 3,
                        "PROTECTION": 4,
                    },
                    "missing_primitives_v1": [],
                    "dead_slot_ids_v1": [],
                    "primitive_concentration_index_v1": 0.2,
                },
                "profile_bracket_enforcement_v1": {"counts": {"game_changers_in_deck": 0}},
                "engine_coherence_v1": {"metrics": {"overlap_score": 0.0}},
                "redundancy_index_v1": {"per_requirement": []},
                "commander_reliability_model_v1": {
                    "commander_dependent": False,
                    "metrics": {"protection_coverage_proxy": 0.5},
                },
            },
        }

        with patch("api.engine.deck_complete_engine_v1.get_commander_color_identity_union_v1", return_value={"R"}):
            out = run_deck_complete_engine_v1(
                canonical_deck_input=self._canonical_payload(cards=["Arcane Signet", "Opt", "Rhystic Study"]),
                baseline_build_result=baseline_payload,
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B2",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                target_deck_size=12,
                max_adds=20,
                allow_basic_lands=True,
                land_target_mode="AUTO",
            )

        added_rows = out.get("added_cards_v1") if isinstance(out.get("added_cards_v1"), list) else []
        land_names = [row.get("name") for row in added_rows if isinstance(row, dict)]
        self.assertGreater(len(land_names), 0)
        self.assertEqual(set(land_names), {"Mountain"})

    def test_candidate_pool_empty_still_fills_basics_to_target_when_allowed(self) -> None:
        with patch("api.engine.deck_complete_engine_v1.get_candidate_pool_v1", return_value=[]):
            out = run_deck_complete_engine_v1(
                canonical_deck_input=self._canonical_payload(cards=["Arcane Signet"]),
                baseline_build_result=self._baseline_payload(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B2",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                target_deck_size=20,
                max_adds=200,
                allow_basic_lands=True,
                land_target_mode="AUTO",
            )

        self.assertEqual(out.get("status"), "OK")
        decklist_lines = (
            out.get("completed_decklist_text_v1").splitlines()
            if isinstance(out.get("completed_decklist_text_v1"), str)
            else []
        )
        self.assertIn("Deck", decklist_lines)
        deck_rows = decklist_lines[decklist_lines.index("Deck") + 1 :]
        self.assertEqual(1 + len(deck_rows), 20)

    def test_warns_with_reason_when_basic_lands_disallowed_and_target_unreached(self) -> None:
        with patch("api.engine.deck_complete_engine_v1.get_candidate_pool_v1", return_value=[]):
            out = run_deck_complete_engine_v1(
                canonical_deck_input=self._canonical_payload(cards=["Arcane Signet"]),
                baseline_build_result=self._baseline_payload(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B2",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                target_deck_size=20,
                max_adds=200,
                allow_basic_lands=False,
                land_target_mode="AUTO",
            )

        self.assertEqual(out.get("status"), "WARN")
        codes = out.get("codes") if isinstance(out.get("codes"), list) else []
        self.assertIn("BASIC_LANDS_DISALLOWED", codes)
        self.assertIn("CANDIDATE_POOL_EMPTY", codes)
        self.assertIn("TARGET_SIZE_NOT_REACHED", codes)

    def test_warns_with_reason_when_max_adds_reached_before_target(self) -> None:
        mocked_candidates = [
            {
                "oracle_id": f"ORA_FAKE_{idx:03d}",
                "name": f"Filler Card {idx:03d}",
                "primitive_ids_v1": ["CARD_DRAW", "REMOVAL_SINGLE", "PROTECTION"],
                "primitive_match_score_v1": 3,
                "is_game_changer_v1": False,
            }
            for idx in range(1, 20)
        ]
        with patch("api.engine.deck_complete_engine_v1.get_candidate_pool_v1", return_value=mocked_candidates):
            out = run_deck_complete_engine_v1(
                canonical_deck_input=self._canonical_payload(cards=["Arcane Signet"]),
                baseline_build_result=self._baseline_payload(),
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                bracket_id="B2",
                profile_id="focused",
                mulligan_model_id="NORMAL",
                target_deck_size=20,
                max_adds=3,
                allow_basic_lands=False,
                land_target_mode="AUTO",
            )

        self.assertEqual(out.get("status"), "WARN")
        codes = out.get("codes") if isinstance(out.get("codes"), list) else []
        self.assertIn("MAX_ADDS_REACHED_BEFORE_TARGET", codes)
        self.assertIn("TARGET_SIZE_NOT_REACHED", codes)

    def test_partner_commander_union_color_identity_fills_to_target(self) -> None:
        baseline_payload = {
            "status": "OK",
            "deck_size_total": 3,
            "result": {
                "structural_snapshot_v1": {
                    "primitive_counts_by_id": {
                        "REMOVAL_SINGLE": 9,
                        "BOARD_WIPE": 3,
                        "PROTECTION": 4,
                    },
                    "missing_primitives_v1": [],
                    "dead_slot_ids_v1": [],
                    "primitive_concentration_index_v1": 0.2,
                },
                "profile_bracket_enforcement_v1": {"counts": {"game_changers_in_deck": 0}},
                "engine_coherence_v1": {"metrics": {"overlap_score": 0.0}},
                "redundancy_index_v1": {"per_requirement": []},
                "commander_reliability_model_v1": {
                    "commander_dependent": False,
                    "metrics": {"protection_coverage_proxy": 0.5},
                },
            },
        }

        canonical = self._canonical_payload(cards=["Arcane Signet"])
        canonical["commander"] = "Esior, Wardwing Familiar"
        canonical["commander_list_v1"] = ["Esior, Wardwing Familiar", "Ishai, Ojutai Dragonspeaker"]

        out = run_deck_complete_engine_v1(
            canonical_deck_input=canonical,
            baseline_build_result=baseline_payload,
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            bracket_id="B2",
            profile_id="focused",
            mulligan_model_id="NORMAL",
            target_deck_size=100,
            max_adds=200,
            allow_basic_lands=True,
            land_target_mode="AUTO",
        )

        self.assertEqual(out.get("status"), "OK")
        decklist_text = out.get("completed_decklist_text_v1") if isinstance(out.get("completed_decklist_text_v1"), str) else ""
        self.assertIn("1 Esior, Wardwing Familiar", decklist_text)
        self.assertIn("1 Ishai, Ojutai Dragonspeaker", decklist_text)
        added_rows = out.get("added_cards_v1") if isinstance(out.get("added_cards_v1"), list) else []
        added_names = [row.get("name") for row in added_rows if isinstance(row, dict)]
        self.assertEqual(added_names.count("Plains"), 49)
        self.assertEqual(added_names.count("Island"), 48)


if __name__ == "__main__":
    unittest.main()
