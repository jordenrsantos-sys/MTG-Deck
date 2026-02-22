from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.candidate_pool_v1 import VERSION, get_candidate_pool_v1
from tests.guardrails_fixture_harness import (
    GUARDRAILS_FIXTURE_SNAPSHOT_ID,
    create_guardrails_fixture_db,
    set_guardrails_fixture_env,
)


class CandidatePoolV1Tests(unittest.TestCase):
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

    def test_candidate_pool_is_deterministic_for_same_input(self) -> None:
        kwargs = {
            "db_snapshot_id": GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            "include_primitives": ["RAMP_MANA", "CARD_DRAW"],
            "exclude_card_names": ["Niv-Mizzet, Parun"],
            "commander_color_set": {"U", "R"},
            "bracket_id": "B3",
            "limit": 2000,
        }

        first = get_candidate_pool_v1(**kwargs)
        second = get_candidate_pool_v1(**kwargs)

        self.assertEqual(VERSION, "candidate_pool_v1")
        self.assertEqual(first, second)
        self.assertEqual(
            [row.get("name") for row in first],
            [
                "Hybrid Engine Piece",
                "Arcane Signet",
                "Opt",
                "Rhystic Study",
            ],
        )
        self.assertNotIn("Conjured Practice Token", [row.get("name") for row in first])
        self.assertEqual([row.get("primitive_match_score_v1") for row in first], [2, 1, 1, 1])

    def test_candidate_pool_excludes_illegal_non_deck_object(self) -> None:
        pool = get_candidate_pool_v1(
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            include_primitives=["RAMP_MANA"],
            exclude_card_names=["Niv-Mizzet, Parun"],
            commander_color_set={"U", "R"},
            bracket_id="B3",
            limit=2000,
        )

        names = [row.get("name") for row in pool]
        self.assertIn("Arcane Signet", names)
        self.assertNotIn("Conjured Practice Token", names)

    def test_unknown_identity_cards_are_excluded(self) -> None:
        pool = get_candidate_pool_v1(
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            include_primitives=["RAMP_MANA"],
            exclude_card_names=["Niv-Mizzet, Parun"],
            commander_color_set={"U", "R"},
            bracket_id="B3",
            limit=2000,
        )

        names = [row.get("name") for row in pool]
        self.assertNotIn("Mystery Card", names)
        self.assertNotIn("Cultivate", names)

    def test_candidate_pool_ordering_matches_contract(self) -> None:
        pool = get_candidate_pool_v1(
            db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
            include_primitives=["RAMP_MANA", "CARD_DRAW"],
            exclude_card_names=["Niv-Mizzet, Parun"],
            commander_color_set={"U", "R"},
            bracket_id="B3",
            limit=2000,
        )

        ordered = sorted(
            pool,
            key=lambda row: (
                -int(row.get("primitive_match_score_v1", 0)),
                str(row.get("oracle_id") or ""),
                str(row.get("name") or ""),
            ),
        )
        self.assertEqual(pool, ordered)

    def test_candidate_pool_enforces_gc_limits(self) -> None:
        with patch("api.engine.candidate_pool_v1.GAME_CHANGERS_SET", {"Arcane Signet"}):
            pool = get_candidate_pool_v1(
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                include_primitives=["RAMP_MANA"],
                exclude_card_names=["Niv-Mizzet, Parun"],
                commander_color_set={"U", "R"},
                bracket_id="B2",
                limit=2000,
            )

        names = [row.get("name") for row in pool]
        self.assertNotIn("Arcane Signet", names)

    def test_dev_metrics_breakdown_is_reported_when_enabled(self) -> None:
        breakdown: dict[str, object] = {}
        with patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "1"}, clear=False):
            pool = get_candidate_pool_v1(
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                include_primitives=["RAMP_MANA", "CARD_DRAW"],
                exclude_card_names=["Niv-Mizzet, Parun"],
                commander_color_set={"U", "R"},
                bracket_id="B3",
                limit=3,
                dev_metrics_out=breakdown,
            )

        self.assertGreaterEqual(len(pool), 1)
        self.assertIn("sql_query_ms", breakdown)
        self.assertIn("python_filter_ms", breakdown)
        self.assertIn("color_check_ms", breakdown)
        self.assertIn("gc_check_ms", breakdown)
        self.assertIn("total_candidates_seen", breakdown)
        self.assertEqual(breakdown.get("total_candidates_returned"), len(pool))
        self.assertTrue(bool(breakdown.get("legality_filter_available_v1")))
        self.assertGreater(int(breakdown.get("filtered_illegal_count_v1") or 0), 0)
        examples = breakdown.get("filtered_illegal_examples_top5_v1")
        self.assertIsInstance(examples, list)
        self.assertIn("Conjured Practice Token", examples)

    def test_filter_unavailable_does_not_guess_or_filter(self) -> None:
        breakdown: dict[str, object] = {}
        with patch(
            "api.engine.candidate_pool_v1.list_cards_table_columns",
            return_value=["snapshot_id", "oracle_id", "name", "color_identity", "primitives_json"],
        ), patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "1"}, clear=False):
            pool = get_candidate_pool_v1(
                db_snapshot_id=GUARDRAILS_FIXTURE_SNAPSHOT_ID,
                include_primitives=["RAMP_MANA"],
                exclude_card_names=["Niv-Mizzet, Parun"],
                commander_color_set={"U", "R"},
                bracket_id="B3",
                limit=2000,
                dev_metrics_out=breakdown,
            )

        names = [row.get("name") for row in pool]
        self.assertIn("Conjured Practice Token", names)
        self.assertFalse(bool(breakdown.get("legality_filter_available_v1")))
        self.assertEqual(int(breakdown.get("filtered_illegal_count_v1") or 0), 0)


if __name__ == "__main__":
    unittest.main()
