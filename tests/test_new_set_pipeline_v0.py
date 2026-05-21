"""Phase 13 tests — Track 5 new-set pipeline scaffolding.

Runs the pipeline against a small Scryfall-shaped fixture and verifies:
  - Orchestrator runs all 5 steps without errors.
  - Each step records a status string in per_step_status.
  - Step 5 (flag_potential_combo_pairs) correctly flags cards whose
    oracle_text matches combo-relevant heuristics.
  - Empty input / missing 'cards' key returns a sensible result with
    warnings.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from new_set_pipeline_v0 import (  # type: ignore
    flag_potential_combo_pairs,
    ingest_new_cards,
    score_for_themes,
    tag_with_primitives,
    update_embedding_index,
)


def _fixture_cards() -> list:
    """5-card fixture covering common new-set shapes."""
    return [
        {
            "oracle_id": "fx-001-vampire",
            "name": "Test Vampire",
            "mana_cost": "{2}{B}",
            "cmc": 3,
            "type_line": "Creature — Vampire",
            "oracle_text": "When Test Vampire enters, each opponent loses 2 life and you gain 2 life. Lifelink.",
            "colors": ["B"],
            "color_identity": ["B"],
            "released_at": "2026-06-01",
        },
        {
            "oracle_id": "fx-002-sorcery",
            "name": "Test Tutor",
            "mana_cost": "{1}{B}",
            "cmc": 2,
            "type_line": "Sorcery",
            "oracle_text": "Search your library for a creature card, reveal it, put it into your hand, then shuffle.",
            "colors": ["B"],
            "color_identity": ["B"],
            "released_at": "2026-06-01",
        },
        {
            "oracle_id": "fx-003-artifact",
            "name": "Test Mana Rock",
            "mana_cost": "{2}",
            "cmc": 2,
            "type_line": "Artifact",
            "oracle_text": "Tap: add {B} or {R}.",
            "colors": [],
            "color_identity": ["B", "R"],
            "released_at": "2026-06-01",
        },
        {
            "oracle_id": "fx-004-enchantment",
            "name": "Test Sac Outlet",
            "mana_cost": "{1}{B}",
            "cmc": 2,
            "type_line": "Enchantment",
            "oracle_text": "{T}: sacrifice a creature you control. Each opponent loses 1 life.",
            "colors": ["B"],
            "color_identity": ["B"],
            "released_at": "2026-06-01",
        },
        {
            "oracle_id": "fx-005-land",
            "name": "Test Utility Land",
            "mana_cost": "",
            "cmc": 0,
            "type_line": "Land",
            "oracle_text": "Tap: add one mana of any color.",
            "colors": [],
            "color_identity": [],
            "released_at": "2026-06-01",
        },
    ]


class OrchestratorTests(unittest.TestCase):
    def test_full_pipeline_runs_without_errors(self) -> None:
        result = ingest_new_cards({"cards": _fixture_cards()})
        self.assertEqual(result.new_card_count, 5)
        self.assertEqual(result.tagged_count, 5)
        self.assertEqual(result.theme_scored_count, 5)
        # corpus rows = 0 because no snapshot id (dry-run).
        self.assertEqual(result.corpus_rows_written, 0)
        self.assertEqual(result.embeddings_added, 0)
        # All 5 step statuses present.
        for step in [
            "tag_with_primitives",
            "score_for_themes",
            "update_corpus_metadata",
            "update_embedding_index",
            "flag_potential_combo_pairs",
        ]:
            self.assertIn(step, result.per_step_status)

    def test_dry_run_status_message(self) -> None:
        result = ingest_new_cards({"cards": _fixture_cards()})
        self.assertIn("DRY-RUN", result.per_step_status["update_corpus_metadata"])

    def test_empty_input_returns_warning(self) -> None:
        result = ingest_new_cards({})
        self.assertEqual(result.new_card_count, 0)
        self.assertTrue(result.warnings, "Empty input should warn.")

    def test_missing_cards_key_returns_warning(self) -> None:
        result = ingest_new_cards({"other": "field"})
        self.assertTrue(result.warnings)


class CombosFlaggingTests(unittest.TestCase):
    def test_combo_relevant_text_gets_flagged(self) -> None:
        # Sac outlet matches "sacrifice"; mana rock matches "add {".
        # Vampire matches "lifelink"; tutor doesn't match any of the
        # heuristic phrases.
        flags = flag_potential_combo_pairs(_fixture_cards())
        names = {f["card_name"] for f in flags}
        self.assertIn("Test Sac Outlet", names)
        self.assertIn("Test Mana Rock", names)
        self.assertIn("Test Vampire", names)
        # Tutor: "Search your library..." — none of the heuristic
        # phrases match. Should NOT be flagged.
        self.assertNotIn("Test Tutor", names)

    def test_flag_records_reason_and_partners(self) -> None:
        flags = flag_potential_combo_pairs(_fixture_cards())
        for f in flags:
            self.assertIn("reason", f)
            self.assertIn("candidate_partners", f)
            # iter 3: candidate_partners is always empty.
            self.assertEqual(f["candidate_partners"], [])


class StepStubsTests(unittest.TestCase):
    def test_tag_with_primitives_returns_empty_lists_iter3(self) -> None:
        result = tag_with_primitives(_fixture_cards())
        self.assertEqual(len(result), 5)
        for name, tags in result.items():
            self.assertEqual(tags, [])  # iter 3 stub

    def test_score_for_themes_returns_empty_iter3(self) -> None:
        result = score_for_themes(_fixture_cards())
        self.assertEqual(len(result), 5)

    def test_update_embedding_index_returns_zero_iter3(self) -> None:
        self.assertEqual(update_embedding_index(_fixture_cards()), 0)


if __name__ == "__main__":
    unittest.main()
