"""Iter 5 Phase 10 — mana-cost-aware Voyage downgrade pass tests."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from api.engine.layers import agent_voyage_downgrade_pass_v1 as dp


class ShouldRunDowngradePassTests(unittest.TestCase):
    def test_b4_always_runs(self) -> None:
        self.assertTrue(dp.should_run_downgrade_pass("B4"))

    def test_b5_always_runs(self) -> None:
        self.assertTrue(dp.should_run_downgrade_pass("B5"))

    def test_b3_runs_when_storm_theme(self) -> None:
        profile = {
            "primary": {"theme": "storm", "weight": 0.6},
            "secondary": {"theme": "value_engine", "weight": 0.4},
        }
        self.assertTrue(dp.should_run_downgrade_pass("B3", profile))

    def test_b3_does_not_run_for_casual_themes(self) -> None:
        profile = {
            "primary": {"theme": "tribal", "weight": 1.0},
        }
        self.assertFalse(dp.should_run_downgrade_pass("B3", profile))

    def test_b2_does_not_run_with_no_relevant_theme(self) -> None:
        self.assertFalse(dp.should_run_downgrade_pass("B2"))


class FindCheaperAlternativesTests(unittest.TestCase):
    def test_returns_empty_when_no_cmc(self) -> None:
        self.assertEqual(
            dp.find_cheaper_alternatives("Some Card", None), [],
        )

    def test_returns_empty_when_voyage_unavailable(self) -> None:
        with patch("api.engine.layers.agent_semantic_retrieval_v1.is_available",
                   return_value=False):
            result = dp.find_cheaper_alternatives("Card", 3.0)
        self.assertEqual(result, [])

    def test_filters_by_cmc_below_anchor(self) -> None:
        neighbors = [
            {"name": "Cheap A", "cmc": 1.0, "color_identity": ["B"],
             "similarity": 0.85},
            {"name": "Same B", "cmc": 3.0, "color_identity": ["B"],
             "similarity": 0.80},
            {"name": "Pricey C", "cmc": 5.0, "color_identity": ["B"],
             "similarity": 0.75},
            {"name": "Cheap D", "cmc": 2.0, "color_identity": ["B"],
             "similarity": 0.70},
        ]
        with patch("api.engine.layers.agent_semantic_retrieval_v1.is_available",
                   return_value=True), \
             patch("api.engine.layers.agent_semantic_retrieval_v1.query_neighbors",
                   return_value=neighbors):
            result = dp.find_cheaper_alternatives(
                "Murder", anchor_cmc=3.0, color_identity=["B"],
            )
        # Only Cheap A + Cheap D pass (cmc < 3).
        names = [r["name"] for r in result]
        self.assertEqual(set(names), {"Cheap A", "Cheap D"})
        # Sorted by similarity descending.
        self.assertEqual(result[0]["name"], "Cheap A")

    def test_includes_savings_field(self) -> None:
        neighbors = [
            {"name": "Cheaper", "cmc": 1.0, "color_identity": ["G"],
             "similarity": 0.9},
        ]
        with patch("api.engine.layers.agent_semantic_retrieval_v1.is_available",
                   return_value=True), \
             patch("api.engine.layers.agent_semantic_retrieval_v1.query_neighbors",
                   return_value=neighbors):
            result = dp.find_cheaper_alternatives(
                "Cultivate", anchor_cmc=3.0, color_identity=["G"],
            )
        self.assertEqual(result[0]["savings"], 2.0)


class RunDowngradePassForDeckTests(unittest.TestCase):
    def test_iterates_anchors_and_collects(self) -> None:
        with patch.object(dp, "find_cheaper_alternatives") as mock_find:
            mock_find.side_effect = [
                [{"name": "X", "cmc": 1.0, "similarity": 0.9}],  # for Anchor1
                [],                                                # Anchor2
                [{"name": "Y", "cmc": 2.0, "similarity": 0.8}],  # Anchor3
            ]
            result = dp.run_downgrade_pass_for_deck(
                anchor_names=["Anchor1", "Anchor2", "Anchor3"],
                deck_cards_with_cmc={"Anchor1": 3.0, "Anchor2": 2.0, "Anchor3": 4.0},
            )
        # Only anchors with non-empty alternatives surface.
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["anchor"], "Anchor1")
        self.assertEqual(result[1]["anchor"], "Anchor3")


if __name__ == "__main__":
    unittest.main()
