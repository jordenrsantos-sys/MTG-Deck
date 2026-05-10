"""Tests for tools/playtest/opposition_pool.py (Phase 5b stage 5b.2)."""
from __future__ import annotations

import unittest
from typing import Any, Dict, List

from tools.playtest import opposition_pool


def _make_deck(*, deck_id: str, engine_bracket: str, self_bracket: str = None) -> Dict[str, Any]:
    return {
        "deck_id": deck_id,
        "engine_assigned_bracket": engine_bracket,
        "self_reported_bracket": self_bracket if self_bracket is not None else engine_bracket,
        "commander_oracle_ids": [],
        "deck_oracle_ids": [],
    }


def _make_corpus(decks: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {"version": "external_decks_v1", "pack_id": "external_decks_v1", "decks": decks}


class OppositionPoolBracketPriorityTests(unittest.TestCase):
    def test_priority_order_b3_spreads_outward(self) -> None:
        order = opposition_pool._bracket_priority_order("B3")
        self.assertEqual(order[0], "B3")
        self.assertIn("B2", order)
        self.assertIn("B4", order)

    def test_priority_order_b1_walks_up_only(self) -> None:
        order = opposition_pool._bracket_priority_order("B1")
        self.assertEqual(order[0], "B1")
        self.assertEqual(order[1], "B2")

    def test_priority_order_b5_walks_down_only(self) -> None:
        order = opposition_pool._bracket_priority_order("B5")
        self.assertEqual(order[0], "B5")
        self.assertEqual(order[1], "B4")

    def test_priority_order_unknown_bracket_returns_default(self) -> None:
        order = opposition_pool._bracket_priority_order("BX")
        self.assertEqual(order, list(opposition_pool.BRACKET_ORDER))


class OppositionPoolSelectionTests(unittest.TestCase):
    def test_select_returns_three_decks_at_target_bracket_when_pool_has_four(self) -> None:
        corpus = _make_corpus([
            _make_deck(deck_id="TEST", engine_bracket="B3"),
            _make_deck(deck_id="OPP_A", engine_bracket="B3"),
            _make_deck(deck_id="OPP_B", engine_bracket="B3"),
            _make_deck(deck_id="OPP_C", engine_bracket="B3"),
        ])
        result = opposition_pool.select_opposition(
            target_bracket="B3", exclude_deck_id="TEST", corpus=corpus, seed=0,
        )
        self.assertEqual(len(result["opposition_deck_ids"]), 3)
        self.assertEqual(set(result["opposition_deck_ids"]), {"OPP_A", "OPP_B", "OPP_C"})
        self.assertNotIn("TEST", result["opposition_deck_ids"])
        self.assertFalse(result["fallback_used"])

    def test_collision_detected_when_only_three_total_at_target_bracket(self) -> None:
        # B5 collision: 3 total decks at B5; test deck IS one of them.
        corpus = _make_corpus([
            _make_deck(deck_id="B5_TEST", engine_bracket="B5"),
            _make_deck(deck_id="B5_OPP_A", engine_bracket="B5"),
            _make_deck(deck_id="B5_OPP_B", engine_bracket="B5"),
            _make_deck(deck_id="B4_FALLBACK_A", engine_bracket="B4"),
        ])
        result = opposition_pool.select_opposition(
            target_bracket="B5", exclude_deck_id="B5_TEST", corpus=corpus, seed=0,
        )
        self.assertTrue(result["collision_detected"])
        self.assertEqual(len(result["opposition_deck_ids"]), 3)
        # Should pull 2 from B5 + 1 from B4 fallback.
        self.assertTrue(result["fallback_used"])

    def test_deterministic_selection_for_fixed_seed(self) -> None:
        corpus = _make_corpus([
            _make_deck(deck_id=f"D{i}", engine_bracket="B2") for i in range(10)
        ])
        r1 = opposition_pool.select_opposition(
            target_bracket="B2", exclude_deck_id="D0", corpus=corpus, seed=42,
        )
        r2 = opposition_pool.select_opposition(
            target_bracket="B2", exclude_deck_id="D0", corpus=corpus, seed=42,
        )
        self.assertEqual(r1["opposition_deck_ids"], r2["opposition_deck_ids"])

    def test_different_seeds_produce_different_selections(self) -> None:
        corpus = _make_corpus([
            _make_deck(deck_id=f"D{i}", engine_bracket="B2") for i in range(20)
        ])
        r1 = opposition_pool.select_opposition(
            target_bracket="B2", exclude_deck_id="D0", corpus=corpus, seed=1,
        )
        r2 = opposition_pool.select_opposition(
            target_bracket="B2", exclude_deck_id="D0", corpus=corpus, seed=2,
        )
        # Different seeds: at least one selection should differ.
        self.assertNotEqual(set(r1["opposition_deck_ids"]), set(r2["opposition_deck_ids"]))

    def test_bracket_resolution_falls_back_to_self_reported(self) -> None:
        # Engine-assigned not set; self-reported is.
        deck = {
            "deck_id": "X",
            "engine_assigned_bracket": None,
            "self_reported_bracket": "B4",
        }
        self.assertEqual(opposition_pool._bracket_for_deck(deck), "B4")

    def test_bracket_resolution_returns_none_when_both_missing(self) -> None:
        deck = {"deck_id": "X"}
        self.assertIsNone(opposition_pool._bracket_for_deck(deck))


class OppositionPoolLiveCorpusTests(unittest.TestCase):
    """Live-corpus sanity checks against the post-5a.6.2 26-deck pack."""

    def test_live_corpus_loads(self) -> None:
        corpus = opposition_pool.load_corpus()
        self.assertEqual(corpus.get("pack_id"), "external_decks_v1")
        self.assertEqual(len(corpus.get("decks", [])), 26)

    def test_live_corpus_b5_pod_does_not_collide_due_to_heliod_overshoot(self) -> None:
        # Surprise finding (post-5a.6.2): live corpus has 4 engine_assigned=B5
        # decks (Terra Bytes + Doctor's Orders + Varis Power Ranger + Heliod's
        # over-shoot from self=B4 via 5a.5.2's gc_min=6 floor). Excluding Terra
        # Bytes leaves exactly pod_size=3 B5 opponents → pool fills cleanly,
        # NO collision, NO fallback. Safety #7 does NOT fire on current corpus.
        corpus = opposition_pool.load_corpus()
        result = opposition_pool.select_opposition(
            target_bracket="B5",
            exclude_deck_id="ARCHIDEKT_5662629",  # Terra Bytes
            corpus=corpus,
            seed=0,
        )
        self.assertFalse(result["collision_detected"])
        self.assertFalse(result["fallback_used"])
        self.assertEqual(len(result["opposition_deck_ids"]), 3)

    def test_live_corpus_b3_pod_collides_only_two_decks(self) -> None:
        # Post-5a.6.2: B3 has only 2 engine_assigned decks (Sergeant Benton +
        # Terra Every Summon over-shoot). Any test in B3 (or excluding one)
        # leaves <=2 candidates → collision fires + B2/B4 fallback fills.
        corpus = opposition_pool.load_corpus()
        result = opposition_pool.select_opposition(
            target_bracket="B3",
            exclude_deck_id="ARCHIDEKT_5969200",  # Benton
            corpus=corpus,
            seed=0,
        )
        # Total B3 = 2; collision_detected fires when target_bracket_total <= pod_size.
        self.assertTrue(result["collision_detected"])
        self.assertTrue(result["fallback_used"])
        self.assertEqual(len(result["opposition_deck_ids"]), 3)

    def test_live_corpus_deck_count_by_bracket(self) -> None:
        counts = opposition_pool.deck_count_by_bracket()
        # Per post-5a.6.2 distribution: 0xB1, 17xB2, 2xB3, 3xB4, 4xB5 = 26
        # (Heliod over-shoots from self=B4 to engine=B5 per 5a.5.2's gc_min=6.)
        self.assertEqual(sum(counts.values()), 26)
        self.assertEqual(counts["B5"], 4)
        self.assertEqual(counts["B4"], 3)
        self.assertEqual(counts["B3"], 2)
        self.assertEqual(counts["B2"], 17)
        self.assertEqual(counts["B1"], 0)


if __name__ == "__main__":
    unittest.main()
