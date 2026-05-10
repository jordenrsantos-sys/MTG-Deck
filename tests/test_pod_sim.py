"""Tests for tools/playtest/pod_sim.py (Phase 5b stage 5b.6)."""
from __future__ import annotations

import unittest

from tools.playtest import opposition_pool, pilot, pod_sim


class PodSimSmokeTests(unittest.TestCase):
    def test_run_pod_game_against_live_corpus_completes(self) -> None:
        # Pick a B2 test deck (largest pool — 17 decks); should never collide.
        test_deck_id = "ARCHIDEKT_4593151"  # Octavia
        record = pod_sim.run_pod_game(
            test_deck_id=test_deck_id,
            target_bracket="B2",
            test_personality=pilot.get_preset("casual"),
            seed=42,
        )
        self.assertEqual(record["test_deck_id"], test_deck_id)
        self.assertEqual(record["target_bracket"], "B2")
        self.assertIn("opposition_pool_summary", record)
        self.assertEqual(len(record["opposition_pool_summary"]["opposition_deck_ids"]), 3)
        self.assertNotIn(test_deck_id, record["opposition_pool_summary"]["opposition_deck_ids"])
        self.assertIn("winner_is_test_deck", record)

    def test_run_pod_game_b3_target_triggers_fallback(self) -> None:
        # B3 has only 2 decks in live corpus; opposition pool falls back to
        # nearest neighbors (B2 / B4).
        record = pod_sim.run_pod_game(
            test_deck_id="ARCHIDEKT_4221386",  # Sharuum (engine=B2 actually)
            target_bracket="B3",
            test_personality=pilot.get_preset("casual"),
            seed=0,
        )
        # 2 decks at B3 + need 3 -> fallback fires.
        self.assertTrue(record["opposition_pool_summary"]["fallback_used"])
        self.assertTrue(record["opposition_pool_summary"]["collision_detected"])
        self.assertEqual(len(record["opposition_pool_summary"]["opposition_deck_ids"]), 3)


class PodSimDeterminismTests(unittest.TestCase):
    def test_same_seed_produces_identical_pod_record(self) -> None:
        kwargs = {
            "test_deck_id": "ARCHIDEKT_4593151",
            "target_bracket": "B2",
            "test_personality": pilot.get_preset("shark"),
            "seed": 7,
        }
        r1 = pod_sim.run_pod_game(**kwargs)
        r2 = pod_sim.run_pod_game(**kwargs)
        self.assertEqual(r1["initial_state_hash"], r2["initial_state_hash"])
        self.assertEqual(r1["final_state_hash"], r2["final_state_hash"])
        self.assertEqual(r1["winner_player_id"], r2["winner_player_id"])
        self.assertEqual(
            r1["opposition_pool_summary"]["opposition_deck_ids"],
            r2["opposition_pool_summary"]["opposition_deck_ids"],
        )


class PodSimErrorTests(unittest.TestCase):
    def test_unknown_test_deck_raises(self) -> None:
        with self.assertRaises(KeyError):
            pod_sim.run_pod_game(
                test_deck_id="NOT_IN_CORPUS",
                target_bracket="B3",
                test_personality=pilot.get_preset("casual"),
                seed=0,
            )

    def test_opposition_preset_names_must_match_pod_size(self) -> None:
        with self.assertRaises(ValueError):
            pod_sim.run_pod_game(
                test_deck_id="ARCHIDEKT_4593151",
                target_bracket="B2",
                test_personality=pilot.get_preset("casual"),
                seed=0,
                opposition_preset_names=["shark", "control"],  # only 2 of required 3
            )


class PodSimB5CollisionTests(unittest.TestCase):
    """Safety #7: B5 pod-sim collision documentation."""

    def test_b5_target_with_b5_test_deck_does_not_collide_due_to_heliod(self) -> None:
        # Post-5a.6.2: live corpus has 4 engine_assigned=B5 decks (Heliod over-
        # shoot from self=B4). Excluding any 1 leaves exactly pod_size=3.
        record = pod_sim.run_pod_game(
            test_deck_id="ARCHIDEKT_5662629",  # Terra Bytes (B5)
            target_bracket="B5",
            test_personality=pilot.get_preset("combo"),
            seed=0,
        )
        self.assertFalse(record["opposition_pool_summary"]["collision_detected"])
        self.assertFalse(record["opposition_pool_summary"]["fallback_used"])


if __name__ == "__main__":
    unittest.main()
