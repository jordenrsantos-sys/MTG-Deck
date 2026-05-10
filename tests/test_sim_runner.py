"""Tests for tools/playtest/sim_runner.py (Phase 5b stage 5b.5)."""
from __future__ import annotations

import time
import unittest

from tools.playtest import pilot, sim_runner
from tools.playtest.sim_runner import (
    DEFAULT_MAX_TURNS,
    DeckProfile,
    GameSetup,
    KEY_DECISION_LOG_MAX,
    SIM_RUNNER_V1_VERSION,
    make_mock_deck,
    run_single_game,
)


def _mock_setup(seed: int = 0, personalities=None) -> GameSetup:
    if personalities is None:
        personalities = tuple(pilot.get_preset(name) for name in
                              ("shark", "control", "casual", "vengeful"))
    decks = tuple(make_mock_deck(f"D{i}") for i in range(4))
    return GameSetup(decks=decks, personalities=personalities, seed=seed)


class SimRunnerSmokeTests(unittest.TestCase):
    def test_run_single_game_returns_record_with_required_fields(self) -> None:
        record = run_single_game(_mock_setup(seed=42))
        for key in (
            "version", "game_id", "deck_ids", "personality_names",
            "winner_player_id", "winner_deck_id", "turns_to_resolution",
            "max_turns_hit", "runtime_exceeded", "wall_clock_sec",
            "key_decision_log", "win_condition_used", "replay_seed",
            "initial_state_hash", "final_state_hash", "deck_state_hash",
        ):
            self.assertIn(key, record, f"missing key: {key}")
        self.assertEqual(record["version"], SIM_RUNNER_V1_VERSION)
        self.assertEqual(record["replay_seed"], 42)

    def test_run_single_game_completes_within_timeout(self) -> None:
        # Safety #6: per-game time budget cap. Default timeout is 5 sec; the
        # mock-deck game must complete WELL under that.
        start = time.monotonic()
        record = run_single_game(_mock_setup(seed=1))
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 2.0, f"game took {elapsed:.2f}s; safety threshold is 2s")
        self.assertFalse(record["runtime_exceeded"])

    def test_decision_log_is_bounded(self) -> None:
        # Per Decision 10: bounded log <= KEY_DECISION_LOG_MAX entries.
        record = run_single_game(_mock_setup(seed=7))
        self.assertLessEqual(len(record["key_decision_log"]), KEY_DECISION_LOG_MAX)


class SimRunnerDeterminismTests(unittest.TestCase):
    def test_same_seed_produces_identical_records(self) -> None:
        # Safety #2: determinism preserved.
        r1 = run_single_game(_mock_setup(seed=12345))
        r2 = run_single_game(_mock_setup(seed=12345))
        self.assertEqual(r1["winner_player_id"], r2["winner_player_id"])
        self.assertEqual(r1["turns_to_resolution"], r2["turns_to_resolution"])
        self.assertEqual(r1["initial_state_hash"], r2["initial_state_hash"])
        self.assertEqual(r1["final_state_hash"], r2["final_state_hash"])
        self.assertEqual(r1["key_decision_log"], r2["key_decision_log"])

    def test_different_seeds_produce_different_initial_states(self) -> None:
        r1 = run_single_game(_mock_setup(seed=1))
        r2 = run_single_game(_mock_setup(seed=2))
        # Different seed -> different shuffles -> different initial state.
        self.assertNotEqual(r1["initial_state_hash"], r2["initial_state_hash"])


class SimRunnerWinConditionTests(unittest.TestCase):
    def test_some_seeds_produce_a_winner(self) -> None:
        # Across multiple seeds, expect at least one game to terminate with a winner.
        winners_found = 0
        for seed in range(20):
            record = run_single_game(_mock_setup(seed=seed))
            if record["winner_player_id"] is not None:
                winners_found += 1
        self.assertGreater(winners_found, 0,
                           "expected at least one winning game across 20 seeds")

    def test_runtime_exceeded_does_not_fire_on_short_game(self) -> None:
        record = run_single_game(_mock_setup(seed=99), per_game_timeout_sec=5.0)
        self.assertFalse(record["runtime_exceeded"])


class SimRunnerSetupTests(unittest.TestCase):
    def test_setup_requires_four_decks(self) -> None:
        decks = tuple(make_mock_deck(f"D{i}") for i in range(3))
        personalities = tuple(pilot.get_preset(n) for n in ("shark", "control", "casual"))
        setup = GameSetup(decks=decks, personalities=personalities, seed=0)
        with self.assertRaises(ValueError):
            run_single_game(setup)


class SimRunnerCombatDispatchTests(unittest.TestCase):
    """Stage 5b.11: dispatch table keyed on envelope.primary_type."""

    def _envelope(self, primary_type: str, *, cheap_tutor: int = 0,
                  fast_mana: int = 0, value_staple: int = 0) -> dict:
        return {
            "version": "win_condition_extractor_v1",
            "primary_type": primary_type,
            "primary_win_lines": [{"type": primary_type, "redundancy_count": 1, "enabler_density": 0}],
            "secondary_win_lines": [],
            "win_line_count": 1,
            "redundancy_total": 1,
            "signals_used": {
                "gc": 0, "combo_count": 0, "cheap_tutor": cheap_tutor,
                "fast_mana": fast_mana, "interaction_staple": 0,
                "value_staple": value_staple,
            },
        }

    def test_default_no_envelope_uses_combat_primary(self) -> None:
        # No envelopes_by_player -> defaults to combat-primary -> existing behavior.
        record = run_single_game(_mock_setup(seed=42))
        # Same as 5b.10 baseline: deterministic for fixed seed.
        record2 = run_single_game(_mock_setup(seed=42))
        self.assertEqual(record["final_state_hash"], record2["final_state_hash"])

    def test_combo_primary_with_high_enablers_finishes_faster(self) -> None:
        # Deck 0 = combo-primary with cheap_tutor=3 + fast_mana=3 -> finish_prob = 0.05 + 0.09 + 0.06 = 0.20 per turn.
        # All others combat-primary. Deck 0 should win the majority of seeds.
        envelopes = (
            self._envelope("combo", cheap_tutor=3, fast_mana=3),
            self._envelope("combat"),
            self._envelope("combat"),
            self._envelope("combat"),
        )
        wins_d0 = 0
        for seed in range(50):
            record = run_single_game(_mock_setup(seed=seed), envelopes_by_player=envelopes)
            if record["winner_player_id"] == 0:
                wins_d0 += 1
        # With 0.20 chance per turn * 4-player rotation, deck 0 should win
        # comfortably more than 25% (the random baseline).
        self.assertGreater(wins_d0, 12, f"combo-primary deck won {wins_d0}/50; expected > 12")

    def test_value_grind_uses_value_staple_bonus(self) -> None:
        # Deck 0 = value-grind with value_staple=5; should outperform a
        # value-grind without value_staples (both deal extra damage post-T8).
        envelopes_with = (
            self._envelope("value_grind", value_staple=5),
            self._envelope("combat"),
            self._envelope("combat"),
            self._envelope("combat"),
        )
        envelopes_without = (
            self._envelope("value_grind", value_staple=0),
            self._envelope("combat"),
            self._envelope("combat"),
            self._envelope("combat"),
        )
        wins_with = sum(
            1 for seed in range(30)
            if run_single_game(_mock_setup(seed=seed), envelopes_by_player=envelopes_with)
                .get("winner_player_id") == 0
        )
        wins_without = sum(
            1 for seed in range(30)
            if run_single_game(_mock_setup(seed=seed), envelopes_by_player=envelopes_without)
                .get("winner_player_id") == 0
        )
        # With value_staple bonus, deck 0 should win more or equal often.
        self.assertGreaterEqual(wins_with, wins_without)

    def test_dispatch_is_deterministic_for_fixed_seed(self) -> None:
        envelopes = (
            self._envelope("combo", cheap_tutor=3, fast_mana=2),
            self._envelope("value_grind", value_staple=4),
            self._envelope("combat"),
            self._envelope("combat"),
        )
        r1 = run_single_game(_mock_setup(seed=12345), envelopes_by_player=envelopes)
        r2 = run_single_game(_mock_setup(seed=12345), envelopes_by_player=envelopes)
        self.assertEqual(r1["winner_player_id"], r2["winner_player_id"])
        self.assertEqual(r1["turns_to_resolution"], r2["turns_to_resolution"])
        self.assertEqual(r1["final_state_hash"], r2["final_state_hash"])

    def test_mill_and_alt_win_defer_to_combat_handler(self) -> None:
        # Mill + alt_win don't appear in the corpus today; dispatch defers to
        # combat behavior. Verify a mill-typed envelope produces the same
        # outcome distribution as a combat-typed one for the same seed.
        env_mill = (
            self._envelope("mill"), self._envelope("combat"),
            self._envelope("combat"), self._envelope("combat"),
        )
        env_combat = (
            self._envelope("combat"), self._envelope("combat"),
            self._envelope("combat"), self._envelope("combat"),
        )
        r_mill = run_single_game(_mock_setup(seed=7), envelopes_by_player=env_mill)
        r_combat = run_single_game(_mock_setup(seed=7), envelopes_by_player=env_combat)
        self.assertEqual(r_mill["final_state_hash"], r_combat["final_state_hash"])

    def test_per_game_runtime_under_safety_threshold(self) -> None:
        # Stage 5b.11 spec: per-game runtime must stay under 10ms (3x margin
        # vs pre-5b.11 baseline of 3.25 ms/game). Halt threshold is 50ms.
        import time
        envelopes = (
            self._envelope("combo", cheap_tutor=3, fast_mana=2),
            self._envelope("value_grind", value_staple=4),
            self._envelope("combat"),
            self._envelope("combat"),
        )
        # Average over 5 games to smooth variance.
        runtimes = []
        for seed in range(5):
            start = time.monotonic()
            run_single_game(_mock_setup(seed=seed), envelopes_by_player=envelopes)
            runtimes.append(time.monotonic() - start)
        avg = sum(runtimes) / len(runtimes)
        self.assertLess(avg, 0.05, f"avg per-game {avg*1000:.1f}ms exceeded 50ms halt threshold")


if __name__ == "__main__":
    unittest.main()
