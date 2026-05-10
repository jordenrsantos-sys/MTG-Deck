"""Tests for tools/playtest/pilot.py + pilot_personalities_v1.json (stage 5b.4)."""
from __future__ import annotations

import unittest

from tools.playtest import game_state, pilot, turn_loop
from tools.playtest.game_state import (
    PHASE_MAIN_COMBAT,
    PLAYER_COUNT,
    GameState,
    make_initial_game_state,
    make_initial_player_state,
    update_player_state,
)
from tools.playtest.pilot import PERSONALITY_AXES, PRESET_NAMES, PilotPersonality


def _mock_player(player_id: int) -> "game_state.PlayerState":
    return make_initial_player_state(
        player_id=player_id,
        deck_id=f"DECK_{player_id}",
        commander_oracle_ids=[f"cmdr-{player_id}"],
        library_oracle_ids=[f"oracle-{player_id}-{i}" for i in range(99)],
    )


def _mock_game(seed: int = 0) -> GameState:
    players = tuple(_mock_player(i) for i in range(PLAYER_COUNT))
    return make_initial_game_state(players=players, seed=seed)


class PilotPersonalitiesPackTests(unittest.TestCase):
    def test_pack_loads_with_six_presets(self) -> None:
        pack = pilot.load_personalities_pack()
        self.assertEqual(pack["pack_id"], "pilot_personalities_v1")
        self.assertEqual(set(pack["presets"].keys()), set(PRESET_NAMES))

    def test_pack_axes_match_module_constant(self) -> None:
        pack = pilot.load_personalities_pack()
        self.assertEqual(set(pack["axes"]), set(PERSONALITY_AXES))

    def test_each_preset_has_all_five_axes_in_unit_range(self) -> None:
        pack = pilot.load_personalities_pack()
        for name, preset in pack["presets"].items():
            if name.startswith("_"):
                continue
            for axis in PERSONALITY_AXES:
                self.assertIn(axis, preset, f"{name} missing axis {axis}")
                value = preset[axis]
                self.assertIsInstance(value, (int, float))
                self.assertGreaterEqual(value, 0.0)
                self.assertLessEqual(value, 1.0)


class PilotPresetResolverTests(unittest.TestCase):
    def test_get_preset_resolves_named_preset(self) -> None:
        p = pilot.get_preset("shark")
        self.assertIsInstance(p, PilotPersonality)
        self.assertEqual(p.name, "shark")
        # Shark is high-aggression + high-removal.
        self.assertGreaterEqual(p.aggression, 0.7)
        self.assertGreaterEqual(p.removal_targeting, 0.8)

    def test_get_preset_raises_on_unknown_name(self) -> None:
        with self.assertRaises(KeyError):
            pilot.get_preset("not_a_real_preset")

    def test_all_presets_returns_six_sorted_by_name(self) -> None:
        presets = pilot.all_presets()
        self.assertEqual(len(presets), 6)
        names = [p.name for p in presets]
        self.assertEqual(names, sorted(names))


class PilotDecisionDeterminismTests(unittest.TestCase):
    def test_evaluate_decision_is_deterministic_for_fixed_seed(self) -> None:
        state = _mock_game()
        # Inject a card into player 0's hand and walk to main_combat phase.
        state = update_player_state(state, 0, hand_oracle_ids=("a", "b", "c"))
        from dataclasses import replace
        state = replace(state, phase=PHASE_MAIN_COMBAT)
        personality = pilot.get_preset("shark")
        s1 = pilot.evaluate_pilot_decision(
            state=state, player_id=0, personality=personality, seed=42,
        )
        s2 = pilot.evaluate_pilot_decision(
            state=state, player_id=0, personality=personality, seed=42,
        )
        self.assertEqual(s1.players[0].hand_oracle_ids, s2.players[0].hand_oracle_ids)
        self.assertEqual(s1.players[0].battlefield_oracle_ids, s2.players[0].battlefield_oracle_ids)

    def test_evaluate_decision_no_op_when_player_dead(self) -> None:
        state = _mock_game()
        from dataclasses import replace
        state = replace(state, phase=PHASE_MAIN_COMBAT)
        state = update_player_state(state, 0, is_alive=False, hand_oracle_ids=("a",))
        personality = pilot.get_preset("shark")
        result = pilot.evaluate_pilot_decision(
            state=state, player_id=0, personality=personality, seed=0,
        )
        # Dead player's hand untouched.
        self.assertEqual(result.players[0].hand_oracle_ids, ("a",))

    def test_evaluate_decision_can_play_card_from_hand(self) -> None:
        # With aggression=1.0 and seed where rng < threshold, card should land.
        state = _mock_game()
        from dataclasses import replace
        state = update_player_state(state, 0, hand_oracle_ids=("X", "Y", "Z"))
        state = replace(state, phase=PHASE_MAIN_COMBAT)
        # Find a seed that triggers the play branch.
        personality = pilot.get_preset("shark")  # aggression=0.8, threshold=0.65
        # Try multiple seeds; at least one should land the card.
        landed = False
        for seed in range(20):
            r = pilot.evaluate_pilot_decision(
                state=state, player_id=0, personality=personality, seed=seed,
            )
            if "X" in r.players[0].battlefield_oracle_ids:
                landed = True
                break
        self.assertTrue(landed, "shark personality should land cards across some seeds")


class PilotDecisionCallbackTests(unittest.TestCase):
    def test_make_decision_callback_drives_turn_loop(self) -> None:
        state = _mock_game()
        # Pre-populate hands so the callback has something to play.
        for i in range(PLAYER_COUNT):
            state = update_player_state(state, i, hand_oracle_ids=tuple(f"card-{i}-{j}" for j in range(7)))
        personalities = {i: pilot.get_preset("shark") for i in range(PLAYER_COUNT)}
        cb = pilot.make_decision_callback(personalities, seed=12345)
        # Run a few turns; verify the callback was called and modified state.
        result = turn_loop.play_until_winner_or_max_turns(state, decision_callback=cb, max_turns=4)
        # At least one player should have moved cards from hand to battlefield.
        any_played = any(len(p.battlefield_oracle_ids) > 0 for p in result.players)
        self.assertTrue(any_played)


class PilotManifestRegistrationTests(unittest.TestCase):
    """Calibration-only manifest registration check (safety #1)."""

    def test_pilot_personalities_pack_is_calibration_only_in_manifest(self) -> None:
        from api.engine.curated_pack_manifest_v1 import (
            iter_runtime_pack_entries,
            load_curated_pack_manifest_v1,
        )
        manifest = load_curated_pack_manifest_v1()
        # Should be in the all-entries list.
        all_pack_ids = {e["pack_id"] for e in manifest["packs"]}
        self.assertIn("pilot_personalities_v1", all_pack_ids)
        # Should be FILTERED OUT of runtime entries (calibration_only=True).
        runtime_pack_ids = {e["pack_id"] for e in iter_runtime_pack_entries()}
        self.assertNotIn("pilot_personalities_v1", runtime_pack_ids)


if __name__ == "__main__":
    unittest.main()
