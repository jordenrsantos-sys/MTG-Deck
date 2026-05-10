"""Tests for game_state.py + turn_loop.py (Phase 5b stage 5b.3)."""
from __future__ import annotations

import unittest
from dataclasses import replace

from tools.playtest import game_state, turn_loop
from tools.playtest.game_state import (
    GAME_STATE_V1_VERSION,
    PHASE_DRAW,
    PHASE_END,
    PHASE_MAIN_COMBAT,
    PHASE_UNTAP,
    PHASE_UPKEEP,
    PHASES,
    PLAYER_COUNT,
    STARTING_LIFE,
    GameState,
    PlayerState,
    kill_player,
    make_initial_game_state,
    make_initial_player_state,
    state_hash,
    update_player_state,
    winner,
)


def _mock_player(player_id: int, deck_size: int = 99) -> PlayerState:
    return make_initial_player_state(
        player_id=player_id,
        deck_id=f"DECK_{player_id}",
        commander_oracle_ids=[f"cmdr-{player_id}"],
        library_oracle_ids=[f"oracle-{player_id}-{i}" for i in range(deck_size)],
    )


def _mock_game(seed: int = 0) -> GameState:
    players = tuple(_mock_player(i) for i in range(PLAYER_COUNT))
    return make_initial_game_state(players=players, seed=seed)


class GameStateInitializationTests(unittest.TestCase):
    def test_initial_state_has_four_alive_players_at_starting_life(self) -> None:
        state = _mock_game()
        self.assertEqual(state.version, GAME_STATE_V1_VERSION)
        self.assertEqual(state.turn, 0)
        self.assertEqual(state.phase, PHASE_UNTAP)
        self.assertEqual(len(state.players), PLAYER_COUNT)
        for p in state.players:
            self.assertEqual(p.life, STARTING_LIFE)
            self.assertTrue(p.is_alive)
            self.assertEqual(len(p.library_oracle_ids), 99)
            self.assertEqual(len(p.hand_oracle_ids), 0)
            self.assertEqual(len(p.command_zone_oracle_ids), 1)

    def test_initial_state_rejects_wrong_player_count(self) -> None:
        with self.assertRaises(ValueError):
            make_initial_game_state(
                players=(_mock_player(0), _mock_player(1)), seed=0,
            )


class GameStateDeterminismTests(unittest.TestCase):
    def test_state_hash_deterministic(self) -> None:
        s1 = _mock_game(seed=42)
        s2 = _mock_game(seed=42)
        self.assertEqual(state_hash(s1), state_hash(s2))

    def test_state_hash_changes_after_update(self) -> None:
        s1 = _mock_game()
        s2 = update_player_state(s1, 0, life=35)
        self.assertNotEqual(state_hash(s1), state_hash(s2))

    def test_kill_player_marks_is_alive_false(self) -> None:
        state = _mock_game()
        state2 = kill_player(state, 0)
        self.assertFalse(state2.players[0].is_alive)
        self.assertTrue(state2.players[1].is_alive)


class GameStateWinnerTests(unittest.TestCase):
    def test_no_winner_with_four_alive(self) -> None:
        state = _mock_game()
        self.assertIsNone(winner(state))

    def test_winner_when_three_dead(self) -> None:
        state = _mock_game()
        state = kill_player(state, 0)
        state = kill_player(state, 1)
        state = kill_player(state, 2)
        self.assertEqual(winner(state), 3)


class TurnLoopAdvanceTests(unittest.TestCase):
    def test_advance_phase_walks_untap_to_upkeep(self) -> None:
        state = _mock_game()
        state = turn_loop.advance_phase(state)
        self.assertEqual(state.phase, PHASE_UPKEEP)

    def test_advance_phase_walks_full_phase_cycle_to_next_player(self) -> None:
        state = _mock_game()
        for _ in PHASES:
            state = turn_loop.advance_phase(state)
        # After 5 phases the active player has rotated.
        self.assertEqual(state.active_player, 1)
        self.assertEqual(state.phase, PHASE_UNTAP)

    def test_advance_phase_drew_a_card_during_draw_step(self) -> None:
        state = _mock_game()
        # Walk to draw step (untap -> upkeep -> draw advances and draws).
        state = turn_loop.advance_phase(state)  # untap -> upkeep
        state = turn_loop.advance_phase(state)  # upkeep -> draw
        state = turn_loop.advance_phase(state)  # draw -> main_combat (this advance includes the draw)
        active_player_state = state.players[0]
        self.assertEqual(len(active_player_state.hand_oracle_ids), 1)

    def test_advance_full_turn_completes_one_player_turn(self) -> None:
        state = _mock_game()
        state = turn_loop.advance_full_turn(state)
        # After one full player turn, active_player rotated and turn advanced.
        self.assertEqual(state.active_player, 1)


class TurnLoopDeterminismTests(unittest.TestCase):
    def test_play_until_winner_or_max_terminates_quickly_with_no_callback(self) -> None:
        # No decision callback + no damage = no winner; loop hits max_turns.
        state = _mock_game()
        result = turn_loop.play_until_winner_or_max_turns(state, max_turns=5)
        # No winner emerges (all 4 still alive, no damage dealt by callback).
        self.assertIsNone(winner(result))
        self.assertGreaterEqual(result.turn, 5)

    def test_play_until_winner_terminates_when_three_die(self) -> None:
        state = _mock_game()
        state = kill_player(state, 1)
        state = kill_player(state, 2)
        state = kill_player(state, 3)
        # Single-survivor state — loop exits immediately.
        result = turn_loop.play_until_winner_or_max_turns(state, max_turns=10)
        self.assertEqual(winner(result), 0)

    def test_advance_phase_with_callback_invokes_decision(self) -> None:
        state = _mock_game()
        invocations = []

        def cb(s: GameState, player_id: int) -> GameState:
            invocations.append((s.phase, player_id))
            return s

        # Walk through enough phases to hit upkeep + main_combat callbacks.
        state = turn_loop.advance_full_turn(state, decision_callback=cb)
        # Callback called at least twice (upkeep + main_combat).
        self.assertGreaterEqual(len(invocations), 2)


if __name__ == "__main__":
    unittest.main()
