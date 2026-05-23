"""Phase 1 of mega-task v10 — compact_view helper tests.

Coverage per kickoff Phase 1 gates:
- Fresh game state < 1000 tokens.
- Mid-game (turn 15) state < 4000 tokens.
- Opponent hand contents redacted (counts only).
- Stack contents preserved.
- Output deterministic given same input.
"""
from __future__ import annotations

import unittest

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, StackEntry, Step, Phase,
)
from api.engine.pillar_f.v0_2.policy.prompts import (
    compact_view, estimate_tokens, COMPACT_VIEW_VERSION,
)


def _fresh_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        ps = PlayerState(player_id=pid, name=f"P{pid}", life_total=40,
                         zones=PlayerZones())
        gs.players.append(ps)
    return gs


def _midgame_4p_game(*, turn: int = 15) -> GameState:
    gs = _fresh_4p_game()
    gs.turn_number = turn
    gs.phase = Phase.COMBAT
    gs.step = Step.DECLARE_ATTACKERS
    gs.active_player = 1
    gs.priority_holder = 2
    gs.the_monarch = 0
    # Each player has 6 cards in hand, 25 in library, 10 on battlefield,
    # 4 in graveyard.
    for pid in range(4):
        p = gs.players[pid]
        for j in range(6):
            c = Card(name=f"P{pid}_hand_{j}", owner=pid,
                     mana_cost=f"{{{(j+1) % 5}}}",
                     type_line="Instant",
                     oracle_text=f"Sample text for {pid}/{j}.")
            gs.add_card(c)
            p.zones.hand.append(c.card_id)
        for j in range(25):
            c = Card(name=f"P{pid}_lib_{j}", owner=pid)
            gs.add_card(c)
            p.zones.library.append(c.card_id)
        for j in range(10):
            c = Card(name=f"P{pid}_bf_{j}", owner=pid, controller=pid,
                     type_line="Creature — Soldier",
                     power="2", toughness="2",
                     keywords=["flying"] if j % 3 == 0 else [],
                     tapped=(j % 4 == 0),
                     counters={"+1/+1": j % 3} if j % 5 == 0 else {})
            gs.add_card(c)
            p.zones.battlefield.append(c.card_id)
        for j in range(4):
            c = Card(name=f"P{pid}_gy_{j}", owner=pid)
            gs.add_card(c)
            p.zones.graveyard.append(c.card_id)
        # Commander damage taken from some opponent.
        if pid != 0:
            p.commander_damage_taken_from[f"oracle-P{pid - 1}"] = pid * 3
    # Stack with 2 entries.
    gs.stack.append(StackEntry(
        entry_id="se-bolt", card_id=None, controller=0,
        entry_type="spell", targets=[1],
        description="Lightning Bolt",
    ))
    gs.stack.append(StackEntry(
        entry_id="se-counter", card_id=None, controller=1,
        entry_type="spell", targets=[],
        description="Counterspell @ Bolt",
    ))
    return gs


class CompactViewBasicsTests(unittest.TestCase):
    def test_fresh_game_under_1000_tokens(self) -> None:
        gs = _fresh_4p_game()
        view = gs.perspective_view(viewer_player_id=0)
        text = compact_view(view, viewer_player_id=0)
        n = estimate_tokens(text)
        self.assertLess(n, 1000,
                        f"fresh game compact_view = {n} tokens (expected <1000)")

    def test_midgame_under_4000_tokens(self) -> None:
        gs = _midgame_4p_game(turn=15)
        view = gs.perspective_view(viewer_player_id=0)
        text = compact_view(view, viewer_player_id=0)
        n = estimate_tokens(text)
        self.assertLess(n, 4000,
                        f"turn-15 mid-game compact_view = {n} tokens (expected <4000)")

    def test_compact_view_contains_basic_sections(self) -> None:
        gs = _midgame_4p_game()
        view = gs.perspective_view(viewer_player_id=0)
        text = compact_view(view, viewer_player_id=0)
        self.assertIn("TURN", text)
        self.assertIn("PLAYERS", text)
        self.assertIn("STACK", text)
        self.assertIn("BATTLEFIELD", text)
        self.assertIn("YOUR HAND", text)


class CompactViewRedactionTests(unittest.TestCase):
    def test_opponent_hand_contents_not_revealed(self) -> None:
        gs = _midgame_4p_game()
        view = gs.perspective_view(viewer_player_id=0)
        text = compact_view(view, viewer_player_id=0)
        # P1's hand cards have name "P1_hand_*" — should NOT appear.
        for j in range(6):
            self.assertNotIn(f"P1_hand_{j}", text,
                             f"opponent hand card P1_hand_{j} leaked")

    def test_own_hand_contents_fully_visible(self) -> None:
        gs = _midgame_4p_game()
        view = gs.perspective_view(viewer_player_id=0)
        text = compact_view(view, viewer_player_id=0)
        for j in range(6):
            self.assertIn(f"P0_hand_{j}", text,
                          f"own hand card P0_hand_{j} missing")

    def test_library_cards_not_revealed_for_anyone(self) -> None:
        gs = _midgame_4p_game()
        view = gs.perspective_view(viewer_player_id=0)
        text = compact_view(view, viewer_player_id=0)
        # No P*_lib_* names should leak.
        for pid in range(4):
            for j in range(25):
                self.assertNotIn(f"P{pid}_lib_{j}", text,
                                 f"library card P{pid}_lib_{j} leaked")

    def test_player_summary_shows_hand_count(self) -> None:
        gs = _midgame_4p_game()
        view = gs.perspective_view(viewer_player_id=0)
        text = compact_view(view, viewer_player_id=0)
        # Each player should show "hand=6" in the summary line.
        self.assertIn("hand=6", text)


class CompactViewStackTests(unittest.TestCase):
    def test_stack_contents_preserved(self) -> None:
        gs = _midgame_4p_game()
        view = gs.perspective_view(viewer_player_id=0)
        text = compact_view(view, viewer_player_id=0)
        self.assertIn("Lightning Bolt", text)
        self.assertIn("Counterspell @ Bolt", text)

    def test_stack_empty_renders_empty(self) -> None:
        gs = _fresh_4p_game()
        view = gs.perspective_view(viewer_player_id=0)
        text = compact_view(view, viewer_player_id=0)
        self.assertIn("(empty)", text)


class CompactViewDeterminismTests(unittest.TestCase):
    def test_same_input_produces_same_output(self) -> None:
        gs = _midgame_4p_game()
        view1 = gs.perspective_view(viewer_player_id=0)
        view2 = gs.perspective_view(viewer_player_id=0)
        text1 = compact_view(view1, viewer_player_id=0)
        text2 = compact_view(view2, viewer_player_id=0)
        self.assertEqual(text1, text2)


class CompactViewActionLogTests(unittest.TestCase):
    def test_action_log_filtered_to_last_n_turns(self) -> None:
        gs = _midgame_4p_game(turn=10)
        view = gs.perspective_view(viewer_player_id=0)
        log = [
            "T7 P0 cast Sol_Ring",
            "T7 P1 pass",
            "T8 P1 cast Counterspell",
            "T9 P0 cast Lightning_Bolt",
            "T10 P0 attack",
        ]
        text = compact_view(view, viewer_player_id=0,
                            action_log=log, last_n_turns=3)
        # last 3 turns = T8, T9, T10.
        self.assertIn("T8 P1 cast Counterspell", text)
        self.assertIn("T9 P0 cast Lightning_Bolt", text)
        self.assertIn("T10 P0 attack", text)
        # T7 should NOT appear.
        self.assertNotIn("T7 P0 cast Sol_Ring", text)


class EstimateTokensTests(unittest.TestCase):
    def test_estimate_tokens_proportional_to_length(self) -> None:
        short = "abc"
        long = "abc" * 1000
        self.assertLess(estimate_tokens(short), estimate_tokens(long))

    def test_empty_string_zero_tokens(self) -> None:
        self.assertEqual(estimate_tokens(""), 0)


if __name__ == "__main__":
    unittest.main()
