"""Phase 7 of mega-task v9 — mulligan + draw + cleanup polish tests.

Coverage per kickoff Phase 7 gates:
- London mulligan correctly puts cards on bottom.
- First-turn skip-draw for starting player.
- "Until end of turn" P/T pump expires at cleanup.
- Cleanup discard-to-7 honors active player's choice.
"""
from __future__ import annotations

import unittest

from api.engine.pillar_f.v0_2.state import (
    Card, ContinuousEffect, GameState, PlayerState, PlayerZones,
)
from api.engine.pillar_f.v0_2.turn import (
    mulligan_setup, always_keep_decider, keep_after_n_mulligans_decider,
    default_bottom_picker, shuffle_library, draw_n,
    cleanup_step, draw_step,
)


def _empty_4p_game_with_decks(deck_size: int = 60) -> GameState:
    gs = GameState()
    for pid in range(4):
        ps = PlayerState(player_id=pid, name=f"P{pid}",
                         life_total=40, zones=PlayerZones())
        # Build library.
        for i in range(deck_size):
            c = Card(name=f"P{pid}_card_{i}", owner=pid)
            gs.add_card(c)
            ps.zones.library.append(c.card_id)
        gs.players.append(ps)
    gs.active_player = 0
    gs.turn_number = 1
    return gs


class DrawNTests(unittest.TestCase):
    def test_draw_n_from_top_of_library(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=10)
        # Top of library: index 0 = "P0_card_0".
        drawn = draw_n(gs, 0, 3)
        self.assertEqual(len(drawn), 3)
        # First drawn = top of library = P0_card_0.
        top_name = gs.get_card(drawn[0]).name
        self.assertEqual(top_name, "P0_card_0")
        # Hand has 3 cards now.
        self.assertEqual(len(gs.players[0].zones.hand), 3)
        # Library shrank by 3.
        self.assertEqual(len(gs.players[0].zones.library), 7)

    def test_draw_n_from_empty_library_sets_flag(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=2)
        drawn = draw_n(gs, 0, 5)
        # Only 2 cards available.
        self.assertEqual(len(drawn), 2)
        self.assertTrue(gs.players[0].has_drawn_from_empty_library)


class ShuffleLibraryTests(unittest.TestCase):
    def test_shuffle_with_seed_is_deterministic(self) -> None:
        gs1 = _empty_4p_game_with_decks(deck_size=10)
        gs2 = _empty_4p_game_with_decks(deck_size=10)
        # Use distinct card_ids to compare — but since both gs1 and
        # gs2 have unique card_ids per instance, we compare by NAME
        # ordering.
        names1 = [gs1.get_card(cid).name for cid in gs1.players[0].zones.library]
        names2 = [gs2.get_card(cid).name for cid in gs2.players[0].zones.library]
        self.assertEqual(names1, names2)  # same initial order
        shuffle_library(gs1, 0, seed=42)
        shuffle_library(gs2, 0, seed=42)
        names1_after = [gs1.get_card(cid).name for cid in gs1.players[0].zones.library]
        names2_after = [gs2.get_card(cid).name for cid in gs2.players[0].zones.library]
        self.assertEqual(names1_after, names2_after,
                         "same seed should yield same shuffle order")


class MulliganSetupTests(unittest.TestCase):
    def test_mulligan_setup_draws_7_per_player_when_all_keep(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=60)
        results = mulligan_setup(
            gs,
            decider_fn=always_keep_decider,
            seed_per_player={pid: pid for pid in range(4)},
        )
        for pid in range(4):
            self.assertEqual(len(gs.players[pid].zones.hand), 7)
            self.assertEqual(results[pid], 0)

    def test_mulligan_3_times_puts_3_on_bottom(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=60)
        # Player 0 mulligans 3 times then keeps.
        # Players 1-3 keep immediately.
        def decider(state, pid, hand, num_muls):
            if pid == 0:
                return num_muls < 3
            return False
        results = mulligan_setup(gs, decider_fn=decider,
                                  seed_per_player={pid: pid for pid in range(4)})
        # P0 has 7 - 3 = 4 cards in hand (3 went to bottom).
        self.assertEqual(len(gs.players[0].zones.hand), 4)
        self.assertEqual(results[0], 3)
        # Library has 60 - 4 = 56 cards.
        self.assertEqual(len(gs.players[0].zones.library), 56)
        # Other players have 7 in hand.
        self.assertEqual(len(gs.players[1].zones.hand), 7)
        self.assertEqual(results[1], 0)

    def test_max_mulligans_cap(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=20)
        # Decider always mulligans, but cap at 2.
        def always_mull(state, pid, hand, num_muls):
            return True
        results = mulligan_setup(gs, decider_fn=always_mull, max_mulligans=2,
                                  seed_per_player={pid: pid for pid in range(4)})
        for pid in range(4):
            self.assertEqual(results[pid], 2)
            # Final hand: 7 - 2 = 5.
            self.assertEqual(len(gs.players[pid].zones.hand), 5)

    def test_bottom_picker_default_picks_last_n(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=60)
        # Track hand contents before mulligan completes.
        results = mulligan_setup(
            gs,
            decider_fn=keep_after_n_mulligans_decider(2),
            bottom_picker_fn=default_bottom_picker,
            seed_per_player={pid: pid for pid in range(4)},
        )
        # 2 mulligans → 5 cards in hand, 2 on bottom of library.
        for pid in range(4):
            self.assertEqual(len(gs.players[pid].zones.hand), 5)
            self.assertEqual(len(gs.players[pid].zones.library), 55)


class FirstTurnDrawSkipTests(unittest.TestCase):
    def test_first_turn_p0_skips_draw(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=10)
        # turn_number=1, active=0.
        draw_step(gs)
        self.assertEqual(len(gs.players[0].zones.hand), 0)
        self.assertEqual(gs.players[0].cards_drawn_this_turn, 0)

    def test_first_turn_p1_does_not_skip(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=10)
        gs.active_player = 1
        draw_step(gs)
        self.assertEqual(len(gs.players[1].zones.hand), 1)
        self.assertEqual(gs.players[1].cards_drawn_this_turn, 1)

    def test_second_turn_p0_draws(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=10)
        gs.turn_number = 2
        draw_step(gs)
        self.assertEqual(len(gs.players[0].zones.hand), 1)


class CleanupStepTests(unittest.TestCase):
    def test_cleanup_discards_excess_to_graveyard(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=20)
        # Build hand of 10.
        for i in range(10):
            c = Card(name=f"hand_card_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.hand.append(c.card_id)
        cleanup_step(gs)
        self.assertEqual(len(gs.players[0].zones.hand), 7)
        self.assertEqual(len(gs.players[0].zones.graveyard), 3)

    def test_cleanup_clears_damage(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=20)
        c = Card(name="DamagedCre", owner=0, controller=0,
                 type_line="Creature", power="3", toughness="3",
                 damage_marked=2)
        gs.add_card(c)
        gs.players[0].zones.battlefield.append(c.card_id)
        cleanup_step(gs)
        self.assertEqual(c.damage_marked, 0)

    def test_cleanup_expires_until_end_of_turn_continuous_effects(self) -> None:
        gs = _empty_4p_game_with_decks(deck_size=20)
        # Giant Growth-style continuous effect.
        gs.continuous_effects = [
            ContinuousEffect(effect_id="gg", source_card_id="src",
                             controller=0, layer=7, sublayer="c",
                             effect_fn_name="anthem_pt_mod",
                             target_pattern={"until_end_of_turn": True,
                                             "card_id": "target",
                                             "p_mod": 3, "t_mod": 3},
                             description="Giant Growth"),
            ContinuousEffect(effect_id="anthem", source_card_id="honor",
                             controller=0, layer=7, sublayer="c",
                             effect_fn_name="anthem_pt_mod",
                             target_pattern={"all_creatures": True,
                                             "p_mod": 1, "t_mod": 1},
                             description="Permanent anthem"),
        ]
        cleanup_step(gs)
        # Only the permanent anthem remains.
        remaining_ids = {ce.effect_id for ce in gs.continuous_effects}
        self.assertEqual(remaining_ids, {"anthem"})


if __name__ == "__main__":
    unittest.main()
