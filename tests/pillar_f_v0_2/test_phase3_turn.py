"""Phase 3 of mega-task v9 — turn / phase / step machine tests.

Coverage per kickoff Phase 3 gates:
- Full turn cycle with no actions completes in correct step order.
- "At beginning of combat" trigger fires at the right step.
- Cleanup-step discard-to-7.
- Turn rotation cycles through 4 players.
"""
from __future__ import annotations

import unittest

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, Phase, PlayerState, PlayerZones, Step,
)
from api.engine.pillar_f.v0_2.turn import (
    STEP_ORDER, NO_PRIORITY_STEPS, STEP_TO_PHASE,
    register_step_trigger, clear_step_triggers,
    start_step, step_opens_priority,
    untap_step, draw_step, cleanup_step,
    advance_step, run_turn,
)


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        ps = PlayerState(player_id=pid, name=f"P{pid}", life_total=40,
                         zones=PlayerZones())
        gs.players.append(ps)
    gs.active_player = 0
    gs.turn_number = 1
    gs.step = Step.UNTAP
    return gs


class StepOrderTests(unittest.TestCase):
    def test_step_order_canonical(self) -> None:
        # CR 5.1 canonical order.
        expected = [
            Step.UNTAP, Step.UPKEEP, Step.DRAW, Step.MAIN_1,
            Step.BEGINNING_OF_COMBAT, Step.DECLARE_ATTACKERS,
            Step.DECLARE_BLOCKERS, Step.FIRST_STRIKE_DAMAGE,
            Step.COMBAT_DAMAGE, Step.END_OF_COMBAT, Step.MAIN_2,
            Step.END_STEP, Step.CLEANUP,
        ]
        self.assertEqual(STEP_ORDER, expected)

    def test_phase_mapping_correct(self) -> None:
        self.assertEqual(STEP_TO_PHASE[Step.UNTAP], Phase.BEGINNING)
        self.assertEqual(STEP_TO_PHASE[Step.MAIN_1], Phase.PRECOMBAT_MAIN)
        self.assertEqual(STEP_TO_PHASE[Step.DECLARE_ATTACKERS], Phase.COMBAT)
        self.assertEqual(STEP_TO_PHASE[Step.MAIN_2], Phase.POSTCOMBAT_MAIN)
        self.assertEqual(STEP_TO_PHASE[Step.CLEANUP], Phase.ENDING)

    def test_no_priority_steps(self) -> None:
        self.assertFalse(step_opens_priority(Step.UNTAP))
        self.assertFalse(step_opens_priority(Step.CLEANUP))
        self.assertTrue(step_opens_priority(Step.MAIN_1))
        self.assertTrue(step_opens_priority(Step.COMBAT_DAMAGE))


class UntapStepTests(unittest.TestCase):
    def test_untap_step_untaps_active_player_permanents(self) -> None:
        gs = _empty_4p_game()
        # P0 has 2 tapped permanents.
        c1 = Card(name="P0-cre", owner=0, controller=0,
                  type_line="Creature", tapped=True, summoning_sick=True)
        c2 = Card(name="P0-art", owner=0, controller=0,
                  type_line="Artifact", tapped=True)
        gs.add_card(c1); gs.add_card(c2)
        gs.players[0].zones.battlefield.extend([c1.card_id, c2.card_id])
        untap_step(gs)
        self.assertFalse(c1.tapped)
        self.assertFalse(c2.tapped)
        self.assertFalse(c1.summoning_sick)

    def test_untap_step_empties_mana_pools(self) -> None:
        gs = _empty_4p_game()
        gs.players[0].mana_pool.W = 3
        gs.players[1].mana_pool.U = 2
        untap_step(gs)
        self.assertEqual(gs.players[0].mana_pool.total(), 0)
        self.assertEqual(gs.players[1].mana_pool.total(), 0)

    def test_untap_step_resets_per_turn_counters_for_active_player(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 0
        gs.players[0].lands_played_this_turn = 3
        gs.players[0].cards_drawn_this_turn = 2
        gs.players[0].spells_cast_this_turn = 1
        # P1 counters should NOT reset.
        gs.players[1].lands_played_this_turn = 5
        untap_step(gs)
        self.assertEqual(gs.players[0].lands_played_this_turn, 0)
        self.assertEqual(gs.players[0].cards_drawn_this_turn, 0)
        self.assertEqual(gs.players[0].spells_cast_this_turn, 0)
        self.assertEqual(gs.players[1].lands_played_this_turn, 5)


class DrawStepTests(unittest.TestCase):
    def test_first_turn_draw_skipped_for_starting_player(self) -> None:
        gs = _empty_4p_game()
        for i in range(7):
            c = Card(name=f"Lib{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        # Turn 1, active P0, skip_first_turn_draw=True (default).
        draw_step(gs)
        self.assertEqual(len(gs.players[0].zones.hand), 0)
        self.assertEqual(gs.players[0].cards_drawn_this_turn, 0)

    def test_first_turn_draw_not_skipped_for_other_players(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 1
        for i in range(5):
            c = Card(name=f"P1-Lib{i}", owner=1)
            gs.add_card(c)
            gs.players[1].zones.library.append(c.card_id)
        draw_step(gs)
        self.assertEqual(len(gs.players[1].zones.hand), 1)
        self.assertEqual(gs.players[1].cards_drawn_this_turn, 1)

    def test_subsequent_turns_draw(self) -> None:
        gs = _empty_4p_game()
        gs.turn_number = 2
        for i in range(5):
            c = Card(name=f"P0-Lib{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        draw_step(gs)
        self.assertEqual(len(gs.players[0].zones.hand), 1)

    def test_draw_from_empty_library_sets_flag(self) -> None:
        gs = _empty_4p_game()
        gs.turn_number = 2
        # Empty library — flag should set.
        draw_step(gs)
        self.assertTrue(gs.players[0].has_drawn_from_empty_library)


class CleanupStepTests(unittest.TestCase):
    def test_cleanup_discards_to_7(self) -> None:
        gs = _empty_4p_game()
        for i in range(10):
            c = Card(name=f"Hand{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.hand.append(c.card_id)
        re_enter = cleanup_step(gs)
        self.assertEqual(len(gs.players[0].zones.hand), 7)
        # 3 discards went to graveyard.
        self.assertEqual(len(gs.players[0].zones.graveyard), 3)
        self.assertFalse(re_enter)

    def test_cleanup_clears_damage(self) -> None:
        gs = _empty_4p_game()
        c = Card(name="Wounded", owner=0, controller=0,
                 type_line="Creature", power="3", toughness="3",
                 damage_marked=2)
        gs.add_card(c)
        gs.players[0].zones.battlefield.append(c.card_id)
        cleanup_step(gs)
        self.assertEqual(c.damage_marked, 0)

    def test_cleanup_expires_until_end_of_turn_effects(self) -> None:
        from api.engine.pillar_f.v0_2.state import ContinuousEffect
        gs = _empty_4p_game()
        # 2 effects: one until-end-of-turn, one permanent.
        gs.continuous_effects = [
            ContinuousEffect(effect_id="e1", source_card_id="src",
                             controller=0, layer=6,
                             target_pattern={"until_end_of_turn": True},
                             description="Giant Growth"),
            ContinuousEffect(effect_id="e2", source_card_id="src2",
                             controller=0, layer=6,
                             target_pattern={},
                             description="Anthem"),
        ]
        cleanup_step(gs)
        # Only the anthem remains.
        self.assertEqual(len(gs.continuous_effects), 1)
        self.assertEqual(gs.continuous_effects[0].effect_id, "e2")


class AdvanceStepTests(unittest.TestCase):
    def test_advance_step_within_turn(self) -> None:
        gs = _empty_4p_game()
        gs.step = Step.UNTAP
        nxt = advance_step(gs)
        self.assertEqual(nxt, Step.UPKEEP)
        nxt = advance_step(gs)
        self.assertEqual(nxt, Step.DRAW)
        nxt = advance_step(gs)
        self.assertEqual(nxt, Step.MAIN_1)

    def test_advance_from_cleanup_rotates_to_next_player(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 1
        gs.step = Step.CLEANUP
        nxt = advance_step(gs)
        self.assertEqual(nxt, Step.UNTAP)
        self.assertEqual(gs.active_player, 2)

    def test_turn_number_increments_when_wrapping(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 3
        gs.turn_number = 5
        gs.step = Step.CLEANUP
        advance_step(gs)
        self.assertEqual(gs.active_player, 0)
        self.assertEqual(gs.turn_number, 6)

    def test_rotation_skips_eliminated_players(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 0
        gs.players[1].has_lost = True
        gs.players[2].has_lost = True
        gs.step = Step.CLEANUP
        advance_step(gs)
        self.assertEqual(gs.active_player, 3)


class RunTurnTests(unittest.TestCase):
    def test_run_turn_visits_all_13_steps(self) -> None:
        gs = _empty_4p_game()
        gs.turn_number = 2  # Avoid first-turn draw skip semantics.
        # Put some library so draw doesn't crash.
        for i in range(20):
            c = Card(name=f"Lib{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        visited = run_turn(gs)
        self.assertEqual(visited, STEP_ORDER)

    def test_run_turn_4p_rotates_active_player(self) -> None:
        gs = _empty_4p_game()
        gs.turn_number = 2
        # Build 20-card libraries for each player.
        for pid in range(4):
            for i in range(20):
                c = Card(name=f"P{pid}-Lib{i}", owner=pid)
                gs.add_card(c)
                gs.players[pid].zones.library.append(c.card_id)
        # Run 4 turns.
        for expected_active in [0, 1, 2, 3]:
            self.assertEqual(gs.active_player, expected_active)
            run_turn(gs)
        # Back to P0 + turn 3.
        self.assertEqual(gs.active_player, 0)
        self.assertEqual(gs.turn_number, 3)


class StepTriggerTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_step_triggers()

    def tearDown(self) -> None:
        clear_step_triggers()

    def test_at_beginning_of_combat_trigger_fires(self) -> None:
        gs = _empty_4p_game()
        # Register a "at beginning of combat" trigger.
        register_step_trigger(Step.BEGINNING_OF_COMBAT, {
            "controller": 0,
            "source_card_id": "warleader",
            "resolver": "noop",
            "description": "Warleader: at beginning of combat trigger",
        })
        start_step(gs, Step.BEGINNING_OF_COMBAT)
        # Trigger should be on the stack after start_step.
        self.assertEqual(len(gs.stack), 1)
        self.assertEqual(gs.stack[0].description,
                         "Warleader: at beginning of combat trigger")

    def test_step_with_no_registered_triggers_no_stack_changes(self) -> None:
        gs = _empty_4p_game()
        start_step(gs, Step.MAIN_1)
        self.assertEqual(len(gs.stack), 0)


if __name__ == "__main__":
    unittest.main()
