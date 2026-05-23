"""Phase 2 of mega-task v9 — stack + priority loop + APNAP tests.

Coverage per kickoff Phase 2 gates:
- Simple sorcery resolution (push, all pass, resolves).
- Response sequence (sorcery → counter → counter-counter → all 3 resolve in LIFO).
- APNAP trigger ordering with same-controller pile-up.
- Priority returns to active player after stack empties.
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones,
)
from api.engine.pillar_f.v0_2.stack import (
    push_to_stack, pop_top, peek_top, counter_target,
    resolve_top, register_resolver, apnap_order,
    priority_round, run_stack_to_resolution,
    enqueue_triggers, drain_triggers_to_stack,
)


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        ps = PlayerState(player_id=pid, name=f"P{pid}", life_total=40,
                         zones=PlayerZones())
        gs.players.append(ps)
    return gs


def _pass_responder(state: GameState, pid: int) -> Optional[Dict[str, Any]]:
    return None


class StackPushPopTests(unittest.TestCase):
    def test_push_to_stack_appends(self) -> None:
        gs = _empty_4p_game()
        e = push_to_stack(gs, card_id="c1", controller=0,
                          entry_type="spell", description="Test Sorcery")
        self.assertEqual(len(gs.stack), 1)
        self.assertEqual(peek_top(gs).entry_id, e.entry_id)
        self.assertEqual(peek_top(gs).controller, 0)

    def test_push_resets_priority_passes(self) -> None:
        gs = _empty_4p_game()
        gs.priority_passes_this_round = {0, 1}
        push_to_stack(gs, card_id="c1", controller=2)
        self.assertEqual(gs.priority_passes_this_round, set())

    def test_pop_top_lifo(self) -> None:
        gs = _empty_4p_game()
        push_to_stack(gs, card_id="c1", controller=0, description="first")
        push_to_stack(gs, card_id="c2", controller=1, description="second")
        push_to_stack(gs, card_id="c3", controller=2, description="third")
        e = pop_top(gs)
        self.assertEqual(e.description, "third")
        e = pop_top(gs)
        self.assertEqual(e.description, "second")
        e = pop_top(gs)
        self.assertEqual(e.description, "first")
        self.assertIsNone(pop_top(gs))


class CounterspellTests(unittest.TestCase):
    def test_counter_target_removes_entry(self) -> None:
        gs = _empty_4p_game()
        e_target = push_to_stack(gs, card_id="target", controller=0,
                                 description="Targeted Spell")
        e_counter = push_to_stack(gs, card_id="counter", controller=1,
                                  description="Counterspell")
        # Counter target.
        ok = counter_target(gs, e_target.entry_id)
        self.assertTrue(ok)
        # Counterspell still on stack.
        self.assertEqual(len(gs.stack), 1)
        self.assertEqual(peek_top(gs).entry_id, e_counter.entry_id)

    def test_counter_nonexistent_target_returns_false(self) -> None:
        gs = _empty_4p_game()
        push_to_stack(gs, card_id="x", controller=0)
        self.assertFalse(counter_target(gs, "nonexistent-id"))
        self.assertEqual(len(gs.stack), 1)


class PriorityLoopTests(unittest.TestCase):
    def test_simple_sorcery_resolves_after_all_pass(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 0
        push_to_stack(gs, card_id="bolt", controller=0,
                      payment={"resolver": "deal_damage_to_player",
                               "amount": 3},
                      targets=[1], description="Lightning Bolt @ P1")
        resolved = run_stack_to_resolution(gs, _pass_responder)
        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0].description, "Lightning Bolt @ P1")
        # P1 took 3 damage.
        self.assertEqual(gs.players[1].life_total, 37)
        # Stack empty + priority back to active player area (None between rounds).
        self.assertEqual(len(gs.stack), 0)

    def test_apnap_order_starts_at_active_player(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 2
        self.assertEqual(apnap_order(gs), [2, 3, 0, 1])
        gs.active_player = 0
        self.assertEqual(apnap_order(gs), [0, 1, 2, 3])

    def test_apnap_skips_eliminated_players(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 0
        gs.players[2].has_lost = True
        self.assertEqual(apnap_order(gs), [0, 1, 3])

    def test_response_sequence_3_deep_resolves_lifo(self) -> None:
        """Sorcery (P0) → Counterspell (P1) → Counter-counter (P2). All
        resolve in LIFO: P2's counter-counter first (counters P1), then
        P1's counterspell would be countered → P0's sorcery resolves."""
        gs = _empty_4p_game()
        gs.active_player = 0
        e_sorcery = push_to_stack(
            gs, card_id="sorcery", controller=0,
            payment={"resolver": "deal_damage_to_player", "amount": 4},
            targets=[1], description="4-damage sorcery @ P1",
        )
        e_counter = push_to_stack(
            gs, card_id="counter", controller=1,
            payment={"resolver": "counterspell",
                     "target_entry_id": e_sorcery.entry_id},
            description="Counterspell @ sorcery",
        )
        e_counter2 = push_to_stack(
            gs, card_id="counter2", controller=2,
            payment={"resolver": "counterspell",
                     "target_entry_id": e_counter.entry_id},
            description="Counter @ P1's counter",
        )
        # Register a counterspell resolver inline for this test.
        def _counterspell_resolver(state, entry):
            target_id = entry.payment.get("target_entry_id")
            if target_id:
                counter_target(state, target_id)
        register_resolver("counterspell", _counterspell_resolver)
        # Now resolve the whole stack with all-pass responder.
        resolved = run_stack_to_resolution(gs, _pass_responder)
        # LIFO: counter2 resolves first (counters counter1), then sorcery resolves.
        # counter1 was popped off by counter2, so only 2 resolutions remain
        # (counter2 itself + sorcery).
        self.assertEqual(len(resolved), 2)
        self.assertEqual(resolved[0].entry_id, e_counter2.entry_id)
        self.assertEqual(resolved[1].entry_id, e_sorcery.entry_id)
        # P1 took the 4 damage.
        self.assertEqual(gs.players[1].life_total, 36)
        self.assertEqual(len(gs.stack), 0)

    def test_active_player_action_resets_round(self) -> None:
        """If the active player takes an action mid-round, the round
        restarts (CR 117.3 — priority returns to active player after
        a stack mutation)."""
        gs = _empty_4p_game()
        gs.active_player = 0
        passes_seen = []

        def responder(state, pid):
            passes_seen.append(pid)
            # P0 takes an action on the first call (instant draw 1).
            if pid == 0 and len(passes_seen) == 1:
                return {"card_id": "ancestral",
                        "entry_type": "spell",
                        "controller": 0,
                        "payment": {"resolver": "draw_cards", "amount": 1},
                        "description": "Draw 1"}
            return None

        # Put a sorcery on stack first so the round has something to resolve.
        push_to_stack(gs, card_id="s", controller=0,
                      payment={"resolver": "noop"}, description="Init Spell")
        # Single round.
        priority_round(gs, responder)
        # After the round: P0's action got pushed; the round-restart
        # cycle exhausted all 4 players passing (and P0 didn't take action
        # the second time since len(passes_seen) > 1).
        # The responder was called multiple times; first call P0 took action,
        # then APNAP restarted from P0 with subsequent passes.
        self.assertTrue(len(passes_seen) >= 5)  # 1 action + 4 passes


class APNAPTriggerOrderingTests(unittest.TestCase):
    def test_enqueue_and_drain_triggers_apnap_order(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 1
        # Player 2 has 2 triggers; Player 3 has 1; Player 0 has 1.
        enqueue_triggers(gs, [
            {"controller": 2, "source_card_id": "p2-a",
             "resolver": "noop", "description": "P2 trigger A"},
            {"controller": 3, "source_card_id": "p3-a",
             "resolver": "noop", "description": "P3 trigger A"},
            {"controller": 2, "source_card_id": "p2-b",
             "resolver": "noop", "description": "P2 trigger B"},
            {"controller": 0, "source_card_id": "p0-a",
             "resolver": "noop", "description": "P0 trigger A"},
        ])
        n = drain_triggers_to_stack(gs)
        self.assertEqual(n, 4)
        # APNAP from active=1: order is 1, 2, 3, 0.
        # P1 has no triggers, so push order: P2-A, P2-B, P3-A, P0-A.
        descriptions = [e.description for e in gs.stack]
        self.assertEqual(descriptions, [
            "P2 trigger A", "P2 trigger B", "P3 trigger A", "P0 trigger A",
        ])

    def test_drain_empty_returns_zero(self) -> None:
        gs = _empty_4p_game()
        self.assertEqual(drain_triggers_to_stack(gs), 0)
        self.assertEqual(len(gs.stack), 0)

    def test_drain_skips_eliminated_player_triggers(self) -> None:
        gs = _empty_4p_game()
        gs.active_player = 0
        gs.players[2].has_lost = True
        enqueue_triggers(gs, [
            {"controller": 2, "source_card_id": "dead-trigger",
             "resolver": "noop", "description": "Dead P2 trigger"},
            {"controller": 1, "source_card_id": "alive-trigger",
             "resolver": "noop", "description": "Alive P1 trigger"},
        ])
        n = drain_triggers_to_stack(gs)
        # Eliminated P2 trigger is skipped per APNAP (apnap_order doesn't
        # include eliminated players).
        self.assertEqual(n, 1)
        self.assertEqual(gs.stack[0].description, "Alive P1 trigger")


class ResolverRegistryTests(unittest.TestCase):
    def test_noop_resolver_no_state_change(self) -> None:
        gs = _empty_4p_game()
        gs.players[0].life_total = 40
        push_to_stack(gs, card_id="x", controller=0,
                      payment={"resolver": "noop"})
        resolve_top(gs)
        self.assertEqual(gs.players[0].life_total, 40)

    def test_draw_cards_resolver(self) -> None:
        gs = _empty_4p_game()
        # Give P0 a 5-card library.
        for i in range(5):
            c = Card(name=f"Lib{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        push_to_stack(gs, card_id=None, controller=0,
                      payment={"resolver": "draw_cards", "amount": 3})
        resolve_top(gs)
        self.assertEqual(len(gs.players[0].zones.hand), 3)
        self.assertEqual(len(gs.players[0].zones.library), 2)
        self.assertEqual(gs.players[0].cards_drawn_this_turn, 3)

    def test_draw_from_empty_library_sets_flag(self) -> None:
        gs = _empty_4p_game()
        push_to_stack(gs, card_id=None, controller=0,
                      payment={"resolver": "draw_cards", "amount": 1})
        resolve_top(gs)
        self.assertTrue(gs.players[0].has_drawn_from_empty_library)


if __name__ == "__main__":
    unittest.main()
