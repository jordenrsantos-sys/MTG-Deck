"""Phase 2 of mega-task v12 -- counter-war hook unit tests.

Covers sub-B Phase 9 deferred gate 6:
- All 14 counterspell-family names are registered (v11 ships 7, sub-C
  backfills the other 7).
- make_counterspell_annotation builds the right iter10_annotation
  (resolver key from v11 registry, target_stack_top flag).
- compute_eligible_actions honors target_stack_top: emits cast_spell
  ONLY when state.stack is non-empty, with default_targets resolved
  to top entry's entry_id.
- Counter resolver removes the targeted stack entry on resolution.
- Counter chain depth 3 resolves correctly (A casts spell -> B counters
  -> C counters B -> D counters C -> all resolve in stack order with
  the right surviving spells).

Live multi-LLM counter-density integration deferred to Phase 7.
"""
from __future__ import annotations

import unittest

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, Step, Phase, StackEntry,
)
from api.engine.pillar_f.v0_2.stack import (
    push_to_stack, resolve_top, counter_target,
)
from api.engine.pillar_f.v0_2.policy import (
    compute_eligible_actions, apply_action,
)
from api.engine.pillar_f.v0_2.playtest.counter_war import (
    COUNTERSPELL_FAMILY_NAMES,
    make_counterspell_annotation,
    attach_counterspell_annotation,
)
from api.engine.pillar_f.v0_2.cards.spell.framework import (
    get_spell_resolver_key,
)


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(player_id=pid, name=f"P{pid}",
                                      life_total=40, zones=PlayerZones()))
    gs.active_player = 0
    gs.step = Step.MAIN_1
    gs.phase = Phase.PRECOMBAT_MAIN
    gs.turn_number = 1
    return gs


def _add_counter_to_hand(gs: GameState, player_id: int,
                         card_name: str = "Counterspell") -> Card:
    c = Card(name=card_name, owner=player_id, controller=player_id,
             type_line="Instant", mana_cost="{U}{U}")
    attach_counterspell_annotation(c)
    gs.add_card(c)
    gs.players[player_id].zones.hand.append(c.card_id)
    return c


# ============================================================
# Registration coverage
# ============================================================


class CounterspellRegistrationTests(unittest.TestCase):
    def test_all_14_counterspells_registered(self) -> None:
        for name in COUNTERSPELL_FAMILY_NAMES:
            key = get_spell_resolver_key(name)
            self.assertIsNotNone(
                key, msg=f"No resolver for counterspell {name!r}",
            )

    def test_make_annotation_for_counterspell(self) -> None:
        ann = make_counterspell_annotation("Counterspell")
        self.assertTrue(ann["target_stack_top"])
        self.assertEqual(ann["payment"]["resolver"], "spell_counterspell")
        self.assertIn("counter target spell", ann["description"].lower())

    def test_make_annotation_for_force_of_will(self) -> None:
        """FoW is one of the 7 v11 didn't ship; sub-C backfills."""
        ann = make_counterspell_annotation("Force of Will")
        self.assertTrue(ann["target_stack_top"])
        self.assertEqual(ann["payment"]["resolver"], "spell_force_of_will")

    def test_make_annotation_unknown_card_rejected(self) -> None:
        with self.assertRaises(ValueError):
            make_counterspell_annotation("Lightning Bolt")


# ============================================================
# compute_eligible_actions honors target_stack_top
# ============================================================


class EligibleActionsCounterTests(unittest.TestCase):
    def test_counter_NOT_eligible_when_stack_empty(self) -> None:
        gs = _empty_4p_game()
        _add_counter_to_hand(gs, 0)
        actions = compute_eligible_actions(gs, 0)
        cast_actions = [a for a in actions if a["action_type"] == "cast_spell"]
        # Stack is empty -> no legal target -> no cast action emitted.
        self.assertEqual(cast_actions, [])

    def test_counter_eligible_when_stack_non_empty(self) -> None:
        gs = _empty_4p_game()
        c = _add_counter_to_hand(gs, 0)
        # Put a spell on the stack (e.g., an opponent's Lightning Bolt).
        push_to_stack(
            gs, card_id="some-bolt", controller=1,
            entry_type="spell",
            description="Lightning Bolt (P1)",
            payment={"resolver": "noop"},
        )
        actions = compute_eligible_actions(gs, 0)
        cast_actions = [a for a in actions if a["action_type"] == "cast_spell"]
        self.assertEqual(len(cast_actions), 1)
        # default_targets should be set to top entry's entry_id.
        top_entry_id = gs.stack[-1].entry_id
        self.assertEqual(cast_actions[0]["targets"], [top_entry_id])

    def test_counter_targets_resolved_at_cast_time(self) -> None:
        """If two spells are on the stack, the counter should target the
        TOP entry (most recent), not the bottom one."""
        gs = _empty_4p_game()
        _add_counter_to_hand(gs, 0)
        push_to_stack(gs, card_id="spell-A", controller=1,
                      entry_type="spell",
                      payment={"resolver": "noop"},
                      description="Spell A")
        push_to_stack(gs, card_id="spell-B", controller=2,
                      entry_type="spell",
                      payment={"resolver": "noop"},
                      description="Spell B")
        top_id = gs.stack[-1].entry_id
        actions = compute_eligible_actions(gs, 0)
        cast_actions = [a for a in actions if a["action_type"] == "cast_spell"]
        self.assertEqual(cast_actions[0]["targets"], [top_id])


# ============================================================
# Counter resolver removes targeted stack entry
# ============================================================


class CounterResolutionTests(unittest.TestCase):
    def test_counterspell_removes_target_from_stack(self) -> None:
        gs = _empty_4p_game()
        c = _add_counter_to_hand(gs, 0)
        # Put a target spell on the stack.
        push_to_stack(gs, card_id="bolt", controller=1,
                      entry_type="spell",
                      payment={"resolver": "noop"},
                      description="Lightning Bolt (P1)")
        target_entry_id = gs.stack[-1].entry_id
        # Compute eligible + apply the counter cast.
        actions = compute_eligible_actions(gs, 0)
        cast = [a for a in actions if a["action_type"] == "cast_spell"][0]
        apply_action(gs, 0, cast)
        # Stack should now have 2 entries (bolt + counterspell).
        self.assertEqual(len(gs.stack), 2)
        # Resolve the counter (top of stack).
        resolved = resolve_top(gs)
        self.assertIsNotNone(resolved)
        # Stack should now be empty -- bolt was countered.
        self.assertEqual(len(gs.stack), 0)

    def test_substrate_counter_target_helper_works(self) -> None:
        """Sanity check on substrate's counter_target primitive."""
        gs = _empty_4p_game()
        push_to_stack(gs, card_id="target", controller=1,
                      payment={"resolver": "noop"})
        target_id = gs.stack[-1].entry_id
        ok = counter_target(gs, target_id)
        self.assertTrue(ok)
        self.assertEqual(len(gs.stack), 0)


# ============================================================
# Counter-chain depth-3 integration test
# ============================================================


class CounterChainDepth3Test(unittest.TestCase):
    """Scenario: P1 casts Spell -> P2 Counterspells -> P3 Counterspells
    P2 -> P0 Counterspells P3. Resolution order (top of stack first):

      P0's counter resolves first -> P3's counter (the one P0 targeted)
                                     is countered + leaves stack.
      P2's counter resolves next -> P1's spell is countered + leaves
                                    stack.
      Final state: stack empty; only P1's original spell was countered.

    Implementation note: counter_target removes by entry_id from
    state.stack regardless of position. So even though P3's counter
    targeted P2's counter, after P0 counters P3, P2's counter SURVIVES
    and goes on to counter P1's spell.

    This exercises the response-window prompt path AND the
    target_stack_top resolution AND substrate's counter_target.
    """
    def test_counter_chain_depth_3_resolves_correctly(self) -> None:
        gs = _empty_4p_game()
        # All 4 players have Counterspell in hand.
        c_p0 = _add_counter_to_hand(gs, 0, "Counterspell")
        c_p2 = _add_counter_to_hand(gs, 2, "Counterspell")
        c_p3 = _add_counter_to_hand(gs, 3, "Counterspell")
        # P1 casts a Lightning Bolt-style spell.
        push_to_stack(
            gs, card_id="p1-spell", controller=1,
            entry_type="spell",
            payment={"resolver": "noop"},
            description="P1's Spell",
        )
        p1_spell_id = gs.stack[-1].entry_id
        # P2 casts Counterspell on P1's spell.
        actions_p2 = compute_eligible_actions(gs, 2)
        cast_p2 = [a for a in actions_p2 if a["action_type"] == "cast_spell"][0]
        # Verify it targets P1's spell.
        self.assertEqual(cast_p2["targets"], [p1_spell_id])
        apply_action(gs, 2, cast_p2)
        p2_counter_id = gs.stack[-1].entry_id
        # P3 casts Counterspell on P2's counter.
        actions_p3 = compute_eligible_actions(gs, 3)
        cast_p3 = [a for a in actions_p3 if a["action_type"] == "cast_spell"][0]
        self.assertEqual(cast_p3["targets"], [p2_counter_id])
        apply_action(gs, 3, cast_p3)
        p3_counter_id = gs.stack[-1].entry_id
        # P0 casts Counterspell on P3's counter.
        actions_p0 = compute_eligible_actions(gs, 0)
        cast_p0 = [a for a in actions_p0 if a["action_type"] == "cast_spell"][0]
        self.assertEqual(cast_p0["targets"], [p3_counter_id])
        apply_action(gs, 0, cast_p0)

        # Now stack is depth-4: [P1-spell, P2-counter, P3-counter, P0-counter]
        self.assertEqual(len(gs.stack), 4)
        # Resolve top: P0's counter resolves -> P3's counter is removed.
        resolve_top(gs)
        self.assertEqual(len(gs.stack), 2)
        # Remaining: [P1-spell, P2-counter].
        # Now resolve top again: P2's counter resolves -> P1's spell removed.
        resolve_top(gs)
        self.assertEqual(len(gs.stack), 0)
        # Final: P1's spell was countered (P2 succeeded); P2's counter
        # is in P2's graveyard (resolved); P3's counter is in P3's
        # graveyard (countered before resolving -- substrate's
        # counter_target removes from stack; for iter-10 simplification
        # this just removes-from-stack without explicitly graveyarding
        # the countered card. Not testing graveyard contents here as
        # iter-10's spell-resolver path varies per resolver.)

    def test_double_counter_chain_depth_2_resolves(self) -> None:
        """Simpler version: P1 casts spell, P2 counters, P3 counters P2.
        Resolution: P3's counter removes P2's counter; P1's spell then
        resolves uncountered."""
        gs = _empty_4p_game()
        _add_counter_to_hand(gs, 2)
        _add_counter_to_hand(gs, 3)
        push_to_stack(
            gs, card_id="p1-spell", controller=1,
            entry_type="spell",
            payment={"resolver": "noop"},
            description="P1's Spell",
        )
        actions_p2 = compute_eligible_actions(gs, 2)
        apply_action(
            gs, 2,
            [a for a in actions_p2 if a["action_type"] == "cast_spell"][0],
        )
        actions_p3 = compute_eligible_actions(gs, 3)
        apply_action(
            gs, 3,
            [a for a in actions_p3 if a["action_type"] == "cast_spell"][0],
        )
        # Depth-3: [P1-spell, P2-counter, P3-counter].
        self.assertEqual(len(gs.stack), 3)
        # Resolve top: P3's counter resolves -> P2's counter is removed.
        resolve_top(gs)
        self.assertEqual(len(gs.stack), 1)
        # Stack: [P1-spell]. P1's spell still on stack -- will resolve
        # via noop.
        resolve_top(gs)
        self.assertEqual(len(gs.stack), 0)


if __name__ == "__main__":
    unittest.main()
