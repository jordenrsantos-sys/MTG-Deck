"""Phase 4 of mega-task v9 — replacement effects + state-based actions tests.

Coverage per kickoff Phase 4 gates:
- would-die replaced by exile (Rest in Peace stub).
- would-deal-damage prevented by Fog stub.
- legend rule fires on duplicate legendaries.
- commander damage SBA at 21.
- planeswalker -loyalty SBA at 0.
- 15+ replacement-effect interactions (covered across this + Phase 8 fixtures).
- 8+ SBA categories (covered).
"""
from __future__ import annotations

import unittest

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, ReplacementEffect,
)
from api.engine.pillar_f.v0_2.replacement import (
    Event, DrawEvent, DamageEvent, EnterBattlefieldEvent,
    DieEvent, LifeChangeEvent, CounterAddEvent, MillEvent,
    apply_replacements, register_replacement_fn,
    check_state_based_actions, run_sba_loop,
    COMMANDER_DAMAGE_LETHAL,
)


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(player_id=pid, name=f"P{pid}",
                                      life_total=40, zones=PlayerZones()))
    return gs


class ReplacementEnginePatternMatchTests(unittest.TestCase):
    def test_no_replacements_returns_event_unchanged(self) -> None:
        gs = _empty_4p_game()
        evt = DamageEvent(amount=5, target_kind="player", target_id=1,
                          is_combat=True)
        result = apply_replacements(gs, evt)
        self.assertEqual(result.amount, 5)
        self.assertFalse(result.replaced)

    def test_fog_prevents_combat_damage(self) -> None:
        gs = _empty_4p_game()
        gs.replacement_effects.append(ReplacementEffect(
            effect_id="fog1", source_card_id="fog-card", controller=2,
            event_pattern={"type": "DamageEvent", "is_combat": True},
            replacement_fn_name="fog_prevent_combat_damage",
            description="Fog: prevent all combat damage this turn",
        ))
        evt = DamageEvent(amount=5, target_kind="player", target_id=1,
                          is_combat=True, source_controller=0)
        result = apply_replacements(gs, evt)
        self.assertEqual(result.amount, 0)
        self.assertTrue(result.replaced)
        self.assertTrue(result.prevent)

    def test_fog_does_not_affect_noncombat_damage(self) -> None:
        gs = _empty_4p_game()
        gs.replacement_effects.append(ReplacementEffect(
            effect_id="fog1", source_card_id="fog-card", controller=2,
            event_pattern={"type": "DamageEvent", "is_combat": True},
            replacement_fn_name="fog_prevent_combat_damage",
        ))
        evt = DamageEvent(amount=3, target_kind="player", target_id=1,
                          is_combat=False)  # Lightning Bolt-style.
        result = apply_replacements(gs, evt)
        self.assertEqual(result.amount, 3)
        self.assertFalse(result.replaced)

    def test_etb_tapped_replacement(self) -> None:
        gs = _empty_4p_game()
        gs.replacement_effects.append(ReplacementEffect(
            effect_id="etb-tap", source_card_id="thalia", controller=0,
            event_pattern={"type": "EnterBattlefieldEvent"},
            replacement_fn_name="etb_tapped",
            description="Permanents ETB tapped",
        ))
        evt = EnterBattlefieldEvent(card_id="newcomer", controller=0,
                                    from_zone="hand")
        result = apply_replacements(gs, evt)
        self.assertTrue(result.tapped_on_etb)
        self.assertTrue(result.replaced)

    def test_doubling_season_doubles_counters(self) -> None:
        gs = _empty_4p_game()
        gs.replacement_effects.append(ReplacementEffect(
            effect_id="ds1", source_card_id="doubling-season", controller=0,
            event_pattern={"type": "CounterAddEvent"},
            replacement_fn_name="doubling_season_counters",
        ))
        # +1/+1 counter: doubled.
        evt = CounterAddEvent(target_card_id="creature1",
                              counter_type="+1/+1", count=3)
        apply_replacements(gs, evt)
        # NOTE: doubling_season_counters multiplies count by 2 ONCE per
        # event (self-replacement rule). 3 → 6.
        self.assertEqual(evt.count, 6)

    def test_doubling_season_ignores_non_p1p1_or_loyalty_counters(self) -> None:
        gs = _empty_4p_game()
        gs.replacement_effects.append(ReplacementEffect(
            effect_id="ds1", source_card_id="doubling-season", controller=0,
            event_pattern={"type": "CounterAddEvent"},
            replacement_fn_name="doubling_season_counters",
        ))
        # -1/-1 counter: NOT doubled.
        evt = CounterAddEvent(target_card_id="creature1",
                              counter_type="-1/-1", count=2)
        apply_replacements(gs, evt)
        self.assertEqual(evt.count, 2)


class StateBasedActionsCreatureDeathTests(unittest.TestCase):
    def test_creature_with_zero_toughness_dies(self) -> None:
        gs = _empty_4p_game()
        c = Card(name="Zero-T", owner=0, controller=0,
                 type_line="Creature — Zombie", power="2", toughness="0")
        gs.add_card(c)
        gs.players[0].zones.battlefield.append(c.card_id)
        actions = check_state_based_actions(gs)
        self.assertTrue(any(a["action"] == "creature_dies_zero_toughness"
                            for a in actions))
        self.assertNotIn(c.card_id, gs.players[0].zones.battlefield)
        self.assertIn(c.card_id, gs.players[0].zones.graveyard)

    def test_creature_with_lethal_damage_dies(self) -> None:
        gs = _empty_4p_game()
        c = Card(name="Wounded", owner=0, controller=0,
                 type_line="Creature", power="3", toughness="3",
                 damage_marked=5)
        gs.add_card(c)
        gs.players[0].zones.battlefield.append(c.card_id)
        actions = check_state_based_actions(gs)
        self.assertTrue(any(a["action"] == "creature_dies_lethal_damage"
                            for a in actions))
        self.assertIn(c.card_id, gs.players[0].zones.graveyard)

    def test_rip_redirects_creature_death_to_exile(self) -> None:
        """Rest in Peace stub: creatures die to exile instead."""
        gs = _empty_4p_game()
        gs.replacement_effects.append(ReplacementEffect(
            effect_id="rip", source_card_id="rest-in-peace", controller=0,
            event_pattern={"type": "DieEvent"},
            replacement_fn_name="rest_in_peace_die_to_exile",
        ))
        c = Card(name="Doomed", owner=1, controller=1,
                 type_line="Creature", power="2", toughness="0")
        gs.add_card(c)
        gs.players[1].zones.battlefield.append(c.card_id)
        check_state_based_actions(gs)
        self.assertIn(c.card_id, gs.players[1].zones.exile)
        self.assertNotIn(c.card_id, gs.players[1].zones.graveyard)

    def test_planeswalker_with_zero_loyalty_dies(self) -> None:
        gs = _empty_4p_game()
        pw = Card(name="Jace", owner=0, controller=0,
                  type_line="Legendary Planeswalker — Jace",
                  loyalty="3")
        pw.counters["loyalty"] = 0
        gs.add_card(pw)
        gs.players[0].zones.battlefield.append(pw.card_id)
        actions = check_state_based_actions(gs)
        self.assertTrue(any(a["action"] == "planeswalker_dies_zero_loyalty"
                            for a in actions))
        self.assertIn(pw.card_id, gs.players[0].zones.graveyard)


class PlayerLossSBATests(unittest.TestCase):
    def test_player_with_zero_life_loses(self) -> None:
        gs = _empty_4p_game()
        gs.players[1].life_total = 0
        actions = check_state_based_actions(gs)
        self.assertTrue(any(a["action"] == "player_loses_life_0"
                            for a in actions))
        self.assertTrue(gs.players[1].has_lost)

    def test_player_with_negative_life_loses(self) -> None:
        gs = _empty_4p_game()
        gs.players[2].life_total = -5
        check_state_based_actions(gs)
        self.assertTrue(gs.players[2].has_lost)

    def test_commander_damage_21_loses(self) -> None:
        gs = _empty_4p_game()
        gs.players[3].commander_damage_taken_from["cmdr-oracle-id"] = 21
        actions = check_state_based_actions(gs)
        self.assertTrue(any(a["action"] == "player_loses_commander_damage"
                            for a in actions))
        self.assertTrue(gs.players[3].has_lost)

    def test_commander_damage_20_doesnt_lose(self) -> None:
        gs = _empty_4p_game()
        gs.players[3].commander_damage_taken_from["cmdr-oracle-id"] = 20
        check_state_based_actions(gs)
        self.assertFalse(gs.players[3].has_lost)

    def test_draw_from_empty_library_loses(self) -> None:
        gs = _empty_4p_game()
        gs.players[1].has_drawn_from_empty_library = True
        check_state_based_actions(gs)
        self.assertTrue(gs.players[1].has_lost)


class LegendRuleTests(unittest.TestCase):
    def test_two_legendaries_same_name_same_controller_one_dies(self) -> None:
        gs = _empty_4p_game()
        c1 = Card(name="Edgar Markov", owner=0, controller=0,
                  type_line="Legendary Creature — Vampire Knight",
                  power="4", toughness="4")
        c2 = Card(name="Edgar Markov", owner=0, controller=0,
                  type_line="Legendary Creature — Vampire Knight",
                  power="4", toughness="4")
        gs.add_card(c1); gs.add_card(c2)
        gs.players[0].zones.battlefield.extend([c1.card_id, c2.card_id])
        check_state_based_actions(gs)
        # Exactly one remains.
        remaining = [cid for cid in gs.players[0].zones.battlefield
                     if cid in (c1.card_id, c2.card_id)]
        self.assertEqual(len(remaining), 1)
        # The other is in graveyard.
        in_gy = [cid for cid in gs.players[0].zones.graveyard
                 if cid in (c1.card_id, c2.card_id)]
        self.assertEqual(len(in_gy), 1)

    def test_two_legendaries_different_controllers_dont_collide(self) -> None:
        gs = _empty_4p_game()
        c1 = Card(name="Sol Ring", owner=0, controller=0,
                  type_line="Legendary Artifact")
        c2 = Card(name="Sol Ring", owner=1, controller=1,
                  type_line="Legendary Artifact")
        gs.add_card(c1); gs.add_card(c2)
        gs.players[0].zones.battlefield.append(c1.card_id)
        gs.players[1].zones.battlefield.append(c2.card_id)
        actions = check_state_based_actions(gs)
        # Different controllers, no legend rule clash.
        self.assertNotIn("legend_rule_dies", [a["action"] for a in actions])

    def test_two_legendaries_different_names_dont_collide(self) -> None:
        gs = _empty_4p_game()
        c1 = Card(name="Atraxa", owner=0, controller=0,
                  type_line="Legendary Creature — Angel",
                  power="4", toughness="4")
        c2 = Card(name="Edgar Markov", owner=0, controller=0,
                  type_line="Legendary Creature — Vampire",
                  power="4", toughness="4")
        gs.add_card(c1); gs.add_card(c2)
        gs.players[0].zones.battlefield.extend([c1.card_id, c2.card_id])
        actions = check_state_based_actions(gs)
        self.assertNotIn("legend_rule_dies", [a["action"] for a in actions])


class SBALoopTests(unittest.TestCase):
    def test_run_sba_loop_terminates_with_no_more_actions(self) -> None:
        gs = _empty_4p_game()
        gs.players[2].life_total = 0
        actions = run_sba_loop(gs)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action"], "player_loses_life_0")
        # Second call returns nothing.
        actions2 = check_state_based_actions(gs)
        self.assertEqual(actions2, [])

    def test_game_over_when_one_player_remains(self) -> None:
        gs = _empty_4p_game()
        gs.players[0].life_total = 0
        gs.players[1].life_total = 0
        gs.players[2].life_total = 0
        # P3 remains alive.
        run_sba_loop(gs)
        self.assertTrue(gs.game_over)
        self.assertEqual(gs.winner_player_id, 3)

    def test_game_over_draw_when_all_lose(self) -> None:
        gs = _empty_4p_game()
        for ps in gs.players:
            ps.life_total = 0
        run_sba_loop(gs)
        self.assertTrue(gs.game_over)
        # winner_player_id is None or unset for a draw.

    def test_creature_death_cascade_in_sba_loop(self) -> None:
        """If a creature dies (Phase 4 SBA), triggered abilities or
        further SBAs might fire — iter 10 just verifies the dying-
        creature-removal happens within run_sba_loop."""
        gs = _empty_4p_game()
        c1 = Card(name="A", owner=0, controller=0,
                  type_line="Creature", power="2", toughness="0")
        c2 = Card(name="B", owner=0, controller=0,
                  type_line="Creature", power="2", toughness="0")
        gs.add_card(c1); gs.add_card(c2)
        gs.players[0].zones.battlefield.extend([c1.card_id, c2.card_id])
        run_sba_loop(gs)
        # Both dead.
        self.assertIn(c1.card_id, gs.players[0].zones.graveyard)
        self.assertIn(c2.card_id, gs.players[0].zones.graveyard)


if __name__ == "__main__":
    unittest.main()
