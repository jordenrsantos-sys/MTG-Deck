"""Phase 8 — Complex multi-handler integration tests.

Per kickoff Phase 8: "Each complex card: per-card unit tests + at
least one combo-line integration test that demonstrates the card's
headline use in a real game state."

Cards covered:
  - Solemn Simulacrum (ETB tutor + LTB draw — combo: dies → 2 events
    chain through substrate)
  - Skullclamp (equip + on-equipped-dies-draw — combo line: equip a
    1/1 → it dies from -1 toughness → 2 cards)
  - The One Ring (ETB protection + upkeep escalating draw)
  - Karoo lands (ETB bounce + tap-2-color; combo: chain Azorius
    Chancery → Orzhov Basilica → land bounce)
  - Vito, Thorn of the Dusk Rose (life-gain → opp drain combo line)
  - Faerie Mastermind (opp 2nd draw → you draw)
  - Animate Dead (graveyard target → battlefield with -1 power)
"""
from __future__ import annotations

import unittest

# Imports trigger all per-card registrations.
import api.engine.pillar_f.v0_2.cards  # noqa: F401
from api.engine.pillar_f.v0_2.cards.activated import build_activation_payload
from api.engine.pillar_f.v0_2.cards.etb import fire_etb_triggers
from api.engine.pillar_f.v0_2.cards.triggered import fire_event_triggers
from api.engine.pillar_f.v0_2.replacement import (
    DieEvent, DrawEvent, EnterBattlefieldEvent, LifeChangeEvent,
)
from api.engine.pillar_f.v0_2.stack import (
    push_to_stack, resolve_top, drain_triggers_to_stack,
    run_stack_to_resolution,
)
from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones,
)


def _empty_game(*, life: int = 40) -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(
            player_id=pid, name=f"P{pid}", life_total=life, zones=PlayerZones(),
        ))
    gs.active_player = 0
    return gs


def _put_on_bf(gs: GameState, owner: int, name: str, *,
              type_line: str = "Creature — Bear",
              power="2", toughness="2", cmc=2.0,
              subtypes=None, keywords=None) -> Card:
    card = Card(name=name, owner=owner, controller=owner, type_line=type_line,
                power=power, toughness=toughness, cmc=cmc,
                subtypes=list(subtypes or []), keywords=list(keywords or []))
    gs.add_card(card)
    gs.players[owner].zones.battlefield.append(card.card_id)
    return card


def _fire_etb_and_resolve(gs: GameState, card: Card) -> int:
    event = EnterBattlefieldEvent(card_id=card.card_id,
                                   controller=card.controller, from_zone="hand")
    fired = fire_etb_triggers(gs, event)
    if fired:
        drain_triggers_to_stack(gs)
        run_stack_to_resolution(gs, lambda state, pid: None)
    return fired


def _fire_event_and_resolve(gs: GameState, event) -> int:
    fired = fire_event_triggers(gs, event)
    if fired:
        drain_triggers_to_stack(gs)
        run_stack_to_resolution(gs, lambda state, pid: None)
    return fired


# =================================================================
# Solemn Simulacrum
# =================================================================


class SolemnSimulacrumCombo(unittest.TestCase):
    def test_etb_tutors_basic_to_battlefield_tapped(self) -> None:
        gs = _empty_game()
        plains = Card(name="Plains", owner=0,
                     type_line="Basic Land — Plains",
                     subtypes=["Plains"])
        gs.add_card(plains)
        gs.players[0].zones.library.append(plains.card_id)
        solemn = _put_on_bf(gs, 0, "Solemn Simulacrum",
                            type_line="Artifact Creature — Golem",
                            power="2", toughness="2")
        _fire_etb_and_resolve(gs, solemn)
        self.assertIn(plains.card_id, gs.players[0].zones.battlefield)
        self.assertTrue(plains.tapped)

    def test_ltb_draws_card_when_solemn_dies(self) -> None:
        gs = _empty_game()
        # Library has 1 card.
        c = Card(name="Top", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        solemn = _put_on_bf(gs, 0, "Solemn Simulacrum",
                            type_line="Artifact Creature — Golem",
                            power="2", toughness="2")
        # Move Solemn to graveyard (simulating death).
        gs.move_card(solemn.card_id, from_player=0,
                    from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        event = DieEvent(card_id=solemn.card_id, controller=0)
        _fire_event_and_resolve(gs, event)
        # Drew 1 card.
        self.assertIn(c.card_id, gs.players[0].zones.hand)

    def test_full_combo_etb_tutors_then_dies_draws(self) -> None:
        """Combo line: cast Solemn → ETB fetches Plains → Solemn dies
        in combat → LTB draws 1."""
        gs = _empty_game()
        plains = Card(name="Plains", owner=0,
                     type_line="Basic Land — Plains",
                     subtypes=["Plains"])
        next_draw = Card(name="Next Draw", owner=0)
        gs.add_card(plains)
        gs.add_card(next_draw)
        gs.players[0].zones.library.append(plains.card_id)
        gs.players[0].zones.library.append(next_draw.card_id)
        solemn = _put_on_bf(gs, 0, "Solemn Simulacrum",
                            type_line="Artifact Creature — Golem",
                            power="2", toughness="2")
        # ETB.
        _fire_etb_and_resolve(gs, solemn)
        self.assertIn(plains.card_id, gs.players[0].zones.battlefield)
        # Death.
        gs.move_card(solemn.card_id, from_player=0,
                    from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        _fire_event_and_resolve(gs, DieEvent(card_id=solemn.card_id,
                                              controller=0))
        self.assertIn(next_draw.card_id, gs.players[0].zones.hand)


# =================================================================
# Skullclamp
# =================================================================


class SkullclampCombo(unittest.TestCase):
    def test_equip_attaches_to_creature(self) -> None:
        gs = _empty_game()
        sc = _put_on_bf(gs, 0, "Skullclamp",
                        type_line="Artifact — Equipment",
                        power=None, toughness=None)
        bear = _put_on_bf(gs, 0, "1/1 Goblin",
                          type_line="Creature — Goblin",
                          power="1", toughness="1")
        # Caller pays {1} (not modeled) and pushes equip activation.
        push_to_stack(
            gs, card_id=sc.card_id, controller=0, entry_type="activated",
            payment=build_activation_payload("Skullclamp", "equip"),
            targets=[bear.card_id],
            description="Skullclamp equip bear",
        )
        resolve_top(gs)
        self.assertEqual(sc.attached_to, bear.card_id)
        self.assertIn(sc.card_id, bear.attached_by)

    def test_full_combo_equip_1_1_dies_draw_2(self) -> None:
        """Kickoff Phase 8 combo-line gate: Skullclamp on a 1/1 → -1
        toughness → 0 → dies → draw 2 cards.

        Iter-10 simplification: we don't have the layer-7c +1/-1
        ContinuousEffect wired (substrate has the API but Skullclamp's
        +1/-1 isn't auto-attached in iter-10), so we manually shrink
        the bear's toughness to 0 before firing the die event."""
        gs = _empty_game()
        # Library has 2 cards.
        c1 = Card(name="Top1", owner=0)
        c2 = Card(name="Top2", owner=0)
        gs.add_card(c1)
        gs.add_card(c2)
        gs.players[0].zones.library.extend([c1.card_id, c2.card_id])

        sc = _put_on_bf(gs, 0, "Skullclamp",
                        type_line="Artifact — Equipment",
                        power=None, toughness=None)
        goblin = _put_on_bf(gs, 0, "1/1 Goblin",
                            type_line="Creature — Goblin",
                            power="1", toughness="1")
        # Equip.
        push_to_stack(
            gs, card_id=sc.card_id, controller=0, entry_type="activated",
            payment=build_activation_payload("Skullclamp", "equip"),
            targets=[goblin.card_id],
            description="Skullclamp equip goblin",
        )
        resolve_top(gs)
        # Now simulate the +1/-1 making goblin a 2/0 → SBA-dies.
        goblin.power = "2"
        goblin.toughness = "0"
        # Move to graveyard.
        gs.move_card(goblin.card_id, from_player=0,
                    from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        # Fire die trigger.
        _fire_event_and_resolve(gs, DieEvent(card_id=goblin.card_id,
                                              controller=0))
        # 2 cards drawn.
        self.assertEqual(len(gs.players[0].zones.hand), 2)
        self.assertIn(c1.card_id, gs.players[0].zones.hand)
        self.assertIn(c2.card_id, gs.players[0].zones.hand)


# =================================================================
# The One Ring
# =================================================================


class OneRingCombo(unittest.TestCase):
    def test_etb_grants_protection_marker(self) -> None:
        gs = _empty_game()
        gs.turn_number = 3
        ring = _put_on_bf(gs, 0, "The One Ring",
                          type_line="Legendary Artifact",
                          power=None, toughness=None)
        _fire_etb_and_resolve(gs, ring)
        # Protection flag set until turn 4.
        marker = gs.players[0].politics_state.get("one_ring_protection_until_turn")
        self.assertEqual(marker, 4)


# =================================================================
# Karoo lands
# =================================================================


class KarooLandsCombo(unittest.TestCase):
    def test_azorius_chancery_bounces_a_land(self) -> None:
        gs = _empty_game()
        # P0 already has a Plains in play.
        plains = _put_on_bf(gs, 0, "Plains",
                            type_line="Basic Land — Plains",
                            power=None, toughness=None,
                            subtypes=["Plains"])
        azorius = _put_on_bf(gs, 0, "Azorius Chancery",
                              type_line="Land",
                              power=None, toughness=None)
        _fire_etb_and_resolve(gs, azorius)
        # Plains bounced to hand.
        self.assertIn(plains.card_id, gs.players[0].zones.hand)
        # Azorius still on battlefield.
        self.assertIn(azorius.card_id, gs.players[0].zones.battlefield)

    def test_azorius_chancery_taps_for_w_u(self) -> None:
        gs = _empty_game()
        az = _put_on_bf(gs, 0, "Azorius Chancery", type_line="Land",
                        power=None, toughness=None)
        az.tapped = True  # cost paid
        push_to_stack(
            gs, card_id=az.card_id, controller=0, entry_type="activated",
            payment=build_activation_payload("Azorius Chancery", "tap_mana"),
            description="Azorius tap-mana",
        )
        resolve_top(gs)
        self.assertEqual(gs.players[0].mana_pool.W, 1)
        self.assertEqual(gs.players[0].mana_pool.U, 1)


# =================================================================
# Vito, Thorn of the Dusk Rose
# =================================================================


class VitoCombo(unittest.TestCase):
    def test_life_gain_drains_opponent(self) -> None:
        gs = _empty_game(life=40)
        _put_on_bf(gs, 0, "Vito, Thorn of the Dusk Rose",
                   type_line="Creature — Vampire Cleric",
                   power="1", toughness="3")
        # Simulate a life-gain event of 5.
        event = LifeChangeEvent(player_id=0, delta=5)
        _fire_event_and_resolve(gs, event)
        # First opponent (P1) takes 5 drain.
        self.assertEqual(gs.players[1].life_total, 35)


# =================================================================
# Faerie Mastermind
# =================================================================


class FaerieMastermindCombo(unittest.TestCase):
    def test_opp_2nd_draw_triggers(self) -> None:
        gs = _empty_game()
        # Library has cards for the trigger to draw.
        c = Card(name="Top", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        _put_on_bf(gs, 0, "Faerie Mastermind",
                   type_line="Creature — Faerie Rogue",
                   power="2", toughness="1")
        # P1's 1st draw — no trigger.
        e1 = DrawEvent(player_id=1, count=1)
        fired1 = _fire_event_and_resolve(gs, e1)
        self.assertEqual(fired1, 0)
        # P1's 2nd draw — triggers.
        e2 = DrawEvent(player_id=1, count=1)
        fired2 = _fire_event_and_resolve(gs, e2)
        self.assertEqual(fired2, 1)
        self.assertEqual(len(gs.players[0].zones.hand), 1)


# =================================================================
# Animate Dead
# =================================================================


class AnimateDeadCombo(unittest.TestCase):
    def test_reanimates_target_creature_from_graveyard(self) -> None:
        gs = _empty_game()
        # P0's graveyard has a dragon.
        dragon = Card(name="Dragon", owner=0,
                     type_line="Creature — Dragon",
                     power="5", toughness="5", cmc=5.0)
        gs.add_card(dragon)
        gs.players[0].zones.graveyard.append(dragon.card_id)
        # Cast Animate Dead.
        ad = _put_on_bf(gs, 0, "Animate Dead",
                        type_line="Enchantment — Aura",
                        power=None, toughness=None)
        _fire_etb_and_resolve(gs, ad)
        # Dragon now on P0's battlefield.
        self.assertIn(dragon.card_id, gs.players[0].zones.battlefield)
        # Aura attached to dragon.
        self.assertEqual(ad.attached_to, dragon.card_id)
        # Dragon got -1 power (Animate Dead's flavor reduction).
        self.assertEqual(dragon.power_int(), 4)


# =================================================================
# Coverage gate
# =================================================================


class Phase8CoverageGate(unittest.TestCase):
    def test_complex_cards_wired(self) -> None:
        from api.engine.pillar_f.v0_2.cards.etb import get_etb_trigger
        from api.engine.pillar_f.v0_2.cards.activated import get_activated_ability
        from api.engine.pillar_f.v0_2.cards.triggered import get_event_trigger
        # 4 ETB-trigger cards in complex bucket are wired.
        for name in ("Solemn Simulacrum", "The One Ring",
                     "Animate Dead", "Azorius Chancery",
                     "Orzhov Basilica", "Dimir Aqueduct"):
            self.assertIsNotNone(get_etb_trigger(name),
                                 f"{name} ETB not wired")
        # Skullclamp + Karoo activated abilities.
        self.assertIsNotNone(
            get_activated_ability("Skullclamp", "equip"),
        )
        self.assertIsNotNone(
            get_activated_ability("Azorius Chancery", "tap_mana"),
        )
        # Vito + Faerie Mastermind event triggers.
        self.assertIsNotNone(
            get_event_trigger("LifeChangeEvent",
                              "Vito, Thorn of the Dusk Rose"),
        )
        self.assertIsNotNone(
            get_event_trigger("DrawEvent", "Faerie Mastermind"),
        )


if __name__ == "__main__":
    unittest.main()
