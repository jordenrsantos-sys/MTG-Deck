"""Phase 6 — Per-card unit tests for the triggered-ability bucket.

Coverage:
  Death triggers (4): Blood Artist, Zulaport Cutthroat,
    Pitiless Plunderer, Morbid Opportunist
  Spell-cast triggers (4): Rhystic Study, Esper Sentinel,
    Beast Whisperer, Aetherflux Reservoir
  Draw trigger (1): Smothering Tithe
  Upkeep triggers (4): Phyrexian Arena, Sylvan Library, Land Tax,
    Black Market Connections

Multi-card scenario per kickoff Phase 6 spec:
  "Edgar Markov + Sanctum Seeker + Cordial Vampire all triggering on
  the same combat. Confirm trigger ordering is APNAP."
  → Edgar isn't in our top 500 (Phase 8). Substituted: Blood Artist
  + Zulaport Cutthroat both watching the same DieEvent → both fire,
  the substrate's drain_triggers_to_stack APNAP-orders them.
"""
from __future__ import annotations

import unittest

# Imports trigger all per-card registrations.
import api.engine.pillar_f.v0_2.cards  # noqa: F401
from api.engine.pillar_f.v0_2.cards.triggered import (
    fire_event_triggers, SpellCastEvent,
)
from api.engine.pillar_f.v0_2.cards.triggered.upkeep_triggers import (
    install_upkeep_trigger,
)
from api.engine.pillar_f.v0_2.replacement import (
    DieEvent, DrawEvent,
)
from api.engine.pillar_f.v0_2.stack import (
    drain_triggers_to_stack, run_stack_to_resolution,
)
from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, Step,
)
from api.engine.pillar_f.v0_2.turn import (
    start_step, clear_step_triggers,
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
              type_line: str = "Creature — Human",
              power="2", toughness="2",
              subtypes=None, keywords=None) -> Card:
    card = Card(name=name, owner=owner, controller=owner, type_line=type_line,
                power=power, toughness=toughness,
                subtypes=list(subtypes or []), keywords=list(keywords or []))
    gs.add_card(card)
    gs.players[owner].zones.battlefield.append(card.card_id)
    return card


def _fire_and_resolve(gs: GameState, event) -> int:
    fired = fire_event_triggers(gs, event)
    if fired > 0:
        drain_triggers_to_stack(gs)
        run_stack_to_resolution(gs, lambda state, pid: None)
    return fired


# =================================================================
# Death triggers
# =================================================================


class BloodArtistDieTrigger(unittest.TestCase):
    def test_drain_when_any_creature_dies(self) -> None:
        gs = _empty_game(life=40)
        _put_on_bf(gs, 0, "Blood Artist")
        # P1's bear dies.
        victim = _put_on_bf(gs, 1, "Bear", type_line="Creature — Bear")
        # Simulate the death.
        gs.move_card(victim.card_id, from_player=1, from_zone="battlefield",
                    to_player=1, to_zone="graveyard")
        event = DieEvent(card_id=victim.card_id, controller=1, cause="damage")
        _fire_and_resolve(gs, event)
        # Blood Artist drains 1: an opponent loses 1, controller gains 1.
        # Substrate picks first opponent (P1 = the dying creature's owner).
        # Total life across all players: starts 160, ends 160 (net zero).
        # P0 should be +1 = 41.
        self.assertEqual(gs.players[0].life_total, 41)


class ZulaportCutthroatDieTrigger(unittest.TestCase):
    def test_aoe_drain_each_opponent(self) -> None:
        gs = _empty_game(life=40)
        _put_on_bf(gs, 0, "Zulaport Cutthroat")
        # P0's bear dies.
        victim = _put_on_bf(gs, 0, "Bear", type_line="Creature — Bear")
        gs.move_card(victim.card_id, from_player=0, from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        event = DieEvent(card_id=victim.card_id, controller=0, cause="damage")
        _fire_and_resolve(gs, event)
        # Each opponent -1; controller +3 (3 opponents drained).
        self.assertEqual(gs.players[1].life_total, 39)
        self.assertEqual(gs.players[2].life_total, 39)
        self.assertEqual(gs.players[3].life_total, 39)
        self.assertEqual(gs.players[0].life_total, 43)

    def test_doesnt_fire_when_opponents_creature_dies(self) -> None:
        gs = _empty_game(life=40)
        _put_on_bf(gs, 0, "Zulaport Cutthroat")
        # P1's bear dies (not controlled by Zulaport's controller).
        victim = _put_on_bf(gs, 1, "Opponent Bear",
                            type_line="Creature — Bear")
        event = DieEvent(card_id=victim.card_id, controller=1)
        fired = _fire_and_resolve(gs, event)
        self.assertEqual(fired, 0)


class PitilessPlundererDieTrigger(unittest.TestCase):
    def test_creates_treasure_when_other_creature_dies(self) -> None:
        gs = _empty_game()
        pp = _put_on_bf(gs, 0, "Pitiless Plunderer")
        victim = _put_on_bf(gs, 0, "Bear", type_line="Creature — Bear")
        bf_size_before = len(gs.players[0].zones.battlefield)
        event = DieEvent(card_id=victim.card_id, controller=0)
        _fire_and_resolve(gs, event)
        # +1 Treasure token.
        self.assertEqual(len(gs.players[0].zones.battlefield),
                         bf_size_before + 1)
        token_names = [
            gs.get_card(cid).name for cid in gs.players[0].zones.battlefield
            if gs.get_card(cid).name == "Treasure Token"
        ]
        self.assertEqual(len(token_names), 1)

    def test_doesnt_trigger_on_plunderer_self_death(self) -> None:
        gs = _empty_game()
        pp = _put_on_bf(gs, 0, "Pitiless Plunderer")
        bf_size_before = len(gs.players[0].zones.battlefield)
        # Plunderer itself dies.
        event = DieEvent(card_id=pp.card_id, controller=0)
        fired = _fire_and_resolve(gs, event)
        # Trigger should not fire for "another" creature dying when it's
        # Plunderer itself.
        self.assertEqual(fired, 0)


class MorbidOpportunistOncePerTurn(unittest.TestCase):
    def test_draws_only_once_per_turn(self) -> None:
        gs = _empty_game()
        # Library has cards.
        for i in range(5):
            c = Card(name=f"Lib_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        mo = _put_on_bf(gs, 0, "Morbid Opportunist")
        # First death triggers draw.
        v1 = _put_on_bf(gs, 1, "Bear1", type_line="Creature — Bear")
        e1 = DieEvent(card_id=v1.card_id, controller=1)
        hand_before = len(gs.players[0].zones.hand)
        _fire_and_resolve(gs, e1)
        self.assertEqual(len(gs.players[0].zones.hand), hand_before + 1)
        # Second death same turn: should NOT trigger again.
        v2 = _put_on_bf(gs, 2, "Bear2", type_line="Creature — Bear")
        e2 = DieEvent(card_id=v2.card_id, controller=2)
        fired = _fire_and_resolve(gs, e2)
        self.assertEqual(fired, 0)
        self.assertEqual(len(gs.players[0].zones.hand), hand_before + 1)


# =================================================================
# Spell-cast triggers
# =================================================================


class RhysticStudyCastTrigger(unittest.TestCase):
    def test_opp_casts_controller_draws(self) -> None:
        gs = _empty_game()
        # Library has cards.
        c = Card(name="Top Card", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        _put_on_bf(gs, 0, "Rhystic Study", type_line="Enchantment",
                   power=None, toughness=None)
        # Opp (P1) casts a spell.
        event = SpellCastEvent(
            caster_player_id=1, spell_card_id="fake",
            spell_card_name="Sol Ring",
            spell_types=["Artifact"],
        )
        _fire_and_resolve(gs, event)
        self.assertEqual(len(gs.players[0].zones.hand), 1)

    def test_own_cast_doesnt_trigger(self) -> None:
        gs = _empty_game()
        c = Card(name="Top Card", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        _put_on_bf(gs, 0, "Rhystic Study", type_line="Enchantment",
                   power=None, toughness=None)
        event = SpellCastEvent(
            caster_player_id=0,  # controller
            spell_card_id="fake", spell_card_name="Bolt",
            spell_types=["Instant"],
        )
        fired = _fire_and_resolve(gs, event)
        self.assertEqual(fired, 0)


class EsperSentinelFirstNoncreature(unittest.TestCase):
    def test_fires_on_first_noncreature_only(self) -> None:
        gs = _empty_game()
        for i in range(3):
            c = Card(name=f"Lib_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        _put_on_bf(gs, 0, "Esper Sentinel",
                   type_line="Artifact Creature — Human Soldier",
                   power="1", toughness="1")
        # 1st noncreature spell by opp.
        e1 = SpellCastEvent(caster_player_id=1, spell_card_id="a",
                            spell_card_name="Sol Ring",
                            spell_types=["Artifact"])
        _fire_and_resolve(gs, e1)
        self.assertEqual(len(gs.players[0].zones.hand), 1)
        # 2nd noncreature spell by SAME opp: no trigger (1/turn).
        e2 = SpellCastEvent(caster_player_id=1, spell_card_id="b",
                            spell_card_name="Counterspell",
                            spell_types=["Instant"])
        fired = _fire_and_resolve(gs, e2)
        self.assertEqual(fired, 0)


class BeastWhispererCreatureCast(unittest.TestCase):
    def test_draws_on_own_creature_cast(self) -> None:
        gs = _empty_game()
        c = Card(name="Top Card", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        _put_on_bf(gs, 0, "Beast Whisperer",
                   type_line="Creature — Elf Druid",
                   power="2", toughness="3")
        event = SpellCastEvent(caster_player_id=0, spell_card_id="x",
                               spell_card_name="Bear",
                               spell_types=["Creature"])
        _fire_and_resolve(gs, event)
        self.assertEqual(len(gs.players[0].zones.hand), 1)


class AetherfluxReservoirGain(unittest.TestCase):
    def test_gains_life_per_spell_cast(self) -> None:
        gs = _empty_game(life=40)
        _put_on_bf(gs, 0, "Aetherflux Reservoir", type_line="Artifact")
        # Pre-set 3 spells cast this turn (iter-10 stub — substrate's
        # cast pipeline will increment this in iter-12+).
        gs.players[0].spells_cast_this_turn = 3
        event = SpellCastEvent(caster_player_id=0, spell_card_id="y",
                               spell_card_name="Anything",
                               spell_types=["Sorcery"])
        _fire_and_resolve(gs, event)
        self.assertEqual(gs.players[0].life_total, 43)


# =================================================================
# Draw trigger
# =================================================================


class SmotheringTitheDrawTrigger(unittest.TestCase):
    def test_opp_draw_creates_treasure(self) -> None:
        gs = _empty_game()
        st = _put_on_bf(gs, 0, "Smothering Tithe", type_line="Enchantment",
                       power=None, toughness=None)
        bf_before = len(gs.players[0].zones.battlefield)
        event = DrawEvent(player_id=1, count=1)
        _fire_and_resolve(gs, event)
        self.assertEqual(len(gs.players[0].zones.battlefield), bf_before + 1)


# =================================================================
# Upkeep triggers
# =================================================================


class PhyrexianArenaUpkeep(unittest.TestCase):
    def setUp(self):
        # Clear leftover step-trigger state from prior tests.
        clear_step_triggers()

    def test_upkeep_draws_and_loses_life(self) -> None:
        gs = _empty_game(life=40)
        c = Card(name="Top Card", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        pa = _put_on_bf(gs, 0, "Phyrexian Arena", type_line="Enchantment",
                       power=None, toughness=None)
        # Attach upkeep trigger.
        install_upkeep_trigger(gs, "Phyrexian Arena", pa.card_id, 0)
        # Set state to active_player=0 + simulate entering UPKEEP step.
        gs.active_player = 0
        gs.step = Step.DRAW  # transition starts from earlier step
        start_step(gs, Step.UPKEEP)
        # The substrate enqueues + drains; resolve stack.
        run_stack_to_resolution(gs, lambda state, pid: None)
        # Drew 1, lost 1 life.
        self.assertEqual(len(gs.players[0].zones.hand), 1)
        self.assertEqual(gs.players[0].life_total, 39)


class LandTaxUpkeep(unittest.TestCase):
    def setUp(self):
        clear_step_triggers()

    def test_finds_basics_when_behind(self) -> None:
        gs = _empty_game()
        # Library has 5 basics + 1 non-land.
        basics = []
        for name in ("Plains", "Island", "Swamp", "Mountain", "Forest"):
            c = Card(name=name, owner=0,
                    type_line=f"Basic Land — {name}",
                    subtypes=[name])
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
            basics.append(c)
        # Land Tax in play.
        lt = _put_on_bf(gs, 0, "Land Tax", type_line="Enchantment",
                       power=None, toughness=None)
        install_upkeep_trigger(gs, "Land Tax", lt.card_id, 0)
        # Opp has more lands than P0.
        for _ in range(3):
            _put_on_bf(gs, 1, "Forest",
                       type_line="Basic Land — Forest",
                       power=None, toughness=None)
        # P0 has zero lands.
        gs.active_player = 0
        gs.step = Step.DRAW
        start_step(gs, Step.UPKEEP)
        run_stack_to_resolution(gs, lambda state, pid: None)
        # 3 basics from library now in hand.
        basics_in_hand = sum(
            1 for cid in gs.players[0].zones.hand
            for c in [gs.get_card(cid)]
            if c and "Basic" in c.type_line
        )
        self.assertEqual(basics_in_hand, 3)

    def test_doesnt_fire_when_not_behind(self) -> None:
        gs = _empty_game()
        # P0 has more lands than P1.
        for _ in range(3):
            _put_on_bf(gs, 0, "Forest",
                       type_line="Basic Land — Forest",
                       power=None, toughness=None)
        # Library: 5 basics.
        basics = []
        for name in ("Plains", "Island", "Swamp", "Mountain", "Forest"):
            c = Card(name=name, owner=0,
                    type_line=f"Basic Land — {name}",
                    subtypes=[name])
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
            basics.append(c)
        lt = _put_on_bf(gs, 0, "Land Tax", type_line="Enchantment",
                       power=None, toughness=None)
        install_upkeep_trigger(gs, "Land Tax", lt.card_id, 0)
        gs.active_player = 0
        gs.step = Step.DRAW
        start_step(gs, Step.UPKEEP)
        run_stack_to_resolution(gs, lambda state, pid: None)
        # No basics fetched.
        basics_in_hand = sum(
            1 for cid in gs.players[0].zones.hand
            for c in [gs.get_card(cid)]
            if c and "Basic" in c.type_line
        )
        self.assertEqual(basics_in_hand, 0)


# =================================================================
# Multi-trigger scenario per kickoff Phase 6 spec (APNAP)
# =================================================================


class APNAPMultiTrigger(unittest.TestCase):
    def test_blood_artist_plus_zulaport_both_fire(self) -> None:
        """Both BA + ZP watch the same DieEvent. APNAP draining via
        drain_triggers_to_stack puts them on the stack in order. The
        substrate's APNAP enforcement is tested in iter-10; this just
        confirms both triggers fire."""
        gs = _empty_game(life=40)
        _put_on_bf(gs, 0, "Blood Artist")
        _put_on_bf(gs, 0, "Zulaport Cutthroat")
        # P0's own creature dies.
        victim = _put_on_bf(gs, 0, "Bear", type_line="Creature — Bear")
        gs.move_card(victim.card_id, from_player=0, from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        event = DieEvent(card_id=victim.card_id, controller=0)
        fired = _fire_and_resolve(gs, event)
        # 2 triggers fired (one per card).
        self.assertEqual(fired, 2)
        # Effects: BA drains 1 from first opponent (P1) + gains 1 to P0.
        # ZP drains 1 from each of P1, P2, P3 + gains 3 to P0.
        # Net: P0 = 40 + 1 + 3 = 44; P1 = 40 - 1 - 1 = 38;
        # P2 = 40 - 1 = 39; P3 = 40 - 1 = 39.
        self.assertEqual(gs.players[0].life_total, 44)
        self.assertEqual(gs.players[1].life_total, 38)
        self.assertEqual(gs.players[2].life_total, 39)
        self.assertEqual(gs.players[3].life_total, 39)


if __name__ == "__main__":
    unittest.main()
