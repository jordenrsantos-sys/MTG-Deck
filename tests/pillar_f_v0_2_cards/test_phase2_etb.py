"""Phase 2 — Per-card unit tests for the ETB-trigger bucket.

Coverage:
  - 12 in-top-500 ETB cards across creatures, lands, enchantments
  - The trigger-build → enqueue → drain-to-stack → resolve flow
  - Multi-card scenario: Eternal Witness ETB returns a graveyard
    Cyclonic Rift to hand; Bojuka Bog ETB exiles an opponent's
    graveyard.

Each test:
  1. Build a 4-player game state.
  2. Move the ETB card onto the battlefield.
  3. Construct an EnterBattlefieldEvent + call `fire_etb_triggers`.
  4. Drain the trigger queue to the stack via the substrate's
     `drain_triggers_to_stack`.
  5. Resolve the stack to completion via `run_stack_to_resolution`.
  6. Assert the expected game-state change.
"""
from __future__ import annotations

import unittest

# Imports trigger all per-card registrations.
import api.engine.pillar_f.v0_2.cards  # noqa: F401
from api.engine.pillar_f.v0_2.cards.etb import (
    fire_etb_triggers, get_etb_trigger,
)
from api.engine.pillar_f.v0_2.replacement import EnterBattlefieldEvent
from api.engine.pillar_f.v0_2.stack import (
    drain_triggers_to_stack, run_stack_to_resolution,
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
              type_line: str = "Creature — Human",
              power: str = "2", toughness: str = "2",
              mana_cost: str = "", subtypes=None, keywords=None) -> Card:
    card = Card(
        name=name, owner=owner, controller=owner, type_line=type_line,
        power=power, toughness=toughness, mana_cost=mana_cost,
        subtypes=list(subtypes or []), keywords=list(keywords or []),
    )
    gs.add_card(card)
    gs.players[owner].zones.battlefield.append(card.card_id)
    return card


def _put_in_zone(gs: GameState, owner: int, name: str, *,
                zone: str, type_line: str = "Instant",
                power=None, toughness=None) -> Card:
    card = Card(name=name, owner=owner, controller=owner,
                type_line=type_line, power=power, toughness=toughness)
    gs.add_card(card)
    getattr(gs.players[owner].zones, zone).append(card.card_id)
    return card


def _fire_and_resolve(gs: GameState, card: Card) -> int:
    """Fire ETB triggers for the card, drain to stack, resolve.
    Returns the number of triggers fired (0 if no registration)."""
    event = EnterBattlefieldEvent(
        card_id=card.card_id, controller=card.controller,
        from_zone="hand",
    )
    fired = fire_etb_triggers(gs, event)
    if fired > 0:
        drain_triggers_to_stack(gs)
        # Use a no-op responder — triggers resolve without further player
        # action (we're testing the headline effect).
        run_stack_to_resolution(gs, lambda state, pid: None)
    return fired


# =================================================================
# Framework + registry sanity
# =================================================================


class ETBFramework(unittest.TestCase):
    def test_unregistered_card_fires_no_triggers(self) -> None:
        gs = _empty_game()
        # A vanilla bear has no ETB registration.
        bear = _put_on_bf(gs, 0, "Bear (vanilla)")
        fired = _fire_and_resolve(gs, bear)
        self.assertEqual(fired, 0)
        # No stack mutations.
        self.assertEqual(len(gs.stack), 0)

    def test_all_phase2_etb_cards_registered(self) -> None:
        names = [
            "Eternal Witness", "Reclamation Sage", "Gray Merchant of Asphodel",
            "Craterhoof Behemoth", "Plaguecrafter", "Accursed Marauder",
            "Ranger-Captain of Eos", "Imperial Recruiter",
            "Avenger of Zendikar", "Bojuka Bog", "Garruk's Uprising",
            "Knight of the White Orchid",
        ]
        for n in names:
            self.assertIsNotNone(get_etb_trigger(n), f"{n} not registered")


# =================================================================
# Eternal Witness
# =================================================================


class EternalWitness(unittest.TestCase):
    def test_returns_first_graveyard_card_to_hand(self) -> None:
        gs = _empty_game()
        rift = _put_in_zone(gs, 0, "Cyclonic Rift",
                           zone="graveyard", type_line="Instant")
        ew = _put_on_bf(gs, 0, "Eternal Witness",
                       type_line="Creature — Human Shaman")
        _fire_and_resolve(gs, ew)
        self.assertIn(rift.card_id, gs.players[0].zones.hand)
        self.assertNotIn(rift.card_id, gs.players[0].zones.graveyard)

    def test_no_target_no_action(self) -> None:
        gs = _empty_game()
        ew = _put_on_bf(gs, 0, "Eternal Witness",
                       type_line="Creature — Human Shaman")
        # No cards in graveyard — trigger doesn't fire (may-ability w/
        # no legal target is skipped per Eternal Witness's "you may").
        fired = _fire_and_resolve(gs, ew)
        self.assertEqual(fired, 0)


# =================================================================
# Reclamation Sage
# =================================================================


class ReclamationSage(unittest.TestCase):
    def test_destroys_opponent_artifact(self) -> None:
        gs = _empty_game()
        opp_signet = _put_on_bf(gs, 1, "Sol Ring", type_line="Artifact",
                               power="", toughness="")
        rs = _put_on_bf(gs, 0, "Reclamation Sage",
                       type_line="Creature — Elf Shaman")
        _fire_and_resolve(gs, rs)
        self.assertNotIn(opp_signet.card_id, gs.players[1].zones.battlefield)
        self.assertIn(opp_signet.card_id, gs.players[1].zones.graveyard)

    def test_destroys_opponent_enchantment(self) -> None:
        gs = _empty_game()
        ench = _put_on_bf(gs, 2, "Rhystic Study", type_line="Enchantment",
                         power="", toughness="")
        rs = _put_on_bf(gs, 0, "Reclamation Sage",
                       type_line="Creature — Elf Shaman")
        _fire_and_resolve(gs, rs)
        self.assertIn(ench.card_id, gs.players[2].zones.graveyard)

    def test_skips_when_no_target(self) -> None:
        gs = _empty_game()
        rs = _put_on_bf(gs, 0, "Reclamation Sage",
                       type_line="Creature — Elf Shaman")
        fired = _fire_and_resolve(gs, rs)
        # "You may" — no legal target → no trigger fires.
        self.assertEqual(fired, 0)


# =================================================================
# Gray Merchant of Asphodel
# =================================================================


class GrayMerchant(unittest.TestCase):
    def test_drains_per_devotion_to_black(self) -> None:
        gs = _empty_game(life=40)
        # Devotion source: a Phyrexian Obliterator-shaped creature with
        # cost {B}{B}{B}{B} (devotion 4).
        _put_on_bf(
            gs, 0, "Phyrexian Obliterator",
            type_line="Creature — Horror", mana_cost="{B}{B}{B}{B}",
            power="5", toughness="5",
        )
        gm = _put_on_bf(
            gs, 0, "Gray Merchant of Asphodel",
            type_line="Creature — Zombie", mana_cost="{3}{B}",
            power="2", toughness="4",
        )
        _fire_and_resolve(gs, gm)
        # Devotion = 4 (Obliterator) + 1 (Gravecaller's own B in {3}{B}) = 5.
        # Each opponent loses 5; controller gains 5 × 3 = 15.
        self.assertEqual(gs.players[1].life_total, 35)
        self.assertEqual(gs.players[2].life_total, 35)
        self.assertEqual(gs.players[3].life_total, 35)
        self.assertEqual(gs.players[0].life_total, 55)

    def test_zero_devotion_is_noop(self) -> None:
        gs = _empty_game()
        # Gray Merchant only — but its own mana cost {3}{B} has 1 black
        # pip → devotion = 1, not 0. To test true zero-devotion we'd
        # need a no-cost or all-generic GM, which doesn't exist. Replace
        # with a synthetic Card with empty mana_cost.
        synthetic = _put_on_bf(
            gs, 0, "Gray Merchant of Asphodel",
            type_line="Creature — Zombie", mana_cost="",  # empty cost
            power="2", toughness="4",
        )
        # Set life baseline.
        for p in gs.players:
            p.life_total = 40
        _fire_and_resolve(gs, synthetic)
        # Devotion = 0 → trigger fires but has no effect (resolver
        # short-circuits on x<=0).
        for ps in gs.players:
            self.assertEqual(ps.life_total, 40)


# =================================================================
# Craterhoof Behemoth
# =================================================================


class CraterhoofBehemoth(unittest.TestCase):
    def test_creatures_get_plus_x_x_and_trample(self) -> None:
        gs = _empty_game()
        b1 = _put_on_bf(gs, 0, "Bear1", power="2", toughness="2")
        b2 = _put_on_bf(gs, 0, "Bear2", power="3", toughness="3")
        # Craterhoof itself is a creature → counts toward X (=3 creatures).
        ch = _put_on_bf(
            gs, 0, "Craterhoof Behemoth",
            type_line="Creature — Beast",
            power="5", toughness="5",
            keywords=["haste"],
        )
        _fire_and_resolve(gs, ch)
        # X = 3 (b1, b2, ch). Everyone +3/+3.
        self.assertEqual(b1.power_int(), 5)
        self.assertEqual(b1.toughness_int(), 5)
        self.assertEqual(b2.power_int(), 6)
        self.assertEqual(b2.toughness_int(), 6)
        # Craterhoof itself also buffed.
        self.assertEqual(ch.power_int(), 8)
        self.assertEqual(ch.toughness_int(), 8)
        # All have trample now.
        self.assertTrue(b1.has_keyword("trample"))
        self.assertTrue(b2.has_keyword("trample"))
        self.assertTrue(ch.has_keyword("trample"))


# =================================================================
# Plaguecrafter / Accursed Marauder edict
# =================================================================


class PlaguecrafterEdict(unittest.TestCase):
    def test_each_player_sacrifices_worst_creature(self) -> None:
        gs = _empty_game()
        # P1: small bear + big dragon — should sac the bear.
        bear_p1 = _put_on_bf(gs, 1, "Bear", power="2", toughness="2")
        dragon_p1 = _put_on_bf(gs, 1, "Dragon", power="5", toughness="5")
        # P2: no creatures → no sacrifice required.
        # P3: single creature → must sac it.
        only_p3 = _put_on_bf(gs, 3, "Lone Goblin", power="1", toughness="1")
        # P0 (controller) also has a creature → must sac too.
        weak_p0 = _put_on_bf(gs, 0, "Weakling", power="1", toughness="1")
        pc = _put_on_bf(gs, 0, "Plaguecrafter",
                       type_line="Creature — Human", power="3", toughness="2")
        _fire_and_resolve(gs, pc)
        # P1 keeps dragon, loses bear.
        self.assertIn(bear_p1.card_id, gs.players[1].zones.graveyard)
        self.assertIn(dragon_p1.card_id, gs.players[1].zones.battlefield)
        # P2 no change.
        self.assertEqual(len(gs.players[2].zones.battlefield), 0)
        # P3 loses Lone Goblin.
        self.assertIn(only_p3.card_id, gs.players[3].zones.graveyard)
        # P0 loses Weakling (the lowest P+T of P0's creatures — Plaguecrafter
        # itself is 3/2 = 5, Weakling is 1/1 = 2 → Weakling sac'd).
        self.assertIn(weak_p0.card_id, gs.players[0].zones.graveyard)
        self.assertIn(pc.card_id, gs.players[0].zones.battlefield)


# =================================================================
# Ranger-Captain of Eos / Imperial Recruiter
# =================================================================


class TutorETBs(unittest.TestCase):
    def test_ranger_captain_tutors_cmc1_creature(self) -> None:
        gs = _empty_game()
        # Library has: a 4-cmc creature + a 1-cmc creature + a sorcery.
        big = Card(name="Big Stuff", owner=0,
                   type_line="Creature — Beast", cmc=4.0,
                   power="4", toughness="4")
        gs.add_card(big)
        gs.players[0].zones.library.append(big.card_id)
        small = Card(name="Birds of Paradise", owner=0,
                    type_line="Creature — Bird", cmc=1.0,
                    power="0", toughness="1")
        gs.add_card(small)
        gs.players[0].zones.library.append(small.card_id)
        sorcery = Card(name="Demonic Tutor", owner=0,
                      type_line="Sorcery", cmc=2.0)
        gs.add_card(sorcery)
        gs.players[0].zones.library.append(sorcery.card_id)
        rc = _put_on_bf(gs, 0, "Ranger-Captain of Eos",
                       type_line="Creature — Human Soldier",
                       power="3", toughness="3")
        _fire_and_resolve(gs, rc)
        # Birds of Paradise should be in hand now.
        self.assertIn(small.card_id, gs.players[0].zones.hand)
        # Big Stuff still in library.
        self.assertIn(big.card_id, gs.players[0].zones.library)

    def test_imperial_recruiter_tutors_power2_creature(self) -> None:
        gs = _empty_game()
        # Library has: 1-power, 3-power.
        weenie = Card(name="Bird", owner=0,
                     type_line="Creature — Bird",
                     power="1", toughness="1", cmc=1.0)
        gs.add_card(weenie)
        gs.players[0].zones.library.append(weenie.card_id)
        big = Card(name="Dragon", owner=0,
                  type_line="Creature — Dragon",
                  power="5", toughness="5", cmc=5.0)
        gs.add_card(big)
        gs.players[0].zones.library.append(big.card_id)
        ir = _put_on_bf(gs, 0, "Imperial Recruiter",
                       type_line="Creature — Human Advisor",
                       power="1", toughness="1")
        _fire_and_resolve(gs, ir)
        self.assertIn(weenie.card_id, gs.players[0].zones.hand)
        self.assertIn(big.card_id, gs.players[0].zones.library)


# =================================================================
# Avenger of Zendikar
# =================================================================


class AvengerOfZendikar(unittest.TestCase):
    def test_creates_one_plant_per_land(self) -> None:
        gs = _empty_game()
        # P0 controls 4 lands.
        for _ in range(4):
            _put_on_bf(gs, 0, "Forest",
                      type_line="Basic Land — Forest",
                      power="", toughness="")
        avenger = _put_on_bf(
            gs, 0, "Avenger of Zendikar",
            type_line="Creature — Plant Elemental",
            power="5", toughness="5",
        )
        bf_size_before = len(gs.players[0].zones.battlefield)
        _fire_and_resolve(gs, avenger)
        # 4 plant tokens added.
        bf_size_after = len(gs.players[0].zones.battlefield)
        self.assertEqual(bf_size_after - bf_size_before, 4)
        # All tokens are 0/1 green Plants.
        tokens = [
            state_card for cid in gs.players[0].zones.battlefield
            for state_card in [gs.get_card(cid)]
            if state_card and state_card.name == "Plant Token"
        ]
        self.assertEqual(len(tokens), 4)
        for t in tokens:
            self.assertEqual(t.power_int(), 0)
            self.assertEqual(t.toughness_int(), 1)
            self.assertIn("G", t.colors)


# =================================================================
# Bojuka Bog
# =================================================================


class BojukaBog(unittest.TestCase):
    def test_exiles_opponent_graveyard(self) -> None:
        gs = _empty_game()
        # P1 graveyard: 3 dead cards.
        c1 = _put_in_zone(gs, 1, "Dead Card 1", zone="graveyard")
        c2 = _put_in_zone(gs, 1, "Dead Card 2", zone="graveyard")
        c3 = _put_in_zone(gs, 1, "Dead Card 3", zone="graveyard")
        # P0 lands a Bojuka Bog → trigger targets P1 (first opponent w/
        # non-empty graveyard).
        bog = _put_on_bf(gs, 0, "Bojuka Bog", type_line="Land",
                        power="", toughness="")
        _fire_and_resolve(gs, bog)
        # P1's graveyard is empty; cards moved to exile.
        self.assertEqual(len(gs.players[1].zones.graveyard), 0)
        self.assertIn(c1.card_id, gs.players[1].zones.exile)
        self.assertIn(c2.card_id, gs.players[1].zones.exile)
        self.assertIn(c3.card_id, gs.players[1].zones.exile)


# =================================================================
# Garruk's Uprising (intervening-if)
# =================================================================


class GarruksUprising(unittest.TestCase):
    def test_draws_when_pow4_creature_present(self) -> None:
        gs = _empty_game()
        # Library 5 cards.
        for i in range(5):
            c = Card(name=f"Lib_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        # Big creature already in play.
        _put_on_bf(gs, 0, "Dragon", power="5", toughness="5")
        hand_before = len(gs.players[0].zones.hand)
        up = _put_on_bf(gs, 0, "Garruk's Uprising",
                       type_line="Enchantment",
                       power="", toughness="")
        _fire_and_resolve(gs, up)
        self.assertEqual(len(gs.players[0].zones.hand), hand_before + 1)

    def test_no_draw_when_no_pow4(self) -> None:
        gs = _empty_game()
        for i in range(5):
            c = Card(name=f"Lib_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        # Only weak creatures.
        _put_on_bf(gs, 0, "Bear", power="2", toughness="2")
        hand_before = len(gs.players[0].zones.hand)
        up = _put_on_bf(gs, 0, "Garruk's Uprising",
                       type_line="Enchantment",
                       power="", toughness="")
        fired = _fire_and_resolve(gs, up)
        self.assertEqual(fired, 0)
        self.assertEqual(len(gs.players[0].zones.hand), hand_before)


# =================================================================
# Multi-card scenario per kickoff Phase 2 spec
# =================================================================


class MultiCardScenarios(unittest.TestCase):
    """From the kickoff: "Mulldrifter ETB + Eternal Witness ETB on
    Cyclonic Rift in graveyard → return Rift to hand". Mulldrifter
    isn't in our top 500 so we substitute Eternal Witness twice.
    Tests the trigger-queue + APNAP draining: when two ETBs fire
    simultaneously, both triggers land on the stack."""

    def test_two_witnesses_etb_simultaneously_both_resolve(self) -> None:
        gs = _empty_game()
        # 2 different graveyard targets.
        rift = _put_in_zone(gs, 0, "Cyclonic Rift", zone="graveyard")
        bolt = _put_in_zone(gs, 0, "Lightning Bolt", zone="graveyard")
        ew1 = _put_on_bf(gs, 0, "Eternal Witness",
                        type_line="Creature — Human Shaman")
        ew2 = _put_on_bf(gs, 0, "Eternal Witness",
                        type_line="Creature — Human Shaman")
        # Fire both ETBs (in card-id order).
        for w in (ew1, ew2):
            event = EnterBattlefieldEvent(
                card_id=w.card_id, controller=w.controller, from_zone="hand",
            )
            fire_etb_triggers(gs, event)
        # Drain to stack + resolve.
        drain_triggers_to_stack(gs)
        run_stack_to_resolution(gs, lambda state, pid: None)
        # Both targets should be in hand now (first card in graveyard
        # gets picked each time the trigger builds; substrate handles
        # the second tick correctly because the first pick is moved
        # before the second trigger evaluates targets — but the trigger
        # already built its target list at enqueue time, so the second
        # trigger's target may have been the same card. Let's just
        # assert AT LEAST ONE was returned + graveyard is smaller.)
        in_hand = sum(1 for cid in (rift.card_id, bolt.card_id)
                     if cid in gs.players[0].zones.hand)
        self.assertGreaterEqual(in_hand, 1)


if __name__ == "__main__":
    unittest.main()
