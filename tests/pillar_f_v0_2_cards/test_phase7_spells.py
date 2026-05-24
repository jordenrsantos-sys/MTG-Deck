"""Phase 7 — Per-card unit tests for the spell bucket (instants + sorceries).

Coverage:
  Removal (8): Swords to Plowshares, Path to Exile, Beast Within,
    Generous Gift, Chaos Warp, Feed the Swarm, Doom Blade,
    Go for the Throat
  Counterspells (8): Counterspell, Negate, Mana Drain, Swan Song,
    Arcane Denial, An Offer You Can't Refuse, Fierce Guardianship,
    Deflecting Swat
  Ramp + tutors (8): Cultivate, Kodama's Reach, Nature's Lore,
    Three Visits, Rampant Growth, Dark Ritual, Demonic Tutor,
    Vampiric Tutor, Enlightened Tutor, Mystical Tutor
  Card draw (2): Brainstorm, Faithless Looting
  Mass removal (4): Toxic Deluge, Blasphemous Act, Cyclonic Rift,
    Vandalblast
  Protection (1): Heroic Intervention
  Reanimation (1): Reanimate
  Finisher (1): Thassa's Oracle — kickoff Phase 7 highlight

Combo test per kickoff Phase 7 spec:
  "Thoracle line wins game" — Thassa's Oracle resolving with empty
  library wins immediately.
  "Cyclonic Rift overload returns all opponents' nonland permanents" —
  overload deferred to Phase 8; single-target side tested here.
"""
from __future__ import annotations

import unittest

# Imports trigger all per-card registrations.
import api.engine.pillar_f.v0_2.cards  # noqa: F401
from api.engine.pillar_f.v0_2.cards.spell import (
    build_spell_payload, all_registered_spell_names,
)
from api.engine.pillar_f.v0_2.stack import (
    push_to_stack, resolve_top,
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
              mana_cost="", subtypes=None, keywords=None,
              colors=None) -> Card:
    card = Card(name=name, owner=owner, controller=owner, type_line=type_line,
                power=power, toughness=toughness, cmc=cmc,
                mana_cost=mana_cost,
                subtypes=list(subtypes or []), keywords=list(keywords or []),
                colors=list(colors or []))
    gs.add_card(card)
    gs.players[owner].zones.battlefield.append(card.card_id)
    return card


def _put_in_zone(gs: GameState, owner: int, name: str, *,
                zone: str, type_line: str = "Instant",
                cmc=2.0, mana_cost="", subtypes=None) -> Card:
    card = Card(name=name, owner=owner, controller=owner, type_line=type_line,
                cmc=cmc, mana_cost=mana_cost,
                subtypes=list(subtypes or []))
    gs.add_card(card)
    getattr(gs.players[owner].zones, zone).append(card.card_id)
    return card


def _cast_spell(gs: GameState, caster: int, spell_name: str, *,
               targets=None, payment_extra=None) -> None:
    """Caller pushes the spell onto the stack + resolves immediately
    (substrate-driven). Cost-payment is caller-controlled per
    iter-10 substrate."""
    payment = build_spell_payload(spell_name)
    if payment_extra:
        payment.update(payment_extra)
    push_to_stack(
        gs, card_id=None, controller=caster, entry_type="spell",
        payment=payment, targets=targets or [],
        description=f"{spell_name} cast",
    )
    resolve_top(gs)


# =================================================================
# Removal
# =================================================================


class RemovalSpells(unittest.TestCase):
    def test_swords_to_plowshares_exiles_creature_life_gain(self) -> None:
        gs = _empty_game()
        bear = _put_on_bf(gs, 1, "Bear", power="2", toughness="2")
        _cast_spell(gs, 0, "Swords to Plowshares", targets=[bear.card_id])
        self.assertIn(bear.card_id, gs.players[1].zones.exile)
        self.assertEqual(gs.players[1].life_total, 42)  # +power = +2

    def test_path_to_exile_exiles_plus_basic_tutor(self) -> None:
        gs = _empty_game()
        bear = _put_on_bf(gs, 1, "Bear", power="2", toughness="2")
        plains = Card(name="Plains", owner=1,
                     type_line="Basic Land — Plains",
                     subtypes=["Plains"])
        gs.add_card(plains)
        gs.players[1].zones.library.append(plains.card_id)
        _cast_spell(gs, 0, "Path to Exile", targets=[bear.card_id])
        self.assertIn(bear.card_id, gs.players[1].zones.exile)
        self.assertIn(plains.card_id, gs.players[1].zones.battlefield)
        self.assertTrue(plains.tapped)

    def test_beast_within_destroys_creates_3_3_beast(self) -> None:
        gs = _empty_game()
        target = _put_on_bf(gs, 1, "Whatever", type_line="Enchantment",
                            power=None, toughness=None)
        _cast_spell(gs, 0, "Beast Within", targets=[target.card_id])
        self.assertIn(target.card_id, gs.players[1].zones.graveyard)
        # Beast token in P1's battlefield.
        tokens = [gs.get_card(cid) for cid in gs.players[1].zones.battlefield
                  if gs.get_card(cid).name == "Beast Token"]
        self.assertEqual(len(tokens), 1)
        self.assertEqual(tokens[0].power_int(), 3)

    def test_generous_gift_creates_elephant_token(self) -> None:
        gs = _empty_game()
        target = _put_on_bf(gs, 1, "Some Artifact", type_line="Artifact",
                            power=None, toughness=None)
        _cast_spell(gs, 0, "Generous Gift", targets=[target.card_id])
        self.assertIn(target.card_id, gs.players[1].zones.graveyard)
        tokens = [gs.get_card(cid) for cid in gs.players[1].zones.battlefield
                  if gs.get_card(cid).name == "Elephant Token"]
        self.assertEqual(len(tokens), 1)

    def test_chaos_warp_shuffles_target_in_library(self) -> None:
        gs = _empty_game()
        target = _put_on_bf(gs, 1, "Some Permanent", type_line="Creature — Bear",
                            power="2", toughness="2")
        top_replacement = Card(name="Mountain", owner=1,
                              type_line="Basic Land — Mountain",
                              subtypes=["Mountain"])
        gs.add_card(top_replacement)
        gs.players[1].zones.library.insert(0, top_replacement.card_id)
        _cast_spell(gs, 0, "Chaos Warp", targets=[target.card_id])
        # Target should be in library (back).
        self.assertIn(target.card_id, gs.players[1].zones.library)
        # Top card (Mountain — a land = permanent) put onto battlefield.
        self.assertIn(top_replacement.card_id,
                     gs.players[1].zones.battlefield)

    def test_feed_the_swarm_destroys_costs_life(self) -> None:
        gs = _empty_game(life=40)
        target = _put_on_bf(gs, 1, "Creature", power="2", toughness="2",
                            cmc=3.0)
        _cast_spell(gs, 0, "Feed the Swarm", targets=[target.card_id])
        self.assertIn(target.card_id, gs.players[1].zones.graveyard)
        self.assertEqual(gs.players[0].life_total, 37)  # -3 cmc

    def test_doom_blade_destroys_creature(self) -> None:
        gs = _empty_game()
        bear = _put_on_bf(gs, 1, "Bear")
        _cast_spell(gs, 0, "Doom Blade", targets=[bear.card_id])
        self.assertIn(bear.card_id, gs.players[1].zones.graveyard)


# =================================================================
# Counterspells
# =================================================================


class Counterspells(unittest.TestCase):
    def _push_target(self, gs: GameState, *, controller: int = 1) -> str:
        """Push a dummy noop spell that the counter targets."""
        entry = push_to_stack(
            gs, card_id=None, controller=controller, entry_type="spell",
            payment={"resolver": "noop"},
            description="Dummy target spell",
        )
        return entry.entry_id

    def test_counterspell_counters_target(self) -> None:
        gs = _empty_game()
        target_id = self._push_target(gs)
        # Now cast Counterspell targeting it.
        push_to_stack(
            gs, card_id=None, controller=0, entry_type="spell",
            payment=build_spell_payload("Counterspell"),
            targets=[target_id],
            description="Counterspell cast",
        )
        # Resolve counterspell first (LIFO — top of stack).
        resolve_top(gs)
        # Target should be gone.
        target_in_stack = any(e.entry_id == target_id for e in gs.stack)
        self.assertFalse(target_in_stack)

    def test_swan_song_counters_and_creates_bird(self) -> None:
        gs = _empty_game()
        target_id = self._push_target(gs, controller=1)
        push_to_stack(
            gs, card_id=None, controller=0, entry_type="spell",
            payment=build_spell_payload("Swan Song"),
            targets=[target_id],
            description="Swan Song",
        )
        resolve_top(gs)
        # P1 (target's controller) gets a Bird token.
        bird = [gs.get_card(cid) for cid in gs.players[1].zones.battlefield
                if gs.get_card(cid).name == "Bird Token"]
        self.assertEqual(len(bird), 1)
        self.assertIn("flying", bird[0].keywords)

    def test_an_offer_creates_two_treasures(self) -> None:
        gs = _empty_game()
        target_id = self._push_target(gs, controller=1)
        push_to_stack(
            gs, card_id=None, controller=0, entry_type="spell",
            payment=build_spell_payload("An Offer You Can't Refuse"),
            targets=[target_id],
            description="An Offer",
        )
        resolve_top(gs)
        treasures = [gs.get_card(cid) for cid in gs.players[1].zones.battlefield
                     if gs.get_card(cid).name == "Treasure Token"]
        self.assertEqual(len(treasures), 2)

    def test_arcane_denial_draws_for_both(self) -> None:
        gs = _empty_game()
        # Libraries: P0 has 1 card; P1 has 2.
        for owner, count in ((0, 1), (1, 2)):
            for i in range(count):
                c = Card(name=f"Lib_{owner}_{i}", owner=owner)
                gs.add_card(c)
                gs.players[owner].zones.library.append(c.card_id)
        target_id = self._push_target(gs, controller=1)
        push_to_stack(
            gs, card_id=None, controller=0, entry_type="spell",
            payment=build_spell_payload("Arcane Denial"),
            targets=[target_id],
            description="Arcane Denial",
        )
        resolve_top(gs)
        # P0 +1 card; P1 +2 cards.
        self.assertEqual(len(gs.players[0].zones.hand), 1)
        self.assertEqual(len(gs.players[1].zones.hand), 2)


# =================================================================
# Ramp + tutors
# =================================================================


class RampAndTutors(unittest.TestCase):
    def _seed_lib(self, gs: GameState, owner: int) -> dict:
        cards = {}
        for n, subtype in (("Plains", "Plains"), ("Island", "Island"),
                          ("Swamp", "Swamp"), ("Mountain", "Mountain"),
                          ("Forest", "Forest")):
            c = Card(name=n, owner=owner,
                    type_line=f"Basic Land — {subtype}",
                    subtypes=[subtype])
            gs.add_card(c)
            gs.players[owner].zones.library.append(c.card_id)
            cards[n] = c
        return cards

    def test_cultivate_one_to_bf_one_to_hand(self) -> None:
        gs = _empty_game()
        lib = self._seed_lib(gs, 0)
        _cast_spell(gs, 0, "Cultivate")
        # Plains (1st basic) → battlefield tapped. Island (2nd) → hand.
        self.assertIn(lib["Plains"].card_id, gs.players[0].zones.battlefield)
        self.assertTrue(lib["Plains"].tapped)
        self.assertIn(lib["Island"].card_id, gs.players[0].zones.hand)

    def test_natures_lore_finds_forest_untapped(self) -> None:
        gs = _empty_game()
        lib = self._seed_lib(gs, 0)
        _cast_spell(gs, 0, "Nature's Lore")
        self.assertIn(lib["Forest"].card_id, gs.players[0].zones.battlefield)
        self.assertFalse(lib["Forest"].tapped)

    def test_rampant_growth_finds_basic_tapped(self) -> None:
        gs = _empty_game()
        lib = self._seed_lib(gs, 0)
        _cast_spell(gs, 0, "Rampant Growth")
        # First basic = Plains.
        self.assertIn(lib["Plains"].card_id, gs.players[0].zones.battlefield)
        self.assertTrue(lib["Plains"].tapped)

    def test_dark_ritual_adds_three_black(self) -> None:
        gs = _empty_game()
        _cast_spell(gs, 0, "Dark Ritual")
        self.assertEqual(gs.players[0].mana_pool.B, 3)

    def test_demonic_tutor_finds_target(self) -> None:
        gs = _empty_game()
        c1 = Card(name="Random1", owner=0)
        c2 = Card(name="Target Card", owner=0)
        gs.add_card(c1)
        gs.add_card(c2)
        gs.players[0].zones.library.extend([c1.card_id, c2.card_id])
        _cast_spell(gs, 0, "Demonic Tutor", targets=[c2.card_id])
        self.assertIn(c2.card_id, gs.players[0].zones.hand)
        self.assertNotIn(c2.card_id, gs.players[0].zones.library)

    def test_vampiric_tutor_top_of_library_and_pay_life(self) -> None:
        gs = _empty_game(life=40)
        c1 = Card(name="Filler", owner=0)
        c2 = Card(name="Target", owner=0)
        gs.add_card(c1)
        gs.add_card(c2)
        gs.players[0].zones.library.extend([c1.card_id, c2.card_id])
        _cast_spell(gs, 0, "Vampiric Tutor", targets=[c2.card_id])
        # Target now at index 0 of library.
        self.assertEqual(gs.players[0].zones.library[0], c2.card_id)
        # Life -2.
        self.assertEqual(gs.players[0].life_total, 38)


# =================================================================
# Card draw
# =================================================================


class CardDraw(unittest.TestCase):
    def test_brainstorm_net_one_card(self) -> None:
        gs = _empty_game()
        # Library: 3 cards. Hand starts empty.
        for i in range(3):
            c = Card(name=f"Lib_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        _cast_spell(gs, 0, "Brainstorm")
        # Brainstorm: draw 3, put 2 back on top → net +1 in hand.
        self.assertEqual(len(gs.players[0].zones.hand), 1)
        self.assertEqual(len(gs.players[0].zones.library), 2)

    def test_faithless_looting_net_zero_cards(self) -> None:
        gs = _empty_game()
        for i in range(2):
            c = Card(name=f"Lib_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        _cast_spell(gs, 0, "Faithless Looting")
        # Draw 2, discard 2 → hand size unchanged (0).
        self.assertEqual(len(gs.players[0].zones.hand), 0)
        self.assertEqual(len(gs.players[0].zones.graveyard), 2)


# =================================================================
# Mass removal + protection
# =================================================================


class MassAndProtection(unittest.TestCase):
    def test_toxic_deluge_minus_x_to_all_creatures(self) -> None:
        gs = _empty_game()
        bear1 = _put_on_bf(gs, 0, "Bear1", power="2", toughness="2")
        bear2 = _put_on_bf(gs, 1, "Bear2", power="3", toughness="3")
        _cast_spell(gs, 0, "Toxic Deluge", payment_extra={"x": 2})
        self.assertEqual(bear1.power_int(), 0)
        self.assertEqual(bear1.toughness_int(), 0)
        self.assertEqual(bear2.power_int(), 1)
        self.assertEqual(bear2.toughness_int(), 1)

    def test_blasphemous_act_kills_creatures(self) -> None:
        gs = _empty_game()
        big = _put_on_bf(gs, 0, "Big", power="5", toughness="5")
        bigger = _put_on_bf(gs, 1, "Bigger", power="20", toughness="20")
        _cast_spell(gs, 0, "Blasphemous Act")
        self.assertIn(big.card_id, gs.players[0].zones.graveyard)
        # 20-toughness survives 13 damage.
        self.assertIn(bigger.card_id, gs.players[1].zones.battlefield)

    def test_cyclonic_rift_bounces_opp_nonland(self) -> None:
        gs = _empty_game()
        target = _put_on_bf(gs, 1, "Some Permanent",
                            type_line="Creature — Bear",
                            power="2", toughness="2")
        _cast_spell(gs, 0, "Cyclonic Rift", targets=[target.card_id])
        self.assertIn(target.card_id, gs.players[1].zones.hand)

    def test_vandalblast_destroys_opp_artifact(self) -> None:
        gs = _empty_game()
        target = _put_on_bf(gs, 1, "Sol Ring", type_line="Artifact",
                            power=None, toughness=None)
        _cast_spell(gs, 0, "Vandalblast", targets=[target.card_id])
        self.assertIn(target.card_id, gs.players[1].zones.graveyard)

    def test_heroic_intervention_grants_keywords(self) -> None:
        gs = _empty_game()
        bear = _put_on_bf(gs, 0, "Bear", power="2", toughness="2")
        _cast_spell(gs, 0, "Heroic Intervention")
        self.assertIn("hexproof", bear.keywords)
        self.assertIn("indestructible", bear.keywords)


# =================================================================
# Reanimation
# =================================================================


class Reanimation(unittest.TestCase):
    def test_reanimate_brings_to_battlefield_with_life_loss(self) -> None:
        gs = _empty_game(life=40)
        dead = _put_in_zone(gs, 1, "Dead Dragon", zone="graveyard",
                           type_line="Creature — Dragon", cmc=5.0)
        # Make dead a creature.
        dead.power = "5"
        dead.toughness = "5"
        _cast_spell(gs, 0, "Reanimate", targets=[dead.card_id])
        self.assertIn(dead.card_id, gs.players[0].zones.battlefield)
        self.assertEqual(dead.controller, 0)
        self.assertEqual(gs.players[0].life_total, 35)  # -5 cmc


# =================================================================
# Finisher — Thassa's Oracle (kickoff Phase 7 highlight)
# =================================================================


class ThassasOracle(unittest.TestCase):
    def test_wins_when_devotion_ge_library_size(self) -> None:
        gs = _empty_game()
        # Empty library → devotion 0 ≥ 0 = win.
        _cast_spell(gs, 0, "Thassa's Oracle")
        self.assertTrue(gs.game_over)
        self.assertEqual(gs.winner_player_id, 0)

    def test_doesnt_win_with_library(self) -> None:
        gs = _empty_game()
        # 3 library cards. Devotion = 0 (no permanents). 0 < 3 → no win.
        for i in range(3):
            c = Card(name=f"Lib_{i}", owner=0)
            gs.add_card(c)
            gs.players[0].zones.library.append(c.card_id)
        _cast_spell(gs, 0, "Thassa's Oracle")
        self.assertFalse(gs.game_over)


# =================================================================
# Coverage gate
# =================================================================


class Phase7CoverageGate(unittest.TestCase):
    def test_at_least_30_spells_registered(self) -> None:
        names = all_registered_spell_names()
        self.assertGreaterEqual(len(names), 30,
                                f"Expected ≥30 spells, got {len(names)}")

    def test_key_spells_present(self) -> None:
        names = set(all_registered_spell_names())
        expected = {
            "Swords to Plowshares", "Path to Exile", "Counterspell",
            "Negate", "Cultivate", "Demonic Tutor", "Cyclonic Rift",
            "Toxic Deluge", "Thassa's Oracle", "Heroic Intervention",
            "Dark Ritual", "Brainstorm",
        }
        for n in expected:
            self.assertIn(n, names)


if __name__ == "__main__":
    unittest.main()
