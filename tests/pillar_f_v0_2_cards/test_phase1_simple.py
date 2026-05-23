"""Phase 1 — Per-card unit tests for the simple-permanent bucket.

Coverage:
  - 5 in-top-500 basic lands (Plains, Island, Swamp, Mountain, Forest)
  - Wastes + snow-covered basics (registered by Phase 0, not in top 500
    but verified here for completeness)
  - The 6 in-top-500 mana dorks (Llanowar Elves, Birds of Paradise,
    Elvish Mystic, Fyndhorn Elves, Avacyn's Pilgrim, Arbor Elf)
  - Sequencing (multiple taps accumulate, any-color dork honors choice)
  - Phase 1 doesn't need any layer-engine or replacement work — these
    cards have no continuous or replacement abilities. Phase 4 and
    Phase 5 will exercise those.

The "card enters the battlefield, key ability fires correctly" gate
from the kickoff is exercised by: (a) constructing a Card for each
named permanent, (b) putting it on the battlefield, (c) pushing the
named ability onto the stack as a tap-activated entry, (d) resolving,
(e) asserting mana-pool state.
"""
from __future__ import annotations

import unittest

# Triggers all per-card registrations.
import api.engine.pillar_f.v0_2.cards  # noqa: F401
from api.engine.pillar_f.v0_2.cards.simple.basic_lands import BASIC_LAND_RESOLVERS
from api.engine.pillar_f.v0_2.cards.simple.mana_dorks import MANA_DORK_RESOLVERS
from api.engine.pillar_f.v0_2.stack import push_to_stack, resolve_top
from api.engine.pillar_f.v0_2.state import Card, GameState, PlayerState, PlayerZones


def _empty_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(
            player_id=pid, name=f"P{pid}", life_total=40, zones=PlayerZones(),
        ))
    gs.active_player = 0
    return gs


def _put_on_battlefield(gs: GameState, owner: int, name: str,
                       type_line: str) -> Card:
    card = Card(name=name, owner=owner, controller=owner, type_line=type_line)
    gs.add_card(card)
    gs.players[owner].zones.battlefield.append(card.card_id)
    return card


def _tap_for_mana(gs: GameState, card: Card, resolver_name: str,
                 payment_extra: dict = None) -> None:
    """Common helper: tap the card + push its tap-mana ability + resolve."""
    card.tapped = True  # CR 605.1: tap is the cost; pay it before stack push
    payload = {"resolver": resolver_name}
    if payment_extra:
        payload.update(payment_extra)
    push_to_stack(
        gs, card_id=card.card_id, controller=card.controller,
        entry_type="activated", payment=payload,
        description=f"{card.name} — tap for mana",
    )
    resolve_top(gs)


# =================================================================
# Basic lands
# =================================================================


class BasicLands(unittest.TestCase):
    """5 in-top-500 basic lands + Wastes + snow basics."""

    def test_plains_taps_for_white(self) -> None:
        gs = _empty_game()
        plains = _put_on_battlefield(gs, 0, "Plains", "Basic Land — Plains")
        _tap_for_mana(gs, plains, BASIC_LAND_RESOLVERS["Plains"])
        self.assertTrue(plains.tapped)
        self.assertEqual(gs.players[0].mana_pool.W, 1)

    def test_island_taps_for_blue(self) -> None:
        gs = _empty_game()
        island = _put_on_battlefield(gs, 0, "Island", "Basic Land — Island")
        _tap_for_mana(gs, island, BASIC_LAND_RESOLVERS["Island"])
        self.assertEqual(gs.players[0].mana_pool.U, 1)

    def test_swamp_taps_for_black(self) -> None:
        gs = _empty_game()
        swamp = _put_on_battlefield(gs, 0, "Swamp", "Basic Land — Swamp")
        _tap_for_mana(gs, swamp, BASIC_LAND_RESOLVERS["Swamp"])
        self.assertEqual(gs.players[0].mana_pool.B, 1)

    def test_mountain_taps_for_red(self) -> None:
        gs = _empty_game()
        mountain = _put_on_battlefield(gs, 0, "Mountain", "Basic Land — Mountain")
        _tap_for_mana(gs, mountain, BASIC_LAND_RESOLVERS["Mountain"])
        self.assertEqual(gs.players[0].mana_pool.R, 1)

    def test_forest_taps_for_green(self) -> None:
        gs = _empty_game()
        forest = _put_on_battlefield(gs, 0, "Forest", "Basic Land — Forest")
        _tap_for_mana(gs, forest, BASIC_LAND_RESOLVERS["Forest"])
        self.assertEqual(gs.players[0].mana_pool.G, 1)

    def test_wastes_taps_for_colorless(self) -> None:
        gs = _empty_game()
        wastes = _put_on_battlefield(gs, 0, "Wastes", "Basic Land")
        _tap_for_mana(gs, wastes, BASIC_LAND_RESOLVERS["Wastes"])
        self.assertEqual(gs.players[0].mana_pool.C, 1)

    def test_snow_covered_basics_use_same_resolver(self) -> None:
        """Snow-covered basics produce the same color as their non-snow
        counterpart (iter-10 collapses snow to C+source-tag per the
        ManaPool docstring; the EFFECT side is identical W/U/B/R/G)."""
        gs = _empty_game()
        snow_plains = _put_on_battlefield(
            gs, 0, "Snow-Covered Plains", "Basic Snow Land — Plains",
        )
        _tap_for_mana(gs, snow_plains,
                     BASIC_LAND_RESOLVERS["Snow-Covered Plains"])
        self.assertEqual(gs.players[0].mana_pool.W, 1)


class MultipleTapsAccumulate(unittest.TestCase):
    """Sanity: two Forests + one Plains add 2 G + 1 W to the pool."""

    def test_two_forests_one_plains(self) -> None:
        gs = _empty_game()
        f1 = _put_on_battlefield(gs, 0, "Forest", "Basic Land — Forest")
        f2 = _put_on_battlefield(gs, 0, "Forest", "Basic Land — Forest")
        plains = _put_on_battlefield(gs, 0, "Plains", "Basic Land — Plains")
        _tap_for_mana(gs, f1, "basic_tap_G")
        _tap_for_mana(gs, f2, "basic_tap_G")
        _tap_for_mana(gs, plains, "basic_tap_W")
        self.assertEqual(gs.players[0].mana_pool.G, 2)
        self.assertEqual(gs.players[0].mana_pool.W, 1)
        self.assertEqual(gs.players[0].mana_pool.total(), 3)

    def test_taps_credit_correct_controller(self) -> None:
        """A Forest controlled by player 2 taps mana INTO player 2's
        pool, not player 0's (the active player)."""
        gs = _empty_game()
        # gs.active_player stays 0 — but the land's controller is 2.
        forest_p2 = _put_on_battlefield(gs, 2, "Forest", "Basic Land — Forest")
        _tap_for_mana(gs, forest_p2, "basic_tap_G")
        self.assertEqual(gs.players[2].mana_pool.G, 1)
        self.assertEqual(gs.players[0].mana_pool.G, 0)


# =================================================================
# Mana dorks
# =================================================================


class ManaDorks(unittest.TestCase):
    """6 in-top-500 mana dorks."""

    def _dork(self, gs: GameState, name: str) -> Card:
        # Mana dorks are creatures, not lands; type line reflects that.
        # Summoning sickness DOES apply at iter-10 substrate level —
        # the dork must have summoning_sick=False to tap. Use a card
        # that's already past its summon turn for the test.
        card = _put_on_battlefield(
            gs, 0, name, "Creature — Elf Druid",
        )
        card.summoning_sick = False
        # Mana dorks have power/toughness; set explicitly.
        card.power = "1"
        card.toughness = "1"
        return card

    def test_llanowar_elves_taps_for_green(self) -> None:
        gs = _empty_game()
        llanowar = self._dork(gs, "Llanowar Elves")
        _tap_for_mana(gs, llanowar, MANA_DORK_RESOLVERS["Llanowar Elves"])
        self.assertEqual(gs.players[0].mana_pool.G, 1)

    def test_elvish_mystic_taps_for_green(self) -> None:
        gs = _empty_game()
        em = self._dork(gs, "Elvish Mystic")
        _tap_for_mana(gs, em, MANA_DORK_RESOLVERS["Elvish Mystic"])
        self.assertEqual(gs.players[0].mana_pool.G, 1)

    def test_fyndhorn_elves_taps_for_green(self) -> None:
        gs = _empty_game()
        fe = self._dork(gs, "Fyndhorn Elves")
        _tap_for_mana(gs, fe, MANA_DORK_RESOLVERS["Fyndhorn Elves"])
        self.assertEqual(gs.players[0].mana_pool.G, 1)

    def test_arbor_elf_taps_for_green(self) -> None:
        gs = _empty_game()
        ae = self._dork(gs, "Arbor Elf")
        _tap_for_mana(gs, ae, MANA_DORK_RESOLVERS["Arbor Elf"])
        self.assertEqual(gs.players[0].mana_pool.G, 1)

    def test_avacyns_pilgrim_taps_for_white(self) -> None:
        gs = _empty_game()
        ap = self._dork(gs, "Avacyn's Pilgrim")
        _tap_for_mana(gs, ap, MANA_DORK_RESOLVERS["Avacyn's Pilgrim"])
        self.assertEqual(gs.players[0].mana_pool.W, 1)

    def test_birds_of_paradise_any_color_default_C(self) -> None:
        gs = _empty_game()
        bop = self._dork(gs, "Birds of Paradise")
        _tap_for_mana(gs, bop, MANA_DORK_RESOLVERS["Birds of Paradise"])
        # No color specified → defaults to C (iter-10 stub; iter-11+
        # will plumb the LLM choice).
        self.assertEqual(gs.players[0].mana_pool.C, 1)

    def test_birds_of_paradise_with_explicit_color(self) -> None:
        gs = _empty_game()
        bop = self._dork(gs, "Birds of Paradise")
        _tap_for_mana(gs, bop, MANA_DORK_RESOLVERS["Birds of Paradise"],
                     payment_extra={"color": "B"})
        self.assertEqual(gs.players[0].mana_pool.B, 1)
        self.assertEqual(gs.players[0].mana_pool.C, 0)

    def test_noble_hierarch_with_explicit_color(self) -> None:
        gs = _empty_game()
        nh = self._dork(gs, "Noble Hierarch")
        # Noble Hierarch's any-color ability stub.
        _tap_for_mana(gs, nh, MANA_DORK_RESOLVERS["Noble Hierarch"],
                     payment_extra={"color": "U"})
        self.assertEqual(gs.players[0].mana_pool.U, 1)


# =================================================================
# Coverage gate: Phase 1's named cards are all in the registry maps
# =================================================================


class Phase1CoverageGate(unittest.TestCase):
    """Lock in: the 5 in-top-500 basic-land names + the 6 in-top-500
    mana-dork names all have resolver entries. If we ever break this
    invariant by name typo or refactor, this test catches it."""

    def test_all_top500_basic_lands_have_resolvers(self) -> None:
        for name in ("Plains", "Island", "Swamp", "Mountain", "Forest"):
            self.assertIn(name, BASIC_LAND_RESOLVERS,
                          f"{name} missing from BASIC_LAND_RESOLVERS")

    def test_all_top500_mana_dorks_have_resolvers(self) -> None:
        for name in ("Llanowar Elves", "Birds of Paradise", "Elvish Mystic",
                     "Fyndhorn Elves", "Avacyn's Pilgrim", "Arbor Elf"):
            self.assertIn(name, MANA_DORK_RESOLVERS,
                          f"{name} missing from MANA_DORK_RESOLVERS")


if __name__ == "__main__":
    unittest.main()
