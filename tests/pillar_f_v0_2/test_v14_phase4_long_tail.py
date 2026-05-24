"""Phase 4 of mega-task v14 — long-tail activated handlers tests.

Verifies that each of the 23 cards previously in v11's "fall-through"
bucket now has an ActivatedAbilityMeta + resolver registered. Per-card
behavior tests for the cards whose resolvers do non-trivial work
(Crystal Vein, Reassembling Skeleton, Staff of Domination, Urza's
lands urzatron bonus).

Per-card coverage check uses the v11 framework's
`get_activated_abilities_for_card(card_name)`. The coverage tool's
fall-through detection passes once this returns a non-empty list.
"""
from __future__ import annotations

import unittest

# Eager-import the cards package + long-tail module so registrations
# fire before tests query the registry.
from api.engine.pillar_f.v0_2.cards import activated  # noqa: F401

from api.engine.pillar_f.v0_2.cards.activated.framework import (
    get_activated_abilities_for_card,
)
from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, StackEntry,
)
from api.engine.pillar_f.v0_2.stack import push_to_stack, resolve_top


LONG_TAIL_CARDS = [
    "Ancient Den",
    "Bender's Waterskin",
    "Blazemire Verge",
    "Castle Garenbrig",
    "Castle Locthwain",
    "Crystal Vein",
    "Graven Cairns",
    "Great Furnace",
    "Hall of Heliod's Generosity",
    "Mossfire Valley",
    "Palladium Myr",
    "Reassembling Skeleton",
    "Reflecting Pool",
    "Shizo, Death's Storehouse",
    "Staff of Domination",
    "Sungrass Prairie",
    "The Mycosynth Gardens",
    "Throne of Eldraine",
    "Treasure Vault",
    "Underground Sea",
    "Urza's Mine",
    "Urza's Power Plant",
    "Urza's Tower",
]


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(
            player_id=pid, name=f"P{pid}",
            life_total=40, zones=PlayerZones(),
        ))
    gs.active_player = 0
    gs.turn_number = 3
    return gs


class CoverageRegistrationTests(unittest.TestCase):
    """Every long-tail card now has at least one registered
    activated ability -- this is what oracle_seed_coverage.py
    checks to mark a card 'full handler' instead of fall-through."""

    def test_all_23_cards_have_registered_abilities(self) -> None:
        missing: list[str] = []
        for name in LONG_TAIL_CARDS:
            abilities = get_activated_abilities_for_card(name)
            if not abilities:
                missing.append(name)
        self.assertEqual(
            missing, [],
            f"Cards still in fall-through bucket: {missing}",
        )

    def test_long_tail_count_matches_kickoff(self) -> None:
        self.assertEqual(len(LONG_TAIL_CARDS), 23)


class ManaProductionTests(unittest.TestCase):
    """Spot-check that the registered tap-for-mana resolvers actually
    add the expected mana to the controller's pool when resolved."""

    def _add_to_battlefield(
        self, gs: GameState, owner: int, *, name: str,
    ) -> Card:
        c = Card(
            name=name, owner=owner, controller=owner,
            type_line="Land",
        )
        gs.add_card(c)
        gs.players[owner].zones.battlefield.append(c.card_id)
        return c

    def test_great_furnace_adds_red(self) -> None:
        gs = _empty_4p_game()
        c = self._add_to_battlefield(gs, 0, name="Great Furnace")
        push_to_stack(
            gs, card_id=c.card_id, controller=0,
            entry_type="activated",
            payment={"resolver": "act_great_furnace_tap"},
        )
        resolve_top(gs)
        self.assertEqual(gs.players[0].mana_pool.R, 1)

    def test_ancient_den_adds_white(self) -> None:
        gs = _empty_4p_game()
        c = self._add_to_battlefield(gs, 1, name="Ancient Den")
        push_to_stack(
            gs, card_id=c.card_id, controller=1,
            entry_type="activated",
            payment={"resolver": "act_ancient_den_tap"},
        )
        resolve_top(gs)
        self.assertEqual(gs.players[1].mana_pool.W, 1)

    def test_palladium_myr_adds_2_colorless(self) -> None:
        gs = _empty_4p_game()
        c = self._add_to_battlefield(gs, 0, name="Palladium Myr")
        push_to_stack(
            gs, card_id=c.card_id, controller=0,
            entry_type="activated",
            payment={"resolver": "act_palladium_myr_tap"},
        )
        resolve_top(gs)
        self.assertEqual(gs.players[0].mana_pool.C, 2)

    def test_urza_tower_normal_adds_one_colorless(self) -> None:
        """Without urzatron_active flag in payment, base mana = 1."""
        gs = _empty_4p_game()
        c = self._add_to_battlefield(gs, 0, name="Urza's Tower")
        push_to_stack(
            gs, card_id=c.card_id, controller=0,
            entry_type="activated",
            payment={"resolver": "act_urzas_tower_tap"},
        )
        resolve_top(gs)
        self.assertEqual(gs.players[0].mana_pool.C, 1)

    def test_urza_tower_with_urzatron_bonus_adds_three(self) -> None:
        """With urzatron_active=True flag, the bonus path adds 3."""
        gs = _empty_4p_game()
        c = self._add_to_battlefield(gs, 0, name="Urza's Tower")
        push_to_stack(
            gs, card_id=c.card_id, controller=0,
            entry_type="activated",
            payment={
                "resolver": "act_urzas_tower_tap",
                "urzatron_active": True,
            },
        )
        resolve_top(gs)
        self.assertEqual(gs.players[0].mana_pool.C, 3)


class SpecialActivationsTests(unittest.TestCase):
    """Crystal Vein's sac-for-mana + Staff of Domination's untap +
    Reassembling Skeleton's graveyard recursion are the resolvers
    that do MORE than simple tap-for-mana. Spot-check each."""

    def test_crystal_vein_adds_two_colorless(self) -> None:
        gs = _empty_4p_game()
        c = Card(name="Crystal Vein", owner=0, controller=0,
                 type_line="Land")
        gs.add_card(c)
        gs.players[0].zones.battlefield.append(c.card_id)
        push_to_stack(
            gs, card_id=c.card_id, controller=0,
            entry_type="activated",
            payment={"resolver": "act_crystal_vein_tap_sac"},
        )
        resolve_top(gs)
        self.assertEqual(gs.players[0].mana_pool.C, 2)

    def test_staff_of_domination_untap_target(self) -> None:
        gs = _empty_4p_game()
        staff = Card(name="Staff of Domination", owner=0, controller=0,
                     type_line="Artifact")
        target = Card(name="Goblin", owner=0, controller=0,
                       type_line="Creature -- Goblin",
                       power="1", toughness="1", tapped=True)
        gs.add_card(staff)
        gs.add_card(target)
        gs.players[0].zones.battlefield.extend([staff.card_id, target.card_id])
        self.assertTrue(target.tapped)
        push_to_stack(
            gs, card_id=staff.card_id, controller=0,
            entry_type="activated",
            payment={
                "resolver": "act_staff_of_domination_untap",
                "target_card_id": target.card_id,
            },
        )
        resolve_top(gs)
        self.assertFalse(target.tapped)

    def test_reassembling_skeleton_returns_from_graveyard(self) -> None:
        gs = _empty_4p_game()
        sk = Card(name="Reassembling Skeleton", owner=0, controller=0,
                   type_line="Creature -- Skeleton",
                   power="1", toughness="1", tapped=False)
        gs.add_card(sk)
        gs.players[0].zones.graveyard.append(sk.card_id)
        self.assertNotIn(sk.card_id, gs.players[0].zones.battlefield)
        push_to_stack(
            gs, card_id=sk.card_id, controller=0,
            entry_type="activated",
            payment={
                "resolver": "act_reassembling_skeleton_recur",
                "source_card_id": sk.card_id,
            },
        )
        resolve_top(gs)
        self.assertIn(sk.card_id, gs.players[0].zones.battlefield)
        self.assertNotIn(sk.card_id, gs.players[0].zones.graveyard)
        # Returns TAPPED per the oracle text.
        self.assertTrue(sk.tapped)


if __name__ == "__main__":
    unittest.main()
