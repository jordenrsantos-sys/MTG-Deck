"""Phase 4 — Per-card unit tests for the continuous bucket.

Coverage:
  Layered effects (substrate layer engine):
    - Urborg, Tomb of Yawgmoth: each land is also a Swamp
    - Yavimaya, Cradle of Growth: each land is also a Forest
    - Rhythm of the Wild: grant riot to controller's creatures (stub —
      "nontoken" filter is iter-11+)
    - Anger: grant haste to controller's creatures (stub — zone-
      conditional activation is iter-11+)

  Static modifiers (parallel registry; not yet consulted by cast
  pipeline but verified as queryable data):
    - 5 Medallions + Foundry Inspector + Etherium Sculptor + Goblin
      Anarchomancer (cost reduction)
    - Propaganda + Ghostly Prison (attack tax)
    - Grand Abolisher + Drannith Magistrate + Hexing Squelcher
      (spell restrictions)
    - Exploration + Azusa (additional land drops)

  Multi-card layer ordering (per kickoff Phase 4 gate):
    - Urborg + Mountain → Mountain is a Swamp + Mountain (layer 4)
    - Two type adders compose (Urborg + Yavimaya → each land is BOTH
      Swamp AND Forest)
"""
from __future__ import annotations

import unittest

# Imports trigger all per-card registrations.
import api.engine.pillar_f.v0_2.cards  # noqa: F401
from api.engine.pillar_f.v0_2.cards.continuous import (
    attach_layered_effects_for, detach_layered_effects_for,
    get_static_modifiers, query_active_static_modifiers,
    all_static_modifier_card_names,
)
from api.engine.pillar_f.v0_2.layers import apply_continuous_effects
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
              type_line: str, power=None, toughness=None,
              subtypes=None, keywords=None,
              colors=None) -> Card:
    card = Card(
        name=name, owner=owner, controller=owner, type_line=type_line,
        power=power, toughness=toughness,
        subtypes=list(subtypes or []), keywords=list(keywords or []),
        colors=list(colors or []),
    )
    gs.add_card(card)
    gs.players[owner].zones.battlefield.append(card.card_id)
    return card


# =================================================================
# Layered effects (substrate layer engine)
# =================================================================


class UrborgYavimaya(unittest.TestCase):
    def test_urborg_makes_mountain_also_swamp(self) -> None:
        gs = _empty_game()
        mountain = _put_on_bf(gs, 0, "Mountain",
                              type_line="Basic Land — Mountain",
                              subtypes=["Mountain"])
        urborg = _put_on_bf(gs, 0, "Urborg, Tomb of Yawgmoth",
                            type_line="Legendary Land",
                            subtypes=[])
        attach_layered_effects_for(gs, urborg.card_id)
        table = apply_continuous_effects(gs)
        chars = table[mountain.card_id]
        # Mountain should now have both Mountain and Swamp subtypes.
        self.assertIn("Mountain", chars.subtypes)
        self.assertIn("Swamp", chars.subtypes)

    def test_yavimaya_makes_island_also_forest(self) -> None:
        gs = _empty_game()
        island = _put_on_bf(gs, 0, "Island",
                            type_line="Basic Land — Island",
                            subtypes=["Island"])
        yavimaya = _put_on_bf(gs, 0, "Yavimaya, Cradle of Growth",
                              type_line="Legendary Land",
                              subtypes=[])
        attach_layered_effects_for(gs, yavimaya.card_id)
        table = apply_continuous_effects(gs)
        chars = table[island.card_id]
        self.assertIn("Island", chars.subtypes)
        self.assertIn("Forest", chars.subtypes)

    def test_urborg_plus_yavimaya_composes(self) -> None:
        gs = _empty_game()
        plains = _put_on_bf(gs, 0, "Plains",
                            type_line="Basic Land — Plains",
                            subtypes=["Plains"])
        urborg = _put_on_bf(gs, 0, "Urborg, Tomb of Yawgmoth",
                            type_line="Legendary Land",
                            subtypes=[])
        yavimaya = _put_on_bf(gs, 0, "Yavimaya, Cradle of Growth",
                              type_line="Legendary Land",
                              subtypes=[])
        attach_layered_effects_for(gs, urborg.card_id)
        attach_layered_effects_for(gs, yavimaya.card_id)
        table = apply_continuous_effects(gs)
        chars = table[plains.card_id]
        # Plains now has Plains + Swamp + Forest.
        self.assertIn("Plains", chars.subtypes)
        self.assertIn("Swamp", chars.subtypes)
        self.assertIn("Forest", chars.subtypes)

    def test_detach_removes_effect(self) -> None:
        gs = _empty_game()
        mountain = _put_on_bf(gs, 0, "Mountain",
                              type_line="Basic Land — Mountain",
                              subtypes=["Mountain"])
        urborg = _put_on_bf(gs, 0, "Urborg, Tomb of Yawgmoth",
                            type_line="Legendary Land",
                            subtypes=[])
        attach_layered_effects_for(gs, urborg.card_id)
        # Detach — simulate Urborg leaving the battlefield.
        removed = detach_layered_effects_for(gs, urborg.card_id)
        self.assertEqual(removed, 1)
        table = apply_continuous_effects(gs)
        chars = table[mountain.card_id]
        self.assertIn("Mountain", chars.subtypes)
        self.assertNotIn("Swamp", chars.subtypes)


class RhythmOfTheWildAndAnger(unittest.TestCase):
    def test_rhythm_grants_riot_to_creatures(self) -> None:
        gs = _empty_game()
        # Existing creature with no riot.
        bear = _put_on_bf(gs, 0, "Bear", type_line="Creature — Bear",
                          power="2", toughness="2")
        rotw = _put_on_bf(gs, 0, "Rhythm of the Wild",
                          type_line="Enchantment", subtypes=[])
        attach_layered_effects_for(gs, rotw.card_id)
        table = apply_continuous_effects(gs)
        chars = table[bear.card_id]
        self.assertIn("riot", [k.lower() for k in chars.keywords])

    def test_anger_grants_haste(self) -> None:
        gs = _empty_game()
        bear = _put_on_bf(gs, 0, "Bear", type_line="Creature — Bear",
                          power="2", toughness="2")
        anger = _put_on_bf(gs, 0, "Anger",
                           type_line="Creature — Incarnation",
                           power="2", toughness="2",
                           keywords=["haste"])
        attach_layered_effects_for(gs, anger.card_id)
        table = apply_continuous_effects(gs)
        chars = table[bear.card_id]
        self.assertIn("haste", [k.lower() for k in chars.keywords])


# =================================================================
# Static modifiers (queryable data)
# =================================================================


class CostReducerStaticModifiers(unittest.TestCase):
    def test_jet_medallion_registered(self) -> None:
        mods = get_static_modifiers("Jet Medallion")
        self.assertEqual(len(mods), 1)
        m = mods[0]
        self.assertEqual(m.effect_key, "cost_reduction")
        self.assertEqual(m.params["reduction"], 1)
        self.assertEqual(m.params["color_filter"], "B")

    def test_all_5_medallions_registered(self) -> None:
        expected = {"Jet Medallion": "B", "Sapphire Medallion": "U",
                    "Ruby Medallion": "R", "Pearl Medallion": "W",
                    "Emerald Medallion": "G"}
        for name, color in expected.items():
            mods = get_static_modifiers(name)
            self.assertEqual(len(mods), 1, f"{name} should have 1 mod")
            self.assertEqual(mods[0].params["color_filter"], color)

    def test_foundry_inspector_type_filter(self) -> None:
        mods = get_static_modifiers("Foundry Inspector")
        self.assertEqual(len(mods), 1)
        self.assertEqual(mods[0].params["type_filter"], "Artifact")

    def test_goblin_anarchomancer_multi_color(self) -> None:
        mods = get_static_modifiers("Goblin Anarchomancer")
        self.assertEqual(len(mods), 1)
        self.assertIn("R", mods[0].params["color_filter_any"])
        self.assertIn("G", mods[0].params["color_filter_any"])


class AttackTaxesAndRestrictions(unittest.TestCase):
    def test_propaganda_attack_tax(self) -> None:
        mods = get_static_modifiers("Propaganda")
        self.assertEqual(len(mods), 1)
        self.assertEqual(mods[0].effect_key, "attack_tax")
        self.assertEqual(mods[0].params["tax_mana"], 2)

    def test_ghostly_prison_attack_tax(self) -> None:
        mods = get_static_modifiers("Ghostly Prison")
        self.assertEqual(mods[0].params["tax_mana"], 2)

    def test_grand_abolisher_restricts_opponents(self) -> None:
        mods = get_static_modifiers("Grand Abolisher")
        self.assertEqual(mods[0].effect_key, "spell_restriction")
        self.assertEqual(mods[0].params["applies_to"], "opponents")

    def test_drannith_magistrate_hand_only(self) -> None:
        mods = get_static_modifiers("Drannith Magistrate")
        self.assertIn("cast_from_non_hand_zones",
                     mods[0].params["restrict"])


class AdditionalLandDrops(unittest.TestCase):
    def test_exploration_grants_plus_one(self) -> None:
        mods = get_static_modifiers("Exploration")
        self.assertEqual(mods[0].effect_key, "additional_land_drops")
        self.assertEqual(mods[0].params["additional_drops_per_turn"], 1)

    def test_azusa_grants_plus_two(self) -> None:
        mods = get_static_modifiers("Azusa, Lost but Seeking")
        self.assertEqual(mods[0].params["additional_drops_per_turn"], 2)


class QueryActiveStaticModifiers(unittest.TestCase):
    def test_query_returns_only_battlefield_cards(self) -> None:
        gs = _empty_game()
        # In play: Ruby Medallion (P0) + Sapphire Medallion (P1).
        ruby = _put_on_bf(gs, 0, "Ruby Medallion", type_line="Artifact",
                          subtypes=[])
        sap = _put_on_bf(gs, 1, "Sapphire Medallion", type_line="Artifact",
                         subtypes=[])
        # In hand (NOT in play): Jet Medallion.
        jet = Card(name="Jet Medallion", owner=0, type_line="Artifact")
        gs.add_card(jet)
        gs.players[0].zones.hand.append(jet.card_id)

        active = query_active_static_modifiers(gs, effect_key="cost_reduction")
        names = [a["card_name"] for a in active]
        self.assertIn("Ruby Medallion", names)
        self.assertIn("Sapphire Medallion", names)
        self.assertNotIn("Jet Medallion", names)
        # Each entry attributes the right controller.
        ruby_entries = [a for a in active if a["card_name"] == "Ruby Medallion"]
        self.assertEqual(ruby_entries[0]["controller"], 0)
        sap_entries = [a for a in active if a["card_name"] == "Sapphire Medallion"]
        self.assertEqual(sap_entries[0]["controller"], 1)

    def test_filter_by_effect_key(self) -> None:
        gs = _empty_game()
        _put_on_bf(gs, 0, "Propaganda", type_line="Enchantment",
                   subtypes=[])
        _put_on_bf(gs, 0, "Exploration", type_line="Enchantment",
                   subtypes=[])
        only_taxes = query_active_static_modifiers(gs, effect_key="attack_tax")
        self.assertEqual(len(only_taxes), 1)
        self.assertEqual(only_taxes[0]["card_name"], "Propaganda")
        only_drops = query_active_static_modifiers(
            gs, effect_key="additional_land_drops",
        )
        self.assertEqual(len(only_drops), 1)
        self.assertEqual(only_drops[0]["card_name"], "Exploration")


# =================================================================
# Coverage gate per kickoff Phase 4
# =================================================================


class Phase4CoverageGate(unittest.TestCase):
    def test_all_19_continuous_cards_covered(self) -> None:
        layered = {"Urborg, Tomb of Yawgmoth", "Yavimaya, Cradle of Growth",
                   "Rhythm of the Wild", "Anger"}
        static = {
            "Jet Medallion", "Sapphire Medallion", "Ruby Medallion",
            "Pearl Medallion", "Emerald Medallion",
            "Foundry Inspector", "Etherium Sculptor", "Goblin Anarchomancer",
            "Propaganda", "Ghostly Prison",
            "Grand Abolisher", "Drannith Magistrate", "Hexing Squelcher",
            "Exploration", "Azusa, Lost but Seeking",
        }
        # Sanity: 4 layered + 15 static = 19 in the continuous bucket.
        self.assertEqual(len(layered) + len(static), 19)
        # Static names all present in registry.
        registered = set(all_static_modifier_card_names())
        for name in static:
            self.assertIn(name, registered,
                          f"{name} not registered as static modifier")


if __name__ == "__main__":
    unittest.main()
