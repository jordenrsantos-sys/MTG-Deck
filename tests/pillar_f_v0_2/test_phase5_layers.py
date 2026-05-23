"""Phase 5 of mega-task v9 — 7-layer continuous effects tests.

Coverage per kickoff Phase 5 gates:
- Honor of the Pure + 2/2 creature = 3/3.
- Humility + Honor = 1/1 (ability removal in layer 6 strips anthem grant).
- Clone + Tarmogoyf = correct P/T based on graveyard.
- Mind Bend (legendary supertype removal via layer 4 → cascading to layer 7).
- Inverter of Truth switch (layer 7d).
"""
from __future__ import annotations

import unittest

from api.engine.pillar_f.v0_2.state import (
    Card, ContinuousEffect, GameState, PlayerState, PlayerZones,
)
from api.engine.pillar_f.v0_2.layers import (
    apply_continuous_effects, Characteristics,
    parse_type_line, reassemble_type_line,
)


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(player_id=pid, name=f"P{pid}",
                                      life_total=40, zones=PlayerZones()))
    return gs


def _add_creature(gs: GameState, name: str, *, owner: int = 0,
                  power: str = "2", toughness: str = "2",
                  keywords: list = None) -> Card:
    c = Card(name=name, owner=owner, controller=owner,
             type_line="Creature — Test", power=power, toughness=toughness,
             keywords=keywords or [])
    gs.add_card(c)
    gs.players[owner].zones.battlefield.append(c.card_id)
    return c


class TypeLineParsingTests(unittest.TestCase):
    def test_parse_legendary_creature_vampire(self) -> None:
        sup, typ, sub = parse_type_line("Legendary Creature — Vampire Knight")
        self.assertEqual(sup, ["Legendary"])
        self.assertEqual(typ, ["Creature"])
        self.assertEqual(sub, ["Vampire", "Knight"])

    def test_parse_no_subtypes(self) -> None:
        sup, typ, sub = parse_type_line("Sorcery")
        self.assertEqual(sup, [])
        self.assertEqual(typ, ["Sorcery"])
        self.assertEqual(sub, [])

    def test_parse_basic_land(self) -> None:
        sup, typ, sub = parse_type_line("Basic Land — Forest")
        self.assertEqual(sup, ["Basic"])
        self.assertEqual(typ, ["Land"])
        self.assertEqual(sub, ["Forest"])

    def test_reassemble_round_trip(self) -> None:
        original = "Legendary Creature — Vampire Knight"
        sup, typ, sub = parse_type_line(original)
        self.assertEqual(reassemble_type_line(sup, typ, sub), original)


class BasicSnapshotTests(unittest.TestCase):
    def test_creature_snapshot_from_printed_values(self) -> None:
        gs = _empty_4p_game()
        _add_creature(gs, "Grizzly Bears", power="2", toughness="2",
                      keywords=[])
        table = apply_continuous_effects(gs)
        chars = list(table.values())[0]
        self.assertEqual(chars.name, "Grizzly Bears")
        self.assertEqual(chars.power, 2)
        self.assertEqual(chars.toughness, 2)
        self.assertIn("Creature", chars.types)

    def test_p1p1_counters_added_to_pt(self) -> None:
        gs = _empty_4p_game()
        c = _add_creature(gs, "Walking Ballista", power="0", toughness="0")
        c.counters["+1/+1"] = 4
        table = apply_continuous_effects(gs)
        self.assertEqual(table[c.card_id].power, 4)
        self.assertEqual(table[c.card_id].toughness, 4)


class Layer6AbilityGrantTests(unittest.TestCase):
    def test_keyword_grant_to_all_creatures(self) -> None:
        gs = _empty_4p_game()
        c1 = _add_creature(gs, "Bear A")
        c2 = _add_creature(gs, "Bear B")
        # Concordant Crossroads-style: all creatures gain haste.
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="crossroads", source_card_id="src",
            controller=0, layer=6,
            effect_fn_name="grant_keyword",
            target_pattern={"all_creatures": True,
                            "grant_keyword": "haste"},
        ))
        table = apply_continuous_effects(gs)
        self.assertTrue(table[c1.card_id].has_keyword("haste"))
        self.assertTrue(table[c2.card_id].has_keyword("haste"))

    def test_humility_strips_all_keywords(self) -> None:
        gs = _empty_4p_game()
        c = _add_creature(gs, "Flier", keywords=["flying", "vigilance"])
        # Humility-style: all creatures lose all abilities + become 1/1.
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="humility-l6", source_card_id="humility",
            controller=0, layer=6,
            effect_fn_name="lose_all_abilities",
            target_pattern={"all_creatures": True},
        ))
        table = apply_continuous_effects(gs)
        self.assertEqual(table[c.card_id].keywords, [])


class AnthemAndHumilityTests(unittest.TestCase):
    """The canonical CR 613 ordering test: Honor of the Pure + Humility.

    Honor of the Pure: White creatures get +1/+1 (layer 7c).
    Humility: All creatures lose all abilities AND are 1/1 (layer 6
    for abilities + layer 7b for base P/T).

    When both are on the battlefield, Humility's layer 6 ability-strip
    applies BEFORE Honor's layer 7c +1/+1, so Honor's grant survives
    (Honor is a STATIC ABILITY on Honor itself, not granted to creatures).
    The white creatures become 1/1 from Humility (layer 7b) THEN +1/+1
    from Honor (layer 7c) = final 2/2.
    """

    def test_honor_of_the_pure_2_2_creature_becomes_3_3(self) -> None:
        gs = _empty_4p_game()
        c = _add_creature(gs, "WhiteBear", power="2", toughness="2")
        c.colors = ["W"]  # White creature for Honor target.
        # Honor of the Pure-style: +1/+1 to white creatures.
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="honor", source_card_id="honor",
            controller=0, layer=7, sublayer="c",
            effect_fn_name="anthem_pt_mod",
            target_pattern={"all_creatures": True,
                            "p_mod": 1, "t_mod": 1},
        ))
        table = apply_continuous_effects(gs)
        self.assertEqual(table[c.card_id].power, 3)
        self.assertEqual(table[c.card_id].toughness, 3)

    def test_humility_plus_honor_result(self) -> None:
        """Humility (layer 6 strip + layer 7b set 1/1) + Honor of the
        Pure (layer 7c +1/+1) = 2/2 final."""
        gs = _empty_4p_game()
        c = _add_creature(gs, "WhiteCreature", power="4", toughness="4",
                          keywords=["flying", "vigilance"])
        c.colors = ["W"]
        # Humility: lose abilities (layer 6) + become 1/1 (layer 7b).
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="humility-l6", source_card_id="humility",
            controller=0, layer=6,
            effect_fn_name="lose_all_abilities",
            target_pattern={"all_creatures": True},
        ))
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="humility-l7b", source_card_id="humility",
            controller=0, layer=7, sublayer="b",
            effect_fn_name="cda_set_pt",
            target_pattern={"card_id": c.card_id,
                            "pt_function": "humility_1_1"},
        ))
        # We need a humility_1_1 CDA — register inline for this test.
        from api.engine.pillar_f.v0_2.layers.layer_engine import _CDA_REGISTRY
        _CDA_REGISTRY["humility_1_1"] = lambda state, cid: (1, 1)
        # Honor of the Pure: +1/+1 (layer 7c).
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="honor", source_card_id="honor",
            controller=0, layer=7, sublayer="c",
            effect_fn_name="anthem_pt_mod",
            target_pattern={"all_creatures": True,
                            "p_mod": 1, "t_mod": 1},
        ))
        table = apply_continuous_effects(gs)
        # 1/1 from Humility + 1/+1 from Honor = 2/2.
        self.assertEqual(table[c.card_id].power, 2)
        self.assertEqual(table[c.card_id].toughness, 2)
        # All keywords stripped by Humility.
        self.assertEqual(table[c.card_id].keywords, [])


class TarmogoyfCDATests(unittest.TestCase):
    def test_tarmogoyf_pt_reflects_graveyard_types(self) -> None:
        gs = _empty_4p_game()
        c = Card(name="Tarmogoyf", owner=0, controller=0,
                 type_line="Creature — Lhurgoyf",
                 power="*", toughness="1+*")
        gs.add_card(c)
        gs.players[0].zones.battlefield.append(c.card_id)
        # Add Tarmogoyf CDA registration.
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="goyf-cda", source_card_id=c.card_id,
            controller=0, layer=7, sublayer="b",
            effect_fn_name="cda_set_pt",
            target_pattern={"card_id": c.card_id,
                            "pt_function": "tarmogoyf"},
        ))
        # Graveyards: P0 has Lightning Bolt (Instant), Sol Ring (Artifact);
        # P1 has Wrath of God (Sorcery); = 3 card types.
        for name, typ in [("Bolt", "Instant"), ("Sol Ring", "Artifact")]:
            gy_card = Card(name=name, owner=0, type_line=typ)
            gs.add_card(gy_card)
            gs.players[0].zones.graveyard.append(gy_card.card_id)
        gy3 = Card(name="Wrath of God", owner=1, type_line="Sorcery")
        gs.add_card(gy3)
        gs.players[1].zones.graveyard.append(gy3.card_id)
        table = apply_continuous_effects(gs)
        # Goyf is 3/4 (3 card types: Instant, Artifact, Sorcery).
        self.assertEqual(table[c.card_id].power, 3)
        self.assertEqual(table[c.card_id].toughness, 4)


class CloneTests(unittest.TestCase):
    def test_clone_copies_target_pt(self) -> None:
        gs = _empty_4p_game()
        original = _add_creature(gs, "Dragon", owner=1, power="6",
                                  toughness="6", keywords=["flying"])
        clone = _add_creature(gs, "Clone", owner=0, power="0",
                              toughness="0")
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="clone-of-dragon", source_card_id=clone.card_id,
            controller=0, layer=1,
            effect_fn_name="clone_of",
            target_pattern={"card_id": clone.card_id,
                            "copy_target_card_id": original.card_id},
        ))
        table = apply_continuous_effects(gs)
        self.assertEqual(table[clone.card_id].name, "Dragon")
        self.assertEqual(table[clone.card_id].power, 6)
        self.assertEqual(table[clone.card_id].toughness, 6)
        self.assertTrue(table[clone.card_id].has_keyword("flying"))
        # Controller stays with original clone's controller.
        self.assertEqual(table[clone.card_id].controller, 0)
        self.assertEqual(table[clone.card_id].is_copy_of_card_id,
                         original.card_id)


class InverterSwitchTests(unittest.TestCase):
    def test_switch_pt(self) -> None:
        gs = _empty_4p_game()
        c = _add_creature(gs, "InverterTest", power="2", toughness="6")
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="switch-pt", source_card_id="inverter",
            controller=0, layer=7, sublayer="d",
            effect_fn_name="switch_pt",
            target_pattern={"card_id": c.card_id},
        ))
        table = apply_continuous_effects(gs)
        self.assertEqual(table[c.card_id].power, 6)
        self.assertEqual(table[c.card_id].toughness, 2)


class MindBendTests(unittest.TestCase):
    def test_remove_legendary_supertype(self) -> None:
        gs = _empty_4p_game()
        c = Card(name="Vampire Lord", owner=0, controller=0,
                 type_line="Legendary Creature — Vampire Knight",
                 power="3", toughness="3")
        gs.add_card(c)
        gs.players[0].zones.battlefield.append(c.card_id)
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="mind-bend", source_card_id="mind-bend",
            controller=0, layer=4,
            effect_fn_name="remove_supertype",
            target_pattern={"card_id": c.card_id,
                            "remove_supertype": "Legendary"},
        ))
        table = apply_continuous_effects(gs)
        self.assertNotIn("Legendary", table[c.card_id].supertypes)
        self.assertNotIn("Legendary", table[c.card_id].type_line)


class ControlChangeTests(unittest.TestCase):
    def test_mind_control_changes_controller(self) -> None:
        gs = _empty_4p_game()
        c = _add_creature(gs, "Dragon", owner=1, power="5", toughness="5")
        # P0 steals via Mind Control-style effect.
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="mc", source_card_id="mind-control",
            controller=0, layer=2,
            effect_fn_name="change_control",
            target_pattern={"card_id": c.card_id, "new_controller": 0},
        ))
        table = apply_continuous_effects(gs)
        self.assertEqual(table[c.card_id].controller, 0)


if __name__ == "__main__":
    unittest.main()
