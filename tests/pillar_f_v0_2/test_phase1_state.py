"""Phase 1 of mega-task v9 — GameState object model unit tests.

Coverage per kickoff:
- Zone-by-zone state mutation via direct API calls.
- perspective_view redaction (opponent hand/library opaque, own visible).
- commander_damage_taken_from tracking.
- JSON round-trip for 4-player state with stack + cards.
"""
from __future__ import annotations

import json
import unittest

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, ManaPool,
    Phase, Step, DayNight, StackEntry, STATE_VERSION,
)


def _build_4p_game(*, cards_per_zone: int = 5) -> GameState:
    """Construct a synthetic 4-player game with N cards per zone per
    player. Returns a fully-populated GameState."""
    gs = GameState()
    for pid in range(4):
        ps = PlayerState(player_id=pid, name=f"P{pid}",
                         life_total=40, zones=PlayerZones())
        for zone in ("hand", "library", "battlefield",
                     "graveyard", "exile"):
            for j in range(cards_per_zone):
                c = Card(
                    name=f"Card_{pid}_{zone}_{j}",
                    oracle_id=f"oracle-{pid}-{zone}-{j}",
                    type_line="Creature — Test" if zone == "battlefield" else "Instant",
                    power="2" if zone == "battlefield" else None,
                    toughness="2" if zone == "battlefield" else None,
                    owner=pid, controller=pid,
                )
                gs.add_card(c)
                getattr(ps.zones, zone).append(c.card_id)
        # Commander in command zone.
        cmdr = Card(name=f"Commander_{pid}", oracle_id=f"cmdr-{pid}",
                    type_line="Legendary Creature — Test", owner=pid,
                    controller=pid, power="3", toughness="3")
        gs.add_card(cmdr)
        ps.zones.command.append(cmdr.card_id)
        gs.commander_card_ids[pid] = cmdr.card_id
        gs.players.append(ps)
    return gs


class CardModelTests(unittest.TestCase):
    def test_card_id_unique_per_instance(self) -> None:
        c1 = Card(name="Sol Ring")
        c2 = Card(name="Sol Ring")
        self.assertNotEqual(c1.card_id, c2.card_id)

    def test_creature_detection(self) -> None:
        c = Card(type_line="Legendary Creature — Vampire Knight")
        self.assertTrue(c.is_creature())
        self.assertTrue(c.is_legendary())
        self.assertFalse(c.is_land())

    def test_keyword_detection_case_insensitive(self) -> None:
        c = Card(keywords=["Flying", "Trample"])
        self.assertTrue(c.has_keyword("flying"))
        self.assertTrue(c.has_keyword("TRAMPLE"))
        self.assertFalse(c.has_keyword("haste"))

    def test_power_int_handles_star(self) -> None:
        c = Card(power="*", toughness="*")
        self.assertEqual(c.power_int(), 0)
        self.assertEqual(c.toughness_int(), 0)

    def test_controller_defaults_to_owner(self) -> None:
        c = Card(owner=2)
        self.assertEqual(c.controller, 2)


class ZonesMutationTests(unittest.TestCase):
    def test_move_card_between_zones(self) -> None:
        gs = _build_4p_game(cards_per_zone=2)
        p0 = gs.players[0]
        # Pick a card in P0's hand.
        cid = p0.zones.hand[0]
        # Move to battlefield.
        gs.move_card(cid, from_player=0, from_zone="hand",
                     to_player=0, to_zone="battlefield")
        self.assertNotIn(cid, p0.zones.hand)
        self.assertIn(cid, p0.zones.battlefield)
        # Controller stays with owner.
        self.assertEqual(gs.get_card(cid).controller, 0)

    def test_move_to_battlefield_under_opponent_control(self) -> None:
        gs = _build_4p_game(cards_per_zone=2)
        cid = gs.players[0].zones.hand[0]
        # P1 gains control via move (simulates Mind Control etc.).
        gs.move_card(cid, from_player=0, from_zone="hand",
                     to_player=1, to_zone="battlefield")
        self.assertNotIn(cid, gs.players[0].zones.hand)
        self.assertIn(cid, gs.players[1].zones.battlefield)
        self.assertEqual(gs.get_card(cid).controller, 1)

    def test_move_card_not_in_zone_raises(self) -> None:
        gs = _build_4p_game(cards_per_zone=1)
        with self.assertRaises(ValueError):
            gs.move_card("nonexistent-card-id",
                         from_player=0, from_zone="hand",
                         to_player=0, to_zone="battlefield")

    def test_library_add_to_top(self) -> None:
        z = PlayerZones()
        z.add_card("first", "library", to_top=False)
        z.add_card("second-to-top", "library", to_top=True)
        # Top is index 0.
        self.assertEqual(z.library, ["second-to-top", "first"])

    def test_find_zone_returns_correct_zone(self) -> None:
        z = PlayerZones()
        z.battlefield.append("bf-card")
        z.graveyard.append("gy-card")
        self.assertEqual(z.find_zone("bf-card"), "battlefield")
        self.assertEqual(z.find_zone("gy-card"), "graveyard")
        self.assertIsNone(z.find_zone("never-existed"))


class CommanderDamageTests(unittest.TestCase):
    def test_commander_damage_tracking(self) -> None:
        gs = _build_4p_game(cards_per_zone=1)
        # P0 takes 7 damage from P1's commander (oracle id "cmdr-1").
        gs.players[0].commander_damage_taken_from["cmdr-1"] = 7
        self.assertEqual(gs.players[0].commander_damage_taken_from["cmdr-1"], 7)

    def test_commander_card_id_per_player(self) -> None:
        gs = _build_4p_game(cards_per_zone=1)
        for pid in range(4):
            self.assertIn(pid, gs.commander_card_ids)
            cmdr_id = gs.commander_card_ids[pid]
            self.assertIn(cmdr_id, gs.players[pid].zones.command)


class ManaPoolTests(unittest.TestCase):
    def test_empty_clears_pool(self) -> None:
        mp = ManaPool(W=2, U=3, C=1)
        self.assertEqual(mp.total(), 6)
        mp.empty()
        self.assertEqual(mp.total(), 0)

    def test_pool_round_trip(self) -> None:
        mp = ManaPool(W=1, U=2, B=3, R=4, G=5, C=6)
        mp2 = ManaPool.from_dict(mp.to_dict())
        self.assertEqual(mp.total(), mp2.total())
        self.assertEqual(mp.W, mp2.W)
        self.assertEqual(mp.C, mp2.C)


class JsonRoundTripTests(unittest.TestCase):
    def test_4p_game_json_round_trip_preserves_state(self) -> None:
        gs = _build_4p_game(cards_per_zone=5)
        gs.turn_number = 7
        gs.phase = Phase.COMBAT
        gs.step = Step.DECLARE_ATTACKERS
        gs.active_player = 2
        gs.priority_holder = 2
        gs.the_monarch = 1
        gs.day_or_night = DayNight.DAY
        # Push 3 stack entries.
        gs.stack.append(StackEntry(entry_id="se1", card_id=None,
                                   controller=0, entry_type="spell",
                                   description="Sorcery"))
        gs.stack.append(StackEntry(entry_id="se2", card_id=None,
                                   controller=1, entry_type="spell",
                                   description="Counter"))
        gs.stack.append(StackEntry(entry_id="se3", card_id=None,
                                   controller=2, entry_type="spell",
                                   description="Counter-counter"))
        # Round-trip via JSON.
        s = gs.to_json()
        gs2 = GameState.from_json(s)

        self.assertEqual(gs2.version, STATE_VERSION)
        self.assertEqual(gs2.turn_number, 7)
        self.assertEqual(gs2.phase, Phase.COMBAT)
        self.assertEqual(gs2.step, Step.DECLARE_ATTACKERS)
        self.assertEqual(gs2.active_player, 2)
        self.assertEqual(gs2.priority_holder, 2)
        self.assertEqual(gs2.the_monarch, 1)
        self.assertEqual(gs2.day_or_night, DayNight.DAY)
        self.assertEqual(len(gs2.players), 4)
        self.assertEqual(len(gs2.stack), 3)
        self.assertEqual(gs2.stack[0].entry_id, "se1")
        self.assertEqual(gs2.stack[2].controller, 2)
        # Cards intact.
        self.assertEqual(len(gs2.cards_by_id), len(gs.cards_by_id))
        # Zones intact.
        for pid in range(4):
            self.assertEqual(len(gs2.players[pid].zones.hand),
                             len(gs.players[pid].zones.hand))
            self.assertEqual(gs2.players[pid].zones.battlefield,
                             gs.players[pid].zones.battlefield)
        # Commander mapping intact (note: JSON keys are strings; from_dict
        # converts back to ints).
        self.assertEqual(gs2.commander_card_ids, gs.commander_card_ids)

    def test_round_trip_preserves_card_state(self) -> None:
        gs = _build_4p_game(cards_per_zone=1)
        # Modify a card's state.
        cid = gs.players[0].zones.battlefield[0]
        card = gs.get_card(cid)
        card.tapped = True
        card.damage_marked = 3
        card.counters["+1/+1"] = 2
        card.summoning_sick = True
        # Round-trip.
        gs2 = GameState.from_json(gs.to_json())
        card2 = gs2.get_card(cid)
        self.assertTrue(card2.tapped)
        self.assertEqual(card2.damage_marked, 3)
        self.assertEqual(card2.counters["+1/+1"], 2)
        self.assertTrue(card2.summoning_sick)


class PerspectiveViewTests(unittest.TestCase):
    def test_opponent_hand_is_opaque(self) -> None:
        gs = _build_4p_game(cards_per_zone=3)
        view = gs.perspective_view(viewer_player_id=0)
        # P0's hand cards remain fully visible.
        for cid in gs.players[0].zones.hand:
            entry = view["cards_by_id"][cid]
            self.assertNotIn("opaque", entry)
            self.assertIn("name", entry)
        # P1's hand cards are opaque.
        for cid in gs.players[1].zones.hand:
            entry = view["cards_by_id"][cid]
            self.assertTrue(entry.get("opaque"))
            self.assertEqual(entry.get("zone"), "hand")
            self.assertEqual(entry.get("owner"), 1)

    def test_all_libraries_opaque_including_viewers(self) -> None:
        gs = _build_4p_game(cards_per_zone=3)
        view = gs.perspective_view(viewer_player_id=0)
        # Viewer's OWN library is opaque (CR: nobody sees library order).
        for cid in gs.players[0].zones.library:
            entry = view["cards_by_id"][cid]
            self.assertTrue(entry.get("opaque"),
                            f"viewer's own library card {cid} should be opaque")
        # Same for opponents.
        for cid in gs.players[2].zones.library:
            entry = view["cards_by_id"][cid]
            self.assertTrue(entry.get("opaque"))

    def test_battlefield_is_public(self) -> None:
        gs = _build_4p_game(cards_per_zone=3)
        view = gs.perspective_view(viewer_player_id=0)
        for pid in range(4):
            for cid in gs.players[pid].zones.battlefield:
                entry = view["cards_by_id"][cid]
                self.assertNotIn("opaque", entry,
                                 f"battlefield card {cid} should be visible")
                self.assertIn("name", entry)

    def test_face_down_battlefield_opaque_to_non_controller(self) -> None:
        gs = _build_4p_game(cards_per_zone=2)
        # P1 has a face-down card on battlefield.
        cid = gs.players[1].zones.battlefield[0]
        card = gs.get_card(cid)
        card.face_down = True
        # P0 viewing: opaque.
        view_p0 = gs.perspective_view(viewer_player_id=0)
        self.assertTrue(view_p0["cards_by_id"][cid].get("opaque"))
        # P1 (controller) viewing: visible.
        view_p1 = gs.perspective_view(viewer_player_id=1)
        self.assertNotIn("opaque", view_p1["cards_by_id"][cid])

    def test_view_carries_viewer_id(self) -> None:
        gs = _build_4p_game(cards_per_zone=1)
        view = gs.perspective_view(viewer_player_id=2)
        self.assertEqual(view["viewer_player_id"], 2)

    def test_stack_is_public_in_perspective_view(self) -> None:
        gs = _build_4p_game(cards_per_zone=1)
        gs.stack.append(StackEntry(entry_id="vis", card_id=None,
                                   controller=0, entry_type="spell",
                                   description="Public Spell"))
        view = gs.perspective_view(viewer_player_id=3)
        self.assertEqual(len(view["stack"]), 1)
        self.assertEqual(view["stack"][0]["description"], "Public Spell")


if __name__ == "__main__":
    unittest.main()
