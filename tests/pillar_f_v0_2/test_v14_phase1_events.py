"""Phase 1 of mega-task v14 — substrate event emission tests.

Covers the three new event types in api/engine/pillar_f/v0_2/
replacement/events.py:

- TokenCreateEvent (NEW)
- LibrarySearchEvent (NEW)
- CombatDamageDealtEvent (PROMOTED from v11 shim)

Verifies:
- Dataclass construction with sensible defaults
- Field shapes match the kickoff contract
- v11 shim re-exports the substrate dataclass (drop-in compat)
- Combat code emits CombatDamageDealtEvent at each damage site
  (verified via patching fire_event_triggers)
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List
from unittest.mock import patch

from api.engine.pillar_f.v0_2.replacement.events import (
    TokenCreateEvent, LibrarySearchEvent, CombatDamageDealtEvent,
    EVENT_TYPES,
)
from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones,
)
from api.engine.pillar_f.v0_2.combat import (
    AttackerDeclaration, BlockerAssignment, CombatState,
    declare_attackers, declare_blockers, deal_combat_damage,
)


class TokenCreateEventTests(unittest.TestCase):
    def test_default_construction(self) -> None:
        evt = TokenCreateEvent()
        self.assertEqual(evt.event_type, "TokenCreateEvent")
        self.assertEqual(evt.count, 1)
        self.assertIsNone(evt.creator_card_id)
        self.assertEqual(evt.token_types, [])
        self.assertFalse(evt.prevent)

    def test_treasure_token_spec(self) -> None:
        evt = TokenCreateEvent(
            creator_card_id="src-1",
            controller_id=0,
            token_name="Treasure Token",
            token_types=["Artifact"],
            token_subtypes=["Treasure"],
            count=2,
        )
        self.assertEqual(evt.token_name, "Treasure Token")
        self.assertEqual(evt.count, 2)

    def test_event_type_in_registry(self) -> None:
        self.assertIn("TokenCreateEvent", EVENT_TYPES)


class LibrarySearchEventTests(unittest.TestCase):
    def test_default_construction(self) -> None:
        evt = LibrarySearchEvent()
        self.assertEqual(evt.event_type, "LibrarySearchEvent")
        self.assertTrue(evt.shuffle_after)
        self.assertFalse(evt.reveal)

    def test_tutor_predicate(self) -> None:
        evt = LibrarySearchEvent(
            searcher_id=0, target_player_id=0,
            search_predicate="any", shuffle_after=True,
        )
        self.assertEqual(evt.search_predicate, "any")

    def test_fetch_land_predicate(self) -> None:
        evt = LibrarySearchEvent(
            searcher_id=1, target_player_id=1,
            search_predicate="basic_land",
        )
        self.assertEqual(evt.search_predicate, "basic_land")

    def test_event_type_in_registry(self) -> None:
        self.assertIn("LibrarySearchEvent", EVENT_TYPES)


class CombatDamageDealtEventTests(unittest.TestCase):
    def test_default_construction(self) -> None:
        evt = CombatDamageDealtEvent()
        self.assertEqual(evt.event_type, "CombatDamageDealtEvent")
        self.assertEqual(evt.amount, 0)
        self.assertFalse(evt.is_first_strike)
        self.assertEqual(evt.target_kind, "player")

    def test_typical_creature_attack(self) -> None:
        evt = CombatDamageDealtEvent(
            source_card_id="bear-1", source_controller=0,
            target_kind="player", target_id=1, amount=2,
        )
        self.assertEqual(evt.source_card_id, "bear-1")
        self.assertEqual(evt.target_id, 1)
        self.assertEqual(evt.amount, 2)

    def test_v11_shim_re_exports_substrate_class(self) -> None:
        """The v11 cards/triggered/framework.py shim should re-export
        the substrate dataclass so existing listeners that
        `isinstance(e, CombatDamageDealtEvent)` against the v11 import
        path still match substrate-emitted events."""
        from api.engine.pillar_f.v0_2.cards.triggered.framework import (
            CombatDamageDealtEvent as ShimCombatDamageDealtEvent,
        )
        self.assertIs(ShimCombatDamageDealtEvent, CombatDamageDealtEvent)


class CombatEmissionEndToEndTests(unittest.TestCase):
    """v14 Phase 1 wired substrate combat code to emit
    CombatDamageDealtEvent at 4 sites: unblocked, blocked-to-blocker,
    trample-excess, blocker-to-attacker. Verify by patching
    fire_event_triggers and asserting it gets called."""

    def _empty_4p_game(self) -> GameState:
        gs = GameState()
        for pid in range(4):
            gs.players.append(PlayerState(
                player_id=pid, name=f"P{pid}",
                life_total=40, zones=PlayerZones(),
            ))
        gs.active_player = 0
        return gs

    def _make_creature(
        self, gs: GameState, owner: int, *,
        name: str, power: str = "2", toughness: str = "2",
        keywords=None,
    ) -> Card:
        c = Card(
            name=name, owner=owner, controller=owner,
            type_line="Creature -- Bear",
            power=power, toughness=toughness,
            keywords=list(keywords or []),
        )
        gs.add_card(c)
        gs.players[owner].zones.battlefield.append(c.card_id)
        return c

    def test_unblocked_attack_emits_combat_damage_event(self) -> None:
        gs = self._empty_4p_game()
        bear = self._make_creature(gs, 0, name="Bear",
                                    power="3", toughness="3")
        cs = declare_attackers(gs, [
            AttackerDeclaration(attacker_card_id=bear.card_id, target=1),
        ])
        declare_blockers(gs, [], combat_state=cs)
        with patch(
            "api.engine.pillar_f.v0_2.cards.triggered.framework."
            "fire_event_triggers",
        ) as mock_fire:
            deal_combat_damage(gs, cs, is_first_strike=False)
        # At least one CombatDamageDealtEvent fired (the unblocked
        # 3 damage to P1).
        cdd_calls = [
            call for call in mock_fire.call_args_list
            if isinstance(call.args[1], CombatDamageDealtEvent)
        ]
        self.assertGreaterEqual(len(cdd_calls), 1)
        evt = cdd_calls[0].args[1]
        self.assertEqual(evt.source_card_id, bear.card_id)
        self.assertEqual(evt.target_id, 1)
        self.assertEqual(evt.amount, 3)
        self.assertEqual(evt.target_kind, "player")
        self.assertFalse(evt.is_first_strike)

    def test_blocked_attack_emits_event_to_blocker_and_back(self) -> None:
        gs = self._empty_4p_game()
        atk = self._make_creature(gs, 0, name="Atk",
                                   power="3", toughness="3")
        blk = self._make_creature(gs, 1, name="Blk",
                                   power="2", toughness="2")
        cs = declare_attackers(gs, [
            AttackerDeclaration(attacker_card_id=atk.card_id, target=1),
        ])
        declare_blockers(gs, [
            BlockerAssignment(attacker_card_id=atk.card_id,
                              blocker_card_ids=[blk.card_id]),
        ], combat_state=cs)
        with patch(
            "api.engine.pillar_f.v0_2.cards.triggered.framework."
            "fire_event_triggers",
        ) as mock_fire:
            deal_combat_damage(gs, cs, is_first_strike=False)
        cdd_calls = [
            call.args[1] for call in mock_fire.call_args_list
            if isinstance(call.args[1], CombatDamageDealtEvent)
        ]
        # Expect at least 2 events: attacker -> blocker, blocker -> attacker.
        self.assertGreaterEqual(len(cdd_calls), 2)
        sources = {e.source_card_id for e in cdd_calls}
        self.assertIn(atk.card_id, sources)
        self.assertIn(blk.card_id, sources)

    def test_no_event_emitted_when_damage_zero(self) -> None:
        gs = self._empty_4p_game()
        # 0-power attacker (does no damage; should not emit event).
        atk = self._make_creature(gs, 0, name="Wimp",
                                   power="0", toughness="2")
        cs = declare_attackers(gs, [
            AttackerDeclaration(attacker_card_id=atk.card_id, target=1),
        ])
        declare_blockers(gs, [], combat_state=cs)
        with patch(
            "api.engine.pillar_f.v0_2.cards.triggered.framework."
            "fire_event_triggers",
        ) as mock_fire:
            deal_combat_damage(gs, cs, is_first_strike=False)
        cdd_calls = [
            call for call in mock_fire.call_args_list
            if isinstance(call.args[1], CombatDamageDealtEvent)
        ]
        self.assertEqual(len(cdd_calls), 0)


if __name__ == "__main__":
    unittest.main()
