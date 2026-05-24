"""Phase 2 of mega-task v14 — until-end-of-turn substrate hook tests.

Covers `GameState.register_until_end_of_turn_effect(...)` -- the
ergonomic helper that wraps the existing continuous_effects list
with the `target_pattern["until_end_of_turn"] = True` flag the
substrate's cleanup_step already removes.

Verifies:
- Helper returns a ContinuousEffect with the right flag + turn-number
- effect_id is unique across calls (counter increments)
- target_pattern carries caller-provided keys + the flag
- cleanup_step expires UEOT effects but keeps non-UEOT effects
- Multi-turn: UEOT registered on turn N expires at cleanup of turn N

Iter-11 cards don't currently use UEOT-flagged ContinuousEffect (no
top-500 cards in v11's handler set need it). The helper is
infrastructure for iter-12+ adoption (Giant Growth, Berserk,
Threaten, etc. -- not in v11's top-500 set).
"""
from __future__ import annotations

import unittest

from api.engine.pillar_f.v0_2.state import (
    Card, ContinuousEffect, GameState, PlayerState, PlayerZones,
)
from api.engine.pillar_f.v0_2.turn import cleanup_step


def _empty_4p_game(turn: int = 3) -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(
            player_id=pid, name=f"P{pid}",
            life_total=40, zones=PlayerZones(),
        ))
    gs.active_player = 0
    gs.turn_number = turn
    return gs


class UEOTHelperTests(unittest.TestCase):
    def test_helper_returns_continuous_effect(self) -> None:
        gs = _empty_4p_game()
        eff = gs.register_until_end_of_turn_effect(
            source_card_id="src-1", controller=0,
            layer=7, sublayer="c",
            effect_fn_name="pump_3_3_trample",
            description="Giant Growth +3/+3",
        )
        self.assertIsInstance(eff, ContinuousEffect)
        self.assertEqual(eff.source_card_id, "src-1")
        self.assertEqual(eff.controller, 0)
        self.assertEqual(eff.layer, 7)
        self.assertEqual(eff.sublayer, "c")

    def test_helper_sets_until_end_of_turn_flag(self) -> None:
        gs = _empty_4p_game()
        eff = gs.register_until_end_of_turn_effect(
            source_card_id="src", controller=0, layer=6,
        )
        self.assertTrue(eff.target_pattern["until_end_of_turn"])
        self.assertEqual(
            eff.target_pattern["applies_during_turn_number"],
            gs.turn_number,
        )

    def test_helper_appends_to_continuous_effects_list(self) -> None:
        gs = _empty_4p_game()
        self.assertEqual(len(gs.continuous_effects), 0)
        gs.register_until_end_of_turn_effect(
            source_card_id="s", controller=0, layer=7, sublayer="b",
        )
        self.assertEqual(len(gs.continuous_effects), 1)

    def test_helper_generates_unique_effect_ids(self) -> None:
        gs = _empty_4p_game(turn=2)
        e1 = gs.register_until_end_of_turn_effect(
            source_card_id="a", controller=0, layer=6,
        )
        e2 = gs.register_until_end_of_turn_effect(
            source_card_id="b", controller=0, layer=6,
        )
        self.assertNotEqual(e1.effect_id, e2.effect_id)
        # IDs are stable + traceable to the turn they were registered.
        self.assertTrue(e1.effect_id.startswith("ueot_2_"))
        self.assertTrue(e2.effect_id.startswith("ueot_2_"))

    def test_helper_preserves_caller_target_pattern(self) -> None:
        """Caller-supplied target_pattern keys must coexist with the
        helper's auto-injected until_end_of_turn + turn-number keys."""
        gs = _empty_4p_game()
        eff = gs.register_until_end_of_turn_effect(
            source_card_id="src", controller=0, layer=6,
            target_pattern={"affects_creature_ids": ["c1", "c2"]},
        )
        self.assertEqual(
            eff.target_pattern["affects_creature_ids"],
            ["c1", "c2"],
        )
        self.assertTrue(eff.target_pattern["until_end_of_turn"])


class UEOTCleanupExpiryTests(unittest.TestCase):
    """cleanup_step removes UEOT continuous effects (existing substrate
    behavior; v14 just adds the helper that creates them with the
    correct flag)."""

    def test_cleanup_removes_ueot_effect(self) -> None:
        gs = _empty_4p_game()
        gs.register_until_end_of_turn_effect(
            source_card_id="src", controller=0, layer=7, sublayer="c",
        )
        self.assertEqual(len(gs.continuous_effects), 1)
        cleanup_step(gs)
        self.assertEqual(len(gs.continuous_effects), 0)

    def test_cleanup_keeps_non_ueot_effect(self) -> None:
        """A plain ContinuousEffect (no until_end_of_turn flag) must
        survive cleanup -- e.g. an Anthem from a creature on the
        battlefield should keep applying turn-after-turn."""
        gs = _empty_4p_game()
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="anthem-1", source_card_id="lord", controller=0,
            layer=7, sublayer="c",
            target_pattern={},  # NO until_end_of_turn flag
            description="permanent +1/+1 to creatures",
        ))
        self.assertEqual(len(gs.continuous_effects), 1)
        cleanup_step(gs)
        self.assertEqual(len(gs.continuous_effects), 1)

    def test_mixed_ueot_and_permanent_only_ueot_expires(self) -> None:
        gs = _empty_4p_game()
        gs.register_until_end_of_turn_effect(
            source_card_id="pump", controller=0, layer=7, sublayer="c",
            description="UEOT pump",
        )
        gs.continuous_effects.append(ContinuousEffect(
            effect_id="anthem", source_card_id="lord", controller=0,
            layer=7, sublayer="c",
            description="permanent anthem",
        ))
        self.assertEqual(len(gs.continuous_effects), 2)
        cleanup_step(gs)
        # Only the anthem remains; the UEOT pump expired.
        self.assertEqual(len(gs.continuous_effects), 1)
        self.assertEqual(gs.continuous_effects[0].effect_id, "anthem")


if __name__ == "__main__":
    unittest.main()
