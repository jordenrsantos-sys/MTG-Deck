"""Phase 7 of mega-task v10 — threat-vector + politics state tracker.

Coverage per kickoff Phase 7 gates:
- compute_threat_vector returns expected values for fixed-state
  opponents (empty board = low, large board = high, keyword bumps
  applied).
- update_politics_state correctly records combat damage as
  recent_aggression.
- deals history caps at 50 entries (oldest drops).
- alliances enum transitions on triggering events (combat → rival,
  deal_made → ally, deal_broken → snap to rival).
- recent_aggression decays via roll_damage_log_for_turn.
"""
from __future__ import annotations

import unittest
from typing import Any, Dict

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones,
)
from api.engine.pillar_f.v0_2.policy.politics import (
    compute_threat_vector, compute_all_threat_vectors,
    update_politics_state, roll_damage_log_for_turn,
    export_politics_context,
    DEALS_CAP, RECENT_AGGRESSION_WINDOW,
)


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(player_id=pid, name=f"P{pid}",
                                      life_total=40, zones=PlayerZones()))
    gs.active_player = 0
    gs.turn_number = 1
    return gs


def _make_creature(
    gs: GameState, owner: int, *, name: str = "Bear",
    power: str = "2", toughness: str = "2",
    keywords=None, type_line: str = "Creature — Bear",
    cmc: int = 2,
) -> Card:
    c = Card(name=name, owner=owner, controller=owner,
             type_line=type_line, power=power, toughness=toughness,
             keywords=list(keywords or []), cmc=cmc)
    gs.add_card(c)
    gs.players[owner].zones.battlefield.append(c.card_id)
    return c


# ============================================================
# compute_threat_vector tests
# ============================================================


class ComputeThreatVectorTests(unittest.TestCase):
    def test_empty_board_low_threat(self) -> None:
        gs = _empty_4p_game()
        v = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        # No board, full life → only (1 - life_pressure) = 0 → score 0.
        self.assertEqual(v["board_strength"], 0.0)
        self.assertLess(v["score"], 0.15)

    def test_large_board_raises_threat(self) -> None:
        gs = _empty_4p_game()
        # Give P1 four 4/4 creatures (16 power + 16 toughness = 32 total).
        for i in range(4):
            _make_creature(gs, 1, name=f"Giant {i}",
                           power="4", toughness="4")
        v = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        # 32 / 30 = 1.07 → clamped to 1.0.
        self.assertEqual(v["board_strength"], 1.0)
        # Final score above 0.35 (board_strength=1.0 × 0.4 = 0.4 alone).
        self.assertGreater(v["score"], 0.35)

    def test_keyword_lifelink_boosts_threat(self) -> None:
        gs = _empty_4p_game()
        # Plain 4/4 = 8 base.
        _make_creature(gs, 1, name="Plain", power="4", toughness="4")
        v_plain = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        gs = _empty_4p_game()
        # Lifelink 4/4 = 8 × 1.10 = 8.8.
        _make_creature(gs, 1, name="Healer", power="4", toughness="4",
                       keywords=["lifelink"])
        v_lifelink = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        self.assertGreater(
            v_lifelink["board_strength"], v_plain["board_strength"],
        )

    def test_keyword_hexproof_boosts_more(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 1, name="Plain", power="4", toughness="4")
        v_plain = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        gs = _empty_4p_game()
        _make_creature(gs, 1, name="Untouchable", power="4", toughness="4",
                       keywords=["hexproof"])
        v_hex = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        # Hexproof multiplier 1.30 > lifelink 1.10.
        self.assertAlmostEqual(
            v_hex["board_strength"], v_plain["board_strength"] * 1.30,
            places=4,
        )

    def test_low_life_increases_threat(self) -> None:
        gs = _empty_4p_game()
        gs.players[1].life_total = 40  # full life
        v_full = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        gs.players[1].life_total = 5
        v_low = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        # Low life pressure (5/40 = 0.125) → (1 - 0.125) × 0.15 = 0.131
        # high life pressure (40/40 = 1.0) → (1 - 1.0) × 0.15 = 0.0
        self.assertGreater(v_low["score"], v_full["score"])

    def test_full_hand_raises_tempo(self) -> None:
        gs = _empty_4p_game()
        # Add 7 cards to P1's hand.
        for i in range(7):
            c = Card(name=f"Card{i}", owner=1, controller=1)
            gs.add_card(c)
            gs.players[1].zones.hand.append(c.card_id)
        v = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        # 7 cards × 1.5 = 10.5 / 12 = 0.875.
        self.assertGreater(v["tempo"], 0.5)

    def test_recent_aggression_from_damage_log(self) -> None:
        gs = _empty_4p_game()
        # Manually set viewer's politics damage log.
        gs.players[0].politics_state = {
            "damage_log": {1: 10}, "damage_log_turn_window": [],
            "threats": {}, "deals": [], "alliances": {},
        }
        v = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        # 10 / 20 = 0.5.
        self.assertAlmostEqual(v["recent_aggression"], 0.5, places=3)

    def test_archetype_aggro_signal(self) -> None:
        gs = _empty_4p_game()
        # P1 has 3 1-cmc creatures = aggro signal.
        for i in range(3):
            _make_creature(gs, 1, name=f"Goblin {i}", cmc=1,
                           power="1", toughness="1")
        v = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        self.assertGreaterEqual(v["archetype_hint"], 0.4)

    def test_archetype_control_signal_from_instants_in_gy(self) -> None:
        gs = _empty_4p_game()
        for i in range(3):
            c = Card(name=f"Counterspell {i}", owner=1, controller=1,
                     type_line="Instant")
            gs.add_card(c)
            gs.players[1].zones.graveyard.append(c.card_id)
        v = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        self.assertGreaterEqual(v["archetype_hint"], 0.4)

    def test_self_threat_is_zero(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Beast", power="5", toughness="5")
        v = compute_threat_vector(gs, viewer_id=0, opponent_id=0)
        self.assertEqual(v["score"], 0.0)

    def test_eliminated_opponent_is_zero(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 1, name="Beast", power="5", toughness="5")
        gs.players[1].has_lost = True
        v = compute_threat_vector(gs, viewer_id=0, opponent_id=1)
        self.assertEqual(v["score"], 0.0)

    def test_compute_all_skips_viewer_and_dead(self) -> None:
        gs = _empty_4p_game()
        gs.players[2].has_lost = True
        all_v = compute_all_threat_vectors(gs, viewer_id=0)
        self.assertNotIn(0, all_v)
        self.assertNotIn(2, all_v)
        self.assertIn(1, all_v)
        self.assertIn(3, all_v)


# ============================================================
# update_politics_state tests
# ============================================================


class UpdatePoliticsStateTests(unittest.TestCase):
    def test_combat_damage_records_in_damage_log(self) -> None:
        gs = _empty_4p_game()
        update_politics_state(gs, viewer_id=0, event={
            "type": "combat_damage", "from": 1, "amount": 5,
        })
        ps = gs.players[0].politics_state
        self.assertEqual(ps["damage_log"].get(1), 5)

    def test_combat_damage_bumps_alliance_toward_rival(self) -> None:
        gs = _empty_4p_game()
        # Start at default neutral.
        update_politics_state(gs, viewer_id=0, event={
            "type": "combat_damage", "from": 1, "amount": 3,
        })
        ps = gs.players[0].politics_state
        # neutral → rival.
        self.assertEqual(ps["alliances"][1], "rival")

    def test_deal_made_records_and_bumps_alliance_to_ally(self) -> None:
        gs = _empty_4p_game()
        update_politics_state(gs, viewer_id=0, event={
            "type": "deal_made", "with": 2,
            "deal_type": "no_attack_pact", "agreed_turn": 3,
        })
        ps = gs.players[0].politics_state
        self.assertEqual(len(ps["deals"]), 1)
        self.assertEqual(ps["deals"][0]["opponent_player_id"], 2)
        self.assertEqual(ps["deals"][0]["deal_type"], "no_attack_pact")
        self.assertFalse(ps["deals"][0]["kept"])
        # neutral → ally.
        self.assertEqual(ps["alliances"][2], "ally")

    def test_deal_honored_marks_kept_and_keeps_ally(self) -> None:
        gs = _empty_4p_game()
        update_politics_state(gs, viewer_id=0, event={
            "type": "deal_made", "with": 2, "deal_type": "x",
        })
        update_politics_state(gs, viewer_id=0, event={
            "type": "deal_honored", "with": 2, "deal_type": "x",
        })
        ps = gs.players[0].politics_state
        self.assertTrue(ps["deals"][0]["kept"])
        self.assertEqual(ps["alliances"][2], "ally")

    def test_deal_broken_snaps_alliance_to_rival(self) -> None:
        gs = _empty_4p_game()
        update_politics_state(gs, viewer_id=0, event={
            "type": "deal_made", "with": 2, "deal_type": "x",
        })
        # was ally
        update_politics_state(gs, viewer_id=0, event={
            "type": "deal_broken", "with": 2, "deal_type": "x",
        })
        ps = gs.players[0].politics_state
        # deal_broken snaps to rival (not neutral via bump).
        self.assertEqual(ps["alliances"][2], "rival")

    def test_deals_capped_at_50(self) -> None:
        gs = _empty_4p_game()
        for i in range(60):
            update_politics_state(gs, viewer_id=0, event={
                "type": "deal_made", "with": 2,
                "deal_type": f"d{i}", "agreed_turn": i,
            })
        deals = gs.players[0].politics_state["deals"]
        self.assertEqual(len(deals), DEALS_CAP)
        # Oldest dropped: first remaining deal is d10 (60 - 50 = 10 dropped).
        self.assertEqual(deals[0]["deal_type"], "d10")
        self.assertEqual(deals[-1]["deal_type"], "d59")

    def test_threat_recompute_upserts_threats(self) -> None:
        gs = _empty_4p_game()
        update_politics_state(gs, viewer_id=0, event={
            "type": "threat_recompute", "opponent_id": 2,
            "threat_dict": {"score": 0.75, "board_strength": 0.5},
        })
        ps = gs.players[0].politics_state
        self.assertEqual(ps["threats"][2]["score"], 0.75)

    def test_spell_cast_against_logs_minor_aggression(self) -> None:
        gs = _empty_4p_game()
        update_politics_state(gs, viewer_id=0, event={
            "type": "spell_cast_against", "from": 1,
            "spell_card_id": "doomblade",
        })
        ps = gs.players[0].politics_state
        # 1 unit of damage-equivalent logged.
        self.assertEqual(ps["damage_log"].get(1), 1)
        # Alliance NOT bumped (spell-cast is milder than combat damage).
        self.assertEqual(ps["alliances"].get(1, "neutral"), "neutral")


# ============================================================
# Damage decay tests
# ============================================================


class DamageDecayTests(unittest.TestCase):
    def test_old_damage_drops_after_window(self) -> None:
        gs = _empty_4p_game()
        gs.turn_number = 1
        update_politics_state(gs, viewer_id=0, event={
            "type": "combat_damage", "from": 1, "amount": 5,
        })
        # Advance turn to T = 1 + RECENT_AGGRESSION_WINDOW + 1 = 5.
        gs.turn_number = 1 + RECENT_AGGRESSION_WINDOW + 1
        roll_damage_log_for_turn(gs, viewer_id=0, current_turn=gs.turn_number)
        ps = gs.players[0].politics_state
        # T1's 5 damage is now outside the window.
        self.assertEqual(ps["damage_log"].get(1, 0), 0)

    def test_damage_inside_window_retained(self) -> None:
        gs = _empty_4p_game()
        gs.turn_number = 3
        update_politics_state(gs, viewer_id=0, event={
            "type": "combat_damage", "from": 1, "amount": 4,
        })
        # Still inside window at T4 (3-turn window = turns [2, 3, 4]).
        gs.turn_number = 4
        roll_damage_log_for_turn(gs, viewer_id=0, current_turn=gs.turn_number)
        ps = gs.players[0].politics_state
        self.assertEqual(ps["damage_log"].get(1), 4)


# ============================================================
# export_politics_context tests (LLM-facing contract)
# ============================================================


class ExportPoliticsContextTests(unittest.TestCase):
    def test_returns_threats_alliances_deals(self) -> None:
        gs = _empty_4p_game()
        # Seed a couple of events.
        update_politics_state(gs, viewer_id=0, event={
            "type": "combat_damage", "from": 1, "amount": 3,
        })
        update_politics_state(gs, viewer_id=0, event={
            "type": "deal_made", "with": 2, "deal_type": "x",
        })
        ctx = export_politics_context(gs, viewer_id=0)
        self.assertIn("threats", ctx)
        self.assertIn("alliances", ctx)
        self.assertIn("deals", ctx)
        self.assertEqual(ctx["alliances"][1], "rival")
        self.assertEqual(ctx["alliances"][2], "ally")
        self.assertEqual(len(ctx["deals"]), 1)

    def test_empty_for_unknown_viewer(self) -> None:
        gs = _empty_4p_game()
        ctx = export_politics_context(gs, viewer_id=99)
        self.assertEqual(ctx, {})


if __name__ == "__main__":
    unittest.main()
