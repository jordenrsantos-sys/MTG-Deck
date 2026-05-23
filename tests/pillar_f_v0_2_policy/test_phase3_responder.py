"""Phase 3 of mega-task v10 — LLM priority responder unit tests.

Coverage per kickoff Phase 3 gates:
- compute_eligible_actions returns pass_priority always.
- Lands in hand on active main phase → play_land actions emitted.
- Cards with iter10_annotation → cast_spell actions emitted.
- LLM responder with mock returns expected push_to_stack kwargs.
- Re-prompt loop on invalid response.
- 3rd-failure fallback to pass.
- Cost tracker records calls correctly.

Live 2-LLM head-to-head smoke deferred to Phase 9 integration test
(saves ~$2 per pytest run; Phase 9 runs once and asserts the gate).
"""
from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, Step, Phase,
)
from api.engine.pillar_f.v0_2.policy import (
    compute_eligible_actions, apply_action,
    make_llm_priority_responder,
    cheap_fallback_responder,
)
from api.engine.pillar_f.v0_2.policy.cost import CostTracker


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(player_id=pid, name=f"P{pid}",
                                      life_total=40, zones=PlayerZones()))
    gs.active_player = 0
    gs.step = Step.MAIN_1
    gs.phase = Phase.PRECOMBAT_MAIN
    return gs


def _add_card_to_hand(gs: GameState, player_id: int, *, name: str,
                     type_line: str = "Instant",
                     iter10_annotation: Optional[Dict[str, Any]] = None) -> Card:
    c = Card(name=name, owner=player_id, controller=player_id,
             type_line=type_line)
    if iter10_annotation is not None:
        c.iter10_annotation = iter10_annotation
    gs.add_card(c)
    gs.players[player_id].zones.hand.append(c.card_id)
    return c


# ============================================================
# MockLLMClient — programmable for unit tests
# ============================================================


@dataclass
class MockCallResult:
    ok: bool = True
    text: str = ""
    parsed_json: Optional[Any] = None
    input_tokens: int = 100
    output_tokens: int = 50
    cost_usd: float = 0.001
    latency_ms: int = 10
    model: str = "mock"
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    retries: int = 0


@dataclass
class MockLLMClient:
    """Programmable mock that returns canned responses in order. Each
    call_with_budget pops the next response. is_available() always
    returns True."""
    responses: List[MockCallResult] = field(default_factory=list)
    calls: List[Dict[str, Any]] = field(default_factory=list)

    def is_available(self) -> bool:
        return True

    def call_with_budget(self, *, system, user, max_input_tokens,
                         max_output_tokens, **kwargs) -> MockCallResult:
        self.calls.append({
            "system_len": len(system),
            "user_len": len(user),
            "max_input_tokens": max_input_tokens,
            "max_output_tokens": max_output_tokens,
        })
        if self.responses:
            return self.responses.pop(0)
        return MockCallResult(
            ok=False, text="", error_code="no_response_queued",
            error_message="MockLLMClient ran out of canned responses.",
        )


# ============================================================
# Eligible-actions tests
# ============================================================


class EligibleActionsTests(unittest.TestCase):
    def test_pass_priority_always_present(self) -> None:
        gs = _empty_4p_game()
        actions = compute_eligible_actions(gs, 0)
        action_types = [a["action_type"] for a in actions]
        self.assertIn("pass_priority", action_types)

    def test_active_player_main_phase_land_emits_play_land(self) -> None:
        gs = _empty_4p_game()
        land = _add_card_to_hand(gs, 0, name="Swamp",
                                  type_line="Basic Land — Swamp")
        actions = compute_eligible_actions(gs, 0)
        play_lands = [a for a in actions if a["action_type"] == "play_land"]
        self.assertEqual(len(play_lands), 1)
        self.assertEqual(play_lands[0]["card_id"], land.card_id)

    def test_no_play_land_after_lands_played_this_turn(self) -> None:
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 0, name="Swamp", type_line="Basic Land — Swamp")
        gs.players[0].lands_played_this_turn = 1
        actions = compute_eligible_actions(gs, 0)
        self.assertEqual(
            [a for a in actions if a["action_type"] == "play_land"], [])

    def test_no_play_land_on_opponent_turn(self) -> None:
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 1, name="Swamp", type_line="Basic Land — Swamp")
        # P0 is active; P1 has the land. compute for P1.
        actions = compute_eligible_actions(gs, 1)
        self.assertEqual(
            [a for a in actions if a["action_type"] == "play_land"], [])

    def test_no_play_land_with_nonempty_stack(self) -> None:
        from api.engine.pillar_f.v0_2.stack import push_to_stack
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 0, name="Swamp", type_line="Basic Land — Swamp")
        push_to_stack(gs, card_id="x", controller=0)
        actions = compute_eligible_actions(gs, 0)
        self.assertEqual(
            [a for a in actions if a["action_type"] == "play_land"], [])

    def test_card_with_iter10_annotation_emits_cast_spell(self) -> None:
        gs = _empty_4p_game()
        bolt = _add_card_to_hand(
            gs, 0, name="Lightning Bolt", type_line="Instant",
            iter10_annotation={
                "description": "deals 3 damage",
                "payment": {"resolver": "deal_damage_to_player", "amount": 3},
                "default_targets": [1],
            },
        )
        actions = compute_eligible_actions(gs, 0)
        cast_actions = [a for a in actions if a["action_type"] == "cast_spell"]
        self.assertEqual(len(cast_actions), 1)
        self.assertEqual(cast_actions[0]["card_id"], bolt.card_id)
        self.assertEqual(cast_actions[0]["payment"]["resolver"],
                         "deal_damage_to_player")

    def test_unannotated_card_skipped(self) -> None:
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 0, name="Mystery", type_line="Instant")
        actions = compute_eligible_actions(gs, 0)
        cast_actions = [a for a in actions if a["action_type"] == "cast_spell"]
        self.assertEqual(cast_actions, [])

    def test_eliminated_player_only_gets_pass(self) -> None:
        gs = _empty_4p_game()
        gs.players[0].has_lost = True
        _add_card_to_hand(gs, 0, name="Swamp", type_line="Basic Land — Swamp")
        actions = compute_eligible_actions(gs, 0)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["action_type"], "pass_priority")


# ============================================================
# apply_action tests
# ============================================================


class ApplyActionTests(unittest.TestCase):
    def test_apply_play_land_moves_card(self) -> None:
        gs = _empty_4p_game()
        land = _add_card_to_hand(gs, 0, name="Swamp",
                                  type_line="Basic Land — Swamp")
        action = {
            "action_type": "play_land", "card_id": land.card_id,
            "targets": [], "payment": {},
        }
        apply_action(gs, 0, action)
        self.assertNotIn(land.card_id, gs.players[0].zones.hand)
        self.assertIn(land.card_id, gs.players[0].zones.battlefield)
        self.assertEqual(gs.players[0].lands_played_this_turn, 1)

    def test_apply_cast_spell_pushes_to_stack(self) -> None:
        gs = _empty_4p_game()
        bolt = _add_card_to_hand(
            gs, 0, name="Bolt",
            iter10_annotation={
                "payment": {"resolver": "deal_damage_to_player", "amount": 3},
                "default_targets": [1],
            },
        )
        action = {
            "action_type": "cast_spell", "card_id": bolt.card_id,
            "targets": [1],
            "payment": {"resolver": "deal_damage_to_player", "amount": 3},
            "description": "Bolt P1",
        }
        apply_action(gs, 0, action)
        self.assertEqual(len(gs.stack), 1)
        self.assertEqual(gs.stack[0].targets, [1])
        self.assertNotIn(bolt.card_id, gs.players[0].zones.hand)
        # Iter-10 stub: card immediately to graveyard.
        self.assertIn(bolt.card_id, gs.players[0].zones.graveyard)

    def test_apply_pass_priority_no_op(self) -> None:
        gs = _empty_4p_game()
        action = {"action_type": "pass_priority", "card_id": None,
                  "targets": [], "payment": {}}
        # Snapshot state.
        before_hand = list(gs.players[0].zones.hand)
        before_stack = list(gs.stack)
        apply_action(gs, 0, action)
        self.assertEqual(gs.players[0].zones.hand, before_hand)
        self.assertEqual(gs.stack, before_stack)


# ============================================================
# LLM responder tests with MockLLMClient
# ============================================================


class LLMResponderTests(unittest.TestCase):
    def test_responder_passes_when_only_pass_available(self) -> None:
        """When eligible_actions = [pass], responder shouldn't call LLM."""
        gs = _empty_4p_game()  # No cards in hand.
        # Defender (non-active) player gets only pass.
        mock = MockLLMClient(responses=[])
        ct = CostTracker()
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        result = responder(gs, 1)
        self.assertIsNone(result)
        # No LLM calls made.
        self.assertEqual(len(mock.calls), 0)
        self.assertEqual(ct.total_spend(), 0.0)

    def test_responder_calls_llm_when_actions_available(self) -> None:
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 0, name="Swamp", type_line="Basic Land — Swamp")
        # LLM picks the land (index 1).
        mock = MockLLMClient(responses=[
            MockCallResult(ok=True, text='{"action_index": 1, "rationale": "land drop"}',
                            cost_usd=0.025),
        ])
        ct = CostTracker()
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        result = responder(gs, 0)
        # Land got played → no stack push → return None.
        self.assertIsNone(result)
        self.assertEqual(gs.players[0].lands_played_this_turn, 1)
        # Cost tracked.
        self.assertEqual(ct.total_spend(), 0.025)
        self.assertEqual(len(mock.calls), 1)

    def test_responder_reprompts_on_parse_failure(self) -> None:
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 0, name="Swamp", type_line="Basic Land — Swamp")
        mock = MockLLMClient(responses=[
            MockCallResult(ok=True, text='garbage no json',
                            cost_usd=0.025),  # parse fail
            MockCallResult(ok=True, text='{"action_index": 1, "rationale": "ok"}',
                            cost_usd=0.025),  # success
        ])
        ct = CostTracker()
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        responder(gs, 0)
        self.assertEqual(len(mock.calls), 2)
        # Land played on retry.
        self.assertEqual(gs.players[0].lands_played_this_turn, 1)

    def test_responder_falls_back_to_pass_after_3_failures(self) -> None:
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 0, name="Swamp", type_line="Basic Land — Swamp")
        mock = MockLLMClient(responses=[
            MockCallResult(ok=True, text='not json', cost_usd=0.025),
            MockCallResult(ok=True, text='also bad', cost_usd=0.025),
            MockCallResult(ok=True, text='still bad', cost_usd=0.025),
        ])
        ct = CostTracker()
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        result = responder(gs, 0)
        # 3 attempts (1 initial + 2 retries) all fail → fallback pass.
        self.assertEqual(len(mock.calls), 3)
        # No land played.
        self.assertEqual(gs.players[0].lands_played_this_turn, 0)
        self.assertIsNone(result)

    def test_responder_passes_when_llm_unavailable(self) -> None:
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 0, name="Swamp", type_line="Basic Land — Swamp")
        mock = MockLLMClient()
        # Override is_available.
        mock.is_available = lambda: False
        ct = CostTracker()
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        result = responder(gs, 0)
        self.assertIsNone(result)
        self.assertEqual(len(mock.calls), 0)

    def test_responder_records_rationale_history(self) -> None:
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 0, name="Swamp", type_line="Basic Land — Swamp")
        mock = MockLLMClient(responses=[
            MockCallResult(ok=True, text='{"action_index": 1, "rationale": "ramp"}',
                            cost_usd=0.01),
        ])
        ct = CostTracker()
        history: Dict[int, List[str]] = {}
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
            rationale_history_by_player=history,
        )
        responder(gs, 0)
        self.assertIn(0, history)
        self.assertEqual(len(history[0]), 1)
        self.assertIn("ramp", history[0][0])


# ============================================================
# Cost tracker tests
# ============================================================


class CostTrackerTests(unittest.TestCase):
    def test_record_call_accumulates(self) -> None:
        ct = CostTracker()
        ct.record_call(player_id=0, turn_number=1, cost_usd=0.05)
        ct.record_call(player_id=0, turn_number=1, cost_usd=0.07)
        self.assertAlmostEqual(ct.spend_for_player_turn(0, 1), 0.12)
        self.assertAlmostEqual(ct.spend_for_player(0), 0.12)
        self.assertAlmostEqual(ct.total_spend(), 0.12)

    def test_per_turn_ceiling_triggers_fallback(self) -> None:
        ct = CostTracker(per_turn_ceiling_usd=0.10)
        ct.record_call(player_id=0, turn_number=3, cost_usd=0.15)
        self.assertTrue(ct.is_player_in_fallback(0, 3))
        # Different turn — no fallback.
        self.assertFalse(ct.is_player_in_fallback(0, 4))

    def test_per_game_ceiling_halts_game(self) -> None:
        ct = CostTracker(per_game_ceiling_usd=0.20)
        ct.record_call(player_id=0, turn_number=1, cost_usd=0.10)
        ct.record_call(player_id=1, turn_number=1, cost_usd=0.15)
        self.assertTrue(ct.game_halted_for_cost)

    def test_reset_fallbacks_for_turn_clears_old_flags(self) -> None:
        ct = CostTracker(per_turn_ceiling_usd=0.05)
        ct.record_call(player_id=0, turn_number=2, cost_usd=0.10)
        self.assertTrue(ct.is_player_in_fallback(0, 2))
        ct.reset_fallbacks_for_turn(3)
        self.assertFalse(ct.is_player_in_fallback(0, 2))


class CheapFallbackTests(unittest.TestCase):
    def test_cheap_fallback_always_returns_none(self) -> None:
        gs = _empty_4p_game()
        for pid in range(4):
            self.assertIsNone(cheap_fallback_responder(gs, pid))


if __name__ == "__main__":
    unittest.main()
