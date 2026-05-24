"""Phase 8 of mega-task v10 — cost guardrails + cheap-fallback responder.

Coverage per kickoff Phase 8 gates:
- cost-tracker accumulates correctly across calls (Phase 3 unit
  tests cover this — Phase 8 adds end-to-end behavioral coverage).
- per-turn ceiling triggers fallback during a live responder loop
  (priority responder consults cost_tracker.is_player_in_fallback).
- per-game ceiling halts game (responder returns None and never
  calls LLM after halt).
- fallback responder returns legal actions only (only pass_priority
  is always legal — verified across all 4 player seats).
- Cost flows through BOTH priority responder AND mulligan decider
  (cross-component shared CostTracker).
- Turn rollover clears per-turn fallback (so a temporary spike on
  T3 doesn't muzzle the player for the rest of the game).
"""
from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, Step, Phase,
)
from api.engine.pillar_f.v0_2.policy import (
    make_llm_priority_responder, cheap_fallback_responder,
    make_llm_mulligan_decider, make_llm_bottom_picker,
)
from api.engine.pillar_f.v0_2.policy.cost import (
    CostTracker, DEFAULT_PER_TURN_CEILING_USD, DEFAULT_PER_GAME_CEILING_USD,
)


# ============================================================
# MockLLMClient
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
    responses: List[MockCallResult] = field(default_factory=list)
    calls: List[Dict[str, Any]] = field(default_factory=list)
    available: bool = True

    def is_available(self) -> bool:
        return self.available

    def call_with_budget(self, *, system, user, max_input_tokens,
                         max_output_tokens, **kwargs) -> MockCallResult:
        self.calls.append({
            "system_len": len(system), "user_len": len(user),
        })
        if self.responses:
            return self.responses.pop(0)
        return MockCallResult(
            ok=False, text="", error_code="no_response_queued",
            error_message="MockLLMClient ran out of canned responses.",
        )


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(player_id=pid, name=f"P{pid}",
                                      life_total=40, zones=PlayerZones()))
    gs.active_player = 0
    gs.step = Step.MAIN_1
    gs.phase = Phase.PRECOMBAT_MAIN
    gs.turn_number = 1
    return gs


def _add_castable_instant(gs: GameState, player_id: int, *,
                          name: str = "Bolt") -> Card:
    """Add an iter10-annotated instant to player's hand so eligible-
    actions has more than just pass_priority."""
    c = Card(name=name, owner=player_id, controller=player_id,
             type_line="Instant", mana_cost="{R}")
    c.iter10_annotation = {
        "description": "deals 3 damage",
        "payment": {"resolver": "deal_damage_to_player", "amount": 3},
        "default_targets": [(player_id + 1) % 4],
    }
    gs.add_card(c)
    gs.players[player_id].zones.hand.append(c.card_id)
    return c


# ============================================================
# Defaults visibility test
# ============================================================


class CostDefaultsTests(unittest.TestCase):
    def test_default_per_turn_ceiling_is_0_30(self) -> None:
        self.assertEqual(DEFAULT_PER_TURN_CEILING_USD, 0.30)

    def test_default_per_game_ceiling_is_10(self) -> None:
        self.assertEqual(DEFAULT_PER_GAME_CEILING_USD, 10.0)

    def test_fresh_cost_tracker_uses_defaults(self) -> None:
        ct = CostTracker()
        self.assertEqual(ct.per_turn_ceiling_usd,
                         DEFAULT_PER_TURN_CEILING_USD)
        self.assertEqual(ct.per_game_ceiling_usd,
                         DEFAULT_PER_GAME_CEILING_USD)


# ============================================================
# Cheap-fallback responder behavior
# ============================================================


class CheapFallbackResponderTests(unittest.TestCase):
    def test_cheap_fallback_returns_none_for_all_seats(self) -> None:
        gs = _empty_4p_game()
        for pid in range(4):
            self.assertIsNone(cheap_fallback_responder(gs, pid))

    def test_cheap_fallback_unaffected_by_eligible_actions(self) -> None:
        """Even with castable instants in hand, fallback responder
        returns None (pass). pass_priority is always a legal action,
        so this maintains game legality regardless of state."""
        gs = _empty_4p_game()
        _add_castable_instant(gs, 1)
        _add_castable_instant(gs, 2)
        self.assertIsNone(cheap_fallback_responder(gs, 1))
        self.assertIsNone(cheap_fallback_responder(gs, 2))


# ============================================================
# LLM responder honors cost guardrails end-to-end
# ============================================================


class ResponderCostGuardrailsTests(unittest.TestCase):
    def test_responder_skips_llm_when_player_in_fallback(self) -> None:
        gs = _empty_4p_game()
        _add_castable_instant(gs, 0, name="Bolt0")
        mock = MockLLMClient()
        ct = CostTracker(per_turn_ceiling_usd=0.10)
        # Manually pre-flip fallback for P0 on T1.
        ct.fallback_until_turn_end[0] = gs.turn_number
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        result = responder(gs, player_id=0)
        # Responder short-circuits to None — no LLM call.
        self.assertIsNone(result)
        self.assertEqual(len(mock.calls), 0)

    def test_responder_skips_llm_when_game_halted(self) -> None:
        gs = _empty_4p_game()
        _add_castable_instant(gs, 0)
        mock = MockLLMClient()
        ct = CostTracker(per_game_ceiling_usd=0.05)
        ct.game_halted_for_cost = True
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        result = responder(gs, player_id=0)
        self.assertIsNone(result)
        self.assertEqual(len(mock.calls), 0)

    def test_responder_trips_per_turn_ceiling_mid_loop(self) -> None:
        """First responder call records a cost that exceeds per-turn
        ceiling. Second call (same turn) finds player in fallback and
        skips LLM."""
        gs = _empty_4p_game()
        _add_castable_instant(gs, 0)
        ct = CostTracker(per_turn_ceiling_usd=0.05)
        # First response: legal action with cost above the ceiling.
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text=('{"action_type": "pass_priority", "action_index": 0, '
                  '"rationale": "hold"}'),
            cost_usd=0.10,
        )])
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        responder(gs, player_id=0)
        # After first call, P0 is in fallback for this turn.
        self.assertTrue(ct.is_player_in_fallback(0, gs.turn_number))
        # Second call same turn — no LLM call.
        result = responder(gs, player_id=0)
        self.assertIsNone(result)
        self.assertEqual(len(mock.calls), 1)

    def test_responder_trips_per_game_ceiling_halts(self) -> None:
        gs = _empty_4p_game()
        _add_castable_instant(gs, 0)
        ct = CostTracker(per_game_ceiling_usd=0.05)
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text='{"action_type": "pass_priority", "action_index": 0}',
            cost_usd=0.10,
        )])
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        responder(gs, player_id=0)
        self.assertTrue(ct.game_halted_for_cost)
        # All subsequent calls — any player, any turn — are skipped.
        for pid in range(4):
            self.assertIsNone(responder(gs, player_id=pid))

    def test_turn_rollover_clears_per_turn_fallback(self) -> None:
        """A player flagged for fallback on T3 should be able to
        ask LLM again on T4 (per-turn budget is per-turn)."""
        gs = _empty_4p_game()
        _add_castable_instant(gs, 0)
        ct = CostTracker(per_turn_ceiling_usd=0.05)
        # Set fallback on T3.
        ct.fallback_until_turn_end[0] = 3
        # T4 rollover — engine calls reset_fallbacks_for_turn(4).
        ct.reset_fallbacks_for_turn(4)
        # P0 is no longer in fallback at T4.
        self.assertFalse(ct.is_player_in_fallback(0, 4))
        # Verify responder calls LLM normally on T4.
        gs.turn_number = 4
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text='{"action_type": "pass_priority", "action_index": 0}',
            cost_usd=0.001,
        )])
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        responder(gs, player_id=0)
        self.assertEqual(len(mock.calls), 1)


# ============================================================
# Cross-component: cost flows through priority + mulligan
# ============================================================


class CrossComponentCostFlowTests(unittest.TestCase):
    def test_mulligan_and_priority_share_cost_tracker(self) -> None:
        """A single CostTracker must accumulate spend across both
        mulligan_decider AND priority_responder calls. The per-game
        ceiling fires regardless of which prompt drove the spend."""
        gs = _empty_4p_game()
        _add_castable_instant(gs, 0)
        ct = CostTracker(per_game_ceiling_usd=0.05)
        mock = MockLLMClient(responses=[
            # Mulligan call costs $0.03.
            MockCallResult(ok=True, text='{"keep": true}', cost_usd=0.03),
            # Priority call costs $0.04 → total $0.07 > $0.05 ceiling.
            MockCallResult(
                ok=True,
                text='{"action_type": "pass_priority", "action_index": 0}',
                cost_usd=0.04,
            ),
        ])
        decider = make_llm_mulligan_decider(
            llm_client=mock, cost_tracker=ct,
        )
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=ct,
        )
        # P0 mulligan call.
        decider(gs, player_id=0, current_hand=[], num_mulligans=0)
        self.assertFalse(ct.game_halted_for_cost)  # under ceiling so far
        self.assertAlmostEqual(ct.total_spend(), 0.03, places=5)
        # P0 priority call — should push us over.
        responder(gs, player_id=0)
        self.assertTrue(ct.game_halted_for_cost)
        self.assertAlmostEqual(ct.total_spend(), 0.07, places=5)


# ============================================================
# Cost events / observability
# ============================================================


class CostEventsTests(unittest.TestCase):
    def test_per_turn_ceiling_emits_event(self) -> None:
        ct = CostTracker(per_turn_ceiling_usd=0.05)
        ct.record_call(player_id=0, turn_number=1, cost_usd=0.10,
                       purpose="main_phase_priority")
        # Find the COST_CEILING_HIT event.
        ceiling_events = [
            e for e in ct.events if e.get("event") == "COST_CEILING_HIT"
        ]
        self.assertEqual(len(ceiling_events), 1)
        self.assertEqual(ceiling_events[0]["player_id"], 0)
        self.assertEqual(ceiling_events[0]["turn_number"], 1)

    def test_per_game_ceiling_emits_event(self) -> None:
        ct = CostTracker(per_game_ceiling_usd=0.05)
        ct.record_call(player_id=0, turn_number=1, cost_usd=0.10,
                       purpose="response_window")
        halt_events = [
            e for e in ct.events
            if e.get("event") == "GAME_COST_CEILING_EXCEEDED"
        ]
        self.assertEqual(len(halt_events), 1)

    def test_purpose_recorded_per_call(self) -> None:
        ct = CostTracker()
        ct.record_call(player_id=0, turn_number=1, cost_usd=0.01,
                       purpose="mulligan_decider")
        ct.record_call(player_id=0, turn_number=1, cost_usd=0.02,
                       purpose="main_phase_priority")
        purposes = [e.get("purpose") for e in ct.events if "purpose" in e]
        self.assertIn("mulligan_decider", purposes)
        self.assertIn("main_phase_priority", purposes)


if __name__ == "__main__":
    unittest.main()
