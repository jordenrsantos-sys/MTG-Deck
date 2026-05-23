"""Phase 5 of mega-task v10 — response-window prompt + responder routing.

Coverage per kickoff Phase 5 gates:
- Response prompt assembles with stack-top context.
- Parser handles pass + cast-Counterspell-targeting-top (reuses
  parse_action_response since JSON shape is identical to main-phase).
- Responder switches to response_window prompt when state.stack is
  non-empty.
- summarize_stack_top handles StackEntry, dict, and missing inputs.

Counter-war integration test (3-deep chain with 4-player priority
opportunities) deferred to Phase 9 integration test — pricier to run
on every pytest invocation; one live run there asserts the gate.
"""
from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, StackEntry, Step, Phase,
)
from api.engine.pillar_f.v0_2.stack import push_to_stack
from api.engine.pillar_f.v0_2.policy import (
    make_llm_priority_responder,
)
from api.engine.pillar_f.v0_2.policy.cost import CostTracker
from api.engine.pillar_f.v0_2.policy.prompts import (
    RESPONSE_WINDOW_SYSTEM_PROMPT,
    build_response_window_prompt,
    summarize_stack_top,
)
from api.engine.pillar_f.v0_2.policy.parsers import parse_action_response


# ============================================================
# Helpers (mirrors Phase 3 test scaffolding)
# ============================================================


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


def _eligible_pass_and_cast() -> List[Dict[str, Any]]:
    return [
        {"action_type": "pass_priority", "card_id": None,
         "ability_idx": None, "targets": [], "payment": {},
         "description": "Pass priority."},
        {"action_type": "cast_spell", "card_id": "cs-1",
         "ability_idx": None, "targets": ["stack:top"], "payment":
         {"resolver": "counter_target_spell"},
         "description": "Cast Counterspell (counter target spell)"},
    ]


# ============================================================
# MockLLMClient (copied from Phase 3 test scaffolding)
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

    def is_available(self) -> bool:
        return True

    def call_with_budget(self, *, system, user, max_input_tokens,
                         max_output_tokens, **kwargs) -> MockCallResult:
        self.calls.append({
            "system": system, "user": user,
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
# summarize_stack_top tests
# ============================================================


class SummarizeStackTopTests(unittest.TestCase):
    def test_none_returns_empty_marker(self) -> None:
        text = summarize_stack_top(None)
        self.assertIn("stack is empty", text)

    def test_stack_entry_dataclass(self) -> None:
        entry = StackEntry(
            entry_id="s-1", card_id="c-1", controller=0,
            entry_type="spell",
            targets=[1],
            payment={"resolver": "deal_damage_to_player", "amount": 3},
            description="Lightning Bolt",
        )
        text = summarize_stack_top(entry)
        self.assertIn("Controller: P0", text)
        self.assertIn("Type: spell", text)
        self.assertIn("Lightning Bolt", text)
        self.assertIn("Resolver: deal_damage_to_player", text)
        self.assertIn("Targets: 1", text)

    def test_dict_form_works(self) -> None:
        d = {"entry_id": "s-1", "card_id": "c-1", "controller": 2,
             "entry_type": "activated", "targets": [],
             "payment": {"resolver": "draw_cards", "count": 1},
             "description": "Phyrexian Arena trigger"}
        text = summarize_stack_top(d)
        self.assertIn("Controller: P2", text)
        self.assertIn("activated", text)
        self.assertIn("Targets: (none)", text)
        self.assertIn("Resolver: draw_cards", text)

    def test_unparseable_input(self) -> None:
        text = summarize_stack_top(42)
        self.assertIn("unparseable", text)


# ============================================================
# build_response_window_prompt assembly tests
# ============================================================


class ResponseWindowPromptAssemblyTests(unittest.TestCase):
    def test_includes_stack_top_and_eligible_responses(self) -> None:
        stack_top_text = summarize_stack_top(StackEntry(
            entry_id="s-1", card_id="bolt-1", controller=0,
            entry_type="spell", targets=[1],
            payment={"resolver": "deal_damage_to_player", "amount": 3},
            description="Lightning Bolt (3 to P1)",
        ))
        prompt = build_response_window_prompt(
            "compact-state-text", stack_top_text, _eligible_pass_and_cast(),
        )
        self.assertIn("STACK TOP", prompt)
        self.assertIn("Lightning Bolt", prompt)
        self.assertIn("ELIGIBLE RESPONSES", prompt)
        self.assertIn("Pass priority.", prompt)
        self.assertIn("Counterspell", prompt)
        self.assertIn("compact-state-text", prompt)

    def test_no_eligible_responses_shows_none(self) -> None:
        prompt = build_response_window_prompt(
            "compact", "(empty)", [],
        )
        self.assertIn("(none — must pass)", prompt)

    def test_politics_context_included_when_provided(self) -> None:
        politics = {
            "threats": {1: {"score": 0.85}, 2: {"score": 0.20}},
            "alliances": {2: "ally"},
        }
        prompt = build_response_window_prompt(
            "compact", "(stack)", _eligible_pass_and_cast(),
            politics_context=politics,
        )
        self.assertIn("threat_score=0.85", prompt)
        self.assertIn("P2=ally", prompt)

    def test_deck_archetype_hint_included(self) -> None:
        prompt = build_response_window_prompt(
            "compact", "(stack)", _eligible_pass_and_cast(),
            deck_archetype_hint="UB Control",
        )
        self.assertIn("UB Control", prompt)

    def test_last_error_included_on_reprompt(self) -> None:
        prompt = build_response_window_prompt(
            "compact", "(stack)", _eligible_pass_and_cast(),
            last_error_message="action_index 99 out of range",
        )
        self.assertIn("PRIOR RESPONSE FAILED", prompt)
        self.assertIn("action_index 99 out of range", prompt)

    def test_rationale_history_included(self) -> None:
        prompt = build_response_window_prompt(
            "compact", "(stack)", _eligible_pass_and_cast(),
            rationale_history=["T1 pass: held up mana for counter"],
        )
        self.assertIn("RECENT ACTION RATIONALES", prompt)
        self.assertIn("held up mana", prompt)


# ============================================================
# Parser reuse tests — JSON contract matches main-phase
# ============================================================


class ResponseWindowParserReuseTests(unittest.TestCase):
    def test_pass_parses(self) -> None:
        raw = ('{"action_type": "pass_priority", "action_index": 0, '
               '"rationale": "let it resolve"}')
        parsed, err = parse_action_response(raw, _eligible_pass_and_cast())
        self.assertIsNone(err)
        self.assertEqual(parsed.action_type, "pass_priority")
        self.assertEqual(parsed.action_index, 0)

    def test_cast_counterspell_parses(self) -> None:
        raw = ('{"action_type": "cast_spell", "action_index": 1, '
               '"rationale": "counter their threat"}')
        parsed, err = parse_action_response(raw, _eligible_pass_and_cast())
        self.assertIsNone(err)
        self.assertEqual(parsed.action_type, "cast_spell")
        self.assertEqual(parsed.action_index, 1)
        self.assertEqual(parsed.eligible_action["payment"]["resolver"],
                         "counter_target_spell")

    def test_out_of_range_index_rejected(self) -> None:
        raw = ('{"action_type": "cast_spell", "action_index": 99}')
        parsed, err = parse_action_response(raw, _eligible_pass_and_cast())
        self.assertIsNone(parsed)
        self.assertIn("out of range", err)


# ============================================================
# System-prompt constants tests
# ============================================================


class ResponseWindowSystemPromptTests(unittest.TestCase):
    def test_system_prompt_has_json_contract(self) -> None:
        self.assertIn("VALID JSON ONLY", RESPONSE_WINDOW_SYSTEM_PROMPT)
        self.assertIn("action_index", RESPONSE_WINDOW_SYSTEM_PROMPT)

    def test_system_prompt_recommends_default_pass(self) -> None:
        text = RESPONSE_WINDOW_SYSTEM_PROMPT.lower()
        # "Default to pass" is the policy default per scoping doc.
        self.assertIn("default to pass", text)


# ============================================================
# Responder routing tests — does it pick the right prompt?
# ============================================================


class ResponderRoutingTests(unittest.TestCase):
    def test_responder_uses_response_window_when_stack_nonempty(self) -> None:
        gs = _empty_4p_game()
        # Put Counterspell-style annotation on P1 (the responder).
        cs = _add_card_to_hand(
            gs, 1, name="Counterspell", type_line="Instant",
            iter10_annotation={
                "description": "counter target spell",
                "payment": {"resolver": "noop"},
                "default_targets": ["stack:top"],
            },
        )
        # Push a stack object controlled by P0.
        push_to_stack(gs, card_id="some-spell", controller=0,
                      description="Some Spell")
        # Canned LLM response: pick action_index 1 (cast the counter).
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text=('{"action_type": "cast_spell", "action_index": 1, '
                  '"rationale": "counter the threat"}'),
            cost_usd=0.002,
        )])
        cost = CostTracker()
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=cost,
        )
        responder(gs, player_id=1)
        # Exactly one LLM call, and it used the response-window system
        # prompt (not the main-phase one).
        self.assertEqual(len(mock.calls), 1)
        self.assertEqual(mock.calls[0]["system"], RESPONSE_WINDOW_SYSTEM_PROMPT)
        # User prompt contains the STACK TOP section.
        self.assertIn("STACK TOP", mock.calls[0]["user"])
        # Cost recorded for P1 on the current turn.
        self.assertAlmostEqual(
            cost.spend_for_player_turn(1, gs.turn_number), 0.002, places=5,
        )
        # Last event's purpose was "response_window".
        last_cost_event = [
            e for e in cost.events if "purpose" in e
        ][-1]
        self.assertEqual(last_cost_event["purpose"], "response_window")

    def test_responder_uses_main_phase_when_stack_empty(self) -> None:
        from api.engine.pillar_f.v0_2.policy.prompts import (
            MAIN_PHASE_SYSTEM_PROMPT,
        )
        gs = _empty_4p_game()
        _add_card_to_hand(gs, 0, name="Swamp",
                          type_line="Basic Land — Swamp")
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text=('{"action_type": "play_land", "action_index": 1, '
                  '"rationale": "ramp"}'),
            cost_usd=0.001,
        )])
        cost = CostTracker()
        responder = make_llm_priority_responder(
            llm_client=mock, cost_tracker=cost,
        )
        responder(gs, player_id=0)
        self.assertEqual(len(mock.calls), 1)
        self.assertEqual(mock.calls[0]["system"], MAIN_PHASE_SYSTEM_PROMPT)
        # User prompt does NOT contain the STACK TOP section.
        self.assertNotIn("STACK TOP", mock.calls[0]["user"])


if __name__ == "__main__":
    unittest.main()
