"""Phase 6 of mega-task v10 — mulligan + bottom-picker prompts.

Coverage per kickoff Phase 6 gates:
- Mulligan prompt assembles correctly (hand cards, mulligan count,
  archetype hint, re-prompt error).
- Bottom-picker prompt assembles correctly (PUT_ON_BOTTOM count,
  hand card_ids visible for echo).
- Parsers handle expected output shapes (booleans + card_id lists).
- Validators reject: wrong-count bottom array, unknown card_id,
  duplicate card_id, non-bool keep.
- LLM-driven MulliganDeciderFn + BottomPickerFn factories return
  callables compatible with the substrate's mulligan_setup hooks.
- Integration with mulligan_setup: substrate runs the LLM-piloted
  loop end-to-end with mock responses.

Live 4-LLM full mulligan cycle ship-gate deferred to the Phase 6
smoke runner (tools/test_pillar_f_v0_2_policy_mulligan_smoke.py),
invoked once and asserted under $1 per scoping doc.
"""
from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones,
)
from api.engine.pillar_f.v0_2.turn import mulligan_setup
from api.engine.pillar_f.v0_2.policy import (
    make_llm_mulligan_decider, make_llm_bottom_picker,
)
from api.engine.pillar_f.v0_2.policy.cost import CostTracker
from api.engine.pillar_f.v0_2.policy.prompts.mulligan import (
    MULLIGAN_SYSTEM_PROMPT, BOTTOM_PICKER_SYSTEM_PROMPT,
    build_mulligan_prompt, build_bottom_picker_prompt,
)
from api.engine.pillar_f.v0_2.policy.parsers import (
    parse_mulligan_response, parse_bottom_picker_response,
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


def _hand_desc(n: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i in range(n):
        out.append({
            "card_id": f"c-{i}",
            "name": f"Card {i}",
            "type_line": "Instant" if i % 2 else "Sorcery",
            "mana_cost": "{2}{R}",
            "oracle_text": f"Test oracle text card {i}.",
        })
    return out


# ============================================================
# Mulligan prompt assembly tests
# ============================================================


class MulliganPromptAssemblyTests(unittest.TestCase):
    def test_prompt_shows_mulligan_count(self) -> None:
        prompt = build_mulligan_prompt(_hand_desc(7), 2)
        self.assertIn("MULLIGANS TAKEN: 2", prompt)
        self.assertIn("put 2 card(s) on the bottom", prompt)

    def test_zero_mulligans_no_bottom_warning(self) -> None:
        prompt = build_mulligan_prompt(_hand_desc(7), 0)
        self.assertIn("MULLIGANS TAKEN: 0", prompt)
        self.assertNotIn("put 0 card(s) on the bottom", prompt)

    def test_prompt_lists_all_hand_cards(self) -> None:
        prompt = build_mulligan_prompt(_hand_desc(7), 0)
        for i in range(7):
            self.assertIn(f"Card {i}", prompt)

    def test_archetype_hint_included(self) -> None:
        prompt = build_mulligan_prompt(
            _hand_desc(7), 1, deck_archetype_hint="Mono-Red Aggro",
        )
        self.assertIn("Mono-Red Aggro", prompt)

    def test_last_error_in_reprompt(self) -> None:
        prompt = build_mulligan_prompt(
            _hand_desc(7), 0, last_error_message="missing 'keep' key",
        )
        self.assertIn("PRIOR RESPONSE FAILED", prompt)
        self.assertIn("missing 'keep' key", prompt)


# ============================================================
# Bottom-picker prompt assembly tests
# ============================================================


class BottomPickerPromptAssemblyTests(unittest.TestCase):
    def test_prompt_shows_put_on_bottom_count(self) -> None:
        prompt = build_bottom_picker_prompt(_hand_desc(7), 3)
        self.assertIn("PUT_ON_BOTTOM: 3", prompt)
        self.assertIn("7 − 3 = 4", prompt)

    def test_card_ids_visible_in_hand_list(self) -> None:
        prompt = build_bottom_picker_prompt(_hand_desc(7), 2)
        for i in range(7):
            self.assertIn(f"card_id=c-{i}", prompt)


# ============================================================
# Mulligan parser tests
# ============================================================


class MulliganParserTests(unittest.TestCase):
    def test_keep_true(self) -> None:
        parsed, err = parse_mulligan_response(
            '{"keep": true, "rationale": "3 lands + curve"}'
        )
        self.assertIsNone(err)
        self.assertTrue(parsed.keep)

    def test_keep_false(self) -> None:
        parsed, err = parse_mulligan_response(
            '{"keep": false, "rationale": "no lands"}'
        )
        self.assertIsNone(err)
        self.assertFalse(parsed.keep)

    def test_missing_keep_key_rejected(self) -> None:
        parsed, err = parse_mulligan_response('{"rationale": "??"}')
        self.assertIsNone(parsed)
        self.assertIn("keep", err)

    def test_string_keep_coerced(self) -> None:
        parsed, err = parse_mulligan_response('{"keep": "yes"}')
        self.assertIsNone(err)
        self.assertTrue(parsed.keep)
        parsed, err = parse_mulligan_response('{"keep": "no"}')
        self.assertIsNone(err)
        self.assertFalse(parsed.keep)

    def test_non_bool_non_coerce_rejected(self) -> None:
        parsed, err = parse_mulligan_response('{"keep": 42}')
        self.assertIsNone(parsed)
        self.assertIn("must be a boolean", err)

    def test_markdown_fences_stripped(self) -> None:
        parsed, err = parse_mulligan_response(
            '```json\n{"keep": true}\n```'
        )
        self.assertIsNone(err)
        self.assertTrue(parsed.keep)


# ============================================================
# Bottom-picker parser tests
# ============================================================


class BottomPickerParserTests(unittest.TestCase):
    def test_clean_parse(self) -> None:
        parsed, err = parse_bottom_picker_response(
            '{"cards_to_bottom": ["c-5", "c-6"], "rationale": "excess"}',
            hand_card_ids=[f"c-{i}" for i in range(7)],
            n_to_put_on_bottom=2,
        )
        self.assertIsNone(err)
        self.assertEqual(parsed.cards_to_bottom, ["c-5", "c-6"])

    def test_wrong_count_rejected(self) -> None:
        parsed, err = parse_bottom_picker_response(
            '{"cards_to_bottom": ["c-5"]}',
            hand_card_ids=[f"c-{i}" for i in range(7)],
            n_to_put_on_bottom=2,
        )
        self.assertIsNone(parsed)
        self.assertIn("exactly 2", err)

    def test_unknown_card_id_rejected(self) -> None:
        parsed, err = parse_bottom_picker_response(
            '{"cards_to_bottom": ["unknown-1", "c-6"]}',
            hand_card_ids=[f"c-{i}" for i in range(7)],
            n_to_put_on_bottom=2,
        )
        self.assertIsNone(parsed)
        self.assertIn("not in YOUR_HAND", err)

    def test_duplicate_rejected(self) -> None:
        parsed, err = parse_bottom_picker_response(
            '{"cards_to_bottom": ["c-5", "c-5"]}',
            hand_card_ids=[f"c-{i}" for i in range(7)],
            n_to_put_on_bottom=2,
        )
        self.assertIsNone(parsed)
        self.assertIn("duplicated", err)

    def test_missing_key_rejected(self) -> None:
        parsed, err = parse_bottom_picker_response(
            '{"rationale": "no"}',
            hand_card_ids=[f"c-{i}" for i in range(7)],
            n_to_put_on_bottom=2,
        )
        self.assertIsNone(parsed)
        self.assertIn("cards_to_bottom", err)


# ============================================================
# System-prompt constants
# ============================================================


class MulliganSystemPromptTests(unittest.TestCase):
    def test_mulligan_system_prompt_has_json_contract(self) -> None:
        self.assertIn("VALID JSON ONLY", MULLIGAN_SYSTEM_PROMPT)
        self.assertIn("keep", MULLIGAN_SYSTEM_PROMPT)
        self.assertIn("London", MULLIGAN_SYSTEM_PROMPT)

    def test_bottom_picker_system_prompt_has_json_contract(self) -> None:
        self.assertIn("VALID JSON ONLY", BOTTOM_PICKER_SYSTEM_PROMPT)
        self.assertIn("cards_to_bottom", BOTTOM_PICKER_SYSTEM_PROMPT)


# ============================================================
# Factory tests — LLM-driven decider + bottom-picker
# ============================================================


def _seed_game_with_library(num_cards_per_player: int = 30) -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(player_id=pid, name=f"P{pid}",
                                      life_total=40, zones=PlayerZones()))
        for i in range(num_cards_per_player):
            c = Card(name=f"P{pid}-Card{i}", owner=pid, controller=pid,
                     type_line="Instant", mana_cost="{R}",
                     oracle_text=f"Test card {i}.")
            gs.add_card(c)
            gs.players[pid].zones.library.append(c.card_id)
    return gs


class LLMMulliganDeciderFactoryTests(unittest.TestCase):
    def test_decider_inverts_keep_to_mulligan(self) -> None:
        gs = _seed_game_with_library()
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True, text='{"keep": true, "rationale": "fine"}',
        )])
        cost = CostTracker()
        decider = make_llm_mulligan_decider(
            llm_client=mock, cost_tracker=cost,
        )
        # Draw 7 for P0 then ask decider.
        gs.players[0].zones.hand = gs.players[0].zones.library[:7]
        result = decider(gs, 0, list(gs.players[0].zones.hand), 0)
        # LLM said keep=True → substrate gets False (don't mulligan).
        self.assertFalse(result)
        self.assertEqual(len(mock.calls), 1)

    def test_decider_returns_true_on_keep_false(self) -> None:
        gs = _seed_game_with_library()
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True, text='{"keep": false, "rationale": "no lands"}',
        )])
        cost = CostTracker()
        decider = make_llm_mulligan_decider(
            llm_client=mock, cost_tracker=cost,
        )
        gs.players[0].zones.hand = gs.players[0].zones.library[:7]
        result = decider(gs, 0, list(gs.players[0].zones.hand), 0)
        self.assertTrue(result)

    def test_decider_unavailable_llm_keeps(self) -> None:
        gs = _seed_game_with_library()
        mock = MockLLMClient(available=False)
        cost = CostTracker()
        decider = make_llm_mulligan_decider(
            llm_client=mock, cost_tracker=cost,
        )
        gs.players[0].zones.hand = gs.players[0].zones.library[:7]
        result = decider(gs, 0, list(gs.players[0].zones.hand), 0)
        self.assertFalse(result)  # don't mulligan
        self.assertEqual(len(mock.calls), 0)

    def test_decider_records_cost(self) -> None:
        gs = _seed_game_with_library()
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True, text='{"keep": true}', cost_usd=0.003,
        )])
        cost = CostTracker()
        decider = make_llm_mulligan_decider(
            llm_client=mock, cost_tracker=cost,
        )
        gs.players[0].zones.hand = gs.players[0].zones.library[:7]
        decider(gs, 0, list(gs.players[0].zones.hand), 0)
        self.assertAlmostEqual(
            cost.spend_for_player_turn(0, 0), 0.003, places=5,
        )

    def test_decider_falls_back_to_keep_after_reprompts_exhaust(self) -> None:
        gs = _seed_game_with_library()
        # All 3 responses malformed.
        mock = MockLLMClient(responses=[
            MockCallResult(ok=True, text='{"bad": "no keep"}'),
            MockCallResult(ok=True, text='nonsense'),
            MockCallResult(ok=True, text='{"keep": 99}'),
        ])
        cost = CostTracker()
        decider = make_llm_mulligan_decider(
            llm_client=mock, cost_tracker=cost,
        )
        gs.players[0].zones.hand = gs.players[0].zones.library[:7]
        result = decider(gs, 0, list(gs.players[0].zones.hand), 0)
        # Conservative fallback = keep (substrate gets False).
        self.assertFalse(result)
        self.assertEqual(len(mock.calls), 3)


class LLMBottomPickerFactoryTests(unittest.TestCase):
    def test_picker_returns_llm_choice(self) -> None:
        gs = _seed_game_with_library()
        hand = [f"P0-{i}" for i in range(7)]
        # Put cards into hand for P0 explicitly so card lookup works.
        gs.players[0].zones.hand.clear()
        gs.players[0].zones.hand.extend(gs.players[0].zones.library[:7])
        hand_ids = list(gs.players[0].zones.hand)
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text=('{"cards_to_bottom": ["%s", "%s"], "rationale": "weak"}'
                  % (hand_ids[5], hand_ids[6])),
            cost_usd=0.002,
        )])
        cost = CostTracker()
        picker = make_llm_bottom_picker(
            llm_client=mock, cost_tracker=cost,
        )
        chosen = picker(gs, 0, hand_ids, 2)
        self.assertEqual(chosen, [hand_ids[5], hand_ids[6]])

    def test_picker_zero_returns_empty(self) -> None:
        gs = _seed_game_with_library()
        mock = MockLLMClient()
        cost = CostTracker()
        picker = make_llm_bottom_picker(
            llm_client=mock, cost_tracker=cost,
        )
        chosen = picker(gs, 0, ["a", "b", "c"], 0)
        self.assertEqual(chosen, [])
        # No LLM call when n=0.
        self.assertEqual(len(mock.calls), 0)

    def test_picker_falls_back_to_last_n_on_failure(self) -> None:
        gs = _seed_game_with_library()
        gs.players[0].zones.hand.extend(gs.players[0].zones.library[:7])
        hand_ids = list(gs.players[0].zones.hand)
        # All 3 responses malformed.
        mock = MockLLMClient(responses=[
            MockCallResult(ok=True, text='{}'),
            MockCallResult(ok=True, text='nonsense'),
            MockCallResult(ok=True, text='{"cards_to_bottom": [1, 2]}'),
        ])
        cost = CostTracker()
        picker = make_llm_bottom_picker(
            llm_client=mock, cost_tracker=cost,
        )
        chosen = picker(gs, 0, hand_ids, 2)
        # Fallback = last 2 cards.
        self.assertEqual(chosen, hand_ids[-2:])

    def test_picker_unavailable_llm_falls_back(self) -> None:
        gs = _seed_game_with_library()
        mock = MockLLMClient(available=False)
        cost = CostTracker()
        picker = make_llm_bottom_picker(
            llm_client=mock, cost_tracker=cost,
        )
        chosen = picker(gs, 0, ["a", "b", "c", "d"], 2)
        self.assertEqual(chosen, ["c", "d"])
        self.assertEqual(len(mock.calls), 0)


# ============================================================
# Integration: LLM-driven mulligan_setup end-to-end
# ============================================================


class LLMMulliganIntegrationTests(unittest.TestCase):
    def test_full_setup_keeps_all_first_hands(self) -> None:
        """All 4 players keep their opening hand on the first decision."""
        gs = _seed_game_with_library(num_cards_per_player=30)
        # 4 keep responses (one per player).
        mock = MockLLMClient(responses=[
            MockCallResult(ok=True, text='{"keep": true}', cost_usd=0.001)
            for _ in range(4)
        ])
        cost = CostTracker()
        decider = make_llm_mulligan_decider(
            llm_client=mock, cost_tracker=cost,
        )
        picker = make_llm_bottom_picker(
            llm_client=mock, cost_tracker=cost,
        )
        results = mulligan_setup(
            gs, decider_fn=decider, bottom_picker_fn=picker,
        )
        # All players kept after 0 mulligans → 7 cards in hand each, 0 on bottom.
        for pid in range(4):
            self.assertEqual(len(gs.players[pid].zones.hand), 7)
            self.assertEqual(results[pid], 0)
        self.assertEqual(len(mock.calls), 4)

    def test_full_setup_one_mulligan_then_keep_with_bottom(self) -> None:
        """P0 mulligans once, then keeps, then bottoms 1 card."""
        gs = _seed_game_with_library(num_cards_per_player=30)
        # We need to know which card_id will be in hand when the bottom-
        # picker prompt is called. Use a "pick the first card" response
        # but match the actual hand. Use a lambda-style mock: programmable
        # via call sequence. Simpler: provide enough responses for the
        # full flow and use the hand at the time-of-call.
        # We'll inspect the prompts to find a valid card_id to return.
        responses: List[MockCallResult] = []

        # 1. P0 decision @ 0 mulls: mulligan (keep=false)
        responses.append(MockCallResult(
            ok=True, text='{"keep": false, "rationale": "no lands"}',
        ))
        # 2. P0 decision @ 1 mull: keep
        responses.append(MockCallResult(
            ok=True, text='{"keep": true, "rationale": "ok now"}',
        ))
        # 3. P0 bottom-picker: we'll patch this in after we see the hand.
        # Use a sentinel that we'll replace by intercepting the call.

        # Simpler: subclass MockLLMClient to dynamically respond on call 3.
        @dataclass
        class DynamicMock(MockLLMClient):
            def call_with_budget(self, *, system, user, max_input_tokens,
                                 max_output_tokens, **kwargs):
                if "PUT_ON_BOTTOM" in user:
                    # Find the first card_id= entry in the prompt.
                    import re
                    m = re.search(r"card_id=(\S+)", user)
                    if m:
                        cid = m.group(1)
                        self.calls.append({"system": system, "user": user})
                        return MockCallResult(
                            ok=True,
                            text=('{"cards_to_bottom": ["%s"], '
                                  '"rationale": "weakest"}') % cid,
                            cost_usd=0.001,
                        )
                return super().call_with_budget(
                    system=system, user=user,
                    max_input_tokens=max_input_tokens,
                    max_output_tokens=max_output_tokens, **kwargs,
                )

        # P0 will: mull (1), keep (2), bottom (3 — dynamic).
        # P1, P2, P3 each keep immediately (1 call each = 3 more).
        dyn_responses = [
            # P0 mull@0
            MockCallResult(ok=True, text='{"keep": false}', cost_usd=0.001),
            # P0 keep@1
            MockCallResult(ok=True, text='{"keep": true}', cost_usd=0.001),
            # P0 bottom-picker → handled dynamically by DynamicMock
            # P1 keep@0
            MockCallResult(ok=True, text='{"keep": true}', cost_usd=0.001),
            # P2 keep@0
            MockCallResult(ok=True, text='{"keep": true}', cost_usd=0.001),
            # P3 keep@0
            MockCallResult(ok=True, text='{"keep": true}', cost_usd=0.001),
        ]
        mock = DynamicMock(responses=dyn_responses)
        cost = CostTracker()
        decider = make_llm_mulligan_decider(
            llm_client=mock, cost_tracker=cost,
        )
        picker = make_llm_bottom_picker(
            llm_client=mock, cost_tracker=cost,
        )
        results = mulligan_setup(
            gs, decider_fn=decider, bottom_picker_fn=picker,
        )
        # P0: 1 mulligan, hand should be 7 − 1 = 6 cards.
        self.assertEqual(results[0], 1)
        self.assertEqual(len(gs.players[0].zones.hand), 6)
        # P1, P2, P3: 0 mulligans, 7 cards.
        for pid in [1, 2, 3]:
            self.assertEqual(results[pid], 0)
            self.assertEqual(len(gs.players[pid].zones.hand), 7)


if __name__ == "__main__":
    unittest.main()
