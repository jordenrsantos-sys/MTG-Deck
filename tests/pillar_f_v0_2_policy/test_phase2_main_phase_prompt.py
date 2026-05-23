"""Phase 2 of mega-task v10 — main-phase prompt + parser + validator tests.

Coverage per kickoff Phase 2 gates:
- Prompt assembles correctly given fixture state + politics context.
- Parser handles malformed JSON gracefully.
- Validator catches illegal action_index (out of range).
- Integration: build prompt → mock LLM returns legal action → parser passes.
- Cost-measurement smoke deferred to Phase 3 (real LLM client).
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List

from api.engine.pillar_f.v0_2.policy.prompts import (
    build_main_phase_prompt,
    compute_eligible_actions_passes_only,
    MAIN_PHASE_SYSTEM_PROMPT,
)
from api.engine.pillar_f.v0_2.policy.parsers import (
    parse_action_response,
    ActionResponse,
    fallback_pass_response,
    validate_eligible_action_present,
)


def _basic_actions() -> List[Dict[str, Any]]:
    return [
        {"action_type": "pass_priority", "description": "Pass priority."},
        {"action_type": "play_land", "card_id": "swamp-1",
         "description": "Play land: Swamp"},
        {"action_type": "cast_spell", "card_id": "bolt-1",
         "payment": {"resolver": "deal_damage_to_player", "amount": 3},
         "targets": [2], "description": "Cast Lightning Bolt at P2"},
    ]


def _politics_context() -> Dict[str, Any]:
    return {
        "threats": {
            1: {"score": 0.5, "board_strength": 8.0, "tempo": 3.0,
                "life_pressure": 0.7, "recent_aggression": 1.0},
            2: {"score": 0.8, "board_strength": 14.0, "tempo": 5.0,
                "life_pressure": 0.4, "recent_aggression": 3.0},
            3: {"score": 0.3, "board_strength": 4.0, "tempo": 2.0,
                "life_pressure": 0.9, "recent_aggression": 0.0},
        },
        "alliances": {1: "neutral", 2: "rival", 3: "ally"},
        "deals": [
            {"opponent_player_id": 3, "deal_type": "no-attack",
             "agreed_turn": 5, "kept": True},
        ],
    }


class PromptAssemblyTests(unittest.TestCase):
    def test_prompt_includes_compact_view_text(self) -> None:
        compact = "== TURN 5 | phase=combat ==\nP0 life=40 hand=4"
        prompt = build_main_phase_prompt(compact, _basic_actions())
        self.assertIn("== TURN 5", prompt)
        self.assertIn("ELIGIBLE ACTIONS", prompt)

    def test_prompt_includes_indexed_eligible_actions(self) -> None:
        prompt = build_main_phase_prompt("state", _basic_actions())
        self.assertIn("[0] Pass priority.", prompt)
        self.assertIn("[1] Play land: Swamp", prompt)
        self.assertIn("[2] Cast Lightning Bolt at P2", prompt)

    def test_prompt_includes_politics_context_when_provided(self) -> None:
        prompt = build_main_phase_prompt(
            "state", _basic_actions(),
            politics_context=_politics_context(),
        )
        self.assertIn("threat_score=0.80", prompt)  # P2
        self.assertIn("P3=ally", prompt)
        self.assertIn("P2=rival", prompt)

    def test_prompt_skips_politics_block_when_not_provided(self) -> None:
        prompt = build_main_phase_prompt("state", _basic_actions())
        self.assertNotIn("POLITICS CONTEXT", prompt)

    def test_prompt_includes_deck_archetype_hint(self) -> None:
        prompt = build_main_phase_prompt(
            "state", _basic_actions(),
            deck_archetype_hint="vampire-tribal-aristocrats",
        )
        self.assertIn("vampire-tribal-aristocrats", prompt)

    def test_prompt_includes_rationale_history(self) -> None:
        prompt = build_main_phase_prompt(
            "state", _basic_actions(),
            rationale_history=["Held mana for counterspell",
                               "Played Sol Ring T1"],
        )
        self.assertIn("Held mana for counterspell", prompt)
        self.assertIn("Played Sol Ring T1", prompt)

    def test_prompt_includes_error_on_reprompt(self) -> None:
        prompt = build_main_phase_prompt(
            "state", _basic_actions(),
            last_error_message="action_index 7 out of range",
        )
        self.assertIn("PRIOR RESPONSE FAILED VALIDATION", prompt)
        self.assertIn("action_index 7 out of range", prompt)

    def test_system_prompt_constant_includes_json_contract(self) -> None:
        self.assertIn("VALID JSON ONLY", MAIN_PHASE_SYSTEM_PROMPT)
        self.assertIn("action_index", MAIN_PHASE_SYSTEM_PROMPT)


class ParserHappyPathTests(unittest.TestCase):
    def test_parse_clean_json(self) -> None:
        actions = _basic_actions()
        raw = '{"action_type": "cast_spell", "action_index": 2, "rationale": "Burn P2."}'
        resp, err = parse_action_response(raw, actions)
        self.assertIsNone(err)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.action_type, "cast_spell")
        self.assertEqual(resp.action_index, 2)
        self.assertEqual(resp.card_id, "bolt-1")
        self.assertEqual(resp.targets, [2])
        self.assertEqual(resp.rationale, "Burn P2.")

    def test_parse_with_markdown_fences(self) -> None:
        actions = _basic_actions()
        raw = '```json\n{"action_index": 0, "rationale": "Hold up mana"}\n```'
        resp, err = parse_action_response(raw, actions)
        self.assertIsNone(err)
        self.assertEqual(resp.action_index, 0)

    def test_parse_with_trailing_prose(self) -> None:
        actions = _basic_actions()
        raw = ('Here is my action:\n'
               '{"action_index": 1, "rationale": "Develop mana"}\n'
               'Hope this works.')
        resp, err = parse_action_response(raw, actions)
        self.assertIsNone(err)
        self.assertEqual(resp.action_index, 1)


class ParserErrorHandlingTests(unittest.TestCase):
    def test_empty_text_returns_error(self) -> None:
        resp, err = parse_action_response("", _basic_actions())
        self.assertIsNone(resp)
        self.assertIn("Empty", err)

    def test_no_json_object_returns_error(self) -> None:
        resp, err = parse_action_response(
            "I think I should pass priority here.", _basic_actions(),
        )
        self.assertIsNone(resp)
        self.assertIn("Could not find JSON", err)

    def test_malformed_json_returns_error(self) -> None:
        resp, err = parse_action_response(
            '{"action_index": 0, "rationale": missing-quote}',
            _basic_actions(),
        )
        self.assertIsNone(resp)
        self.assertIn("JSON parse error", err)

    def test_missing_action_index_returns_error(self) -> None:
        resp, err = parse_action_response(
            '{"action_type": "pass_priority", "rationale": "hold"}',
            _basic_actions(),
        )
        self.assertIsNone(resp)
        self.assertIn("missing required key", err)

    def test_action_index_out_of_range_returns_error(self) -> None:
        resp, err = parse_action_response(
            '{"action_index": 99, "rationale": "hello"}',
            _basic_actions(),
        )
        self.assertIsNone(resp)
        self.assertIn("out of range", err)

    def test_negative_action_index_returns_error(self) -> None:
        resp, err = parse_action_response(
            '{"action_index": -1, "rationale": "x"}',
            _basic_actions(),
        )
        self.assertIsNone(resp)
        self.assertIn("out of range", err)

    def test_non_int_action_index_returns_error(self) -> None:
        resp, err = parse_action_response(
            '{"action_index": "abc", "rationale": "x"}',
            _basic_actions(),
        )
        self.assertIsNone(resp)
        self.assertIn("must be int", err)

    def test_alternate_key_names_accepted(self) -> None:
        # LLM may emit "index" or "action_idx" instead of "action_index".
        for key in ("index", "action_idx"):
            raw = f'{{"{key}": 1, "rationale": "ok"}}'
            resp, err = parse_action_response(raw, _basic_actions())
            self.assertIsNone(err, f"key {key!r} should be accepted")
            self.assertEqual(resp.action_index, 1)


class FallbackTests(unittest.TestCase):
    def test_fallback_pass_response_returns_pass_action(self) -> None:
        resp = fallback_pass_response(_basic_actions())
        self.assertIsNotNone(resp)
        self.assertEqual(resp.action_type, "pass_priority")

    def test_fallback_returns_none_when_no_pass_in_eligible(self) -> None:
        actions = [
            {"action_type": "cast_spell", "card_id": "x",
             "description": "Cast X"},
        ]
        resp = fallback_pass_response(actions)
        self.assertIsNone(resp)


class ValidationHelperTests(unittest.TestCase):
    def test_validate_empty_actions_fails(self) -> None:
        ok, err = validate_eligible_action_present([])
        self.assertFalse(ok)
        self.assertIn("Empty", err)

    def test_validate_actions_missing_action_type_key_fails(self) -> None:
        ok, err = validate_eligible_action_present([{"card_id": "x"}])
        self.assertFalse(ok)
        self.assertIn("action_type", err)

    def test_validate_ok_for_minimal_eligible(self) -> None:
        ok, err = validate_eligible_action_present(
            compute_eligible_actions_passes_only()
        )
        self.assertTrue(ok)


class IntegrationTests(unittest.TestCase):
    def test_full_loop_build_prompt_mock_response_parse(self) -> None:
        # Build prompt.
        compact = "== TURN 1 ==\nP0 life=40 hand=7"
        actions = _basic_actions()
        prompt = build_main_phase_prompt(compact, actions)
        self.assertIn("ELIGIBLE ACTIONS", prompt)
        # Simulate LLM returning a legal response.
        mock_response = (
            '{"action_type": "play_land", "action_index": 1, '
            '"rationale": "T1 land drop"}'
        )
        resp, err = parse_action_response(mock_response, actions)
        self.assertIsNone(err)
        # Action is executable.
        self.assertEqual(resp.card_id, "swamp-1")
        self.assertEqual(resp.action_type, "play_land")


if __name__ == "__main__":
    unittest.main()
