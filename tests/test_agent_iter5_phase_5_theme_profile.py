"""Iter 5 Phase 5 — B2 structured weighted theme profile tests.

Verifies:
  - _normalize_theme_profile parses well-formed LLM output
  - _normalize_theme_profile re-normalizes weights to sum 1.0
  - _normalize_theme_profile falls back deterministically on bad input
  - _infer_theme_profile_mode picks the right mode from inputs
    (cards_only / hint_led / hybrid / bare_commander)
  - System prompt + user prompt include the theme_profile schema
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_build_deck_v1 import (
    _INTENT_INTERPRETER_SYSTEM_PROMPT,
    _build_intent_interpreter_user_prompt,
    _infer_theme_profile_mode,
    _normalize_theme_profile,
)


class InferModeTests(unittest.TestCase):
    def test_bare_commander_when_both_empty(self) -> None:
        self.assertEqual(_infer_theme_profile_mode([], []), "bare_commander")

    def test_cards_only_when_no_hints(self) -> None:
        self.assertEqual(
            _infer_theme_profile_mode([], ["Edgar Markov", "Vito"]),
            "cards_only",
        )

    def test_hint_led_when_no_cards(self) -> None:
        self.assertEqual(
            _infer_theme_profile_mode(["dragon_tribal"], []),
            "hint_led",
        )

    def test_hybrid_when_both_present(self) -> None:
        self.assertEqual(
            _infer_theme_profile_mode(["aristocrats"], ["Viscera Seer"]),
            "hybrid",
        )


class NormalizeThemeProfileTests(unittest.TestCase):
    def test_well_formed_llm_output_passes_through(self) -> None:
        raw = {
            "primary":   {"theme": "dragon_tribal", "weight": 0.6},
            "secondary": {"theme": "graveyard_recursion", "weight": 0.3},
            "tertiary":  {"theme": "value_engine", "weight": 0.1},
            "mode": "hybrid",
        }
        result = _normalize_theme_profile(raw, theme_hints=["dragons"],
                                          must_include_cards=["Ur-Dragon"])
        self.assertEqual(result["primary"]["theme"], "dragon_tribal")
        self.assertAlmostEqual(result["primary"]["weight"], 0.6, places=3)
        self.assertEqual(result["mode"], "hybrid")

    def test_renormalizes_weights_to_sum_one(self) -> None:
        raw = {
            "primary":   {"theme": "tribal", "weight": 2.0},   # sums = 3.0
            "secondary": {"theme": "value", "weight": 1.0},
            "mode": "hint_led",
        }
        result = _normalize_theme_profile(raw, theme_hints=["tribal"],
                                          must_include_cards=[])
        total = (result["primary"]["weight"]
                 + result["secondary"]["weight"]
                 + result["tertiary"]["weight"])
        self.assertAlmostEqual(total, 1.0, places=2)

    def test_falls_back_when_raw_invalid(self) -> None:
        result = _normalize_theme_profile(None, theme_hints=["control"],
                                          must_include_cards=[])
        self.assertEqual(result["primary"]["theme"], "control")
        self.assertEqual(result["mode"], "hint_led")

    def test_falls_back_when_all_empty(self) -> None:
        result = _normalize_theme_profile(None, theme_hints=[],
                                          must_include_cards=[])
        self.assertEqual(result["mode"], "bare_commander")
        self.assertEqual(result["primary"]["theme"], "default")

    def test_invalid_mode_string_recomputed(self) -> None:
        raw = {
            "primary": {"theme": "x", "weight": 1.0},
            "mode": "nonsense",
        }
        result = _normalize_theme_profile(
            raw, theme_hints=["tribal"], must_include_cards=["Edgar"],
        )
        self.assertEqual(result["mode"], "hybrid")


class PromptSchemaTests(unittest.TestCase):
    def test_system_prompt_mentions_theme_profile(self) -> None:
        self.assertIn("theme_profile", _INTENT_INTERPRETER_SYSTEM_PROMPT.lower())
        self.assertIn("primary", _INTENT_INTERPRETER_SYSTEM_PROMPT.lower())
        self.assertIn("bare-commander", _INTENT_INTERPRETER_SYSTEM_PROMPT.lower())

    def test_user_prompt_output_schema_includes_theme_profile(self) -> None:
        prompt = _build_intent_interpreter_user_prompt(
            commander="Test", bracket="B3",
            theme_hints=["tribal"], must_include_cards=["Card A"],
        )
        self.assertIn("theme_profile", prompt)
        self.assertIn("primary", prompt)
        self.assertIn("secondary", prompt)
        self.assertIn("tertiary", prompt)


if __name__ == "__main__":
    unittest.main()
