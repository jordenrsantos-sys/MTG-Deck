"""Iter 5 Phase 6 — theme profile cascade through C2.1 / C2.2 / D2.

Verifies:
  - _render_theme_profile_block renders a complete USER THEME PROFILE
    block when given a profile
  - Returns empty string for None / non-dict input (backwards-compat)
  - Block mentions all three slots with weights
  - Block contains the "honor user themes" guidance
  - C2.1 / C2.2 / D2 user prompts include the rendered block when an
    intent_analysis with theme_profile is passed
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_build_deck_v1 import (
    _build_candidate_critic_user_prompt,
    _build_final_critic_batch_user_prompt,
    _build_wild_combo_user_prompt,
    _render_theme_profile_block,
)


def _fake_intent_with_profile():
    return {
        "likely_win_condition": "Test win condition",
        "implicit_themes": ["t1", "t2"],
        "theme_profile": {
            "primary":   {"theme": "dragon_tribal", "weight": 0.6},
            "secondary": {"theme": "graveyard_recursion", "weight": 0.3},
            "tertiary":  {"theme": "value_engine", "weight": 0.1},
            "mode": "hybrid",
        },
    }


class RenderThemeProfileBlockTests(unittest.TestCase):
    def test_renders_all_three_slots(self) -> None:
        block = _render_theme_profile_block(
            _fake_intent_with_profile()["theme_profile"]
        )
        self.assertIn("USER THEME PROFILE", block)
        self.assertIn("dragon_tribal", block)
        self.assertIn("graveyard_recursion", block)
        self.assertIn("value_engine", block)
        self.assertIn("0.60", block)
        self.assertIn("MAXIMIZE QUALITY WITHIN THE USER'S DECLARED CONSTRAINTS",
                      block)

    def test_returns_empty_when_none(self) -> None:
        self.assertEqual(_render_theme_profile_block(None), "")

    def test_returns_empty_when_not_dict(self) -> None:
        self.assertEqual(_render_theme_profile_block("not a dict"), "")

    def test_omits_empty_secondary_tertiary(self) -> None:
        partial = {
            "primary":   {"theme": "only_one", "weight": 1.0},
            "secondary": {"theme": "", "weight": 0.0},
            "tertiary":  {"theme": "", "weight": 0.0},
            "mode": "hint_led",
        }
        block = _render_theme_profile_block(partial)
        self.assertIn("only_one", block)
        # Secondary/tertiary lines absent when their themes are empty.
        self.assertNotIn("- Secondary:", block)
        self.assertNotIn("- Tertiary:", block)


class C21CascadeTests(unittest.TestCase):
    def test_c21_user_prompt_includes_theme_profile(self) -> None:
        prompt = _build_candidate_critic_user_prompt(
            commander="X", bracket="B3", theme_hints=["t"],
            intent_analysis=_fake_intent_with_profile(),
            current_deck_summary=[{"card_name": "A", "source": "user_intent"}],
            swappable_slots=[{"card_name": "B", "source": "agent"}],
            candidate_pool=[],
            bracket_policy_summary="",
            deck_primitive_index=None,
        )
        self.assertIn("USER THEME PROFILE", prompt)
        self.assertIn("dragon_tribal", prompt)


class C22CascadeTests(unittest.TestCase):
    def test_c22_user_prompt_includes_theme_profile(self) -> None:
        prompt = _build_wild_combo_user_prompt(
            commander="X", bracket="B3", theme_hints=["t"],
            intent_analysis=_fake_intent_with_profile(),
            deck=[],
            wide_pool=[{"name": "Card", "type_line": "Instant",
                        "cmc": 1, "primitives": [], "oracle_text": ""}],
            bracket_policy_summary="",
        )
        self.assertIn("USER THEME PROFILE", prompt)
        self.assertIn("dragon_tribal", prompt)


class D2CascadeTests(unittest.TestCase):
    def test_d2_batch_user_prompt_includes_theme_profile(self) -> None:
        prompt = _build_final_critic_batch_user_prompt(
            commander="X", bracket="B3", theme_hints=["t"],
            intent_analysis=_fake_intent_with_profile(),
            deck=[{"card_name": "A", "source": "agent",
                   "reason": "test"}],
            batch_priority_cards=[{"card_name": "A", "source": "agent",
                                   "reason": "test"}],
            classified_themes=[],
            strength_check_summary=None,
            include_narrative_and_suggestions=True,
        )
        self.assertIn("USER THEME PROFILE", prompt)
        self.assertIn("dragon_tribal", prompt)


if __name__ == "__main__":
    unittest.main()
