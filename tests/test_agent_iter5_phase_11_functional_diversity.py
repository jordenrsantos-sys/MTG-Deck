"""Iter 5 Phase 11 — functional diversity prompt-engineering tests."""
from __future__ import annotations

import unittest

from api.engine.layers.agent_build_deck_v1 import (
    _build_candidate_critic_user_prompt,
    _build_wild_combo_user_prompt,
    _functional_diversity_block_from_profile,
    _render_functional_diversity_block,
)


class RenderFunctionalDiversityBlockTests(unittest.TestCase):
    def test_renders_targets(self) -> None:
        targets = {
            "lands": 36, "ramp": 10, "draw": 10, "interaction": 10,
            "creatures": 28, "win_conditions": 4,
        }
        block = _render_functional_diversity_block(targets)
        self.assertIn("FUNCTIONAL DIVERSITY GUIDANCE", block)
        self.assertIn("ramp pieces target: 10", block)
        self.assertIn("card-advantage pieces target: 10", block)
        self.assertIn("interaction (counters/removal/wipes) target: 10",
                      block)
        self.assertIn("variety is GOOD", block)

    def test_empty_targets_empty_block(self) -> None:
        self.assertEqual(_render_functional_diversity_block({}), "")
        self.assertEqual(_render_functional_diversity_block(None), "")

    def test_extras_surfaced(self) -> None:
        targets = {
            "lands": 36, "ramp": 10, "draw": 10, "interaction": 10,
            "sac_outlets": 6,
        }
        block = _render_functional_diversity_block(targets)
        self.assertIn("sac outlets target: 6", block)


class FromProfileWrapperTests(unittest.TestCase):
    def test_blends_then_renders(self) -> None:
        profile = {
            "primary":   {"theme": "storm", "weight": 1.0},
        }
        block = _functional_diversity_block_from_profile(profile)
        self.assertIn("FUNCTIONAL DIVERSITY GUIDANCE", block)
        # Storm targets: ramp=12, draw=12, interaction=6.
        self.assertIn("ramp pieces target: 12", block)
        self.assertIn("interaction (counters/removal/wipes) target: 6", block)


class C21IntegrationTests(unittest.TestCase):
    def test_c21_user_prompt_includes_functional_diversity(self) -> None:
        prompt = _build_candidate_critic_user_prompt(
            commander="X", bracket="B3", theme_hints=["t"],
            intent_analysis={
                "likely_win_condition": "test",
                "implicit_themes": [],
                "theme_profile": {
                    "primary": {"theme": "tribal", "weight": 1.0},
                    "secondary": {"theme": "", "weight": 0},
                    "tertiary": {"theme": "", "weight": 0},
                    "mode": "hint_led",
                },
            },
            current_deck_summary=[{"card_name": "A", "source": "user_intent"}],
            swappable_slots=[{"card_name": "B", "source": "agent"}],
            candidate_pool=[],
            bracket_policy_summary="",
            deck_primitive_index=None,
        )
        self.assertIn("FUNCTIONAL DIVERSITY GUIDANCE", prompt)


class C22IntegrationTests(unittest.TestCase):
    def test_c22_user_prompt_includes_functional_diversity(self) -> None:
        prompt = _build_wild_combo_user_prompt(
            commander="X", bracket="B3", theme_hints=["t"],
            intent_analysis={
                "likely_win_condition": "test",
                "theme_profile": {
                    "primary": {"theme": "storm", "weight": 1.0},
                    "secondary": {"theme": "", "weight": 0},
                    "tertiary": {"theme": "", "weight": 0},
                    "mode": "hint_led",
                },
            },
            deck=[],
            wide_pool=[{"name": "Card", "type_line": "Instant",
                        "cmc": 1, "primitives": [], "oracle_text": ""}],
            bracket_policy_summary="",
        )
        self.assertIn("FUNCTIONAL DIVERSITY GUIDANCE", prompt)


if __name__ == "__main__":
    unittest.main()
