"""Iter 5 Phase 7 — theme-aware Pillar E target counts tests.

Verifies:
  - Matrix loads with default theme always present
  - Single-theme profile returns that theme's row
  - Hybrid profile blends weights correctly
  - Storm-leaning profile gets ~33 lands vs tribal's ~37
  - Missing theme name falls back to default
  - Empty/missing profile returns default
"""
from __future__ import annotations

import unittest

from api.engine.layers.theme_target_blender_v1 import (
    blend_targets_for_profile, load_target_matrix,
)


class LoadMatrixTests(unittest.TestCase):
    def test_default_theme_present(self) -> None:
        matrix = load_target_matrix()
        self.assertIn("default", matrix)
        self.assertIn("lands", matrix["default"])

    def test_canonical_themes_present(self) -> None:
        matrix = load_target_matrix()
        for theme in ("storm", "tribal", "control", "landfall",
                      "counters_matter", "aristocrats"):
            self.assertIn(theme, matrix)


class BlendTargetsTests(unittest.TestCase):
    def test_pure_storm_profile(self) -> None:
        profile = {
            "primary":   {"theme": "storm", "weight": 1.0},
            "secondary": {"theme": "", "weight": 0.0},
            "tertiary":  {"theme": "", "weight": 0.0},
        }
        targets = blend_targets_for_profile(profile)
        # Storm: lands=32.
        self.assertEqual(targets["lands"], 32)

    def test_pure_tribal_profile(self) -> None:
        profile = {
            "primary": {"theme": "tribal", "weight": 1.0},
        }
        targets = blend_targets_for_profile(profile)
        # Tribal: lands=37.
        self.assertEqual(targets["lands"], 37)

    def test_storm_60_tribal_40_blend(self) -> None:
        profile = {
            "primary":   {"theme": "storm", "weight": 0.6},
            "secondary": {"theme": "tribal", "weight": 0.4},
        }
        targets = blend_targets_for_profile(profile)
        # 32*0.6 + 37*0.4 = 19.2 + 14.8 = 34 lands.
        self.assertEqual(targets["lands"], 34)

    def test_unknown_theme_falls_back_to_default(self) -> None:
        profile = {
            "primary": {"theme": "nonexistent_archetype_xyz", "weight": 1.0},
        }
        targets = blend_targets_for_profile(profile)
        # Default: lands=36.
        self.assertEqual(targets["lands"], 36)

    def test_empty_profile_returns_default(self) -> None:
        targets = blend_targets_for_profile(None)
        self.assertEqual(targets.get("lands"), 36)
        targets = blend_targets_for_profile({})
        self.assertEqual(targets.get("lands"), 36)

    def test_three_way_blend(self) -> None:
        profile = {
            "primary":   {"theme": "tribal", "weight": 0.6},
            "secondary": {"theme": "graveyard_recursion", "weight": 0.3},
            "tertiary":  {"theme": "value_engine", "weight": 0.1},
        }
        targets = blend_targets_for_profile(profile)
        # tribal lands=37, graveyard_recursion=35, value_engine=36.
        # Blended: 37*0.6 + 35*0.3 + 36*0.1 = 22.2 + 10.5 + 3.6 = 36.3 -> 36.
        self.assertEqual(targets["lands"], 36)


if __name__ == "__main__":
    unittest.main()
