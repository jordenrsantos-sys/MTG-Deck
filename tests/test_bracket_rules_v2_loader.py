from __future__ import annotations

import unittest

from api.engine.bracket_rules_v2 import resolve_bracket_rules_v2


class BracketRulesV2LoaderTests(unittest.TestCase):
    def test_resolves_b1_policies(self) -> None:
        rules, version, unknown_flag = resolve_bracket_rules_v2("B1")

        self.assertEqual(version, "bracket_rules_v2")
        self.assertFalse(unknown_flag)
        self.assertEqual(
            rules,
            {
                "mass_land_denial": "DISALLOW",
                "extra_turn_chains": "DISALLOW",
                "two_card_combos": "DISALLOW",
            },
        )

    def test_resolves_b3_two_card_combos_track_only(self) -> None:
        rules, version, unknown_flag = resolve_bracket_rules_v2("B3")

        self.assertEqual(version, "bracket_rules_v2")
        self.assertFalse(unknown_flag)
        self.assertEqual(rules.get("two_card_combos"), "TRACK_ONLY")
        self.assertEqual(rules.get("mass_land_denial"), "DISALLOW")
        self.assertEqual(rules.get("extra_turn_chains"), "DISALLOW")

    def test_unknown_bracket_returns_unknown_flag_true(self) -> None:
        rules, version, unknown_flag = resolve_bracket_rules_v2("B9")

        self.assertEqual(version, "bracket_rules_v2")
        self.assertTrue(unknown_flag)
        self.assertEqual(rules, {})

    def test_deterministic_for_same_input(self) -> None:
        first = resolve_bracket_rules_v2("B3")
        second = resolve_bracket_rules_v2("B3")

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
