from __future__ import annotations

import unittest

from api.engine.layers.bracket_compliance_summary_v1 import (
    BRACKET_COMPLIANCE_SUMMARY_V1_VERSION,
    run_bracket_compliance_summary_v1,
)


class BracketComplianceSummaryV1Tests(unittest.TestCase):
    def test_skip_when_enforcement_payload_missing(self) -> None:
        payload = run_bracket_compliance_summary_v1(None)

        self.assertEqual(payload.get("version"), BRACKET_COMPLIANCE_SUMMARY_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertIsNone(payload.get("bracket_id"))

        counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
        self.assertEqual(
            counts,
            {
                "game_changers": None,
                "mass_land_denial": None,
                "extra_turns": None,
                "two_card_combos": None,
            },
        )
        self.assertEqual(payload.get("violations"), [])
        self.assertEqual(payload.get("flags"), [])
        self.assertEqual(payload.get("unknowns"), [])

        versions = payload.get("versions") if isinstance(payload.get("versions"), dict) else {}
        self.assertEqual(
            versions,
            {
                "gc_limits_version": None,
                "bracket_rules_version": None,
                "two_card_combos_version": None,
            },
        )

    def test_error_when_violations_exist(self) -> None:
        enforcement = {
            "bracket_id": "B2",
            "counts": {
                "game_changers_in_deck": 2,
            },
            "category_results": {
                "mass_land_denial": {"count": 1},
                "extra_turn_chains": {"count": 0},
                "two_card_combos": {"count": 1, "policy": "DISALLOW"},
            },
            "violations": [
                {"code": "TWO_CARD_COMBOS_DISALLOWED", "category": "two_card_combos", "message": "z"},
                {"code": "EXTRA_TURN_CHAINS_DISALLOWED", "category": "extra_turn_chains", "message": "a"},
            ],
            "unknowns": [],
            "gc_limits_version": "gc_limits_v1",
            "bracket_rules_version": "bracket_rules_v2",
        }

        payload = run_bracket_compliance_summary_v1(enforcement)

        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(payload.get("bracket_id"), "B2")

        counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
        self.assertEqual(counts.get("game_changers"), 2)
        self.assertEqual(counts.get("mass_land_denial"), 1)
        self.assertEqual(counts.get("extra_turns"), 0)
        self.assertEqual(counts.get("two_card_combos"), 1)

        violations = payload.get("violations") if isinstance(payload.get("violations"), list) else []
        self.assertEqual(
            violations,
            [
                {
                    "code": "EXTRA_TURN_CHAINS_DISALLOWED",
                    "category": "extra_turn_chains",
                    "message": "a",
                },
                {
                    "code": "TWO_CARD_COMBOS_DISALLOWED",
                    "category": "two_card_combos",
                    "message": "z",
                },
            ],
        )

    def test_warn_when_only_unknowns_exist(self) -> None:
        enforcement = {
            "bracket_id": "B3",
            "counts": {
                "game_changers_in_deck": 1,
            },
            "category_results": {
                "mass_land_denial": {"count": None},
                "extra_turn_chains": {"count": None},
                "two_card_combos": {"count": 0, "policy": "TRACK_ONLY"},
            },
            "violations": [],
            "unknowns": [
                {"code": "MISSING_SUPPORT_MASS_LAND_DENIAL", "message": "b"},
                {"code": "MISSING_SUPPORT_EXTRA_TURN_CHAINS", "message": "a"},
            ],
            "gc_limits_version": "gc_limits_v1",
            "bracket_rules_version": "bracket_rules_v2",
        }

        payload = run_bracket_compliance_summary_v1(enforcement)

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(payload.get("violations"), [])

        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        self.assertEqual(
            unknowns,
            [
                {"code": "MISSING_SUPPORT_EXTRA_TURN_CHAINS", "message": "a"},
                {"code": "MISSING_SUPPORT_MASS_LAND_DENIAL", "message": "b"},
            ],
        )

    def test_track_only_combo_presence_generates_flag(self) -> None:
        enforcement = {
            "bracket_id": "B3",
            "counts": {
                "game_changers_in_deck": 1,
            },
            "category_results": {
                "mass_land_denial": {"count": 0},
                "extra_turn_chains": {"count": 0},
                "two_card_combos": {"count": 2, "policy": "TRACK_ONLY"},
            },
            "violations": [],
            "unknowns": [],
            "gc_limits_version": "gc_limits_v1",
            "bracket_rules_version": "bracket_rules_v2",
        }

        payload = run_bracket_compliance_summary_v1(enforcement)

        self.assertEqual(payload.get("status"), "OK")
        flags = payload.get("flags") if isinstance(payload.get("flags"), list) else []
        self.assertEqual(
            flags,
            [
                {
                    "code": "TWO_CARD_COMBOS_PRESENT_TRACK_ONLY",
                    "category": "two_card_combos",
                    "message": "Two-card combos are present and tracked by policy (not a violation).",
                }
            ],
        )

    def test_deterministic_for_same_input(self) -> None:
        enforcement = {
            "bracket_id": "B4",
            "counts": {
                "game_changers_in_deck": 0,
            },
            "category_results": {
                "mass_land_denial": {"count": 0},
                "extra_turn_chains": {"count": 0},
                "two_card_combos": {"count": 1, "policy": "ALLOW"},
            },
            "violations": [],
            "unknowns": [],
            "gc_limits_version": "gc_limits_v1",
            "bracket_rules_version": "bracket_rules_v2",
            "two_card_combos_version": "two_card_combos_v1",
        }

        first = run_bracket_compliance_summary_v1(enforcement)
        second = run_bracket_compliance_summary_v1(enforcement)

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
