from __future__ import annotations

import unittest

from api.engine.layers.redundancy_index_v1 import REDUNDANCY_INDEX_V1_VERSION, run_redundancy_index_v1


class RedundancyIndexV1Tests(unittest.TestCase):
    def test_skip_when_required_effects_payload_missing(self) -> None:
        payload = run_redundancy_index_v1(
            required_effects_coverage=None,
            primitive_index_by_slot={"S1": ["CARD_DRAW_BURST"]},
            deck_slot_ids_playable=["S1"],
        )

        self.assertEqual(payload.get("version"), REDUNDANCY_INDEX_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason"), "REQUIRED_EFFECTS_COVERAGE_MISSING")
        self.assertEqual(payload.get("per_requirement"), [])

    def test_skip_when_primitive_index_missing(self) -> None:
        payload = run_redundancy_index_v1(
            required_effects_coverage={
                "version": "required_effects_coverage_v1",
                "status": "OK",
                "coverage": [
                    {
                        "primitive": "CARD_DRAW_BURST",
                        "min": 2,
                        "count": 2,
                        "supported": True,
                        "met": True,
                    }
                ],
            },
            primitive_index_by_slot=None,
            deck_slot_ids_playable=["S1"],
        )

        self.assertEqual(payload.get("version"), REDUNDANCY_INDEX_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason"), "PRIMITIVE_INDEX_UNAVAILABLE")

    def test_ok_ratios_and_levels(self) -> None:
        payload = run_redundancy_index_v1(
            required_effects_coverage={
                "version": "required_effects_coverage_v1",
                "status": "OK",
                "coverage": [
                    {
                        "primitive": "STACK_COUNTERSPELL",
                        "min": 2,
                        "count": 2,
                        "supported": True,
                        "met": True,
                    },
                    {
                        "primitive": "MANA_RAMP_ARTIFACT_ROCK",
                        "min": 2,
                        "count": 3,
                        "supported": True,
                        "met": True,
                    },
                ],
            },
            primitive_index_by_slot={
                "S1": ["MANA_RAMP_ARTIFACT_ROCK", "STACK_COUNTERSPELL"],
                "S2": ["MANA_RAMP_ARTIFACT_ROCK"],
                "S3": ["MANA_RAMP_ARTIFACT_ROCK", "STACK_COUNTERSPELL"],
            },
            deck_slot_ids_playable=["S3", "S1", "S2"],
        )

        self.assertEqual(payload.get("version"), REDUNDANCY_INDEX_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("reason"), None)

        self.assertEqual(
            payload.get("per_requirement"),
            [
                {
                    "primitive": "MANA_RAMP_ARTIFACT_ROCK",
                    "min": 2,
                    "count": 3,
                    "supported": True,
                    "redundancy_ratio": 1.5,
                    "redundancy_level": "HIGH",
                },
                {
                    "primitive": "STACK_COUNTERSPELL",
                    "min": 2,
                    "count": 2,
                    "supported": True,
                    "redundancy_ratio": 1.0,
                    "redundancy_level": "OK",
                },
            ],
        )

        self.assertEqual(
            payload.get("summary"),
            {
                "avg_redundancy_ratio": 1.25,
                "low_redundancy_count": 0,
                "unsupported_count": 0,
            },
        )
        self.assertEqual(payload.get("notes"), [])

    def test_warn_when_supported_requirement_under_min(self) -> None:
        payload = run_redundancy_index_v1(
            required_effects_coverage={
                "version": "required_effects_coverage_v1",
                "status": "WARN",
                "coverage": [
                    {
                        "primitive": "CARD_DRAW_BURST",
                        "min": 3,
                        "count": 1,
                        "supported": True,
                        "met": False,
                    }
                ],
            },
            primitive_index_by_slot={
                "S1": ["CARD_DRAW_BURST"],
                "S2": [],
                "S3": ["MANA_RAMP_ARTIFACT_ROCK"],
            },
            deck_slot_ids_playable=["S2", "S3", "S1"],
        )

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(
            payload.get("per_requirement"),
            [
                {
                    "primitive": "CARD_DRAW_BURST",
                    "min": 3,
                    "count": 1,
                    "supported": True,
                    "redundancy_ratio": 0.333333,
                    "redundancy_level": "LOW",
                }
            ],
        )
        self.assertEqual(
            payload.get("summary"),
            {
                "avg_redundancy_ratio": 0.333333,
                "low_redundancy_count": 1,
                "unsupported_count": 0,
            },
        )

        notes = payload.get("notes") if isinstance(payload.get("notes"), list) else []
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0].get("code"), "REDUNDANCY_BELOW_MIN")

    def test_determinism_repeat_call_identical(self) -> None:
        kwargs = {
            "required_effects_coverage": {
                "version": "required_effects_coverage_v1",
                "status": "WARN",
                "coverage": [
                    {
                        "primitive": "CARD_DRAW_BURST",
                        "min": 3,
                        "count": 1,
                        "supported": True,
                        "met": False,
                    },
                    {
                        "primitive": "UNSUPPORTED_X",
                        "min": 1,
                        "count": None,
                        "supported": False,
                        "met": None,
                    },
                ],
            },
            "primitive_index_by_slot": {
                "S1": ["CARD_DRAW_BURST"],
                "S2": [],
            },
            "deck_slot_ids_playable": ["S2", "S1"],
        }

        first = run_redundancy_index_v1(**kwargs)
        second = run_redundancy_index_v1(**kwargs)
        self.assertEqual(first, second)

    def test_ordering_stable_by_primitive(self) -> None:
        payload = run_redundancy_index_v1(
            required_effects_coverage={
                "version": "required_effects_coverage_v1",
                "status": "OK",
                "coverage": [
                    {
                        "primitive": "Z_LAST",
                        "min": 1,
                        "count": 1,
                        "supported": True,
                        "met": True,
                    },
                    {
                        "primitive": "A_FIRST",
                        "min": 1,
                        "count": 1,
                        "supported": True,
                        "met": True,
                    },
                ],
            },
            primitive_index_by_slot={
                "S1": ["A_FIRST", "Z_LAST"],
            },
            deck_slot_ids_playable=["S1"],
        )

        per_requirement = payload.get("per_requirement") if isinstance(payload.get("per_requirement"), list) else []
        primitives = [entry.get("primitive") for entry in per_requirement if isinstance(entry, dict)]
        self.assertEqual(primitives, ["A_FIRST", "Z_LAST"])


if __name__ == "__main__":
    unittest.main()
