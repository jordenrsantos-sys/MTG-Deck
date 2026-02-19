from __future__ import annotations

import unittest

from api.engine.structural_snapshot_v1 import build_structural_snapshot_v1


class StructuralSnapshotV1DeterminismTests(unittest.TestCase):
    def test_structural_snapshot_is_deterministic_and_sorted(self) -> None:
        primitive_index_by_slot = {
            "C0": ["P3", "P4", "P3"],
            "S0": ["P2", "P1", "P2"],
            "S1": ["P1"],
            "S2": [],
            "S3": ["P3", "P2"],
            "S4": ["P3"],
            "S5": [],
        }
        deck_slot_ids = ["S5", "S2", "S0", "S4", "S3", "S1"]
        required_primitives = ["P9", "P3", "P4", "P1"]
        basic_land_slot_ids = ["S2"]

        snapshot_one = build_structural_snapshot_v1(
            snapshot_id="snap_001",
            taxonomy_version="taxonomy_v1_23",
            ruleset_version="taxonomy_v1_23",
            profile_id="default",
            bracket_id="B3",
            commander_slot_id="C0",
            deck_slot_ids=list(deck_slot_ids),
            primitive_index_by_slot=dict(primitive_index_by_slot),
            required_primitives=list(required_primitives),
            basic_land_slot_ids=list(basic_land_slot_ids),
        )

        snapshot_two = build_structural_snapshot_v1(
            snapshot_id="snap_001",
            taxonomy_version="taxonomy_v1_23",
            ruleset_version="taxonomy_v1_23",
            profile_id="default",
            bracket_id="B3",
            commander_slot_id="C0",
            deck_slot_ids=list(deck_slot_ids),
            primitive_index_by_slot=dict(primitive_index_by_slot),
            required_primitives=list(required_primitives),
            basic_land_slot_ids=list(basic_land_slot_ids),
        )

        self.assertEqual(snapshot_one, snapshot_two)
        self.assertEqual(
            list(snapshot_one.keys()),
            [
                "snapshot_id",
                "taxonomy_version",
                "ruleset_version",
                "profile_id",
                "bracket_id",
                "required_primitives_v1",
                "present_primitives_v1",
                "missing_primitives_v1",
                "primitive_counts_by_id",
                "primitive_concentration_index_v1",
                "dead_slot_ids_v1",
                "commander_dependency_signal_v1",
                "structural_health_summary_v1",
            ],
        )

        self.assertEqual(snapshot_one["required_primitives_v1"], ["P1", "P3", "P4", "P9"])
        self.assertEqual(snapshot_one["present_primitives_v1"], ["P1", "P2", "P3"])
        self.assertEqual(snapshot_one["missing_primitives_v1"], ["P4", "P9"])
        self.assertEqual(snapshot_one["dead_slot_ids_v1"], ["S5"])

        self.assertEqual(
            list(snapshot_one["primitive_counts_by_id"].keys()),
            ["P1", "P2", "P3"],
        )
        self.assertEqual(
            snapshot_one["primitive_counts_by_id"],
            {
                "P1": 2,
                "P2": 2,
                "P3": 2,
            },
        )

        self.assertEqual(snapshot_one["primitive_concentration_index_v1"], 0.333333)
        self.assertEqual(snapshot_one["commander_dependency_signal_v1"], 0.25)

        summary = snapshot_one["structural_health_summary_v1"]
        self.assertEqual(
            list(summary.keys()),
            ["dead_slot_count", "missing_required_count", "top_primitives"],
        )
        self.assertEqual(summary["missing_required_count"], 2)
        self.assertEqual(summary["dead_slot_count"], 1)
        self.assertEqual(
            summary["top_primitives"],
            [
                {"primitive_id": "P1", "count": 2},
                {"primitive_id": "P2", "count": 2},
                {"primitive_id": "P3", "count": 2},
            ],
        )


if __name__ == "__main__":
    unittest.main()
