from __future__ import annotations

import unittest

from api.engine.layers.required_effects_coverage_v1 import (
    REQUIRED_EFFECTS_COVERAGE_V1_VERSION,
    run_required_effects_coverage_v1,
)
from api.engine.required_effects_v1 import load_required_effects_v1, resolve_required_effects_v1


class RequiredEffectsCoverageV1Tests(unittest.TestCase):
    def test_loader_and_resolver_are_deterministic(self) -> None:
        first_payload = load_required_effects_v1()
        second_payload = load_required_effects_v1()
        self.assertEqual(first_payload, second_payload)
        self.assertEqual(first_payload.get("version"), "required_effects_v1")

        first_requirements, first_version = resolve_required_effects_v1("commander")
        second_requirements, second_version = resolve_required_effects_v1("commander")

        self.assertEqual(first_requirements, second_requirements)
        self.assertEqual(first_version, second_version)
        self.assertEqual(first_version, "required_effects_v1")

        requirements_map = (
            first_requirements.get("requirements") if isinstance(first_requirements.get("requirements"), dict) else {}
        )
        self.assertEqual(list(requirements_map.keys()), sorted(requirements_map.keys()))

    def test_coverage_counts_correct_for_synthetic_inputs(self) -> None:
        payload = run_required_effects_coverage_v1(
            deck_slot_ids_playable=["S3", "S2", "C0", "S1"],
            primitive_index_by_slot={
                "C0": ["MANA_RAMP_ARTIFACT_ROCK", "CARD_DRAW_BURST"],
                "S1": ["MANA_RAMP_ARTIFACT_ROCK", "STACK_COUNTERSPELL"],
                "S2": ["TARGETED_REMOVAL_CREATURE", "BOARDWIPE_CREATURES"],
                "S3": ["MANA_RAMP_ARTIFACT_ROCK", "TARGETED_REMOVAL_CREATURE"],
            },
            format="commander",
            requirements_dict={
                "requirements": {
                    "MANA_RAMP_ARTIFACT_ROCK": 3,
                    "CARD_DRAW_BURST": 1,
                },
                "taxonomy_primitive_ids": [
                    "MANA_RAMP_ARTIFACT_ROCK",
                    "CARD_DRAW_BURST",
                ],
            },
            requirements_version="required_effects_v1",
        )

        self.assertEqual(payload.get("version"), REQUIRED_EFFECTS_COVERAGE_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("requirements_version"), "required_effects_v1")
        self.assertEqual(payload.get("unknowns"), [])
        self.assertEqual(payload.get("missing"), [])

        coverage = payload.get("coverage") if isinstance(payload.get("coverage"), list) else []
        self.assertEqual(
            coverage,
            [
                {
                    "primitive": "CARD_DRAW_BURST",
                    "min": 1,
                    "count": 1,
                    "supported": True,
                    "met": True,
                },
                {
                    "primitive": "MANA_RAMP_ARTIFACT_ROCK",
                    "min": 3,
                    "count": 3,
                    "supported": True,
                    "met": True,
                },
            ],
        )

    def test_warn_when_supported_requirement_missing(self) -> None:
        payload = run_required_effects_coverage_v1(
            deck_slot_ids_playable=["C0", "S1", "S2", "S3"],
            primitive_index_by_slot={
                "C0": ["MANA_RAMP_ARTIFACT_ROCK"],
                "S1": ["MANA_RAMP_ARTIFACT_ROCK"],
                "S2": ["TARGETED_REMOVAL_CREATURE"],
                "S3": ["CARD_DRAW_BURST"],
            },
            format="commander",
            requirements_dict={
                "requirements": {
                    "MANA_RAMP_ARTIFACT_ROCK": 3,
                    "CARD_DRAW_BURST": 2,
                },
                "taxonomy_primitive_ids": [
                    "MANA_RAMP_ARTIFACT_ROCK",
                    "CARD_DRAW_BURST",
                ],
            },
            requirements_version="required_effects_v1",
        )

        self.assertEqual(payload.get("status"), "WARN")
        missing = payload.get("missing") if isinstance(payload.get("missing"), list) else []
        self.assertEqual(
            missing,
            [
                {
                    "primitive": "CARD_DRAW_BURST",
                    "min": 2,
                    "count": 1,
                },
                {
                    "primitive": "MANA_RAMP_ARTIFACT_ROCK",
                    "min": 3,
                    "count": 2,
                },
            ],
        )

    def test_unknown_when_unsupported_primitive_present(self) -> None:
        payload = run_required_effects_coverage_v1(
            deck_slot_ids_playable=["C0", "S1"],
            primitive_index_by_slot={
                "C0": ["MANA_RAMP_ARTIFACT_ROCK"],
                "S1": ["CARD_DRAW_BURST"],
            },
            format="commander",
            requirements_dict={
                "requirements": {
                    "MANA_RAMP_ARTIFACT_ROCK": 1,
                    "UNSUPPORTED_PRIMITIVE_XYZ": 1,
                },
                "taxonomy_primitive_ids": ["MANA_RAMP_ARTIFACT_ROCK"],
            },
            requirements_version="required_effects_v1",
        )

        self.assertEqual(payload.get("status"), "WARN")

        coverage = payload.get("coverage") if isinstance(payload.get("coverage"), list) else []
        unsupported = [row for row in coverage if isinstance(row, dict) and row.get("primitive") == "UNSUPPORTED_PRIMITIVE_XYZ"]
        self.assertEqual(
            unsupported,
            [
                {
                    "primitive": "UNSUPPORTED_PRIMITIVE_XYZ",
                    "min": 1,
                    "count": None,
                    "supported": False,
                    "met": None,
                }
            ],
        )

        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        self.assertEqual(
            unknowns,
            [
                {
                    "code": "REQUIRED_PRIMITIVE_UNSUPPORTED",
                    "message": (
                        "Required primitive 'UNSUPPORTED_PRIMITIVE_XYZ' is unsupported by runtime taxonomy coverage definitions."
                    ),
                }
            ],
        )

    def test_determinism_repeated_call_identical(self) -> None:
        kwargs = {
            "deck_slot_ids_playable": ["S3", "S2", "C0", "S1"],
            "primitive_index_by_slot": {
                "C0": ["MANA_RAMP_ARTIFACT_ROCK", "CARD_DRAW_BURST"],
                "S1": ["MANA_RAMP_ARTIFACT_ROCK", "STACK_COUNTERSPELL"],
                "S2": ["TARGETED_REMOVAL_CREATURE", "BOARDWIPE_CREATURES"],
                "S3": ["MANA_RAMP_ARTIFACT_ROCK", "TARGETED_REMOVAL_CREATURE"],
            },
            "format": "commander",
            "requirements_dict": {
                "requirements": {
                    "MANA_RAMP_ARTIFACT_ROCK": 3,
                    "CARD_DRAW_BURST": 1,
                },
                "taxonomy_primitive_ids": [
                    "MANA_RAMP_ARTIFACT_ROCK",
                    "CARD_DRAW_BURST",
                ],
            },
            "requirements_version": "required_effects_v1",
        }

        first = run_required_effects_coverage_v1(**kwargs)
        second = run_required_effects_coverage_v1(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
