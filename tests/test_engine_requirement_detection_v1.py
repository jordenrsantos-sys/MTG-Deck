from __future__ import annotations

import unittest
from unittest.mock import patch

from api.engine.dependency_signatures_v1 import load_dependency_signatures_v1
from api.engine.layers.engine_requirement_detection_v1 import (
    ENGINE_REQUIREMENT_DETECTION_V1_VERSION,
    run_engine_requirement_detection_v1,
)


class EngineRequirementDetectionV1Tests(unittest.TestCase):
    def test_loader_is_deterministic_and_sorted(self) -> None:
        first = load_dependency_signatures_v1()
        second = load_dependency_signatures_v1()

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), "dependency_signatures_v1")

        signatures = first.get("signatures") if isinstance(first.get("signatures"), dict) else {}
        self.assertEqual(list(signatures.keys()), sorted(signatures.keys()))

        for signature_name in sorted(signatures.keys()):
            signature_payload = signatures.get(signature_name) if isinstance(signatures.get(signature_name), dict) else {}
            primitives = (
                signature_payload.get("any_required_primitives")
                if isinstance(signature_payload.get("any_required_primitives"), list)
                else []
            )
            self.assertEqual(primitives, sorted(primitives))

    def test_skip_when_primitive_index_missing(self) -> None:
        payload = run_engine_requirement_detection_v1(
            primitive_index_by_slot=None,
            slot_ids_by_primitive={"SAC_OUTLET_FREE": ["S1"]},
            commander_slot_id="C0",
        )

        self.assertEqual(payload.get("version"), ENGINE_REQUIREMENT_DETECTION_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "PRIMITIVE_INDEX_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("unknowns"), [])
        self.assertEqual(payload.get("engine_requirements_v1"), {})

    def test_warn_unknowns_and_commander_missing(self) -> None:
        with patch(
            "api.engine.layers.engine_requirement_detection_v1.load_dependency_signatures_v1",
            return_value={
                "version": "dependency_signatures_v1",
                "signatures": {
                    "A_SIG": {
                        "any_required_primitives": [
                            "KNOWN_A",
                            "UNKNOWN_PRIMITIVE_ID::LANDFALL_BASED",
                        ]
                    },
                    "B_SIG": {
                        "any_required_primitives": [
                            "KNOWN_B",
                            "KNOWN_MISSING",
                        ]
                    },
                },
            },
        ):
            payload = run_engine_requirement_detection_v1(
                primitive_index_by_slot={
                    "C0": ["KNOWN_A"],
                    "S1": ["KNOWN_A"],
                    "S2": ["KNOWN_B"],
                },
                slot_ids_by_primitive={
                    "KNOWN_A": ["C0", "S1"],
                    "KNOWN_B": ["S2"],
                },
                commander_slot_id=None,
            )

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(payload.get("reason_code"), None)

        self.assertEqual(
            payload.get("codes"),
            [
                "COMMANDER_SLOT_ID_MISSING",
                "ENGINE_REQ_MANA_HUNGRY_UNIMPLEMENTED",
                "ENGINE_REQ_PERMANENT_TYPE_UNIMPLEMENTED",
                "ENGINE_REQ_SHUFFLE_UNIMPLEMENTED",
                "UNKNOWN_PRIMITIVE_ID_IN_SIGNATURES",
            ],
        )

        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        self.assertEqual(
            unknowns,
            [
                {
                    "code": "UNKNOWN_PRIMITIVE_ID_IN_SIGNATURES",
                    "primitive_ids": [
                        "KNOWN_MISSING",
                        "UNKNOWN_PRIMITIVE_ID::LANDFALL_BASED",
                    ],
                }
            ],
        )

        requirements = (
            payload.get("engine_requirements_v1")
            if isinstance(payload.get("engine_requirements_v1"), dict)
            else {}
        )
        self.assertEqual(
            requirements,
            {
                "A_SIG": True,
                "B_SIG": True,
                "commander_dependent": "UNKNOWN",
                "mana_hungry": False,
                "requires_shuffle": False,
                "requires_specific_permanent_type": [],
            },
        )

    def test_commander_dependent_low_when_no_shared_primitives(self) -> None:
        with patch(
            "api.engine.layers.engine_requirement_detection_v1.load_dependency_signatures_v1",
            return_value={
                "version": "dependency_signatures_v1",
                "signatures": {
                    "SAC_BASED": {
                        "any_required_primitives": [
                            "SAC_OUTLET_FREE",
                        ]
                    }
                },
            },
        ):
            payload = run_engine_requirement_detection_v1(
                primitive_index_by_slot={
                    "C0": ["COMMANDER_ONLY_PRIMITIVE"],
                    "S1": ["SAC_OUTLET_FREE"],
                },
                slot_ids_by_primitive={
                    "SAC_OUTLET_FREE": ["S1"],
                },
                commander_slot_id="C0",
            )

        requirements = (
            payload.get("engine_requirements_v1")
            if isinstance(payload.get("engine_requirements_v1"), dict)
            else {}
        )
        self.assertEqual(requirements.get("commander_dependent"), "LOW")

    def test_determinism_repeated_call_identical(self) -> None:
        kwargs = {
            "primitive_index_by_slot": {
                "C0": ["SAC_OUTLET_FREE", "SPELL_COPY"],
                "S1": ["SAC_OUTLET_FREE"],
                "S2": ["SPELL_COPY"],
            },
            "slot_ids_by_primitive": {
                "SAC_OUTLET_FREE": ["C0", "S1"],
                "SPELL_COPY": ["C0", "S2"],
            },
            "commander_slot_id": "C0",
        }

        first = run_engine_requirement_detection_v1(**kwargs)
        second = run_engine_requirement_detection_v1(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
