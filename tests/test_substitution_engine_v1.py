from __future__ import annotations

import unittest

from api.engine.bucket_substitutions_v1 import load_bucket_substitutions_v1
from api.engine.layers.substitution_engine_v1 import (
    SUBSTITUTION_ENGINE_V1_VERSION,
    run_substitution_engine_v1,
)


class SubstitutionEngineV1Tests(unittest.TestCase):
    def test_loader_is_deterministic_and_sorted(self) -> None:
        first = load_bucket_substitutions_v1()
        second = load_bucket_substitutions_v1()

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), "bucket_substitutions_v1")

        format_defaults = first.get("format_defaults") if isinstance(first.get("format_defaults"), dict) else {}
        commander_defaults = format_defaults.get("commander") if isinstance(format_defaults.get("commander"), dict) else {}
        buckets = commander_defaults.get("buckets") if isinstance(commander_defaults.get("buckets"), dict) else {}

        self.assertEqual(list(buckets.keys()), sorted(buckets.keys()))

        for bucket_payload in buckets.values():
            self.assertIsInstance(bucket_payload, dict)
            primary_primitives = bucket_payload.get("primary_primitives") if isinstance(bucket_payload.get("primary_primitives"), list) else []
            self.assertEqual(primary_primitives, sorted(primary_primitives))

    def test_skip_when_bucket_substitutions_payload_unavailable(self) -> None:
        payload = run_substitution_engine_v1(
            primitive_index_by_slot={"S1": ["MANA_RAMP_ARTIFACT_ROCK"]},
            deck_slot_ids_playable=["S1"],
            engine_requirement_detection_v1_payload={"engine_requirements_v1": {}},
            format="commander",
            bucket_substitutions_payload=None,
        )

        self.assertEqual(payload.get("version"), SUBSTITUTION_ENGINE_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "BUCKET_SUBSTITUTIONS_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("buckets"), [])

    def test_skip_when_format_missing(self) -> None:
        assumptions = load_bucket_substitutions_v1()
        payload = run_substitution_engine_v1(
            primitive_index_by_slot={"S1": ["MANA_RAMP_ARTIFACT_ROCK"]},
            deck_slot_ids_playable=["S1"],
            engine_requirement_detection_v1_payload={"engine_requirements_v1": {}},
            format="legacy",
            bucket_substitutions_payload=assumptions,
        )

        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "FORMAT_BUCKET_SUBSTITUTIONS_UNAVAILABLE")

    def test_skip_when_primitive_index_unavailable(self) -> None:
        assumptions = load_bucket_substitutions_v1()
        payload = run_substitution_engine_v1(
            primitive_index_by_slot=None,
            deck_slot_ids_playable=["S1"],
            engine_requirement_detection_v1_payload={"engine_requirements_v1": {}},
            format="commander",
            bucket_substitutions_payload=assumptions,
        )

        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "PRIMITIVE_INDEX_UNAVAILABLE")

    def test_ok_computation_aggregation_rounding_and_k_int(self) -> None:
        payload = run_substitution_engine_v1(
            primitive_index_by_slot={
                "S1": ["P_PRIMARY", "P_SUB_A", "P_SUB_B"],
                "S2": ["P_PRIMARY", "P_SUB_A"],
                "S3": ["P_SUB_B"],
                "S4": [],
            },
            deck_slot_ids_playable=["S3", "S2", "S1", "S4"],
            engine_requirement_detection_v1_payload={
                "engine_requirements_v1": {
                    "FLAG_X": True,
                    "FLAG_Y": False,
                }
            },
            format="commander",
            bucket_substitutions_payload={
                "version": "bucket_substitutions_v1_test",
                "format_defaults": {
                    "commander": {
                        "buckets": {
                            "ALPHA": {
                                "primary_primitives": ["P_PRIMARY"],
                                "base_substitutions": [
                                    {"primitive": "P_SUB_A", "weight": 0.5},
                                ],
                                "conditional_substitutions": [
                                    {
                                        "requirement_flag": "FLAG_X",
                                        "substitutions": [
                                            {"primitive": "P_SUB_A", "weight": 0.25},
                                            {"primitive": "P_SUB_B", "weight": 0.3333335},
                                        ],
                                    },
                                    {
                                        "requirement_flag": "FLAG_Y",
                                        "substitutions": [
                                            {"primitive": "P_SUB_B", "weight": 0.4},
                                        ],
                                    },
                                ],
                            }
                        }
                    }
                },
            },
        )

        self.assertEqual(payload.get("version"), SUBSTITUTION_ENGINE_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("reason_code"), None)
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("substitutions_version"), "bucket_substitutions_v1_test")

        buckets = payload.get("buckets") if isinstance(payload.get("buckets"), list) else []
        self.assertEqual(len(buckets), 1)

        alpha = buckets[0] if isinstance(buckets[0], dict) else {}
        self.assertEqual(alpha.get("bucket"), "ALPHA")
        self.assertEqual(alpha.get("k_primary"), 2)
        self.assertEqual(alpha.get("effective_K"), 4.166667)
        self.assertEqual(alpha.get("K_int"), 4)
        self.assertEqual(alpha.get("active_requirement_flags"), ["FLAG_X"])
        self.assertEqual(
            alpha.get("substitution_terms"),
            [
                {
                    "primitive": "P_SUB_A",
                    "weight": 0.75,
                    "k_substitute": 2,
                    "contribution": 1.5,
                },
                {
                    "primitive": "P_SUB_B",
                    "weight": 0.333334,
                    "k_substitute": 2,
                    "contribution": 0.666667,
                },
            ],
        )

    def test_warn_codes_when_engine_requirements_missing_or_flag_unavailable(self) -> None:
        payload = run_substitution_engine_v1(
            primitive_index_by_slot={
                "S1": ["P_PRIMARY", "P_SUB_A"],
                "S2": ["P_SUB_A"],
            },
            deck_slot_ids_playable=["S1", "S2"],
            engine_requirement_detection_v1_payload={},
            format="commander",
            bucket_substitutions_payload={
                "version": "bucket_substitutions_v1_test",
                "format_defaults": {
                    "commander": {
                        "buckets": {
                            "ALPHA": {
                                "primary_primitives": ["P_PRIMARY"],
                                "base_substitutions": [],
                                "conditional_substitutions": [
                                    {
                                        "requirement_flag": "FLAG_X",
                                        "substitutions": [
                                            {"primitive": "P_SUB_A", "weight": 0.25},
                                        ],
                                    }
                                ],
                            }
                        }
                    }
                },
            },
        )

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(
            payload.get("codes"),
            [
                "ENGINE_REQUIREMENTS_UNAVAILABLE",
                "SUBSTITUTION_REQUIREMENT_FLAG_UNAVAILABLE",
            ],
        )

    def test_clamp_and_k_int_floor_policy(self) -> None:
        primitive_index_by_slot = {
            f"S{index}": ["P_PRIMARY", "P_SUB"]
            for index in range(110)
        }
        playable_slots = sorted(primitive_index_by_slot.keys())

        payload = run_substitution_engine_v1(
            primitive_index_by_slot=primitive_index_by_slot,
            deck_slot_ids_playable=playable_slots,
            engine_requirement_detection_v1_payload={"engine_requirements_v1": {}},
            format="commander",
            bucket_substitutions_payload={
                "version": "bucket_substitutions_v1_test",
                "format_defaults": {
                    "commander": {
                        "buckets": {
                            "ALPHA": {
                                "primary_primitives": ["P_PRIMARY"],
                                "base_substitutions": [
                                    {"primitive": "P_SUB", "weight": 1.0},
                                    {"primitive": "P_SUB", "weight": 1.0},
                                ],
                                "conditional_substitutions": [],
                            }
                        }
                    }
                },
            },
        )

        buckets = payload.get("buckets") if isinstance(payload.get("buckets"), list) else []
        alpha = buckets[0] if buckets and isinstance(buckets[0], dict) else {}

        self.assertEqual(alpha.get("k_primary"), 110)
        self.assertEqual(alpha.get("effective_K"), 99.0)
        self.assertEqual(alpha.get("K_int"), 99)

    def test_determinism_repeat_call_identical(self) -> None:
        assumptions = load_bucket_substitutions_v1()
        kwargs = {
            "primitive_index_by_slot": {
                "S1": ["MANA_RAMP_ARTIFACT_ROCK", "CARD_DRAW_BURST"],
                "S2": ["STACK_COUNTERSPELL"],
                "S3": ["TARGETED_REMOVAL_CREATURE"],
            },
            "deck_slot_ids_playable": ["S3", "S2", "S1"],
            "engine_requirement_detection_v1_payload": {
                "engine_requirements_v1": {
                    "LANDFALL_BASED": True,
                    "SAC_BASED": False,
                    "SPELL_DENSITY_BASED": True,
                }
            },
            "format": "commander",
            "bucket_substitutions_payload": assumptions,
        }

        first = run_substitution_engine_v1(**kwargs)
        second = run_substitution_engine_v1(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
