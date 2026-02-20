from __future__ import annotations

import unittest

from api.engine.layers.commander_reliability_model_v1 import (
    COMMANDER_RELIABILITY_MODEL_V1_VERSION,
    run_commander_reliability_model_v1,
)


class CommanderReliabilityModelV1Tests(unittest.TestCase):
    def _baseline_payload(self) -> dict:
        return {
            "version": "probability_checkpoint_layer_v1",
            "status": "OK",
            "reason_code": None,
            "codes": [],
            "probabilities_by_bucket": [
                {
                    "bucket": "RAMP",
                    "probabilities_by_checkpoint": [
                        {"checkpoint": 9, "p_ge_1": 0.5},
                        {"checkpoint": 10, "p_ge_1": 0.6},
                        {"checkpoint": 12, "p_ge_1": 0.8},
                    ],
                }
            ],
        }

    def _stress_payload(self) -> dict:
        return {
            "version": "stress_transform_engine_v1",
            "status": "OK",
            "reason_code": None,
            "codes": [],
            "stress_adjusted_probabilities_by_bucket": [
                {
                    "bucket": "RAMP",
                    "probabilities_by_checkpoint": [
                        {"checkpoint": 9, "p_ge_1": 0.4},
                        {"checkpoint": 10, "p_ge_1": 0.45},
                        {"checkpoint": 12, "p_ge_1": 0.6},
                    ],
                }
            ],
        }

    def _engine_requirements_payload(self, commander_dependent: str) -> dict:
        return {
            "version": "engine_requirement_detection_v1",
            "status": "OK",
            "reason_code": None,
            "codes": [],
            "unknowns": [],
            "engine_requirements_v1": {
                "commander_dependent": commander_dependent,
            },
        }

    def test_skip_when_commander_missing(self) -> None:
        payload = run_commander_reliability_model_v1(
            commander_slot_id=None,
            probability_checkpoint_layer_v1_payload=self._baseline_payload(),
            stress_transform_engine_v1_payload=self._stress_payload(),
            engine_requirement_detection_v1_payload=self._engine_requirements_payload("LOW"),
            primitive_index_by_slot={},
            deck_slot_ids_playable=[],
        )

        self.assertEqual(payload.get("version"), COMMANDER_RELIABILITY_MODEL_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "COMMANDER_SLOT_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])

    def test_warn_when_commander_dependent_high_and_protection_proxy_unavailable(self) -> None:
        payload = run_commander_reliability_model_v1(
            commander_slot_id="C0",
            probability_checkpoint_layer_v1_payload=self._baseline_payload(),
            stress_transform_engine_v1_payload=self._stress_payload(),
            engine_requirement_detection_v1_payload=self._engine_requirements_payload("HIGH"),
            primitive_index_by_slot=None,
            deck_slot_ids_playable=None,
        )

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(
            payload.get("codes"),
            ["COMMANDER_RELIABILITY_PROTECTION_PROXY_UNAVAILABLE"],
        )

        metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
        self.assertEqual(metrics.get("cast_reliability_t3"), 0.5)
        self.assertEqual(metrics.get("cast_reliability_t4"), 0.6)
        self.assertEqual(metrics.get("cast_reliability_t6"), 0.8)
        self.assertIsNone(metrics.get("protection_coverage_proxy"))
        self.assertEqual(metrics.get("commander_fragility_delta"), 0.15)

        notes = payload.get("notes") if isinstance(payload.get("notes"), list) else []
        self.assertEqual(
            notes,
            [
                "Commander dependency is HIGH while protection coverage proxy is unavailable.",
                "Protection coverage proxy unavailable from primitive index/playable slot inputs.",
            ],
        )

    def test_determinism_repeat_call_identical(self) -> None:
        kwargs = {
            "commander_slot_id": "C0",
            "probability_checkpoint_layer_v1_payload": self._baseline_payload(),
            "stress_transform_engine_v1_payload": self._stress_payload(),
            "engine_requirement_detection_v1_payload": self._engine_requirements_payload("MED"),
            "primitive_index_by_slot": {
                "S1": ["HEXPROOF_PROTECTION"],
                "S2": ["CARD_DRAW_BURST"],
                "S3": ["INDESTRUCTIBLE_PROTECTION"],
            },
            "deck_slot_ids_playable": ["S2", "S1", "S3", "S1"],
        }

        first = run_commander_reliability_model_v1(**kwargs)
        second = run_commander_reliability_model_v1(**kwargs)
        self.assertEqual(first, second)

        self.assertEqual(first.get("status"), "OK")
        metrics = first.get("metrics") if isinstance(first.get("metrics"), dict) else {}
        self.assertEqual(metrics.get("protection_coverage_proxy"), 0.666667)


if __name__ == "__main__":
    unittest.main()
