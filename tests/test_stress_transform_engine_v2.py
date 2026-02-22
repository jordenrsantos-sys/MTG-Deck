from __future__ import annotations

import unittest

from api.engine.layers.stress_transform_engine_v2 import (
    STRESS_TRANSFORM_ENGINE_V2_VERSION,
    run_stress_transform_engine_v2,
)
from api.engine.stress_operator_policy_v1 import load_stress_operator_policy_v1


class StressTransformEngineV2Tests(unittest.TestCase):
    def _substitution_payload(self) -> dict:
        return {
            "version": "substitution_engine_v1",
            "status": "OK",
            "format": "commander",
            "buckets": [
                {"bucket": "RAMP", "effective_K": 12.5, "K_int": 12},
            ],
        }

    def _checkpoint_payload(self) -> dict:
        return {
            "version": "probability_checkpoint_layer_v1",
            "status": "OK",
            "format": "commander",
            "checkpoints": [7, 9, 10, 12],
            "checkpoint_draws": [
                {"checkpoint": 7, "effective_n": 7.0, "n_int": 7},
                {"checkpoint": 9, "effective_n": 9.0, "n_int": 9},
                {"checkpoint": 10, "effective_n": 10.0, "n_int": 10},
                {"checkpoint": 12, "effective_n": 12.0, "n_int": 12},
            ],
        }

    def _math_core_payload(self) -> dict:
        return {
            "version": "probability_math_core_v1",
            "status": "OK",
        }

    def _stress_model_payload(self) -> dict:
        return {
            "version": "stress_model_definition_v1",
            "status": "OK",
            "format": "commander",
            "selected_model_id": "PUNISHING_POD_V0",
            "operators": [
                {"op": "TARGETED_REMOVAL", "count": 2},
                {"op": "BOARD_WIPE", "by_turn": 4, "surviving_engine_fraction": 0.6},
                {"op": "STAX_TAX", "by_turn": 4, "inflation_factor": 1.2},
            ],
        }

    def test_deterministic_ordering_with_by_turn_and_policy_precedence(self) -> None:
        payload = run_stress_transform_engine_v2(
            substitution_engine_v1_payload=self._substitution_payload(),
            probability_checkpoint_layer_v1_payload=self._checkpoint_payload(),
            stress_model_definition_v1_payload=self._stress_model_payload(),
            probability_math_core_v1_payload=self._math_core_payload(),
            stress_operator_policy_v1_payload=load_stress_operator_policy_v1(),
        )

        self.assertEqual(payload.get("version"), STRESS_TRANSFORM_ENGINE_V2_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("policy_version"), "stress_operator_policy_v1")

        applied = payload.get("operators_applied") if isinstance(payload.get("operators_applied"), list) else []
        self.assertEqual(
            [entry.get("op") for entry in applied if isinstance(entry, dict)],
            ["BOARD_WIPE", "STAX_TAX", "TARGETED_REMOVAL"],
        )
        self.assertEqual(
            [entry.get("by_turn") for entry in applied if isinstance(entry, dict)],
            [4, 4, 4],
        )

    def test_repeat_call_identical(self) -> None:
        kwargs = {
            "substitution_engine_v1_payload": self._substitution_payload(),
            "probability_checkpoint_layer_v1_payload": self._checkpoint_payload(),
            "stress_model_definition_v1_payload": self._stress_model_payload(),
            "probability_math_core_v1_payload": self._math_core_payload(),
            "stress_operator_policy_v1_payload": load_stress_operator_policy_v1(),
        }
        first = run_stress_transform_engine_v2(**kwargs)
        second = run_stress_transform_engine_v2(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
