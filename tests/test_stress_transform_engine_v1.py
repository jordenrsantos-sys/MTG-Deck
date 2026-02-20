from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import unittest

from api.engine.layers.stress_transform_engine_v1 import (
    STRESS_TRANSFORM_ENGINE_V1_VERSION,
    run_stress_transform_engine_v1,
)
from api.engine.probability_math_core_v1 import hypergeom_p_ge_1


def _round6_half_up(value: float) -> float:
    return float(Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


class StressTransformEngineV1Tests(unittest.TestCase):
    def _substitution_payload(self) -> dict:
        return {
            "version": "substitution_engine_v1",
            "status": "OK",
            "format": "commander",
            "buckets": [
                {"bucket": "RAMP", "effective_K": 12.5, "K_int": 12},
                {"bucket": "DRAW", "effective_K": 8.0, "K_int": 8},
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
            "probabilities_by_bucket": [],
        }

    def _stress_model_payload(self, operators: list[dict]) -> dict:
        return {
            "version": "stress_model_definition_v1",
            "status": "OK",
            "reason_code": None,
            "codes": [],
            "stress_models_version": "stress_models_v1",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "request_override_model_id": None,
            "selected_model_id": "PUNISHING_POD_V0",
            "selection_source": "profile_bracket",
            "operators": operators,
        }

    def _math_core_payload(self) -> dict:
        return {
            "version": "probability_math_core_v1",
            "status": "OK",
            "reason_code": None,
            "codes": [],
            "math_backend": "int_comb",
            "available_functions": ["comb", "hypergeom_p_ge_1", "hypergeom_p_ge_x"],
            "validated_buckets": 2,
        }

    def test_skip_when_substitution_unavailable(self) -> None:
        payload = run_stress_transform_engine_v1(
            substitution_engine_v1_payload=None,
            probability_checkpoint_layer_v1_payload=self._checkpoint_payload(),
            stress_model_definition_v1_payload=self._stress_model_payload(
                [{"op": "TARGETED_REMOVAL", "count": 1}]
            ),
            probability_math_core_v1_payload=self._math_core_payload(),
        )

        self.assertEqual(payload.get("version"), STRESS_TRANSFORM_ENGINE_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "SUBSTITUTION_ENGINE_UNAVAILABLE")

    def test_k_reduction_math_targeted_removal(self) -> None:
        payload = run_stress_transform_engine_v1(
            substitution_engine_v1_payload=self._substitution_payload(),
            probability_checkpoint_layer_v1_payload=self._checkpoint_payload(),
            stress_model_definition_v1_payload=self._stress_model_payload(
                [{"op": "TARGETED_REMOVAL", "count": 2}]
            ),
            probability_math_core_v1_payload=self._math_core_payload(),
        )

        self.assertEqual(payload.get("status"), "OK")

        rows = payload.get("stress_adjusted_effective_K") if isinstance(payload.get("stress_adjusted_effective_K"), list) else []
        ramp = rows[1] if rows and isinstance(rows[1], dict) else {}
        self.assertEqual(ramp.get("bucket"), "RAMP")
        self.assertEqual(ramp.get("effective_K_before"), 12.5)
        self.assertEqual(ramp.get("effective_K_after"), 10.5)
        self.assertEqual(ramp.get("K_int_after"), 10)

        probs_rows = payload.get("stress_adjusted_probabilities_by_bucket") if isinstance(payload.get("stress_adjusted_probabilities_by_bucket"), list) else []
        ramp_probs = probs_rows[1].get("probabilities_by_checkpoint") if len(probs_rows) > 1 and isinstance(probs_rows[1], dict) else []
        p_turn6 = ramp_probs[-1].get("p_ge_1") if ramp_probs and isinstance(ramp_probs[-1], dict) else None
        expected = _round6_half_up(hypergeom_p_ge_1(N=99, K_int=10, n=12))
        self.assertEqual(p_turn6, expected)

    def test_wipe_survival_applies_to_k_and_recomputes(self) -> None:
        payload = run_stress_transform_engine_v1(
            substitution_engine_v1_payload=self._substitution_payload(),
            probability_checkpoint_layer_v1_payload=self._checkpoint_payload(),
            stress_model_definition_v1_payload=self._stress_model_payload(
                [{"op": "BOARD_WIPE", "by_turn": 6, "surviving_engine_fraction": 0.5}]
            ),
            probability_math_core_v1_payload=self._math_core_payload(),
        )

        self.assertEqual(payload.get("status"), "OK")
        rows = payload.get("stress_adjusted_effective_K") if isinstance(payload.get("stress_adjusted_effective_K"), list) else []
        ramp = rows[1] if len(rows) > 1 and isinstance(rows[1], dict) else {}
        self.assertEqual(ramp.get("effective_K_after"), 6.25)
        self.assertEqual(ramp.get("K_int_after"), 6)

    def test_graveyard_penalty_applies_to_k_and_recomputes(self) -> None:
        payload = run_stress_transform_engine_v1(
            substitution_engine_v1_payload=self._substitution_payload(),
            probability_checkpoint_layer_v1_payload=self._checkpoint_payload(),
            stress_model_definition_v1_payload=self._stress_model_payload(
                [{"op": "GRAVEYARD_HATE_WINDOW", "turns": [4, 5, 6], "graveyard_penalty": 0.4}]
            ),
            probability_math_core_v1_payload=self._math_core_payload(),
        )

        self.assertEqual(payload.get("status"), "OK")
        rows = payload.get("stress_adjusted_effective_K") if isinstance(payload.get("stress_adjusted_effective_K"), list) else []
        ramp = rows[1] if len(rows) > 1 and isinstance(rows[1], dict) else {}
        self.assertEqual(ramp.get("effective_K_after"), 5.0)
        self.assertEqual(ramp.get("K_int_after"), 5)

    def test_deterministic_operator_ordering(self) -> None:
        operators_unsorted = [
            {"op": "TARGETED_REMOVAL", "count": 1},
            {"op": "STAX_TAX", "by_turn": 4, "inflation_factor": 1.2},
            {"op": "BOARD_WIPE", "by_turn": 6, "surviving_engine_fraction": 0.8},
        ]

        payload = run_stress_transform_engine_v1(
            substitution_engine_v1_payload=self._substitution_payload(),
            probability_checkpoint_layer_v1_payload=self._checkpoint_payload(),
            stress_model_definition_v1_payload=self._stress_model_payload(operators_unsorted),
            probability_math_core_v1_payload=self._math_core_payload(),
        )

        self.assertEqual(payload.get("status"), "OK")

        ordered = payload.get("operators_applied") if isinstance(payload.get("operators_applied"), list) else []
        self.assertEqual(
            [row.get("op") for row in ordered if isinstance(row, dict)],
            ["BOARD_WIPE", "STAX_TAX", "TARGETED_REMOVAL"],
        )

        impacts = payload.get("operator_impacts") if isinstance(payload.get("operator_impacts"), list) else []
        self.assertEqual(
            [
                ((entry.get("operator") or {}).get("op"))
                for entry in impacts
                if isinstance(entry, dict)
            ],
            ["BOARD_WIPE", "STAX_TAX", "TARGETED_REMOVAL"],
        )

    def test_determinism_repeat_call_identical(self) -> None:
        kwargs = {
            "substitution_engine_v1_payload": self._substitution_payload(),
            "probability_checkpoint_layer_v1_payload": self._checkpoint_payload(),
            "stress_model_definition_v1_payload": self._stress_model_payload(
                [
                    {"op": "BOARD_WIPE", "by_turn": 6, "surviving_engine_fraction": 0.6},
                    {"op": "TARGETED_REMOVAL", "count": 2},
                ]
            ),
            "probability_math_core_v1_payload": self._math_core_payload(),
        }

        first = run_stress_transform_engine_v1(**kwargs)
        second = run_stress_transform_engine_v1(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
