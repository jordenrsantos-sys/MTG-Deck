from __future__ import annotations

import unittest

from api.engine.layers.probability_math_core_v1 import run_probability_math_core_v1
from api.engine.probability_math_core_v1 import (
    PROBABILITY_MATH_CORE_V1_VERSION,
    comb,
    hypergeom_p_ge_1,
    hypergeom_p_ge_x,
)


class ProbabilityMathCoreV1Tests(unittest.TestCase):
    def test_comb_known_values(self) -> None:
        self.assertEqual(comb(5, 2), 10)
        self.assertEqual(comb(6, 3), 20)
        self.assertEqual(comb(10, 0), 1)
        self.assertEqual(comb(10, 10), 1)

    def test_comb_invalid_input_raises_explicit_code(self) -> None:
        with self.assertRaises(RuntimeError) as raised:
            comb(5, 6)
        self.assertIn("PROBABILITY_MATH_CORE_V1_INVALID_INPUT", str(raised.exception))

    def test_hypergeom_edge_cases(self) -> None:
        self.assertEqual(hypergeom_p_ge_1(99, 0, 7), 0.0)
        self.assertEqual(hypergeom_p_ge_1(99, 99, 7), 1.0)
        self.assertEqual(hypergeom_p_ge_1(99, 10, 0), 0.0)
        self.assertEqual(hypergeom_p_ge_1(99, 10, 99), 1.0)
        self.assertEqual(hypergeom_p_ge_x(99, 10, 7, 0), 1.0)
        self.assertEqual(hypergeom_p_ge_x(99, 3, 7, 4), 0.0)

    def test_hypergeom_invalid_input_raises_explicit_code(self) -> None:
        with self.assertRaises(RuntimeError) as raised:
            hypergeom_p_ge_x(99, 5, 4, 6)
        self.assertIn("PROBABILITY_MATH_CORE_V1_INVALID_INPUT", str(raised.exception))

    def test_hypergeom_determinism_repeat_call_identical(self) -> None:
        first = hypergeom_p_ge_x(99, 12, 10, 2)
        second = hypergeom_p_ge_x(99, 12, 10, 2)
        self.assertEqual(first, second)

    def test_layer_skip_when_substitution_unavailable(self) -> None:
        payload = run_probability_math_core_v1(substitution_engine_v1_payload=None)
        self.assertEqual(payload.get("version"), PROBABILITY_MATH_CORE_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "SUBSTITUTION_ENGINE_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("math_backend"), "int_comb")

    def test_layer_error_when_bucket_values_invalid(self) -> None:
        payload = run_probability_math_core_v1(
            substitution_engine_v1_payload={
                "buckets": [
                    {"bucket": "A", "effective_K": "bad", "K_int": 1},
                    {"bucket": "B", "effective_K": 2.0, "K_int": "bad"},
                ]
            }
        )

        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(
            payload.get("codes"),
            [
                "PROBABILITY_MATH_BUCKET_EFFECTIVE_K_INVALID",
                "PROBABILITY_MATH_BUCKET_K_INT_INVALID",
            ],
        )

    def test_layer_error_when_k_int_policy_violated(self) -> None:
        payload = run_probability_math_core_v1(
            substitution_engine_v1_payload={
                "buckets": [
                    {"bucket": "A", "effective_K": 12.9, "K_int": 13},
                ]
            }
        )

        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(payload.get("codes"), ["PROBABILITY_MATH_K_INT_POLICY_VIOLATION"])

    def test_layer_ok_with_valid_substitution_payload(self) -> None:
        payload = run_probability_math_core_v1(
            substitution_engine_v1_payload={
                "buckets": [
                    {"bucket": "A", "effective_K": 12.9, "K_int": 12},
                    {"bucket": "B", "effective_K": -5.0, "K_int": 0},
                    {"bucket": "C", "effective_K": 120.0, "K_int": 99},
                ]
            }
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("reason_code"), None)
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("validated_buckets"), 3)
        self.assertEqual(
            payload.get("available_functions"),
            ["comb", "hypergeom_p_ge_1", "hypergeom_p_ge_x"],
        )

    def test_layer_determinism_repeat_call_identical(self) -> None:
        kwargs = {
            "substitution_engine_v1_payload": {
                "buckets": [
                    {"bucket": "RAMP", "effective_K": 12.5, "K_int": 12},
                    {"bucket": "DRAW", "effective_K": 8.0, "K_int": 8},
                ]
            }
        }

        first = run_probability_math_core_v1(**kwargs)
        second = run_probability_math_core_v1(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
