from __future__ import annotations

import unittest

from api.engine.layers.probability_checkpoint_layer_v1 import (
    PROBABILITY_CHECKPOINT_LAYER_V1_VERSION,
    run_probability_checkpoint_layer_v1,
)


class ProbabilityCheckpointLayerV1Tests(unittest.TestCase):
    def _mulligan_payload(
        self,
        *,
        default_policy: str = "NORMAL",
        normal: tuple[float, float, float, float] = (7.0, 9.0, 10.0, 12.0),
        friendly: tuple[float, float, float, float] = (7.5, 9.5, 10.5, 12.5),
        draw10_shuffle3: tuple[float, float, float, float] = (7.25, 9.25, 10.25, 12.25),
    ) -> dict:
        return {
            "version": "mulligan_model_v1",
            "status": "OK",
            "reason_code": None,
            "codes": [],
            "assumptions_version": "mulligan_assumptions_v1",
            "format": "commander",
            "default_policy": default_policy,
            "checkpoints": [7, 9, 10, 12],
            "policy_effective_n": [
                {
                    "policy": "DRAW10_SHUFFLE3",
                    "effective_n_by_checkpoint": [
                        {"checkpoint": 7, "effective_n": draw10_shuffle3[0]},
                        {"checkpoint": 9, "effective_n": draw10_shuffle3[1]},
                        {"checkpoint": 10, "effective_n": draw10_shuffle3[2]},
                        {"checkpoint": 12, "effective_n": draw10_shuffle3[3]},
                    ],
                },
                {
                    "policy": "FRIENDLY",
                    "effective_n_by_checkpoint": [
                        {"checkpoint": 7, "effective_n": friendly[0]},
                        {"checkpoint": 9, "effective_n": friendly[1]},
                        {"checkpoint": 10, "effective_n": friendly[2]},
                        {"checkpoint": 12, "effective_n": friendly[3]},
                    ],
                },
                {
                    "policy": "NORMAL",
                    "effective_n_by_checkpoint": [
                        {"checkpoint": 7, "effective_n": normal[0]},
                        {"checkpoint": 9, "effective_n": normal[1]},
                        {"checkpoint": 10, "effective_n": normal[2]},
                        {"checkpoint": 12, "effective_n": normal[3]},
                    ],
                },
            ],
        }

    def test_skip_when_substitution_unavailable(self) -> None:
        payload = run_probability_checkpoint_layer_v1(
            format="commander",
            substitution_engine_v1_payload=None,
            mulligan_model_v1_payload=self._mulligan_payload(),
        )

        self.assertEqual(payload.get("version"), PROBABILITY_CHECKPOINT_LAYER_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "SUBSTITUTION_ENGINE_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("checkpoint_draws"), [])
        self.assertEqual(payload.get("probabilities_by_bucket"), [])

    def test_skip_when_mulligan_model_unavailable(self) -> None:
        payload = run_probability_checkpoint_layer_v1(
            format="commander",
            substitution_engine_v1_payload={
                "buckets": [
                    {"bucket": "RAMP", "effective_K": 10.0, "K_int": 10},
                ]
            },
            mulligan_model_v1_payload=None,
        )

        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "MULLIGAN_MODEL_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])

    def test_error_when_bucket_values_invalid(self) -> None:
        payload = run_probability_checkpoint_layer_v1(
            format="commander",
            substitution_engine_v1_payload={
                "buckets": [
                    {"bucket": "A", "effective_K": "bad", "K_int": 1},
                    {"bucket": "B", "effective_K": 2.0, "K_int": "bad"},
                ]
            },
            mulligan_model_v1_payload=self._mulligan_payload(),
        )

        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(
            payload.get("codes"),
            [
                "PROBABILITY_CHECKPOINT_BUCKET_EFFECTIVE_K_INVALID",
                "PROBABILITY_CHECKPOINT_BUCKET_K_INT_INVALID",
            ],
        )

    def test_error_when_k_int_policy_violated(self) -> None:
        payload = run_probability_checkpoint_layer_v1(
            format="commander",
            substitution_engine_v1_payload={
                "buckets": [
                    {"bucket": "A", "effective_K": 12.9, "K_int": 13},
                ]
            },
            mulligan_model_v1_payload=self._mulligan_payload(),
        )

        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(payload.get("codes"), ["PROBABILITY_CHECKPOINT_K_INT_POLICY_VIOLATION"])

    def test_warn_when_effective_n_is_floored(self) -> None:
        payload = run_probability_checkpoint_layer_v1(
            format="commander",
            substitution_engine_v1_payload={
                "buckets": [
                    {"bucket": "RAMP", "effective_K": 12.0, "K_int": 12},
                ]
            },
            mulligan_model_v1_payload=self._mulligan_payload(
                default_policy="FRIENDLY",
                friendly=(7.5, 9.5, 10.5, 12.5),
            ),
        )

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(payload.get("codes"), ["PROBABILITY_CHECKPOINT_EFFECTIVE_N_FLOORED"])

        draws = payload.get("checkpoint_draws") if isinstance(payload.get("checkpoint_draws"), list) else []
        self.assertEqual(draws[0], {"checkpoint": 7, "effective_n": 7.5, "n_int": 7})

    def test_known_edge_probabilities_for_k_zero_and_k_n(self) -> None:
        payload = run_probability_checkpoint_layer_v1(
            format="commander",
            substitution_engine_v1_payload={
                "buckets": [
                    {"bucket": "ALWAYS", "effective_K": 99.0, "K_int": 99},
                    {"bucket": "NEVER", "effective_K": 0.0, "K_int": 0},
                ]
            },
            mulligan_model_v1_payload=self._mulligan_payload(),
        )

        self.assertEqual(payload.get("status"), "OK")
        rows = payload.get("probabilities_by_bucket") if isinstance(payload.get("probabilities_by_bucket"), list) else []

        always_rows = rows[0].get("probabilities_by_checkpoint") if isinstance(rows[0], dict) else []
        never_rows = rows[1].get("probabilities_by_checkpoint") if isinstance(rows[1], dict) else []

        self.assertEqual([row.get("p_ge_1") for row in always_rows], [1.0, 1.0, 1.0, 1.0])
        self.assertEqual([row.get("p_ge_1") for row in never_rows], [0.0, 0.0, 0.0, 0.0])

    def test_mulligan_default_policy_changes_selected_draws(self) -> None:
        substitution_payload = {
            "buckets": [
                {"bucket": "RAMP", "effective_K": 1.0, "K_int": 1},
            ]
        }

        normal_payload = run_probability_checkpoint_layer_v1(
            format="commander",
            substitution_engine_v1_payload=substitution_payload,
            mulligan_model_v1_payload=self._mulligan_payload(
                default_policy="NORMAL",
                normal=(7.0, 9.0, 10.0, 12.0),
                friendly=(7.0, 9.0, 10.0, 20.0),
            ),
        )
        friendly_payload = run_probability_checkpoint_layer_v1(
            format="commander",
            substitution_engine_v1_payload=substitution_payload,
            mulligan_model_v1_payload=self._mulligan_payload(
                default_policy="FRIENDLY",
                normal=(7.0, 9.0, 10.0, 12.0),
                friendly=(7.0, 9.0, 10.0, 20.0),
            ),
        )

        normal_draws = normal_payload.get("checkpoint_draws") if isinstance(normal_payload.get("checkpoint_draws"), list) else []
        friendly_draws = friendly_payload.get("checkpoint_draws") if isinstance(friendly_payload.get("checkpoint_draws"), list) else []

        self.assertEqual(normal_draws[-1], {"checkpoint": 12, "effective_n": 12.0, "n_int": 12})
        self.assertEqual(friendly_draws[-1], {"checkpoint": 12, "effective_n": 20.0, "n_int": 20})

    def test_output_ordering_and_determinism(self) -> None:
        kwargs = {
            "format": "commander",
            "substitution_engine_v1_payload": {
                "buckets": [
                    {"bucket": "Z_BUCKET", "effective_K": 8.0, "K_int": 8},
                    {"bucket": "A_BUCKET", "effective_K": 12.0, "K_int": 12},
                ]
            },
            "mulligan_model_v1_payload": self._mulligan_payload(),
        }

        first = run_probability_checkpoint_layer_v1(**kwargs)
        second = run_probability_checkpoint_layer_v1(**kwargs)
        self.assertEqual(first, second)

        rows = first.get("probabilities_by_bucket") if isinstance(first.get("probabilities_by_bucket"), list) else []
        self.assertEqual([row.get("bucket") for row in rows], ["A_BUCKET", "Z_BUCKET"])

        first_bucket_rows = rows[0].get("probabilities_by_checkpoint") if isinstance(rows[0], dict) else []
        self.assertEqual([row.get("checkpoint") for row in first_bucket_rows], [7, 9, 10, 12])


if __name__ == "__main__":
    unittest.main()
