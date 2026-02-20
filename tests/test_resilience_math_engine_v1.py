from __future__ import annotations

import unittest

from api.engine.layers.resilience_math_engine_v1 import (
    RESILIENCE_MATH_ENGINE_V1_VERSION,
    run_resilience_math_engine_v1,
)


class ResilienceMathEngineV1Tests(unittest.TestCase):
    def _checkpoint_rows(self, values: tuple[float, float, float, float]) -> list[dict]:
        return [
            {"checkpoint": 7, "p_ge_1": values[0]},
            {"checkpoint": 9, "p_ge_1": values[1]},
            {"checkpoint": 10, "p_ge_1": values[2]},
            {"checkpoint": 12, "p_ge_1": values[3]},
        ]

    def _baseline_payload(self) -> dict:
        return {
            "version": "probability_checkpoint_layer_v1",
            "status": "OK",
            "reason_code": None,
            "codes": [],
            "format": "commander",
            "checkpoints": [7, 9, 10, 12],
            "probabilities_by_bucket": [
                {
                    "bucket": "A_BUCKET",
                    "effective_K": 10.0,
                    "K_int": 10,
                    "probabilities_by_checkpoint": self._checkpoint_rows((0.1, 0.2, 0.3, 0.4)),
                },
                {
                    "bucket": "B_BUCKET",
                    "effective_K": 20.0,
                    "K_int": 20,
                    "probabilities_by_checkpoint": self._checkpoint_rows((0.2, 0.3, 0.4, 0.5)),
                },
            ],
        }

    def _stress_payload(self) -> dict:
        return {
            "version": "stress_transform_engine_v1",
            "status": "OK",
            "reason_code": None,
            "codes": [],
            "format": "commander",
            "checkpoints": [7, 9, 10, 12],
            "stress_adjusted_probabilities_by_bucket": [
                {
                    "bucket": "A_BUCKET",
                    "effective_K_after": 5.0,
                    "K_int_after": 5,
                    "probabilities_by_checkpoint": self._checkpoint_rows((0.05, 0.1, 0.15, 0.2)),
                },
                {
                    "bucket": "B_BUCKET",
                    "effective_K_after": 9.0,
                    "K_int_after": 9,
                    "probabilities_by_checkpoint": self._checkpoint_rows((0.1, 0.15, 0.2, 0.25)),
                },
            ],
            "operator_impacts": [
                {
                    "operator_index": 1,
                    "operator": {
                        "op": "BOARD_WIPE",
                        "by_turn": 6,
                        "surviving_engine_fraction": 0.8,
                    },
                    "bucket_impacts": [
                        {
                            "bucket": "A_BUCKET",
                            "effective_K_before": 10.0,
                            "effective_K_after": 8.0,
                            "probabilities_before": self._checkpoint_rows((0.1, 0.2, 0.3, 0.4)),
                            "probabilities_after": self._checkpoint_rows((0.08, 0.16, 0.24, 0.32)),
                        },
                        {
                            "bucket": "B_BUCKET",
                            "effective_K_before": 20.0,
                            "effective_K_after": 16.0,
                            "probabilities_before": self._checkpoint_rows((0.2, 0.3, 0.4, 0.5)),
                            "probabilities_after": self._checkpoint_rows((0.16, 0.24, 0.32, 0.4)),
                        },
                    ],
                },
                {
                    "operator_index": 2,
                    "operator": {
                        "op": "GRAVEYARD_HATE_WINDOW",
                        "turns": [4, 5, 6],
                        "graveyard_penalty": 0.75,
                    },
                    "bucket_impacts": [
                        {
                            "bucket": "A_BUCKET",
                            "effective_K_before": 8.0,
                            "effective_K_after": 6.0,
                            "probabilities_before": self._checkpoint_rows((0.08, 0.16, 0.24, 0.32)),
                            "probabilities_after": self._checkpoint_rows((0.06, 0.12, 0.18, 0.24)),
                        },
                        {
                            "bucket": "B_BUCKET",
                            "effective_K_before": 16.0,
                            "effective_K_after": 12.0,
                            "probabilities_before": self._checkpoint_rows((0.16, 0.24, 0.32, 0.4)),
                            "probabilities_after": self._checkpoint_rows((0.12, 0.18, 0.24, 0.3)),
                        },
                    ],
                },
                {
                    "operator_index": 3,
                    "operator": {
                        "op": "TARGETED_REMOVAL",
                        "count": 2,
                    },
                    "bucket_impacts": [
                        {
                            "bucket": "A_BUCKET",
                            "effective_K_before": 6.0,
                            "effective_K_after": 5.0,
                            "probabilities_before": self._checkpoint_rows((0.06, 0.12, 0.18, 0.24)),
                            "probabilities_after": self._checkpoint_rows((0.05, 0.1, 0.15, 0.2)),
                        },
                        {
                            "bucket": "B_BUCKET",
                            "effective_K_before": 12.0,
                            "effective_K_after": 9.0,
                            "probabilities_before": self._checkpoint_rows((0.12, 0.18, 0.24, 0.3)),
                            "probabilities_after": self._checkpoint_rows((0.1, 0.15, 0.2, 0.25)),
                        },
                    ],
                },
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

    def test_skip_when_baseline_unavailable(self) -> None:
        payload = run_resilience_math_engine_v1(
            probability_checkpoint_layer_v1_payload=None,
            stress_transform_engine_v1_payload=self._stress_payload(),
            engine_requirement_detection_v1_payload=self._engine_requirements_payload("LOW"),
        )

        self.assertEqual(payload.get("version"), RESILIENCE_MATH_ENGINE_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "PROBABILITY_CHECKPOINT_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])

    def test_skip_when_stress_unavailable(self) -> None:
        payload = run_resilience_math_engine_v1(
            probability_checkpoint_layer_v1_payload=self._baseline_payload(),
            stress_transform_engine_v1_payload=None,
            engine_requirement_detection_v1_payload=self._engine_requirements_payload("LOW"),
        )

        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "STRESS_TRANSFORM_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])

    def test_expected_metric_computation_with_operator_impacts(self) -> None:
        payload = run_resilience_math_engine_v1(
            probability_checkpoint_layer_v1_payload=self._baseline_payload(),
            stress_transform_engine_v1_payload=self._stress_payload(),
            engine_requirement_detection_v1_payload=self._engine_requirements_payload("LOW"),
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("codes"), [])

        metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
        self.assertEqual(metrics.get("engine_continuity_after_removal"), 0.791667)
        self.assertEqual(metrics.get("rebuild_after_wipe"), 0.8)
        self.assertEqual(metrics.get("graveyard_fragility_delta"), 0.06)
        self.assertEqual(metrics.get("commander_fragility_delta"), 0.0)

        bucket_metrics = payload.get("bucket_metrics") if isinstance(payload.get("bucket_metrics"), list) else []
        self.assertEqual([row.get("bucket") for row in bucket_metrics], ["A_BUCKET", "B_BUCKET"])

    def test_warn_when_commander_fragility_unavailable(self) -> None:
        payload = run_resilience_math_engine_v1(
            probability_checkpoint_layer_v1_payload=self._baseline_payload(),
            stress_transform_engine_v1_payload=self._stress_payload(),
            engine_requirement_detection_v1_payload=self._engine_requirements_payload("MED"),
        )

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(payload.get("codes"), ["RESILIENCE_COMMANDER_FRAGILITY_UNAVAILABLE"])

        metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
        self.assertIsNone(metrics.get("commander_fragility_delta"))

    def test_determinism_repeat_call_identical(self) -> None:
        kwargs = {
            "probability_checkpoint_layer_v1_payload": self._baseline_payload(),
            "stress_transform_engine_v1_payload": self._stress_payload(),
            "engine_requirement_detection_v1_payload": self._engine_requirements_payload("LOW"),
        }

        first = run_resilience_math_engine_v1(**kwargs)
        second = run_resilience_math_engine_v1(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
