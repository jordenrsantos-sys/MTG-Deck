from __future__ import annotations

import unittest

from api.engine.layers.mulligan_model_v1 import (
    MULLIGAN_MODEL_V1_VERSION,
    run_mulligan_model_v1,
)
from api.engine.mulligan_assumptions_v1 import load_mulligan_assumptions_v1


class MulliganModelV1Tests(unittest.TestCase):
    def test_loader_is_deterministic_and_has_required_shape(self) -> None:
        first = load_mulligan_assumptions_v1()
        second = load_mulligan_assumptions_v1()

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), "mulligan_assumptions_v1")

        format_defaults = first.get("format_defaults") if isinstance(first.get("format_defaults"), dict) else {}
        commander_defaults = format_defaults.get("commander") if isinstance(format_defaults.get("commander"), dict) else {}
        self.assertEqual(commander_defaults.get("default_policy"), "NORMAL")

        policies = commander_defaults.get("policies") if isinstance(commander_defaults.get("policies"), dict) else {}
        self.assertEqual(list(policies.keys()), ["DRAW10_SHUFFLE3", "FRIENDLY", "NORMAL"])

        expected_checkpoints = [7, 9, 10, 12]
        for policy in policies.values():
            by_checkpoint = (
                policy.get("effective_n_by_checkpoint")
                if isinstance(policy, dict) and isinstance(policy.get("effective_n_by_checkpoint"), dict)
                else {}
            )
            self.assertEqual(list(by_checkpoint.keys()), expected_checkpoints)

    def test_skip_when_assumptions_payload_unavailable(self) -> None:
        payload = run_mulligan_model_v1(
            format="commander",
            mulligan_assumptions_payload=None,
        )

        self.assertEqual(payload.get("version"), MULLIGAN_MODEL_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "MULLIGAN_ASSUMPTIONS_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("checkpoints"), [7, 9, 10, 12])
        self.assertEqual(payload.get("policy_effective_n"), [])

    def test_skip_when_format_assumptions_missing(self) -> None:
        assumptions = load_mulligan_assumptions_v1()
        payload = run_mulligan_model_v1(
            format="legacy",
            mulligan_assumptions_payload=assumptions,
        )

        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "FORMAT_ASSUMPTIONS_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("policy_effective_n"), [])

    def test_ok_policy_effective_n_order_and_rounding(self) -> None:
        assumptions = {
            "version": "mulligan_assumptions_v1_test",
            "format_defaults": {
                "commander": {
                    "default_policy": "NORMAL",
                    "policies": {
                        "NORMAL": {
                            "effective_n_by_checkpoint": {
                                7: 7.0000001,
                                9: 9.0,
                                10: 10.0,
                                12: 120.0,
                            }
                        },
                        "DRAW10_SHUFFLE3": {
                            "effective_n_by_checkpoint": {
                                "7": -3.0,
                                "9": 9.2500004,
                                "10": 10.25,
                                "12": 12.25,
                            }
                        },
                        "FRIENDLY": {
                            "effective_n_by_checkpoint": {
                                "7": 7.5,
                                "9": 9.5,
                                "10": 10.5,
                                "12": 12.5,
                            }
                        },
                    },
                }
            },
        }

        payload = run_mulligan_model_v1(
            format="commander",
            mulligan_assumptions_payload=assumptions,
        )

        self.assertEqual(payload.get("version"), MULLIGAN_MODEL_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("reason_code"), None)
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("assumptions_version"), "mulligan_assumptions_v1_test")
        self.assertEqual(payload.get("default_policy"), "NORMAL")
        self.assertEqual(payload.get("checkpoints"), [7, 9, 10, 12])

        rows = payload.get("policy_effective_n") if isinstance(payload.get("policy_effective_n"), list) else []
        policies = [row.get("policy") for row in rows if isinstance(row, dict)]
        self.assertEqual(policies, ["DRAW10_SHUFFLE3", "FRIENDLY", "NORMAL"])

        draw10_rows = rows[0].get("effective_n_by_checkpoint") if isinstance(rows[0], dict) else []
        normal_rows = rows[2].get("effective_n_by_checkpoint") if isinstance(rows[2], dict) else []

        self.assertEqual(draw10_rows[0], {"checkpoint": 7, "effective_n": 0.0})
        self.assertEqual(draw10_rows[1], {"checkpoint": 9, "effective_n": 9.25})
        self.assertEqual(normal_rows[0], {"checkpoint": 7, "effective_n": 7.0})
        self.assertEqual(normal_rows[3], {"checkpoint": 12, "effective_n": 99.0})

    def test_determinism_repeated_call_identical(self) -> None:
        assumptions = load_mulligan_assumptions_v1()

        kwargs = {
            "format": "commander",
            "mulligan_assumptions_payload": assumptions,
        }

        first = run_mulligan_model_v1(**kwargs)
        second = run_mulligan_model_v1(**kwargs)

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
