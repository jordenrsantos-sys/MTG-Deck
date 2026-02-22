from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.stress_operator_policy_v1 import load_stress_operator_policy_v1


class StressOperatorPolicyV1Tests(unittest.TestCase):
    def test_load_returns_normalized_policy(self) -> None:
        payload = load_stress_operator_policy_v1()

        self.assertEqual(payload.get("version"), "stress_operator_policy_v1")
        self.assertEqual(
            payload.get("precedence"),
            [
                "BOARD_WIPE",
                "GRAVEYARD_HATE_WINDOW",
                "STAX_TAX",
                "TARGETED_REMOVAL",
                "WHEEL",
                "HAND_DISRUPTION",
                "COMBAT_PRESSURE",
            ],
        )
        self.assertEqual(payload.get("tie_break"), "op_name_then_json")
        self.assertEqual(
            payload.get("default_by_turn"),
            {
                "COMBAT_PRESSURE": 3,
                "HAND_DISRUPTION": 4,
                "TARGETED_REMOVAL": 4,
                "WHEEL": 5,
            },
        )
        self.assertEqual(
            payload.get("composition"),
            {
                "mode": "sequential",
                "record_impacts": True,
            },
        )

    def test_invalid_unknown_field_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            policy_path = Path(tmp_dir) / "stress_operator_policy_v1.json"
            policy_path.write_text(
                '{"version":"stress_operator_policy_v1","precedence":["TARGETED_REMOVAL"],"tie_break":"op_name_then_json","default_by_turn":{"TARGETED_REMOVAL":4},"composition":{"mode":"sequential","record_impacts":true},"extra":1}',
                encoding="utf-8",
            )

            with patch("api.engine.stress_operator_policy_v1._STRESS_OPERATOR_POLICY_FILE", policy_path):
                with self.assertRaises(RuntimeError) as ctx:
                    load_stress_operator_policy_v1()

        self.assertIn("STRESS_OPERATOR_POLICY_V1_INVALID", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
