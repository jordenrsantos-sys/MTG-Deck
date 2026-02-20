from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from api.engine.layers.weight_multiplier_engine_v1 import (
    WEIGHT_MULTIPLIER_ENGINE_V1_VERSION,
    run_weight_multiplier_engine_v1,
)
from api.engine.weight_rules_v1 import load_weight_rules_v1


class WeightMultiplierEngineV1Tests(unittest.TestCase):
    def test_loader_is_deterministic_and_sorted(self) -> None:
        first = load_weight_rules_v1()
        second = load_weight_rules_v1()

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), "weight_rules_v1")

        format_defaults = first.get("format_defaults") if isinstance(first.get("format_defaults"), dict) else {}
        commander_defaults = format_defaults.get("commander") if isinstance(format_defaults.get("commander"), dict) else {}
        rules = commander_defaults.get("rules") if isinstance(commander_defaults.get("rules"), list) else []

        sorted_rules = sorted(
            rules,
            key=lambda entry: (
                str((entry or {}).get("target_bucket") or ""),
                str((entry or {}).get("rule_id") or ""),
            ),
        )
        self.assertEqual(rules, sorted_rules)

    def test_loader_missing_file_raises_explicit_error_code(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "weight_rules_v1.json"
            with patch("api.engine.weight_rules_v1._WEIGHT_RULES_FILE", missing_path):
                with self.assertRaises(RuntimeError) as raised:
                    load_weight_rules_v1()

        self.assertIn("WEIGHT_RULES_V1_MISSING", str(raised.exception))

    def test_loader_invalid_payload_raises_explicit_error_code(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            invalid_path = Path(tmp_dir) / "weight_rules_v1.json"
            invalid_path.write_text("[]", encoding="utf-8")

            with patch("api.engine.weight_rules_v1._WEIGHT_RULES_FILE", invalid_path):
                with self.assertRaises(RuntimeError) as raised:
                    load_weight_rules_v1()

        self.assertIn("WEIGHT_RULES_V1_INVALID", str(raised.exception))

    def test_skip_when_weight_rules_payload_unavailable(self) -> None:
        payload = run_weight_multiplier_engine_v1(
            engine_requirement_detection_v1_payload={"engine_requirements_v1": {}},
            substitution_engine_v1_payload={"buckets": []},
            format="commander",
            weight_rules_payload=None,
        )

        self.assertEqual(payload.get("version"), WEIGHT_MULTIPLIER_ENGINE_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "WEIGHT_RULES_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])

    def test_skip_when_format_rules_missing(self) -> None:
        rules_payload = load_weight_rules_v1()
        payload = run_weight_multiplier_engine_v1(
            engine_requirement_detection_v1_payload={"engine_requirements_v1": {}},
            substitution_engine_v1_payload={"buckets": []},
            format="legacy",
            weight_rules_payload=rules_payload,
        )

        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "FORMAT_WEIGHT_RULES_UNAVAILABLE")

    def test_ok_evaluation_stacking_defaults_and_rounding(self) -> None:
        payload = run_weight_multiplier_engine_v1(
            engine_requirement_detection_v1_payload={
                "engine_requirements_v1": {
                    "FLAG_A": True,
                    "FLAG_B": True,
                    "FLAG_C": False,
                }
            },
            substitution_engine_v1_payload={
                "buckets": [
                    {"bucket": "RAMP"},
                    {"bucket": "CARD_DRAW"},
                    {"bucket": "INTERACTION"},
                ]
            },
            format="commander",
            weight_rules_payload={
                "version": "weight_rules_v1_test",
                "format_defaults": {
                    "commander": {
                        "rules": [
                            {
                                "rule_id": "card_draw_flag_a",
                                "target_bucket": "CARD_DRAW",
                                "requirement_flag": "FLAG_A",
                                "multiplier": 1.111111,
                            },
                            {
                                "rule_id": "card_draw_flag_b",
                                "target_bucket": "CARD_DRAW",
                                "requirement_flag": "FLAG_B",
                                "multiplier": 1.111111,
                            },
                            {
                                "rule_id": "ramp_flag_c",
                                "target_bucket": "RAMP",
                                "requirement_flag": "FLAG_C",
                                "multiplier": 0.5,
                            },
                        ]
                    }
                },
            },
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("reason_code"), None)
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(payload.get("weight_rules_version"), "weight_rules_v1_test")

        self.assertEqual(
            payload.get("multipliers_by_bucket"),
            [
                {"bucket": "CARD_DRAW", "multiplier": 1.234568},
                {"bucket": "INTERACTION", "multiplier": 1.0},
                {"bucket": "RAMP", "multiplier": 1.0},
            ],
        )
        self.assertEqual(
            payload.get("applied_rules"),
            [
                {
                    "rule_id": "card_draw_flag_a",
                    "target_bucket": "CARD_DRAW",
                    "requirement_flag": "FLAG_A",
                    "multiplier": 1.111111,
                },
                {
                    "rule_id": "card_draw_flag_b",
                    "target_bucket": "CARD_DRAW",
                    "requirement_flag": "FLAG_B",
                    "multiplier": 1.111111,
                },
            ],
        )

    def test_warn_when_engine_requirements_missing_or_flag_unavailable(self) -> None:
        payload = run_weight_multiplier_engine_v1(
            engine_requirement_detection_v1_payload={},
            substitution_engine_v1_payload={"buckets": [{"bucket": "RAMP"}]},
            format="commander",
            weight_rules_payload={
                "version": "weight_rules_v1_test",
                "format_defaults": {
                    "commander": {
                        "rules": [
                            {
                                "rule_id": "ramp_missing_flag",
                                "target_bucket": "RAMP",
                                "requirement_flag": "FLAG_MISSING",
                                "multiplier": 1.1,
                            }
                        ]
                    }
                },
            },
        )

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(
            payload.get("codes"),
            [
                "ENGINE_REQUIREMENTS_UNAVAILABLE",
                "WEIGHT_RULE_REQUIREMENT_FLAG_UNAVAILABLE",
            ],
        )

    def test_determinism_repeat_call_identical(self) -> None:
        rules_payload = load_weight_rules_v1()
        kwargs = {
            "engine_requirement_detection_v1_payload": {
                "engine_requirements_v1": {
                    "GRAVEYARD_DEPENDENT": True,
                    "SAC_BASED": True,
                    "LANDFALL_BASED": False,
                    "TOKEN_BASED": False,
                    "SPELL_DENSITY_BASED": True,
                }
            },
            "substitution_engine_v1_payload": {
                "buckets": [
                    {"bucket": "RAMP"},
                    {"bucket": "INTERACTION"},
                    {"bucket": "CARD_DRAW"},
                ]
            },
            "format": "commander",
            "weight_rules_payload": rules_payload,
        }

        first = run_weight_multiplier_engine_v1(**kwargs)
        second = run_weight_multiplier_engine_v1(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
