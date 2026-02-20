from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from api.engine.layers.stress_model_definition_v1 import (
    STRESS_MODEL_DEFINITION_V1_VERSION,
    run_stress_model_definition_v1,
)
from api.engine.stress_models_v1 import load_stress_models_v1


class StressModelDefinitionV1Tests(unittest.TestCase):
    def test_loader_is_deterministic_and_sorted(self) -> None:
        first = load_stress_models_v1()
        second = load_stress_models_v1()

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), "stress_models_v1")

        format_defaults = first.get("format_defaults") if isinstance(first.get("format_defaults"), dict) else {}
        commander_defaults = format_defaults.get("commander") if isinstance(format_defaults.get("commander"), dict) else {}

        models = commander_defaults.get("models") if isinstance(commander_defaults.get("models"), dict) else {}
        self.assertEqual(list(models.keys()), ["LIGHT_DISRUPTION_V0", "PUNISHING_POD_V0"])

        punishing = models.get("PUNISHING_POD_V0") if isinstance(models.get("PUNISHING_POD_V0"), dict) else {}
        operators = punishing.get("operators") if isinstance(punishing.get("operators"), list) else []
        self.assertEqual(
            [row.get("op") for row in operators if isinstance(row, dict)],
            ["BOARD_WIPE", "GRAVEYARD_HATE_WINDOW", "STAX_TAX", "TARGETED_REMOVAL"],
        )

    def test_loader_missing_file_raises_explicit_error_code(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "stress_models_v1.json"
            with patch("api.engine.stress_models_v1._STRESS_MODELS_FILE", missing_path):
                with self.assertRaises(RuntimeError) as raised:
                    load_stress_models_v1()

        self.assertIn("STRESS_MODELS_V1_MISSING", str(raised.exception))

    def test_loader_invalid_json_raises_explicit_error_code(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            invalid_path = Path(tmp_dir) / "stress_models_v1.json"
            invalid_path.write_text("{", encoding="utf-8")

            with patch("api.engine.stress_models_v1._STRESS_MODELS_FILE", invalid_path):
                with self.assertRaises(RuntimeError) as raised:
                    load_stress_models_v1()

        self.assertIn("STRESS_MODELS_V1_INVALID_JSON", str(raised.exception))

    def test_loader_invalid_payload_raises_explicit_error_code(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            invalid_path = Path(tmp_dir) / "stress_models_v1.json"
            invalid_path.write_text("[]", encoding="utf-8")

            with patch("api.engine.stress_models_v1._STRESS_MODELS_FILE", invalid_path):
                with self.assertRaises(RuntimeError) as raised:
                    load_stress_models_v1()

        self.assertIn("STRESS_MODELS_V1_INVALID", str(raised.exception))

    def test_skip_when_stress_models_payload_unavailable(self) -> None:
        payload = run_stress_model_definition_v1(
            format="commander",
            bracket_id="B2",
            profile_id="focused",
            request_override_model_id=None,
            stress_models_payload=None,
        )

        self.assertEqual(payload.get("version"), STRESS_MODEL_DEFINITION_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "STRESS_MODELS_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])

    def test_skip_when_format_stress_modeling_unavailable(self) -> None:
        payload = run_stress_model_definition_v1(
            format="legacy",
            bracket_id="B2",
            profile_id="focused",
            request_override_model_id=None,
            stress_models_payload=load_stress_models_v1(),
        )

        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "FORMAT_STRESS_MODELING_UNAVAILABLE")

    def test_skip_when_model_selection_unavailable(self) -> None:
        payload = run_stress_model_definition_v1(
            format="commander",
            bracket_id="B2",
            profile_id="focused",
            request_override_model_id=None,
            stress_models_payload={
                "version": "stress_models_v1_test",
                "format_defaults": {
                    "commander": {
                        "selection": {
                            "default_model_id": None,
                            "by_profile_id": {},
                            "by_bracket_id": {},
                            "by_profile_bracket": [],
                        },
                        "models": {
                            "PUNISHING_POD_V0": {
                                "operators": [{"op": "TARGETED_REMOVAL", "count": 2}],
                            }
                        },
                    }
                },
            },
        )

        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "STRESS_MODEL_SELECTION_UNAVAILABLE")

    def test_ok_profile_bracket_selection_and_operator_ordering(self) -> None:
        payload = run_stress_model_definition_v1(
            format="commander",
            bracket_id="B2",
            profile_id="focused",
            request_override_model_id=None,
            stress_models_payload=load_stress_models_v1(),
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("selected_model_id"), "PUNISHING_POD_V0")
        self.assertEqual(payload.get("selection_source"), "profile_bracket")

        operators = payload.get("operators") if isinstance(payload.get("operators"), list) else []
        self.assertEqual(
            [row.get("op") for row in operators if isinstance(row, dict)],
            ["BOARD_WIPE", "GRAVEYARD_HATE_WINDOW", "STAX_TAX", "TARGETED_REMOVAL"],
        )

    def test_warn_override_unknown_falls_back(self) -> None:
        payload = run_stress_model_definition_v1(
            format="commander",
            bracket_id="B2",
            profile_id="focused",
            request_override_model_id="UNKNOWN_MODEL",
            stress_models_payload=load_stress_models_v1(),
        )

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(payload.get("codes"), ["STRESS_MODEL_OVERRIDE_UNKNOWN"])
        self.assertEqual(payload.get("selected_model_id"), "PUNISHING_POD_V0")

    def test_ok_override_wins_selection(self) -> None:
        payload = run_stress_model_definition_v1(
            format="commander",
            bracket_id="B2",
            profile_id="focused",
            request_override_model_id="LIGHT_DISRUPTION_V0",
            stress_models_payload=load_stress_models_v1(),
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("selected_model_id"), "LIGHT_DISRUPTION_V0")
        self.assertEqual(payload.get("selection_source"), "override")

    def test_error_when_selected_model_id_invalid(self) -> None:
        payload = run_stress_model_definition_v1(
            format="commander",
            bracket_id="B2",
            profile_id="focused",
            request_override_model_id=None,
            stress_models_payload={
                "version": "stress_models_v1_test",
                "format_defaults": {
                    "commander": {
                        "selection": {
                            "default_model_id": "MISSING_MODEL",
                            "by_profile_id": {},
                            "by_bracket_id": {},
                            "by_profile_bracket": [],
                        },
                        "models": {
                            "PUNISHING_POD_V0": {
                                "operators": [{"op": "TARGETED_REMOVAL", "count": 2}],
                            }
                        },
                    }
                },
            },
        )

        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(payload.get("codes"), ["STRESS_MODEL_SELECTED_ID_INVALID"])

    def test_error_when_operator_payload_invalid(self) -> None:
        payload = run_stress_model_definition_v1(
            format="commander",
            bracket_id="B2",
            profile_id="focused",
            request_override_model_id=None,
            stress_models_payload={
                "version": "stress_models_v1_test",
                "format_defaults": {
                    "commander": {
                        "selection": {
                            "default_model_id": "PUNISHING_POD_V0",
                            "by_profile_id": {},
                            "by_bracket_id": {},
                            "by_profile_bracket": [],
                        },
                        "models": {
                            "PUNISHING_POD_V0": {
                                "operators": [{"op": "TARGETED_REMOVAL", "count": -1}],
                            }
                        },
                    }
                },
            },
        )

        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(payload.get("codes"), ["STRESS_MODEL_PAYLOAD_INVALID"])

    def test_determinism_repeat_call_identical(self) -> None:
        kwargs = {
            "format": "commander",
            "bracket_id": "B2",
            "profile_id": "focused",
            "request_override_model_id": None,
            "stress_models_payload": load_stress_models_v1(),
        }

        first = run_stress_model_definition_v1(**kwargs)
        second = run_stress_model_definition_v1(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
