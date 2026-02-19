from __future__ import annotations

import unittest
from unittest.mock import patch

from api.engine.ui_contract_validator_v1 import validate_build_response_ui_contract_v1

try:
    import api.engine.constants as engine_constants
    from api.main import app
    from engine.db import find_card_by_name, snapshot_exists
    from fastapi.testclient import TestClient

    _IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - environment-dependent dependency loading
    engine_constants = None
    app = None
    find_card_by_name = None
    snapshot_exists = None
    TestClient = None
    _IMPORT_ERROR = exc


class UIContractV1ValidationTests(unittest.TestCase):
    SNAPSHOT_ID = "20260217_190902"
    LEGAL_COMMANDER = "Krenko, Mob Boss"
    ILLEGAL_COMMANDER = '"Ach! Hans, Run!"'

    def _post_build(self, commander: str, allow_runtime_oracle_text: bool) -> dict:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": self.SNAPSHOT_ID,
            "profile_id": "focused",
            "bracket_id": "B2",
            "format": "commander",
            "commander": commander,
            "cards": [],
            "engine_patches_v0": [],
        }
        with patch.object(engine_constants, "ENGINE_ALLOW_RUNTIME_ORACLE_TEXT", allow_runtime_oracle_text):
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.post("/build", json=payload)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIsInstance(body, dict)
        return body

    def _assert_validator_report_shape(self, report: dict) -> None:
        self.assertEqual(
            list(report.keys()),
            [
                "ui_contract_version_detected",
                "available_panels_v1",
                "missing_required_fields",
                "type_mismatches",
                "unknown_top_level_fields",
                "unknown_result_fields",
                "contract_compliance",
                "warnings",
            ],
        )

    def test_legal_commander_build_passes_validator_with_unknown_fields_warn_only(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")
        if not snapshot_exists(self.SNAPSHOT_ID):
            self.skipTest(f"Snapshot not found: {self.SNAPSHOT_ID}")
        if find_card_by_name(self.SNAPSHOT_ID, self.LEGAL_COMMANDER) is None:
            self.skipTest(f"Commander not found in snapshot: {self.LEGAL_COMMANDER}")

        payload = self._post_build(commander=self.LEGAL_COMMANDER, allow_runtime_oracle_text=True)

        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        self.assertEqual(result.get("ui_contract_version"), "ui_contract_v1")
        self.assertIsInstance(result.get("available_panels_v1"), dict)

        report = validate_build_response_ui_contract_v1(payload)
        self._assert_validator_report_shape(report)
        self.assertEqual(report.get("contract_compliance"), "PASS")
        self.assertEqual(report.get("missing_required_fields"), [])
        self.assertEqual(report.get("type_mismatches"), [])
        self.assertGreater(len(report.get("unknown_result_fields") or []), 0)
        warnings = report.get("warnings") if isinstance(report.get("warnings"), list) else []
        self.assertIn("unknown_result_fields present (WARN_ONLY)", warnings)

    def test_tag_only_runtime_skips_oracle_text_required_layer_and_returns_http_200(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")
        if not snapshot_exists(self.SNAPSHOT_ID):
            self.skipTest(f"Snapshot not found: {self.SNAPSHOT_ID}")
        if find_card_by_name(self.SNAPSHOT_ID, self.LEGAL_COMMANDER) is None:
            self.skipTest(f"Commander not found in snapshot: {self.LEGAL_COMMANDER}")

        baseline_payload = self._post_build(commander=self.LEGAL_COMMANDER, allow_runtime_oracle_text=True)
        payload = self._post_build(commander=self.LEGAL_COMMANDER, allow_runtime_oracle_text=False)

        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        self.assertEqual(result.get("ui_contract_version"), "ui_contract_v1")

        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        unknown_codes = [entry.get("code") for entry in unknowns if isinstance(entry, dict)]
        self.assertIn("LAYER_SKIPPED_ORACLE_TEXT_REQUIRED", unknown_codes)

        baseline_result = baseline_payload.get("result") if isinstance(baseline_payload.get("result"), dict) else {}
        if "structural_snapshot_v1" in baseline_result:
            self.assertIn("structural_snapshot_v1", result)
        if "graph_v1" in baseline_result:
            self.assertIn("graph_v1", result)

        available_panels = result.get("available_panels_v1") if isinstance(result.get("available_panels_v1"), dict) else {}
        self.assertIn("has_proof_attempts", available_panels)
        self.assertIs(available_panels.get("has_proof_attempts"), False)

    def test_illegal_commander_build_keeps_minimal_result_envelope_and_passes_validator(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")
        if not snapshot_exists(self.SNAPSHOT_ID):
            self.skipTest(f"Snapshot not found: {self.SNAPSHOT_ID}")
        if find_card_by_name(self.SNAPSHOT_ID, self.ILLEGAL_COMMANDER) is None:
            self.skipTest(f"Commander not found in snapshot: {self.ILLEGAL_COMMANDER}")

        payload = self._post_build(commander=self.ILLEGAL_COMMANDER, allow_runtime_oracle_text=False)

        self.assertEqual(payload.get("status"), "ILLEGAL_COMMANDER")
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        self.assertEqual(result.get("ui_contract_version"), "ui_contract_v1")
        self.assertIn("available_panels_v1", result)
        self.assertEqual(result.get("available_panels_v1"), {})

        report = validate_build_response_ui_contract_v1(payload)
        self._assert_validator_report_shape(report)
        self.assertEqual(report.get("contract_compliance"), "PASS")
        self.assertEqual(report.get("missing_required_fields"), [])
        self.assertEqual(report.get("type_mismatches"), [])


if __name__ == "__main__":
    unittest.main()
