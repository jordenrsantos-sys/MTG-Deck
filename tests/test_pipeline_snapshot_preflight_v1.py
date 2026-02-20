from __future__ import annotations

import sqlite3
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from api.engine.pipeline_build import run_build_pipeline


class _BuildResponse(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class PipelineSnapshotPreflightV1Tests(unittest.TestCase):
    def _build_request(self, commander: str) -> SimpleNamespace:
        return SimpleNamespace(
            db_snapshot_id="TEST_SNAPSHOT_0001",
            profile_id="focused",
            bracket_id="B2",
            format="commander",
            commander=commander,
            cards=[],
            engine_patches_v0=[],
        )

    def test_preflight_error_short_circuits_before_downstream_pipeline(self) -> None:
        preflight_payload = {
            "version": "snapshot_preflight_v1",
            "snapshot_id": "TEST_SNAPSHOT_0001",
            "status": "ERROR",
            "errors": [
                {
                    "code": "SNAPSHOT_MANIFEST_MISSING",
                    "message": "Snapshot manifest_json is missing or empty.",
                }
            ],
            "checks": {
                "snapshot_exists": True,
                "manifest_present": False,
                "tags_compiled": None,
                "schema_ok": True,
            },
        }

        preflight_con = sqlite3.connect(":memory:")
        preflight_con.row_factory = sqlite3.Row
        stub_api_main = types.ModuleType("api.main")
        stub_api_main.BuildResponse = _BuildResponse
        try:
            with (
                patch.dict(sys.modules, {"api.main": stub_api_main}),
                patch("api.engine.pipeline_build.cards_db_connect", return_value=preflight_con),
                patch("api.engine.pipeline_build.run_snapshot_preflight_v1", return_value=preflight_payload),
                patch(
                    "api.engine.pipeline_build.resolve_runtime_taxonomy_version",
                    side_effect=AssertionError("downstream version resolution must not run on preflight ERROR"),
                ),
            ):
                payload = run_build_pipeline(req=self._build_request(commander="Krenko, Mob Boss"), conn=None, repo_root_path=None)
        finally:
            preflight_con.close()

        self.assertEqual(payload.get("status"), "ERROR")
        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        self.assertEqual(unknowns[0].get("code") if unknowns and isinstance(unknowns[0], dict) else None, "SNAPSHOT_PREFLIGHT_ERROR")

        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        self.assertEqual(result.get("snapshot_preflight_v1"), preflight_payload)

        available_panels = result.get("available_panels_v1") if isinstance(result.get("available_panels_v1"), dict) else {}
        self.assertIs(available_panels.get("has_snapshot_preflight_v1"), True)

        self.assertNotIn("graph_v1", result)
        self.assertNotIn("structural_snapshot_v1", result)

    def test_preflight_payload_is_present_in_early_nonfatal_response_envelope(self) -> None:
        preflight_payload = {
            "version": "snapshot_preflight_v1",
            "snapshot_id": "TEST_SNAPSHOT_0001",
            "status": "OK",
            "errors": [],
            "checks": {
                "snapshot_exists": True,
                "manifest_present": True,
                "tags_compiled": True,
                "schema_ok": True,
            },
        }

        preflight_con = sqlite3.connect(":memory:")
        preflight_con.row_factory = sqlite3.Row
        stub_api_main = types.ModuleType("api.main")
        stub_api_main.BuildResponse = _BuildResponse
        try:
            with (
                patch.dict(sys.modules, {"api.main": stub_api_main}),
                patch("api.engine.pipeline_build.cards_db_connect", return_value=preflight_con),
                patch("api.engine.pipeline_build.run_snapshot_preflight_v1", return_value=preflight_payload),
                patch("api.engine.pipeline_build.resolve_runtime_taxonomy_version", return_value="taxonomy_v_test"),
                patch("api.engine.pipeline_build.resolve_runtime_ruleset_version", return_value="ruleset_v_test"),
                patch("api.engine.pipeline_build.find_card_by_name", return_value=None),
                patch("api.engine.pipeline_build.suggest_card_names", return_value=[]),
            ):
                payload = run_build_pipeline(req=self._build_request(commander="Missing Commander"), conn=None, repo_root_path=None)
        finally:
            preflight_con.close()

        self.assertEqual(payload.get("status"), "UNKNOWN_COMMANDER")

        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        self.assertEqual(result.get("snapshot_preflight_v1"), preflight_payload)

        available_panels = result.get("available_panels_v1") if isinstance(result.get("available_panels_v1"), dict) else {}
        self.assertIs(available_panels.get("has_snapshot_preflight_v1"), True)


if __name__ == "__main__":
    unittest.main()
