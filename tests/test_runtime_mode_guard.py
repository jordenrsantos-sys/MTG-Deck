from __future__ import annotations

import os
import sqlite3
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import api.engine.constants as engine_constants
from api.engine.pipeline_build import run_build_pipeline
from api.engine.runtime_mode_guard import assert_runtime_safe_mode


class _BuildResponse(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class RuntimeModeGuardTests(unittest.TestCase):
    def _request(self) -> SimpleNamespace:
        return SimpleNamespace(
            db_snapshot_id="TEST_SNAPSHOT_0001",
            profile_id="focused",
            bracket_id="B2",
            format="commander",
            commander="Missing Commander",
            cards=[],
            engine_patches_v0=[],
        )

    def test_normal_mode_allows_pipeline_entry(self) -> None:
        preflight_payload = {
            "version": "snapshot_preflight_v1",
            "snapshot_id": "TEST_SNAPSHOT_0001",
            "status": "ERROR",
            "errors": [{"code": "SNAPSHOT_MANIFEST_MISSING", "message": "missing manifest"}],
            "checks": {
                "snapshot_exists": True,
                "manifest_present": False,
                "tags_compiled": None,
                "schema_ok": True,
            },
        }

        stub_api_main = types.ModuleType("api.main")
        stub_api_main.BuildResponse = _BuildResponse

        with (
            patch.dict(
                os.environ,
                {
                    "ENGINE_ENABLE_PROOF": "0",
                    "ENGINE_ENABLE_ORACLE_RUNTIME": "0",
                    "ENGINE_ALLOW_RUNTIME_ORACLE_TEXT": "0",
                },
                clear=False,
            ),
            patch.object(engine_constants, "ENGINE_ALLOW_RUNTIME_ORACLE_TEXT", False),
            patch.dict(sys.modules, {"api.main": stub_api_main}),
            patch("api.engine.pipeline_build.cards_db_connect", side_effect=lambda: sqlite3.connect(":memory:")),
            patch("api.engine.pipeline_build.run_snapshot_preflight_v1", return_value=preflight_payload),
        ):
            payload = run_build_pipeline(req=self._request(), conn=None, repo_root_path=None)

        self.assertEqual(payload.get("status"), "ERROR")
        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        first_unknown = unknowns[0] if unknowns and isinstance(unknowns[0], dict) else {}
        self.assertEqual(first_unknown.get("code"), "SNAPSHOT_PREFLIGHT_ERROR")

    def test_guard_raises_when_proof_env_var_enabled(self) -> None:
        with (
            patch.dict(os.environ, {"ENGINE_ENABLE_PROOF": "1"}, clear=False),
            patch.object(engine_constants, "ENGINE_ALLOW_RUNTIME_ORACLE_TEXT", False),
        ):
            with self.assertRaisesRegex(RuntimeError, "RUNTIME_SAFE_MODE_VIOLATION: ENGINE_ENABLE_PROOF"):
                run_build_pipeline(req=self._request(), conn=None, repo_root_path=None)

    def test_guard_raises_when_runtime_oracle_text_flag_enabled(self) -> None:
        with patch.object(engine_constants, "ENGINE_ALLOW_RUNTIME_ORACLE_TEXT", True):
            with self.assertRaisesRegex(RuntimeError, "RUNTIME_SAFE_MODE_VIOLATION: ENGINE_ALLOW_RUNTIME_ORACLE_TEXT_CONST"):
                assert_runtime_safe_mode()


if __name__ == "__main__":
    unittest.main()
