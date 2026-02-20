from __future__ import annotations

import os
import sqlite3
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from api.engine.pipeline_build import run_build_pipeline


TEST_SNAPSHOT_ID = "TEST_SNAPSHOT_0001"


class _BuildResponse(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class StructuralPayloadIsolationV1Tests(unittest.TestCase):
    def _build_request(self) -> SimpleNamespace:
        return SimpleNamespace(
            db_snapshot_id=TEST_SNAPSHOT_ID,
            profile_id="focused",
            bracket_id="B2",
            format="commander",
            commander="Missing Commander",
            cards=["Missing Card A", "Missing Card B"],
            engine_patches_v0=[],
        )

    def _find_card_by_name_side_effect(self, snapshot_id: str, name: str) -> dict | None:
        _ = snapshot_id
        if name == "Missing Commander":
            return {
                "name": "Missing Commander",
                "oracle_id": "oracle_missing_commander",
                "color_identity": ["R"],
                "legalities": {"commander": "legal"},
                "type_line": "Legendary Creature - Goblin",
            }
        if name == "Missing Card A":
            return {
                "name": "Missing Card A",
                "oracle_id": "oracle_missing_card_a",
                "color_identity": ["R"],
                "legalities": {"commander": "legal"},
                "type_line": "Sorcery",
            }
        if name == "Missing Card B":
            return {
                "name": "Missing Card B",
                "oracle_id": "oracle_missing_card_b",
                "color_identity": ["R"],
                "legalities": {"commander": "legal"},
                "type_line": "Sorcery",
            }
        return None

    def _run_pipeline(self, *, legacy_structural_enabled: bool) -> dict:
        stub_api_main = types.ModuleType("api.main")
        stub_api_main.BuildResponse = _BuildResponse

        preflight_payload = {
            "version": "snapshot_preflight_v1",
            "snapshot_id": TEST_SNAPSHOT_ID,
            "status": "OK",
            "errors": [],
            "checks": {
                "snapshot_exists": True,
                "manifest_present": True,
                "tags_compiled": True,
                "schema_ok": True,
            },
        }

        env_value = "1" if legacy_structural_enabled else "0"

        with (
            patch.dict(sys.modules, {"api.main": stub_api_main}),
            patch.dict(os.environ, {"ENGINE_ENABLE_LEGACY_STRUCTURAL_V1": env_value}, clear=False),
            patch("api.engine.pipeline_build.cards_db_connect", side_effect=lambda: sqlite3.connect(":memory:")),
            patch("api.engine.pipeline_build.run_snapshot_preflight_v1", return_value=preflight_payload),
            patch("api.engine.pipeline_build.resolve_runtime_taxonomy_version", return_value="taxonomy_v_test"),
            patch("api.engine.pipeline_build.resolve_runtime_ruleset_version", return_value="ruleset_v_test"),
            patch("api.engine.pipeline_build.run_snapshot_preflight", return_value={"status": "OK"}),
            patch("api.engine.pipeline_build.is_legal_commander_card", return_value=(True, "legal")),
            patch("api.engine.pipeline_build.find_card_by_name", side_effect=self._find_card_by_name_side_effect),
            patch("api.engine.pipeline_build.suggest_card_names", return_value=[]),
            patch("api.engine.pipeline_build.ensure_tag_tables", return_value=None),
            patch(
                "api.engine.pipeline_build.bulk_get_card_tags",
                return_value={
                    "oracle_missing_commander": {
                        "primitive_ids": ["MANA_RAMP_ARTIFACT_ROCK"],
                        "ruleset_version": "ruleset_v_test",
                        "evidence": {"matches": []},
                    },
                    "oracle_missing_card_a": {
                        "primitive_ids": ["CARD_DRAW_BURST", "STACK_COUNTERSPELL"],
                        "ruleset_version": "ruleset_v_test",
                        "evidence": {"matches": []},
                    },
                    "oracle_missing_card_b": {
                        "primitive_ids": ["TARGETED_REMOVAL_CREATURE", "BOARDWIPE_CREATURES"],
                        "ruleset_version": "ruleset_v_test",
                        "evidence": {"matches": []},
                    },
                },
            ),
        ):
            return run_build_pipeline(req=self._build_request(), conn=None, repo_root_path=None)

    def test_default_runtime_exposes_structural_snapshot_only(self) -> None:
        payload = self._run_pipeline(legacy_structural_enabled=False)
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}

        self.assertIn("structural_snapshot_v1", result)
        self.assertNotIn("structural_v1", result)

    def test_legacy_structural_payload_requires_explicit_enable(self) -> None:
        payload = self._run_pipeline(legacy_structural_enabled=True)
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}

        self.assertIn("structural_snapshot_v1", result)
        self.assertIn("structural_v1", result)

        unknowns = payload.get("unknowns") if isinstance(payload.get("unknowns"), list) else []
        unknown_codes = [entry.get("code") for entry in unknowns if isinstance(entry, dict)]
        self.assertIn("STRUCTURAL_V1_DEPRECATED", unknown_codes)


if __name__ == "__main__":
    unittest.main()
