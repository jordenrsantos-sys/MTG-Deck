from __future__ import annotations

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


class PipelineGraphBoundsPolicyV1Tests(unittest.TestCase):
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

    def test_pipeline_uses_loaded_graph_bounds_policy(self) -> None:
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

        custom_bounds_policy = {
            "version": "graph_bounds_policy_v1_test",
            "bounds": {
                "MAX_PRIMS_PER_SLOT": 2,
                "MAX_SLOTS_PER_PRIM": 2,
                "MAX_CARD_CARD_EDGES_TOTAL": 1,
            },
        }

        with (
            patch.dict(sys.modules, {"api.main": stub_api_main}),
            patch("api.engine.pipeline_build.cards_db_connect", side_effect=lambda: sqlite3.connect(":memory:")),
            patch("api.engine.pipeline_build.run_snapshot_preflight_v1", return_value=preflight_payload),
            patch("api.engine.pipeline_build.resolve_runtime_taxonomy_version", return_value="taxonomy_v_test"),
            patch("api.engine.pipeline_build.resolve_runtime_ruleset_version", return_value="ruleset_v_test"),
            patch("api.engine.pipeline_build.run_snapshot_preflight", return_value={"status": "OK"}),
            patch("api.engine.pipeline_build.is_legal_commander_card", return_value=(True, "legal")),
            patch("api.engine.pipeline_build.find_card_by_name", side_effect=self._find_card_by_name_side_effect),
            patch("api.engine.pipeline_build.suggest_card_names", return_value=[]),
            patch("api.engine.pipeline_build.ensure_tag_tables", return_value=None),
            patch("api.engine.pipeline_build.load_graph_bounds_policy_v1", return_value=custom_bounds_policy),
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
            payload = run_build_pipeline(req=self._build_request(), conn=None, repo_root_path=None)

        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        graph_v1 = result.get("graph_v1") if isinstance(result.get("graph_v1"), dict) else {}
        self.assertEqual(graph_v1.get("bounds"), custom_bounds_policy["bounds"])

        pipeline_versions = result.get("pipeline_versions") if isinstance(result.get("pipeline_versions"), dict) else {}
        self.assertEqual(
            pipeline_versions.get("graph_bounds_policy_version"),
            "graph_bounds_policy_v1_test",
        )


if __name__ == "__main__":
    unittest.main()
