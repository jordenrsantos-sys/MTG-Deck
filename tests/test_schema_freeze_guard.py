from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from api.engine.pipeline_build import run_build_pipeline


TEST_SNAPSHOT_ID = "TEST_SNAPSHOT_0001"
_EXPECTED_RESULT_KEY_COUNT = 185
_EXPECTED_RESULT_KEYS_SHA256 = "c98f3ecbd20c49a0dae22e95ff24472221a7f4dc8513a690fbda7fd14b401a89"


class _BuildResponse(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


def _build_request() -> SimpleNamespace:
    return SimpleNamespace(
        db_snapshot_id=TEST_SNAPSHOT_ID,
        profile_id="focused",
        bracket_id="B2",
        format="commander",
        commander="Missing Commander",
        cards=["Missing Card A", "Missing Card B"],
        engine_patches_v0=[],
    )


def _find_card_by_name_side_effect(snapshot_id: str, name: str) -> dict | None:
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


class SchemaFreezeGuardTests(unittest.TestCase):
    def test_result_top_level_keys_match_frozen_snapshot(self) -> None:
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

        stub_api_main = types.ModuleType("api.main")
        stub_api_main.BuildResponse = _BuildResponse

        with (
            patch.dict(sys.modules, {"api.main": stub_api_main}),
            patch("api.engine.pipeline_build.cards_db_connect", side_effect=lambda: sqlite3.connect(":memory:")),
            patch("api.engine.pipeline_build.run_snapshot_preflight_v1", return_value=preflight_payload),
            patch("api.engine.pipeline_build.resolve_runtime_taxonomy_version", return_value="taxonomy_v_test"),
            patch("api.engine.pipeline_build.resolve_runtime_ruleset_version", return_value="ruleset_v_test"),
            patch("api.engine.pipeline_build.run_snapshot_preflight", return_value={"status": "OK"}),
            patch("api.engine.pipeline_build.is_legal_commander_card", return_value=(True, "legal")),
            patch("api.engine.pipeline_build.find_card_by_name", side_effect=_find_card_by_name_side_effect),
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
            payload = run_build_pipeline(req=_build_request(), conn=None, repo_root_path=None)

        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        result_keys = sorted(result.keys())

        digest = hashlib.sha256(
            json.dumps(result_keys, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

        self.assertEqual(
            len(result_keys),
            _EXPECTED_RESULT_KEY_COUNT,
            f"Result top-level key count drifted. Current keys: {result_keys}",
        )
        self.assertEqual(
            digest,
            _EXPECTED_RESULT_KEYS_SHA256,
            f"Result top-level key snapshot drifted. Current keys: {result_keys}",
        )


if __name__ == "__main__":
    unittest.main()
