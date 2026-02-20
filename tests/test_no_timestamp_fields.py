from __future__ import annotations

import sqlite3
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from api.engine.pipeline_build import run_build_pipeline


_FORBIDDEN_TIMESTAMP_KEYS = {"timestamp", "generated_at", "created_at"}
TEST_SNAPSHOT_ID = "TEST_SNAPSHOT_0001"


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


def _collect_forbidden_paths(value, path: str, out: list[str]) -> None:
    if isinstance(value, dict):
        for key in sorted(value.keys(), key=lambda item: str(item)):
            key_text = str(key)
            child_path = f"{path}.{key_text}" if path != "$" else f"$.{key_text}"
            if isinstance(key, str) and key.lower() in _FORBIDDEN_TIMESTAMP_KEYS:
                out.append(child_path)
            _collect_forbidden_paths(value.get(key), child_path, out)
        return

    if isinstance(value, list):
        for idx, item in enumerate(value):
            _collect_forbidden_paths(item, f"{path}[{idx}]", out)


class NoTimestampFieldsTests(unittest.TestCase):
    def test_build_payload_contains_no_timestamp_like_fields(self) -> None:
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

        forbidden_paths: list[str] = []
        _collect_forbidden_paths(payload, "$", forbidden_paths)

        self.assertFalse(
            forbidden_paths,
            "Found forbidden timestamp-like keys in build payload:\n- " + "\n- ".join(sorted(forbidden_paths)),
        )


if __name__ == "__main__":
    unittest.main()
