from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from api.engine.pipeline_build import run_build_pipeline

TEST_SNAPSHOT_ID = "TEST_SNAPSHOT_0001"


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
    if name != "Missing Commander":
        return None
    return {
        "name": "Missing Commander",
        "oracle_id": "oracle_missing_commander",
        "color_identity": ["R"],
        "legalities": {"commander": "legal"},
        "type_line": "Legendary Creature — Goblin",
    }


class _BuildResponse(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


def test_pipeline_reports_typed_graph_invariants_when_graph_present(mtg_test_db_path: Path) -> None:
    _ = mtg_test_db_path

    stub_api_main = types.ModuleType("api.main")
    stub_api_main.BuildResponse = _BuildResponse

    with (
        patch.dict(sys.modules, {"api.main": stub_api_main}),
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
                    "primitive_ids": ["RAMP_MANA"],
                    "ruleset_version": "ruleset_v_test",
                    "evidence": {"matches": []},
                }
            },
        ),
    ):
        payload = run_build_pipeline(req=_build_request(), conn=None, repo_root_path=None)

    assert isinstance(payload, dict)

    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    graph_v1 = result.get("graph_v1")
    assert isinstance(graph_v1, dict)

    typed_payload = result.get("typed_graph_invariants_v1")
    assert isinstance(typed_payload, dict)
    assert isinstance(typed_payload.get("version"), str)
    assert typed_payload.get("version") != ""
    assert isinstance(typed_payload.get("status"), str)
    assert typed_payload.get("status") != ""

    available_panels = result.get("available_panels_v1") if isinstance(result.get("available_panels_v1"), dict) else {}
    assert available_panels.get("has_typed_graph_invariants_v1") is True
