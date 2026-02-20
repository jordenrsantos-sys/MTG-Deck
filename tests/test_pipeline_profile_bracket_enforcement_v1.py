from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from api.engine.constants_disruption import DISRUPTION_PRIMITIVE_IDS
from api.engine.layers.profile_bracket_enforcement_v1 import PROFILE_BRACKET_ENFORCEMENT_V1_VERSION
from api.engine.layers.vulnerability_index_v1 import VULNERABILITY_INDEX_V1_VERSION
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
    if name == "Missing Commander":
        return {
            "name": "Missing Commander",
            "oracle_id": "oracle_missing_commander",
            "color_identity": ["R"],
            "legalities": {"commander": "legal"},
            "type_line": "Legendary Creature — Goblin",
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


class _BuildResponse(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


def test_pipeline_reports_profile_bracket_enforcement_payload_and_panel(mtg_test_db_path: Path) -> None:
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
                },
                "oracle_missing_card_a": {
                    "primitive_ids": ["extra_turn"],
                    "ruleset_version": "ruleset_v_test",
                    "evidence": {"matches": []},
                },
                "oracle_missing_card_b": {
                    "primitive_ids": [],
                    "ruleset_version": "ruleset_v_test",
                    "evidence": {"matches": []},
                }
            },
        ),
    ):
        payload = run_build_pipeline(req=_build_request(), conn=None, repo_root_path=None)

    assert isinstance(payload, dict)

    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    enforcement_payload = result.get("profile_bracket_enforcement_v1")
    assert isinstance(enforcement_payload, dict)
    assert enforcement_payload.get("version") == PROFILE_BRACKET_ENFORCEMENT_V1_VERSION
    assert enforcement_payload.get("gc_limits_version") == "gc_limits_v1"
    assert enforcement_payload.get("bracket_rules_version") == "bracket_rules_v2"

    limits = enforcement_payload.get("limits") if isinstance(enforcement_payload.get("limits"), dict) else {}
    assert limits.get("min") == 0
    assert limits.get("max") == 0

    category_results = (
        enforcement_payload.get("category_results")
        if isinstance(enforcement_payload.get("category_results"), dict)
        else {}
    )
    mass_land_denial = (
        category_results.get("mass_land_denial")
        if isinstance(category_results.get("mass_land_denial"), dict)
        else {}
    )
    assert mass_land_denial.get("supported") is True
    assert mass_land_denial.get("count") == 0

    extra_turn_chains = (
        category_results.get("extra_turn_chains")
        if isinstance(category_results.get("extra_turn_chains"), dict)
        else {}
    )
    assert extra_turn_chains.get("supported") is True
    assert extra_turn_chains.get("count") == 1

    two_card_combos = (
        category_results.get("two_card_combos")
        if isinstance(category_results.get("two_card_combos"), dict)
        else {}
    )
    assert two_card_combos.get("supported") is True
    assert two_card_combos.get("count") == 1

    violations = enforcement_payload.get("violations") if isinstance(enforcement_payload.get("violations"), list) else []
    assert any(
        isinstance(entry, dict)
        and entry.get("code") == "EXTRA_TURN_CHAINS_DISALLOWED"
        and entry.get("category") == "extra_turn_chains"
        for entry in violations
    )
    assert any(
        isinstance(entry, dict)
        and entry.get("code") == "TWO_CARD_COMBOS_DISALLOWED"
        and entry.get("category") == "two_card_combos"
        for entry in violations
    )

    status_value = enforcement_payload.get("status")
    assert isinstance(status_value, str)
    assert status_value == "ERROR"

    summary_payload = result.get("bracket_compliance_summary_v1")
    assert isinstance(summary_payload, dict)
    assert summary_payload.get("version") == "bracket_compliance_summary_v1"
    assert summary_payload.get("status") == "ERROR"
    assert summary_payload.get("bracket_id") == "B2"

    summary_counts = summary_payload.get("counts") if isinstance(summary_payload.get("counts"), dict) else {}
    assert summary_counts.get("game_changers") == 0
    assert summary_counts.get("mass_land_denial") == 0
    assert summary_counts.get("extra_turns") == 1
    assert summary_counts.get("two_card_combos") == 1

    summary_versions = summary_payload.get("versions") if isinstance(summary_payload.get("versions"), dict) else {}
    assert summary_versions.get("gc_limits_version") == "gc_limits_v1"
    assert summary_versions.get("bracket_rules_version") == "bracket_rules_v2"
    assert summary_versions.get("two_card_combos_version") == "two_card_combos_v2"

    graph_analytics_summary = result.get("graph_analytics_summary_v1")
    assert isinstance(graph_analytics_summary, dict)
    assert graph_analytics_summary.get("version") == "graph_analytics_summary_v1"
    assert graph_analytics_summary.get("status") == "OK"
    assert graph_analytics_summary.get("reason") is None

    graph_counts = graph_analytics_summary.get("counts") if isinstance(graph_analytics_summary.get("counts"), dict) else {}
    assert graph_counts.get("nodes") == 3
    assert graph_counts.get("edges") == 1
    assert graph_counts.get("playable_nodes") == 2

    graph_pathways_summary = result.get("graph_pathways_summary_v1")
    assert isinstance(graph_pathways_summary, dict)
    assert graph_pathways_summary.get("version") == "graph_pathways_summary_v1"
    assert graph_pathways_summary.get("status") == "OK"
    assert graph_pathways_summary.get("reason") is None

    top_hubs = graph_pathways_summary.get("top_hubs") if isinstance(graph_pathways_summary.get("top_hubs"), list) else []
    assert len(top_hubs) == 2

    top_edges = graph_pathways_summary.get("top_edges") if isinstance(graph_pathways_summary.get("top_edges"), list) else []
    assert top_edges == []

    top_components = (
        graph_pathways_summary.get("top_components") if isinstance(graph_pathways_summary.get("top_components"), list) else []
    )
    assert len(top_components) == 2

    disruption_surface = result.get("disruption_surface_v1")
    assert isinstance(disruption_surface, dict)
    assert disruption_surface.get("version") == "disruption_surface_v1"

    if len(DISRUPTION_PRIMITIVE_IDS) > 0:
        assert disruption_surface.get("status") == "OK"
        assert disruption_surface.get("reason") is None

        disruption_totals = disruption_surface.get("totals") if isinstance(disruption_surface.get("totals"), dict) else {}
        assert disruption_totals.get("disruption_slots") == 0
        assert disruption_totals.get("disruption_primitives_hit") == 0
    else:
        assert disruption_surface.get("status") == "SKIP"
        assert disruption_surface.get("reason") == "NO_DISRUPTION_PRIMITIVES_DEFINED"

    vulnerability_index = result.get("vulnerability_index_v1")
    assert isinstance(vulnerability_index, dict)
    assert vulnerability_index.get("version") == VULNERABILITY_INDEX_V1_VERSION
    assert vulnerability_index.get("status") == "OK"

    vulnerability_scores = vulnerability_index.get("scores") if isinstance(vulnerability_index.get("scores"), dict) else {}
    assert isinstance(vulnerability_scores.get("graveyard_reliance"), float)
    assert isinstance(vulnerability_scores.get("commander_dependence"), float)
    assert isinstance(vulnerability_scores.get("single_engine_reliance"), float)
    assert isinstance(vulnerability_scores.get("setup_dependency"), float)
    assert isinstance(vulnerability_scores.get("interaction_exposure"), float)

    pipeline_versions = result.get("pipeline_versions") if isinstance(result.get("pipeline_versions"), dict) else {}
    assert pipeline_versions.get("gc_limits_version") == "gc_limits_v1"
    assert pipeline_versions.get("bracket_rules_version") == "bracket_rules_v2"
    assert pipeline_versions.get("two_card_combos_version") == "two_card_combos_v2"
    assert pipeline_versions.get("spellbook_variants_version") == "commander_spellbook_variants_v1"

    available_panels = result.get("available_panels_v1") if isinstance(result.get("available_panels_v1"), dict) else {}
    assert available_panels.get("has_profile_bracket_enforcement_v1") is True
    assert available_panels.get("has_bracket_compliance_summary_v1") is True
    assert available_panels.get("has_graph_analytics_summary_v1") is True
    assert available_panels.get("has_graph_pathways_summary_v1") is True
    assert available_panels.get("has_disruption_surface_v1") is True
    assert available_panels.get("has_vulnerability_index_v1") is True
