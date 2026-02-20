from __future__ import annotations

import unittest

from api.engine.repro_bundle_export_v1 import build_repro_bundle_export_v1
from api.engine.run_bundle_v0 import build_run_bundle_v0


_EXPECTED_FILE_PATHS = [
    "repro_bundle_manifest_v1.json",
    "request_input.json",
    "build_result.json",
    "rules/gc_limits_v1.json",
    "rules/bracket_rules_v2.json",
    "rules/two_card_combos_v1.json",
]


def _files_by_path(bundle_payload: dict) -> dict:
    files = bundle_payload.get("files") if isinstance(bundle_payload.get("files"), list) else []
    out = {}
    for entry in files:
        if not isinstance(entry, dict):
            continue
        path = entry.get("path")
        if isinstance(path, str):
            out[path] = entry
    return out


class ReproBundleExportV1Tests(unittest.TestCase):
    def _request_input(self) -> dict:
        return {
            "db_snapshot_id": "TEST_SNAPSHOT_0001",
            "profile_id": "focused",
            "bracket_id": "B2",
            "format": "commander",
            "commander": "Missing Commander",
            "cards": ["Missing Card A"],
            "engine_patches_v0": [],
        }

    def _build_payload(self) -> dict:
        return {
            "engine_version": "0.2.3",
            "ruleset_version": "ruleset_v0",
            "bracket_definition_version": "bracket_v0",
            "game_changers_version": "gc_v0_userlist_2025-11-20",
            "db_snapshot_id": "TEST_SNAPSHOT_0001",
            "profile_id": "focused",
            "bracket_id": "B2",
            "status": "OK",
            "build_hash_v1": "build_hash_example",
            "unknowns": [],
            "result": {
                "ui_contract_version": "ui_contract_v1",
                "pipeline_versions": {
                    "engine_version": "0.2.3",
                    "gc_limits_version": "gc_limits_v1",
                    "bracket_rules_version": "bracket_rules_v2",
                    "two_card_combos_version": "two_card_combos_v1",
                },
                "available_panels_v1": {
                    "has_snapshot_preflight_v1": True,
                    "has_typed_graph_invariants_v1": True,
                    "has_profile_bracket_enforcement_v1": False,
                    "has_bracket_compliance_summary_v1": False,
                    "has_graph_analytics_summary_v1": False,
                    "has_graph_pathways_summary_v1": False,
                    "has_disruption_surface_v1": True,
                    "has_vulnerability_index_v1": True,
                },
                "snapshot_preflight_v1": {"status": "OK"},
                "typed_graph_invariants_v1": {"status": "OK"},
                "disruption_surface_v1": {"status": "OK"},
                "vulnerability_index_v1": {"status": "OK"},
                "build_hash_v1": "build_hash_example",
            },
        }

    def test_bundle_export_is_deterministic_and_contains_expected_files(self) -> None:
        first = build_repro_bundle_export_v1(
            request_input=self._request_input(),
            build_payload=self._build_payload(),
        )
        second = build_repro_bundle_export_v1(
            request_input=self._request_input(),
            build_payload=self._build_payload(),
        )

        self.assertEqual(first, second)
        self.assertEqual(first.get("file_paths"), _EXPECTED_FILE_PATHS)

        by_path = _files_by_path(first)
        self.assertEqual(sorted(by_path.keys()), sorted(_EXPECTED_FILE_PATHS))

        manifest_entry = by_path.get("repro_bundle_manifest_v1.json")
        self.assertIsInstance(manifest_entry, dict)
        manifest_json = manifest_entry.get("json") if isinstance(manifest_entry.get("json"), dict) else {}
        self.assertEqual(manifest_json.get("version"), "repro_bundle_manifest_v1")
        self.assertEqual(
            manifest_json.get("included_layers"),
            [
                "snapshot_preflight_v1",
                "typed_graph_invariants_v1",
                "disruption_surface_v1",
                "vulnerability_index_v1",
            ],
        )

        gc_limits_entry = by_path.get("rules/gc_limits_v1.json")
        bracket_rules_entry = by_path.get("rules/bracket_rules_v2.json")
        two_card_entry = by_path.get("rules/two_card_combos_v1.json")

        self.assertIsInstance(gc_limits_entry, dict)
        self.assertIsInstance(bracket_rules_entry, dict)
        self.assertIsInstance(two_card_entry, dict)

        self.assertEqual((gc_limits_entry.get("json") or {}).get("version"), "gc_limits_v1")
        self.assertEqual((bracket_rules_entry.get("json") or {}).get("version"), "bracket_rules_v2")
        self.assertEqual((two_card_entry.get("json") or {}).get("version"), "two_card_combos_v1")


class RunBundleV0ReproExportIntegrationTests(unittest.TestCase):
    def test_run_bundle_includes_repro_export_payload(self) -> None:
        build_payload = {
            "engine_version": "0.2.3",
            "ruleset_version": "ruleset_v0",
            "bracket_definition_version": "bracket_v0",
            "game_changers_version": "gc_v0_userlist_2025-11-20",
            "db_snapshot_id": "TEST_SNAPSHOT_0001",
            "profile_id": "focused",
            "bracket_id": "B2",
            "status": "OK",
            "build_hash_v1": "build_hash_example",
            "graph_hash_v2": "graph_hash_example",
            "unknowns": [],
            "result": {
                "ui_contract_version": "ui_contract_v1",
                "available_panels_v1": {
                    "has_snapshot_preflight_v1": True,
                    "has_typed_graph_invariants_v1": True,
                    "has_profile_bracket_enforcement_v1": False,
                    "has_bracket_compliance_summary_v1": False,
                    "has_graph_analytics_summary_v1": False,
                    "has_graph_pathways_summary_v1": False,
                    "has_disruption_surface_v1": True,
                    "has_vulnerability_index_v1": True,
                },
                "pipeline_versions": {
                    "engine_version": "0.2.3",
                    "gc_limits_version": "gc_limits_v1",
                    "bracket_rules_version": "bracket_rules_v2",
                    "two_card_combos_version": "two_card_combos_v1",
                },
                "snapshot_preflight_v1": {"status": "OK"},
                "typed_graph_invariants_v1": {"status": "OK"},
                "disruption_surface_v1": {"status": "OK"},
                "vulnerability_index_v1": {"status": "OK"},
                "build_hash_v1": "build_hash_example",
            },
        }

        stored_run = {
            "run_id": "RUN_TEST_0001",
            "endpoint": "build",
            "request": {
                "db_snapshot_id": "TEST_SNAPSHOT_0001",
                "profile_id": "focused",
                "bracket_id": "B2",
                "format": "commander",
                "commander": "Missing Commander",
                "cards": [],
                "engine_patches_v0": [],
            },
            "response": build_payload,
            "input_hash_v1": "input_hash_example",
            "output_build_hash_v1": "build_hash_example",
            "output_proof_attempts_hash_v2": None,
        }

        bundle = build_run_bundle_v0(stored_run)

        self.assertIsInstance(bundle, dict)
        repro_export = bundle.get("repro_bundle_export_v1")
        self.assertIsInstance(repro_export, dict)
        self.assertEqual(repro_export.get("file_paths"), _EXPECTED_FILE_PATHS)

        by_path = _files_by_path(repro_export)
        self.assertIn("repro_bundle_manifest_v1.json", by_path)
        self.assertIn("request_input.json", by_path)
        self.assertIn("build_result.json", by_path)
        self.assertIn("rules/gc_limits_v1.json", by_path)
        self.assertIn("rules/bracket_rules_v2.json", by_path)
        self.assertIn("rules/two_card_combos_v1.json", by_path)


if __name__ == "__main__":
    unittest.main()
