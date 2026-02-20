from __future__ import annotations

import unittest

from api.engine.layers.repro_bundle_manifest_v1 import (
    REPRO_BUNDLE_MANIFEST_V1_VERSION,
    build_repro_bundle_manifest_v1,
)


class ReproBundleManifestV1Tests(unittest.TestCase):
    def _build_result_payload(self) -> dict:
        return {
            "build_hash_v1": "build_hash_example",
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
        }

    def test_manifest_included_layers_and_versions_are_deterministic(self) -> None:
        build_result = self._build_result_payload()

        first = build_repro_bundle_manifest_v1(build_result)
        second = build_repro_bundle_manifest_v1(build_result)

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), REPRO_BUNDLE_MANIFEST_V1_VERSION)
        self.assertEqual(
            first.get("included_layers"),
            [
                "snapshot_preflight_v1",
                "typed_graph_invariants_v1",
                "disruption_surface_v1",
                "vulnerability_index_v1",
            ],
        )

        engine_versions = first.get("engine_versions") if isinstance(first.get("engine_versions"), dict) else {}
        self.assertEqual(
            list(engine_versions.keys()),
            [
                "bracket_rules_version",
                "engine_version",
                "gc_limits_version",
                "two_card_combos_version",
            ],
        )

    def test_normalized_json_sha256_stable_across_repeated_calls(self) -> None:
        build_result = self._build_result_payload()
        first = build_repro_bundle_manifest_v1(build_result)
        second = build_repro_bundle_manifest_v1(build_result)

        first_hashes = first.get("hashes") if isinstance(first.get("hashes"), dict) else {}
        second_hashes = second.get("hashes") if isinstance(second.get("hashes"), dict) else {}

        self.assertEqual(first_hashes.get("normalized_json_sha256"), second_hashes.get("normalized_json_sha256"))
        self.assertIsInstance(first_hashes.get("normalized_json_sha256"), str)
        self.assertEqual(len(first_hashes.get("normalized_json_sha256")), 64)

    def test_hash_excludes_ui_redacted_path_markers(self) -> None:
        baseline = self._build_result_payload()

        with_redacted_markers = self._build_result_payload()
        with_redacted_markers["game_changers_path"] = "<LOCAL_PATH_REDACTED>"

        baseline_manifest = build_repro_bundle_manifest_v1(baseline)
        redacted_manifest = build_repro_bundle_manifest_v1(with_redacted_markers)

        baseline_hashes = baseline_manifest.get("hashes") if isinstance(baseline_manifest.get("hashes"), dict) else {}
        redacted_hashes = redacted_manifest.get("hashes") if isinstance(redacted_manifest.get("hashes"), dict) else {}

        self.assertEqual(baseline_hashes.get("normalized_json_sha256"), redacted_hashes.get("normalized_json_sha256"))


if __name__ == "__main__":
    unittest.main()
