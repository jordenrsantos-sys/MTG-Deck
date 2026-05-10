"""Tests for layer_registry_v1 (Engine-4A topological resolver)."""
from __future__ import annotations

import unittest

from api.engine.layer_registry import (
    LAYER_REGISTRY,
    LAYER_REGISTRY_V1_VERSION,
    filter_response_to_resolved,
    is_known_layer,
    known_layer_ids,
    resolve_partial,
)


class LayerRegistryCatalogTests(unittest.TestCase):
    def test_registry_version(self) -> None:
        self.assertEqual(LAYER_REGISTRY_V1_VERSION, "layer_registry_v1")

    def test_known_layer_ids_sorted(self) -> None:
        ids = known_layer_ids()
        self.assertEqual(ids, sorted(ids))
        # Spot-check core layers.
        self.assertIn("seed_synergy_detection_v1", ids)
        self.assertIn("commander_recommendation_v1", ids)
        self.assertIn("seed_to_deck_v1", ids)

    def test_is_known_layer(self) -> None:
        self.assertTrue(is_known_layer("seed_synergy_detection_v1"))
        self.assertFalse(is_known_layer("not_a_real_layer"))
        self.assertFalse(is_known_layer(""))
        self.assertFalse(is_known_layer(None))  # type: ignore[arg-type]


class ResolvePartialTests(unittest.TestCase):
    def test_single_layer_with_no_deps(self) -> None:
        r = resolve_partial(["snapshot_preflight_v1"])
        self.assertEqual(r["resolved"], ["snapshot_preflight_v1"])
        self.assertTrue(r["partial_eligible"])
        self.assertEqual(r["unknown"], [])

    def test_single_layer_pulls_transitive_deps(self) -> None:
        # seed_synergy_detection_v1 depends on primitive_index_v1 which
        # depends on snapshot_preflight_v1.
        r = resolve_partial(["seed_synergy_detection_v1"])
        self.assertIn("snapshot_preflight_v1", r["resolved"])
        self.assertIn("primitive_index_v1", r["resolved"])
        self.assertIn("seed_synergy_detection_v1", r["resolved"])
        # Topological order: deps before consumers.
        idx_pre = r["resolved"].index("snapshot_preflight_v1")
        idx_pi = r["resolved"].index("primitive_index_v1")
        idx_seed = r["resolved"].index("seed_synergy_detection_v1")
        self.assertLess(idx_pre, idx_pi)
        self.assertLess(idx_pi, idx_seed)

    def test_seed_to_deck_pulls_phase_2_and_phase_1(self) -> None:
        # seed_to_deck_v1 depends on seed_synergy_detection_v1 +
        # commander_recommendation_v1; both transitively pull primitive_index_v1.
        r = resolve_partial(["seed_to_deck_v1"])
        for required in (
            "snapshot_preflight_v1",
            "primitive_index_v1",
            "seed_synergy_detection_v1",
            "commander_recommendation_v1",
            "seed_to_deck_v1",
        ):
            self.assertIn(required, r["resolved"])
        self.assertTrue(r["partial_eligible"])

    def test_unknown_layer_id_surfaces(self) -> None:
        r = resolve_partial(["not_a_real_layer", "seed_synergy_detection_v1"])
        self.assertIn("not_a_real_layer", r["unknown"])
        self.assertIn("seed_synergy_detection_v1", r["resolved"])
        self.assertFalse(r["partial_eligible"])

    def test_sufficiency_summary_marked_non_partial_safe(self) -> None:
        # sufficiency_summary_v1 depends on 14 upstream payloads not yet
        # catalogued; flagged non-partial-safe per autonomous_repair_log soft-
        # safety #4.
        r = resolve_partial(["sufficiency_summary_v1"])
        self.assertIn("sufficiency_summary_v1", r["non_partial_safe"])
        self.assertFalse(r["partial_eligible"])

    def test_deduplicates_and_sorts_requested(self) -> None:
        r = resolve_partial(
            [
                "seed_synergy_detection_v1",
                "seed_synergy_detection_v1",
                "snapshot_preflight_v1",
            ]
        )
        self.assertEqual(
            r["requested"],
            ["seed_synergy_detection_v1", "snapshot_preflight_v1"],
        )

    def test_resolve_is_deterministic(self) -> None:
        r1 = resolve_partial(["seed_to_deck_v1", "engine_coherence_v1"])
        r2 = resolve_partial(["seed_to_deck_v1", "engine_coherence_v1"])
        self.assertEqual(r1, r2)

    def test_empty_input_returns_empty_resolution(self) -> None:
        r = resolve_partial([])
        self.assertEqual(r["resolved"], [])
        self.assertTrue(r["partial_eligible"])

    def test_handles_invalid_input_types_gracefully(self) -> None:
        # Non-string values are filtered out.
        r = resolve_partial(["seed_synergy_detection_v1", 42, None, ""])  # type: ignore[list-item]
        self.assertIn("seed_synergy_detection_v1", r["resolved"])


class FilterResponseTests(unittest.TestCase):
    def test_filters_to_resolved_and_keeps_envelope_fields(self) -> None:
        full = {
            "ui_contract_version": "v1",
            "available_panels_v1": {"x": True},
            "ruleset_version": "rs_v1",
            "snapshot_id": "snap-1",
            "seed_synergy_detection_v1": {"version": "ssd_v1"},
            "commander_recommendation_v1": {"version": "cr_v1"},
            "seed_to_deck_v1": {"version": "std_v1"},
            "extra_irrelevant_key": "drop me",
        }
        out = filter_response_to_resolved(
            full, ["seed_synergy_detection_v1", "snapshot_preflight_v1"]
        )
        # Resolved layers kept.
        self.assertIn("seed_synergy_detection_v1", out)
        # Envelope fields kept.
        self.assertIn("ui_contract_version", out)
        self.assertIn("available_panels_v1", out)
        self.assertIn("ruleset_version", out)
        self.assertIn("snapshot_id", out)
        # Non-resolved + non-envelope dropped.
        self.assertNotIn("commander_recommendation_v1", out)
        self.assertNotIn("seed_to_deck_v1", out)
        self.assertNotIn("extra_irrelevant_key", out)

    def test_filter_handles_non_dict(self) -> None:
        self.assertEqual(filter_response_to_resolved(None, ["x"]), {})  # type: ignore[arg-type]
        self.assertEqual(filter_response_to_resolved("not-a-dict", ["x"]), {})  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
