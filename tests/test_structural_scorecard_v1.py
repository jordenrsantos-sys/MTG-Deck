from __future__ import annotations

import unittest

from api.engine.layers.structural_scorecard_v1 import (
    STRUCTURAL_SCORECARD_V1_VERSION,
    run_structural_scorecard_v1,
)


class StructuralScorecardV1Tests(unittest.TestCase):
    def test_skip_when_all_inputs_missing(self) -> None:
        payload = run_structural_scorecard_v1()

        self.assertEqual(payload.get("version"), STRUCTURAL_SCORECARD_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason"), "NO_SUBSCORES_AVAILABLE")

        headline = payload.get("headline") if isinstance(payload.get("headline"), dict) else {}
        self.assertIsNone(headline.get("grade"))
        self.assertIsNone(headline.get("score_0_100"))

        subscores = payload.get("subscores") if isinstance(payload.get("subscores"), dict) else {}
        self.assertIsNone(subscores.get("policy_compliance"))
        self.assertIsNone(subscores.get("graph_cohesion"))
        self.assertIsNone(subscores.get("interaction_coverage"))
        self.assertIsNone(subscores.get("vulnerability"))

        self.assertEqual(payload.get("badges"), [])

        sources = payload.get("sources") if isinstance(payload.get("sources"), dict) else {}
        self.assertEqual(
            sources,
            {
                "bracket_compliance_summary_v1": False,
                "graph_analytics_summary_v1": False,
                "disruption_surface_v1": False,
                "vulnerability_index_v1": False,
            },
        )

    def test_ok_scoring_with_synthetic_inputs(self) -> None:
        payload = run_structural_scorecard_v1(
            bracket_compliance={"status": "OK"},
            graph_analytics={
                "status": "OK",
                "counts": {"nodes": 10, "playable_nodes": 10},
                "connectivity": {"avg_out_degree": 1.0, "avg_in_degree": 1.0},
                "components": {
                    "component_count": 1,
                    "largest_component_nodes": 10,
                },
            },
            disruption_surface={
                "status": "OK",
                "totals": {"disruption_slots": 8},
            },
            vulnerability_index={
                "status": "OK",
                "scores": {
                    "graveyard_reliance": 0.2,
                    "commander_dependence": 0.1,
                    "single_engine_reliance": 0.4,
                    "setup_dependency": 0.3,
                    "interaction_exposure": 0.2,
                },
            },
            structural_snapshot_v1={"deck_size": 100},
            typed_graph_invariants={"status": "OK"},
        )

        self.assertEqual(payload.get("version"), STRUCTURAL_SCORECARD_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertIsNone(payload.get("reason"))

        headline = payload.get("headline") if isinstance(payload.get("headline"), dict) else {}
        self.assertEqual(headline.get("score_0_100"), 85)
        self.assertEqual(headline.get("grade"), "B")

        subscores = payload.get("subscores") if isinstance(payload.get("subscores"), dict) else {}
        self.assertEqual(
            subscores,
            {
                "policy_compliance": 100,
                "graph_cohesion": 85,
                "interaction_coverage": 80,
                "vulnerability": 76,
            },
        )
        self.assertEqual(payload.get("badges"), [])

        sources = payload.get("sources") if isinstance(payload.get("sources"), dict) else {}
        self.assertEqual(
            sources,
            {
                "bracket_compliance_summary_v1": True,
                "graph_analytics_summary_v1": True,
                "disruption_surface_v1": True,
                "vulnerability_index_v1": True,
            },
        )

    def test_deterministic_repeat_call_identical(self) -> None:
        kwargs = {
            "bracket_compliance": {"status": "OK"},
            "graph_analytics": {
                "status": "OK",
                "counts": {"nodes": 12, "playable_nodes": 12},
                "connectivity": {"avg_out_degree": 0.5, "avg_in_degree": 0.5},
                "components": {
                    "component_count": 2,
                    "largest_component_nodes": 9,
                },
            },
            "disruption_surface": {
                "status": "OK",
                "totals": {"disruption_slots": 6},
            },
            "vulnerability_index": {
                "status": "OK",
                "scores": {
                    "graveyard_reliance": 0.3,
                    "commander_dependence": 0.2,
                    "single_engine_reliance": 0.5,
                    "setup_dependency": 0.4,
                    "interaction_exposure": 0.3,
                },
            },
            "structural_snapshot_v1": {"deck_size": 100},
            "typed_graph_invariants": {"status": "OK"},
        }

        first = run_structural_scorecard_v1(**kwargs)
        second = run_structural_scorecard_v1(**kwargs)

        self.assertEqual(first, second)

    def test_badges_sorted_deterministically(self) -> None:
        payload = run_structural_scorecard_v1(
            bracket_compliance={"status": "WARN"},
            disruption_surface={
                "status": "OK",
                "totals": {"disruption_slots": 5},
            },
            vulnerability_index={
                "status": "OK",
                "scores": {
                    "graveyard_reliance": 0.3,
                    "commander_dependence": 0.2,
                    "single_engine_reliance": 0.8,
                    "setup_dependency": 0.4,
                    "interaction_exposure": 0.7,
                },
            },
            typed_graph_invariants={"status": "ERROR"},
        )

        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(payload.get("reason"), "GRAPH_INVARIANTS_ERROR")

        badges = payload.get("badges") if isinstance(payload.get("badges"), list) else []
        badge_codes = [entry.get("code") for entry in badges if isinstance(entry, dict)]
        self.assertEqual(
            badge_codes,
            [
                "GRAPH_INVARIANTS_ERROR",
                "BRACKET_COMPLIANCE_WARN",
                "HIGH_INTERACTION_EXPOSURE",
                "HIGH_SINGLE_ENGINE_RELIANCE",
            ],
        )

    def test_grade_mapping_boundaries(self) -> None:
        cases = [
            (
                {
                    "disruption_surface": {
                        "status": "OK",
                        "totals": {"disruption_slots": 9},
                    }
                },
                "A",
                90,
            ),
            (
                {
                    "disruption_surface": {
                        "status": "OK",
                        "totals": {"disruption_slots": 8},
                    }
                },
                "B",
                80,
            ),
            (
                {
                    "disruption_surface": {
                        "status": "OK",
                        "totals": {"disruption_slots": 7},
                    }
                },
                "C",
                70,
            ),
            (
                {"bracket_compliance": {"status": "WARN"}},
                "D",
                60,
            ),
            (
                {"bracket_compliance": {"status": "ERROR"}},
                "F",
                0,
            ),
        ]

        for kwargs, expected_grade, expected_score in cases:
            with self.subTest(expected_grade=expected_grade, expected_score=expected_score):
                payload = run_structural_scorecard_v1(**kwargs)
                headline = payload.get("headline") if isinstance(payload.get("headline"), dict) else {}
                self.assertEqual(headline.get("grade"), expected_grade)
                self.assertEqual(headline.get("score_0_100"), expected_score)


if __name__ == "__main__":
    unittest.main()
