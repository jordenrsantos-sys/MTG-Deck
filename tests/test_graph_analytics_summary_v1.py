from __future__ import annotations

import unittest

from api.engine.layers.graph_analytics_summary_v1 import (
    GRAPH_ANALYTICS_SUMMARY_V1_VERSION,
    run_graph_analytics_summary_v1,
)


class GraphAnalyticsSummaryV1Tests(unittest.TestCase):
    def _synthetic_graph_v1(self) -> dict:
        return {
            "bipartite": {
                "nodes": [
                    {"id": "prim:CARD_DRAW", "kind": "primitive"},
                    {"id": "prim:RAMP_MANA", "kind": "primitive"},
                    {"id": "slot:S0", "kind": "slot"},
                    {"id": "slot:S1", "kind": "slot"},
                    {"id": "slot:S2", "kind": "slot"},
                ],
                "edges": [
                    {"a": "slot:S0", "b": "prim:RAMP_MANA", "kind": "has_primitive"},
                    {"a": "slot:S1", "b": "prim:CARD_DRAW", "kind": "has_primitive"},
                    {"a": "slot:S1", "b": "prim:RAMP_MANA", "kind": "has_primitive"},
                ],
                "stats": {},
            },
            "candidate_edges": [
                {
                    "a": "slot:S0",
                    "b": "slot:S1",
                    "kind": "shared_primitive",
                    "shared_primitives": ["RAMP_MANA"],
                }
            ],
            "bounds": {
                "MAX_PRIMS_PER_SLOT": 24,
                "MAX_SLOTS_PER_PRIM": 80,
                "MAX_CARD_CARD_EDGES_TOTAL": 5000,
            },
            "stats": {},
        }

    def test_skip_when_graph_missing(self) -> None:
        for graph_v1 in (None, {}):
            with self.subTest(graph_v1=graph_v1):
                payload = run_graph_analytics_summary_v1(
                    graph_v1=graph_v1,
                    primitive_index_by_slot={},
                    deck_slot_ids_playable=[],
                    typed_graph_invariants=None,
                )

                self.assertEqual(payload.get("version"), GRAPH_ANALYTICS_SUMMARY_V1_VERSION)
                self.assertEqual(payload.get("status"), "SKIP")
                self.assertEqual(payload.get("reason"), "GRAPH_MISSING")

    def test_skip_when_typed_graph_invariants_error(self) -> None:
        payload = run_graph_analytics_summary_v1(
            graph_v1=self._synthetic_graph_v1(),
            primitive_index_by_slot={},
            deck_slot_ids_playable=[],
            typed_graph_invariants={"status": "ERROR"},
        )

        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason"), "GRAPH_INVARIANTS_ERROR")

    def test_ok_summary_computes_counts_degrees_components_and_top_primitives(self) -> None:
        payload = run_graph_analytics_summary_v1(
            graph_v1=self._synthetic_graph_v1(),
            primitive_index_by_slot={
                "S0": ["RAMP_MANA", "CARD_DRAW"],
                "S1": ["RAMP_MANA", "RAMP_MANA", "TOKEN_PRODUCTION"],
                "S2": ["TOKEN_PRODUCTION", "CARD_DRAW"],
            },
            deck_slot_ids_playable=["S2", "S1"],
            typed_graph_invariants={"status": "OK"},
        )

        self.assertEqual(payload.get("version"), GRAPH_ANALYTICS_SUMMARY_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertIsNone(payload.get("reason"))

        counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
        self.assertEqual(counts.get("nodes"), 5)
        self.assertEqual(counts.get("edges"), 4)
        self.assertEqual(counts.get("playable_nodes"), 2)

        top_primitives = payload.get("top_primitives_by_slot_coverage") if isinstance(payload.get("top_primitives_by_slot_coverage"), list) else []
        self.assertEqual(
            top_primitives,
            [
                {"primitive": "TOKEN_PRODUCTION", "slots": 2},
                {"primitive": "CARD_DRAW", "slots": 1},
                {"primitive": "RAMP_MANA", "slots": 1},
            ],
        )

        connectivity = payload.get("connectivity") if isinstance(payload.get("connectivity"), dict) else {}
        self.assertEqual(connectivity.get("avg_out_degree"), 0.8)
        self.assertEqual(connectivity.get("avg_in_degree"), 0.8)
        self.assertEqual(connectivity.get("max_out_degree"), 2)
        self.assertEqual(connectivity.get("max_in_degree"), 2)

        components = payload.get("components") if isinstance(payload.get("components"), dict) else {}
        self.assertEqual(components.get("component_count"), 2)
        self.assertEqual(components.get("largest_component_nodes"), 4)
        self.assertEqual(components.get("largest_component_edges"), 4)

    def test_deterministic_for_same_input(self) -> None:
        kwargs = {
            "graph_v1": self._synthetic_graph_v1(),
            "primitive_index_by_slot": {
                "S0": ["RAMP_MANA", "CARD_DRAW"],
                "S1": ["RAMP_MANA", "TOKEN_PRODUCTION"],
                "S2": ["TOKEN_PRODUCTION", "CARD_DRAW"],
            },
            "deck_slot_ids_playable": ["S2", "S1"],
            "typed_graph_invariants": {"status": "OK"},
        }

        first = run_graph_analytics_summary_v1(**kwargs)
        second = run_graph_analytics_summary_v1(**kwargs)

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
