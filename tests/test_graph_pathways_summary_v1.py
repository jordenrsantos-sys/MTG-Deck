from __future__ import annotations

import unittest

from api.engine.layers.graph_pathways_summary_v1 import (
    GRAPH_PATHWAYS_SUMMARY_V1_VERSION,
    run_graph_pathways_summary_v1,
)


class GraphPathwaysSummaryV1Tests(unittest.TestCase):
    def _synthetic_graph_v1(self) -> dict:
        return {
            "bipartite": {
                "nodes": [
                    {"id": "slot:S0", "kind": "slot"},
                    {"id": "slot:S1", "kind": "slot"},
                    {"id": "slot:S2", "kind": "slot"},
                    {"id": "slot:S3", "kind": "slot"},
                    {"id": "slot:S4", "kind": "slot"},
                    {"id": "slot:S5", "kind": "slot"},
                    {"id": "slot:S6", "kind": "slot"},
                    {"id": "slot:S7", "kind": "slot"},
                    {"id": "prim:RAMP_MANA", "kind": "primitive"},
                ],
                "edges": [
                    {"a": "slot:S0", "b": "prim:RAMP_MANA", "kind": "has_primitive"},
                ],
                "stats": {},
            },
            "candidate_edges": [
                {"a": "slot:S1", "b": "slot:S2", "kind": "shared_primitive", "weight": 7},
                {"a": "slot:S0", "b": "slot:S1", "kind": "shared_primitive", "weight": 7},
                {"a": "slot:S3", "b": "slot:S4", "kind": "shared_primitive", "weight": 1},
                {"a": "slot:S6", "b": "slot:S7", "kind": "shared_primitive", "weight": 1},
                {"a": "slot:S0", "b": "slot:S2", "kind": "shared_primitive"},
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
                payload = run_graph_pathways_summary_v1(
                    graph_v1=graph_v1,
                    deck_slot_ids_playable=[],
                    typed_graph_invariants=None,
                    commander_slot_id=None,
                )

                self.assertEqual(payload.get("version"), GRAPH_PATHWAYS_SUMMARY_V1_VERSION)
                self.assertEqual(payload.get("status"), "SKIP")
                self.assertEqual(payload.get("reason"), "GRAPH_MISSING")

    def test_skip_when_invariants_error(self) -> None:
        payload = run_graph_pathways_summary_v1(
            graph_v1=self._synthetic_graph_v1(),
            deck_slot_ids_playable=["S0", "S1"],
            typed_graph_invariants={"status": "ERROR"},
            commander_slot_id="S1",
        )

        self.assertEqual(payload.get("version"), GRAPH_PATHWAYS_SUMMARY_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason"), "GRAPH_INVARIANTS_ERROR")

    def test_ok_summary_hubs_edges_components_are_sorted_deterministically(self) -> None:
        payload = run_graph_pathways_summary_v1(
            graph_v1=self._synthetic_graph_v1(),
            deck_slot_ids_playable=["S7", "S6", "S5", "S4", "S3", "S2", "S1", "S0"],
            typed_graph_invariants={"status": "OK"},
            commander_slot_id="S1",
        )

        self.assertEqual(payload.get("version"), GRAPH_PATHWAYS_SUMMARY_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertIsNone(payload.get("reason"))

        top_hubs = payload.get("top_hubs") if isinstance(payload.get("top_hubs"), list) else []
        self.assertEqual(
            top_hubs,
            [
                {"slot_id": "S0", "degree_total": 2, "in_degree": 0, "out_degree": 2, "is_commander": False},
                {"slot_id": "S1", "degree_total": 2, "in_degree": 1, "out_degree": 1, "is_commander": True},
                {"slot_id": "S2", "degree_total": 2, "in_degree": 2, "out_degree": 0, "is_commander": False},
                {"slot_id": "S3", "degree_total": 1, "in_degree": 0, "out_degree": 1, "is_commander": False},
                {"slot_id": "S4", "degree_total": 1, "in_degree": 1, "out_degree": 0, "is_commander": False},
                {"slot_id": "S6", "degree_total": 1, "in_degree": 0, "out_degree": 1, "is_commander": False},
                {"slot_id": "S7", "degree_total": 1, "in_degree": 1, "out_degree": 0, "is_commander": False},
                {"slot_id": "S5", "degree_total": 0, "in_degree": 0, "out_degree": 0, "is_commander": False},
            ],
        )

        top_edges = payload.get("top_edges") if isinstance(payload.get("top_edges"), list) else []
        self.assertEqual(
            top_edges,
            [
                {"src": "S0", "dst": "S1", "weight": 7},
                {"src": "S1", "dst": "S2", "weight": 7},
                {"src": "S3", "dst": "S4", "weight": 1},
                {"src": "S6", "dst": "S7", "weight": 1},
                {"src": "S0", "dst": "S2", "weight": None},
            ],
        )

        top_components = payload.get("top_components") if isinstance(payload.get("top_components"), list) else []
        self.assertEqual(
            top_components,
            [
                {"component_id": 1, "node_count": 3, "edge_count": 3, "playable_nodes": 3},
                {"component_id": 2, "node_count": 2, "edge_count": 1, "playable_nodes": 2},
                {"component_id": 3, "node_count": 2, "edge_count": 1, "playable_nodes": 2},
                {"component_id": 4, "node_count": 1, "edge_count": 0, "playable_nodes": 1},
            ],
        )

    def test_deterministic_for_same_input(self) -> None:
        kwargs = {
            "graph_v1": self._synthetic_graph_v1(),
            "deck_slot_ids_playable": ["S7", "S6", "S5", "S4", "S3", "S2", "S1", "S0"],
            "typed_graph_invariants": {"status": "OK"},
            "commander_slot_id": "S1",
        }

        first = run_graph_pathways_summary_v1(**kwargs)
        second = run_graph_pathways_summary_v1(**kwargs)

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
