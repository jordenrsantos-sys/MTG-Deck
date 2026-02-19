from __future__ import annotations

import unittest

from api.engine.graph_expand_v1 import build_bipartite_graph_v1, expand_candidate_edges_v1


class GraphExpandV1DeterminismTests(unittest.TestCase):
    def test_same_inputs_twice_produces_identical_graph_v1(self) -> None:
        deck_slot_ids = ["S3", "S1", "S0", "S2", "S0"]
        primitive_index_by_slot = {
            "S0": ["P2", "P1", "P2"],
            "S1": ["P1", "P3"],
            "S2": ["P3", "P2"],
            "S3": [],
        }
        bounds = {
            "MAX_PRIMS_PER_SLOT": 8,
            "MAX_SLOTS_PER_PRIM": 8,
            "MAX_CARD_CARD_EDGES_TOTAL": 100,
        }

        bipartite_one = build_bipartite_graph_v1(deck_slot_ids, primitive_index_by_slot)
        expanded_one = expand_candidate_edges_v1(bipartite_one, bounds)
        graph_v1_one = {
            "bipartite": bipartite_one,
            "candidate_edges": expanded_one.get("candidate_edges", []),
            "bounds": dict(bounds),
            "stats": {
                "bipartite": bipartite_one.get("stats", {}),
                "expanded": expanded_one.get("stats", {}),
            },
        }

        bipartite_two = build_bipartite_graph_v1(deck_slot_ids, primitive_index_by_slot)
        expanded_two = expand_candidate_edges_v1(bipartite_two, bounds)
        graph_v1_two = {
            "bipartite": bipartite_two,
            "candidate_edges": expanded_two.get("candidate_edges", []),
            "bounds": dict(bounds),
            "stats": {
                "bipartite": bipartite_two.get("stats", {}),
                "expanded": expanded_two.get("stats", {}),
            },
        }

        self.assertEqual(graph_v1_one, graph_v1_two)

        node_order = [node.get("id") for node in bipartite_one.get("nodes", []) if isinstance(node, dict)]
        self.assertEqual(node_order, sorted(node_order, key=lambda value: (value.split(":", 1)[0], value)))

        stats_one = expanded_one.get("stats") if isinstance(expanded_one.get("stats"), dict) else {}
        stats_two = expanded_two.get("stats") if isinstance(expanded_two.get("stats"), dict) else {}
        self.assertEqual(stats_one, stats_two)
        self.assertEqual(
            list(stats_one.keys()),
            [
                "n_slot_nodes",
                "n_prim_nodes",
                "n_bipartite_edges",
                "n_candidate_edges",
                "caps_hit",
            ],
        )
        caps_hit = stats_one.get("caps_hit") if isinstance(stats_one.get("caps_hit"), dict) else {}
        self.assertEqual(
            list(caps_hit.keys()),
            ["max_prims_per_slot", "max_slots_per_prim", "max_edges_total"],
        )

    def test_bounds_enforce_caps_deterministically(self) -> None:
        deck_slot_ids = ["S3", "S2", "S1", "S0"]
        primitive_index_by_slot = {
            "S0": ["P3", "P2", "P1"],
            "S1": ["P3", "P2", "P1"],
            "S2": ["P3", "P2", "P1"],
            "S3": ["P3", "P2", "P1"],
        }

        bipartite = build_bipartite_graph_v1(deck_slot_ids, primitive_index_by_slot)
        expanded = expand_candidate_edges_v1(
            bipartite,
            {
                "MAX_PRIMS_PER_SLOT": 2,
                "MAX_SLOTS_PER_PRIM": 3,
                "MAX_CARD_CARD_EDGES_TOTAL": 2,
            },
        )

        candidate_edges = expanded.get("candidate_edges", [])
        self.assertEqual(
            candidate_edges,
            [
                {
                    "a": "slot:S0",
                    "b": "slot:S1",
                    "kind": "shared_primitive",
                    "shared_primitives": ["P1", "P2"],
                },
                {
                    "a": "slot:S0",
                    "b": "slot:S2",
                    "kind": "shared_primitive",
                    "shared_primitives": ["P1", "P2"],
                },
            ],
        )

        stats = expanded.get("stats") if isinstance(expanded.get("stats"), dict) else {}
        self.assertEqual(
            list(stats.keys()),
            [
                "n_slot_nodes",
                "n_prim_nodes",
                "n_bipartite_edges",
                "n_candidate_edges",
                "caps_hit",
            ],
        )
        self.assertEqual(stats.get("n_slot_nodes"), 4)
        self.assertEqual(stats.get("n_prim_nodes"), 3)
        self.assertEqual(stats.get("n_bipartite_edges"), 12)
        self.assertEqual(stats.get("n_candidate_edges"), 2)

        caps_hit = stats.get("caps_hit") if isinstance(stats.get("caps_hit"), dict) else {}
        self.assertEqual(
            list(caps_hit.keys()),
            ["max_prims_per_slot", "max_slots_per_prim", "max_edges_total"],
        )
        self.assertTrue(bool(caps_hit.get("max_prims_per_slot")))
        self.assertTrue(bool(caps_hit.get("max_slots_per_prim")))
        self.assertTrue(bool(caps_hit.get("max_edges_total")))


if __name__ == "__main__":
    unittest.main()
