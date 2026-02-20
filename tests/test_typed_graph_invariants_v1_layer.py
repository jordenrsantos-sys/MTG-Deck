from __future__ import annotations

import unittest

from api.engine.graph_expand_v1 import build_bipartite_graph_v1, expand_candidate_edges_v1
from api.engine.layers.typed_graph_invariants_v1 import (
    TYPED_GRAPH_INVARIANTS_V1_VERSION,
    run_typed_graph_invariants_v1,
)


_CHECK_KEYS = [
    "graph_present",
    "node_count",
    "edge_count",
    "duplicate_node_ids",
    "dangling_edges",
    "self_edges",
    "invalid_node_refs",
    "bounds_ok",
    "ordering_ok",
]


class TypedGraphInvariantsV1LayerTests(unittest.TestCase):
    def _build_valid_graph_v1(self) -> dict:
        deck_slot_ids = ["S2", "S0", "S1"]
        primitive_index_by_slot = {
            "S0": ["RAMP_MANA", "TOKEN_PRODUCTION"],
            "S1": ["TOKEN_PRODUCTION", "CARD_DRAW"],
            "S2": ["CARD_DRAW"],
        }
        bounds = {
            "MAX_PRIMS_PER_SLOT": 24,
            "MAX_SLOTS_PER_PRIM": 80,
            "MAX_CARD_CARD_EDGES_TOTAL": 5000,
        }

        bipartite = build_bipartite_graph_v1(deck_slot_ids=deck_slot_ids, primitive_index_by_slot=primitive_index_by_slot)
        expanded = expand_candidate_edges_v1(graph=bipartite, bounds=bounds)

        return {
            "bipartite": bipartite,
            "candidate_edges": expanded.get("candidate_edges", []),
            "bounds": dict(bounds),
            "stats": expanded.get("stats", {}),
        }

    def test_skip_when_graph_missing_or_empty(self) -> None:
        for value in (None, {}):
            with self.subTest(graph_v1=value):
                payload = run_typed_graph_invariants_v1(graph_v1=value)
                self.assertEqual(payload.get("version"), TYPED_GRAPH_INVARIANTS_V1_VERSION)
                self.assertEqual(payload.get("status"), "SKIP")
                self.assertEqual(payload.get("errors"), [])

                checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
                self.assertEqual(list(checks.keys()), _CHECK_KEYS)
                self.assertIs(checks.get("graph_present"), False)

    def test_valid_graph_payload_returns_ok_with_deterministic_shape(self) -> None:
        graph_v1 = self._build_valid_graph_v1()

        payload = run_typed_graph_invariants_v1(graph_v1=graph_v1)

        self.assertEqual(list(payload.keys()), ["version", "status", "errors", "checks"])
        self.assertEqual(payload.get("version"), TYPED_GRAPH_INVARIANTS_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("errors"), [])

        checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
        self.assertEqual(list(checks.keys()), _CHECK_KEYS)
        self.assertIs(checks.get("graph_present"), True)
        self.assertEqual(checks.get("node_count"), len(graph_v1["bipartite"]["nodes"]))
        self.assertEqual(
            checks.get("edge_count"),
            len(graph_v1["bipartite"]["edges"]) + len(graph_v1["candidate_edges"]),
        )
        self.assertEqual(checks.get("duplicate_node_ids"), 0)
        self.assertEqual(checks.get("dangling_edges"), 0)
        self.assertEqual(checks.get("self_edges"), 0)
        self.assertEqual(checks.get("invalid_node_refs"), 0)
        self.assertIs(checks.get("bounds_ok"), True)
        self.assertIs(checks.get("ordering_ok"), True)

    def test_invalid_graph_reports_sorted_errors_and_expected_counts(self) -> None:
        graph_v1 = {
            "bipartite": {
                "nodes": [
                    {"id": "slot:S2", "kind": "slot"},
                    {"id": "slot:S1", "kind": "slot"},
                    {"id": "slot:S1", "kind": "slot"},
                    {"kind": "slot"},
                ],
                "edges": [
                    {"a": "slot:S1", "b": "slot:S1", "kind": "has_primitive"},
                    {"a": "slot:S9", "b": "prim:P1", "kind": "has_primitive"},
                    {"a": "slot:S1", "kind": "has_primitive"},
                ],
                "stats": {},
            },
            "candidate_edges": [
                {"a": "slot:S2", "b": "slot:S1", "kind": "shared_primitive", "shared_primitives": ["P1"]},
                {"a": "slot:S1", "b": "slot:S3", "kind": "shared_primitive", "shared_primitives": []},
            ],
            "bounds": {
                "MAX_PRIMS_PER_SLOT": 0,
                "MAX_SLOTS_PER_PRIM": 0,
                "MAX_CARD_CARD_EDGES_TOTAL": 0,
            },
            "stats": {},
        }

        payload = run_typed_graph_invariants_v1(graph_v1=graph_v1)

        self.assertEqual(payload.get("status"), "ERROR")

        checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
        self.assertEqual(list(checks.keys()), _CHECK_KEYS)
        self.assertIs(checks.get("graph_present"), True)
        self.assertEqual(checks.get("node_count"), 4)
        self.assertEqual(checks.get("edge_count"), 5)
        self.assertEqual(checks.get("duplicate_node_ids"), 1)
        self.assertEqual(checks.get("dangling_edges"), 2)
        self.assertEqual(checks.get("self_edges"), 1)
        self.assertEqual(checks.get("invalid_node_refs"), 2)
        self.assertIs(checks.get("bounds_ok"), False)
        self.assertIs(checks.get("ordering_ok"), False)

        errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
        self.assertGreater(len(errors), 0)

        self.assertEqual(
            errors,
            sorted(
                errors,
                key=lambda error: (
                    str((error or {}).get("code") or ""),
                    str((error or {}).get("path") or ""),
                    str((error or {}).get("message") or ""),
                ),
            ),
        )

        error_codes = [entry.get("code") for entry in errors if isinstance(entry, dict)]
        self.assertIn("GRAPH_DUPLICATE_NODE_IDS", error_codes)
        self.assertIn("GRAPH_DANGLING_EDGE", error_codes)
        self.assertIn("GRAPH_SELF_EDGE", error_codes)
        self.assertIn("GRAPH_EDGE_REF_INVALID", error_codes)
        self.assertIn("GRAPH_BOUNDS_EXCEEDED", error_codes)
        self.assertIn("GRAPH_NODE_ORDER_UNSTABLE", error_codes)
        self.assertIn("GRAPH_EDGE_ORDER_UNSTABLE", error_codes)

    def test_deterministic_for_same_input(self) -> None:
        graph_v1 = self._build_valid_graph_v1()

        first = run_typed_graph_invariants_v1(graph_v1=graph_v1)
        second = run_typed_graph_invariants_v1(graph_v1=graph_v1)

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
