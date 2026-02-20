from __future__ import annotations

import unittest

from api.engine.graph_expand_v1 import build_bipartite_graph_v1, expand_candidate_edges_v1
from api.engine.layers.graph_v1_schema_assert_v1 import (
    GRAPH_V1_SCHEMA_ASSERT_V1_VERSION,
    run_graph_v1_schema_assert_v1,
)


class GraphV1SchemaAssertV1Tests(unittest.TestCase):
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

    def test_valid_graph_payload_returns_ok(self) -> None:
        payload = run_graph_v1_schema_assert_v1(self._build_valid_graph_v1())

        self.assertEqual(payload.get("version"), GRAPH_V1_SCHEMA_ASSERT_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("reason_code"), None)
        self.assertEqual(payload.get("codes"), [])

    def test_missing_required_top_level_key_returns_error(self) -> None:
        graph_v1 = self._build_valid_graph_v1()
        graph_v1.pop("bounds", None)

        payload = run_graph_v1_schema_assert_v1(graph_v1)

        self.assertEqual(payload.get("status"), "ERROR")
        self.assertEqual(payload.get("reason_code"), "GRAPH_V1_SCHEMA_ASSERT_FAILED")
        codes = payload.get("codes") if isinstance(payload.get("codes"), list) else []
        self.assertIn("GRAPH_V1_REQUIRED_TOP_LEVEL_KEY_MISSING", codes)

    def test_extra_top_level_key_returns_error(self) -> None:
        graph_v1 = self._build_valid_graph_v1()
        graph_v1["extra_field"] = {"not": "allowed"}

        payload = run_graph_v1_schema_assert_v1(graph_v1)

        self.assertEqual(payload.get("status"), "ERROR")
        codes = payload.get("codes") if isinstance(payload.get("codes"), list) else []
        self.assertIn("GRAPH_V1_TOP_LEVEL_EXTRA_KEYS", codes)

    def test_codes_are_sorted_and_deterministic(self) -> None:
        graph_v1 = {
            "bipartite": {
                "nodes": [{"id": "slot:S1", "kind": "slot", "extra": True}],
                "edges": [{"a": "slot:S1", "kind": "has_primitive"}],
                "stats": {},
            },
            "candidate_edges": [{"a": "slot:S1", "b": "slot:S2", "kind": "bad_kind", "shared_primitives": []}],
            "bounds": {},
            "stats": {},
            "unknown": 1,
        }

        first = run_graph_v1_schema_assert_v1(graph_v1)
        second = run_graph_v1_schema_assert_v1(graph_v1)

        self.assertEqual(first, second)
        codes = first.get("codes") if isinstance(first.get("codes"), list) else []
        self.assertEqual(codes, sorted(codes))


if __name__ == "__main__":
    unittest.main()
