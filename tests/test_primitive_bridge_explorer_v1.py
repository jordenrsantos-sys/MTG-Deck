from __future__ import annotations

import unittest

from api.engine.layers.primitive_bridge_explorer_v1 import (
    PRIMITIVE_BRIDGE_EXPLORER_VERSION,
    run_primitive_bridge_explorer_v1,
)


def _primitive_index_by_slot_fixture() -> dict:
    return {
        "C0": ["COMMANDER_SUPPORT", "TOKEN_PRODUCTION"],
        "S1": ["TOKEN_PRODUCTION", "SAC_OUTLET"],
        "S2": ["SAC_OUTLET", "RECURSION_TO_HAND"],
        "S3": ["RECURSION_TO_HAND", "ARISTOCRAT_DRAIN"],
        "S4": ["MANA_RAMP_ARTIFACT_ROCK"],
    }


def _slot_ids_by_primitive_fixture() -> dict:
    return {
        "ARISTOCRAT_DRAIN": ["S3"],
        "COMMANDER_SUPPORT": ["C0"],
        "MANA_RAMP_ARTIFACT_ROCK": ["S4"],
        "RECURSION_TO_HAND": ["S2", "S3"],
        "SAC_OUTLET": ["S1", "S2"],
        "TOKEN_PRODUCTION": ["C0", "S1"],
    }


def _graph_v1_fixture() -> dict:
    return {
        "bipartite": {
            "nodes": [
                {"id": "slot:C0", "kind": "slot"},
                {"id": "slot:S1", "kind": "slot"},
                {"id": "slot:S2", "kind": "slot"},
                {"id": "slot:S3", "kind": "slot"},
                {"id": "slot:S4", "kind": "slot"},
            ],
            "edges": [],
            "stats": {},
        },
        "candidate_edges": [
            {"a": "slot:C0", "b": "slot:S1", "kind": "shared_primitive", "shared_primitives": ["TOKEN_PRODUCTION"]},
            {"a": "slot:S1", "b": "slot:S2", "kind": "shared_primitive", "shared_primitives": ["SAC_OUTLET"]},
            {"a": "slot:S2", "b": "slot:S3", "kind": "shared_primitive", "shared_primitives": ["RECURSION_TO_HAND"]},
            {"a": "slot:S3", "b": "slot:S4", "kind": "shared_primitive", "shared_primitives": []},
        ],
        "bounds": {},
        "stats": {},
    }


class PrimitiveBridgeExplorerV1Tests(unittest.TestCase):
    def _run_payload(self) -> dict:
        return run_primitive_bridge_explorer_v1(
            primitive_index_by_slot=_primitive_index_by_slot_fixture(),
            slot_ids_by_primitive=_slot_ids_by_primitive_fixture(),
            graph_v1=_graph_v1_fixture(),
            required_primitives_v0=["MANA_RAMP_ARTIFACT_ROCK", "CARD_DRAW_BURST"],
            commander_dependency_metadata={
                "engine_requirements_v1": {
                    "commander_dependent": "LOW",
                }
            },
            bridge_amplification_bonus_weight=0.25,
        )

    def test_skip_when_required_inputs_unavailable(self) -> None:
        missing_primitive_index = run_primitive_bridge_explorer_v1(
            primitive_index_by_slot=None,
            slot_ids_by_primitive={},
            graph_v1=_graph_v1_fixture(),
        )
        self.assertEqual(missing_primitive_index.get("version"), PRIMITIVE_BRIDGE_EXPLORER_VERSION)
        self.assertEqual(missing_primitive_index.get("status"), "SKIP")
        self.assertEqual(missing_primitive_index.get("reason_code"), "PRIMITIVE_INDEX_UNAVAILABLE")

        missing_graph = run_primitive_bridge_explorer_v1(
            primitive_index_by_slot=_primitive_index_by_slot_fixture(),
            slot_ids_by_primitive=_slot_ids_by_primitive_fixture(),
            graph_v1=None,
        )
        self.assertEqual(missing_graph.get("status"), "SKIP")
        self.assertEqual(missing_graph.get("reason_code"), "GRAPH_V1_UNAVAILABLE")

    def test_determinism_repeat_call_identical(self) -> None:
        first = self._run_payload()
        second = self._run_payload()
        self.assertEqual(first, second)

    def test_chain_ordering_and_bounds_are_deterministic(self) -> None:
        payload = self._run_payload()

        self.assertEqual(payload.get("version"), PRIMITIVE_BRIDGE_EXPLORER_VERSION)
        self.assertIn(payload.get("status"), {"OK", "WARN"})

        bridge_clusters = payload.get("bridge_clusters_v1") if isinstance(payload.get("bridge_clusters_v1"), list) else []
        self.assertGreaterEqual(len(bridge_clusters), 1)

        expected_cluster_order = sorted(
            bridge_clusters,
            key=lambda row: (
                -float(row.get("bridge_score") or 0.0),
                -float(row.get("novelty_score") or 0.0),
                tuple(row.get("primitive_chain") or []),
                tuple(row.get("slot_ids") or []),
            ),
        )
        self.assertEqual(bridge_clusters, expected_cluster_order)

        primitive_set_signatures = [
            tuple(sorted(set(row.get("primitive_chain") or [])))
            for row in bridge_clusters
            if isinstance(row, dict)
        ]
        self.assertEqual(len(primitive_set_signatures), len(set(primitive_set_signatures)))

        bounds = payload.get("bounds") if isinstance(payload.get("bounds"), dict) else {}
        evaluated = bounds.get("evaluated_chain_candidates")
        self.assertIsInstance(evaluated, int)
        self.assertLessEqual(evaluated, 500)

    def test_input_order_variation_does_not_change_output(self) -> None:
        canonical = run_primitive_bridge_explorer_v1(
            primitive_index_by_slot=_primitive_index_by_slot_fixture(),
            slot_ids_by_primitive=_slot_ids_by_primitive_fixture(),
            graph_v1=_graph_v1_fixture(),
            required_primitives_v0=["MANA_RAMP_ARTIFACT_ROCK", "CARD_DRAW_BURST"],
            commander_dependency_metadata={"engine_requirements_v1": {"commander_dependent": "LOW"}},
            bridge_amplification_bonus_weight=0.25,
        )

        shuffled = run_primitive_bridge_explorer_v1(
            primitive_index_by_slot={
                "S4": ["MANA_RAMP_ARTIFACT_ROCK"],
                "S2": ["RECURSION_TO_HAND", "SAC_OUTLET"],
                "S1": ["SAC_OUTLET", "TOKEN_PRODUCTION"],
                "C0": ["TOKEN_PRODUCTION", "COMMANDER_SUPPORT"],
                "S3": ["ARISTOCRAT_DRAIN", "RECURSION_TO_HAND"],
            },
            slot_ids_by_primitive={
                "TOKEN_PRODUCTION": ["S1", "C0"],
                "ARISTOCRAT_DRAIN": ["S3"],
                "RECURSION_TO_HAND": ["S3", "S2"],
                "MANA_RAMP_ARTIFACT_ROCK": ["S4"],
                "SAC_OUTLET": ["S2", "S1"],
                "COMMANDER_SUPPORT": ["C0"],
            },
            graph_v1={
                "candidate_edges": [
                    {"a": "slot:S3", "b": "slot:S4", "kind": "shared_primitive", "shared_primitives": []},
                    {"a": "slot:S2", "b": "slot:S3", "kind": "shared_primitive", "shared_primitives": ["RECURSION_TO_HAND"]},
                    {"a": "slot:S1", "b": "slot:S2", "kind": "shared_primitive", "shared_primitives": ["SAC_OUTLET"]},
                    {"a": "slot:C0", "b": "slot:S1", "kind": "shared_primitive", "shared_primitives": ["TOKEN_PRODUCTION"]},
                ],
                "bipartite": {"nodes": [], "edges": [], "stats": {}},
                "bounds": {},
                "stats": {},
            },
            required_primitives_v0=["CARD_DRAW_BURST", "MANA_RAMP_ARTIFACT_ROCK"],
            commander_dependency_metadata={"engine_requirements_v1": {"commander_dependent": "LOW"}},
            bridge_amplification_bonus_weight=0.25,
        )

        self.assertEqual(canonical, shuffled)


if __name__ == "__main__":
    unittest.main()
