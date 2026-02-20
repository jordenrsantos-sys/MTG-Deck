from __future__ import annotations

import unittest

from api.engine.layers.engine_coherence_v1 import (
    ENGINE_COHERENCE_V1_VERSION,
    run_engine_coherence_v1,
)


class EngineCoherenceV1Tests(unittest.TestCase):
    def test_skip_when_primitive_index_unavailable(self) -> None:
        payload = run_engine_coherence_v1(
            primitive_index_by_slot=None,
            deck_slot_ids_playable=["S1"],
        )

        self.assertEqual(payload.get("version"), ENGINE_COHERENCE_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "PRIMITIVE_INDEX_UNAVAILABLE")
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(
            payload.get("summary"),
            {
                "playable_slots_total": 0,
                "non_dead_slots_total": 0,
                "dead_slots_total": 0,
                "dead_slot_ratio": 0.0,
                "primitive_concentration_index": 0.0,
                "overlap_score": 0.0,
            },
        )

    def test_warn_when_dead_slots_present_and_metrics_are_deterministic(self) -> None:
        payload = run_engine_coherence_v1(
            primitive_index_by_slot={
                "S1": ["P_A"],
                "S2": ["P_A", "P_B"],
                "S3": [],
                "S4": ["P_B", "P_C"],
            },
            deck_slot_ids_playable=["S4", "S3", "S2", "S1", "S1", ""],
        )

        self.assertEqual(payload.get("version"), ENGINE_COHERENCE_V1_VERSION)
        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(payload.get("reason_code"), None)
        self.assertEqual(payload.get("codes"), ["DEAD_SLOTS_PRESENT"])

        self.assertEqual(
            payload.get("summary"),
            {
                "playable_slots_total": 4,
                "non_dead_slots_total": 3,
                "dead_slots_total": 1,
                "dead_slot_ratio": 0.25,
                "primitive_concentration_index": 0.666667,
                "overlap_score": 0.277778,
            },
        )
        self.assertEqual(
            payload.get("dead_slots"),
            [
                {
                    "slot_id": "S3",
                    "primitive_count": 0,
                    "primitives": [],
                }
            ],
        )
        self.assertEqual(
            payload.get("top_primitive_concentration"),
            [
                {
                    "primitive": "P_A",
                    "slots_with_primitive": 2,
                    "share": 0.666667,
                },
                {
                    "primitive": "P_B",
                    "slots_with_primitive": 2,
                    "share": 0.666667,
                },
                {
                    "primitive": "P_C",
                    "slots_with_primitive": 1,
                    "share": 0.333333,
                },
            ],
        )

    def test_ok_when_no_dead_slots_and_overlap_computes(self) -> None:
        payload = run_engine_coherence_v1(
            primitive_index_by_slot={
                "S1": ["P_A", "P_B"],
                "S2": ["P_B", "P_C"],
                "S3": ["P_A", "P_C"],
            },
            deck_slot_ids_playable=["S3", "S1", "S2"],
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertEqual(payload.get("reason_code"), None)
        self.assertEqual(payload.get("codes"), [])
        self.assertEqual(
            payload.get("summary"),
            {
                "playable_slots_total": 3,
                "non_dead_slots_total": 3,
                "dead_slots_total": 0,
                "dead_slot_ratio": 0.0,
                "primitive_concentration_index": 0.666667,
                "overlap_score": 0.333333,
            },
        )
        top_rows = payload.get("top_primitive_concentration") if isinstance(payload.get("top_primitive_concentration"), list) else []
        top_primitives = [entry.get("primitive") for entry in top_rows if isinstance(entry, dict)]
        self.assertEqual(top_primitives, ["P_A", "P_B", "P_C"])

    def test_overlap_score_is_independent_from_graph_variants(self) -> None:
        primitive_index_by_slot = {
            "S1": ["P_A", "P_B"],
            "S2": ["P_B", "P_C"],
            "S3": ["P_A", "P_C"],
        }
        deck_slot_ids_playable = ["S3", "S1", "S2"]

        graph_v1_a = {
            "nodes": [{"id": "S1"}, {"id": "S2"}, {"id": "S3"}],
            "edges": [["S1", "S2"]],
        }
        graph_v1_b = {
            "nodes": [{"id": "S1"}, {"id": "S2"}, {"id": "S3"}],
            "edges": [["S1", "S3"], ["S2", "S3"]],
        }

        payload_a = run_engine_coherence_v1(
            primitive_index_by_slot=primitive_index_by_slot,
            deck_slot_ids_playable=deck_slot_ids_playable,
        )
        payload_b = run_engine_coherence_v1(
            primitive_index_by_slot=primitive_index_by_slot,
            deck_slot_ids_playable=deck_slot_ids_playable,
        )

        self.assertNotEqual(graph_v1_a, graph_v1_b)
        self.assertEqual(
            (payload_a.get("summary") or {}).get("overlap_score"),
            (payload_b.get("summary") or {}).get("overlap_score"),
        )
        self.assertEqual(payload_a, payload_b)

    def test_overlap_pair_iteration_is_deterministic_for_different_dict_insertion_orders(self) -> None:
        primitive_index_by_slot_a = {
            "S1": ["P_A", "P_B"],
            "S2": ["P_B", "P_C"],
            "S3": ["P_A", "P_C"],
        }
        primitive_index_by_slot_b = {
            "S3": ["P_C", "P_A", "P_C"],
            "S1": ["P_B", "P_A", "P_A"],
            "S2": ["P_C", "P_B", "P_C"],
        }

        payload_a = run_engine_coherence_v1(
            primitive_index_by_slot=primitive_index_by_slot_a,
            deck_slot_ids_playable=["S3", "S1", "S2", "S1", ""],
        )
        payload_b = run_engine_coherence_v1(
            primitive_index_by_slot=primitive_index_by_slot_b,
            deck_slot_ids_playable=["S2", "S3", "S1"],
        )

        self.assertEqual(
            (payload_a.get("summary") or {}).get("overlap_score"),
            (payload_b.get("summary") or {}).get("overlap_score"),
        )
        self.assertEqual(payload_a, payload_b)

    def test_determinism_repeated_call_identical(self) -> None:
        kwargs = {
            "primitive_index_by_slot": {
                "S1": ["P_A", "P_B"],
                "S2": ["P_B", "P_C"],
                "S3": [],
            },
            "deck_slot_ids_playable": ["S2", "S1", "S3"],
        }

        first = run_engine_coherence_v1(**kwargs)
        second = run_engine_coherence_v1(**kwargs)

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
