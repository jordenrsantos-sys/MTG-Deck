from __future__ import annotations

import unittest

from api.engine.layers.counterfactual_stress_test_v1 import (
    COUNTERFACTUAL_STRESS_TEST_V1_VERSION,
    run_counterfactual_stress_test_v1,
)


def _synthetic_graph_v1() -> dict:
    return {
        "bipartite": {
            "nodes": [
                {"id": "slot:C0", "kind": "slot", "slot_id": "C0"},
                {"id": "slot:S1", "kind": "slot", "slot_id": "S1"},
                {"id": "slot:S2", "kind": "slot", "slot_id": "S2"},
                {"id": "slot:S3", "kind": "slot", "slot_id": "S3"},
                {"id": "prim:SELF_MILL", "kind": "prim"},
                {"id": "prim:RECURSION_TO_HAND", "kind": "prim"},
                {"id": "prim:MANA_RAMP_ARTIFACT_ROCK", "kind": "prim"},
            ],
            "edges": [
                {"kind": "slot_prim", "a": "slot:C0", "b": "prim:SELF_MILL"},
                {"kind": "slot_prim", "a": "slot:S1", "b": "prim:SELF_MILL"},
                {"kind": "slot_prim", "a": "slot:C0", "b": "prim:RECURSION_TO_HAND"},
                {"kind": "slot_prim", "a": "slot:S1", "b": "prim:MANA_RAMP_ARTIFACT_ROCK"},
                {"kind": "slot_prim", "a": "slot:S2", "b": "prim:MANA_RAMP_ARTIFACT_ROCK"},
            ],
            "stats": {},
        },
        "candidate_edges": [
            {"kind": "candidate", "a": "slot:C0", "b": "slot:S1"},
            {"kind": "candidate", "a": "slot:S1", "b": "slot:S2"},
            {"kind": "candidate", "a": "slot:S2", "b": "slot:S3"},
        ],
        "bounds": {},
        "stats": {},
    }


def _synthetic_primitive_index() -> dict:
    return {
        "C0": ["SELF_MILL", "RECURSION_TO_HAND"],
        "S1": ["SELF_MILL", "MANA_RAMP_ARTIFACT_ROCK"],
        "S2": ["MANA_RAMP_ARTIFACT_ROCK"],
        "S3": ["CARD_DRAW_BURST"],
    }


class CounterfactualStressTestV1Tests(unittest.TestCase):
    def test_skip_when_graph_missing_or_invariants_error(self) -> None:
        missing_graph = run_counterfactual_stress_test_v1(
            graph_v1=None,
            primitive_index_by_slot=_synthetic_primitive_index(),
            deck_slot_ids_playable=["C0", "S1", "S2", "S3"],
        )
        self.assertEqual(missing_graph.get("version"), COUNTERFACTUAL_STRESS_TEST_V1_VERSION)
        self.assertEqual(missing_graph.get("status"), "SKIP")
        self.assertEqual(missing_graph.get("reason"), "GRAPH_MISSING")
        self.assertEqual(missing_graph.get("scenarios"), [])

        invariants_error = run_counterfactual_stress_test_v1(
            graph_v1=_synthetic_graph_v1(),
            primitive_index_by_slot=_synthetic_primitive_index(),
            deck_slot_ids_playable=["C0", "S1", "S2", "S3"],
            typed_graph_invariants={"status": "ERROR"},
        )
        self.assertEqual(invariants_error.get("status"), "SKIP")
        self.assertEqual(invariants_error.get("reason"), "GRAPH_INVARIANTS_ERROR")
        self.assertEqual(invariants_error.get("scenarios"), [])

    def test_ok_case_validates_lost_fraction_and_ordering(self) -> None:
        payload = run_counterfactual_stress_test_v1(
            graph_v1=_synthetic_graph_v1(),
            primitive_index_by_slot=_synthetic_primitive_index(),
            deck_slot_ids_playable=["S3", "S2", "C0", "S1"],
            typed_graph_invariants={"status": "OK"},
            pathways={
                "status": "OK",
                "top_hubs": [
                    {"slot_id": "S2", "degree_total": 4},
                    {"slot_id": "S1", "degree_total": 5},
                    {"slot_id": "S3", "degree_total": 3},
                ],
            },
            commander_slot_id="C0",
        )

        self.assertEqual(payload.get("version"), COUNTERFACTUAL_STRESS_TEST_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertIsNone(payload.get("reason"))

        scenarios = payload.get("scenarios") if isinstance(payload.get("scenarios"), list) else []
        self.assertEqual(len(scenarios), 13)

        scenario_ids = [entry.get("scenario_id") for entry in scenarios if isinstance(entry, dict)]
        self.assertEqual(
            scenario_ids,
            [
                "remove_commander_slot",
                "remove_top_hub_1",
                "remove_top_hub_2",
                "remove_top_hub_3",
                "remove_primitive_artifact_recursion",
                "remove_primitive_cast_from_graveyard",
                "remove_primitive_reanimation_to_battlefield",
                "remove_primitive_recursion_to_hand",
                "remove_primitive_self_mill",
                "remove_primitive_spell_recursion_instant_sorc",
                "remove_primitive_tutor_to_graveyard",
                "remove_primitive_artifact_reliance",
                "remove_primitive_enchantment_reliance",
            ],
        )

        scenario_map = {
            entry.get("scenario_id"): entry
            for entry in scenarios
            if isinstance(entry, dict) and isinstance(entry.get("scenario_id"), str)
        }

        commander_metrics = (
            (scenario_map.get("remove_commander_slot") or {}).get("metrics")
            if isinstance((scenario_map.get("remove_commander_slot") or {}).get("metrics"), dict)
            else {}
        )
        self.assertEqual(
            commander_metrics,
            {
                "playable_nodes_before": 4,
                "playable_nodes_after": 3,
                "lost_nodes": 1,
                "lost_fraction": 0.25,
            },
        )

        self_mill_metrics = (
            (scenario_map.get("remove_primitive_self_mill") or {}).get("metrics")
            if isinstance((scenario_map.get("remove_primitive_self_mill") or {}).get("metrics"), dict)
            else {}
        )
        self.assertEqual(
            self_mill_metrics,
            {
                "playable_nodes_before": 4,
                "playable_nodes_after": 2,
                "lost_nodes": 2,
                "lost_fraction": 0.5,
            },
        )

        artifact_metrics = (
            (scenario_map.get("remove_primitive_artifact_reliance") or {}).get("metrics")
            if isinstance((scenario_map.get("remove_primitive_artifact_reliance") or {}).get("metrics"), dict)
            else {}
        )
        self.assertEqual(
            artifact_metrics,
            {
                "playable_nodes_before": 4,
                "playable_nodes_after": 1,
                "lost_nodes": 3,
                "lost_fraction": 0.75,
            },
        )

        enchantment_notes = (
            (scenario_map.get("remove_primitive_enchantment_reliance") or {}).get("notes")
            if isinstance((scenario_map.get("remove_primitive_enchantment_reliance") or {}).get("notes"), list)
            else []
        )
        self.assertEqual(
            enchantment_notes,
            [
                {
                    "code": "PRIMITIVE_NOT_PRESENT_IN_PLAYABLE_SLOTS",
                    "message": "Primitive does not appear in playable slots; scenario is no-op.",
                }
            ],
        )

    def test_determinism_repeated_calls_identical(self) -> None:
        kwargs = {
            "graph_v1": _synthetic_graph_v1(),
            "primitive_index_by_slot": _synthetic_primitive_index(),
            "deck_slot_ids_playable": ["S3", "S2", "C0", "S1"],
            "typed_graph_invariants": {"status": "OK"},
            "pathways": {
                "status": "OK",
                "top_hubs": [
                    {"slot_id": "S2", "degree_total": 4},
                    {"slot_id": "S1", "degree_total": 5},
                    {"slot_id": "S3", "degree_total": 3},
                ],
            },
            "commander_slot_id": "C0",
        }

        first = run_counterfactual_stress_test_v1(**kwargs)
        second = run_counterfactual_stress_test_v1(**kwargs)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
