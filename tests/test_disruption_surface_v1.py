from __future__ import annotations

import unittest
from unittest.mock import patch

import api.engine.layers.disruption_surface_v1 as disruption_surface
from api.engine.layers.disruption_surface_v1 import (
    DISRUPTION_SURFACE_V1_VERSION,
    run_disruption_surface_v1,
)


class DisruptionSurfaceV1Tests(unittest.TestCase):
    def test_skip_when_no_disruption_primitives_defined(self) -> None:
        with patch.object(disruption_surface, "DISRUPTION_PRIMITIVE_IDS", []):
            payload = run_disruption_surface_v1(
                primitive_index_by_slot={"S0": ["STACK_COUNTERSPELL"]},
                deck_slot_ids_playable=["S0"],
            )

        self.assertEqual(payload.get("version"), DISRUPTION_SURFACE_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason"), "NO_DISRUPTION_PRIMITIVES_DEFINED")
        self.assertEqual(payload.get("hub_mapping"), [])

    def test_ok_deck_level_summary(self) -> None:
        payload = run_disruption_surface_v1(
            primitive_index_by_slot={
                "S0": ["STACK_COUNTERSPELL", "RAMP_MANA"],
                "S1": ["TARGETED_REMOVAL_CREATURE", "STACK_COUNTERSPELL"],
                "S2": ["RAMP_MANA"],
                "S3": ["STATIC_TAX_SPELLS"],
            },
            deck_slot_ids_playable=["S2", "S1", "S0"],
            pathways_summary=None,
            typed_graph_invariants=None,
        )

        self.assertEqual(payload.get("version"), DISRUPTION_SURFACE_V1_VERSION)
        self.assertEqual(payload.get("status"), "OK")
        self.assertIsNone(payload.get("reason"))
        self.assertEqual(payload.get("definitions_version"), "disruption_primitives_v1")

        totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
        self.assertEqual(totals.get("disruption_slots"), 2)
        self.assertEqual(totals.get("disruption_primitives_hit"), 2)

        top_primitives = (
            payload.get("top_disruption_primitives")
            if isinstance(payload.get("top_disruption_primitives"), list)
            else []
        )
        self.assertEqual(
            top_primitives,
            [
                {"primitive": "STACK_COUNTERSPELL", "slots": 2},
                {"primitive": "TARGETED_REMOVAL_CREATURE", "slots": 1},
            ],
        )
        self.assertEqual(payload.get("hub_mapping"), [])

    def test_ok_hub_mapping_with_synthetic_pathways(self) -> None:
        payload = run_disruption_surface_v1(
            primitive_index_by_slot={
                "S0": ["STACK_COUNTERSPELL"],
                "S1": ["TARGETED_REMOVAL_CREATURE", "STACK_COUNTERSPELL"],
                "S2": ["BOARDWIPE_CREATURES"],
                "S3": ["RAMP_MANA"],
                "S4": ["STATIC_TAX_SPELLS"],
            },
            deck_slot_ids_playable=["S0", "S1", "S2", "S3", "S4"],
            pathways_summary={
                "status": "OK",
                "top_hubs": [
                    {"slot_id": "S1", "degree_total": 5, "component_id": 1},
                    {"slot_id": "S4", "degree_total": 2, "component_id": 2},
                ],
                "top_components": [
                    {
                        "component_id": 1,
                        "node_count": 3,
                        "edge_count": 2,
                        "playable_nodes": 3,
                        "slot_ids": ["S0", "S1", "S3"],
                    },
                    {
                        "component_id": 2,
                        "node_count": 2,
                        "edge_count": 1,
                        "playable_nodes": 2,
                        "slot_ids": ["S2", "S4"],
                    },
                ],
            },
            typed_graph_invariants={"status": "OK"},
        )

        self.assertEqual(payload.get("status"), "OK")

        hub_mapping = payload.get("hub_mapping") if isinstance(payload.get("hub_mapping"), list) else []
        self.assertEqual(
            hub_mapping,
            [
                {
                    "hub_slot_id": "S1",
                    "hub_degree_total": 5,
                    "disruption_slots_in_component": 2,
                    "top_disruption_primitives_in_component": [
                        {"primitive": "STACK_COUNTERSPELL", "slots": 2},
                        {"primitive": "TARGETED_REMOVAL_CREATURE", "slots": 1},
                    ],
                },
                {
                    "hub_slot_id": "S4",
                    "hub_degree_total": 2,
                    "disruption_slots_in_component": 2,
                    "top_disruption_primitives_in_component": [
                        {"primitive": "BOARDWIPE_CREATURES", "slots": 1},
                        {"primitive": "STATIC_TAX_SPELLS", "slots": 1},
                    ],
                },
            ],
        )

    def test_invariants_error_keeps_deck_level_summary_and_skips_hub_mapping(self) -> None:
        payload = run_disruption_surface_v1(
            primitive_index_by_slot={
                "S0": ["STACK_COUNTERSPELL"],
                "S1": ["TARGETED_REMOVAL_CREATURE", "STACK_COUNTERSPELL"],
                "S2": ["BOARDWIPE_CREATURES"],
            },
            deck_slot_ids_playable=["S0", "S1", "S2"],
            pathways_summary={
                "status": "OK",
                "top_hubs": [
                    {"slot_id": "S1", "degree_total": 5, "component_id": 1},
                ],
                "top_components": [
                    {"component_id": 1, "slot_ids": ["S0", "S1", "S2"]},
                ],
            },
            typed_graph_invariants={"status": "ERROR"},
        )

        self.assertEqual(payload.get("status"), "OK")
        self.assertIsNone(payload.get("reason"))

        totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
        self.assertEqual(totals.get("disruption_slots"), 3)
        self.assertEqual(totals.get("disruption_primitives_hit"), 3)
        self.assertEqual(payload.get("hub_mapping"), [])

    def test_deterministic_for_same_input(self) -> None:
        kwargs = {
            "primitive_index_by_slot": {
                "S0": ["STACK_COUNTERSPELL"],
                "S1": ["TARGETED_REMOVAL_CREATURE", "STACK_COUNTERSPELL"],
                "S2": ["BOARDWIPE_CREATURES"],
                "S3": ["RAMP_MANA"],
                "S4": ["STATIC_TAX_SPELLS"],
            },
            "deck_slot_ids_playable": ["S0", "S1", "S2", "S3", "S4"],
            "pathways_summary": {
                "status": "OK",
                "top_hubs": [
                    {"slot_id": "S1", "degree_total": 5, "component_id": 1},
                    {"slot_id": "S4", "degree_total": 2, "component_id": 2},
                ],
                "top_components": [
                    {"component_id": 1, "slot_ids": ["S0", "S1", "S3"]},
                    {"component_id": 2, "slot_ids": ["S2", "S4"]},
                ],
            },
            "typed_graph_invariants": {"status": "OK"},
        }

        first = run_disruption_surface_v1(**kwargs)
        second = run_disruption_surface_v1(**kwargs)

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
