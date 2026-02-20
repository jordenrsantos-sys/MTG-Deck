from __future__ import annotations

import unittest

from api.engine.pipeline_build import build_available_panels_v1


_AVAILABLE_PANEL_KEYS = [
    "has_canonical_slots",
    "has_unknowns_canonical",
    "has_deck_cards_summary",
    "has_rules_db",
    "has_rules_topic_selection_trace",
    "has_proof_scaffolds",
    "has_proof_attempts",
    "has_primitive_index",
    "has_structural_reporting",
    "has_graph",
    "has_typed_graph_invariants_v1",
    "has_profile_bracket_enforcement_v1",
    "has_bracket_compliance_summary_v1",
    "has_graph_analytics_summary_v1",
    "has_graph_pathways_summary_v1",
    "has_disruption_surface_v1",
    "has_vulnerability_index_v1",
    "has_engine_requirement_detection_v1",
    "has_engine_coherence_v1",
    "has_mulligan_model_v1",
    "has_substitution_engine_v1",
    "has_weight_multiplier_engine_v1",
    "has_probability_math_core_v1",
    "has_probability_checkpoint_layer_v1",
    "has_stress_model_definition_v1",
    "has_stress_transform_engine_v1",
    "has_resilience_math_engine_v1",
    "has_commander_reliability_model_v1",
    "has_sufficiency_summary_v1",
    "has_required_effects_coverage_v1",
    "has_redundancy_index_v1",
    "has_counterfactual_stress_test_v1",
    "has_structural_scorecard_v1",
    "has_motifs",
    "has_disruption",
    "has_pathways",
    "has_combo_skeleton",
    "has_combo_candidates",
    "has_patch_loop",
    "has_snapshot_preflight_v1",
]


class AvailablePanelsV1Tests(unittest.TestCase):
    def test_all_flags_false_when_payloads_missing_or_empty(self) -> None:
        baseline = build_available_panels_v1()
        self.assertEqual(list(baseline.keys()), _AVAILABLE_PANEL_KEYS)
        self.assertTrue(all(flag is False for flag in baseline.values()))

        explicit_empty = build_available_panels_v1(
            deck_cards_canonical_input_order=[],
            unknowns_canonical=[],
            deck_cards_playable=[],
            deck_cards_nonplayable=[],
            deck_cards_unknown=[],
            rules_db_available_for_build=False,
            rules_topic_selection_trace="",
            combo_proof_scaffolds_v0=[],
            combo_proof_attempts_v0=[],
            primitive_index_by_slot={},
            structural_snapshot_v1={},
            graph_v1={},
            graph_nodes=[],
            graph_edges=[],
            graph_components=[],
            motifs=[],
            disruption_totals={},
            disruption_articulation_nodes=[],
            disruption_bridge_edges=[],
            disruption_node_impact=[],
            disruption_commander_risk={},
            pathways_totals={},
            pathways_commander_distances={},
            pathways_commander_reachable_slots=[],
            pathways_commander_unreachable_slots=[],
            pathways_hubs=[],
            pathways_commander_bridge_candidates=[],
            combo_skeleton_components=[],
            combo_skeleton_totals={},
            combo_candidates_v0=[],
            patch_loop_v0={},
            patch_error_v0=None,
            snapshot_preflight_v1={},
            typed_graph_invariants_v1={},
            profile_bracket_enforcement_v1={},
            vulnerability_index_v1={},
            engine_requirement_detection_v1={},
            engine_coherence_v1={},
            mulligan_model_v1={},
            substitution_engine_v1={},
            weight_multiplier_engine_v1={},
            probability_math_core_v1={},
            probability_checkpoint_layer_v1={},
            stress_model_definition_v1={},
            stress_transform_engine_v1={},
            resilience_math_engine_v1={},
            commander_reliability_model_v1={},
            sufficiency_summary_v1={},
            required_effects_coverage_v1={},
            redundancy_index_v1={},
            counterfactual_stress_test_v1={},
            structural_scorecard_v1={},
        )
        self.assertTrue(all(flag is False for flag in explicit_empty.values()))

    def test_single_payload_flips_only_its_corresponding_flag(self) -> None:
        cases = [
            ("has_canonical_slots", {"deck_cards_canonical_input_order": [{"slot_id": "S1"}]}),
            ("has_unknowns_canonical", {"unknowns_canonical": [{"code": "X"}]}),
            ("has_deck_cards_summary", {"deck_cards_playable": [{"slot_id": "S1"}]}),
            ("has_rules_db", {"rules_db_available_for_build": True}),
            ("has_rules_topic_selection_trace", {"rules_topic_selection_trace": {"topic": "PRIORITY"}}),
            ("has_proof_scaffolds", {"combo_proof_scaffolds_v0": [{"candidate_id": "cand-1"}]}),
            ("has_proof_attempts", {"combo_proof_attempts_v0": [{"attempt_id": "att-1"}]}),
            ("has_primitive_index", {"primitive_index_by_slot": {"S1": ["RAMP_MANA"]}}),
            ("has_structural_reporting", {"structural_snapshot_v1": {"deck_size": 100}}),
            ("has_graph", {"graph_v1": {"graph_nodes": ["S1"]}}),
            (
                "has_typed_graph_invariants_v1",
                {
                    "typed_graph_invariants_v1": {
                        "version": "typed_graph_invariants_v1",
                        "status": "OK",
                        "errors": [],
                        "checks": {
                            "graph_present": True,
                            "node_count": 2,
                            "edge_count": 1,
                            "duplicate_node_ids": 0,
                            "dangling_edges": 0,
                            "self_edges": 0,
                            "invalid_node_refs": 0,
                            "bounds_ok": True,
                            "ordering_ok": True,
                        },
                    }
                },
            ),
            (
                "has_profile_bracket_enforcement_v1",
                {
                    "profile_bracket_enforcement_v1": {
                        "version": "profile_bracket_enforcement_v1",
                        "status": "OK",
                        "profile_id": "focused",
                        "bracket_id": "B2",
                        "profile_definition_version": "profile_defaults_v1_10",
                        "bracket_definition_version": "bracket_v0",
                        "game_changers_version": "gc_v0_userlist_2025-11-20",
                        "counts": {
                            "deck_size_total": 100,
                            "game_changers_in_deck": 0,
                        },
                        "violations": [],
                        "unknowns": [],
                    }
                },
            ),
            (
                "has_bracket_compliance_summary_v1",
                {
                    "bracket_compliance_summary_v1": {
                        "version": "bracket_compliance_summary_v1",
                        "status": "OK",
                        "bracket_id": "B3",
                        "counts": {
                            "game_changers": 1,
                            "mass_land_denial": 0,
                            "extra_turns": 0,
                            "two_card_combos": 2,
                        },
                        "violations": [],
                        "flags": [
                            {
                                "code": "TWO_CARD_COMBOS_PRESENT_TRACK_ONLY",
                                "category": "two_card_combos",
                                "message": "track-only",
                            }
                        ],
                        "unknowns": [],
                        "versions": {
                            "gc_limits_version": "gc_limits_v1",
                            "bracket_rules_version": "bracket_rules_v2",
                            "two_card_combos_version": "two_card_combos_v2",
                        },
                    }
                },
            ),
            (
                "has_graph_analytics_summary_v1",
                {
                    "graph_analytics_summary_v1": {
                        "version": "graph_analytics_summary_v1",
                        "status": "OK",
                        "reason": None,
                        "counts": {
                            "nodes": 4,
                            "edges": 2,
                            "playable_nodes": 2,
                        },
                        "top_primitives_by_slot_coverage": [
                            {"primitive": "RAMP_MANA", "slots": 2},
                        ],
                        "connectivity": {
                            "avg_out_degree": 0.5,
                            "avg_in_degree": 0.5,
                            "max_out_degree": 1,
                            "max_in_degree": 1,
                        },
                        "components": {
                            "component_count": 2,
                            "largest_component_nodes": 3,
                            "largest_component_edges": 2,
                        },
                    }
                },
            ),
            (
                "has_graph_pathways_summary_v1",
                {
                    "graph_pathways_summary_v1": {
                        "version": "graph_pathways_summary_v1",
                        "status": "OK",
                        "reason": None,
                        "top_hubs": [
                            {
                                "slot_id": "S1",
                                "degree_total": 3,
                                "in_degree": 1,
                                "out_degree": 2,
                                "is_commander": False,
                            }
                        ],
                        "top_edges": [
                            {
                                "src": "S0",
                                "dst": "S1",
                                "weight": None,
                            }
                        ],
                        "top_components": [
                            {
                                "component_id": 1,
                                "node_count": 2,
                                "edge_count": 1,
                                "playable_nodes": 2,
                            }
                        ],
                    }
                },
            ),
            (
                "has_disruption_surface_v1",
                {
                    "disruption_surface_v1": {
                        "version": "disruption_surface_v1",
                        "status": "OK",
                        "reason": None,
                        "definitions_version": "disruption_primitives_v1",
                        "totals": {
                            "disruption_slots": 2,
                            "disruption_primitives_hit": 2,
                        },
                        "top_disruption_primitives": [
                            {
                                "primitive": "STACK_COUNTERSPELL",
                                "slots": 2,
                            }
                        ],
                        "hub_mapping": [],
                    }
                },
            ),
            (
                "has_vulnerability_index_v1",
                {
                    "vulnerability_index_v1": {
                        "version": "vulnerability_index_v1",
                        "status": "OK",
                        "reason": None,
                        "scores": {
                            "graveyard_reliance": 0.25,
                            "commander_dependence": 0.1,
                            "single_engine_reliance": 0.4,
                            "setup_dependency": 0.3,
                            "interaction_exposure": 0.8,
                        },
                        "signals": {
                            "graveyard_slots": 2,
                            "recursion_slots": 1,
                            "commander_dependency_signal": 0.1,
                            "top_primitive_concentration": [
                                {
                                    "primitive": "TUTOR_ANY_TO_HAND",
                                    "share": 0.4,
                                }
                            ],
                        },
                        "notes": [],
                    }
                },
            ),
            (
                "has_engine_requirement_detection_v1",
                {
                    "engine_requirement_detection_v1": {
                        "version": "engine_requirement_detection_v1",
                        "status": "WARN",
                        "reason_code": None,
                        "codes": [
                            "ENGINE_REQ_MANA_HUNGRY_UNIMPLEMENTED",
                            "ENGINE_REQ_PERMANENT_TYPE_UNIMPLEMENTED",
                            "ENGINE_REQ_SHUFFLE_UNIMPLEMENTED",
                        ],
                        "unknowns": [],
                        "engine_requirements_v1": {
                            "GRAVEYARD_DEPENDENT": False,
                            "SAC_BASED": True,
                            "LANDFALL_BASED": False,
                            "SPELL_DENSITY_BASED": True,
                            "TOKEN_BASED": False,
                            "commander_dependent": "UNKNOWN",
                            "mana_hungry": False,
                            "requires_shuffle": False,
                            "requires_specific_permanent_type": [],
                        },
                    }
                },
            ),
            (
                "has_engine_coherence_v1",
                {
                    "engine_coherence_v1": {
                        "version": "engine_coherence_v1",
                        "status": "WARN",
                        "reason_code": None,
                        "codes": ["DEAD_SLOTS_PRESENT"],
                        "summary": {
                            "playable_slots_total": 3,
                            "non_dead_slots_total": 2,
                            "dead_slots_total": 1,
                            "dead_slot_ratio": 0.333333,
                            "primitive_concentration_index": 1.0,
                            "overlap_score": 0.0,
                        },
                        "dead_slots": [
                            {
                                "slot_id": "S2",
                                "primitive_count": 0,
                                "primitives": [],
                            }
                        ],
                        "top_primitive_concentration": [
                            {
                                "primitive": "MANA_RAMP_ARTIFACT_ROCK",
                                "slots_with_primitive": 2,
                                "share": 1.0,
                            }
                        ],
                    }
                },
            ),
            (
                "has_mulligan_model_v1",
                {
                    "mulligan_model_v1": {
                        "version": "mulligan_model_v1",
                        "status": "OK",
                        "reason_code": None,
                        "codes": [],
                        "assumptions_version": "mulligan_assumptions_v1",
                        "format": "commander",
                        "default_policy": "NORMAL",
                        "checkpoints": [7, 9, 10, 12],
                        "policy_effective_n": [
                            {
                                "policy": "NORMAL",
                                "effective_n_by_checkpoint": [
                                    {"checkpoint": 7, "effective_n": 7.0},
                                    {"checkpoint": 9, "effective_n": 9.0},
                                    {"checkpoint": 10, "effective_n": 10.0},
                                    {"checkpoint": 12, "effective_n": 12.0},
                                ],
                            }
                        ],
                    }
                },
            ),
            (
                "has_substitution_engine_v1",
                {
                    "substitution_engine_v1": {
                        "version": "substitution_engine_v1",
                        "status": "OK",
                        "reason_code": None,
                        "codes": [],
                        "substitutions_version": "bucket_substitutions_v1",
                        "format": "commander",
                        "buckets": [
                            {
                                "bucket": "RAMP",
                                "k_primary": 10,
                                "effective_K": 12.5,
                                "K_int": 12,
                                "active_requirement_flags": ["LANDFALL_BASED"],
                                "substitution_terms": [
                                    {
                                        "primitive": "EXTRA_LAND_DROPS",
                                        "weight": 0.4,
                                        "k_substitute": 5,
                                        "contribution": 2.0,
                                    }
                                ],
                            }
                        ],
                    }
                },
            ),
            (
                "has_weight_multiplier_engine_v1",
                {
                    "weight_multiplier_engine_v1": {
                        "version": "weight_multiplier_engine_v1",
                        "status": "OK",
                        "reason_code": None,
                        "codes": [],
                        "weight_rules_version": "weight_rules_v1",
                        "format": "commander",
                        "multipliers_by_bucket": [
                            {
                                "bucket": "RAMP",
                                "multiplier": 1.12,
                            }
                        ],
                        "applied_rules": [
                            {
                                "rule_id": "ramp_landfall_bonus",
                                "target_bucket": "RAMP",
                                "requirement_flag": "LANDFALL_BASED",
                                "multiplier": 1.12,
                            }
                        ],
                    }
                },
            ),
            (
                "has_probability_math_core_v1",
                {
                    "probability_math_core_v1": {
                        "version": "probability_math_core_v1",
                        "status": "OK",
                        "reason_code": None,
                        "codes": [],
                        "math_backend": "int_comb",
                        "available_functions": [
                            "comb",
                            "hypergeom_p_ge_1",
                            "hypergeom_p_ge_x",
                        ],
                        "validated_buckets": 1,
                    }
                },
            ),
            (
                "has_probability_checkpoint_layer_v1",
                {
                    "probability_checkpoint_layer_v1": {
                        "version": "probability_checkpoint_layer_v1",
                        "status": "OK",
                        "reason_code": None,
                        "codes": [],
                        "format": "commander",
                        "deck_size_N": 99,
                        "default_policy": "NORMAL",
                        "checkpoints": [7, 9, 10, 12],
                        "checkpoint_draws": [
                            {"checkpoint": 7, "effective_n": 7.0, "n_int": 7},
                            {"checkpoint": 9, "effective_n": 9.0, "n_int": 9},
                            {"checkpoint": 10, "effective_n": 10.0, "n_int": 10},
                            {"checkpoint": 12, "effective_n": 12.0, "n_int": 12},
                        ],
                        "probabilities_by_bucket": [
                            {
                                "bucket": "RAMP",
                                "effective_K": 12.5,
                                "K_int": 12,
                                "probabilities_by_checkpoint": [
                                    {"checkpoint": 7, "effective_n": 7.0, "n_int": 7, "p_ge_1": 0.615148},
                                    {"checkpoint": 9, "effective_n": 9.0, "n_int": 9, "p_ge_1": 0.72774},
                                    {"checkpoint": 10, "effective_n": 10.0, "n_int": 10, "p_ge_1": 0.775732},
                                    {"checkpoint": 12, "effective_n": 12.0, "n_int": 12, "p_ge_1": 0.851995},
                                ],
                            }
                        ],
                    }
                },
            ),
            (
                "has_stress_model_definition_v1",
                {
                    "stress_model_definition_v1": {
                        "version": "stress_model_definition_v1",
                        "status": "OK",
                        "reason_code": None,
                        "codes": [],
                        "stress_models_version": "stress_models_v1",
                        "format": "commander",
                        "profile_id": "focused",
                        "bracket_id": "B2",
                        "request_override_model_id": None,
                        "selected_model_id": "PUNISHING_POD_V0",
                        "selection_source": "profile_bracket",
                        "operators": [
                            {
                                "op": "BOARD_WIPE",
                                "by_turn": 6,
                                "surviving_engine_fraction": 0.6,
                            },
                            {
                                "op": "GRAVEYARD_HATE_WINDOW",
                                "turns": [4, 5, 6],
                                "graveyard_penalty": 0.7,
                            },
                            {
                                "op": "STAX_TAX",
                                "by_turn": 4,
                                "inflation_factor": 1.25,
                            },
                            {
                                "op": "TARGETED_REMOVAL",
                                "count": 2,
                            },
                        ],
                    }
                },
            ),
            (
                "has_stress_transform_engine_v1",
                {
                    "stress_transform_engine_v1": {
                        "version": "stress_transform_engine_v1",
                        "status": "OK",
                        "reason_code": None,
                        "codes": [],
                        "format": "commander",
                        "deck_size_N": 99,
                        "selected_model_id": "PUNISHING_POD_V0",
                        "operators_applied": [
                            {"op": "TARGETED_REMOVAL", "count": 2},
                        ],
                        "checkpoints": [7, 9, 10, 12],
                        "checkpoint_draws": [
                            {"checkpoint": 7, "effective_n": 7.0, "n_int": 7},
                            {"checkpoint": 9, "effective_n": 9.0, "n_int": 9},
                            {"checkpoint": 10, "effective_n": 10.0, "n_int": 10},
                            {"checkpoint": 12, "effective_n": 12.0, "n_int": 12},
                        ],
                        "stress_adjusted_effective_K": [
                            {
                                "bucket": "RAMP",
                                "effective_K_before": 12.5,
                                "K_int_before": 12,
                                "effective_K_after": 10.5,
                                "K_int_after": 10,
                            }
                        ],
                        "stress_adjusted_probabilities_by_bucket": [
                            {
                                "bucket": "RAMP",
                                "effective_K_after": 10.5,
                                "K_int_after": 10,
                                "probabilities_by_checkpoint": [
                                    {"checkpoint": 7, "effective_n": 7.0, "n_int": 7, "p_ge_1": 0.542398},
                                    {"checkpoint": 9, "effective_n": 9.0, "n_int": 9, "p_ge_1": 0.651245},
                                    {"checkpoint": 10, "effective_n": 10.0, "n_int": 10, "p_ge_1": 0.699663},
                                    {"checkpoint": 12, "effective_n": 12.0, "n_int": 12, "p_ge_1": 0.778733},
                                ],
                            }
                        ],
                        "operator_impacts": [],
                    }
                },
            ),
            (
                "has_resilience_math_engine_v1",
                {
                    "resilience_math_engine_v1": {
                        "version": "resilience_math_engine_v1",
                        "status": "WARN",
                        "reason_code": None,
                        "codes": ["RESILIENCE_COMMANDER_FRAGILITY_UNAVAILABLE"],
                        "format": "commander",
                        "checkpoints": [7, 9, 10, 12],
                        "commander_dependency": "MED",
                        "metrics": {
                            "engine_continuity_after_removal": 0.81,
                            "rebuild_after_wipe": 0.75,
                            "graveyard_fragility_delta": 0.14,
                            "commander_fragility_delta": None,
                        },
                        "bucket_metrics": [
                            {
                                "bucket": "RAMP",
                                "baseline_effective_K": 12.5,
                                "stress_effective_K": 10.5,
                                "baseline_p_ge_1_mean": 0.742654,
                                "stress_p_ge_1_mean": 0.66801,
                                "stress_delta_p_ge_1_mean": 0.074644,
                            }
                        ],
                    }
                },
            ),
            (
                "has_commander_reliability_model_v1",
                {
                    "commander_reliability_model_v1": {
                        "version": "commander_reliability_model_v1",
                        "status": "WARN",
                        "reason_code": None,
                        "codes": ["COMMANDER_RELIABILITY_PROTECTION_PROXY_UNAVAILABLE"],
                        "commander_dependent": "HIGH",
                        "checkpoint_mapping": {"t3": 9, "t4": 10, "t6": 12},
                        "metrics": {
                            "cast_reliability_t3": 0.5,
                            "cast_reliability_t4": 0.6,
                            "cast_reliability_t6": 0.8,
                            "protection_coverage_proxy": None,
                            "commander_fragility_delta": 0.15,
                        },
                        "notes": [
                            "Commander dependency is HIGH while protection coverage proxy is unavailable.",
                            "Protection coverage proxy unavailable from primitive index/playable slot inputs.",
                        ],
                    }
                },
            ),
            (
                "has_sufficiency_summary_v1",
                {
                    "sufficiency_summary_v1": {
                        "version": "sufficiency_summary_v1",
                        "status": "WARN",
                        "reason_code": None,
                        "codes": [
                            "SUFFICIENCY_COMMANDER_PROTECTION_PROXY_UNAVAILABLE",
                            "SUFFICIENCY_REQUIRED_EFFECTS_SOURCE_WARN",
                        ],
                        "failures": [],
                        "warnings": [
                            "SUFFICIENCY_COMMANDER_PROTECTION_PROXY_UNAVAILABLE",
                            "SUFFICIENCY_REQUIRED_EFFECTS_SOURCE_WARN",
                        ],
                        "domain_verdicts": {
                            "required_effects": {
                                "status": "WARN",
                                "codes": ["SUFFICIENCY_REQUIRED_EFFECTS_SOURCE_WARN"],
                            },
                            "baseline_prob": {"status": "PASS", "codes": []},
                            "stress_prob": {"status": "PASS", "codes": []},
                            "coherence": {"status": "PASS", "codes": []},
                            "resilience": {"status": "PASS", "codes": []},
                            "commander": {
                                "status": "WARN",
                                "codes": ["SUFFICIENCY_COMMANDER_PROTECTION_PROXY_UNAVAILABLE"],
                            },
                        },
                        "thresholds_used": {
                            "profile_thresholds_version": "profile_thresholds_v1",
                            "calibration_snapshot_version": "calibration_snapshot_v1",
                            "format": "commander",
                            "requested_profile_id": "focused",
                            "selected_profile_id": "focused",
                            "selection_source": "profile",
                            "domains": {},
                        },
                        "versions_used": {
                            "sufficiency_summary_version": "sufficiency_summary_v1",
                            "profile_thresholds_version": "profile_thresholds_v1",
                            "calibration_snapshot_version": "calibration_snapshot_v1",
                        },
                    }
                },
            ),
            (
                "has_required_effects_coverage_v1",
                {
                    "required_effects_coverage_v1": {
                        "version": "required_effects_coverage_v1",
                        "status": "WARN",
                        "reason": None,
                        "requirements_version": "required_effects_v1",
                        "coverage": [
                            {
                                "primitive": "MANA_RAMP_ARTIFACT_ROCK",
                                "min": 10,
                                "count": 6,
                                "supported": True,
                                "met": False,
                            }
                        ],
                        "missing": [
                            {
                                "primitive": "MANA_RAMP_ARTIFACT_ROCK",
                                "min": 10,
                                "count": 6,
                            }
                        ],
                        "unknowns": [],
                    }
                },
            ),
            (
                "has_redundancy_index_v1",
                {
                    "redundancy_index_v1": {
                        "version": "redundancy_index_v1",
                        "status": "WARN",
                        "reason": None,
                        "per_requirement": [
                            {
                                "primitive": "MANA_RAMP_ARTIFACT_ROCK",
                                "min": 10,
                                "count": 6,
                                "supported": True,
                                "redundancy_ratio": 0.6,
                                "redundancy_level": "LOW",
                            }
                        ],
                        "summary": {
                            "avg_redundancy_ratio": 0.6,
                            "low_redundancy_count": 1,
                            "unsupported_count": 0,
                        },
                        "notes": [
                            {
                                "code": "REDUNDANCY_BELOW_MIN",
                                "message": "Primitive 'MANA_RAMP_ARTIFACT_ROCK' is below minimum redundancy (6/10).",
                            }
                        ],
                    }
                },
            ),
            (
                "has_counterfactual_stress_test_v1",
                {
                    "counterfactual_stress_test_v1": {
                        "version": "counterfactual_stress_test_v1",
                        "status": "OK",
                        "reason": None,
                        "scenarios": [
                            {
                                "scenario_id": "remove_commander_slot",
                                "removed": {
                                    "type": "slot",
                                    "value": "C0",
                                },
                                "metrics": {
                                    "playable_nodes_before": 4,
                                    "playable_nodes_after": 3,
                                    "lost_nodes": 1,
                                    "lost_fraction": 0.25,
                                },
                                "notes": [],
                            }
                        ],
                    }
                },
            ),
            (
                "has_structural_scorecard_v1",
                {
                    "structural_scorecard_v1": {
                        "version": "structural_scorecard_v1",
                        "status": "OK",
                        "reason": None,
                        "headline": {
                            "grade": "B",
                            "score_0_100": 84,
                        },
                        "subscores": {
                            "policy_compliance": 100,
                            "graph_cohesion": 78,
                            "interaction_coverage": 80,
                            "vulnerability": 76,
                        },
                        "badges": [],
                        "sources": {
                            "bracket_compliance_summary_v1": True,
                            "graph_analytics_summary_v1": True,
                            "disruption_surface_v1": True,
                            "vulnerability_index_v1": True,
                        },
                    }
                },
            ),
            ("has_motifs", {"motifs": [{"motif_id": "M1"}]}),
            ("has_disruption", {"disruption_totals": {"bridges_total": 1}}),
            ("has_pathways", {"pathways_totals": {"reachable_total": 1}}),
            ("has_combo_skeleton", {"combo_skeleton_components": [{"component_id": "CC1"}]}),
            ("has_combo_candidates", {"combo_candidates_v0": [{"candidate_id": "CID1"}]}),
            ("has_patch_loop", {"patch_loop_v0": {"patches_total": 1}}),
            (
                "has_snapshot_preflight_v1",
                {
                    "snapshot_preflight_v1": {
                        "version": "snapshot_preflight_v1",
                        "snapshot_id": "TEST_SNAPSHOT_0001",
                        "status": "OK",
                        "errors": [],
                        "checks": {
                            "snapshot_exists": True,
                            "manifest_present": True,
                            "tags_compiled": True,
                            "schema_ok": True,
                        },
                    }
                },
            ),
        ]

        for expected_flag, kwargs in cases:
            with self.subTest(expected_flag=expected_flag):
                panels = build_available_panels_v1(**kwargs)
                self.assertEqual(list(panels.keys()), _AVAILABLE_PANEL_KEYS)
                for flag_name, flag_value in panels.items():
                    if flag_name == expected_flag:
                        self.assertIs(flag_value, True)
                    else:
                        self.assertIs(flag_value, False)

    def test_available_panels_output_is_deterministic(self) -> None:
        payload_kwargs = {
            "deck_cards_canonical_input_order": [{"slot_id": "S1"}],
            "unknowns_canonical": [{"code": "TEST_UNKNOWN"}],
            "deck_cards_playable": [{"slot_id": "S1"}],
            "rules_db_available_for_build": True,
            "rules_topic_selection_trace": {"topic": "PRIORITY", "selected_rule_ids_final": ["117.3b"]},
            "combo_proof_scaffolds_v0": [{"candidate_id": "cand-1"}],
            "combo_proof_attempts_v0": [{"attempt_id": "att-1"}],
            "primitive_index_by_slot": {"S1": ["RAMP_MANA"]},
            "structural_snapshot_v1": {"deck_size": 100},
            "graph_v1": {"graph_nodes": ["S1"], "graph_edges": []},
            "typed_graph_invariants_v1": {
                "version": "typed_graph_invariants_v1",
                "status": "OK",
                "errors": [],
                "checks": {
                    "graph_present": True,
                    "node_count": 2,
                    "edge_count": 1,
                    "duplicate_node_ids": 0,
                    "dangling_edges": 0,
                    "self_edges": 0,
                    "invalid_node_refs": 0,
                    "bounds_ok": True,
                    "ordering_ok": True,
                },
            },
            "profile_bracket_enforcement_v1": {
                "version": "profile_bracket_enforcement_v1",
                "status": "OK",
                "profile_id": "focused",
                "bracket_id": "B2",
                "profile_definition_version": "profile_defaults_v1_10",
                "bracket_definition_version": "bracket_v0",
                "game_changers_version": "gc_v0_userlist_2025-11-20",
                "counts": {
                    "deck_size_total": 100,
                    "game_changers_in_deck": 0,
                },
                "violations": [],
                "unknowns": [],
            },
            "engine_coherence_v1": {
                "version": "engine_coherence_v1",
                "status": "OK",
                "reason_code": None,
                "codes": [],
                "summary": {
                    "playable_slots_total": 2,
                    "non_dead_slots_total": 2,
                    "dead_slots_total": 0,
                    "dead_slot_ratio": 0.0,
                    "primitive_concentration_index": 0.5,
                    "overlap_score": 0.0,
                },
                "dead_slots": [],
                "top_primitive_concentration": [
                    {
                        "primitive": "RAMP_MANA",
                        "slots_with_primitive": 1,
                        "share": 0.5,
                    }
                ],
            },
            "mulligan_model_v1": {
                "version": "mulligan_model_v1",
                "status": "OK",
                "reason_code": None,
                "codes": [],
                "assumptions_version": "mulligan_assumptions_v1",
                "format": "commander",
                "default_policy": "NORMAL",
                "checkpoints": [7, 9, 10, 12],
                "policy_effective_n": [
                    {
                        "policy": "NORMAL",
                        "effective_n_by_checkpoint": [
                            {"checkpoint": 7, "effective_n": 7.0},
                            {"checkpoint": 9, "effective_n": 9.0},
                            {"checkpoint": 10, "effective_n": 10.0},
                            {"checkpoint": 12, "effective_n": 12.0},
                        ],
                    }
                ],
            },
            "substitution_engine_v1": {
                "version": "substitution_engine_v1",
                "status": "OK",
                "reason_code": None,
                "codes": [],
                "substitutions_version": "bucket_substitutions_v1",
                "format": "commander",
                "buckets": [
                    {
                        "bucket": "RAMP",
                        "k_primary": 10,
                        "effective_K": 12.5,
                        "K_int": 12,
                        "active_requirement_flags": ["LANDFALL_BASED"],
                        "substitution_terms": [
                            {
                                "primitive": "EXTRA_LAND_DROPS",
                                "weight": 0.4,
                                "k_substitute": 5,
                                "contribution": 2.0,
                            }
                        ],
                    }
                ],
            },
            "weight_multiplier_engine_v1": {
                "version": "weight_multiplier_engine_v1",
                "status": "OK",
                "reason_code": None,
                "codes": [],
                "weight_rules_version": "weight_rules_v1",
                "format": "commander",
                "multipliers_by_bucket": [
                    {
                        "bucket": "RAMP",
                        "multiplier": 1.12,
                    }
                ],
                "applied_rules": [
                    {
                        "rule_id": "ramp_landfall_bonus",
                        "target_bucket": "RAMP",
                        "requirement_flag": "LANDFALL_BASED",
                        "multiplier": 1.12,
                    }
                ],
            },
            "probability_math_core_v1": {
                "version": "probability_math_core_v1",
                "status": "OK",
                "reason_code": None,
                "codes": [],
                "math_backend": "int_comb",
                "available_functions": [
                    "comb",
                    "hypergeom_p_ge_1",
                    "hypergeom_p_ge_x",
                ],
                "validated_buckets": 1,
            },
            "probability_checkpoint_layer_v1": {
                "version": "probability_checkpoint_layer_v1",
                "status": "OK",
                "reason_code": None,
                "codes": [],
                "format": "commander",
                "deck_size_N": 99,
                "default_policy": "NORMAL",
                "checkpoints": [7, 9, 10, 12],
                "checkpoint_draws": [
                    {"checkpoint": 7, "effective_n": 7.0, "n_int": 7},
                    {"checkpoint": 9, "effective_n": 9.0, "n_int": 9},
                    {"checkpoint": 10, "effective_n": 10.0, "n_int": 10},
                    {"checkpoint": 12, "effective_n": 12.0, "n_int": 12},
                ],
                "probabilities_by_bucket": [
                    {
                        "bucket": "RAMP",
                        "effective_K": 12.5,
                        "K_int": 12,
                        "probabilities_by_checkpoint": [
                            {"checkpoint": 7, "effective_n": 7.0, "n_int": 7, "p_ge_1": 0.615148},
                            {"checkpoint": 9, "effective_n": 9.0, "n_int": 9, "p_ge_1": 0.72774},
                            {"checkpoint": 10, "effective_n": 10.0, "n_int": 10, "p_ge_1": 0.775732},
                            {"checkpoint": 12, "effective_n": 12.0, "n_int": 12, "p_ge_1": 0.851995},
                        ],
                    }
                ],
            },
            "stress_model_definition_v1": {
                "version": "stress_model_definition_v1",
                "status": "OK",
                "reason_code": None,
                "codes": [],
                "stress_models_version": "stress_models_v1",
                "format": "commander",
                "profile_id": "focused",
                "bracket_id": "B2",
                "request_override_model_id": None,
                "selected_model_id": "PUNISHING_POD_V0",
                "selection_source": "profile_bracket",
                "operators": [
                    {
                        "op": "BOARD_WIPE",
                        "by_turn": 6,
                        "surviving_engine_fraction": 0.6,
                    },
                    {
                        "op": "GRAVEYARD_HATE_WINDOW",
                        "turns": [4, 5, 6],
                        "graveyard_penalty": 0.7,
                    },
                    {
                        "op": "STAX_TAX",
                        "by_turn": 4,
                        "inflation_factor": 1.25,
                    },
                    {
                        "op": "TARGETED_REMOVAL",
                        "count": 2,
                    },
                ],
            },
            "stress_transform_engine_v1": {
                "version": "stress_transform_engine_v1",
                "status": "OK",
                "reason_code": None,
                "codes": [],
                "format": "commander",
                "deck_size_N": 99,
                "selected_model_id": "PUNISHING_POD_V0",
                "operators_applied": [
                    {"op": "TARGETED_REMOVAL", "count": 2},
                ],
                "checkpoints": [7, 9, 10, 12],
                "checkpoint_draws": [
                    {"checkpoint": 7, "effective_n": 7.0, "n_int": 7},
                    {"checkpoint": 9, "effective_n": 9.0, "n_int": 9},
                    {"checkpoint": 10, "effective_n": 10.0, "n_int": 10},
                    {"checkpoint": 12, "effective_n": 12.0, "n_int": 12},
                ],
                "stress_adjusted_effective_K": [
                    {
                        "bucket": "RAMP",
                        "effective_K_before": 12.5,
                        "K_int_before": 12,
                        "effective_K_after": 10.5,
                        "K_int_after": 10,
                    }
                ],
                "stress_adjusted_probabilities_by_bucket": [
                    {
                        "bucket": "RAMP",
                        "effective_K_after": 10.5,
                        "K_int_after": 10,
                        "probabilities_by_checkpoint": [
                            {"checkpoint": 7, "effective_n": 7.0, "n_int": 7, "p_ge_1": 0.542398},
                            {"checkpoint": 9, "effective_n": 9.0, "n_int": 9, "p_ge_1": 0.651245},
                            {"checkpoint": 10, "effective_n": 10.0, "n_int": 10, "p_ge_1": 0.699663},
                            {"checkpoint": 12, "effective_n": 12.0, "n_int": 12, "p_ge_1": 0.778733},
                        ],
                    }
                ],
                "operator_impacts": [],
            },
            "resilience_math_engine_v1": {
                "version": "resilience_math_engine_v1",
                "status": "WARN",
                "reason_code": None,
                "codes": ["RESILIENCE_COMMANDER_FRAGILITY_UNAVAILABLE"],
                "format": "commander",
                "checkpoints": [7, 9, 10, 12],
                "commander_dependency": "MED",
                "metrics": {
                    "engine_continuity_after_removal": 0.81,
                    "rebuild_after_wipe": 0.75,
                    "graveyard_fragility_delta": 0.14,
                    "commander_fragility_delta": None,
                },
                "bucket_metrics": [
                    {
                        "bucket": "RAMP",
                        "baseline_effective_K": 12.5,
                        "stress_effective_K": 10.5,
                        "baseline_p_ge_1_mean": 0.742654,
                        "stress_p_ge_1_mean": 0.66801,
                        "stress_delta_p_ge_1_mean": 0.074644,
                    }
                ],
            },
            "commander_reliability_model_v1": {
                "version": "commander_reliability_model_v1",
                "status": "WARN",
                "reason_code": None,
                "codes": ["COMMANDER_RELIABILITY_PROTECTION_PROXY_UNAVAILABLE"],
                "commander_dependent": "HIGH",
                "checkpoint_mapping": {"t3": 9, "t4": 10, "t6": 12},
                "metrics": {
                    "cast_reliability_t3": 0.5,
                    "cast_reliability_t4": 0.6,
                    "cast_reliability_t6": 0.8,
                    "protection_coverage_proxy": None,
                    "commander_fragility_delta": 0.15,
                },
                "notes": [
                    "Commander dependency is HIGH while protection coverage proxy is unavailable.",
                    "Protection coverage proxy unavailable from primitive index/playable slot inputs.",
                ],
            },
            "sufficiency_summary_v1": {
                "version": "sufficiency_summary_v1",
                "status": "WARN",
                "reason_code": None,
                "codes": [
                    "SUFFICIENCY_COMMANDER_PROTECTION_PROXY_UNAVAILABLE",
                    "SUFFICIENCY_REQUIRED_EFFECTS_SOURCE_WARN",
                ],
                "failures": [],
                "warnings": [
                    "SUFFICIENCY_COMMANDER_PROTECTION_PROXY_UNAVAILABLE",
                    "SUFFICIENCY_REQUIRED_EFFECTS_SOURCE_WARN",
                ],
                "domain_verdicts": {
                    "required_effects": {
                        "status": "WARN",
                        "codes": ["SUFFICIENCY_REQUIRED_EFFECTS_SOURCE_WARN"],
                    },
                    "baseline_prob": {"status": "PASS", "codes": []},
                    "stress_prob": {"status": "PASS", "codes": []},
                    "coherence": {"status": "PASS", "codes": []},
                    "resilience": {"status": "PASS", "codes": []},
                    "commander": {
                        "status": "WARN",
                        "codes": ["SUFFICIENCY_COMMANDER_PROTECTION_PROXY_UNAVAILABLE"],
                    },
                },
                "thresholds_used": {
                    "profile_thresholds_version": "profile_thresholds_v1",
                    "calibration_snapshot_version": "calibration_snapshot_v1",
                    "format": "commander",
                    "requested_profile_id": "focused",
                    "selected_profile_id": "focused",
                    "selection_source": "profile",
                    "domains": {},
                },
                "versions_used": {
                    "sufficiency_summary_version": "sufficiency_summary_v1",
                    "profile_thresholds_version": "profile_thresholds_v1",
                    "calibration_snapshot_version": "calibration_snapshot_v1",
                },
            },
            "motifs": [{"motif_id": "M1"}],
            "disruption_totals": {"bridges_total": 1},
            "pathways_totals": {"reachable_total": 1},
            "combo_skeleton_components": [{"component_id": "CC1"}],
            "combo_candidates_v0": [{"candidate_id": "CID1"}],
            "patch_loop_v0": {"patches_total": 1},
            "snapshot_preflight_v1": {
                "version": "snapshot_preflight_v1",
                "snapshot_id": "TEST_SNAPSHOT_0001",
                "status": "OK",
                "errors": [],
                "checks": {
                    "snapshot_exists": True,
                    "manifest_present": True,
                    "tags_compiled": True,
                    "schema_ok": True,
                },
            },
        }

        first = build_available_panels_v1(**payload_kwargs)
        second = build_available_panels_v1(**payload_kwargs)

        self.assertEqual(first, second)
        self.assertEqual(list(first.keys()), _AVAILABLE_PANEL_KEYS)


if __name__ == "__main__":
    unittest.main()
