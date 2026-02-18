from typing import Any, Dict, List


def run_proof_scaffold_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Proof scaffold layer (proof_scaffold_v1).
    Selects rule-topics, citations, and scaffold metadata deterministically.
    Must preserve exact output + ordering + hashing payloads.
    """

    combo_candidates_v0 = state["combo_candidates_v0"]
    slot_by_id = state["slot_by_id"]
    path_distance_by_slot = state["path_distance_by_slot"]
    commander_playable = state["commander_playable"]
    disruption_commander_risk = state["disruption_commander_risk"]
    sorted_unique = state["sorted_unique"]
    order_by_node_order = state["order_by_node_order"]
    articulation_set = state["articulation_set"]
    node_order = state["node_order"]
    bridge_edge_set = state["bridge_edge_set"]
    hub_rank_by_slot = state["hub_rank_by_slot"]
    node_by_slot_id = state["node_by_slot_id"]
    disruption_node_impact_by_slot = state["disruption_node_impact_by_slot"]
    proof_rule_topics_v1 = state["proof_rule_topics_v1"]
    rules_db_available = state["rules_db_available"]
    rules_db_connect = state["rules_db_connect"]
    ruleset_id_default = state["ruleset_id_default"]
    builtin_topic_selection_defaults = state["builtin_topic_selection_defaults"]
    builtin_topic_selection_topics = state["builtin_topic_selection_topics"]
    rules_topic_config_obj = state["rules_topic_config_obj"]
    rules_topic_config_available = state["rules_topic_config_available"]
    rules_topic_config_version = state["rules_topic_config_version"]
    engine_topic_prefer_sections_override_by_topic = state["engine_topic_prefer_sections_override_by_topic"]
    engine_topic_take_final_override_by_topic = state["engine_topic_take_final_override_by_topic"]
    rules_lookup_by_fts_with_trace = state["rules_lookup_by_fts_with_trace"]
    make_rule_citation = state["make_rule_citation"]
    rules_db_abs_path = state["rules_db_abs_path"]
    default_topic_selection_policy_id = state["default_topic_selection_policy_id"]
    proof_scaffold_rules_policy_version = state["proof_scaffold_rules_policy_version"]
    proof_scaffold_layer_version = state["proof_scaffold_layer_version"]
    proof_scaffold_ruleset_version = state["proof_scaffold_ruleset_version"]
    make_slot_id = state["make_slot_id"]
    component_by_slot = state["component_by_slot"]
    graph_hash_v2 = state["graph_hash_v2"]
    stable_json_dumps = state["stable_json_dumps"]
    sha256_hex = state["sha256_hex"]
    combo_candidates_hash_v1 = state["combo_candidates_hash_v1"]
    disruption_hash_v1 = state["disruption_hash_v1"]
    pathways_hash_v1 = state["pathways_hash_v1"]

    proof_todo_checklist_template = [
        {
            "id": "T0_IDENTIFY_ENGINE",
            "done": False,
            "text": "Identify which primitives represent the engine loop (graph-only).",
        },
        {
            "id": "T1_MAP_TO_RULES_TEXT",
            "done": False,
            "text": "Map candidate nodes to actual rules text interactions (requires DB oracle_text access later).",
        },
        {
            "id": "T2_VALIDATE_TIMING",
            "done": False,
            "text": "Validate timing legality and interaction windows (future rules engine).",
        },
        {
            "id": "T3_VALIDATE_COSTS",
            "done": False,
            "text": "Validate costs/resources and confirm closure (future proof engine).",
        },
        {
            "id": "T4_IDENTIFY_BREAKPOINTS",
            "done": False,
            "text": "List vulnerability nodes/edges and interaction breakpoints (use disruption overlays).",
        },
    ]

    combo_proof_scaffolds_v0: List[Dict[str, Any]] = []
    rules_db_available_for_build = bool(rules_db_available)
    rules_conn = rules_db_connect() if rules_db_available_for_build else None
    if rules_conn is None:
        rules_db_available_for_build = False
    ruleset_id_for_build = ruleset_id_default
    if rules_conn is not None:
        try:
            row = rules_conn.execute(
                "SELECT ruleset_id FROM ruleset_source ORDER BY ruleset_id DESC LIMIT 1"
            ).fetchone()
            if row and isinstance(row[0], str):
                ruleset_id_for_build = row[0]
        except Exception:
            ruleset_id_for_build = ruleset_id_default

    topic_selection_rules_version = "builtin_default"
    topic_selection_take_final = int(builtin_topic_selection_defaults.get("take_final", 5))
    topic_selection_fetch_raw = int(builtin_topic_selection_defaults.get("fts_fetch_raw", 25))
    topic_prefer_sections_by_topic: Dict[str, List[str]] = {
        topic_id: sorted_unique([s for s in (cfg.get("prefer_sections") or []) if isinstance(s, str)])
        for topic_id, cfg in builtin_topic_selection_topics.items()
    }

    loaded_topic_rules_version = (
        rules_topic_config_obj.get("topic_selection_rules_version")
        if isinstance(rules_topic_config_obj, dict)
        else None
    )
    if (
        rules_topic_config_available
        and isinstance(rules_topic_config_obj, dict)
        and loaded_topic_rules_version == rules_topic_config_version
    ):
        topic_selection_rules_version = rules_topic_config_version
        config_defaults = rules_topic_config_obj.get("defaults")
        if isinstance(config_defaults, dict):
            take_final_cfg = config_defaults.get("take_final")
            if isinstance(take_final_cfg, int) and take_final_cfg > 0:
                topic_selection_take_final = take_final_cfg
            fts_fetch_cfg = config_defaults.get("fts_fetch_raw")
            if isinstance(fts_fetch_cfg, int) and fts_fetch_cfg > 0:
                topic_selection_fetch_raw = fts_fetch_cfg

        config_topics = rules_topic_config_obj.get("topics")
        if isinstance(config_topics, dict):
            for topic_id, topic_cfg in config_topics.items():
                if not isinstance(topic_id, str) or not isinstance(topic_cfg, dict):
                    continue
                prefer_sections_cfg = topic_cfg.get("prefer_sections")
                if isinstance(prefer_sections_cfg, list):
                    topic_prefer_sections_by_topic[topic_id] = sorted_unique(
                        [s for s in prefer_sections_cfg if isinstance(s, str)]
                    )

    for topic_id in sorted(engine_topic_prefer_sections_override_by_topic.keys()):
        topic_prefer_sections_by_topic[topic_id] = engine_topic_prefer_sections_override_by_topic[topic_id]

    ruleset_sha_cache: dict[str, Any] = {}
    rules_topic_matches_by_id: Dict[str, List[Dict[str, Any]]] = {}
    rules_topic_selection_topic_traces: List[Dict[str, Any]] = []
    for topic in proof_rule_topics_v1:
        topic_id = topic["topic_id"]
        fts_query = topic["fts_query"]
        prefer_sections = topic_prefer_sections_by_topic.get(topic_id, [])
        topic_take_final = engine_topic_take_final_override_by_topic.get(topic_id, topic_selection_take_final)
        lookup_result = rules_lookup_by_fts_with_trace(
            rules_conn,
            ruleset_id_for_build,
            fts_query,
            limit=topic_take_final,
            fetch_limit_raw=topic_selection_fetch_raw,
            prefer_sections=prefer_sections,
        )
        topic_matches = lookup_result.get("matches") or []
        rules_topic_matches_by_id[topic_id] = [dict(match) for match in topic_matches if isinstance(match, dict)]
        topic_trace = lookup_result.get("trace") or {}

        prefer_sections_applied = list(topic_trace.get("prefer_sections_applied") or [])
        fts_returned_rule_ids_raw = list(topic_trace.get("fts_returned_rule_ids_raw") or [])
        selected_rule_ids_sorted = list(topic_trace.get("selected_rule_ids_sorted") or [])
        preferred_rule_ids_sorted = list(topic_trace.get("preferred_rule_ids_sorted") or [])
        nonpreferred_rule_ids_sorted = list(topic_trace.get("nonpreferred_rule_ids_sorted") or [])
        selected_rule_ids_final = list(topic_trace.get("selected_rule_ids_final") or [])

        preferred_hits_total = len(preferred_rule_ids_sorted)
        nonpreferred_hits_total = len(nonpreferred_rule_ids_sorted)
        selected_rule_ids_final_total = len(selected_rule_ids_final)

        preferred_rule_ids_set = set(preferred_rule_ids_sorted)
        nonpreferred_rule_ids_set = set(nonpreferred_rule_ids_sorted)
        selected_preferred_total = sum(1 for rule_id in selected_rule_ids_final if rule_id in preferred_rule_ids_set)
        selected_nonpreferred_total = sum(1 for rule_id in selected_rule_ids_final if rule_id in nonpreferred_rule_ids_set)

        selected_all_preferred = selected_rule_ids_final_total > 0 and selected_nonpreferred_total == 0
        fallback_used = selected_nonpreferred_total > 0
        preferred_sections_missing_in_results = (
            bool(prefer_sections_applied)
            and preferred_hits_total == 0
            and len(fts_returned_rule_ids_raw) > 0
        )

        if selected_rule_ids_final_total == 0:
            selection_status = "NO_MATCHES"
        elif selected_all_preferred:
            selection_status = "OK_PREFERRED_ONLY"
        elif fallback_used:
            selection_status = "OK_MIXED_FALLBACK"
        elif preferred_sections_missing_in_results:
            selection_status = "NO_PREFERRED_HITS"
        else:
            selection_status = "OK_MIXED_FALLBACK"

        rules_topic_selection_topic_traces.append(
            {
                "topic_id": topic_id,
                "fts_query": fts_query,
                "fts_fetch_limit_raw": int(topic_trace.get("fts_fetch_limit_raw", 25)),
                "prefer_sections_applied": prefer_sections_applied,
                "fts_returned_rule_ids_raw": fts_returned_rule_ids_raw,
                "selected_rule_ids_sorted": selected_rule_ids_sorted,
                "preferred_rule_ids_sorted": preferred_rule_ids_sorted,
                "nonpreferred_rule_ids_sorted": nonpreferred_rule_ids_sorted,
                "selected_rule_ids_final": selected_rule_ids_final,
                "preferred_hits_total": preferred_hits_total,
                "nonpreferred_hits_total": nonpreferred_hits_total,
                "selected_rule_ids_final_total": selected_rule_ids_final_total,
                "selected_preferred_total": selected_preferred_total,
                "selected_nonpreferred_total": selected_nonpreferred_total,
                "selected_all_preferred": selected_all_preferred,
                "fallback_used": fallback_used,
                "preferred_sections_missing_in_results": preferred_sections_missing_in_results,
                "selection_status": selection_status,
            }
        )

    rules_topic_selection_trace = {
        "applies_to_scaffolds": "ALL",
        "rules_db_available": rules_db_available_for_build,
        "rules_db_path": str(rules_db_abs_path),
        "ruleset_id": ruleset_id_for_build,
        "topic_selection_rules_version": topic_selection_rules_version,
        "policy_id": default_topic_selection_policy_id,
        "take_final": topic_selection_take_final,
        "fts_fetch_raw": topic_selection_fetch_raw,
        "topics_total": len(rules_topic_selection_topic_traces),
        "topics_with_any_match_total": sum(
            1 for item in rules_topic_selection_topic_traces if item.get("selected_rule_ids_final")
        ),
        "topics_selected_preferred_only_total": sum(
            1 for item in rules_topic_selection_topic_traces if item.get("selected_all_preferred") is True
        ),
        "topics_with_fallback_total": sum(
            1 for item in rules_topic_selection_topic_traces if item.get("fallback_used") is True
        ),
        "topics_no_matches_total": sum(
            1 for item in rules_topic_selection_topic_traces if int(item.get("selected_rule_ids_final_total", 0)) == 0
        ),
        "total_selected_rule_ids_final": sum(
            len(item.get("selected_rule_ids_final") or []) for item in rules_topic_selection_topic_traces
        ),
        "topic_traces": rules_topic_selection_topic_traces,
    }
    topic_selection_trace_by_topic_id = {
        t.get("topic_id"): t
        for t in rules_topic_selection_topic_traces
        if isinstance(t.get("topic_id"), str)
    }

    for candidate in combo_candidates_v0:
        candidate_id = candidate.get("candidate_id")
        slot_ids = [sid for sid in (candidate.get("slot_ids") or []) if isinstance(sid, str)]
        cycle_edge_keys = [edge_key for edge_key in (candidate.get("cycle_edge_keys") or []) if isinstance(edge_key, str)]
        card_oracle_ids = []
        for sid in slot_ids:
            card_oracle_ids.append((slot_by_id.get(sid) or {}).get("resolved_oracle_id"))

        reachable_distances = [
            d for sid in slot_ids for d in [path_distance_by_slot.get(sid)] if isinstance(d, int)
        ]
        candidate_reachable_from_commander = bool(commander_playable and reachable_distances)
        min_commander_distance_to_candidate = min(reachable_distances) if reachable_distances else None
        commander_risk_flags = sorted_unique(disruption_commander_risk.get("risk_flags") or [])
        if not bool(candidate.get("commander_involved")):
            commander_risk_flags = sorted_unique([*commander_risk_flags, "COMMANDER_NOT_IN_CANDIDATE"])
        if commander_playable and not candidate_reachable_from_commander:
            commander_risk_flags = sorted_unique([*commander_risk_flags, "COMMANDER_PATH_UNREACHABLE"])

        candidate_articulation_slots = order_by_node_order(
            [sid for sid in slot_ids if sid in articulation_set],
            node_order,
        )
        candidate_bridge_edges = sorted(
            {edge_key for edge_key in cycle_edge_keys if edge_key in bridge_edge_set}
        )
        candidate_hub_slots = []
        for sid in slot_ids:
            if sid in hub_rank_by_slot:
                node_info = node_by_slot_id.get(sid) or {}
                candidate_hub_slots.append(
                    {
                        "slot_id": sid,
                        "hub_rank": hub_rank_by_slot[sid],
                        "degree": int(node_info.get("degree", 0)),
                    }
                )
        candidate_hub_slots.sort(key=lambda x: (int(x.get("hub_rank", 10**9)), str(x.get("slot_id") or "")))

        node_impact_summary = []
        for sid in slot_ids:
            impact = disruption_node_impact_by_slot.get(sid) or {}
            node_impact_summary.append(
                {
                    "slot_id": sid,
                    "delta_components": int(impact.get("delta_components", 0)),
                    "largest_component_size_after_removal": int(impact.get("largest_component_size_after_removal", 0)),
                }
            )

        rules_topics = []
        rules_citation_by_rule_id: Dict[str, Dict[str, Any]] = {}
        for topic in proof_rule_topics_v1:
            topic_id = topic["topic_id"]
            fts_query = topic["fts_query"]
            matches = [
                dict(match)
                for match in rules_topic_matches_by_id.get(topic_id, [])
                if isinstance(match, dict)
            ]
            citations = []
            for match in matches:
                rule_id = match.get("rule_id")
                if not isinstance(rule_id, str):
                    continue
                citation = make_rule_citation(
                    rules_conn,
                    ruleset_id_for_build,
                    rule_id,
                    ruleset_sha_cache,
                )
                citations.append(citation)
                rules_citation_by_rule_id[rule_id] = citation

            topic_trace = topic_selection_trace_by_topic_id.get(topic_id) or {}
            selected_rule_ids_for_topic = [
                m.get("rule_id") for m in matches if isinstance(m.get("rule_id"), str)
            ]

            rules_topics.append(
                {
                    "topic_id": topic_id,
                    "fts_query": fts_query,
                    "prefer_sections_applied": list(topic_trace.get("prefer_sections_applied") or []),
                    "rule_ids": selected_rule_ids_for_topic,
                    "matches": matches,
                    "citations": citations,
                }
            )

        rules_topics = sorted(
            rules_topics,
            key=lambda t: (str(t.get("topic_id") or ""), str(t.get("fts_query") or "")),
        )

        rules_topics_with_hits_total = sum(
            1 for t in rules_topics if len([m for m in (t.get("matches") or []) if isinstance(m.get("rule_id"), str)]) > 0
        )
        rules_selected_rule_ids_total = len(
            sorted_unique(
                [
                    m.get("rule_id")
                    for t in rules_topics
                    for m in (t.get("matches") or [])
                    if isinstance(m.get("rule_id"), str)
                ]
            )
        )

        rules_citations_union_by_key: Dict[str, Dict[str, Any]] = {}
        for t in rules_topics:
            topic_id = t.get("topic_id")
            for citation in (t.get("citations") or []):
                ruleset_id = citation.get("ruleset_id")
                rule_id = citation.get("rule_id")
                if not isinstance(ruleset_id, str) or not isinstance(rule_id, str):
                    continue
                key = f"{ruleset_id}|{rule_id}"
                if key in rules_citations_union_by_key:
                    continue
                rules_citations_union_by_key[key] = {
                    "ruleset_id": ruleset_id,
                    "rule_id": rule_id,
                    "section_id": citation.get("section_id") if isinstance(citation.get("section_id"), str) else None,
                    "source_sha256": citation.get("source_sha256") if isinstance(citation.get("source_sha256"), str) else None,
                    "topic_origin": topic_id if isinstance(topic_id, str) else None,
                }

        rules_citations_union = sorted(
            rules_citations_union_by_key.values(),
            key=lambda c: ((c.get("topic_origin") or ""), (c.get("section_id") or ""), c.get("rule_id") or ""),
        )

        if ruleset_id_for_build not in ruleset_sha_cache:
            try:
                row = (
                    rules_conn.execute(
                        "SELECT source_sha256 FROM ruleset_source WHERE ruleset_id = ?",
                        (ruleset_id_for_build,),
                    ).fetchone()
                    if rules_conn is not None
                    else None
                )
                ruleset_sha_cache[ruleset_id_for_build] = row[0] if row and isinstance(row[0], str) else None
            except Exception:
                ruleset_sha_cache[ruleset_id_for_build] = None

        rules_snapshot = {
            "ruleset_id": ruleset_id_for_build,
            "rules_db_path": str(rules_db_abs_path),
            "rules_db_available": rules_db_available_for_build,
            "rules_source_sha256": ruleset_sha_cache.get(ruleset_id_for_build),
        }

        if not rules_db_available_for_build:
            scaffold_status = "UNPROVEN_GRAPH_ONLY"
        elif rules_topics_with_hits_total > 0:
            scaffold_status = "UNPROVEN_WITH_RULE_CONTEXT"
        else:
            scaffold_status = "UNPROVEN_GRAPH_ONLY"

        topics_total = len(rules_topics)
        rules_coverage_v1 = {
            "topics_total": topics_total,
            "topics_with_hits": rules_topics_with_hits_total,
            "selected_rule_ids_total": rules_selected_rule_ids_total,
            "coverage_ratio": round((rules_topics_with_hits_total / topics_total), 4) if topics_total > 0 else 0.0,
        }

        graph_has_cycle = bool(candidate.get("is_graph_consistent")) and int(candidate.get("cycle_len") or 0) > 0
        typed_edge_types_union = sorted(candidate.get("typed_edge_types_union") or [])
        cycle_len_value = int(candidate.get("cycle_len") or 0)
        if rules_topics_with_hits_total > 0:
            alignment_signal = "GRAPH_PLUS_RULE_CONTEXT"
        else:
            alignment_signal = "GRAPH_ONLY"

        structural_rule_alignment_v1 = {
            "graph_has_cycle": graph_has_cycle,
            "typed_edge_types_union": typed_edge_types_union,
            "cycle_len": cycle_len_value,
            "rules_topics_with_hits": rules_topics_with_hits_total,
            "alignment_signal": alignment_signal,
        }

        rules_citations_flat = sorted(
            rules_citation_by_rule_id.values(),
            key=lambda c: ((c.get("section_id") or ""), c.get("rule_id") or ""),
        )

        rules_context = {
            "rules_db_available": rules_db_available_for_build,
            "ruleset_id": ruleset_id_for_build,
            "topic_selection_rules_version": topic_selection_rules_version,
            "selection_policy_id": default_topic_selection_policy_id,
            "proof_scaffold_rules_policy_version": proof_scaffold_rules_policy_version,
            "topics": rules_topics,
        }

        scaffold = {
            "candidate_id": candidate_id,
            "candidate_hash_v1": candidate.get("candidate_hash_v1"),
            "status": scaffold_status,
            "proof_version": proof_scaffold_layer_version,
            "cycle_len": candidate.get("cycle_len"),
            "component_id": candidate.get("component_id"),
            "slot_ids": slot_ids,
            "card_oracle_ids": card_oracle_ids,
            "graph_evidence": {
                "cycle_edge_keys": cycle_edge_keys,
                "typed_edge_types_union": sorted(candidate.get("typed_edge_types_union") or []),
                "primitives_union": sorted(candidate.get("primitives_union") or []),
            },
            "commander_context": {
                "commander_slot_id": make_slot_id("C", 0),
                "commander_involved": bool(candidate.get("commander_involved")),
                "commander_component_id": component_by_slot.get("C0"),
                "candidate_reachable_from_commander": candidate_reachable_from_commander,
                "min_commander_distance_to_candidate": min_commander_distance_to_candidate,
                "commander_risk_flags": commander_risk_flags,
            },
            "vulnerability_overlay": {
                "candidate_articulation_slots": candidate_articulation_slots,
                "candidate_bridge_edges": candidate_bridge_edges,
                "candidate_hub_slots": candidate_hub_slots,
                "node_impact_summary": node_impact_summary,
            },
            "proof_placeholders": {
                "required_board_state": [],
                "required_resources": [],
                "timing_assumptions": [],
                "loop_closure_argument": None,
                "cost_legality_confirmation": None,
                "timing_legality_confirmation": None,
            },
            "proof_todo_checklist": [dict(item) for item in proof_todo_checklist_template],
            "rules_context": rules_context,
            "rules_citations_flat": rules_citations_flat,
            "rules_citations_union": rules_citations_union,
            "rules_topics_with_hits_total": rules_topics_with_hits_total,
            "rules_selected_rule_ids_total": rules_selected_rule_ids_total,
            "rules_coverage_v1": rules_coverage_v1,
            "structural_rule_alignment_v1": structural_rule_alignment_v1,
            "rules_snapshot": rules_snapshot,
        }

        scaffold_fingerprint_payload_v1 = {
            "candidate_id": scaffold.get("candidate_id"),
            "candidate_hash_v1": scaffold.get("candidate_hash_v1"),
            "cycle_len": scaffold.get("cycle_len"),
            "slot_ids": slot_ids,
            "oracle_ids": card_oracle_ids,
            "cycle_edge_keys": cycle_edge_keys,
            "typed_edge_types_union": sorted(candidate.get("typed_edge_types_union") or []),
            "commander_context": {
                "commander_involved": scaffold["commander_context"]["commander_involved"],
                "candidate_reachable_from_commander": scaffold["commander_context"]["candidate_reachable_from_commander"],
                "min_commander_distance_to_candidate": scaffold["commander_context"]["min_commander_distance_to_candidate"],
                "commander_risk_flags": scaffold["commander_context"]["commander_risk_flags"],
            },
            "vulnerability_overlay": {
                "candidate_articulation_slots": candidate_articulation_slots,
                "candidate_bridge_edges": candidate_bridge_edges,
                "candidate_hub_slots": [
                    {
                        "slot_id": h.get("slot_id"),
                        "hub_rank": h.get("hub_rank"),
                    }
                    for h in candidate_hub_slots
                ],
                "node_impact_summary": [
                    {
                        "slot_id": n.get("slot_id"),
                        "delta_components": n.get("delta_components"),
                    }
                    for n in node_impact_summary
                ],
            },
        }
        scaffold_hash_v1 = sha256_hex(stable_json_dumps(scaffold_fingerprint_payload_v1))
        scaffold["scaffold_hash_v1"] = scaffold_hash_v1

        scaffold_fingerprint_payload_v2 = {
            "candidate_hash_v1": scaffold.get("candidate_hash_v1"),
            "ruleset_id": ruleset_id_for_build,
            "rule_ids_union": [
                row.get("rule_id")
                for row in rules_citations_union
                if isinstance(row.get("rule_id"), str)
            ],
            "cycle_edge_keys": cycle_edge_keys,
            "typed_edge_types_union": typed_edge_types_union,
        }
        scaffold_hash_v2 = sha256_hex(stable_json_dumps(scaffold_fingerprint_payload_v2))
        scaffold["scaffold_hash_v2"] = scaffold_hash_v2

        scaffold["patch_anchor_v1"] = {
            "candidate_id": scaffold.get("candidate_id"),
            "candidate_hash_v1": scaffold.get("candidate_hash_v1"),
            "scaffold_hash_v2": scaffold_hash_v2,
            "ruleset_id": ruleset_id_for_build,
            "graph_hash_v2": graph_hash_v2,
            "build_hash_v1": None,
        }

        scaffold_fingerprint_payload_v3 = {
            "candidate_hash_v1": scaffold.get("candidate_hash_v1"),
            "rules_context": {
                "rules_db_available": rules_context.get("rules_db_available"),
                "ruleset_id": rules_context.get("ruleset_id"),
                "topic_selection_rules_version": topic_selection_rules_version,
                "selection_policy_id": default_topic_selection_policy_id,
                "topics": [
                    {
                        "topic_id": t.get("topic_id"),
                        "selected_rule_ids_final": [
                            m.get("rule_id")
                            for m in (t.get("matches") or [])
                            if isinstance(m.get("rule_id"), str)
                        ],
                    }
                    for t in rules_topics
                ],
            },
        }
        scaffold_hash_v3 = sha256_hex(stable_json_dumps(scaffold_fingerprint_payload_v3))
        scaffold["scaffold_hash_v3"] = scaffold_hash_v3
        combo_proof_scaffolds_v0.append(scaffold)

    combo_proof_scaffolds_v0 = sorted(
        combo_proof_scaffolds_v0,
        key=lambda s: (str(s.get("candidate_id") or ""), str(s.get("candidate_hash_v1") or "")),
    )

    all_union_rule_ids = sorted_unique(
        [
            c.get("rule_id")
            for s in combo_proof_scaffolds_v0
            for c in (s.get("rules_citations_union") or [])
            if isinstance(c.get("rule_id"), str)
        ]
    )
    existing_union_rule_ids: set[str] = set()
    if rules_conn is not None and all_union_rule_ids:
        try:
            placeholders = ",".join(["?"] * len(all_union_rule_ids))
            rows = rules_conn.execute(
                f"SELECT rule_id FROM rules_rule WHERE ruleset_id = ? AND rule_id IN ({placeholders})",
                [ruleset_id_for_build, *all_union_rule_ids],
            ).fetchall()
            existing_union_rule_ids = {row[0] for row in rows if row and isinstance(row[0], str)}
        except Exception:
            existing_union_rule_ids = set()

    proof_scaffolds_rules_context_consistent = True
    for s in combo_proof_scaffolds_v0:
        if not isinstance(s.get("rules_snapshot"), dict):
            proof_scaffolds_rules_context_consistent = False
            break
        if not isinstance(s.get("scaffold_hash_v2"), str):
            proof_scaffolds_rules_context_consistent = False
            break
        union_rule_ids = [
            c.get("rule_id")
            for c in (s.get("rules_citations_union") or [])
            if isinstance(c.get("rule_id"), str)
        ]
        if any(rule_id not in existing_union_rule_ids for rule_id in union_rule_ids):
            proof_scaffolds_rules_context_consistent = False
            break

    if rules_conn is not None:
        rules_conn.close()

    proof_scaffold_fingerprint_payload_v1 = {
        "proof_scaffold_layer_version": proof_scaffold_layer_version,
        "proof_scaffold_ruleset_version": proof_scaffold_ruleset_version,
        "combo_candidates_hash_v1": combo_candidates_hash_v1,
        "disruption_hash_v1": disruption_hash_v1,
        "pathways_hash_v1": pathways_hash_v1,
        "scaffolds_compact": [
            {
                "candidate_id": s.get("candidate_id"),
                "scaffold_hash_v1": s.get("scaffold_hash_v1"),
            }
            for s in combo_proof_scaffolds_v0
        ],
    }
    proof_scaffolds_hash_v1 = sha256_hex(stable_json_dumps(proof_scaffold_fingerprint_payload_v1))

    proof_scaffold_fingerprint_payload_v2 = {
        "proof_scaffold_layer_version": proof_scaffold_layer_version,
        "proof_scaffold_ruleset_version": proof_scaffold_ruleset_version,
        "proof_scaffold_rules_policy_version": proof_scaffold_rules_policy_version,
        "ruleset_id": ruleset_id_for_build,
        "combo_candidates_hash_v1": combo_candidates_hash_v1,
        "disruption_hash_v1": disruption_hash_v1,
        "pathways_hash_v1": pathways_hash_v1,
        "scaffolds_compact": [
            {
                "candidate_id": s.get("candidate_id"),
                "candidate_hash_v1": s.get("candidate_hash_v1"),
                "scaffold_hash_v2": s.get("scaffold_hash_v2"),
            }
            for s in combo_proof_scaffolds_v0
        ],
    }
    proof_scaffolds_hash_v2 = sha256_hex(stable_json_dumps(proof_scaffold_fingerprint_payload_v2))

    proof_scaffold_fingerprint_payload_v3 = {
        "proof_scaffold_layer_version": proof_scaffold_layer_version,
        "proof_scaffold_ruleset_version": proof_scaffold_ruleset_version,
        "combo_candidates_hash_v1": combo_candidates_hash_v1,
        "disruption_hash_v1": disruption_hash_v1,
        "pathways_hash_v1": pathways_hash_v1,
        "topic_selection_rules_version": topic_selection_rules_version,
        "selection_policy_id": default_topic_selection_policy_id,
        "scaffolds_compact": [
            {
                "candidate_id": s.get("candidate_id"),
                "scaffold_hash_v3": s.get("scaffold_hash_v3"),
            }
            for s in combo_proof_scaffolds_v0
        ],
    }
    proof_scaffolds_hash_v3 = sha256_hex(stable_json_dumps(proof_scaffold_fingerprint_payload_v3))

    state["combo_proof_scaffolds_v0"] = combo_proof_scaffolds_v0
    state["rules_db_available_for_build"] = rules_db_available_for_build
    state["ruleset_id_for_build"] = ruleset_id_for_build
    state["topic_selection_rules_version"] = topic_selection_rules_version
    state["rules_topic_selection_trace"] = rules_topic_selection_trace
    state["proof_scaffolds_rules_context_consistent"] = proof_scaffolds_rules_context_consistent
    state["proof_scaffold_fingerprint_payload_v1"] = proof_scaffold_fingerprint_payload_v1
    state["proof_scaffold_fingerprint_payload_v2"] = proof_scaffold_fingerprint_payload_v2
    state["proof_scaffold_fingerprint_payload_v3"] = proof_scaffold_fingerprint_payload_v3
    state["proof_scaffolds_hash_v1"] = proof_scaffolds_hash_v1
    state["proof_scaffolds_hash_v2"] = proof_scaffolds_hash_v2
    state["proof_scaffolds_hash_v3"] = proof_scaffolds_hash_v3

    return state
