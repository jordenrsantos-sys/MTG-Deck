from typing import Any, Dict, List


def run_proof_attempt_v1(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Proof attempt layer (proof_attempt_v1).
    Builds oracle anchors + deterministic excerpts and produces proof_attempts_hash_v2.
    Must preserve exact output + ordering + hashing payloads.
    """

    combo_proof_scaffolds_v0 = state["combo_proof_scaffolds_v0"]
    slot_by_id = state["slot_by_id"]
    commander_canonical_slot = state["commander_canonical_slot"]
    db_snapshot_id = state["db_snapshot_id"]
    sorted_unique = state["sorted_unique"]
    slot_sort_key = state["slot_sort_key"]
    normalize_oracle_text_excerpt = state["normalize_oracle_text_excerpt"]
    lookup_cards_by_oracle_id = state["lookup_cards_by_oracle_id"]
    add_unknown = state["add_unknown"]
    unknowns = state["unknowns"]
    strip_hash_fields = state["strip_hash_fields"]
    stable_json_dumps = state["stable_json_dumps"]
    sha256_hex = state["sha256_hex"]
    proof_attempt_layer_version_v1 = state["proof_attempt_layer_version_v1"]
    proof_scaffolds_hash_v2 = state["proof_scaffolds_hash_v2"]

    combo_proof_attempts_v0: List[Dict[str, Any]] = []
    proof_attempt_hash_stable = True
    oracle_anchor_lookup_cache: Dict[str, Dict[str, Any] | None] = {}
    missing_oracle_text_unknown_oracle_ids: set[str] = set()
    proof_gaps_template_v0 = [
        {"id": "G0_MAP_TO_ORACLE_TEXT", "missing": True},
        {"id": "G1_DEFINE_ACTION_SEQUENCE", "missing": True},
        {"id": "G2_SHOW_LOOP_CLOSURE", "missing": True},
        {"id": "G3_VALIDATE_COSTS", "missing": True},
        {"id": "G4_VALIDATE_TIMING_WINDOWS", "missing": True},
        {"id": "G5_IDENTIFY_INTERACTION_BREAKPOINTS", "missing": True},
    ]

    for scaffold in sorted(
        combo_proof_scaffolds_v0,
        key=lambda s: (str(s.get("candidate_id") or ""), str(s.get("candidate_hash_v1") or "")),
    ):
        scaffold_status = scaffold.get("status")
        attempt_status = "ATTEMPT_TEMPLATE_ONLY" if scaffold_status == "UNPROVEN_WITH_RULE_CONTEXT" else "NO_RULE_CONTEXT"

        graph_evidence = dict(scaffold.get("graph_evidence") or {})
        rules_context_for_attempt = scaffold.get("rules_context") or {}
        attempt = {
            "candidate_id": scaffold.get("candidate_id"),
            "candidate_hash_v1": scaffold.get("candidate_hash_v1"),
            "attempt_version": proof_attempt_layer_version_v1,
            "status": attempt_status,
            "evidence_pack": {
                "graph_evidence": graph_evidence,
                "commander_context": dict(scaffold.get("commander_context") or {}),
                "typed_edge_types_union": list(graph_evidence.get("typed_edge_types_union") or []),
                "primitives_union": list(graph_evidence.get("primitives_union") or []),
                "ruleset_id": (
                    rules_context_for_attempt.get("ruleset_id")
                    if isinstance(rules_context_for_attempt.get("ruleset_id"), str)
                    else None
                ),
                "rules_citations_union": list(scaffold.get("rules_citations_union") or []),
                "rules_topics_with_hits_total": int(scaffold.get("rules_topics_with_hits_total") or 0),
                "rules_coverage_v1": dict(scaffold.get("rules_coverage_v1") or {}),
                "structural_rule_alignment_v1": dict(scaffold.get("structural_rule_alignment_v1") or {}),
            },
            "proof_template": {
                "required_board_state": [],
                "required_resources": [],
                "action_sequence": [],
                "timing_windows": [],
                "closure_claim": None,
                "cost_legality": {"confirmed": None, "notes": None, "rules": []},
                "timing_legality": {"confirmed": None, "notes": None, "rules": []},
                "breakpoints": [],
            },
            "proof_gaps_v0": [dict(item) for item in proof_gaps_template_v0],
        }

        candidate_slot_ids = [sid for sid in (scaffold.get("slot_ids") or []) if isinstance(sid, str)]
        oracle_anchor_refs_slot_ids = list(candidate_slot_ids)
        if "C0" in slot_by_id and "C0" not in oracle_anchor_refs_slot_ids:
            oracle_anchor_refs_slot_ids.append("C0")
        oracle_anchor_refs_slot_ids = sorted_unique(oracle_anchor_refs_slot_ids)
        oracle_anchor_refs_slot_ids.sort(key=slot_sort_key)

        oracle_anchor_refs_by_slot = {}
        involved_oracle_ids_raw: List[str] = []
        for sid in oracle_anchor_refs_slot_ids:
            resolved_oracle_id = (slot_by_id.get(sid) or {}).get("resolved_oracle_id")
            resolved_oracle_id_value = resolved_oracle_id if isinstance(resolved_oracle_id, str) else None
            oracle_anchor_refs_by_slot[sid] = resolved_oracle_id_value
            if isinstance(resolved_oracle_id_value, str):
                involved_oracle_ids_raw.append(resolved_oracle_id_value)

        for oracle_id in (scaffold.get("card_oracle_ids") or []):
            if isinstance(oracle_id, str):
                involved_oracle_ids_raw.append(oracle_id)

        commander_anchor_oracle_id = (commander_canonical_slot or {}).get("resolved_oracle_id")
        if isinstance(commander_anchor_oracle_id, str):
            involved_oracle_ids_raw.append(commander_anchor_oracle_id)

        involved_oracle_ids_unique = sorted_unique(involved_oracle_ids_raw)
        cache_misses = [oid for oid in involved_oracle_ids_unique if oid not in oracle_anchor_lookup_cache]
        if cache_misses:
            lookup_rows = lookup_cards_by_oracle_id(db_snapshot_id, cache_misses)
            for oid in cache_misses:
                oracle_anchor_lookup_cache[oid] = lookup_rows.get(oid)

        oracle_anchor_cards = []
        for oracle_id in involved_oracle_ids_unique:
            row = oracle_anchor_lookup_cache.get(oracle_id)
            row_name = row.get("name") if isinstance(row, dict) else None
            row_type_line = row.get("type_line") if isinstance(row, dict) else None
            row_mana_cost = row.get("mana_cost") if isinstance(row, dict) else None
            row_oracle_text = row.get("oracle_text") if isinstance(row, dict) else None
            oracle_text_excerpt = normalize_oracle_text_excerpt(row_oracle_text, max_chars=240)

            if oracle_text_excerpt is None and oracle_id not in missing_oracle_text_unknown_oracle_ids:
                add_unknown(
                    unknowns,
                    code="MISSING_ORACLE_TEXT",
                    input_value=oracle_id,
                    message="Card oracle text not found in local DB.",
                    reason="Proof attempt oracle anchoring incomplete for this oracle_id.",
                    suggestions=[],
                )
                missing_oracle_text_unknown_oracle_ids.add(oracle_id)

            oracle_anchor_cards.append(
                {
                    "oracle_id": oracle_id,
                    "name": row_name if isinstance(row_name, str) else None,
                    "type_line": row_type_line if isinstance(row_type_line, str) else None,
                    "mana_cost": row_mana_cost if isinstance(row_mana_cost, str) else None,
                    "oracle_text_excerpt": oracle_text_excerpt,
                    "source": "cards_db",
                    "excerpt_policy": {
                        "max_chars": 240,
                        "method": "first_240_chars",
                    },
                }
            )

        attempt["oracle_anchors_v1"] = {
            "cards": oracle_anchor_cards,
            "cards_total": len(oracle_anchor_cards),
            "oracle_ids_unique_total": len(involved_oracle_ids_unique),
        }
        attempt["oracle_anchor_refs_by_slot"] = oracle_anchor_refs_by_slot

        candidate_oracle_ids_raw_v2 = [
            oracle_id
            for oracle_id in (scaffold.get("card_oracle_ids") or [])
            if isinstance(oracle_id, str) and oracle_id != ""
        ]
        oracle_ids_raw_v2 = list(candidate_oracle_ids_raw_v2)
        commander_oracle_id_v2 = (commander_canonical_slot or {}).get("resolved_oracle_id")
        if isinstance(commander_oracle_id_v2, str) and commander_oracle_id_v2 != "":
            oracle_ids_raw_v2.append(commander_oracle_id_v2)
        oracle_ids_unique_sorted_v2 = sorted_unique([oid for oid in oracle_ids_raw_v2 if oid])

        cache_misses_v2 = [oid for oid in oracle_ids_unique_sorted_v2 if oid not in oracle_anchor_lookup_cache]
        if cache_misses_v2:
            lookup_rows_v2 = lookup_cards_by_oracle_id(db_snapshot_id, cache_misses_v2)
            for oid in cache_misses_v2:
                oracle_anchor_lookup_cache[oid] = lookup_rows_v2.get(oid)

        anchors_v2 = []
        for oracle_id in oracle_ids_unique_sorted_v2:
            row_v2 = oracle_anchor_lookup_cache.get(oracle_id)
            row_name_v2 = row_v2.get("name") if isinstance(row_v2, dict) else None
            row_type_line_v2 = row_v2.get("type_line") if isinstance(row_v2, dict) else None
            row_mana_cost_v2 = row_v2.get("mana_cost") if isinstance(row_v2, dict) else None
            row_oracle_text_v2 = row_v2.get("oracle_text") if isinstance(row_v2, dict) else None
            oracle_text_excerpt_v2 = normalize_oracle_text_excerpt(row_oracle_text_v2, max_chars=240)

            if row_v2 is None and oracle_id not in missing_oracle_text_unknown_oracle_ids:
                add_unknown(
                    unknowns,
                    code="MISSING_ORACLE_TEXT",
                    input_value=oracle_id,
                    message="Card oracle text not found in local DB.",
                    reason="Proof attempt oracle anchoring incomplete for this oracle_id.",
                    suggestions=[],
                )
                missing_oracle_text_unknown_oracle_ids.add(oracle_id)

            anchors_v2.append(
                {
                    "oracle_id": oracle_id,
                    "name": row_name_v2 if isinstance(row_name_v2, str) else None,
                    "type_line": row_type_line_v2 if isinstance(row_type_line_v2, str) else None,
                    "mana_cost": row_mana_cost_v2 if isinstance(row_mana_cost_v2, str) else None,
                    "oracle_text_excerpt": oracle_text_excerpt_v2,
                    "source": "cards_db",
                    "excerpt_policy": {
                        "max_chars": 240,
                        "method": "first_240_chars_whitespace_normalized",
                    },
                }
            )

        attempt["oracle_anchors_v2"] = {
            "cards": anchors_v2,
            "cards_total": len(anchors_v2),
            "oracle_ids_unique_total": len(oracle_ids_unique_sorted_v2),
            "oracle_ids_raw_total": len([x for x in oracle_ids_raw_v2 if x]),
            "oracle_ids_raw_including_duplicates": list(oracle_ids_raw_v2),
        }

        anchor_index_by_oracle_id_v2 = {
            row.get("oracle_id"): idx
            for idx, row in enumerate(anchors_v2)
            if isinstance(row.get("oracle_id"), str)
        }
        slot_ids_for_anchor_refs_v2 = sorted_unique([
            sid for sid in (scaffold.get("slot_ids") or []) if isinstance(sid, str)
        ])
        if (
            "C0" in slot_by_id
            and isinstance(commander_oracle_id_v2, str)
            and commander_oracle_id_v2 in oracle_ids_unique_sorted_v2
            and "C0" not in slot_ids_for_anchor_refs_v2
        ):
            slot_ids_for_anchor_refs_v2.append("C0")
        slot_ids_for_anchor_refs_v2.sort(key=slot_sort_key)

        oracle_anchor_refs_by_slot_v2 = {}
        slot_ids_by_oracle_id_v2_temp: Dict[str, List[str]] = {}
        for slot_id in slot_ids_for_anchor_refs_v2:
            slot_oracle_id = (slot_by_id.get(slot_id) or {}).get("resolved_oracle_id")
            slot_oracle_id_value = slot_oracle_id if isinstance(slot_oracle_id, str) else None
            anchor_index = (
                anchor_index_by_oracle_id_v2.get(slot_oracle_id_value)
                if isinstance(slot_oracle_id_value, str)
                else None
            )
            oracle_anchor_refs_by_slot_v2[slot_id] = {
                "oracle_id": slot_oracle_id_value,
                "anchor_index": anchor_index if isinstance(anchor_index, int) else None,
            }
            if isinstance(slot_oracle_id_value, str):
                slot_ids_by_oracle_id_v2_temp.setdefault(slot_oracle_id_value, []).append(slot_id)

        slot_ids_by_oracle_id_v2 = {
            oracle_id: sorted(slot_ids, key=slot_sort_key)
            for oracle_id, slot_ids in sorted(slot_ids_by_oracle_id_v2_temp.items(), key=lambda x: x[0])
        }

        attempt["oracle_anchor_refs_by_slot_v2"] = oracle_anchor_refs_by_slot_v2
        attempt["slot_ids_by_oracle_id_v2"] = slot_ids_by_oracle_id_v2

        attempt_hash_payload_v1 = {
            "candidate_id": attempt.get("candidate_id"),
            "candidate_hash_v1": attempt.get("candidate_hash_v1"),
            "attempt_version": attempt.get("attempt_version"),
            "status": attempt.get("status"),
            "evidence_pack": attempt.get("evidence_pack"),
            "proof_template": attempt.get("proof_template"),
            "proof_gaps_v0": attempt.get("proof_gaps_v0"),
        }
        attempt_hash_v1 = sha256_hex(stable_json_dumps(attempt_hash_payload_v1))
        attempt["attempt_hash_v1"] = attempt_hash_v1

        recomputed_attempt_hash_v1 = sha256_hex(
            stable_json_dumps(
                {
                    "candidate_id": attempt.get("candidate_id"),
                    "candidate_hash_v1": attempt.get("candidate_hash_v1"),
                    "attempt_version": attempt.get("attempt_version"),
                    "status": attempt.get("status"),
                    "evidence_pack": attempt.get("evidence_pack"),
                    "proof_template": attempt.get("proof_template"),
                    "proof_gaps_v0": attempt.get("proof_gaps_v0"),
                }
            )
        )
        if recomputed_attempt_hash_v1 != attempt_hash_v1:
            proof_attempt_hash_stable = False

        attempt_hash_payload_v2 = strip_hash_fields(attempt)
        attempt_hash_v2 = sha256_hex(stable_json_dumps(attempt_hash_payload_v2))
        attempt["attempt_hash_v2"] = attempt_hash_v2

        attempt_hash_payload_v3 = strip_hash_fields(attempt)
        attempt_hash_v3 = sha256_hex(stable_json_dumps(attempt_hash_payload_v3))
        attempt["attempt_hash_v3"] = attempt_hash_v3

        combo_proof_attempts_v0.append(attempt)

    proof_attempts_total_matches_scaffolds = len(combo_proof_attempts_v0) == len(combo_proof_scaffolds_v0)
    proof_attempt_hashes_in_order = [
        attempt.get("attempt_hash_v1")
        for attempt in combo_proof_attempts_v0
        if isinstance(attempt.get("attempt_hash_v1"), str)
    ]
    proof_attempts_hash_payload_v1 = {
        "proof_scaffolds_hash_v2": proof_scaffolds_hash_v2,
        "attempt_hashes": proof_attempt_hashes_in_order,
    }
    proof_attempts_hash_v1 = sha256_hex(stable_json_dumps(proof_attempts_hash_payload_v1))
    proof_attempt_hashes_v2_in_order = [
        attempt.get("attempt_hash_v2")
        for attempt in combo_proof_attempts_v0
        if isinstance(attempt.get("attempt_hash_v2"), str)
    ]
    proof_attempts_hash_payload_v2 = {
        "proof_attempts_hash_v1": proof_attempts_hash_v1,
        "attempt_hashes_v2": proof_attempt_hashes_v2_in_order,
    }
    proof_attempts_hash_v2 = sha256_hex(stable_json_dumps(proof_attempts_hash_payload_v2))
    proof_attempt_hashes_v3_in_order = [
        attempt.get("attempt_hash_v3")
        for attempt in combo_proof_attempts_v0
        if isinstance(attempt.get("attempt_hash_v3"), str)
    ]
    proof_attempts_hash_payload_v3 = {
        "proof_attempts_hash_v1": proof_attempts_hash_v1 if isinstance(proof_attempts_hash_v1, str) else None,
        "proof_attempts_hash_v2": proof_attempts_hash_v2 if isinstance(proof_attempts_hash_v2, str) else None,
        "attempt_hashes_v3": proof_attempt_hashes_v3_in_order,
    }
    proof_attempts_hash_v3 = sha256_hex(stable_json_dumps(proof_attempts_hash_payload_v3))

    state["combo_proof_attempts_v0"] = combo_proof_attempts_v0
    state["proof_attempt_hash_stable"] = proof_attempt_hash_stable
    state["proof_attempts_total_matches_scaffolds"] = proof_attempts_total_matches_scaffolds
    state["proof_attempts_hash_v1"] = proof_attempts_hash_v1
    state["proof_attempts_hash_v2"] = proof_attempts_hash_v2
    state["proof_attempts_hash_v3"] = proof_attempts_hash_v3

    return state
