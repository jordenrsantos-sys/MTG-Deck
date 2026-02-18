from typing import Any, Dict, List


_LAYER_HASH_KEYS_V0 = [
    "graph_hash_v2",
    "motif_hash_v1",
    "disruption_hash_v1",
    "pathways_hash_v1",
    "combo_candidates_hash_v1",
    "proof_scaffolds_hash_v3",
]

_TOP_PRIMITIVES_LIMIT_V0 = 15
_TOP_CANDIDATES_LIMIT_V0 = 10


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _missing_panel(reason: str) -> Dict[str, Any]:
    return {
        "available": False,
        "reason": reason,
    }


def _extract_deck_payload(response_payload: Dict[str, Any]) -> Dict[str, Any]:
    return _as_dict(response_payload.get("deck_complete_v0"))


def _extract_build_report(response_payload: Dict[str, Any], deck_payload: Dict[str, Any]) -> Dict[str, Any]:
    build_report = _as_dict(deck_payload.get("build_report"))
    if build_report:
        return build_report
    if isinstance(response_payload.get("build_hash_v1"), str):
        return response_payload
    return {}


def _extract_result_payload(build_report: Dict[str, Any]) -> Dict[str, Any]:
    return _as_dict(build_report.get("result"))


def _pick_first_non_empty_str(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value != "":
            return value
    return None


def _extract_meta(
    stored_run: Dict[str, Any],
    request_payload: Dict[str, Any],
    deck_payload: Dict[str, Any],
    build_report: Dict[str, Any],
) -> Dict[str, Any]:
    deck_inputs = _as_dict(deck_payload.get("inputs"))

    return {
        "engine_version": _pick_first_non_empty_str(
            build_report.get("engine_version"),
            stored_run.get("engine_version"),
        ),
        "ruleset_version": _pick_first_non_empty_str(build_report.get("ruleset_version")),
        "bracket_definition_version": _pick_first_non_empty_str(build_report.get("bracket_definition_version")),
        "game_changers_version": _pick_first_non_empty_str(build_report.get("game_changers_version")),
        "db_snapshot_id": _pick_first_non_empty_str(
            build_report.get("db_snapshot_id"),
            stored_run.get("db_snapshot_id"),
            deck_inputs.get("db_snapshot_id"),
            request_payload.get("db_snapshot_id"),
        ),
        "profile_id": _pick_first_non_empty_str(
            build_report.get("profile_id"),
            stored_run.get("profile_id"),
            deck_inputs.get("profile_id"),
            request_payload.get("profile_id"),
        ),
        "bracket_id": _pick_first_non_empty_str(
            build_report.get("bracket_id"),
            stored_run.get("bracket_id"),
            deck_inputs.get("bracket_id"),
            request_payload.get("bracket_id"),
        ),
    }


def _extract_layer_hashes(stored_run: Dict[str, Any], result_payload: Dict[str, Any]) -> Dict[str, Any]:
    layer_hashes_stored = _as_dict(stored_run.get("layer_hashes"))
    out: Dict[str, Any] = {}

    for key in _LAYER_HASH_KEYS_V0:
        value = layer_hashes_stored.get(key)
        if not isinstance(value, str):
            value = result_payload.get(key)
        out[key] = value if isinstance(value, str) else None

    return out


def _panel_build_header(run_id: str | None, meta: Dict[str, Any], hashes: Dict[str, Any]) -> Dict[str, Any]:
    build_hash_v1 = hashes.get("build_hash_v1")
    if not isinstance(build_hash_v1, str) or build_hash_v1 == "":
        return _missing_panel("missing field build_hash_v1")

    return {
        "available": True,
        "run_id": run_id,
        "build_hash_v1": build_hash_v1,
        "engine_version": meta.get("engine_version"),
        "ruleset_version": meta.get("ruleset_version"),
        "bracket_definition_version": meta.get("bracket_definition_version"),
        "game_changers_version": meta.get("game_changers_version"),
        "db_snapshot_id": meta.get("db_snapshot_id"),
        "profile_id": meta.get("profile_id"),
        "bracket_id": meta.get("bracket_id"),
    }


def _panel_decklist(deck_payload: Dict[str, Any]) -> Dict[str, Any]:
    final_deck = _as_dict(deck_payload.get("final_deck"))
    commander = final_deck.get("commander")
    cards = final_deck.get("cards")

    if not isinstance(commander, str):
        return _missing_panel("missing field final_deck.commander")
    if not isinstance(cards, list):
        return _missing_panel("missing field final_deck.cards")

    return {
        "available": True,
        "commander": commander,
        "cards": list(cards),
        "cards_total": len(cards),
        "deck_size_total": len(cards) + 1,
    }


def _panel_structural_health(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    structural_coverage = result_payload.get("structural_coverage")
    commander_dependency_signal = result_payload.get("commander_dependency_signal")
    primitive_concentration_index = result_payload.get("primitive_concentration_index")
    dead_slot_ids = result_payload.get("dead_slot_ids")

    if not isinstance(structural_coverage, dict):
        return _missing_panel("missing field result.structural_coverage")
    if not isinstance(commander_dependency_signal, dict):
        return _missing_panel("missing field result.commander_dependency_signal")
    if not _is_number(primitive_concentration_index):
        return _missing_panel("missing field result.primitive_concentration_index")
    if not isinstance(dead_slot_ids, list):
        return _missing_panel("missing field result.dead_slot_ids")

    return {
        "available": True,
        "structural_coverage": structural_coverage,
        "commander_dependency_signal": commander_dependency_signal,
        "primitive_concentration_index": primitive_concentration_index,
        "dead_slot_ids_count": len(dead_slot_ids),
    }


def _panel_canonical_slots(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    canonical_slots_all = result_payload.get("canonical_slots_all")

    source_slots: List[Any] = []
    if isinstance(canonical_slots_all, list):
        source_slots = canonical_slots_all
    else:
        commander_slot = result_payload.get("commander_canonical_slot")
        deck_slots = result_payload.get("deck_cards_canonical_input_order")
        if isinstance(commander_slot, dict):
            source_slots.append(commander_slot)
        if isinstance(deck_slots, list):
            source_slots.extend(deck_slots)

    if not source_slots:
        return _missing_panel("missing field result.canonical_slots_all")

    slots_compact: List[Dict[str, Any]] = []
    totals = {
        "total": 0,
        "playable": 0,
        "nonplayable": 0,
        "unknown": 0,
    }

    for entry in source_slots:
        if not isinstance(entry, dict):
            continue
        status_value = entry.get("status")

        if status_value == "PLAYABLE":
            totals["playable"] += 1
        elif status_value == "UNKNOWN":
            totals["unknown"] += 1
        else:
            totals["nonplayable"] += 1

        slots_compact.append(
            {
                "slot_id": entry.get("slot_id"),
                "status": status_value,
                "input": entry.get("input"),
                "resolved_name": entry.get("resolved_name"),
                "codes": list(entry.get("codes")) if isinstance(entry.get("codes"), list) else [],
            }
        )

    totals["total"] = len(slots_compact)

    if not slots_compact:
        return _missing_panel("missing field result.canonical_slots_all")

    return {
        "available": True,
        "totals": totals,
        "slots": slots_compact,
    }


def _panel_primitive_index(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    primitive_index_totals = result_payload.get("primitive_index_totals")
    if not isinstance(primitive_index_totals, dict):
        return _missing_panel("missing field result.primitive_index_totals")

    sortable: List[tuple[str, int]] = []

    slot_ids_by_primitive = result_payload.get("slot_ids_by_primitive")
    if isinstance(slot_ids_by_primitive, dict):
        for primitive, slot_ids in slot_ids_by_primitive.items():
            if not isinstance(primitive, str):
                continue
            if isinstance(slot_ids, list):
                count = len([sid for sid in slot_ids if isinstance(sid, str)])
            else:
                count = 0
            sortable.append((primitive, int(count)))
    else:
        primitive_counts = result_payload.get("primitive_counts")
        if isinstance(primitive_counts, dict):
            for primitive, count in primitive_counts.items():
                if not isinstance(primitive, str):
                    continue
                if isinstance(count, int):
                    sortable.append((primitive, int(count)))

    if not sortable:
        return _missing_panel("missing field result.slot_ids_by_primitive")

    sortable.sort(key=lambda row: (-row[1], row[0]))

    return {
        "available": True,
        "primitive_index_totals": primitive_index_totals,
        "top_primitives": [
            {
                "primitive": primitive,
                "count": count,
            }
            for primitive, count in sortable[:_TOP_PRIMITIVES_LIMIT_V0]
        ],
    }


def _panel_graph_summary(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    graph_nodes_total = result_payload.get("graph_nodes_total")
    graph_edges_total = result_payload.get("graph_edges_total")

    if isinstance(graph_nodes_total, int):
        node_count = graph_nodes_total
    elif isinstance(result_payload.get("graph_nodes"), list):
        node_count = len(result_payload.get("graph_nodes"))
    else:
        node_count = None

    if isinstance(graph_edges_total, int):
        edge_count = graph_edges_total
    elif isinstance(result_payload.get("graph_edges"), list):
        edge_count = len(result_payload.get("graph_edges"))
    else:
        edge_count = None

    if not isinstance(node_count, int):
        return _missing_panel("missing field result.graph_nodes_total")
    if not isinstance(edge_count, int):
        return _missing_panel("missing field result.graph_edges_total")

    panel: Dict[str, Any] = {
        "available": True,
        "node_count": node_count,
        "edge_count": edge_count,
    }

    if isinstance(result_payload.get("graph_typed_edges_total"), int):
        panel["typed_edge_count"] = result_payload.get("graph_typed_edges_total")

    graph_totals = result_payload.get("graph_totals")
    if isinstance(graph_totals, dict) and isinstance(graph_totals.get("connected_components_total"), int):
        panel["components_count"] = graph_totals.get("connected_components_total")
    elif isinstance(result_payload.get("graph_components"), list):
        panel["components_count"] = len(result_payload.get("graph_components"))

    return panel


def _panel_motifs(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    motifs = result_payload.get("motifs")
    motif_totals = result_payload.get("motif_totals")

    if not isinstance(motifs, list) and not isinstance(motif_totals, dict):
        return _missing_panel("missing field result.motifs")

    top_motif_ids: List[str] = []
    if isinstance(motifs, list):
        for row in motifs[:_TOP_CANDIDATES_LIMIT_V0]:
            if isinstance(row, dict) and isinstance(row.get("motif_id"), str):
                top_motif_ids.append(row.get("motif_id"))

    panel: Dict[str, Any] = {
        "available": True,
        "motifs_total": len(motifs) if isinstance(motifs, list) else 0,
        "top_motif_ids": top_motif_ids,
    }

    if isinstance(motif_totals, dict):
        panel["motif_totals"] = motif_totals

    return panel


def _panel_disruption(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    disruption_totals = result_payload.get("disruption_totals")
    disruption_commander_risk = result_payload.get("disruption_commander_risk")

    if not isinstance(disruption_totals, dict) and not isinstance(disruption_commander_risk, dict):
        return _missing_panel("missing field result.disruption_totals")

    panel: Dict[str, Any] = {
        "available": True,
    }
    if isinstance(disruption_totals, dict):
        panel["disruption_totals"] = disruption_totals
    if isinstance(disruption_commander_risk, dict):
        panel["disruption_commander_risk"] = disruption_commander_risk

    return panel


def _panel_pathways(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    pathways_totals = result_payload.get("pathways_totals")

    reachable_total = None
    if isinstance(result_payload.get("pathways_commander_reachable_total"), int):
        reachable_total = result_payload.get("pathways_commander_reachable_total")
    elif isinstance(result_payload.get("pathways_commander_reachable_slots"), list):
        reachable_total = len(result_payload.get("pathways_commander_reachable_slots"))

    unreachable_total = None
    if isinstance(result_payload.get("pathways_commander_unreachable_total"), int):
        unreachable_total = result_payload.get("pathways_commander_unreachable_total")
    elif isinstance(result_payload.get("pathways_commander_unreachable_slots"), list):
        unreachable_total = len(result_payload.get("pathways_commander_unreachable_slots"))

    if not isinstance(pathways_totals, dict) and reachable_total is None and unreachable_total is None:
        return _missing_panel("missing field result.pathways_totals")

    panel: Dict[str, Any] = {
        "available": True,
    }
    if isinstance(pathways_totals, dict):
        panel["pathways_totals"] = pathways_totals
    if isinstance(reachable_total, int):
        panel["commander_reachable_total"] = reachable_total
    if isinstance(unreachable_total, int):
        panel["commander_unreachable_total"] = unreachable_total

    return panel


def _panel_combos(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    candidates = result_payload.get("combo_candidates_v0")
    candidates_total = result_payload.get("combo_candidates_v0_total")

    if isinstance(candidates, list):
        count = int(candidates_total) if isinstance(candidates_total, int) else len(candidates)
        top_candidate_ids = [
            row.get("candidate_id")
            for row in candidates
            if isinstance(row, dict) and isinstance(row.get("candidate_id"), str)
        ]
        return {
            "available": True,
            "candidates_total": count,
            "top_candidate_ids": top_candidate_ids[:_TOP_CANDIDATES_LIMIT_V0],
        }

    if isinstance(candidates_total, int):
        return {
            "available": True,
            "candidates_total": int(candidates_total),
            "top_candidate_ids": [],
        }

    return _missing_panel("missing field result.combo_candidates_v0")


def _panel_proofs(result_payload: Dict[str, Any]) -> Dict[str, Any]:
    proof_attempts = result_payload.get("combo_proof_attempts_v0")
    proof_scaffolds = result_payload.get("combo_proof_scaffolds_v0")

    if not isinstance(proof_attempts, list) and not isinstance(proof_scaffolds, list):
        return _missing_panel("missing field result.combo_proof_attempts_v0")

    attempts = proof_attempts if isinstance(proof_attempts, list) else []
    scaffolds = proof_scaffolds if isinstance(proof_scaffolds, list) else []

    oracle_anchors_cards_total = 0
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        anchors = attempt.get("oracle_anchors_v2")
        if not isinstance(anchors, dict):
            anchors = attempt.get("oracle_anchors_v1")
        if not isinstance(anchors, dict):
            continue

        cards_total = anchors.get("cards_total")
        if isinstance(cards_total, int):
            oracle_anchors_cards_total += int(cards_total)
        elif isinstance(anchors.get("cards"), list):
            oracle_anchors_cards_total += len(anchors.get("cards"))

    rules_coverage_values: List[float] = []
    scaffolds_with_rule_context = 0
    for scaffold in scaffolds:
        if not isinstance(scaffold, dict):
            continue
        if scaffold.get("status") == "UNPROVEN_WITH_RULE_CONTEXT":
            scaffolds_with_rule_context += 1
        rules_coverage_v1 = scaffold.get("rules_coverage_v1")
        if isinstance(rules_coverage_v1, dict) and _is_number(rules_coverage_v1.get("coverage_ratio")):
            rules_coverage_values.append(float(rules_coverage_v1.get("coverage_ratio")))

    panel: Dict[str, Any] = {
        "available": True,
        "proof_attempts_total": len(attempts),
        "proof_scaffolds_total": len(scaffolds),
        "oracle_anchors_cards_total": oracle_anchors_cards_total,
        "proof_scaffolds_with_rule_context_total": scaffolds_with_rule_context,
    }

    if rules_coverage_values:
        panel["proof_scaffold_coverage_ratio_mean"] = round(
            sum(rules_coverage_values) / float(len(rules_coverage_values)),
            4,
        )

    if isinstance(result_payload.get("proof_attempts_hash_v2"), str):
        panel["proof_attempts_hash_v2"] = result_payload.get("proof_attempts_hash_v2")

    return panel


def _panel_refinement_trace(deck_payload: Dict[str, Any]) -> Dict[str, Any]:
    refinement = deck_payload.get("refinement")
    if not isinstance(refinement, dict):
        return _missing_panel("missing field deck_complete_v0.refinement")

    iterations = deck_payload.get("iterations")
    iterations = iterations if isinstance(iterations, list) else []

    refine_iterations = [
        item
        for item in iterations
        if isinstance(item, dict) and item.get("iter_type") == "refine"
    ]

    return {
        "available": True,
        "iters_run": refinement.get("iters_run"),
        "best_score_v0": refinement.get("best_score_v0"),
        "accepted_swaps": refinement.get("accepted_swaps"),
        "rejected_swaps": refinement.get("rejected_swaps"),
        "iterations": refine_iterations,
    }


def build_run_bundle_v0(stored_run: Dict[str, Any]) -> Dict[str, Any]:
    run_obj = _as_dict(stored_run)

    request_payload = _as_dict(run_obj.get("request"))
    response_payload = _as_dict(run_obj.get("response"))

    deck_payload = _extract_deck_payload(response_payload)
    build_report = _extract_build_report(response_payload=response_payload, deck_payload=deck_payload)
    result_payload = _extract_result_payload(build_report)

    meta = _extract_meta(
        stored_run=run_obj,
        request_payload=request_payload,
        deck_payload=deck_payload,
        build_report=build_report,
    )

    hashes = {
        "input_hash_v1": run_obj.get("input_hash_v1") if isinstance(run_obj.get("input_hash_v1"), str) else None,
        "build_hash_v1": _pick_first_non_empty_str(
            run_obj.get("output_build_hash_v1"),
            build_report.get("build_hash_v1"),
        ),
        "proof_attempts_hash_v2": _pick_first_non_empty_str(
            run_obj.get("output_proof_attempts_hash_v2"),
            result_payload.get("proof_attempts_hash_v2"),
        ),
        "layer_hashes": _extract_layer_hashes(run_obj, result_payload),
    }

    run_id = run_obj.get("run_id") if isinstance(run_obj.get("run_id"), str) else None
    endpoint = run_obj.get("endpoint") if isinstance(run_obj.get("endpoint"), str) else None

    ui_panels = {
        "build_header": _panel_build_header(run_id=run_id, meta=meta, hashes=hashes),
        "decklist": _panel_decklist(deck_payload),
        "structural_health": _panel_structural_health(result_payload),
        "canonical_slots": _panel_canonical_slots(result_payload),
        "primitive_index": _panel_primitive_index(result_payload),
        "graph_summary": _panel_graph_summary(result_payload),
        "motifs": _panel_motifs(result_payload),
        "disruption": _panel_disruption(result_payload),
        "pathways": _panel_pathways(result_payload),
        "combos": _panel_combos(result_payload),
        "proofs": _panel_proofs(result_payload),
        "refinement_trace": _panel_refinement_trace(deck_payload),
    }

    return {
        "run_id": run_id,
        "endpoint": endpoint,
        "meta": meta,
        "hashes": hashes,
        "request": request_payload,
        "response": response_payload,
        "ui_panels": ui_panels,
    }
