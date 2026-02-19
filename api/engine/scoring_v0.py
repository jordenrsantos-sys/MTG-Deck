from typing import Any, Dict


def _round_metric(value: float) -> float:
    return float(f"{value:.6f}")


def score_deck_v0(state: Dict[str, Any]) -> Dict[str, Any]:
    result = state.get("result") if isinstance(state, dict) else {}
    result = result if isinstance(result, dict) else {}

    structural_snapshot_v1 = result.get("structural_snapshot_v1")
    structural_snapshot_v1 = structural_snapshot_v1 if isinstance(structural_snapshot_v1, dict) else {}

    required_primitives_v1 = structural_snapshot_v1.get("required_primitives_v1")
    required_primitives_v1 = required_primitives_v1 if isinstance(required_primitives_v1, list) else []
    missing_primitives_v1 = structural_snapshot_v1.get("missing_primitives_v1")
    missing_primitives_v1 = missing_primitives_v1 if isinstance(missing_primitives_v1, list) else []

    required_total = len(required_primitives_v1)
    required_met = max(required_total - len(missing_primitives_v1), 0)
    structural_completion_ratio_v1 = (required_met / required_total) if required_total > 0 else 0.0

    primitive_concentration_index_v1 = float(structural_snapshot_v1.get("primitive_concentration_index_v1") or 0.0)
    commander_overlap_ratio_v1 = float(structural_snapshot_v1.get("commander_dependency_signal_v1") or 0.0)

    dead_slot_ids_v1 = structural_snapshot_v1.get("dead_slot_ids_v1")
    dead_slot_ids_v1 = dead_slot_ids_v1 if isinstance(dead_slot_ids_v1, list) else []
    dead_slots_count_v1 = len(dead_slot_ids_v1)

    needs = result.get("needs")
    needs = needs if isinstance(needs, list) else []
    needs_penalty = min(float(len(needs)) / 10.0, 1.0)

    score_total = (
        (1.6 * structural_completion_ratio_v1)
        - (0.35 * primitive_concentration_index_v1)
        - (0.3 * commander_overlap_ratio_v1)
        - (0.03 * float(dead_slots_count_v1))
        - (0.2 * needs_penalty)
    )

    return {
        "score_total": _round_metric(score_total),
        "components": {
            "structural_completion_ratio_v1": _round_metric(structural_completion_ratio_v1),
            "primitive_concentration_index_v1": _round_metric(primitive_concentration_index_v1),
            "commander_overlap_ratio_v1": _round_metric(commander_overlap_ratio_v1),
            "dead_slots_count_v1": dead_slots_count_v1,
            "needs_penalty": _round_metric(needs_penalty),
        },
    }
