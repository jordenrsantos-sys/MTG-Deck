from typing import Any, Dict


def _round_metric(value: float) -> float:
    return float(f"{value:.6f}")


def score_deck_v0(state: Dict[str, Any]) -> Dict[str, Any]:
    result = state.get("result") if isinstance(state, dict) else {}
    result = result if isinstance(result, dict) else {}

    structural_coverage = result.get("structural_coverage")
    structural_coverage = structural_coverage if isinstance(structural_coverage, dict) else {}
    required_primitives = structural_coverage.get("required_primitives_v0")
    required_primitives = required_primitives if isinstance(required_primitives, list) else []

    required_total = len(required_primitives)
    required_met = sum(
        1
        for item in required_primitives
        if isinstance(item, dict) and item.get("meets_minimum") is True
    )
    structural_coverage_ratio = (required_met / required_total) if required_total > 0 else 0.0

    primitive_concentration_index = float(result.get("primitive_concentration_index") or 0.0)
    commander_dependency_signal = result.get("commander_dependency_signal")
    commander_dependency_signal = commander_dependency_signal if isinstance(commander_dependency_signal, dict) else {}
    commander_overlap_ratio = float(commander_dependency_signal.get("overlap_ratio") or 0.0)

    dead_slot_ids = result.get("dead_slot_ids")
    dead_slot_ids = dead_slot_ids if isinstance(dead_slot_ids, list) else []
    dead_slot_ids_count = len(dead_slot_ids)

    needs = result.get("needs")
    needs = needs if isinstance(needs, list) else []
    needs_penalty = min(float(len(needs)) / 10.0, 1.0)

    score_total = (
        (1.6 * structural_coverage_ratio)
        - (0.35 * primitive_concentration_index)
        - (0.3 * commander_overlap_ratio)
        - (0.03 * float(dead_slot_ids_count))
        - (0.2 * needs_penalty)
    )

    return {
        "score_total": _round_metric(score_total),
        "components": {
            "structural_coverage_ratio": _round_metric(structural_coverage_ratio),
            "primitive_concentration_index": _round_metric(primitive_concentration_index),
            "commander_overlap_ratio": _round_metric(commander_overlap_ratio),
            "dead_slot_ids_count": dead_slot_ids_count,
            "needs_penalty": _round_metric(needs_penalty),
        },
    }
