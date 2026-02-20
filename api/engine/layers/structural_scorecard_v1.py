from __future__ import annotations

from typing import Any, Dict, List


STRUCTURAL_SCORECARD_V1_VERSION = "structural_scorecard_v1"

_INTERACTION_COVERAGE_TARGET_SLOTS = 10


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return int(value)
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def _payload_status(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    return _nonempty_str(payload.get("status"))


def _source_present(payload: Any) -> bool:
    return _payload_status(payload) is not None


def _clamp_score(value: float) -> int:
    return max(0, min(100, int(round(float(value)))))


def _mean(values: List[float]) -> float | None:
    if len(values) == 0:
        return None
    return float(sum(values) / float(len(values)))


def _grade_from_score(score_0_100: int | None) -> str | None:
    if score_0_100 is None:
        return None
    if score_0_100 >= 90:
        return "A"
    if score_0_100 >= 80:
        return "B"
    if score_0_100 >= 70:
        return "C"
    if score_0_100 >= 60:
        return "D"
    return "F"


def _policy_compliance_score(bracket_compliance: Any) -> int | None:
    status = _payload_status(bracket_compliance)
    if status is None or status == "SKIP":
        return None
    if status == "ERROR":
        return 0
    if status == "WARN":
        return 60
    if status == "OK":
        return 100
    return None


def _graph_cohesion_score(graph_analytics: Any) -> int | None:
    status = _payload_status(graph_analytics)
    if status is None or status == "SKIP":
        return None

    counts = graph_analytics.get("counts") if isinstance(graph_analytics.get("counts"), dict) else {}
    connectivity = graph_analytics.get("connectivity") if isinstance(graph_analytics.get("connectivity"), dict) else {}
    components = graph_analytics.get("components") if isinstance(graph_analytics.get("components"), dict) else {}

    total_nodes = _nonnegative_int(counts.get("nodes"))
    if total_nodes is None or total_nodes <= 0:
        total_nodes = _nonnegative_int(counts.get("playable_nodes")) or 0

    largest_component_nodes = _nonnegative_int(components.get("largest_component_nodes")) or 0
    component_count = _nonnegative_int(components.get("component_count")) or 0

    avg_out_degree = _number(connectivity.get("avg_out_degree")) or 0.0
    avg_in_degree = _number(connectivity.get("avg_in_degree")) or 0.0

    largest_share = 0.0
    if total_nodes > 0:
        largest_share = min(1.0, float(largest_component_nodes) / float(total_nodes))

    avg_degree = max(0.0, (avg_out_degree + avg_in_degree) / 2.0)
    degree_ratio = min(1.0, avg_degree / 2.0)

    component_penalty = min(60.0, float(max(0, component_count - 1)) * 12.0)
    cohesion_raw = (largest_share * 70.0) + (degree_ratio * 30.0) - component_penalty
    return _clamp_score(cohesion_raw)


def _interaction_coverage_score(disruption_surface: Any) -> int | None:
    status = _payload_status(disruption_surface)
    if status is None or status == "SKIP":
        return None

    totals = disruption_surface.get("totals") if isinstance(disruption_surface.get("totals"), dict) else {}
    disruption_slots = _nonnegative_int(totals.get("disruption_slots"))
    if disruption_slots is None:
        return None

    raw_score = (float(disruption_slots) / float(_INTERACTION_COVERAGE_TARGET_SLOTS)) * 100.0
    return min(100, _clamp_score(raw_score))


def _vulnerability_score(vulnerability_index: Any) -> int | None:
    status = _payload_status(vulnerability_index)
    if status is None or status == "SKIP":
        return None

    scores = vulnerability_index.get("scores") if isinstance(vulnerability_index.get("scores"), dict) else {}
    normalized_scores: List[float] = []
    for key in sorted(scores.keys()):
        value = _number(scores.get(key))
        if value is None:
            continue
        normalized_scores.append(max(0.0, min(1.0, value)))

    average_vulnerability = _mean(normalized_scores)
    if average_vulnerability is None:
        return None

    return _clamp_score((1.0 - average_vulnerability) * 100.0)


def _build_badges(
    *,
    bracket_compliance: Any,
    vulnerability_index: Any,
    typed_graph_invariants: Any,
) -> List[Dict[str, str]]:
    badges: List[Dict[str, str]] = []

    typed_status = _payload_status(typed_graph_invariants)
    if typed_status == "ERROR":
        badges.append(
            {
                "code": "GRAPH_INVARIANTS_ERROR",
                "severity": "ERROR",
                "message": "Typed graph invariants reported ERROR; graph-derived confidence is reduced.",
            }
        )

    bracket_status = _payload_status(bracket_compliance)
    if bracket_status == "ERROR":
        badges.append(
            {
                "code": "BRACKET_COMPLIANCE_ERROR",
                "severity": "ERROR",
                "message": "Bracket compliance summary reported ERROR.",
            }
        )
    elif bracket_status == "WARN":
        badges.append(
            {
                "code": "BRACKET_COMPLIANCE_WARN",
                "severity": "WARN",
                "message": "Bracket compliance summary reported WARN.",
            }
        )

    vulnerability_status = _payload_status(vulnerability_index)
    vulnerability_scores = (
        vulnerability_index.get("scores")
        if isinstance(vulnerability_index, dict) and isinstance(vulnerability_index.get("scores"), dict)
        else {}
    )

    if vulnerability_status is not None and vulnerability_status != "SKIP":
        single_engine_reliance = _number(vulnerability_scores.get("single_engine_reliance"))
        if single_engine_reliance is not None and single_engine_reliance >= 0.75:
            badges.append(
                {
                    "code": "HIGH_SINGLE_ENGINE_RELIANCE",
                    "severity": "WARN",
                    "message": "Single-engine reliance is high (>= 0.75).",
                }
            )

        interaction_exposure = _number(vulnerability_scores.get("interaction_exposure"))
        if interaction_exposure is not None and interaction_exposure >= 0.6:
            badges.append(
                {
                    "code": "HIGH_INTERACTION_EXPOSURE",
                    "severity": "WARN",
                    "message": "Interaction exposure is high (>= 0.6).",
                }
            )

    return sorted(
        badges,
        key=lambda badge: (
            str(badge.get("severity") or ""),
            str(badge.get("code") or ""),
        ),
    )


def _resolve_status_and_reason(
    *,
    headline_score_0_100: int | None,
    badges: List[Dict[str, str]],
    bracket_compliance: Any,
    typed_graph_invariants: Any,
) -> tuple[str, str | None]:
    typed_status = _payload_status(typed_graph_invariants)
    if typed_status == "ERROR":
        return "ERROR", "GRAPH_INVARIANTS_ERROR"

    if headline_score_0_100 is None:
        return "SKIP", "NO_SUBSCORES_AVAILABLE"

    bracket_status = _payload_status(bracket_compliance)
    if bracket_status == "ERROR":
        return "ERROR", "BRACKET_COMPLIANCE_ERROR"
    if bracket_status == "WARN":
        return "WARN", "BRACKET_COMPLIANCE_WARN"

    if any((badge.get("severity") == "ERROR") for badge in badges):
        return "ERROR", "ERROR_BADGES_PRESENT"

    if any((badge.get("severity") == "WARN") for badge in badges):
        return "WARN", "RISK_BADGES_PRESENT"

    return "OK", None


def run_structural_scorecard_v1(
    bracket_compliance: Any = None,
    graph_analytics: Any = None,
    disruption_surface: Any = None,
    vulnerability_index: Any = None,
    structural_snapshot_v1: Any = None,
    typed_graph_invariants: Any = None,
) -> dict:
    _ = structural_snapshot_v1

    policy_compliance = _policy_compliance_score(bracket_compliance)
    graph_cohesion = _graph_cohesion_score(graph_analytics)
    interaction_coverage = _interaction_coverage_score(disruption_surface)
    vulnerability = _vulnerability_score(vulnerability_index)

    available_subscores = [
        score
        for score in [policy_compliance, graph_cohesion, interaction_coverage, vulnerability]
        if score is not None
    ]

    headline_score_0_100: int | None = None
    if len(available_subscores) > 0:
        headline_score_0_100 = _clamp_score(sum(available_subscores) / float(len(available_subscores)))

    headline_grade = _grade_from_score(headline_score_0_100)

    badges = _build_badges(
        bracket_compliance=bracket_compliance,
        vulnerability_index=vulnerability_index,
        typed_graph_invariants=typed_graph_invariants,
    )

    status, reason = _resolve_status_and_reason(
        headline_score_0_100=headline_score_0_100,
        badges=badges,
        bracket_compliance=bracket_compliance,
        typed_graph_invariants=typed_graph_invariants,
    )

    return {
        "version": STRUCTURAL_SCORECARD_V1_VERSION,
        "status": status,
        "reason": reason,
        "headline": {
            "grade": headline_grade,
            "score_0_100": headline_score_0_100,
        },
        "subscores": {
            "policy_compliance": policy_compliance,
            "graph_cohesion": graph_cohesion,
            "interaction_coverage": interaction_coverage,
            "vulnerability": vulnerability,
        },
        "badges": badges,
        "sources": {
            "bracket_compliance_summary_v1": _source_present(bracket_compliance),
            "graph_analytics_summary_v1": _source_present(graph_analytics),
            "disruption_surface_v1": _source_present(disruption_surface),
            "vulnerability_index_v1": _source_present(vulnerability_index),
        },
    }
