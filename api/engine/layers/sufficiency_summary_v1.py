from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple


SUFFICIENCY_SUMMARY_V1_VERSION = "sufficiency_summary_v1"

_DOMAIN_ORDER: tuple[str, ...] = (
    "required_effects",
    "baseline_prob",
    "stress_prob",
    "coherence",
    "resilience",
    "commander",
)

_REQUIRED_UPSTREAM_PAYLOADS: tuple[Tuple[str, str], ...] = (
    ("engine_requirement_detection_v1", "engine_requirement_detection_v1_payload"),
    ("engine_coherence_v1", "engine_coherence_v1_payload"),
    ("mulligan_model_v1", "mulligan_model_v1_payload"),
    ("substitution_engine_v1", "substitution_engine_v1_payload"),
    ("weight_multiplier_engine_v1", "weight_multiplier_engine_v1_payload"),
    ("probability_math_core_v1", "probability_math_core_v1_payload"),
    ("probability_checkpoint_layer_v1", "probability_checkpoint_layer_v1_payload"),
    ("stress_model_definition_v1", "stress_model_definition_v1_payload"),
    ("stress_transform_engine_v1", "stress_transform_engine_v1_payload"),
    ("resilience_math_engine_v1", "resilience_math_engine_v1_payload"),
    ("commander_reliability_model_v1", "commander_reliability_model_v1_payload"),
)

_VERSION_KEYS: tuple[str, ...] = (
    "engine_coherence_version",
    "mulligan_model_version",
    "substitution_engine_version",
    "weight_multiplier_engine_version",
    "probability_model_version",
    "probability_checkpoint_version",
    "stress_model_version",
    "stress_transform_version",
    "resilience_math_engine_version",
    "commander_reliability_model_version",
    "required_effects_version",
    "profile_thresholds_version",
    "calibration_snapshot_version",
    "sufficiency_summary_version",
)


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _payload_ready(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    status = _nonempty_str(payload.get("status"))
    return status in {"OK", "WARN"}


def _sorted_unique(items: Set[str]) -> List[str]:
    return sorted({item for item in items if isinstance(item, str) and item.strip() != ""})


def _default_domain_verdicts(*, status: str) -> Dict[str, Dict[str, Any]]:
    return {
        domain: {
            "status": status,
            "codes": [],
        }
        for domain in _DOMAIN_ORDER
    }


def _normalize_domains(raw_domains: Any) -> Dict[str, Dict[str, Any]]:
    source = raw_domains if isinstance(raw_domains, dict) else {}
    return {
        domain: _as_dict(source.get(domain))
        for domain in _DOMAIN_ORDER
    }


def _build_thresholds_used(*, payload: Any, format_token: str, profile_token: str) -> Dict[str, Any]:
    source = _as_dict(payload)
    return {
        "profile_thresholds_version": _nonempty_str(source.get("profile_thresholds_version")),
        "calibration_snapshot_version": _nonempty_str(source.get("calibration_snapshot_version")),
        "format": _nonempty_str(source.get("format")) or format_token,
        "requested_profile_id": _nonempty_str(source.get("requested_profile_id")) or profile_token,
        "selected_profile_id": _nonempty_str(source.get("selected_profile_id")) or "",
        "selection_source": _nonempty_str(source.get("selection_source")) or "unavailable",
        "domains": _normalize_domains(source.get("domains")),
    }


def _build_versions_used(*, pipeline_versions: Any, thresholds_used: Dict[str, Any]) -> Dict[str, Any]:
    source = _as_dict(pipeline_versions)
    versions_used: Dict[str, Any] = {}
    for key in _VERSION_KEYS:
        versions_used[key] = _nonempty_str(source.get(key))

    versions_used["profile_thresholds_version"] = (
        versions_used.get("profile_thresholds_version")
        or _nonempty_str(thresholds_used.get("profile_thresholds_version"))
    )
    versions_used["calibration_snapshot_version"] = (
        versions_used.get("calibration_snapshot_version")
        or _nonempty_str(thresholds_used.get("calibration_snapshot_version"))
    )

    return versions_used


def _result_payload(
    *,
    status: str,
    reason_code: str | None,
    codes: List[str],
    failures: List[str],
    warnings: List[str],
    domain_verdicts: Dict[str, Dict[str, Any]],
    thresholds_used: Dict[str, Any],
    versions_used: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "version": SUFFICIENCY_SUMMARY_V1_VERSION,
        "status": status,
        "reason_code": reason_code,
        "codes": list(codes),
        "failures": list(failures),
        "warnings": list(warnings),
        "domain_verdicts": domain_verdicts,
        "thresholds_used": thresholds_used,
        "versions_used": versions_used,
    }


def _finalize_domain(
    *,
    domain: str,
    fail_codes: Set[str],
    warn_codes: Set[str],
    domain_verdicts: Dict[str, Dict[str, Any]],
    failures: Set[str],
    warnings: Set[str],
) -> None:
    status = "PASS"
    if len(fail_codes) > 0:
        status = "FAIL"
    elif len(warn_codes) > 0:
        status = "WARN"

    domain_codes = _sorted_unique(set(fail_codes).union(warn_codes))
    domain_verdicts[domain] = {
        "status": status,
        "codes": domain_codes,
    }

    failures.update({code for code in fail_codes if isinstance(code, str)})
    warnings.update({code for code in warn_codes if isinstance(code, str)})


def _threshold_number(domain_thresholds: Dict[str, Any], key: str) -> float:
    value = domain_thresholds.get(key)
    if _is_number(value):
        return float(value)
    return 0.0


def run_sufficiency_summary_v1(
    *,
    format: Any,
    profile_id: Any,
    profile_thresholds_v1_payload: Any,
    engine_requirement_detection_v1_payload: Any,
    engine_coherence_v1_payload: Any,
    mulligan_model_v1_payload: Any,
    substitution_engine_v1_payload: Any,
    weight_multiplier_engine_v1_payload: Any,
    probability_math_core_v1_payload: Any,
    probability_checkpoint_layer_v1_payload: Any,
    stress_model_definition_v1_payload: Any,
    stress_transform_engine_v1_payload: Any,
    resilience_math_engine_v1_payload: Any,
    commander_reliability_model_v1_payload: Any,
    required_effects_coverage_v1_payload: Any = None,
    bracket_compliance_summary_v1_payload: Any = None,
    pipeline_versions: Any = None,
) -> Dict[str, Any]:
    _ = bracket_compliance_summary_v1_payload

    format_token = _nonempty_str(format) or ""
    profile_token = _nonempty_str(profile_id) or ""

    thresholds_used = _build_thresholds_used(
        payload=profile_thresholds_v1_payload,
        format_token=format_token,
        profile_token=profile_token,
    )
    versions_used = _build_versions_used(
        pipeline_versions=pipeline_versions,
        thresholds_used=thresholds_used,
    )

    upstream_by_name: Dict[str, Any] = {
        "engine_requirement_detection_v1_payload": engine_requirement_detection_v1_payload,
        "engine_coherence_v1_payload": engine_coherence_v1_payload,
        "mulligan_model_v1_payload": mulligan_model_v1_payload,
        "substitution_engine_v1_payload": substitution_engine_v1_payload,
        "weight_multiplier_engine_v1_payload": weight_multiplier_engine_v1_payload,
        "probability_math_core_v1_payload": probability_math_core_v1_payload,
        "probability_checkpoint_layer_v1_payload": probability_checkpoint_layer_v1_payload,
        "stress_model_definition_v1_payload": stress_model_definition_v1_payload,
        "stress_transform_engine_v1_payload": stress_transform_engine_v1_payload,
        "resilience_math_engine_v1_payload": resilience_math_engine_v1_payload,
        "commander_reliability_model_v1_payload": commander_reliability_model_v1_payload,
    }

    missing_upstream_codes: Set[str] = set()
    for component_name, payload_key in _REQUIRED_UPSTREAM_PAYLOADS:
        if not _payload_ready(upstream_by_name.get(payload_key)):
            missing_upstream_codes.add(f"SUFFICIENCY_REQUIRED_UPSTREAM_UNAVAILABLE_{component_name.upper()}")

    if len(missing_upstream_codes) > 0:
        return _result_payload(
            status="SKIP",
            reason_code="UPSTREAM_PHASE3_UNAVAILABLE",
            codes=_sorted_unique(missing_upstream_codes),
            failures=[],
            warnings=[],
            domain_verdicts=_default_domain_verdicts(status="SKIP"),
            thresholds_used=thresholds_used,
            versions_used=versions_used,
        )

    domains = _as_dict(thresholds_used.get("domains"))
    domains_ready = all(isinstance(domains.get(domain), dict) and len(domains.get(domain) or {}) > 0 for domain in _DOMAIN_ORDER)
    if not domains_ready or _nonempty_str(thresholds_used.get("selected_profile_id")) is None:
        return _result_payload(
            status="SKIP",
            reason_code="PROFILE_THRESHOLDS_UNAVAILABLE",
            codes=["SUFFICIENCY_PROFILE_THRESHOLDS_UNAVAILABLE"],
            failures=[],
            warnings=[],
            domain_verdicts=_default_domain_verdicts(status="SKIP"),
            thresholds_used=thresholds_used,
            versions_used=versions_used,
        )

    if _nonempty_str(thresholds_used.get("calibration_snapshot_version")) is None:
        return _result_payload(
            status="SKIP",
            reason_code="CALIBRATION_SNAPSHOT_UNAVAILABLE",
            codes=["SUFFICIENCY_CALIBRATION_SNAPSHOT_UNAVAILABLE"],
            failures=[],
            warnings=[],
            domain_verdicts=_default_domain_verdicts(status="SKIP"),
            thresholds_used=thresholds_used,
            versions_used=versions_used,
        )

    failures: Set[str] = set()
    warnings: Set[str] = set()
    domain_verdicts = _default_domain_verdicts(status="PASS")

    # required_effects
    required_effects_thresholds = _as_dict(domains.get("required_effects"))
    required_effects_fail_codes: Set[str] = set()
    required_effects_warn_codes: Set[str] = set()

    required_effects_payload = _as_dict(required_effects_coverage_v1_payload)
    required_effects_status = _nonempty_str(required_effects_payload.get("status"))
    if required_effects_status is None:
        required_effects_warn_codes.add("SUFFICIENCY_REQUIRED_EFFECTS_PAYLOAD_UNAVAILABLE")
    else:
        if required_effects_status == "WARN":
            required_effects_warn_codes.add("SUFFICIENCY_REQUIRED_EFFECTS_SOURCE_WARN")

        missing_total = len(_as_list(required_effects_payload.get("missing")))
        unknowns_total = len(_as_list(required_effects_payload.get("unknowns")))

        max_missing = int(_threshold_number(required_effects_thresholds, "max_missing"))
        max_unknowns = int(_threshold_number(required_effects_thresholds, "max_unknowns"))

        if missing_total > max_missing:
            required_effects_fail_codes.add("SUFFICIENCY_REQUIRED_EFFECTS_MISSING_EXCEEDS_MAX")
        if unknowns_total > max_unknowns:
            required_effects_warn_codes.add("SUFFICIENCY_REQUIRED_EFFECTS_UNKNOWNS_EXCEED_MAX")

    _finalize_domain(
        domain="required_effects",
        fail_codes=required_effects_fail_codes,
        warn_codes=required_effects_warn_codes,
        domain_verdicts=domain_verdicts,
        failures=failures,
        warnings=warnings,
    )

    # baseline_prob
    baseline_thresholds = _as_dict(domains.get("baseline_prob"))
    baseline_fail_codes: Set[str] = set()
    baseline_warn_codes: Set[str] = set()

    commander_payload = _as_dict(commander_reliability_model_v1_payload)
    if _nonempty_str(commander_payload.get("status")) == "WARN":
        baseline_warn_codes.add("SUFFICIENCY_BASELINE_SOURCE_WARN")

    commander_metrics = _as_dict(commander_payload.get("metrics"))
    baseline_checks = (
        ("cast_reliability_t3", "cast_reliability_t3_min", "T3"),
        ("cast_reliability_t4", "cast_reliability_t4_min", "T4"),
        ("cast_reliability_t6", "cast_reliability_t6_min", "T6"),
    )
    for metric_key, threshold_key, turn_label in baseline_checks:
        value = commander_metrics.get(metric_key)
        threshold = _threshold_number(baseline_thresholds, threshold_key)
        if not _is_number(value):
            baseline_warn_codes.add(f"SUFFICIENCY_BASELINE_CAST_RELIABILITY_{turn_label}_UNAVAILABLE")
            continue
        if float(value) < threshold:
            baseline_fail_codes.add(f"SUFFICIENCY_BASELINE_CAST_RELIABILITY_{turn_label}_BELOW_MIN")

    _finalize_domain(
        domain="baseline_prob",
        fail_codes=baseline_fail_codes,
        warn_codes=baseline_warn_codes,
        domain_verdicts=domain_verdicts,
        failures=failures,
        warnings=warnings,
    )

    # stress_prob
    stress_thresholds = _as_dict(domains.get("stress_prob"))
    stress_fail_codes: Set[str] = set()
    stress_warn_codes: Set[str] = set()

    resilience_payload = _as_dict(resilience_math_engine_v1_payload)
    if _nonempty_str(resilience_payload.get("status")) == "WARN":
        stress_warn_codes.add("SUFFICIENCY_STRESS_SOURCE_WARN")

    resilience_metrics = _as_dict(resilience_payload.get("metrics"))

    continuity = resilience_metrics.get("engine_continuity_after_removal")
    continuity_min = _threshold_number(stress_thresholds, "engine_continuity_after_removal_min")
    if not _is_number(continuity):
        stress_warn_codes.add("SUFFICIENCY_STRESS_CONTINUITY_UNAVAILABLE")
    elif float(continuity) < continuity_min:
        stress_fail_codes.add("SUFFICIENCY_STRESS_CONTINUITY_BELOW_MIN")

    rebuild = resilience_metrics.get("rebuild_after_wipe")
    rebuild_min = _threshold_number(stress_thresholds, "rebuild_after_wipe_min")
    if not _is_number(rebuild):
        stress_warn_codes.add("SUFFICIENCY_STRESS_REBUILD_UNAVAILABLE")
    elif float(rebuild) < rebuild_min:
        stress_fail_codes.add("SUFFICIENCY_STRESS_REBUILD_BELOW_MIN")

    graveyard_delta = resilience_metrics.get("graveyard_fragility_delta")
    graveyard_max = _threshold_number(stress_thresholds, "graveyard_fragility_delta_max")
    if not _is_number(graveyard_delta):
        stress_warn_codes.add("SUFFICIENCY_STRESS_GRAVEYARD_FRAGILITY_UNAVAILABLE")
    elif float(graveyard_delta) > graveyard_max:
        stress_fail_codes.add("SUFFICIENCY_STRESS_GRAVEYARD_FRAGILITY_ABOVE_MAX")

    _finalize_domain(
        domain="stress_prob",
        fail_codes=stress_fail_codes,
        warn_codes=stress_warn_codes,
        domain_verdicts=domain_verdicts,
        failures=failures,
        warnings=warnings,
    )

    # coherence
    coherence_thresholds = _as_dict(domains.get("coherence"))
    coherence_fail_codes: Set[str] = set()
    coherence_warn_codes: Set[str] = set()

    coherence_payload = _as_dict(engine_coherence_v1_payload)
    if _nonempty_str(coherence_payload.get("status")) == "WARN":
        coherence_warn_codes.add("SUFFICIENCY_COHERENCE_SOURCE_WARN")

    coherence_summary = _as_dict(coherence_payload.get("summary"))

    dead_slot_ratio = coherence_summary.get("dead_slot_ratio")
    dead_slot_ratio_max = _threshold_number(coherence_thresholds, "dead_slot_ratio_max")
    if not _is_number(dead_slot_ratio):
        coherence_warn_codes.add("SUFFICIENCY_COHERENCE_DEAD_SLOT_RATIO_UNAVAILABLE")
    elif float(dead_slot_ratio) > dead_slot_ratio_max:
        coherence_fail_codes.add("SUFFICIENCY_COHERENCE_DEAD_SLOT_RATIO_ABOVE_MAX")

    overlap_score = coherence_summary.get("overlap_score")
    overlap_score_min = _threshold_number(coherence_thresholds, "overlap_score_min")
    if not _is_number(overlap_score):
        coherence_warn_codes.add("SUFFICIENCY_COHERENCE_OVERLAP_SCORE_UNAVAILABLE")
    elif float(overlap_score) < overlap_score_min:
        coherence_fail_codes.add("SUFFICIENCY_COHERENCE_OVERLAP_SCORE_BELOW_MIN")

    _finalize_domain(
        domain="coherence",
        fail_codes=coherence_fail_codes,
        warn_codes=coherence_warn_codes,
        domain_verdicts=domain_verdicts,
        failures=failures,
        warnings=warnings,
    )

    # resilience
    resilience_thresholds = _as_dict(domains.get("resilience"))
    resilience_fail_codes: Set[str] = set()
    resilience_warn_codes: Set[str] = set()

    if _nonempty_str(resilience_payload.get("status")) == "WARN":
        resilience_warn_codes.add("SUFFICIENCY_RESILIENCE_SOURCE_WARN")

    resilience_commander_fragility = resilience_metrics.get("commander_fragility_delta")
    resilience_commander_fragility_max = _threshold_number(
        resilience_thresholds,
        "commander_fragility_delta_max",
    )
    if not _is_number(resilience_commander_fragility):
        resilience_warn_codes.add("SUFFICIENCY_RESILIENCE_COMMANDER_FRAGILITY_UNAVAILABLE")
    elif float(resilience_commander_fragility) > resilience_commander_fragility_max:
        resilience_fail_codes.add("SUFFICIENCY_RESILIENCE_COMMANDER_FRAGILITY_ABOVE_MAX")

    _finalize_domain(
        domain="resilience",
        fail_codes=resilience_fail_codes,
        warn_codes=resilience_warn_codes,
        domain_verdicts=domain_verdicts,
        failures=failures,
        warnings=warnings,
    )

    # commander
    commander_thresholds = _as_dict(domains.get("commander"))
    commander_fail_codes: Set[str] = set()
    commander_warn_codes: Set[str] = set()

    if _nonempty_str(commander_payload.get("status")) == "WARN":
        commander_warn_codes.add("SUFFICIENCY_COMMANDER_SOURCE_WARN")

    commander_dependent = _nonempty_str(commander_payload.get("commander_dependent")) or "UNKNOWN"

    protection_proxy = commander_metrics.get("protection_coverage_proxy")
    protection_proxy_min = _threshold_number(commander_thresholds, "protection_coverage_proxy_min")
    if commander_dependent != "LOW":
        if not _is_number(protection_proxy):
            commander_warn_codes.add("SUFFICIENCY_COMMANDER_PROTECTION_PROXY_UNAVAILABLE")
        elif float(protection_proxy) < protection_proxy_min:
            commander_fail_codes.add("SUFFICIENCY_COMMANDER_PROTECTION_PROXY_BELOW_MIN")

    commander_fragility_delta = commander_metrics.get("commander_fragility_delta")
    commander_fragility_delta_max = _threshold_number(commander_thresholds, "commander_fragility_delta_max")
    if not _is_number(commander_fragility_delta):
        commander_warn_codes.add("SUFFICIENCY_COMMANDER_FRAGILITY_DELTA_UNAVAILABLE")
    elif float(commander_fragility_delta) > commander_fragility_delta_max:
        commander_fail_codes.add("SUFFICIENCY_COMMANDER_FRAGILITY_DELTA_ABOVE_MAX")

    _finalize_domain(
        domain="commander",
        fail_codes=commander_fail_codes,
        warn_codes=commander_warn_codes,
        domain_verdicts=domain_verdicts,
        failures=failures,
        warnings=warnings,
    )

    failures_sorted = _sorted_unique(failures)
    warnings_sorted = _sorted_unique(warnings)

    status = "PASS"
    if len(failures_sorted) > 0:
        status = "FAIL"
    elif len(warnings_sorted) > 0:
        status = "WARN"

    all_codes = _sorted_unique(set(failures_sorted).union(set(warnings_sorted)))

    return _result_payload(
        status=status,
        reason_code=None,
        codes=all_codes,
        failures=failures_sorted,
        warnings=warnings_sorted,
        domain_verdicts=domain_verdicts,
        thresholds_used=thresholds_used,
        versions_used=versions_used,
    )
