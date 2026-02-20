from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Set


WEIGHT_MULTIPLIER_ENGINE_V1_VERSION = "weight_multiplier_engine_v1"


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _round6_half_up(value: float) -> float:
    return float(Decimal(str(float(value))).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _clean_sorted_unique_strings(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []

    cleaned = {
        token
        for token in (_nonempty_str(item) for item in values)
        if token is not None
    }
    return sorted(cleaned)


def _extract_requirement_flags(engine_requirement_detection_v1_payload: Any) -> tuple[Dict[str, Any], bool]:
    if not isinstance(engine_requirement_detection_v1_payload, dict):
        return {}, False

    engine_requirements = engine_requirement_detection_v1_payload.get("engine_requirements_v1")
    if not isinstance(engine_requirements, dict):
        return {}, False

    return dict(engine_requirements), True


def _extract_substitution_buckets(substitution_engine_v1_payload: Any) -> List[str]:
    if not isinstance(substitution_engine_v1_payload, dict):
        return []

    buckets = substitution_engine_v1_payload.get("buckets")
    if not isinstance(buckets, list):
        return []

    bucket_ids: List[str] = []
    for row in buckets:
        if not isinstance(row, dict):
            continue
        bucket = _nonempty_str(row.get("bucket"))
        if bucket is None:
            continue
        bucket_ids.append(bucket)

    return _clean_sorted_unique_strings(bucket_ids)


def _skip_payload(*, reason_code: str, weight_rules_version: str | None, format_token: str) -> Dict[str, Any]:
    return {
        "version": WEIGHT_MULTIPLIER_ENGINE_V1_VERSION,
        "status": "SKIP",
        "reason_code": reason_code,
        "codes": [],
        "weight_rules_version": weight_rules_version,
        "format": format_token,
        "multipliers_by_bucket": [],
        "applied_rules": [],
    }


def run_weight_multiplier_engine_v1(
    *,
    engine_requirement_detection_v1_payload: Any,
    substitution_engine_v1_payload: Any,
    format: Any,
    weight_rules_payload: Any,
) -> Dict[str, Any]:
    format_token = _nonempty_str(format) or ""

    if not isinstance(weight_rules_payload, dict):
        return _skip_payload(
            reason_code="WEIGHT_RULES_UNAVAILABLE",
            weight_rules_version=None,
            format_token=format_token,
        )

    weight_rules_version = _nonempty_str(weight_rules_payload.get("version"))
    format_defaults = weight_rules_payload.get("format_defaults")
    if not isinstance(format_defaults, dict):
        return _skip_payload(
            reason_code="WEIGHT_RULES_UNAVAILABLE",
            weight_rules_version=weight_rules_version,
            format_token=format_token,
        )

    format_entry = format_defaults.get(format_token)
    if not isinstance(format_entry, dict):
        format_entry = format_defaults.get(format_token.lower()) if isinstance(format_token, str) else None

    if not isinstance(format_entry, dict):
        return _skip_payload(
            reason_code="FORMAT_WEIGHT_RULES_UNAVAILABLE",
            weight_rules_version=weight_rules_version,
            format_token=format_token,
        )

    rules_raw = format_entry.get("rules")
    if not isinstance(rules_raw, list):
        return _skip_payload(
            reason_code="FORMAT_WEIGHT_RULES_UNAVAILABLE",
            weight_rules_version=weight_rules_version,
            format_token=format_token,
        )

    requirement_flags, has_requirement_flags = _extract_requirement_flags(engine_requirement_detection_v1_payload)

    codes: Set[str] = set()
    if not has_requirement_flags:
        codes.add("ENGINE_REQUIREMENTS_UNAVAILABLE")

    rules: List[Dict[str, Any]] = []
    candidate_bucket_ids: Set[str] = set(_extract_substitution_buckets(substitution_engine_v1_payload))

    for row in rules_raw:
        if not isinstance(row, dict):
            continue

        rule_id = _nonempty_str(row.get("rule_id"))
        target_bucket = _nonempty_str(row.get("target_bucket"))
        requirement_flag = _nonempty_str(row.get("requirement_flag"))
        multiplier_raw = row.get("multiplier")

        if (
            rule_id is None
            or target_bucket is None
            or requirement_flag is None
            or isinstance(multiplier_raw, bool)
            or not isinstance(multiplier_raw, (int, float))
            or float(multiplier_raw) < 0.0
        ):
            continue

        multiplier = float(multiplier_raw)

        candidate_bucket_ids.add(target_bucket)
        rules.append(
            {
                "rule_id": rule_id,
                "target_bucket": target_bucket,
                "requirement_flag": requirement_flag,
                "multiplier": multiplier,
            }
        )

    rules_sorted = sorted(
        rules,
        key=lambda entry: (
            str(entry.get("target_bucket") or ""),
            str(entry.get("rule_id") or ""),
            str(entry.get("requirement_flag") or ""),
            float(entry.get("multiplier") or 0.0),
        ),
    )

    multiplier_by_bucket: Dict[str, float] = {
        bucket: 1.0
        for bucket in sorted(candidate_bucket_ids)
    }

    applied_rules: List[Dict[str, Any]] = []

    for rule in rules_sorted:
        requirement_flag = str(rule.get("requirement_flag") or "")
        requirement_value = requirement_flags.get(requirement_flag)

        if not isinstance(requirement_value, bool):
            codes.add("WEIGHT_RULE_REQUIREMENT_FLAG_UNAVAILABLE")
            continue

        if requirement_value is not True:
            continue

        bucket = str(rule.get("target_bucket") or "")
        if bucket == "":
            continue

        current = float(multiplier_by_bucket.get(bucket, 1.0))
        next_value = current * float(rule.get("multiplier") or 0.0)
        multiplier_by_bucket[bucket] = next_value

        applied_rules.append(
            {
                "rule_id": str(rule.get("rule_id") or ""),
                "target_bucket": bucket,
                "requirement_flag": requirement_flag,
                "multiplier": _round6_half_up(float(rule.get("multiplier") or 0.0)),
            }
        )

    multipliers_by_bucket = [
        {
            "bucket": bucket,
            "multiplier": _round6_half_up(float(multiplier_by_bucket[bucket])),
        }
        for bucket in sorted(multiplier_by_bucket.keys())
    ]

    status = "OK"
    codes_sorted = sorted(codes)
    if len(codes_sorted) > 0:
        status = "WARN"

    return {
        "version": WEIGHT_MULTIPLIER_ENGINE_V1_VERSION,
        "status": status,
        "reason_code": None,
        "codes": codes_sorted,
        "weight_rules_version": weight_rules_version,
        "format": format_token,
        "multipliers_by_bucket": multipliers_by_bucket,
        "applied_rules": applied_rules,
    }
