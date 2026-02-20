from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


_WEIGHT_RULES_FILE = (
    Path(__file__).resolve().parent
    / "data"
    / "sufficiency"
    / "weight_rules_v1.json"
)


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _coerce_multiplier(value: Any, *, field_path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _runtime_error("WEIGHT_RULES_V1_INVALID", f"{field_path} must be numeric")

    multiplier = float(value)
    if multiplier < 0.0:
        raise _runtime_error("WEIGHT_RULES_V1_INVALID", f"{field_path} must be >= 0.0")

    return multiplier


def _normalize_rules(raw: Any, *, field_path: str) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        raise _runtime_error("WEIGHT_RULES_V1_INVALID", f"{field_path} must be an array")

    normalized: List[Dict[str, Any]] = []
    seen_rule_ids: set[str] = set()

    for index, row in enumerate(raw):
        if not isinstance(row, dict):
            raise _runtime_error("WEIGHT_RULES_V1_INVALID", f"{field_path}[{index}] must be an object")

        rule_id = _nonempty_str(row.get("rule_id"))
        if rule_id is None:
            raise _runtime_error(
                "WEIGHT_RULES_V1_INVALID",
                f"{field_path}[{index}].rule_id must be a non-empty string",
            )

        if rule_id in seen_rule_ids:
            raise _runtime_error(
                "WEIGHT_RULES_V1_INVALID",
                f"{field_path}[{index}].rule_id must be unique within the format",
            )
        seen_rule_ids.add(rule_id)

        target_bucket = _nonempty_str(row.get("target_bucket"))
        if target_bucket is None:
            raise _runtime_error(
                "WEIGHT_RULES_V1_INVALID",
                f"{field_path}[{index}].target_bucket must be a non-empty string",
            )

        requirement_flag = _nonempty_str(row.get("requirement_flag"))
        if requirement_flag is None:
            raise _runtime_error(
                "WEIGHT_RULES_V1_INVALID",
                f"{field_path}[{index}].requirement_flag must be a non-empty string",
            )

        multiplier = _coerce_multiplier(
            row.get("multiplier"),
            field_path=f"{field_path}[{index}].multiplier",
        )

        normalized.append(
            {
                "rule_id": rule_id,
                "target_bucket": target_bucket,
                "requirement_flag": requirement_flag,
                "multiplier": multiplier,
            }
        )

    return sorted(
        normalized,
        key=lambda entry: (
            str(entry.get("target_bucket") or ""),
            str(entry.get("rule_id") or ""),
            str(entry.get("requirement_flag") or ""),
            float(entry.get("multiplier") or 0.0),
        ),
    )


def _normalize_format_defaults(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("WEIGHT_RULES_V1_INVALID", "format_defaults must be an object")

    normalized: Dict[str, Any] = {}

    for format_key_raw in sorted(raw.keys(), key=lambda item: str(item)):
        format_key = _nonempty_str(format_key_raw)
        if format_key is None:
            raise _runtime_error("WEIGHT_RULES_V1_INVALID", "format_defaults keys must be non-empty strings")

        format_payload = raw.get(format_key_raw)
        if not isinstance(format_payload, dict):
            raise _runtime_error("WEIGHT_RULES_V1_INVALID", f"format_defaults.{format_key} must be an object")

        normalized[format_key] = {
            "rules": _normalize_rules(
                format_payload.get("rules"),
                field_path=f"format_defaults.{format_key}.rules",
            )
        }

    return normalized


def load_weight_rules_v1() -> Dict[str, Any]:
    if not _WEIGHT_RULES_FILE.is_file():
        raise _runtime_error("WEIGHT_RULES_V1_MISSING", str(_WEIGHT_RULES_FILE))

    try:
        parsed = json.loads(_WEIGHT_RULES_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("WEIGHT_RULES_V1_INVALID_JSON", str(_WEIGHT_RULES_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("WEIGHT_RULES_V1_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version is None:
        raise _runtime_error("WEIGHT_RULES_V1_INVALID", "version must be a non-empty string")

    return {
        "version": version,
        "format_defaults": _normalize_format_defaults(parsed.get("format_defaults")),
    }
