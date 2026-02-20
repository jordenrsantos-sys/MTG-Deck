from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Tuple

_GC_LIMITS_FILE = Path(__file__).resolve().parent / "data" / "brackets" / "gc_limits_v1.json"
_BRACKET_INT_RE = re.compile(r"^B(\d+)$")


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _coerce_limit(value: Any, *, field_path: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _runtime_error("GC_LIMITS_V1_INVALID", f"{field_path} must be a non-negative integer or null")
    return int(value)


def _normalize_rule_obj(raw: Any, *, field_path: str) -> Dict[str, int | None]:
    if not isinstance(raw, dict):
        raise _runtime_error("GC_LIMITS_V1_INVALID", f"{field_path} must be an object")
    if "min" not in raw or "max" not in raw:
        raise _runtime_error("GC_LIMITS_V1_INVALID", f"{field_path} must include min and max")

    return {
        "min": _coerce_limit(raw.get("min"), field_path=f"{field_path}.min"),
        "max": _coerce_limit(raw.get("max"), field_path=f"{field_path}.max"),
    }


def load_gc_limits_v1() -> dict:
    if not _GC_LIMITS_FILE.is_file():
        raise _runtime_error("GC_LIMITS_V1_MISSING", str(_GC_LIMITS_FILE))

    try:
        parsed = json.loads(_GC_LIMITS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("GC_LIMITS_V1_INVALID_JSON", str(_GC_LIMITS_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("GC_LIMITS_V1_INVALID", "root must be an object")

    version_raw = parsed.get("version")
    if not isinstance(version_raw, str) or version_raw.strip() == "":
        raise _runtime_error("GC_LIMITS_V1_INVALID", "version must be a non-empty string")
    version = version_raw.strip()

    rules_raw = parsed.get("rules")
    if not isinstance(rules_raw, dict):
        raise _runtime_error("GC_LIMITS_V1_INVALID", "rules must be an object")

    rules: Dict[str, Dict[str, int | None]] = {}
    for bracket_id in sorted(rules_raw.keys()):
        if not isinstance(bracket_id, str) or bracket_id.strip() == "":
            raise _runtime_error("GC_LIMITS_V1_INVALID", "rules keys must be non-empty strings")
        rules[bracket_id] = _normalize_rule_obj(rules_raw.get(bracket_id), field_path=f"rules.{bracket_id}")

    default_for_b_ge_4 = _normalize_rule_obj(
        parsed.get("default_for_b_ge_4"), field_path="default_for_b_ge_4"
    )
    default_for_unknown = _normalize_rule_obj(
        parsed.get("default_for_unknown"), field_path="default_for_unknown"
    )

    return {
        "version": version,
        "rules": rules,
        "default_for_b_ge_4": default_for_b_ge_4,
        "default_for_unknown": default_for_unknown,
    }


def resolve_gc_limits(bracket_id: str) -> Tuple[int | None, int | None, str, bool]:
    limits_payload = load_gc_limits_v1()

    version = limits_payload["version"]
    rules = limits_payload["rules"]

    bracket_token = bracket_id.strip() if isinstance(bracket_id, str) else ""

    rule_obj = rules.get(bracket_token)
    unknown_flag = False

    if not isinstance(rule_obj, dict):
        match = _BRACKET_INT_RE.fullmatch(bracket_token)
        if match is not None and int(match.group(1)) >= 4:
            rule_obj = limits_payload["default_for_b_ge_4"]
        else:
            rule_obj = limits_payload["default_for_unknown"]
            unknown_flag = True

    min_val = rule_obj.get("min") if isinstance(rule_obj, dict) else None
    max_val = rule_obj.get("max") if isinstance(rule_obj, dict) else None

    return min_val, max_val, version, unknown_flag
