from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


_BUCKET_SUBSTITUTIONS_FILE = (
    Path(__file__).resolve().parent
    / "data"
    / "sufficiency"
    / "bucket_substitutions_v1.json"
)


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _normalize_nonempty_string_list(raw: Any, *, field_path: str) -> List[str]:
    if not isinstance(raw, list):
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"{field_path} must be an array")

    cleaned = {
        token
        for token in (_nonempty_str(item) for item in raw)
        if token is not None
    }

    if len(cleaned) == 0:
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"{field_path} must include at least one non-empty string")

    return sorted(cleaned)


def _coerce_weight(value: Any, *, field_path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"{field_path} must be numeric")

    weight = float(value)
    if weight < 0.0 or weight > 1.0:
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"{field_path} must be in [0.0, 1.0]")

    return weight


def _normalize_substitution_rows(raw: Any, *, field_path: str) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"{field_path} must be an array")

    rows: List[Dict[str, Any]] = []
    for index, row in enumerate(raw):
        if not isinstance(row, dict):
            raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"{field_path}[{index}] must be an object")

        primitive = _nonempty_str(row.get("primitive"))
        if primitive is None:
            raise _runtime_error(
                "BUCKET_SUBSTITUTIONS_V1_INVALID",
                f"{field_path}[{index}].primitive must be a non-empty string",
            )

        weight = _coerce_weight(row.get("weight"), field_path=f"{field_path}[{index}].weight")

        rows.append(
            {
                "primitive": primitive,
                "weight": weight,
            }
        )

    return sorted(rows, key=lambda entry: (str(entry.get("primitive") or ""), float(entry.get("weight") or 0.0)))


def _normalize_conditional_rows(raw: Any, *, field_path: str) -> List[Dict[str, Any]]:
    if not isinstance(raw, list):
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"{field_path} must be an array")

    rows: List[Dict[str, Any]] = []
    for index, row in enumerate(raw):
        if not isinstance(row, dict):
            raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"{field_path}[{index}] must be an object")

        requirement_flag = _nonempty_str(row.get("requirement_flag"))
        if requirement_flag is None:
            raise _runtime_error(
                "BUCKET_SUBSTITUTIONS_V1_INVALID",
                f"{field_path}[{index}].requirement_flag must be a non-empty string",
            )

        substitutions = _normalize_substitution_rows(
            row.get("substitutions"),
            field_path=f"{field_path}[{index}].substitutions",
        )

        rows.append(
            {
                "requirement_flag": requirement_flag,
                "substitutions": substitutions,
            }
        )

    return sorted(
        rows,
        key=lambda entry: (
            str(entry.get("requirement_flag") or ""),
            json.dumps(entry.get("substitutions") or [], ensure_ascii=True, sort_keys=True),
        ),
    )


def _normalize_bucket_payload(raw: Any, *, field_path: str) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"{field_path} must be an object")

    return {
        "primary_primitives": _normalize_nonempty_string_list(
            raw.get("primary_primitives"),
            field_path=f"{field_path}.primary_primitives",
        ),
        "base_substitutions": _normalize_substitution_rows(
            raw.get("base_substitutions"),
            field_path=f"{field_path}.base_substitutions",
        ),
        "conditional_substitutions": _normalize_conditional_rows(
            raw.get("conditional_substitutions"),
            field_path=f"{field_path}.conditional_substitutions",
        ),
    }


def _normalize_format_defaults(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", "format_defaults must be an object")

    normalized: Dict[str, Any] = {}

    for format_key_raw in sorted(raw.keys(), key=lambda item: str(item)):
        format_key = _nonempty_str(format_key_raw)
        if format_key is None:
            raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", "format_defaults keys must be non-empty strings")

        format_payload = raw.get(format_key_raw)
        if not isinstance(format_payload, dict):
            raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"format_defaults.{format_key} must be an object")

        buckets_raw = format_payload.get("buckets")
        if not isinstance(buckets_raw, dict):
            raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"format_defaults.{format_key}.buckets must be an object")

        if len(buckets_raw) == 0:
            raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", f"format_defaults.{format_key}.buckets must not be empty")

        normalized_buckets: Dict[str, Any] = {}
        for bucket_key_raw in sorted(buckets_raw.keys(), key=lambda item: str(item)):
            bucket_key = _nonempty_str(bucket_key_raw)
            if bucket_key is None:
                raise _runtime_error(
                    "BUCKET_SUBSTITUTIONS_V1_INVALID",
                    f"format_defaults.{format_key}.buckets keys must be non-empty strings",
                )

            normalized_buckets[bucket_key] = _normalize_bucket_payload(
                buckets_raw.get(bucket_key_raw),
                field_path=f"format_defaults.{format_key}.buckets.{bucket_key}",
            )

        normalized[format_key] = {
            "buckets": normalized_buckets,
        }

    return normalized


def load_bucket_substitutions_v1() -> Dict[str, Any]:
    if not _BUCKET_SUBSTITUTIONS_FILE.is_file():
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_MISSING", str(_BUCKET_SUBSTITUTIONS_FILE))

    try:
        parsed = json.loads(_BUCKET_SUBSTITUTIONS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID_JSON", str(_BUCKET_SUBSTITUTIONS_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version is None:
        raise _runtime_error("BUCKET_SUBSTITUTIONS_V1_INVALID", "version must be a non-empty string")

    return {
        "version": version,
        "format_defaults": _normalize_format_defaults(parsed.get("format_defaults")),
    }
