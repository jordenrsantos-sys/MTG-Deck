from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Set, Tuple


_REQUIRED_EFFECTS_FILE = Path(__file__).resolve().parent / "data" / "requirements" / "required_effects_v1.json"
_REPO_ROOT = Path(__file__).resolve().parents[2]
_TAXONOMY_PACKS_DIR = _REPO_ROOT / "taxonomy" / "packs"


def _runtime_error(code: str, detail: str) -> RuntimeError:
    return RuntimeError(f"{code}: {detail}")


def _nonempty_str(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _coerce_nonnegative_int(value: Any, *, field_path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _runtime_error("REQUIRED_EFFECTS_V1_INVALID", f"{field_path} must be a non-negative integer")
    return int(value)


def _normalize_requirements_for_format(raw: Any, *, format_key: str) -> Dict[str, int]:
    if not isinstance(raw, dict):
        raise _runtime_error("REQUIRED_EFFECTS_V1_INVALID", f"format_defaults.{format_key} must be an object")

    normalized: Dict[str, int] = {}
    for primitive_key in sorted(raw.keys(), key=lambda item: str(item)):
        primitive_id = _nonempty_str(primitive_key)
        if primitive_id is None:
            raise _runtime_error("REQUIRED_EFFECTS_V1_INVALID", "requirement keys must be non-empty strings")

        requirement_entry = raw.get(primitive_key)
        if not isinstance(requirement_entry, dict):
            raise _runtime_error(
                "REQUIRED_EFFECTS_V1_INVALID",
                f"format_defaults.{format_key}.{primitive_id} must be an object",
            )

        if "min" not in requirement_entry:
            raise _runtime_error(
                "REQUIRED_EFFECTS_V1_INVALID",
                f"format_defaults.{format_key}.{primitive_id} must include min",
            )

        normalized[primitive_id] = _coerce_nonnegative_int(
            requirement_entry.get("min"),
            field_path=f"format_defaults.{format_key}.{primitive_id}.min",
        )

    return normalized


def _taxonomy_pack_dir(taxonomy_version: str | None = None) -> Path | None:
    requested = _nonempty_str(taxonomy_version)
    if requested is not None:
        candidate = _TAXONOMY_PACKS_DIR / requested
        if candidate.is_dir():
            return candidate

    if not _TAXONOMY_PACKS_DIR.is_dir():
        return None

    pack_dirs = sorted(
        [entry for entry in _TAXONOMY_PACKS_DIR.iterdir() if entry.is_dir()],
        key=lambda path: path.name,
    )
    if len(pack_dirs) == 0:
        return None

    return pack_dirs[-1]


def _load_taxonomy_primitive_ids(*, taxonomy_version: str | None = None) -> Set[str]:
    pack_dir = _taxonomy_pack_dir(taxonomy_version=taxonomy_version)
    if pack_dir is None:
        return set()

    primitives_file = pack_dir / "primitives.json"
    if not primitives_file.is_file():
        return set()

    try:
        parsed = json.loads(primitives_file.read_text(encoding="utf-8"))
    except Exception:
        return set()

    if not isinstance(parsed, list):
        return set()

    primitive_ids: Set[str] = set()
    for entry in parsed:
        if not isinstance(entry, dict):
            continue
        primitive_id = _nonempty_str(entry.get("primitive_id"))
        if primitive_id is not None:
            primitive_ids.add(primitive_id)

    return primitive_ids


def load_required_effects_v1() -> dict:
    if not _REQUIRED_EFFECTS_FILE.is_file():
        raise _runtime_error("REQUIRED_EFFECTS_V1_MISSING", str(_REQUIRED_EFFECTS_FILE))

    try:
        parsed = json.loads(_REQUIRED_EFFECTS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise _runtime_error("REQUIRED_EFFECTS_V1_INVALID_JSON", str(_REQUIRED_EFFECTS_FILE)) from exc

    if not isinstance(parsed, dict):
        raise _runtime_error("REQUIRED_EFFECTS_V1_INVALID", "root must be an object")

    version = _nonempty_str(parsed.get("version"))
    if version is None:
        raise _runtime_error("REQUIRED_EFFECTS_V1_INVALID", "version must be a non-empty string")

    format_defaults_raw = parsed.get("format_defaults")
    if not isinstance(format_defaults_raw, dict):
        raise _runtime_error("REQUIRED_EFFECTS_V1_INVALID", "format_defaults must be an object")

    format_defaults: Dict[str, Dict[str, int]] = {}
    for format_key_raw in sorted(format_defaults_raw.keys(), key=lambda item: str(item)):
        format_key = _nonempty_str(format_key_raw)
        if format_key is None:
            raise _runtime_error("REQUIRED_EFFECTS_V1_INVALID", "format_defaults keys must be non-empty strings")

        format_defaults[format_key] = _normalize_requirements_for_format(
            format_defaults_raw.get(format_key_raw),
            format_key=format_key,
        )

    return {
        "version": version,
        "format_defaults": format_defaults,
    }


def resolve_required_effects_v1(format: str, taxonomy_version: str | None = None) -> Tuple[Dict[str, Any], str]:
    payload = load_required_effects_v1()

    version = payload["version"]
    format_defaults = payload["format_defaults"]

    format_token = _nonempty_str(format)
    if format_token is None:
        format_token = ""

    requirements_raw = format_defaults.get(format_token)
    if not isinstance(requirements_raw, dict):
        requirements_raw = format_defaults.get(format_token.lower()) if isinstance(format_token, str) else None

    requirements = requirements_raw if isinstance(requirements_raw, dict) else {}
    requirements_sorted = {key: int(requirements[key]) for key in sorted(requirements.keys(), key=lambda item: str(item))}

    taxonomy_primitive_ids = sorted(_load_taxonomy_primitive_ids(taxonomy_version=taxonomy_version))

    return {
        "format": format_token,
        "requirements": requirements_sorted,
        "taxonomy_primitive_ids": taxonomy_primitive_ids,
    }, version
